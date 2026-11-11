from datetime import datetime

import httpx

from miniapm.models import TimeWindow, ToolResult


class ClickHouseBackend:
    def __init__(
        self, base_url: str = "http://127.0.0.1:8123", dataset_id: str | None = None
    ) -> None:
        self.client = httpx.Client(base_url=base_url, timeout=5, trust_env=False)
        self.dataset_id = dataset_id

    def _dataset_filter(self, parameters: dict) -> str:
        if self.dataset_id:
            parameters["dataset_id"] = self.dataset_id
            return "dataset_id = {dataset_id:UUID}"
        return "1 = 1"

    def close(self) -> None:
        self.client.close()

    def _query(self, sql: str, parameters: dict) -> list[dict]:
        response = self.client.post(
            "/",
            params={
                "query": sql + " FORMAT JSON",
                **{f"param_{key}": value for key, value in parameters.items()},
            },
        )
        response.raise_for_status()
        return response.json()["data"]

    def _result(self, window: TimeWindow, data: object) -> str:
        return ToolResult[object](
            status="ok" if data else "empty",
            window=window,
            source_kind="simulated",
            source_name="clickhouse",
            data=data,
            warnings=["ClickHouse results are bounded query samples"],
        ).model_dump_json()

    def query_logs(
        self,
        service: str,
        start: datetime,
        end: datetime,
        trace_id: str | None,
        version: str | None,
        error_code: str | None,
        limit: int,
    ) -> str:
        window = TimeWindow(start=start, end=end)
        filters = [
            "service = {service:String}",
            "event_time >= parseDateTime64BestEffort({start:String}, 3, 'UTC')",
            "event_time < parseDateTime64BestEffort({end:String}, 3, 'UTC')",
        ]
        parameters = {"service": service, "start": window.start.isoformat(), "end": window.end.isoformat(), "limit": max(1, min(limit, 200))}
        filters.append(self._dataset_filter(parameters))
        if trace_id:
            filters.append("trace_id = {trace_id:String}")
            parameters["trace_id"] = trace_id
        if version:
            filters.append("version = {version:String}")
            parameters["version"] = version
        if error_code:
            filters.append("error_code = {error_code:String}")
            parameters["error_code"] = error_code
        rows = self._query(
            "SELECT * FROM miniapm.logs_dedup WHERE " + " AND ".join(filters) + " ORDER BY event_time, trace_id LIMIT {limit:UInt32}",
            parameters,
        )
        return self._result(window, rows)

    def query_traces(
        self, service: str, start: datetime, end: datetime, limit: int
    ) -> str:
        window = TimeWindow(start=start, end=end)
        parameters = {
            "service": service,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
            "limit": max(1, min(limit, 20)),
        }
        dataset_filter = self._dataset_filter(parameters)
        rows = self._query(
            f"""
            WITH candidates AS (
                SELECT trace_id
                FROM miniapm.spans_dedup
                WHERE service = {{service:String}}
                  AND {dataset_filter}
                  AND started_at >= parseDateTime64BestEffort({{start:String}}, 3, 'UTC')
                  AND started_at < parseDateTime64BestEffort({{end:String}}, 3, 'UTC')
                GROUP BY trace_id
                ORDER BY max(ended_at - started_at) DESC
                LIMIT {{limit:UInt32}}
            )
            SELECT *, dateDiff('millisecond', started_at, ended_at) / 1000 AS duration_seconds
            FROM miniapm.spans_dedup
            WHERE trace_id IN candidates
            ORDER BY trace_id, started_at, span_id
            """,
            parameters,
        )
        grouped = {}
        for row in rows:
            grouped.setdefault(row["trace_id"], []).append(row)
        traces = [self._trace_tree(trace_id, spans) for trace_id, spans in grouped.items()]
        traces.sort(key=lambda trace: trace["duration_seconds"] or -1, reverse=True)
        return self._result(window, traces)

    def _trace_tree(self, trace_id: str, rows: list[dict]) -> dict:
        nodes = {}
        for row in rows:
            nodes[row["span_id"]] = {
                "span_id": row["span_id"],
                "parent_span_id": row["parent_span_id"],
                "service": row["service"],
                "instance": row["instance"],
                "version": row["version"],
                "kind": row["kind"],
                "status": row["status"],
                "duration_seconds": row["duration_seconds"],
                "missing": False,
                "children": [],
            }
        roots = []
        for node in list(nodes.values()):
            parent_id = node["parent_span_id"]
            if parent_id is None:
                roots.append(node)
            elif parent_id in nodes:
                nodes[parent_id]["children"].append(node)
            else:
                missing_parent = nodes.get(parent_id)
                if missing_parent is None:
                    missing_parent = {
                        "span_id": parent_id,
                        "parent_span_id": None,
                        "service": "unknown",
                        "instance": "unknown",
                        "version": "unknown",
                        "kind": "unknown",
                        "status": "unknown",
                        "duration_seconds": None,
                        "missing": True,
                        "children": [],
                    }
                    nodes[parent_id] = missing_parent
                    roots.append(missing_parent)
                missing_parent["children"].append(node)
        root = next((node for node in nodes.values() if node["parent_span_id"] is None and not node["missing"]), None)
        return {
            "trace_id": trace_id,
            "root_service": root["service"] if root else "unknown",
            "duration_seconds": root["duration_seconds"] if root else None,
            "roots": roots,
        }

    def window_metrics(self, service: str, start: datetime, end: datetime) -> dict:
        window = TimeWindow(start=start, end=end)
        parameters = {
            "service": service,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
        }
        dataset_filter = self._dataset_filter(parameters)
        rows = self._query(
            f"""
            SELECT count() AS request_count,
                   countIf(status = 'error') AS error_count,
                   avg(dateDiff('millisecond', started_at, ended_at) / 1000) AS avg_seconds,
                   quantile(0.95)(dateDiff('millisecond', started_at, ended_at) / 1000) AS p95_seconds
            FROM miniapm.spans_dedup
            WHERE service = {{service:String}} AND kind = 'server'
              AND {dataset_filter}
              AND started_at >= parseDateTime64BestEffort({{start:String}}, 3, 'UTC')
              AND started_at < parseDateTime64BestEffort({{end:String}}, 3, 'UTC')
            """,
            parameters,
        )
        return rows[0]

    def group_analysis(
        self, service: str, start: datetime, end: datetime, dimension: str
    ) -> str:
        if dimension not in {"version", "instance", "route", "error_code"}:
            raise ValueError("unsupported group dimension")
        window = TimeWindow(start=start, end=end)
        parameters = {
            "service": service,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
        }
        dataset_filter = self._dataset_filter(parameters)
        server_filter = f"""
            service = {{service:String}} AND kind = 'server'
            AND {dataset_filter}
            AND started_at >= parseDateTime64BestEffort({{start:String}}, 3, 'UTC')
            AND started_at < parseDateTime64BestEffort({{end:String}}, 3, 'UTC')
        """
        if dimension == "error_code":
            rows = self._query(
                f"""
                WITH server_spans AS (
                    SELECT trace_id, dateDiff('millisecond', started_at, ended_at) / 1000 AS duration_seconds
                    FROM miniapm.spans_dedup WHERE {server_filter}
                ), totals AS (
                    SELECT count() AS request_count FROM server_spans
                ), affected AS (
                    SELECT DISTINCT trace_id, error_code FROM miniapm.logs_dedup
                    WHERE service = {{service:String}}
                      AND {dataset_filter}
                      AND event_time >= parseDateTime64BestEffort({{start:String}}, 3, 'UTC')
                      AND event_time < parseDateTime64BestEffort({{end:String}}, 3, 'UTC')
                      AND error_code IS NOT NULL
                )
                SELECT affected.error_code AS group_value,
                       totals.request_count AS request_count,
                       count() AS error_count,
                       avg(server_spans.duration_seconds) AS avg_seconds,
                       max(server_spans.duration_seconds) AS max_seconds,
                       quantile(0.95)(server_spans.duration_seconds) AS p95_seconds
                FROM affected
                INNER JOIN server_spans USING (trace_id)
                CROSS JOIN totals
                GROUP BY group_value, request_count
                ORDER BY error_count DESC, group_value
                """,
                parameters,
            )
        else:
            rows = self._query(
                f"""
                SELECT {dimension} AS group_value,
                       count() AS request_count,
                       countIf(status = 'error') AS error_count,
                       avg(dateDiff('millisecond', started_at, ended_at) / 1000) AS avg_seconds,
                       max(dateDiff('millisecond', started_at, ended_at) / 1000) AS max_seconds,
                       quantile(0.95)(dateDiff('millisecond', started_at, ended_at) / 1000) AS p95_seconds
                FROM miniapm.spans_dedup
                WHERE {server_filter}
                GROUP BY group_value
                ORDER BY group_value
                """,
                parameters,
            )
        if dimension == "error_code":
            total_rows = self._query(
                f"SELECT count() AS total FROM miniapm.spans_dedup WHERE {server_filter}",
                parameters,
            )
            total = total_rows[0]["total"]
        else:
            total = sum(row["request_count"] for row in rows)
        return self._result(
            window,
            {
                "dimension": dimension,
                "request_denominator": total,
                "groups": rows,
            },
        )

    def query_metrics(self, service: str, start: datetime, end: datetime) -> str:
        window = TimeWindow(start=start, end=end)
        data = self.window_metrics(service, window.start, window.end)
        return self._result(window, data if data["request_count"] else None)
