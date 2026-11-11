import json
from datetime import UTC, datetime

import httpx


class BaselineClickHouseBackend:
    def __init__(self, base_url: str = "http://127.0.0.1:8123") -> None:
        self.client = httpx.Client(base_url=base_url, timeout=5, trust_env=False)

    def close(self) -> None:
        self.client.close()

    def _execute(self, query: str, parameters: dict | None = None) -> None:
        response = self.client.post("/", params={"query": query, **(parameters or {})})
        response.raise_for_status()

    def replace_window(self, report: dict) -> None:
        parameters = {
            "param_dataset_id": report["dataset_id"],
            "param_start": report["start_date"],
            "param_end": report["end_date"],
        }
        self._execute(
            """
            ALTER TABLE miniapm.baseline_metrics DELETE
            WHERE dataset_id = {dataset_id:UUID}
              AND window_start = toDate({start:String})
              AND window_end = toDate({end:String})
            """,
            parameters,
        )
        generated_at = datetime.now(UTC).isoformat()
        rows = [
            {
                "dataset_id": report["dataset_id"],
                "service": row["service"],
                "route": row["route"],
                "window_start": report["start_date"],
                "window_end": report["end_date"],
                "request_count": row["request_count"],
                "error_count": row["error_count"],
                "avg_seconds": row["avg_seconds"],
                "p95_seconds": row["p95_seconds"],
                "sample_count": row["request_count"],
                "generated_at": generated_at,
                "engine": report["engine"],
            }
            for row in report["rows"]
        ]
        if not rows:
            raise ValueError("cannot publish an empty baseline")
        payload = "\n".join(json.dumps(row) for row in rows) + "\n"
        response = self.client.post(
            "/",
            params={
                "query": "INSERT INTO miniapm.baseline_metrics FORMAT JSONEachRow",
                "date_time_input_format": "best_effort",
            },
            content=payload,
        )
        response.raise_for_status()

    def query_baseline(
        self, dataset_id: str, service: str, route: str, start_date: str, end_date: str
    ) -> dict | None:
        response = self.client.post(
            "/",
            params={
                "query": """
                    SELECT service, route, window_start, window_end, request_count,
                           error_count, avg_seconds, p95_seconds, sample_count,
                           generated_at, engine
                    FROM miniapm.baseline_metrics
                    WHERE dataset_id = {dataset_id:UUID} AND service = {service:String}
                      AND route = {route:String}
                      AND window_start = toDate({start:String})
                      AND window_end = toDate({end:String})
                    ORDER BY generated_at DESC LIMIT 1 FORMAT JSON
                """,
                "param_dataset_id": dataset_id,
                "param_service": service,
                "param_route": route,
                "param_start": start_date,
                "param_end": end_date,
            },
        )
        response.raise_for_status()
        rows = response.json()["data"]
        return rows[0] if rows else None
