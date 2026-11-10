import math
from datetime import datetime

import httpx

from miniapm.models import TimeWindow, ToolResult


class PrometheusBackend:
    def __init__(self, base_url: str = "http://127.0.0.1:9090") -> None:
        self.base_url = base_url.rstrip("/")

    def _query(self, client: httpx.Client, promql: str, at: datetime) -> float | None:
        response = client.get(
            "/api/v1/query",
            params={"query": promql, "time": at.isoformat()},
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise RuntimeError("Prometheus query failed")
        result = payload["data"]["result"]
        if not result:
            return None
        value = float(result[0]["value"][1])
        return value if math.isfinite(value) else None

    def query_metrics(self, service: str, start: datetime, end: datetime) -> str:
        window = TimeWindow(start=start, end=end)
        range_seconds = int((window.end - window.start).total_seconds())
        selected = f'service="{service}"'
        queries = {
            "qps": f"sum(rate(miniapm_requests_total{{{selected}}}[{range_seconds}s]))",
            "error_ratio": (
                f"sum(rate(miniapm_requests_total{{{selected},status=\"error\"}}[{range_seconds}s]))"
                f" / sum(rate(miniapm_requests_total{{{selected}}}[{range_seconds}s]))"
            ),
            "p95_seconds": (
                "histogram_quantile(0.95, sum by (le) (rate("
                f"miniapm_request_duration_seconds_bucket{{{selected}}}[{range_seconds}s])))"
            ),
            "p99_seconds": (
                "histogram_quantile(0.99, sum by (le) (rate("
                f"miniapm_request_duration_seconds_bucket{{{selected}}}[{range_seconds}s])))"
            ),
        }
        with httpx.Client(base_url=self.base_url, timeout=5) as client:
            values = {
                name: self._query(client, promql, window.end)
                for name, promql in queries.items()
            }
        data = None
        if any(value is not None for value in values.values()):
            data = {
                **values,
                "query_window_seconds": range_seconds,
                "promql": queries,
            }
        return ToolResult[object](
            status="ok" if data else "empty",
            window=window,
            source_kind="simulated",
            source_name="prometheus",
            data=data,
            warnings=["Metrics are queried from Prometheus at the window end"],
        ).model_dump_json()

    def _unavailable(self, start: datetime, end: datetime, kind: str) -> str:
        return ToolResult[object](
            status="error",
            window=TimeWindow(start=start, end=end),
            source_kind="simulated",
            source_name="prometheus",
            data=None,
            warnings=[f"{kind} are not available from the Prometheus-only backend"],
        ).model_dump_json()

    def query_traces(
        self, service: str, start: datetime, end: datetime, limit: int
    ) -> str:
        return self._unavailable(start, end, "traces")

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
        return self._unavailable(start, end, "logs")
