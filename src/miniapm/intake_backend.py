import math
from datetime import datetime

import httpx

from miniapm.models import TimeWindow, ToolResult

QUEUE_SELECTOR = 'queue=~"miniapm.logs|miniapm.spans"'


class IntakeBackend:
    def __init__(self, base_url: str = "http://127.0.0.1:9090") -> None:
        self.base_url = base_url.rstrip("/")

    def _query_vector(
        self, client: httpx.Client, promql: str, at: datetime
    ) -> list[dict]:
        response = client.get(
            "/api/v1/query", params={"query": promql, "time": at.isoformat()}
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise RuntimeError("Prometheus intake query failed")
        values = []
        for item in payload["data"]["result"]:
            value = float(item["value"][1])
            values.append({
                "labels": {
                    key: value
                    for key, value in item["metric"].items()
                    if key in {"queue", "table"}
                },
                "value": value if math.isfinite(value) else None,
            })
        return values

    def query_intake(self, start: datetime, end: datetime) -> str:
        window = TimeWindow(start=start, end=end)
        range_seconds = max(1, int((window.end - window.start).total_seconds()))
        queries = {
            "ready": f"sum by (queue) (rabbitmq_queue_messages_ready{{{QUEUE_SELECTOR}}})",
            "unacked": f"sum by (queue) (rabbitmq_queue_messages_unacked{{{QUEUE_SELECTOR}}})",
            "received_per_second": f"sum by (queue) (rate(miniapm_intake_received_total[{range_seconds}s]))",
            "acknowledged_per_second": f"sum by (queue) (rate(miniapm_intake_acknowledged_total[{range_seconds}s]))",
            "clickhouse_write_p95_seconds": (
                "histogram_quantile(0.95, sum by (le, table) (rate("
                f"miniapm_clickhouse_write_duration_seconds_bucket[{range_seconds}s])))"
            ),
            "ingestion_delay_p95_seconds": (
                "histogram_quantile(0.95, sum by (le, queue) (rate("
                f"miniapm_ingestion_delay_seconds_bucket[{range_seconds}s])))"
            ),
        }
        with httpx.Client(base_url=self.base_url, timeout=5, trust_env=False) as client:
            data = {
                name: self._query_vector(client, promql, window.end)
                for name, promql in queries.items()
            }
        data["query_window_seconds"] = range_seconds
        data["promql"] = queries
        has_samples = any(
            isinstance(value, list) and value for value in data.values()
        )
        return ToolResult[object](
            status="ok" if has_samples else "empty",
            window=window,
            source_kind="platform",
            source_name="prometheus_intake",
            data=data,
            warnings=[
                "Rates and histograms are evaluated at the window end over the full query window",
                "Ingestion delay includes the age of historical event timestamps",
                "Platform lag does not by itself prove or exclude a business fault",
            ],
        ).model_dump_json()
