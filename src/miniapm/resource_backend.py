from datetime import datetime

import httpx


class PrometheusResourceBackend:
    source_kind = "instrumented"
    source_name = "prometheus_linux_process"

    def __init__(self, base_url: str = "http://127.0.0.1:9090") -> None:
        self.base_url = base_url.rstrip("/")

    def query_resources(
        self, dataset_id: str, service: str, start: datetime, end: datetime
    ) -> list[dict]:
        seconds = max(1, int((end - start).total_seconds()))
        selector = f'dataset_id="{dataset_id}",service="{service}"'
        queries = {
            "process_cpu_cores": f"avg by (instance) (rate(miniapm_linux_process_cpu_seconds_total{{{selector}}}[30s]))",
            "process_rss_bytes": f"avg by (instance) (miniapm_linux_process_resident_memory_bytes{{{selector}}})",
        }
        resources = []
        with httpx.Client(base_url=self.base_url, timeout=5, trust_env=False) as client:
            for metric, query in queries.items():
                response = client.get(
                    "/api/v1/query_range",
                    params={
                        "query": query,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "step": max(5, seconds // 40),
                    },
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("status") != "success":
                    raise RuntimeError("Prometheus resource query failed")
                for series in payload["data"]["result"]:
                    resources.append({
                        "metric": metric,
                        "instance": series["metric"].get("instance", "unknown"),
                        "source_kind": "instrumented",
                        "values": series["values"][-41:],
                    })
        return resources
