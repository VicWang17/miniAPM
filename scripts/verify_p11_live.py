import json
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

from miniapm.mysql_context import MySQLContext


def main():
    client = httpx.Client(trust_env=False, timeout=5)
    context = MySQLContext()
    try:
        with context.connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM investigations")
            before = cursor.fetchone()[0]
        groups = client.get("http://127.0.0.1:9090/api/v1/rules").json()["data"]["groups"]
        loaded = next(group for group in groups if group["name"] == "miniapm-slo-experiments")
        alert = {"labels": {"alertname": "P11WiringProbe", "policy": "experiment",
            "strategy": "latency_slo", "service": "order", "dataset_id": "018f2f70-0000-7000-8000-000000000606"},
            "annotations": {"summary": "P11 isolated wiring probe; not a real SLO firing"},
            "startsAt": datetime.now(UTC).isoformat()}
        response = client.post("http://127.0.0.1:9093/api/v2/alerts", json=[alert])
        response.raise_for_status()
        time.sleep(4)
        notifications = client.get("http://127.0.0.1:8000/api/v1/alerts/experiments").json()["notifications"]
        matched = [item for item in notifications if any(a["labels"]["alertname"] == "P11WiringProbe" for a in item["alerts"])]
        assert matched and matched[-1]["receiver"] == "miniapm-experiment-log"
        with context.connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM investigations")
            after = cursor.fetchone()[0]
        assert before == after
        result = {"loaded_rules": len(loaded["rules"]), "alertmanager_post": response.status_code,
                  "receiver": matched[-1]["receiver"], "investigations_before": before,
                  "investigations_after": after, "scope": "short wiring probe only; not a real long-window firing"}
        Path("reports/p11-live.json").write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
    finally:
        context.close()
        client.close()


if __name__ == "__main__":
    main()
