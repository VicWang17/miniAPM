import json
from datetime import UTC, datetime

import httpx


def main() -> None:
    with httpx.Client(base_url="http://127.0.0.1:8123", timeout=10, trust_env=False) as client:
        def execute(query: str) -> None:
            response = client.post(
                "/", params={"query": query, "date_time_input_format": "best_effort"}
            )
            response.raise_for_status()

        def rows(query: str) -> list[dict]:
            response = client.post("/", params={"query": query + " FORMAT JSON"})
            response.raise_for_status()
            return response.json()["data"]

        execute("DROP TABLE IF EXISTS miniapm.ttl_probe")
        execute(
            """
            CREATE TABLE miniapm.ttl_probe (
                event_id UUID,
                event_time DateTime64(3, 'UTC')
            ) ENGINE = MergeTree
            ORDER BY event_time
            TTL event_time + INTERVAL 1 SECOND
            SETTINGS merge_with_ttl_timeout = 1
            """
        )
        try:
            now = datetime.now(UTC).isoformat()
            execute(
                """
                INSERT INTO miniapm.ttl_probe VALUES
                ('018f2f70-0000-7000-8000-000000000001', '2020-01-01 00:00:00.000'),
                ('018f2f70-0000-7000-8000-000000000002', '""" + now + "')"
            )
            execute("OPTIMIZE TABLE miniapm.ttl_probe FINAL")
            remaining = rows("SELECT event_id FROM miniapm.ttl_probe ORDER BY event_id")
            assert len(remaining) == 1
            assert remaining[0]["event_id"] == "018f2f70-0000-7000-8000-000000000002"
            print(json.dumps({"ttl": "expired row removed", "remaining": remaining}, indent=2))
        finally:
            execute("DROP TABLE IF EXISTS miniapm.ttl_probe")


if __name__ == "__main__":
    main()
