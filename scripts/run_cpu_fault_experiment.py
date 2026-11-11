import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx

ORDER_URL = "http://127.0.0.1:8080"
INVENTORY_URL = "http://127.0.0.1:8081"


def request_latencies(client: httpx.Client, count: int = 5) -> list[float]:
    values = []
    for _ in range(count):
        started = time.perf_counter()
        response = client.post(f"{ORDER_URL}/orders")
        response.raise_for_status()
        values.append(time.perf_counter() - started)
    return values


def command(service: str, *arguments: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", service, *arguments],
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


def prometheus(query: str) -> list[dict]:
    response = httpx.get(
        "http://127.0.0.1:9090/api/v1/query",
        params={"query": query},
        trust_env=False,
    )
    response.raise_for_status()
    return response.json()["data"]["result"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run and verify a bounded Linux CPU fault")
    parser.add_argument("--output", type=Path, default=Path("reports/p8-cpu-experiment.json"))
    args = parser.parse_args()
    report = {"started_at": datetime.now(UTC).isoformat(), "container": "instrumented-inventory", "cpu_limit": 0.5}
    with httpx.Client(trust_env=False, timeout=10) as client:
        try:
            client.post(f"{INVENTORY_URL}/fault", json={"enabled": False}).raise_for_status()
            report["baseline_seconds"] = request_latencies(client)
            client.post(f"{INVENTORY_URL}/fault", json={"enabled": True}).raise_for_status()
            report["fault_started_at"] = datetime.now(UTC).isoformat()
            time.sleep(6)
            report["commands"] = {
                "pidstat": command("instrumented-inventory", "pidstat", "-p", "1", "1", "3"),
                "top": command("instrumented-inventory", "top", "-b", "-n", "1"),
                "vmstat": command("instrumented-inventory", "vmstat", "1", "2"),
            }
            report["fault_seconds"] = request_latencies(client)
            report["fault_cpu_rate"] = prometheus(
                'rate(miniapm_linux_process_cpu_seconds_total{service="inventory"}[20s])'
            )
        finally:
            client.post(f"{INVENTORY_URL}/fault", json={"enabled": False}).raise_for_status()
        time.sleep(25)
        report["recovery_seconds"] = request_latencies(client)
        report["recovery_cpu_rate"] = prometheus(
            'rate(miniapm_linux_process_cpu_seconds_total{service="inventory"}[20s])'
        )
    fault_median = sorted(report["fault_seconds"])[2]
    baseline_median = sorted(report["baseline_seconds"])[2]
    assert fault_median > baseline_median * 5
    assert max(report["recovery_seconds"]) < fault_median
    report["finished_at"] = datetime.now(UTC).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "commands"}, indent=2))
    for name, output in report["commands"].items():
        print(f"\n--- {name} ---\n{output}")


if __name__ == "__main__":
    main()
