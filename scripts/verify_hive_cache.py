import argparse
import json
import re
import subprocess
from pathlib import Path

from miniapm.baseline_cache import CachedBaselineBackend
from run_hive_baseline import beeline

DATASET_ID = "018f2f70-0000-7000-8000-000000000702"


def explain(with_partition: bool) -> dict:
    partition = "AND dt = '2026-11-10'" if with_partition else ""
    query = f"""
EXPLAIN SELECT count(*) FROM miniapm_spans
WHERE dataset_id = '{DATASET_ID}' {partition} AND kind = 'server'
"""
    plan = beeline(query)
    match = re.search(r"Statistics: Num rows: (\d+) Data size: (\d+)", plan)
    if not match:
        raise AssertionError("missing TableScan statistics")
    return {
        "estimated_rows": int(match.group(1)),
        "estimated_data_bytes": int(match.group(2)),
        "has_dt_predicate": "dt = '2026-11-10'" in plan,
    }


def cache_experiment() -> list[dict]:
    backend = CachedBaselineBackend()
    arguments = (DATASET_ID, "order", "POST /orders", "2026-11-10", "2026-11-10")
    states = []
    try:
        backend.invalidate(*arguments)
        for expected in ("miss", "hit"):
            result = backend.query_baseline(*arguments)
            assert result["cache_status"] == expected
            states.append({"operation": expected, "status": result["cache_status"]})
        backend.invalidate(*arguments)
        result = backend.query_baseline(*arguments)
        assert result["cache_status"] == "miss"
        states.append({"operation": "invalidate", "status": result["cache_status"]})
        subprocess.run(["docker", "compose", "stop", "redis"], check=True)
        result = backend.query_baseline(*arguments)
        assert result["cache_status"] == "fallback_after_error"
        states.append({"operation": "redis_stopped", "status": result["cache_status"]})
    finally:
        backend.close()
        subprocess.run(["docker", "compose", "start", "redis"], check=True)
    return states


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Hive partition pruning and baseline cache behavior")
    parser.add_argument("--output", type=Path, default=Path("reports/p7-partition-cache.json"))
    args = parser.parse_args()
    report = {
        "dataset_id": DATASET_ID,
        "with_partition": explain(True),
        "without_partition": explain(False),
        "cache": cache_experiment(),
        "note": "Hive estimates and small samples demonstrate pruning behavior, not large-data performance.",
    }
    assert report["with_partition"]["estimated_data_bytes"] < report["without_partition"]["estimated_data_bytes"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
