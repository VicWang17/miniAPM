import argparse
import json
from pathlib import Path

from miniapm.baseline import BaselineClickHouseBackend
from miniapm.baseline_cache import CachedBaselineBackend


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish Hive results to the ClickHouse baseline table")
    parser.add_argument("hive_results", type=Path)
    args = parser.parse_args()
    report = json.loads(args.hive_results.read_text(encoding="utf-8"))
    backend = BaselineClickHouseBackend()
    cache = CachedBaselineBackend(source=backend)
    try:
        backend.replace_window(report)
        for row in report["rows"]:
            cache.invalidate(
                report["dataset_id"], row["service"], row["route"],
                report["start_date"], report["end_date"],
            )
        published = [
            backend.query_baseline(
                report["dataset_id"], row["service"], row["route"],
                report["start_date"], report["end_date"],
            )
            for row in report["rows"]
        ]
    finally:
        cache.close()
    print(json.dumps({"published_rows": len(published), "rows": published}, ensure_ascii=False))


if __name__ == "__main__":
    main()
