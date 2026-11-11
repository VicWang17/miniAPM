import argparse
import json
from pathlib import Path

from miniapm.baseline import BaselineClickHouseBackend


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish Hive results to the ClickHouse baseline table")
    parser.add_argument("hive_results", type=Path)
    args = parser.parse_args()
    report = json.loads(args.hive_results.read_text(encoding="utf-8"))
    backend = BaselineClickHouseBackend()
    try:
        backend.replace_window(report)
        published = [
            backend.query_baseline(
                report["dataset_id"], row["service"], row["route"],
                report["start_date"], report["end_date"],
            )
            for row in report["rows"]
        ]
    finally:
        backend.close()
    print(json.dumps({"published_rows": len(published), "rows": published}, ensure_ascii=False))


if __name__ == "__main__":
    main()
