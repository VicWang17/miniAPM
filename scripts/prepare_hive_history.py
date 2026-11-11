import argparse
import json
from pathlib import Path

from miniapm.hive_history import prepare_partitioned_spans, upload_partitions


def main() -> None:
    parser = argparse.ArgumentParser(description="Write deduplicated date-partitioned spans and upload to HDFS")
    parser.add_argument("dataset_directory", type=Path)
    parser.add_argument("--max-events", type=int, default=100_000)
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()
    result = prepare_partitioned_spans(args.dataset_directory, args.max_events)
    if args.upload:
        result["hdfs_root"] = upload_partitions(args.dataset_directory)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
