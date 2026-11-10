import argparse
import json
from datetime import datetime
from pathlib import Path
from uuid import UUID

from miniapm.batch import BatchGenerationSettings
from miniapm.history import write_history
from miniapm.topology import default_topology


def main() -> None:
    parser = argparse.ArgumentParser(description="Write historical telemetry as JSONL and Parquet")
    parser.add_argument("--requests", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", type=datetime.fromisoformat, default="2026-11-11T00:00:00Z")
    parser.add_argument(
        "--dataset-id",
        type=UUID,
        default="018f2f70-0000-7000-8000-000000000101",
    )
    parser.add_argument("--output-root", type=Path, default=Path("data/generated"))
    args = parser.parse_args()

    result = write_history(
        default_topology(args.dataset_id),
        args.requests,
        args.start,
        args.output_root,
        BatchGenerationSettings(seed=args.seed, batch_size=args.batch_size),
    )
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
