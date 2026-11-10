import argparse
import json
from datetime import datetime
from uuid import UUID

from miniapm.batch import BatchGenerationSettings, GenerationStats, iter_request_batches
from miniapm.topology import default_topology


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate correlated telemetry in bounded batches")
    parser.add_argument("--requests", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", type=datetime.fromisoformat, default="2026-11-11T00:00:00Z")
    parser.add_argument(
        "--dataset-id",
        type=UUID,
        default="018f2f70-0000-7000-8000-000000000101",
    )
    args = parser.parse_args()

    settings = BatchGenerationSettings(seed=args.seed, batch_size=args.batch_size)
    total = GenerationStats()
    batch_count = 0
    for batch in iter_request_batches(
        default_topology(args.dataset_id), args.requests, args.start, settings=settings
    ):
        total = total.add(batch.stats)
        batch_count += 1
    print(json.dumps({"batch_count": batch_count, **total.model_dump()}, indent=2))


if __name__ == "__main__":
    main()
