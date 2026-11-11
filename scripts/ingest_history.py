import argparse
import asyncio
import json
from pathlib import Path

from miniapm.clickhouse_writer import BatchingClickHouseWriter
from miniapm.models import LogEvent, SpanEvent


async def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest historical JSONL into ClickHouse")
    parser.add_argument("dataset_directory", type=Path)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--flush-interval", type=float, default=1)
    parser.add_argument("--max-pending", type=int, default=2000)
    args = parser.parse_args()
    observations = args.dataset_directory / "observations"

    async with BatchingClickHouseWriter(
        batch_size=args.batch_size,
        flush_interval_seconds=args.flush_interval,
        max_pending_events=args.max_pending,
    ) as writer:
        with (observations / "spans.jsonl").open(encoding="utf-8") as source:
            for line in source:
                await writer.add_span(SpanEvent.model_validate_json(line))
        with (observations / "logs.jsonl").open(encoding="utf-8") as source:
            for line in source:
                await writer.add_log(LogEvent.model_validate_json(line))
    print(json.dumps({"status": "ok", "dataset": str(args.dataset_directory)}))


if __name__ == "__main__":
    asyncio.run(main())
