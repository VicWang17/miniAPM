"""P13 noise review entry: manual one-shot run or single-process periodic loop.

Examples:
    uv run python scripts/noise_review.py --window-start 2026-11-11T16:00:00Z \
        --window-end 2026-11-11T16:35:00Z
    uv run python scripts/noise_review.py --loop --interval-minutes 10
"""

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from miniapm.models import TimeWindow
from miniapm.mysql_context import MySQLContext
from miniapm.noise import FindingStore, NoiseReviewScheduler, arun_review_once


class FailingModel:
    """Duck-typed model used only to verify the model-outage path."""

    def bind_tools(self, tools):
        return self

    async def ainvoke(self, messages, **kwargs):
        raise RuntimeError("simulated model outage")


def parse_window(args) -> TimeWindow:
    end = datetime.fromisoformat(args.window_end) if args.window_end else datetime.now(UTC)
    start = (
        datetime.fromisoformat(args.window_start)
        if args.window_start
        else end - timedelta(minutes=args.window_minutes)
    )
    return TimeWindow(start=start, end=end)


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="Run the P13 alert noise review")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--interval-minutes", type=int, default=10)
    parser.add_argument("--unclassified-limit", type=int, default=12)
    parser.add_argument("--suppressed-limit", type=int, default=4)
    parser.add_argument("--normal-limit", type=int, default=4)
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated/noise-review"))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--max-ticks", type=int, default=None)
    parser.add_argument("--simulate-model-failure", action="store_true")
    args = parser.parse_args()

    limits = {
        "unclassified": args.unclassified_limit,
        "suppressed": args.suppressed_limit,
        "normal": args.normal_limit,
    }
    store = FindingStore(args.output_dir / "findings.jsonl")
    model = FailingModel() if args.simulate_model_failure else None

    async def run_once() -> dict:
        context = MySQLContext()
        try:
            window = parse_window(args)
            return await arun_review_once(context, window, model=model, limits=limits)
        finally:
            context.close()

    async def run_and_record() -> dict:
        run = await run_once()
        record = store.record_run(run)
        out = args.output_dir
        out.mkdir(parents=True, exist_ok=True)
        (out / f"run-{run['run_id']}.json").write_text(
            json.dumps(run, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return {"run": run, "stored": record}

    if args.loop:
        scheduler = NoiseReviewScheduler(
            runner=run_and_record,
            interval_minutes=args.interval_minutes,
            logger=lambda message: print(message, flush=True),
        )
        records = await scheduler.run_forever(max_ticks=args.max_ticks)
        print(json.dumps(records, ensure_ascii=False, indent=2))
        return

    result = await run_and_record()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
