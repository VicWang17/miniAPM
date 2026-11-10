import argparse
import asyncio
import json
from uuid import UUID

from miniapm.generation import RequestParameters
from miniapm.realtime import RealtimeSettings, RecordingPublisher, run_realtime
from miniapm.topology import default_topology


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run a bounded wall-clock telemetry replay")
    parser.add_argument("--requests", type=int, default=4)
    parser.add_argument("--rate", type=float, default=4)
    parser.add_argument("--max-in-flight", type=int, default=10)
    parser.add_argument("--slow-every", type=int, default=0)
    parser.add_argument("--slow-seconds", type=float, default=0.3)
    parser.add_argument(
        "--dataset-id",
        type=UUID,
        default="018f2f70-0000-7000-8000-000000000101",
    )
    args = parser.parse_args()
    publisher = RecordingPublisher(args.requests)

    def parameters(request_number: int) -> RequestParameters:
        is_slow = args.slow_every > 0 and request_number % args.slow_every == 0
        return RequestParameters(
            inventory_duration_seconds=args.slow_seconds if is_slow else 0.08
        )

    summary = await run_realtime(
        default_topology(args.dataset_id),
        args.requests,
        publisher,
        RealtimeSettings(
            requests_per_second=args.rate,
            max_in_flight=args.max_in_flight,
        ),
        parameters,
    )
    print(json.dumps(summary.model_dump(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
