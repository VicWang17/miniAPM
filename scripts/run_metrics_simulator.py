import argparse
import asyncio
from uuid import UUID

import uvicorn

from miniapm.generation import RequestParameters
from miniapm.metrics import MetricsPublisher, create_metrics_app
from miniapm.realtime import RealtimeSettings, run_realtime
from miniapm.topology import default_topology


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run the live simulator and metrics endpoint")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--rate", type=float, default=5)
    parser.add_argument("--max-in-flight", type=int, default=100)
    parser.add_argument("--fault-mode", choices=["normal", "slow", "mixed"], default="mixed")
    parser.add_argument(
        "--dataset-id",
        type=UUID,
        default="018f2f70-0000-7000-8000-000000000301",
    )
    args = parser.parse_args()

    publisher = MetricsPublisher()
    server = uvicorn.Server(
        uvicorn.Config(
            create_metrics_app(publisher.registry),
            host="0.0.0.0",
            port=args.port,
            log_level="warning",
        )
    )

    def parameters(request_number: int) -> RequestParameters:
        is_slow = args.fault_mode == "slow" or (
            args.fault_mode == "mixed" and request_number % 20 == 0
        )
        return RequestParameters(
            inventory_duration_seconds=1.6 if is_slow else 0.08,
            error=args.fault_mode == "mixed" and request_number % 100 == 99,
        )

    await asyncio.gather(
        server.serve(),
        run_realtime(
            default_topology(args.dataset_id),
            1_000_000,
            publisher,
            RealtimeSettings(
                requests_per_second=args.rate,
                max_in_flight=args.max_in_flight,
            ),
            parameters,
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
