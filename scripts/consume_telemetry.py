import argparse
import asyncio
import json
import os

from prometheus_client import start_http_server

from miniapm.clickhouse_writer import BatchingClickHouseWriter
from miniapm.intake_metrics import IntakeMetrics
from miniapm.rabbitmq_consumer import RabbitMQConsumer


async def main() -> None:
    parser = argparse.ArgumentParser(description="Consume RabbitMQ events into ClickHouse")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--prefetch", type=int, default=20)
    parser.add_argument("--flush-interval", type=float, default=1)
    parser.add_argument("--max-pending", type=int, default=2000)
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--retry-delay", type=float, default=1)
    parser.add_argument("--metrics-port", type=int, default=8767)
    parser.add_argument("--worker-id", default=f"consumer-{os.getpid()}")
    args = parser.parse_args()

    metrics = IntakeMetrics(args.worker_id)
    if args.metrics_port:
        start_http_server(args.metrics_port, registry=metrics.registry)
    writer = BatchingClickHouseWriter(
        batch_size=args.batch_size,
        flush_interval_seconds=args.flush_interval,
        max_pending_events=args.max_pending,
        metrics=metrics,
    )
    processed = 0
    async with RabbitMQConsumer(
        writer,
        prefetch=args.prefetch,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay,
        metrics=metrics,
    ) as consumer:
        while args.max_messages == 0 or processed < args.max_messages:
            remaining = args.max_messages - processed if args.max_messages else args.batch_size
            batch_limit = min(args.batch_size, remaining)
            logs = await consumer.drain("miniapm.logs", batch_limit)
            spans = await consumer.drain("miniapm.spans", batch_limit)
            processed += logs + spans
            if logs + spans == 0:
                if args.max_messages:
                    break
                await asyncio.sleep(0.2)
    print(json.dumps({"processed": processed}))


if __name__ == "__main__":
    asyncio.run(main())
