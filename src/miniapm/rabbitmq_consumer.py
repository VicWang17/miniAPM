import asyncio
import json
import os
import time
from urllib.parse import quote

import pika

from miniapm.clickhouse_writer import BatchingClickHouseWriter
from miniapm.models import LogEvent, SpanEvent


def rabbitmq_url() -> str:
    password = quote(os.environ["RABBITMQ_PASSWORD"], safe="")
    return f"amqp://miniapm:{password}@127.0.0.1:5672/%2F"


class RabbitMQConsumer:
    def __init__(
        self,
        writer: BatchingClickHouseWriter,
        prefetch: int = 20,
        max_retries: int = 2,
        retry_delay_seconds: float = 1,
    ) -> None:
        self.writer = writer
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url()))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch)
        self.channel.confirm_delivery()

    async def __aenter__(self) -> "RabbitMQConsumer":
        await self.writer.__aenter__()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    def _retry_or_dead_letter(self, queue: str, pending: list[tuple[int, dict]]) -> None:
        routing_key = queue.removeprefix("miniapm.")
        for delivery_tag, event in pending:
            retry_count = event.pop("_retry_count", 0)
            properties = pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
                message_id=event["event_id"],
            )
            if retry_count < self.max_retries:
                destination = routing_key
                event["_retry_count"] = retry_count + 1
            else:
                destination = "dead-letter"
            time.sleep(self.retry_delay_seconds)
            self.channel.basic_publish(
                exchange="miniapm.telemetry",
                routing_key=destination,
                body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                properties=properties,
                mandatory=True,
            )
            self.channel.basic_ack(delivery_tag=delivery_tag)

    async def drain(self, queue: str, max_messages: int) -> int:
        pending = []
        processed = 0
        try:
            for method, _, body in self.channel.consume(
                queue=queue, auto_ack=False, inactivity_timeout=0.2
            ):
                if method is None:
                    break
                event = json.loads(body)
                pending.append((method.delivery_tag, event))
                if queue == "miniapm.logs":
                    await self.writer.add_log(LogEvent.model_validate(event))
                elif queue == "miniapm.spans":
                    await self.writer.add_span(SpanEvent.model_validate(event))
                else:
                    raise ValueError("unsupported queue")
                processed += 1
                if processed >= max_messages:
                    break
            self.channel.cancel()
            await self.writer.flush()
            for delivery_tag, _ in pending:
                self.channel.basic_ack(delivery_tag=delivery_tag)
            return processed
        except Exception:
            self.channel.cancel()
            if pending:
                self._retry_or_dead_letter(queue, pending)
                return processed
            raise

    async def close(self) -> None:
        await self.writer.close()
        self.connection.close()
