import asyncio
import json
import os
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
    ) -> None:
        self.writer = writer
        self.connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url()))
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch)

    async def __aenter__(self) -> "RabbitMQConsumer":
        await self.writer.__aenter__()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    async def drain(self, queue: str, max_messages: int) -> int:
        pending_tags = []
        processed = 0
        try:
            while processed < max_messages:
                method, _, body = self.channel.basic_get(queue=queue, auto_ack=False)
                if method is None:
                    break
                pending_tags.append(method.delivery_tag)
                event = json.loads(body)
                if queue == "miniapm.logs":
                    await self.writer.add_log(LogEvent.model_validate(event))
                elif queue == "miniapm.spans":
                    await self.writer.add_span(SpanEvent.model_validate(event))
                else:
                    raise ValueError("unsupported queue")
                processed += 1
            await self.writer.flush()
            for tag in pending_tags:
                self.channel.basic_ack(delivery_tag=tag)
            return processed
        except Exception:
            for tag in pending_tags:
                self.channel.basic_nack(delivery_tag=tag, requeue=True)
            raise

    async def close(self) -> None:
        await self.writer.close()
        self.connection.close()
