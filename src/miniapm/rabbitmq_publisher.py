import json
import os
from urllib.parse import quote

import pika


class RabbitMQPublisher:
    def __init__(self, url: str | None = None) -> None:
        if url is None:
            password = os.environ["RABBITMQ_PASSWORD"]
            url = f"amqp://miniapm:{quote(password, safe='')}@127.0.0.1:5672/%2F"
        self.connection = pika.BlockingConnection(pika.URLParameters(url))
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

    def publish(self, routing_key: str, event: dict) -> None:
        self.channel.basic_publish(
            exchange="miniapm.telemetry",
            routing_key=routing_key,
            body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
                message_id=event["event_id"],
            ),
            mandatory=True,
        )

    def close(self) -> None:
        self.connection.close()
