import json
import os
import threading
from urllib.parse import quote

import pika


class RabbitMQPublisher:
    def __init__(self, url: str | None = None) -> None:
        if url is None:
            password = os.environ["RABBITMQ_PASSWORD"]
            host = os.getenv("RABBITMQ_HOST", "127.0.0.1")
            url = f"amqp://miniapm:{quote(password, safe='')}@{host}:5672/%2F"
        self.url = url
        self.lock = threading.Lock()
        self._connect()

    def _connect(self) -> None:
        self.connection = pika.BlockingConnection(pika.URLParameters(self.url))
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

    def publish(self, routing_key: str, event: dict) -> None:
        with self.lock:
            if self.connection.is_closed or self.channel.is_closed:
                self._connect()
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
        with self.lock:
            if self.connection.is_open:
                self.connection.close()
