import argparse
import asyncio
import json
from pathlib import Path

from miniapm.clickhouse_writer import BatchingClickHouseWriter, ClickHouseHttpClient
from miniapm.models import LogEvent
from miniapm.rabbitmq_consumer import RabbitMQConsumer
from miniapm.rabbitmq_publisher import RabbitMQPublisher

QUEUES = ("miniapm.logs", "miniapm.spans")


def queue_depths(publisher: RabbitMQPublisher) -> dict[str, int]:
    return {
        queue: publisher.channel.queue_declare(queue=queue, passive=True).method.message_count
        for queue in QUEUES
    }


def event_count(client: ClickHouseHttpClient, table: str, event_id: str) -> int:
    response = client.client.post(
        "/",
        params={"query": f"SELECT count() FROM miniapm.{table} WHERE event_id = '{event_id}'"},
    )
    response.raise_for_status()
    return int(response.text.strip())


async def main() -> None:
    parser = argparse.ArgumentParser(description="Verify backlog recovery and pre-ack disconnection")
    parser.add_argument("dataset_directory", type=Path)
    args = parser.parse_args()
    observations = args.dataset_directory / "observations"

    publisher = RabbitMQPublisher()
    assert queue_depths(publisher) == dict.fromkeys(QUEUES, 0), "telemetry queues must be empty"
    published = {}
    for routing_key in ("logs", "spans"):
        with (observations / f"{routing_key}.jsonl").open(encoding="utf-8") as source:
            events = [json.loads(line) for line in source]
        for event in events:
            publisher.publish(routing_key, event)
        published[routing_key] = len(events)
    backlog = queue_depths(publisher)
    assert backlog == {"miniapm.logs": published["logs"], "miniapm.spans": published["spans"]}

    async with RabbitMQConsumer(BatchingClickHouseWriter(), prefetch=100) as consumer:
        resumed = {
            "miniapm.logs": await consumer.drain("miniapm.logs", 100),
            "miniapm.spans": await consumer.drain("miniapm.spans", 100),
        }
    recovered = queue_depths(publisher)
    assert resumed == backlog and recovered == dict.fromkeys(QUEUES, 0)

    event = LogEvent.model_validate_json(
        (observations / "logs.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    event_id = str(event.event_id)
    client = ClickHouseHttpClient()
    raw_before = event_count(client, "logs", event_id)
    logical_before = event_count(client, "logs_dedup", event_id)
    publisher.publish("logs", event.model_dump(mode="json"))
    fault_consumer = RabbitMQPublisher()
    method, _, body = fault_consumer.channel.basic_get("miniapm.logs", auto_ack=False)
    assert method is not None
    async with BatchingClickHouseWriter(client=ClickHouseHttpClient()) as writer:
        await writer.add_log(LogEvent.model_validate_json(body))
        await writer.flush()
    raw_after_disconnect = event_count(client, "logs", event_id)
    assert raw_after_disconnect == raw_before + 1
    fault_consumer.connection.close()
    assert queue_depths(publisher)["miniapm.logs"] == 1

    async with RabbitMQConsumer(BatchingClickHouseWriter()) as consumer:
        redelivered = await consumer.drain("miniapm.logs", 1)
    raw_after_recovery = event_count(client, "logs", event_id)
    logical_after = event_count(client, "logs_dedup", event_id)
    assert redelivered == 1
    assert raw_after_recovery == raw_after_disconnect + 1
    assert logical_before == logical_after == 1
    assert queue_depths(publisher)["miniapm.logs"] == 0
    publisher.close()
    client.close()
    print(json.dumps({
        "published": published,
        "backlog": backlog,
        "resumed": resumed,
        "recovered": recovered,
        "disconnect_event_id": event_id,
        "raw_counts": [raw_before, raw_after_disconnect, raw_after_recovery],
        "logical_counts": [logical_before, logical_after],
        "redelivered": redelivered,
    }))


if __name__ == "__main__":
    asyncio.run(main())
