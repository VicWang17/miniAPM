import argparse
import asyncio
import json
from pathlib import Path

from miniapm.clickhouse_writer import BatchingClickHouseWriter, ClickHouseHttpClient
from miniapm.models import LogEvent
from miniapm.rabbitmq_consumer import RabbitMQConsumer
from miniapm.rabbitmq_publisher import RabbitMQPublisher


def count_events(client: ClickHouseHttpClient, table: str, dataset_id: str) -> int:
    response = client.client.post(
        "/",
        params={"query": f"SELECT count() FROM miniapm.{table} WHERE dataset_id = '{dataset_id}'"},
    )
    response.raise_for_status()
    return int(response.text.strip())


def depths(publisher: RabbitMQPublisher) -> dict[str, int]:
    return {
        queue: publisher.channel.queue_declare(queue=queue, passive=True).method.message_count
        for queue in ("miniapm.logs", "miniapm.spans")
    }


def publish_file(publisher: RabbitMQPublisher, observations: Path) -> dict[str, int]:
    counts = {}
    for routing_key in ("logs", "spans"):
        with (observations / f"{routing_key}.jsonl").open(encoding="utf-8") as source:
            events = [json.loads(line) for line in source]
        for event in events:
            publisher.publish(routing_key, event)
        counts[routing_key] = len(events)
    return counts


async def main() -> None:
    parser = argparse.ArgumentParser(description="Verify file replay is distinct from broker redelivery")
    parser.add_argument("dataset_directory", type=Path)
    args = parser.parse_args()
    observations = args.dataset_directory / "observations"
    dataset_id = args.dataset_directory.name
    publisher = RabbitMQPublisher()
    assert depths(publisher) == {"miniapm.logs": 0, "miniapm.spans": 0}
    client = ClickHouseHttpClient()
    expected = publish_file(publisher, observations)
    async with RabbitMQConsumer(BatchingClickHouseWriter(), prefetch=100) as consumer:
        first = {
            "logs": await consumer.drain("miniapm.logs", 100),
            "spans": await consumer.drain("miniapm.spans", 100),
        }
    assert first == expected
    first_counts = {
        table: count_events(client, table, dataset_id)
        for table in ("logs", "spans", "logs_dedup", "spans_dedup")
    }

    publish_file(publisher, observations)
    method, properties, body = publisher.channel.basic_get("miniapm.logs", auto_ack=False)
    assert method is not None and not method.redelivered
    async with BatchingClickHouseWriter(client=ClickHouseHttpClient()) as writer:
        await writer.add_log(LogEvent.model_validate_json(body))
        await writer.flush()
    publisher.channel.basic_ack(method.delivery_tag)
    async with RabbitMQConsumer(BatchingClickHouseWriter(), prefetch=100) as consumer:
        second = {
            "logs": 1 + await consumer.drain("miniapm.logs", 100),
            "spans": await consumer.drain("miniapm.spans", 100),
        }
    assert second == expected
    second_counts = {
        table: count_events(client, table, dataset_id)
        for table in ("logs", "spans", "logs_dedup", "spans_dedup")
    }
    assert second_counts["logs"] == first_counts["logs"] * 2
    assert second_counts["spans"] == first_counts["spans"] * 2
    assert second_counts["logs_dedup"] == first_counts["logs_dedup"] == expected["logs"]
    assert second_counts["spans_dedup"] == first_counts["spans_dedup"] == expected["spans"]
    assert depths(publisher) == {"miniapm.logs": 0, "miniapm.spans": 0}
    print(json.dumps({
        "replay_count": 2,
        "file_replay_redelivered": method.redelivered,
        "first_counts": first_counts,
        "second_counts": second_counts,
        "queue_depths": depths(publisher),
    }))
    publisher.close()
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
