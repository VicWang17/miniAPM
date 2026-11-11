import argparse
import json
from pathlib import Path

from miniapm.rabbitmq_publisher import RabbitMQPublisher


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish historical events to RabbitMQ")
    parser.add_argument("dataset_directory", type=Path)
    args = parser.parse_args()
    observations = args.dataset_directory / "observations"
    publisher = RabbitMQPublisher()
    counts = {"logs": 0, "spans": 0}
    try:
        for routing_key in counts:
            with (observations / f"{routing_key}.jsonl").open(encoding="utf-8") as source:
                for line in source:
                    publisher.publish(routing_key, json.loads(line))
                    counts[routing_key] += 1
    finally:
        publisher.close()
    print(json.dumps({"published": counts}))


if __name__ == "__main__":
    main()
