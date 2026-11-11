import argparse
import json
from pathlib import Path

from miniapm.heterogeneous import HeterogeneousContext, HeterogeneousPipeline
from miniapm.rabbitmq_publisher import RabbitMQPublisher


def main():
    parser = argparse.ArgumentParser(description="Human approval gate for an adaptation rule proposal")
    parser.add_argument("proposal_path")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--replay-dataset")
    parser.add_argument("--replay-batch")
    args = parser.parse_args()
    context = HeterogeneousContext()
    publisher = RabbitMQPublisher() if args.confirm else None
    try:
        pipeline = HeterogeneousPipeline(context, publisher)
        result = pipeline.approve_rule(args.proposal_path, args.confirm)
        if args.confirm and args.replay_dataset and args.replay_batch:
            version = json.loads(Path(args.proposal_path).read_text())["rule"]["version"]
            result["replay"] = pipeline.replay(args.replay_dataset, args.replay_batch, version)
        print(json.dumps(result))
    finally:
        if publisher:
            publisher.close()
        context.close()


if __name__ == "__main__":
    main()
