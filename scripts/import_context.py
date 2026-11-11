import argparse
from datetime import UTC, datetime

from miniapm.mysql_context import DeploymentInput, MySQLContext
from miniapm.topology import default_topology


def main() -> None:
    parser = argparse.ArgumentParser(description="Import fixed service topology and deployments")
    parser.add_argument("dataset_id")
    args = parser.parse_args()

    topology = default_topology(args.dataset_id)
    deployments = [
        DeploymentInput(
            service="order",
            version="v1",
            deployed_at=datetime(2026, 11, 10, 0, 0, tzinfo=UTC),
            summary="initial order release",
        ),
        DeploymentInput(
            service="order",
            version="v2",
            deployed_at=datetime(2026, 11, 11, 0, 0, tzinfo=UTC),
            summary="order validation change",
        ),
    ]
    context = MySQLContext()
    try:
        context.import_context(topology, deployments)
        print(f"imported {len(topology.services)} services and {len(deployments)} deployments")
    finally:
        context.close()


if __name__ == "__main__":
    main()
