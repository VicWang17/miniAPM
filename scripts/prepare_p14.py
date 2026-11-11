import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from miniapm.batch import BatchGenerationSettings, iter_request_batches
from miniapm.clickhouse_writer import BatchingClickHouseWriter
from miniapm.heterogeneous import HeterogeneousContext
from miniapm.topology import default_topology

DATASETS = {name: f"018f2f70-0000-7000-8000-0000000014{suffix}" for name, suffix in
            {"propagation": "01", "independent": "02", "insufficient": "03", "other": "04"}.items()}


async def main():
    context = HeterogeneousContext()
    schema = Path("deploy/mysql/init/004-create-correlation-groups.sql").read_text()
    try:
        with context.connection.cursor() as cursor:
            for statement in schema.split(";"):
                if statement.strip():
                    cursor.execute(statement)
            cursor.execute("DELETE FROM alert_candidate_groups WHERE dataset_id IN %s", (tuple(DATASETS.values()),))
            cursor.execute("DELETE FROM alerts WHERE event_key LIKE 'p14-%'")
        context.connection.commit()
        for dataset_id in DATASETS.values():
            context.import_context(default_topology(dataset_id), [])
        now = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10)
        alerts = []
        for name in ("propagation", "independent", "insufficient"):
            alerts.append((f"p14-{name}-order", DATASETS[name], "order", now, name))
            if name != "insufficient":
                alerts.append((f"p14-{name}-inventory", DATASETS[name], "inventory", now + timedelta(seconds=20), name))
        alerts.append(("p14-cross-dataset-order", DATASETS["other"], "order", now, "cross_dataset"))
        with context.connection.cursor() as cursor:
            for event_key, dataset_id, service, started, scenario in alerts:
                labels = {"service": service, "dataset_id": dataset_id, "environment": "local",
                          "source_kind": "simulated", "scenario": scenario,
                          "alertname": "HighLatency" if scenario in {"propagation", "insufficient"} else "IndependentAnomaly"}
                cursor.execute("""INSERT INTO alerts
                    (event_key,fingerprint,started_at,status,labels_json) VALUES (%s,%s,%s,'firing',%s)""",
                    (event_key, event_key, context._mysql_time(started), json.dumps(labels)))
        context.connection.commit()
        async with BatchingClickHouseWriter(batch_size=100) as writer:
            for batch in iter_request_batches(default_topology(DATASETS["propagation"]), 6, now,
                settings=BatchGenerationSettings(slow_rate=1, error_rate=0, missing_rate=0, slow_inventory_seconds=1.6)):
                for events in batch.events:
                    for span in events.spans:
                        await writer.add_span(span)
                    for log in events.logs:
                        await writer.add_log(log)
        groups = {name: context.create_candidate_group(f"p14-{name}-order") for name in ("propagation", "independent", "insufficient")}
        cross = context.create_candidate_group("p14-cross-dataset-order")
        assert len(groups["propagation"].members) == 2
        assert len(cross.members) == 1
        truth = {"propagation": {"expected_members": 2, "expected_relation": "supported",
                    "root_cause_may_be_unconfirmed": True},
                 "independent": {"expected_members": 2, "shared_root_required": False},
                 "insufficient": {"expected_members": 1, "expected_relation": "insufficient"},
                 "cross_dataset": {"expected_members": 1}}
        Path("data/generated/p14-truth.json").write_text(json.dumps(truth, indent=2))
        output = {"dataset_ids": DATASETS, "groups": {key: value.model_dump(mode="json") for key, value in groups.items()},
                  "cross_dataset_members": len(cross.members)}
        Path("data/generated/p14-groups.json").write_text(json.dumps(output, indent=2))
        print(json.dumps({"groups": {key: str(value.group_id) for key, value in groups.items()},
                          "cross_dataset_members": len(cross.members)}, indent=2))
    finally:
        context.close()


if __name__ == "__main__":
    asyncio.run(main())
