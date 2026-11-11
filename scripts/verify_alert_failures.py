import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

from miniapm.alerts import AlertmanagerAlert
from miniapm.clickhouse_backend import ClickHouseBackend
from miniapm.investigation_manager import InvestigationManager
from miniapm.mysql_context import MySQLContext

DATASET_ID = UUID("018f2f70-0000-7000-8000-000000000606")
STARTED_AT = datetime.now(UTC) - timedelta(minutes=1)


class FailingModel:
    def bind_tools(self, tools):
        return self

    async def ainvoke(self, messages):
        raise RuntimeError("controlled model outage")


class BlockingModel:
    def bind_tools(self, tools):
        return self

    async def ainvoke(self, messages):
        await asyncio.Event().wait()


def alert(name: str) -> AlertmanagerAlert:
    return AlertmanagerAlert(
        status="firing",
        labels={"alertname": "FailureProbe", "service": "order"},
        annotations={"summary": name},
        startsAt=STARTED_AT,
        endsAt=datetime.min.replace(tzinfo=UTC),
        fingerprint=f"p606-{name}",
    )


async def wait_terminal(manager: InvestigationManager, investigation_id: str) -> dict:
    await asyncio.wait_for(manager.wait(investigation_id), timeout=10)
    return manager.context.investigation(investigation_id)


async def main() -> None:
    context = MySQLContext()
    manager = InvestigationManager(
        context, lambda dataset_id: ClickHouseBackend(dataset_id=dataset_id)
    )
    failed_record = await manager.submit_alert(alert("model-outage"), DATASET_ID, FailingModel())
    failed = await wait_terminal(manager, failed_record["investigation_id"])
    assert failed["status"] == "completed"
    assert failed["report_json"]["end_reason"] == "model_error"
    assert context.alert(failed_record["event_key"])["status"] == "firing"
    duplicate = await manager.submit_alert(alert("model-outage"), DATASET_ID, FailingModel())
    assert duplicate["result"] == "duplicate"
    await manager.close()

    context = MySQLContext()
    manager = InvestigationManager(
        context, lambda dataset_id: ClickHouseBackend(dataset_id=dataset_id)
    )
    interrupted_record = await manager.submit_alert(
        alert("process-interruption"), DATASET_ID, BlockingModel()
    )
    for _ in range(100):
        current = context.investigation(interrupted_record["investigation_id"])
        if current["status"] == "running":
            break
        await asyncio.sleep(0.05)
    assert current["status"] == "running"
    await manager.close()
    interrupted = context.investigation(interrupted_record["investigation_id"])
    assert interrupted["status"] == "interrupted"
    assert context.alert(interrupted_record["event_key"])["status"] == "firing"
    context.close()
    print(json.dumps({
        "model_unavailable": {
            "status": failed["status"],
            "end_reason": failed["report_json"]["end_reason"],
        },
        "duplicate_after_failure": duplicate["result"],
        "process_interruption": {"status": interrupted["status"], "error": interrupted["error_reason"]},
        "alerts_preserved": True,
    }))


if __name__ == "__main__":
    asyncio.run(main())
