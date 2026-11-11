import argparse
import asyncio
import json
from pathlib import Path

from miniapm.clickhouse_backend import ClickHouseBackend
from miniapm.heterogeneous import HeterogeneousContext
from miniapm.investigation import build_chat_model
from miniapm.investigation_manager import InvestigationManager
from miniapm.reports import save_report


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("group_id")
    args = parser.parse_args()
    context = HeterogeneousContext()
    manager = InvestigationManager(context, lambda dataset_id: ClickHouseBackend(dataset_id=dataset_id))
    try:
        investigation_id = await manager.submit_group(args.group_id, build_chat_model())
        await manager.wait(investigation_id)
        record = context.investigation(investigation_id)
        if record["status"] == "completed":
            from miniapm.reports import InvestigationReport
            save_report(InvestigationReport.model_validate(record["report_json"]), Path("reports"))
        print(json.dumps(record, ensure_ascii=False, indent=2))
    finally:
        await manager.close()
        context.close()


if __name__ == "__main__":
    asyncio.run(main())
