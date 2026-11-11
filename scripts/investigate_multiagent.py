import argparse
import asyncio
import json
from pathlib import Path

from miniapm.clickhouse_backend import ClickHouseBackend
from miniapm.multiagent import arun_multiagent_investigation
from miniapm.mysql_context import MySQLContext
from miniapm.reports import save_report


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run the supervisor and domain subagents")
    parser.add_argument("question")
    parser.add_argument("dataset_id")
    parser.add_argument("--service", choices=["order", "inventory"], default="order")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("reports"))
    args = parser.parse_args()

    backend = ClickHouseBackend(dataset_id=args.dataset_id)
    context = MySQLContext()
    try:
        result = await arun_multiagent_investigation(
            args.question,
            args.service,
            args.start,
            args.end,
            backend,
            context,
            dataset_id=args.dataset_id,
        )
    finally:
        backend.close()
        context.close()
    report = result["report"]
    json_path, markdown_path = save_report(report, args.output_dir)
    print(
        json.dumps(
            {
                "investigation_id": str(report.investigation_id),
                "end_reason": report.end_reason,
                "supervisor_calls": report.tool_calls,
                "shared_tool_attempts": result["shared_tool_attempts"],
                "evidence": [(item.tool_name, item.status) for item in report.evidence],
                "json_report": str(json_path),
                "markdown_report": str(markdown_path),
                "final_answer": report.final_answer,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
