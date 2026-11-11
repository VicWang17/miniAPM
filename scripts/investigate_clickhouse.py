import argparse
import json
from pathlib import Path

from miniapm.clickhouse_backend import ClickHouseBackend
from miniapm.investigation import run_investigation
from miniapm.mysql_context import MySQLContext
from miniapm.reports import save_report
from miniapm.resource_backend import PrometheusResourceBackend


def main() -> None:
    parser = argparse.ArgumentParser(description="Investigate telemetry stored in ClickHouse")
    parser.add_argument("question")
    parser.add_argument("dataset_id")
    parser.add_argument("--service", choices=["order", "inventory"], default="order")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("reports"))
    parser.add_argument("--source-kind", choices=["simulated", "instrumented"], default="simulated")
    parser.add_argument(
        "--store",
        action="store_true",
        help="Persist the finished report to MySQL so later knowledge-case extraction can use it",
    )
    args = parser.parse_args()

    backend = ClickHouseBackend(dataset_id=args.dataset_id, source_kind=args.source_kind)
    try:
        result = run_investigation(
            args.question,
            args.service,
            args.start,
            args.end,
            backend=backend,
            resource_backend=PrometheusResourceBackend() if args.source_kind == "instrumented" else None,
            dataset_id=args.dataset_id,
        )
    finally:
        backend.close()
    report = result["report"]
    json_path, markdown_path = save_report(report, args.output_dir)
    stored = False
    if args.store:
        context = MySQLContext()
        try:
            context.create_investigation(
                str(report.investigation_id),
                args.dataset_id,
                args.question,
                args.service,
                report.window,
                status="running",
            )
            context.complete_investigation(report)
            stored = True
        finally:
            context.close()
    print(
        json.dumps(
            {
                "investigation_id": str(report.investigation_id),
                "end_reason": report.end_reason,
                "tool_calls": report.tool_calls,
                "source_types": report.source_types,
                "stored_to_mysql": stored,
                "evidence": [
                    {
                        "tool": item.tool_name,
                        "status": item.status,
                        "summary_chars": len(item.result_summary),
                    }
                    for item in report.evidence
                ],
                "json_report": str(json_path),
                "markdown_report": str(markdown_path),
                "final_answer": report.final_answer,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
