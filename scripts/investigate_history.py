import argparse
import json
from pathlib import Path
from uuid import UUID

from miniapm.file_backend import FileBackend
from miniapm.investigation import run_investigation
from miniapm.reports import save_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Investigate generated historical JSONL files")
    parser.add_argument("question")
    parser.add_argument("dataset_id", type=UUID)
    parser.add_argument("--service", choices=["order", "inventory"], default="order")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data/generated"))
    parser.add_argument("--output-dir", type=Path, default=Path("reports"))
    args = parser.parse_args()

    result = run_investigation(
        args.question,
        args.service,
        args.start,
        args.end,
        backend=FileBackend(args.data_root / str(args.dataset_id)),
    )
    report = result["report"]
    json_path, markdown_path = save_report(report, args.output_dir)
    print(
        json.dumps(
            {
                "investigation_id": str(report.investigation_id),
                "end_reason": report.end_reason,
                "tool_calls": report.tool_calls,
                "source_types": report.source_types,
                "evidence_statuses": [item.status for item in report.evidence],
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
