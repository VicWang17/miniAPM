import argparse
import json
from datetime import datetime
from pathlib import Path

from miniapm.fixtures import FIXTURE_END, FIXTURE_START
from miniapm.investigation import run_investigation
from miniapm.reports import save_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one fixture-backed investigation")
    parser.add_argument("question")
    parser.add_argument("--service", choices=["order", "inventory"], default="order")
    parser.add_argument("--start", type=datetime.fromisoformat, default=FIXTURE_START)
    parser.add_argument("--end", type=datetime.fromisoformat, default=FIXTURE_END)
    parser.add_argument("--output-dir", type=Path, default=Path("reports"))
    args = parser.parse_args()

    result = run_investigation(args.question, args.service, args.start, args.end)
    report = result["report"]
    json_path, markdown_path = save_report(report, args.output_dir)
    print(
        json.dumps(
            {
                "investigation_id": str(report.investigation_id),
                "end_reason": report.end_reason,
                "tool_calls": report.tool_calls,
                "duration_seconds": report.duration_seconds,
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
