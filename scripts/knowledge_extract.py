"""P12.03 offline extraction: report_json -> screened candidate case (pending).

Example:
    uv run python scripts/knowledge_extract.py <investigation_id>
    uv run python scripts/knowledge_extract.py <investigation_id> --dry-run
"""

import argparse
import json
from pathlib import Path
from uuid import uuid4

from miniapm.case_extract import extract_case
from miniapm.cases import build_candidate, screen_report
from miniapm.mysql_context import MySQLContext


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract a pending case from a finished investigation")
    parser.add_argument("investigation_id")
    parser.add_argument("--output-dir", type=Path, default=Path("data/generated/knowledge-candidates"))
    parser.add_argument("--dry-run", action="store_true", help="Extract and screen without writing MySQL")
    args = parser.parse_args()

    context = MySQLContext()
    try:
        record = context.investigation(args.investigation_id)
        if record is None:
            raise SystemExit(f"investigation not found: {args.investigation_id}")
        if record["status"] != "completed" or not record["report_json"]:
            raise SystemExit(
                f"investigation {args.investigation_id} is {record['status']} without a stored report"
            )
        report = record["report_json"]
        report_reason = screen_report(report)
        if report_reason:
            print(json.dumps({"result": "rejected_by_report_screen", "reason": report_reason}, ensure_ascii=False, indent=2))
            return

        extraction = extract_case(report)
        candidate, candidate_reason = build_candidate(extraction)
        if candidate is None:
            print(
                json.dumps(
                    {
                        "result": "rejected_by_candidate_screen",
                        "reason": candidate_reason,
                        "model_uncertain": extraction.uncertain,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        payload = {
            "investigation_id": args.investigation_id,
            "candidate": candidate.model_dump(),
            "model_uncertain": extraction.uncertain,
            "source_end_reason": report.get("end_reason"),
            "source_evidence_count": len(report.get("evidence") or []),
        }
        args.output_dir.mkdir(parents=True, exist_ok=True)
        dump_path = args.output_dir / f"candidate-{args.investigation_id}.json"
        dump_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        if args.dry_run:
            print(json.dumps({**payload, "result": "dry_run", "dump": str(dump_path)}, ensure_ascii=False, indent=2))
            return

        row = context.upsert_case_candidate(
            case_id=str(uuid4()),
            investigation_id=args.investigation_id,
            dataset_id=record["dataset_id"],
            service=record["service"],
            alert_fingerprint=context.alert_fingerprint(args.investigation_id),
            summary=candidate.summary,
            root_cause=candidate.root_cause,
            resolution=candidate.resolution,
            result_evidence=candidate.result_evidence,
            keywords=candidate.keywords,
        )
        print(json.dumps({**row, "result": "pending", "dump": str(dump_path)}, ensure_ascii=False, indent=2))
    finally:
        context.close()


if __name__ == "__main__":
    main()
