"""P12.04 deterministic approval: human confirms or rejects a pending case.

Examples:
    uv run python scripts/knowledge_approve.py <case_id> \
        --result-evidence "order P95 2.0s -> 0.2s during 14:00-14:15 UTC"
    uv run python scripts/knowledge_approve.py <case_id> --reject "no verified recovery"
"""

import argparse
import json

from miniapm.cases import CaseCandidate, screen_candidate
from miniapm.mysql_context import MySQLContext


def main() -> None:
    parser = argparse.ArgumentParser(description="Approve or reject a pending historical case")
    parser.add_argument("case_id")
    parser.add_argument("--result-evidence", help="Verified recovery evidence, required for approval")
    parser.add_argument("--summary")
    parser.add_argument("--root-cause")
    parser.add_argument("--resolution")
    parser.add_argument("--keywords", help="Comma separated replacement keywords")
    parser.add_argument("--reject", metavar="REASON")
    args = parser.parse_args()

    context = MySQLContext()
    try:
        record = context.case_by_id(args.case_id)
        if record is None:
            raise SystemExit(f"case not found: {args.case_id}")
        if record["status"] == "rejected":
            raise SystemExit("case is already rejected")

        if args.reject:
            row = context.reject_case(args.case_id, args.reject)
            print(json.dumps({"result": "rejected", **row}, ensure_ascii=False, indent=2))
            return

        if record["status"] == "approved" and not any(
            [args.result_evidence, args.summary, args.root_cause, args.resolution, args.keywords]
        ):
            print(json.dumps({"result": "already_approved", **record}, ensure_ascii=False, indent=2))
            return

        keywords = record["keywords"]
        if args.keywords:
            keywords = [item.strip() for item in args.keywords.split(",") if item.strip()]
        candidate = CaseCandidate(
            summary=args.summary or record["summary"],
            root_cause=args.root_cause or record["root_cause"],
            resolution=args.resolution or record["resolution"],
            result_evidence=args.result_evidence or record["result_evidence"],
            keywords=keywords,
        )
        reason = screen_candidate(candidate, require_result_evidence=True)
        if reason:
            raise SystemExit(f"approval refused: {reason}")

        row = context.approve_case(
            args.case_id,
            result_evidence=candidate.result_evidence or "",
            summary=args.summary,
            root_cause=args.root_cause,
            resolution=args.resolution,
            keywords=keywords if args.keywords else None,
        )
        print(json.dumps({"result": "approved", **row}, ensure_ascii=False, indent=2))
    finally:
        context.close()


if __name__ == "__main__":
    main()
