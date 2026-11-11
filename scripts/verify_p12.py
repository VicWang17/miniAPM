"""P12 verification: deterministic screening, storage, recall, rejection and feedback.

Run without a model (unit + store checks):
    uv run python scripts/verify_p12.py

Run only the recall@k section over already approved cases:
    uv run python scripts/verify_p12.py --recall-only
"""

import argparse
import json
from datetime import UTC, datetime
from uuid import uuid4

from miniapm.cases import (
    CaseCandidate,
    create_knowledge_tools,
    screen_candidate,
    screen_report,
)
from miniapm.models import TimeWindow
from miniapm.mysql_context import MySQLContext

DATASET = "018f2f70-0000-7000-8000-000000000606"
REJECT_INVESTIGATION = "62072a31-935b-43b6-94d1-2b7c8f7b0f2a"
WINDOW = TimeWindow(
    start=datetime(2026, 11, 12, 0, 0, tzinfo=UTC),
    end=datetime(2026, 11, 12, 0, 15, tzinfo=UTC),
)

CHECKS = {"passed": 0, "failed": 0}


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        CHECKS["passed"] += 1
        print(f"  PASS {name}")
    else:
        CHECKS["failed"] += 1
        print(f"  FAIL {name} {detail}")


def _report(**overrides) -> dict:
    base = {
        "end_reason": "completed",
        "final_answer": "inventory database connection pool exhaustion caused checkout timeouts",
        "evidence": [{"evidence_id": "E001", "status": "ok", "tool_name": "query_logs"}],
    }
    base.update(overrides)
    return base


def section_screening() -> None:
    print("\n=== report screening ===")
    check("eligible report", screen_report(_report()) is None)
    check("timeout rejected", "timeout" in (screen_report(_report(end_reason="timeout")) or ""))
    check("no evidence rejected", "no evidence" in (screen_report(_report(evidence=[])) or ""))
    check(
        "partial-only evidence rejected",
        "no ok evidence" in (screen_report(_report(evidence=[{"status": "partial"}])) or ""),
    )
    check(
        "forced termination rejected",
        "forced-termination" in (screen_report(_report(final_answer="调查因 timeout 终止；未强行推断根因，请查看已保存的证据。")) or ""),
    )
    check("short answer rejected", "shorter" in (screen_report(_report(final_answer="done")) or ""))

    print("\n=== candidate screening ===")
    good = CaseCandidate(
        summary="inventory connection pool exhausted under load",
        root_cause="inventory DB pool of 20 exhausted by slow queries",
        resolution="raise pool size and add query timeout",
        result_evidence=None,
        keywords=["inventory", "connection pool", "timeout"],
    )
    check("valid candidate passes", screen_candidate(good, require_result_evidence=False) is None)
    check(
        "candidate without result evidence refused at approval",
        "result_evidence" in (screen_candidate(good, require_result_evidence=True) or ""),
    )
    check(
        "placeholder root cause rejected",
        "root_cause" in (screen_candidate(good.model_copy(update={"root_cause": "无法判断"}), False) or ""),
    )
    check(
        "too many keywords rejected",
        "keywords" in (screen_candidate(good.model_copy(update={"keywords": [f"k{i}" for i in range(9)]}), False) or ""),
    )


def section_store(context: MySQLContext) -> dict:
    print("\n=== pending store and dedupe ===")
    investigation_id = REJECT_INVESTIGATION
    existing = context.case_by_investigation(investigation_id)
    if existing is not None:
        context.connection.cursor().execute("DELETE FROM cases WHERE investigation_id=%s", (investigation_id,))
        context.connection.commit()
    row = context.upsert_case_candidate(
        case_id=str(uuid4()),
        investigation_id=investigation_id,
        dataset_id=DATASET,
        service="order",
        alert_fingerprint=None,
        summary="degenerate pending case used by verification",
        root_cause="verification placeholder",
        resolution="verification placeholder",
        result_evidence=None,
        keywords=["verification"],
    )
    check("pending row inserted", row["status"] == "pending")
    again = context.upsert_case_candidate(
        case_id=str(uuid4()),
        investigation_id=investigation_id,
        dataset_id=DATASET,
        service="order",
        alert_fingerprint=None,
        summary="degenerate pending case refreshed",
        root_cause="verification placeholder",
        resolution="verification placeholder",
        result_evidence=None,
        keywords=["verification"],
    )
    check("same investigation keeps one case", again["id"] == row["id"], f"{row['id']} != {again['id']}")
    print("\n=== rejection path ===")
    rejected = context.reject_case(row["id"], "no verified recovery; degenerate investigation")
    check("rejected status stored", rejected["status"] == "rejected")
    check("reject reason stored", "degenerate" in (rejected["reject_reason"] or ""))
    try:
        context.upsert_case_candidate(
            case_id=str(uuid4()),
            investigation_id=investigation_id,
            dataset_id=DATASET,
            service="order",
            alert_fingerprint=None,
            summary="should not overwrite a decided case",
            root_cause="x",
            resolution="y",
            result_evidence=None,
            keywords=["verification"],
        )
        check("decided case is protected", False, "upsert unexpectedly succeeded")
    except ValueError:
        check("decided case is protected", True)
    return row


def section_recall(context: MySQLContext) -> float | None:
    print("\n=== recall tool ===")
    tool = create_knowledge_tools(context, DATASET, WINDOW)
    check("tool name", tool.name == "query_historical_cases")

    empty = json.loads(tool.invoke({"service": "order", "keywords": "payment,timeout,zzz"}))
    check("unmatched query returns empty", empty["status"] == "empty", empty["status"])

    approved = _approved_cases(context)
    if not approved:
        print("  SKIP no approved cases yet; run extraction and approval first")
        return None

    hits_before = {row["id"]: row["hits"] for row in approved}
    tools: dict[str, object] = {}
    total = 0
    found = 0
    for row in approved:
        dataset_id = row["dataset_id"]
        dataset_tool = tools.setdefault(
            dataset_id, create_knowledge_tools(context, dataset_id, WINDOW)
        )
        total += 1
        keywords = ",".join(row["keywords"][:2])
        result = json.loads(dataset_tool.invoke({"service": row["service"], "keywords": keywords}))
        cases = (result["data"] or {}).get("cases", [])
        ids = [item["case_id"] for item in cases]
        check(f"recall self-hit {row['id'][:8]}", row["id"] in ids, f"ids={ids}")
        check(f"bounded result {row['id'][:8]}", len(ids) <= 3)
        check(
            f"dataset isolation {row['id'][:8]}",
            all(context.case_by_id(case_id)["dataset_id"] == dataset_id for case_id in ids),
            f"ids={ids}",
        )
        if row["id"] in ids:
            found += 1

    hits_after = {row["id"]: context.case_by_id(row["id"])["hits"] for row in approved}
    check(
        "recall increments hits",
        all(hits_after[row["id"]] > hits_before[row["id"]] for row in approved),
        f"before={hits_before} after={hits_after}",
    )

    print("\n=== feedback ===")
    target = approved[0]["id"]
    helped_before = context.case_by_id(target)["helped"]
    context.record_case_outcome([target], helped=True)
    helped_after = context.case_by_id(target)["helped"]
    check("helped counter increments", helped_after == helped_before + 1, f"{helped_before} -> {helped_after}")

    recall_at_k = found / total if total else None
    print(f"\n  recall@3 = {found}/{total} = {recall_at_k} (n={total}, dataset-scoped)")
    return recall_at_k


def _approved_cases(context: MySQLContext) -> list[dict]:
    with context.connection.cursor() as cursor:
        cursor.execute(
            f"SELECT {context.CASE_COLUMNS} FROM cases WHERE status='approved'"
        )
        rows = cursor.fetchall()
    return [context._case_row(row) for row in rows]


def section_recall_only(context: MySQLContext) -> None:
    expected = _approved_cases(context)
    print(f"\n=== recall@3 over {len(expected)} approved cases (dataset-scoped) ===")
    if not expected:
        print("  SKIP no approved cases")
        return
    tools: dict[str, object] = {}
    total = 0
    found = 0
    for row in expected:
        dataset_id = row["dataset_id"]
        tool = tools.setdefault(dataset_id, create_knowledge_tools(context, dataset_id, WINDOW))
        total += 1
        keywords = ",".join(row["keywords"][:2])
        result = json.loads(tool.invoke({"service": row["service"], "keywords": keywords}))
        ids = [item["case_id"] for item in (result["data"] or {}).get("cases", [])]
        if row["id"] in ids:
            found += 1
        check(f"recall@3 {row['id'][:8]} via '{keywords}'", row["id"] in ids, f"ids={ids}")
    print(f"\n  recall@3 = {found}/{total} = {found / total} (n={total})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify P12 knowledge base behaviour")
    parser.add_argument("--recall-only", action="store_true")
    args = parser.parse_args()

    context = MySQLContext()
    try:
        if args.recall_only:
            section_recall_only(context)
        else:
            section_screening()
            section_store(context)
            section_recall(context)
    finally:
        context.close()

    print(f"\nchecks: {CHECKS['passed']} passed, {CHECKS['failed']} failed")
    if CHECKS["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
