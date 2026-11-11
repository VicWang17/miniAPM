"""P12 historical case knowledge base: screening, ranking and the recall tool.

This module is deliberately model-free: candidate extraction lives in
``case_extract.py`` and approval is a deterministic CLI step. Recall only uses
SQL filtering plus deterministic keyword scoring, so the same input always
returns the same ranked list.
"""

import json
from typing import Literal

from langchain_core.tools import tool
from pydantic import BaseModel, ConfigDict, Field

from miniapm.models import ContractModel, ServiceName, TimeWindow, ToolResult

FORCED_TERMINATION_MARKERS = ("未强行推断根因", "调查因")
PLACEHOLDER_VALUES = {"unknown", "n/a", "none", "null", "-", "无", "无法判断", "不确定"}
MIN_ANSWER_CHARS = 30
MAX_KEYWORDS = 8


class CaseCandidate(ContractModel):
    summary: str = Field(min_length=5, max_length=500)
    root_cause: str = Field(min_length=3, max_length=2000)
    resolution: str = Field(min_length=3, max_length=2000)
    result_evidence: str | None = Field(default=None, max_length=2000)
    keywords: list[str] = Field(min_length=1, max_length=MAX_KEYWORDS)


class CaseExtraction(ContractModel):
    """Model output for one investigation; ``has_conclusion`` gates extraction."""

    has_conclusion: bool
    summary: str = ""
    root_cause: str | None = None
    resolution: str | None = None
    result_evidence: str | None = None
    keywords: list[str] = Field(default_factory=list)
    uncertain: list[str] = Field(default_factory=list)


def screen_report(report: dict) -> str | None:
    """Return None when the report may yield a case, else the rejection reason."""
    if report.get("end_reason") != "completed":
        return f"end_reason={report.get('end_reason')} is not completed"
    answer = (report.get("final_answer") or "").strip()
    if len(answer) < MIN_ANSWER_CHARS:
        return f"final_answer shorter than {MIN_ANSWER_CHARS} characters"
    if any(marker in answer for marker in FORCED_TERMINATION_MARKERS):
        return "final_answer is a forced-termination template"
    evidence = report.get("evidence") or []
    if not evidence:
        return "no evidence records"
    if not any(item.get("status") == "ok" for item in evidence):
        return "no ok evidence; every query was partial or failed"
    return None


def _is_placeholder(value: str | None) -> bool:
    if value is None:
        return True
    text = value.strip().strip("。.!！")
    return not text or text.lower() in PLACEHOLDER_VALUES


def screen_candidate(candidate: CaseCandidate, require_result_evidence: bool) -> str | None:
    """Return None when the candidate may be stored/approved, else the reason."""
    if _is_placeholder(candidate.summary):
        return "summary is empty or a placeholder"
    if _is_placeholder(candidate.root_cause):
        return "root_cause is empty or a placeholder"
    if _is_placeholder(candidate.resolution):
        return "resolution is empty or a placeholder"
    if require_result_evidence and _is_placeholder(candidate.result_evidence):
        return (
            "result_evidence is required: quote recovery numbers or the discriminating "
            "evidence that verifies the recorded conclusion; '已解决' alone is not enough"
        )
    cleaned = [keyword.strip() for keyword in candidate.keywords if keyword.strip()]
    if not cleaned:
        return "at least one keyword is required"
    if len(cleaned) > MAX_KEYWORDS:
        return f"at most {MAX_KEYWORDS} keywords are allowed"
    return None


def build_candidate(extraction: CaseExtraction) -> tuple[CaseCandidate | None, str | None]:
    """Convert screened model output into a validated candidate."""
    if not extraction.has_conclusion:
        return None, "model reported no supported conclusion"
    try:
        candidate = CaseCandidate(
            summary=extraction.summary,
            root_cause=extraction.root_cause or "",
            resolution=extraction.resolution or "",
            result_evidence=extraction.result_evidence,
            keywords=[keyword for keyword in extraction.keywords if keyword.strip()],
        )
    except ValueError as exc:
        return None, f"candidate validation failed: {exc}"
    reason = screen_candidate(candidate, require_result_evidence=False)
    if reason:
        return None, reason
    return candidate, None


def split_keywords(raw: str) -> list[str]:
    parts = [part.strip() for part in raw.replace("，", ",").split(",")]
    parts = [part for part in parts if part]
    if len(parts) == 1:
        parts = [part for part in parts[0].split() if part]
    return parts[:5]


def rank_cases(rows: list[dict], keywords: list[str], limit: int) -> list[dict]:
    """Deterministic keyword-overlap ranking over an already filtered set."""
    lowered = [keyword.lower() for keyword in keywords if keyword.strip()]
    scored = []
    for row in rows:
        text = " ".join(
            [
                row.get("summary") or "",
                row.get("root_cause") or "",
                row.get("resolution") or "",
                row.get("result_evidence") or "",
                " ".join(row.get("keywords") or []),
            ]
        ).lower()
        score = sum(1 for keyword in lowered if keyword in text)
        if lowered and score == 0:
            continue
        scored.append((score, int(row.get("hits") or 0), row))
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [row for _, _, row in scored[:limit]]


def bounded_case(row: dict) -> dict:
    return {
        "case_id": row["id"],
        "investigation_id": row["investigation_id"],
        "service": row["service"],
        "summary": (row.get("summary") or "")[:240],
        "root_cause": (row.get("root_cause") or "")[:300],
        "resolution": (row.get("resolution") or "")[:300],
        "result_evidence": (row.get("result_evidence") or "")[:300],
        "keywords": (row.get("keywords") or [])[:MAX_KEYWORDS],
        "hits": int(row.get("hits") or 0),
    }


class CaseQueryInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: Literal["order", "inventory"]
    keywords: str = Field(
        min_length=1,
        max_length=200,
        description="Comma or space separated search keywords, e.g. 'backlog,rabbitmq'",
    )
    error_code: str | None = Field(default=None, max_length=100)
    limit: int = Field(default=3, ge=1, le=3)


def create_knowledge_tools(context, dataset_id: str, window: TimeWindow):
    """Build the deterministic historical-case recall tool for one investigation."""

    @tool(args_schema=CaseQueryInput)
    def query_historical_cases(
        service: str,
        keywords: str,
        error_code: str | None = None,
        limit: int = 3,
    ) -> str:
        """Recall approved historical cases with the same service and similar symptoms.

        Returns bounded summaries plus what worked last time. Historical cases are
        references only: they never replace current evidence or prove a root cause.
        """
        parsed = split_keywords(keywords)
        if not parsed:
            return ToolResult[object](
                status="error",
                window=window,
                source_kind="simulated",
                source_name="mysql_cases",
                data=None,
                warnings=["no usable keyword was provided"],
            ).model_dump_json()
        rows = context.search_cases(
            dataset_id=dataset_id,
            service=service,
            keywords=parsed,
            error_code=error_code,
            limit=limit,
        )
        ranked = rank_cases(rows, parsed, limit)
        if not ranked:
            return ToolResult[object](
                status="empty",
                window=window,
                source_kind="simulated",
                source_name="mysql_cases",
                data={"cases": [], "keywords": parsed, "limit": limit},
                warnings=["no approved historical case matched the keywords"],
            ).model_dump_json()
        context.record_case_outcome([row["id"] for row in ranked], helped=False)
        return ToolResult[object](
            status="ok",
            window=window,
            source_kind="simulated",
            source_name="mysql_cases",
            data={
                "cases": [bounded_case(row) for row in ranked],
                "keywords": parsed,
                "limit": limit,
            },
            warnings=["historical cases are references, not proof of the current root cause"],
        ).model_dump_json()

    return query_historical_cases
