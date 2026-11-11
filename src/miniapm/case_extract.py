"""P12.03 offline case extraction: turn a finished investigation report into a candidate.

The model only summarizes evidence that already exists in ``report_json``. It
never reads evaluation truth, never writes to MySQL and cannot approve itself.
"""

import json
import re

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from miniapm.cases import CaseExtraction
from miniapm.investigation import build_chat_model

MAX_ANSWER_CHARS = 3000
MAX_EVIDENCE = 12
MAX_EVIDENCE_SUMMARY = 400

EXTRACTION_PROMPT = """You summarize a finished APM investigation into one reusable case.
Use only the supplied report fields. Never invent facts, identifiers or outcomes.

Return ONLY a JSON object with these keys:
- has_conclusion: boolean. True only when the evidence supports a concrete root cause,
  or a concrete refutation of the leading hypothesis with its reason.
- summary: one or two sentences describing the problem and the environment.
- root_cause: the supported cause, or the refuted hypothesis and why it was rejected. Else null.
- resolution: the action that FIXED or MITIGATED the issue, or a concrete recommended next action.
  Never leave it empty when has_conclusion is true.
- result_evidence: verified evidence for the recorded conclusion, with concrete numbers, identifiers
  or comparisons quoted from the report. For remediated incidents this is recovery evidence
  (for example "order P95 2.0s -> 0.2s"). For diagnostic-only incidents this is the discriminating
  evidence (for example "v1 80% vs v2 40% errors, failing traces share inventory errors").
  Never write only "已解决" or "已恢复" without numbers. Else null.
- keywords: 1 to 8 short searchable tokens (service, component, error code, symptom, metric).
- uncertain: short list of what the evidence cannot establish.

If the evidence is partial, empty or contradictory, set has_conclusion to false and leave
root_cause and resolution null. Do not output markdown, prose or code fences."""


def build_extraction_payload(report: dict) -> dict:
    evidence = []
    for item in (report.get("evidence") or [])[:MAX_EVIDENCE]:
        evidence.append(
            {
                "evidence_id": item.get("evidence_id"),
                "tool_name": item.get("tool_name"),
                "status": item.get("status"),
                "result_summary": (item.get("result_summary") or "")[:MAX_EVIDENCE_SUMMARY],
            }
        )
    return {
        "question": report.get("question"),
        "service": report.get("service"),
        "window": report.get("window"),
        "end_reason": report.get("end_reason"),
        "final_answer": (report.get("final_answer") or "")[:MAX_ANSWER_CHARS],
        "evidence": evidence,
        "limitations": (report.get("limitations") or [])[:10],
    }


def parse_extraction(text: str) -> CaseExtraction:
    """Parse a model reply into CaseExtraction, tolerating code fences."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("model reply did not contain a JSON object")
    payload = json.loads(cleaned[start : end + 1])
    return CaseExtraction(**payload)


def extract_case(
    report: dict,
    model: BaseChatModel | None = None,
    max_attempts: int = 2,
) -> CaseExtraction:
    """Ask the real model for a structured case; raise after bounded retries."""
    chat_model = model or build_chat_model()
    payload = build_extraction_payload(report)
    messages = [
        SystemMessage(content=EXTRACTION_PROMPT),
        HumanMessage(content=json.dumps(payload, ensure_ascii=False, default=str)),
    ]
    last_error: Exception | None = None
    for _ in range(max(1, max_attempts)):
        response = chat_model.invoke(messages)
        text = response.content if isinstance(response.content, str) else str(response.content)
        try:
            return parse_extraction(text)
        except (ValueError, json.JSONDecodeError) as exc:
            last_error = exc
    raise ValueError(f"model did not return a valid case extraction: {last_error}")
