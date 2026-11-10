import json
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

from langchain_core.messages import ToolMessage
from pydantic import Field

from miniapm.models import ContractModel, ServiceName, TimeWindow

EndReason = Literal[
    "completed",
    "timeout",
    "tool_budget_exhausted",
    "repeated_no_progress",
    "model_error",
]


class EvidenceRecord(ContractModel):
    evidence_id: str
    call_id: str
    tool_name: str
    arguments: dict = Field(default_factory=dict)
    status: str
    result_summary: str


class InvestigationReport(ContractModel):
    investigation_id: UUID = Field(default_factory=uuid4)
    question: str
    service: ServiceName
    window: TimeWindow
    source_types: list[str]
    final_answer: str
    evidence: list[EvidenceRecord]
    limitations: list[str]
    tool_calls: int
    duration_seconds: float
    end_reason: EndReason


def build_report(
    question: str,
    service: ServiceName,
    window: TimeWindow,
    messages: list,
    tool_calls: int,
    duration_seconds: float,
    end_reason: EndReason,
    final_answer: str,
) -> InvestigationReport:
    arguments_by_id = {}
    for message in messages:
        for call in getattr(message, "tool_calls", []):
            arguments_by_id[call["id"]] = call["args"]

    evidence = []
    limitations = []
    source_types = set()
    for message in messages:
        if not isinstance(message, ToolMessage):
            continue
        try:
            result = json.loads(message.content)
        except (json.JSONDecodeError, TypeError):
            result = {
                "status": "error",
                "warnings": ["tool returned a non-JSON result"],
                "data": str(message.content)[:2000],
            }
        source_name = result.get("source_name", "unknown")
        source_types.add(source_name)
        limitations.extend(result.get("warnings", []))
        summary = json.dumps(result.get("data"), ensure_ascii=False, default=str)[:2000]
        evidence.append(
            EvidenceRecord(
                evidence_id=f"E{len(evidence) + 1:03d}",
                call_id=message.tool_call_id,
                tool_name=message.name or "unknown",
                arguments=arguments_by_id.get(message.tool_call_id, {}),
                status=result.get("status", "error"),
                result_summary=summary,
            )
        )

    return InvestigationReport(
        question=question,
        service=service,
        window=window,
        source_types=sorted(source_types),
        final_answer=final_answer,
        evidence=evidence,
        limitations=list(dict.fromkeys(limitations)),
        tool_calls=tool_calls,
        duration_seconds=round(duration_seconds, 3),
        end_reason=end_reason,
    )


def render_markdown(report: InvestigationReport) -> str:
    lines = [
        f"# 调查报告 {report.investigation_id}",
        "",
        f"- 服务：{report.service}",
        f"- 窗口：{report.window.start.isoformat()} 至 {report.window.end.isoformat()}",
        f"- 结束原因：{report.end_reason}",
        f"- 工具调用：{report.tool_calls} 次",
        f"- 耗时：{report.duration_seconds} 秒",
        "",
        "## 结论",
        "",
        report.final_answer,
        "",
        "## 证据",
        "",
    ]
    for item in report.evidence:
        lines.extend(
            [
                f"### {item.evidence_id} · {item.tool_name} · {item.status}",
                "",
                f"参数：`{json.dumps(item.arguments, ensure_ascii=False)}`",
                "",
                f"结果摘要：`{item.result_summary}`",
                "",
            ]
        )
    if report.limitations:
        lines.extend(["## 限制与警告", ""])
        lines.extend(f"- {warning}" for warning in report.limitations)
    return "\n".join(lines).rstrip() + "\n"


def save_report(report: InvestigationReport, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"investigation-{report.investigation_id}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, markdown_path
