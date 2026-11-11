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


class SubagentCall(ContractModel):
    call_id: str
    tool_name: str
    arguments: dict = Field(default_factory=dict)
    status: str
    result_summary: str


class SubagentRun(ContractModel):
    agent_name: str
    status: str
    end_reason: str
    summary: str
    calls: list[SubagentCall]


class InvestigationReport(ContractModel):
    investigation_id: UUID = Field(default_factory=uuid4)
    question: str
    service: ServiceName
    window: TimeWindow
    source_types: list[str]
    final_answer: str
    evidence: list[EvidenceRecord]
    subagent_runs: list[SubagentRun] = Field(default_factory=list)
    limitations: list[str]
    tool_calls: int
    supervisor_tool_calls: int = 0
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
    shared_tool_attempts: int | None = None,
    subagent_runs: list[SubagentRun] | None = None,
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
        source_name = result.get("source_name", result.get("agent", "unknown"))
        source_types.add(source_name)
        limitations.extend(result.get("warnings", []))
        if result.get("agent") and result.get("end_reason") != "completed":
            limitations.append(f"{result['agent']} ended with {result['end_reason']}")
        raw_data = result.get("data")
        if raw_data is None and "summary" in result:
            raw_data = {"summary": result["summary"], "calls": result.get("calls", [])}
        summary = json.dumps(raw_data, ensure_ascii=False, default=str)[:2000]
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
        tool_calls=shared_tool_attempts if shared_tool_attempts is not None else tool_calls,
        supervisor_tool_calls=tool_calls,
        subagent_runs=subagent_runs or [],
        duration_seconds=round(duration_seconds, 3),
        end_reason=end_reason,
    )


def validate_report(report: InvestigationReport) -> None:
    if report.end_reason == "completed" and not report.final_answer.strip():
        raise ValueError("completed report requires a final answer")
    if report.end_reason != "completed" and report.end_reason not in report.final_answer:
        raise ValueError("forced termination must include its end reason")
    evidence_ids = [item.evidence_id for item in report.evidence]
    if len(evidence_ids) != len(set(evidence_ids)):
        raise ValueError("evidence_id values must be unique")
    if any(not item.call_id for item in report.evidence):
        raise ValueError("evidence call_id is required")
    if report.supervisor_tool_calls > report.tool_calls:
        raise ValueError("global tool count is smaller than supervisor count")
    if report.tool_calls < len(report.evidence):
        raise ValueError("global tool count is smaller than evidence count")
    if any(item.status in {"error", "partial"} for item in report.evidence) and not report.limitations:
        raise ValueError("error or partial evidence requires limitations")
    for run in report.subagent_runs:
        if not run.summary.strip():
            raise ValueError("subagent summary is required")
        if any(not call.call_id for call in run.calls):
            raise ValueError("subagent call_id is required")


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
    for run in report.subagent_runs:
        lines.extend(
            [
                f"## 子 Agent：{run.agent_name}",
                "",
                f"状态：{run.status}；结束原因：{run.end_reason}",
                "",
                run.summary,
                "",
            ]
        )
        for call in run.calls:
            lines.append(
                f"- {call.call_id} · {call.tool_name} · {call.status} · `{call.result_summary}`"
            )
        lines.append("")
    if report.limitations:
        lines.extend(["## 限制与警告", ""])
        lines.extend(f"- {warning}" for warning in report.limitations)
    return "\n".join(lines).rstrip() + "\n"


def save_report(report: InvestigationReport, output_dir: Path) -> tuple[Path, Path]:
    validate_report(report)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"investigation-{report.investigation_id}"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, markdown_path
