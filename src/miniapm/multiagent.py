import json
import time
from datetime import datetime

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from langgraph.graph.state import CompiledStateGraph
from pydantic import BaseModel, ConfigDict

from miniapm.analysis import analyze_resource_changes
from miniapm.backends import TelemetryBackend
from miniapm.investigation import (
    InvestigationBudget,
    SharedInvestigationBudget,
    create_investigation_agent,
)
from miniapm.models import ServiceName, TimeWindow
from miniapm.reports import build_report
from miniapm.tools import ResourceQueryInput

SUPERVISOR_PROMPT = """You are the lead APM investigator.
Maintain the investigation goal and competing explanations.
Call telemetry_investigation for metrics, logs and traces, or resource_change_investigation for deployments and resource evidence only when needed.
Do not call both by default and do not follow a fixed sequence.
Use returned evidence to decide follow-up queries or finish.
Subagents return bounded summaries, not ground truth. State missing evidence and contradictions explicitly.
Produce the final answer yourself with evidence-based conclusions and limitations.
"""


class SubagentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str


def _initial_state(question: str, deadline: float) -> dict:
    return {
        "messages": [{"role": "user", "content": question}],
        "tool_attempts": 0,
        "deadline": deadline,
        "end_reason": None,
        "previous_signatures": [],
        "no_progress_rounds": 0,
        "final_answer": "",
    }


def _subagent_summary(agent_name: str, result: dict) -> str:
    arguments = {}
    for message in result["messages"]:
        for call in getattr(message, "tool_calls", []):
            arguments[call["id"]] = call["args"]
    calls = []
    for message in result["messages"]:
        if not isinstance(message, ToolMessage):
            continue
        try:
            content = json.loads(message.content)
        except json.JSONDecodeError:
            content = {"status": "error", "data": message.content}
        calls.append(
            {
                "tool": message.name,
                "arguments": arguments.get(message.tool_call_id, {}),
                "status": content.get("status", "error"),
                "summary": json.dumps(content.get("data"), ensure_ascii=False)[:600],
            }
        )
    return json.dumps(
        {
            "agent": agent_name,
            "status": "ok" if result["end_reason"] == "completed" else "partial",
            "end_reason": result["end_reason"],
            "summary": result["final_answer"][:1200],
            "calls": calls[-4:],
        },
        ensure_ascii=False,
    )


def _as_tool(name: str, agent: CompiledStateGraph, deadline: float):
    @tool(name, args_schema=SubagentInput)
    async def invoke(question: str) -> str:
        """Invoke a bounded domain investigation subagent with a structured question."""
        result = await agent.ainvoke(
            _initial_state(question, deadline), config={"recursion_limit": 100}
        )
        return _subagent_summary(name, result)

    return invoke


def create_multiagent_investigation(
    model: BaseChatModel | None,
    window: TimeWindow,
    backend: TelemetryBackend,
    deployment_context,
    resource_backend=None,
    shared_budget: SharedInvestigationBudget | None = None,
) -> CompiledStateGraph:
    shared_budget = shared_budget or SharedInvestigationBudget()
    deadline = shared_budget.deadline
    telemetry_agent = create_investigation_agent(
        model,
        InvestigationBudget(max_tool_calls=3),
        window,
        backend,
        shared_budget=shared_budget,
    )

    @tool(args_schema=ResourceQueryInput)
    def query_resource_changes(
        dataset_id: str, service: str, start: datetime, end: datetime
    ) -> str:
        """Query deployments and available resource evidence for the target."""
        return analyze_resource_changes(
            deployment_context,
            dataset_id,
            service,
            TimeWindow(start=start, end=end),
            resource_backend,
        )

    resource_agent = create_investigation_agent(
        model,
        InvestigationBudget(max_tool_calls=2),
        window,
        backend,
        shared_budget=shared_budget,
        tools_override=(query_resource_changes,),
    )
    return create_investigation_agent(
        model,
        InvestigationBudget(),
        window,
        backend,
        shared_budget=shared_budget,
        tools_override=(
            _as_tool("telemetry_investigation", telemetry_agent, deadline),
            _as_tool("resource_change_investigation", resource_agent, deadline),
        ),
        system_prompt=SUPERVISOR_PROMPT,
    )


async def arun_multiagent_investigation(
    question: str,
    service: ServiceName,
    start: datetime,
    end: datetime,
    backend: TelemetryBackend,
    deployment_context,
    model: BaseChatModel | None = None,
    resource_backend=None,
    dataset_id: str | None = None,
) -> dict:
    window = TimeWindow(start=start, end=end)
    shared_budget = SharedInvestigationBudget()
    agent = create_multiagent_investigation(
        model,
        window,
        backend,
        deployment_context,
        resource_backend,
        shared_budget,
    )
    target = (
        f"{question}\nTarget service: {service}\n"
        f"Time window: start={window.start.isoformat()}, end={window.end.isoformat()}"
    )
    if dataset_id:
        target += f"\nDataset ID: {dataset_id}"
    started = time.monotonic()
    result = await agent.ainvoke(
        _initial_state(target, min(started + 180, shared_budget.deadline)),
        config={"recursion_limit": 100},
    )
    report = build_report(
        question=question,
        service=service,
        window=window,
        messages=result["messages"],
        tool_calls=result["tool_attempts"],
        duration_seconds=time.monotonic() - started,
        end_reason=result["end_reason"],
        final_answer=result["final_answer"],
    )
    return {**result, "report": report, "shared_tool_attempts": shared_budget.attempts}
