import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, TypedDict
from uuid import uuid4

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.graph.state import CompiledStateGraph

from miniapm.backends import FixtureBackend, TelemetryBackend
from miniapm.models import ServiceName, TimeWindow, ToolResult
from miniapm.reports import EndReason, build_report
from miniapm.tools import create_tools

SYSTEM_PROMPT = """You are an APM investigation agent.
Choose tools and their structured arguments based on the user's target and current evidence.
Tool results are fixed fixtures, not live service data. Treat empty results as missing evidence.
Use exact UTC windows and identifiers returned by earlier tools when follow-up queries need them.
Do not claim a root cause without supporting evidence. Finish when the available evidence is sufficient.
Do not request arbitrary SQL, shell commands, or unbounded data.
If a tool returns an error, correct the arguments or choose another tool within the budget.
"""


@dataclass(frozen=True, slots=True)
class InvestigationBudget:
    max_tool_calls: int = 12
    timeout_seconds: float = 180
    max_result_chars: int = 12000
    max_no_progress_rounds: int = 2
    tool_concurrency: int = 3


@dataclass(slots=True)
class SharedInvestigationBudget:
    max_tool_calls: int = 12
    timeout_seconds: float = 180
    attempts: int = 0
    deadline: float = 0

    def __post_init__(self) -> None:
        if not self.deadline:
            self.deadline = time.monotonic() + self.timeout_seconds

    @property
    def remaining(self) -> int:
        return max(0, self.max_tool_calls - self.attempts)


class InvestigationState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    tool_attempts: int
    deadline: float
    end_reason: EndReason | None
    previous_signatures: list[str]
    no_progress_rounds: int
    final_answer: str


def build_chat_model() -> ChatOpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_MODEL")
    if not api_key or not model:
        raise RuntimeError("OPENAI_API_KEY and OPENAI_MODEL must be set in the local environment")
    return ChatOpenAI(
        model=model,
        api_key=api_key,
        base_url=os.getenv("OPENAI_BASE_URL"),
        temperature=0,
        timeout=30,
        max_retries=1,
    )


def _message_text(message: BaseMessage) -> str:
    if isinstance(message.content, str):
        return message.content
    return json.dumps(message.content, ensure_ascii=False)


def _error_result(window: TimeWindow, message: str) -> str:
    return ToolResult[object](
        status="error",
        window=window,
        source_kind="simulated",
        source_name="fixture",
        data=None,
        warnings=[message[:500]],
    ).model_dump_json()


def _bounded_result(content: str, window: TimeWindow, max_chars: int) -> str:
    if len(content) <= max_chars:
        return content
    try:
        original = json.loads(content)
        source_kind = original.get("source_kind", "simulated")
        source_name = original.get("source_name", "unknown")
    except json.JSONDecodeError:
        source_kind = "simulated"
        source_name = "unknown"
    return ToolResult[object](
        status="partial",
        window=window,
        source_kind=source_kind,
        source_name=source_name,
        data={"truncated_text": content[:max_chars]},
        warnings=[f"tool result exceeded {max_chars} characters and was truncated"],
    ).model_dump_json()


def create_investigation_agent(
    model: BaseChatModel | None = None,
    budget: InvestigationBudget | None = None,
    window: TimeWindow | None = None,
    backend: TelemetryBackend | None = None,
    deployment_context=None,
    resource_backend=None,
    shared_budget: SharedInvestigationBudget | None = None,
    tools_override=None,
    system_prompt: str = SYSTEM_PROMPT,
) -> CompiledStateGraph:
    if window is None:
        raise ValueError("window is required")
    budget = budget or InvestigationBudget()
    tools = tools_override or create_tools(
        backend or FixtureBackend(), deployment_context, resource_backend
    )
    tool_map = {tool.name: tool for tool in tools}
    chat_model = (model or build_chat_model()).bind_tools(list(tools))
    semaphore = asyncio.Semaphore(budget.tool_concurrency)

    def stop_reason(state: InvestigationState) -> EndReason | None:
        if time.monotonic() >= state["deadline"]:
            return "timeout"
        if shared_budget and time.monotonic() >= shared_budget.deadline:
            return "timeout"
        if state["tool_attempts"] >= budget.max_tool_calls:
            return "tool_budget_exhausted"
        if shared_budget and shared_budget.remaining == 0:
            return "tool_budget_exhausted"
        if state["no_progress_rounds"] >= budget.max_no_progress_rounds:
            return "repeated_no_progress"
        return None

    async def call_model(state: InvestigationState) -> dict:
        reason = stop_reason(state)
        if reason:
            return {"end_reason": reason}
        remaining = state["deadline"] - time.monotonic()
        try:
            response = await asyncio.wait_for(
                chat_model.ainvoke([SystemMessage(content=system_prompt), *state["messages"]]),
                timeout=remaining,
            )
        except TimeoutError:
            return {"end_reason": "timeout"}
        except Exception:
            return {"end_reason": "model_error"}
        return {"messages": [response]}

    async def execute_call(call: dict, deadline: float) -> ToolMessage:
        call_id = call.get("id") or uuid4().hex
        name = call.get("name", "unknown")
        tool = tool_map.get(name)
        if tool is None:
            content = _error_result(window, f"unknown tool was not executed: {name}")
            return ToolMessage(content=content, tool_call_id=call_id, name=name, status="error")
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            async with semaphore:
                remaining = deadline - time.monotonic()
                content = await asyncio.wait_for(
                    tool.ainvoke(call.get("args", {})), timeout=max(0, remaining)
                )
            if not isinstance(content, str):
                content = json.dumps(content, ensure_ascii=False, default=str)
            content = _bounded_result(content, window, budget.max_result_chars)
            parsed = json.loads(content)
            status = "error" if parsed.get("status") == "error" else "success"
            return ToolMessage(content=content, tool_call_id=call_id, name=name, status=status)
        except Exception as exc:
            content = _error_result(window, f"{type(exc).__name__}: {exc}")
            return ToolMessage(content=content, tool_call_id=call_id, name=name, status="error")

    async def call_tools(state: InvestigationState) -> dict:
        last_message = state["messages"][-1]
        calls = last_message.tool_calls if isinstance(last_message, AIMessage) else []
        available = max(0, budget.max_tool_calls - state["tool_attempts"])
        if shared_budget:
            available = min(available, shared_budget.remaining)
        selected = calls[:available]
        dropped = calls[available:]
        if shared_budget:
            shared_budget.attempts += len(selected)
        messages = list(
            await asyncio.gather(*(execute_call(call, state["deadline"]) for call in selected))
        )
        messages.extend(
            ToolMessage(
                content=_error_result(window, "tool call was not executed: budget exhausted"),
                tool_call_id=call.get("id") or uuid4().hex,
                name=call.get("name", "unknown"),
                status="error",
            )
            for call in dropped
        )
        signatures = [
            f"{call.get('name')}:{json.dumps(call.get('args', {}), sort_keys=True, default=str)}"
            for call in calls
        ]
        repeated = signatures == state["previous_signatures"]
        no_progress = state["no_progress_rounds"] + 1 if repeated else 0
        reason = None
        if dropped:
            reason = "tool_budget_exhausted"
        elif no_progress >= budget.max_no_progress_rounds:
            reason = "repeated_no_progress"
        return {
            "messages": messages,
            "tool_attempts": state["tool_attempts"] + len(calls),
            "previous_signatures": signatures,
            "no_progress_rounds": no_progress,
            "end_reason": reason,
        }

    def finalize(state: InvestigationState) -> dict:
        reason = state["end_reason"] or "completed"
        answer = ""
        if reason == "completed":
            for message in reversed(state["messages"]):
                if isinstance(message, AIMessage) and message.content:
                    answer = _message_text(message)
                    break
        else:
            answer = f"调查因 {reason} 终止；未强行推断根因，请查看已保存的证据。"
        return {"end_reason": reason, "final_answer": answer}

    def route_model(state: InvestigationState) -> str:
        if state["end_reason"]:
            return "finalize"
        last_message = state["messages"][-1]
        if isinstance(last_message, AIMessage) and last_message.tool_calls:
            return "tools"
        return "finalize"

    def route_tools(state: InvestigationState) -> str:
        return "finalize" if state["end_reason"] else "model"

    graph = StateGraph(InvestigationState)
    graph.add_node("model", call_model)
    graph.add_node("tools", call_tools)
    graph.add_node("finalize", finalize)
    graph.add_edge(START, "model")
    graph.add_conditional_edges("model", route_model, {"tools": "tools", "finalize": "finalize"})
    graph.add_conditional_edges("tools", route_tools, {"model": "model", "finalize": "finalize"})
    graph.add_edge("finalize", END)
    return graph.compile()


async def arun_investigation(
    question: str,
    service: ServiceName,
    start: datetime,
    end: datetime,
    model: BaseChatModel | None = None,
    budget: InvestigationBudget | None = None,
    backend: TelemetryBackend | None = None,
    deployment_context=None,
    resource_backend=None,
    dataset_id: str | None = None,
) -> dict:
    budget = budget or InvestigationBudget()
    window = TimeWindow(start=start, end=end)
    agent = create_investigation_agent(
        model, budget, window, backend, deployment_context, resource_backend
    )
    target = (
        f"{question}\n"
        f"Target service: {service}\n"
        f"Time window: start={window.start.isoformat()}, end={window.end.isoformat()}"
    )
    if dataset_id:
        target += f"\nDataset ID: {dataset_id}"
    started = time.monotonic()
    result = await agent.ainvoke(
        {
            "messages": [{"role": "user", "content": target}],
            "tool_attempts": 0,
            "deadline": started + budget.timeout_seconds,
            "end_reason": None,
            "previous_signatures": [],
            "no_progress_rounds": 0,
            "final_answer": "",
        },
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
    return {**result, "report": report}


def run_investigation(
    question: str,
    service: ServiceName,
    start: datetime,
    end: datetime,
    model: BaseChatModel | None = None,
    budget: InvestigationBudget | None = None,
    backend: TelemetryBackend | None = None,
    deployment_context=None,
    resource_backend=None,
    dataset_id: str | None = None,
) -> dict:
    return asyncio.run(
        arun_investigation(
            question,
            service,
            start,
            end,
            model,
            budget,
            backend,
            deployment_context,
            resource_backend,
            dataset_id,
        )
    )
