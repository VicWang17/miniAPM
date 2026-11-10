import os
from datetime import datetime

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_openai import ChatOpenAI
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent

from miniapm.models import ServiceName
from miniapm.tools import query_logs, query_metrics, query_traces

SYSTEM_PROMPT = """You are an APM investigation agent.
Choose tools and their structured arguments based on the user's target and current evidence.
Tool results are fixed fixtures, not live service data. Treat empty results as missing evidence.
Use exact UTC windows and identifiers returned by earlier tools when follow-up queries need them.
Do not claim a root cause without supporting evidence. Finish when the available evidence is sufficient.
Do not request arbitrary SQL, shell commands, or unbounded data.
"""


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


def create_investigation_agent(
    model: BaseChatModel | None = None,
) -> CompiledStateGraph:
    return create_react_agent(
        model=model or build_chat_model(),
        tools=[query_metrics, query_traces, query_logs],
        prompt=SYSTEM_PROMPT,
    )


def run_investigation(
    question: str,
    service: ServiceName,
    start: datetime,
    end: datetime,
    model: BaseChatModel | None = None,
) -> dict:
    agent = create_investigation_agent(model)
    target = (
        f"{question}\n"
        f"Target service: {service}\n"
        f"Time window: start={start.isoformat()}, end={end.isoformat()}"
    )
    return agent.invoke(
        {"messages": [{"role": "user", "content": target}]},
        config={"recursion_limit": 25},
    )
