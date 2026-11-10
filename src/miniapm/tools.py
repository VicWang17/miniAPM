from datetime import datetime
from typing import Literal

from langchain_core.tools import tool
from pydantic import BaseModel, ConfigDict, Field

from miniapm.fixtures import FIXTURE_END, FIXTURE_START, LOGS, METRICS, TRACES
from miniapm.models import TimeWindow, ToolResult

FIXTURE_WARNING = "FIXTURE: fixed sample data, not a live monitoring backend"


class QueryInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: Literal["order", "inventory"]
    start: datetime
    end: datetime


class TraceQueryInput(QueryInput):
    limit: int = Field(default=5, ge=1, le=20)


class LogQueryInput(QueryInput):
    trace_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    limit: int = Field(default=20, ge=1, le=50)


def _result(start: datetime, end: datetime, data: object) -> str:
    window = TimeWindow(start=start, end=end)
    status = "ok" if data else "empty"
    return ToolResult[object](
        status=status,
        window=window,
        source_kind="simulated",
        source_name="fixture",
        data=data,
        warnings=[FIXTURE_WARNING],
    ).model_dump_json()


def _matches_fixture(window: TimeWindow) -> bool:
    return window.start == FIXTURE_START and window.end == FIXTURE_END


@tool(args_schema=QueryInput)
def query_metrics(service: str, start: datetime, end: datetime) -> str:
    """Query request count, errors and latency for a service and UTC time window."""
    window = TimeWindow(start=start, end=end)
    data = METRICS.get(service) if _matches_fixture(window) else None
    return _result(start, end, data)


@tool(args_schema=TraceQueryInput)
def query_traces(service: str, start: datetime, end: datetime, limit: int = 5) -> str:
    """Find slow traces and their spans for a service and UTC time window."""
    window = TimeWindow(start=start, end=end)
    traces = [
        trace
        for trace in TRACES
        if service in {trace["root_service"], *(span["service"] for span in trace["spans"])}
    ][:limit]
    return _result(start, end, traces if _matches_fixture(window) else [])


@tool(args_schema=LogQueryInput)
def query_logs(
    service: str,
    start: datetime,
    end: datetime,
    trace_id: str | None = None,
    limit: int = 20,
) -> str:
    """Query a bounded log sample, optionally narrowed to a trace_id from an earlier result."""
    window = TimeWindow(start=start, end=end)
    logs = [
        log
        for log in LOGS
        if log["service"] == service and (trace_id is None or log["trace_id"] == trace_id)
    ][:limit]
    return _result(start, end, logs if _matches_fixture(window) else [])
