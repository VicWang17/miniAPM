from datetime import datetime
from typing import Literal

from langchain_core.tools import tool
from pydantic import BaseModel, ConfigDict, Field

from miniapm.backends import TelemetryBackend


class QueryInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: Literal["order", "inventory"]
    start: datetime
    end: datetime


class TraceQueryInput(QueryInput):
    limit: int = Field(default=5, ge=1, le=20)


class LogQueryInput(QueryInput):
    trace_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    version: Literal["v1", "v2"] | None = None
    error_code: str | None = Field(default=None, max_length=100)
    limit: int = Field(default=20, ge=1, le=50)


def create_tools(backend: TelemetryBackend):
    @tool(args_schema=QueryInput)
    def query_metrics(service: str, start: datetime, end: datetime) -> str:
        """Query request count, errors and latency for a service and UTC time window."""
        return backend.query_metrics(service, start, end)

    @tool(args_schema=TraceQueryInput)
    def query_traces(service: str, start: datetime, end: datetime, limit: int = 5) -> str:
        """Find traces and their spans for a service and UTC time window."""
        return backend.query_traces(service, start, end, limit)

    @tool(args_schema=LogQueryInput)
    def query_logs(
        service: str,
        start: datetime,
        end: datetime,
        trace_id: str | None = None,
        version: str | None = None,
        error_code: str | None = None,
        limit: int = 20,
    ) -> str:
        """Query bounded logs, optionally filtered by trace_id, version or error_code."""
        return backend.query_logs(
            service, start, end, trace_id, version, error_code, limit
        )

    return query_metrics, query_traces, query_logs
