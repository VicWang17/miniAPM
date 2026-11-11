from datetime import datetime
from typing import Literal

from langchain_core.tools import tool
from pydantic import BaseModel, ConfigDict, Field

from miniapm.analysis import (
    GroupDimension,
    ResourceBackend,
    analyze_groups,
    analyze_resource_changes,
    analyze_traces,
    compare_with_baseline,
    compare_windows,
)
from miniapm.backends import TelemetryBackend
from miniapm.models import TimeWindow


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


class GroupQueryInput(QueryInput):
    dimension: GroupDimension


class WindowCompareInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: Literal["order", "inventory"]
    current_start: datetime
    current_end: datetime
    previous_start: datetime
    previous_end: datetime


class ResourceQueryInput(QueryInput):
    dataset_id: str


class BaselineCompareInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_id: str
    service: Literal["order", "inventory"]
    route: str = "POST /orders"
    current_start: datetime
    current_end: datetime
    baseline_start_date: str
    baseline_end_date: str


def create_tools(
    backend: TelemetryBackend,
    deployment_context=None,
    resource_backend: ResourceBackend | None = None,
    baseline_backend=None,
):
    @tool(args_schema=QueryInput)
    def query_metrics(service: str, start: datetime, end: datetime) -> str:
        """Query request count, errors and latency for a service and UTC time window."""
        return backend.query_metrics(service, start, end)

    @tool(args_schema=TraceQueryInput)
    def query_traces(service: str, start: datetime, end: datetime, limit: int = 5) -> str:
        """Find traces and their spans for a service and UTC time window."""
        return analyze_traces(backend, service, start, end, limit)

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

    tools = [query_metrics, query_traces, query_logs]
    if hasattr(backend, "group_analysis"):
        @tool(args_schema=GroupQueryInput)
        def analyze_grouped_requests(
            service: str, start: datetime, end: datetime, dimension: GroupDimension
        ) -> str:
            """Group request counts, errors and latency by version, instance, route or error code."""
            return analyze_groups(backend, service, start, end, dimension)

        @tool(args_schema=WindowCompareInput)
        def compare_time_windows(
            service: str,
            current_start: datetime,
            current_end: datetime,
            previous_start: datetime,
            previous_end: datetime,
        ) -> str:
            """Compare request and latency statistics between current and previous windows."""
            return compare_windows(
                backend,
                service,
                TimeWindow(start=current_start, end=current_end),
                TimeWindow(start=previous_start, end=previous_end),
            )

        tools.extend([analyze_grouped_requests, compare_time_windows])
    if baseline_backend is not None:
        @tool(args_schema=BaselineCompareInput)
        def compare_historical_baseline(
            dataset_id: str,
            service: Literal["order", "inventory"],
            current_start: datetime,
            current_end: datetime,
            baseline_start_date: str,
            baseline_end_date: str,
            route: str = "POST /orders",
        ) -> str:
            """Compare the current window with a real Hive historical baseline."""
            return compare_with_baseline(
                backend,
                baseline_backend,
                dataset_id,
                service,
                route,
                TimeWindow(start=current_start, end=current_end),
                baseline_start_date,
                baseline_end_date,
            )

        tools.append(compare_historical_baseline)
    if deployment_context is not None:
        @tool(args_schema=ResourceQueryInput)
        def query_resource_changes(
            dataset_id: str, service: str, start: datetime, end: datetime
        ) -> str:
            """Query deployments and available resource evidence for a dataset, service and window."""
            return analyze_resource_changes(
                deployment_context,
                dataset_id,
                service,
                TimeWindow(start=start, end=end),
                resource_backend,
            )

        tools.append(query_resource_changes)
    return tools
