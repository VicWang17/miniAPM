from datetime import datetime
from typing import Literal, Protocol

from miniapm.models import TimeWindow, ToolResult

GroupDimension = Literal["version", "instance", "route", "error_code"]


class GroupAnalysisBackend(Protocol):
    def group_analysis(
        self, service: str, start: datetime, end: datetime, dimension: str
    ) -> str: ...


class WindowBackend(Protocol):
    def window_metrics(self, service: str, start: datetime, end: datetime) -> dict: ...


class TraceBackend(Protocol):
    def query_traces(
        self, service: str, start: datetime, end: datetime, limit: int
    ) -> str: ...


class DeploymentContext(Protocol):
    def deployments(
        self, dataset_id: str, service: str, start: datetime, end: datetime
    ) -> list[dict]: ...


class ResourceBackend(Protocol):
    def query_resources(
        self, dataset_id: str, service: str, start: datetime, end: datetime
    ) -> list[dict]: ...


def analyze_groups(
    backend: GroupAnalysisBackend,
    service: str,
    start: datetime,
    end: datetime,
    dimension: GroupDimension,
) -> str:
    return backend.group_analysis(service, start, end, dimension)


def compare_windows(
    backend: WindowBackend,
    service: str,
    current: TimeWindow,
    previous: TimeWindow,
) -> str:
    current_metrics = backend.window_metrics(service, current.start, current.end)
    previous_metrics = backend.window_metrics(service, previous.start, previous.end)
    delta = {}
    for key in ("request_count", "error_count", "avg_seconds", "p95_seconds"):
        before = previous_metrics.get(key)
        after = current_metrics.get(key)
        delta[key] = after - before if before is not None and after is not None else None
    has_data = current_metrics["request_count"] > 0 and previous_metrics["request_count"] > 0
    return ToolResult[object](
        status="ok" if has_data else "partial",
        window=current,
        source_kind="simulated",
        source_name="clickhouse",
        data={
            "current": current_metrics,
            "previous": previous_metrics,
            "delta": delta,
            "current_window": current.model_dump(mode="json"),
            "previous_window": previous.model_dump(mode="json"),
        },
        warnings=[] if has_data else ["one comparison window has no requests"],
    ).model_dump_json()


def analyze_traces(
    backend: TraceBackend,
    service: str,
    start: datetime,
    end: datetime,
    limit: int = 5,
) -> str:
    return backend.query_traces(service, start, end, limit)


def analyze_resource_changes(
    context: DeploymentContext,
    dataset_id: str,
    service: str,
    window: TimeWindow,
    resource_backend: ResourceBackend | None = None,
) -> str:
    deployments = context.deployments(dataset_id, service, window.start, window.end)
    resources = (
        resource_backend.query_resources(dataset_id, service, window.start, window.end)
        if resource_backend
        else []
    )
    return ToolResult[object](
        status="ok" if resource_backend else "partial",
        window=window,
        source_kind="simulated",
        source_name="mysql",
        data={"deployments": deployments, "resources": resources},
        warnings=[] if resource_backend else ["resource source is pending P8; deployments are not proof of causality"],
    ).model_dump_json()
