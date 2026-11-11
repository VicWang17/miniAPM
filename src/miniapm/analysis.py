from datetime import datetime, timedelta
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
        source_kind=getattr(backend, "source_kind", "simulated"),
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


def compare_with_baseline(
    backend: WindowBackend,
    baseline_backend,
    dataset_id: str,
    service: str,
    route: str,
    current: TimeWindow,
    baseline_start_date: str,
    baseline_end_date: str,
    minimum_samples: int = 5,
) -> str:
    current_metrics = backend.window_metrics(service, current.start, current.end)
    baseline = baseline_backend.query_baseline(
        dataset_id, service, route, baseline_start_date, baseline_end_date
    )
    warnings = []
    if baseline and baseline.get("cache_status") == "fallback_after_error":
        warnings.append("Redis unavailable; baseline read from ClickHouse")
    comparable = baseline is not None
    if baseline is None:
        warnings.append("no matching historical baseline")
    elif baseline["sample_count"] < minimum_samples:
        comparable = False
        warnings.append("historical baseline has insufficient samples")
    else:
        current_end_inclusive = (current.end - timedelta(microseconds=1)).date().isoformat()
        if current.start.date().isoformat() <= baseline_end_date and current_end_inclusive >= baseline_start_date:
            comparable = False
            warnings.append("current window overlaps historical baseline")
    delta = {}
    for key in ("request_count", "error_count", "avg_seconds", "p95_seconds"):
        delta[key] = current_metrics[key] - baseline[key] if comparable else None
    return ToolResult[object](
        status="ok" if comparable else "partial",
        window=current,
        source_kind="simulated",
        source_name="clickhouse_hive_baseline",
        data={
            "current": current_metrics,
            "baseline": baseline,
            "delta": delta,
            "comparable": comparable,
            "route": route,
            "quantile_method": "nearest-rank ceil(0.95*n)",
        },
        warnings=warnings,
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
    deployments = (
        context.deployments(dataset_id, service, window.start, window.end)
        if context is not None
        else []
    )
    resources = (
        resource_backend.query_resources(dataset_id, service, window.start, window.end)
        if resource_backend
        else []
    )
    return ToolResult[object](
        status="ok" if resources else "partial",
        window=window,
        source_kind=getattr(resource_backend, "source_kind", "simulated"),
        source_name=getattr(resource_backend, "source_name", "mysql"),
        data={"deployments": deployments, "resources": resources},
        warnings=[] if resources else ["no matching resource samples; deployments are not proof of causality"],
    ).model_dump_json()
