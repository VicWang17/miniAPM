import json
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Query

from miniapm.analysis import GroupDimension, analyze_groups, analyze_resource_changes, analyze_traces, compare_windows
from miniapm.clickhouse_backend import ClickHouseBackend
from miniapm.config import Settings
from miniapm.models import ContractModel, ServiceContext, ServiceName, TimeWindow
from miniapm.mysql_context import MySQLContext
from miniapm.prometheus_backend import PrometheusBackend

DATASET_ID = UUID("018f2f70-0000-7000-8000-000000000001")
SERVICE_CATALOG = {
    "order": (("order-1",), ("v1",), ("inventory",)),
    "inventory": (("inventory-1",), ("v1",), ()),
}


class MetricsQuery(TimeWindow):
    service: ServiceName


class AnalysisQuery(TimeWindow):
    dataset_id: UUID
    service: ServiceName


class GroupAnalysisQuery(AnalysisQuery):
    dimension: GroupDimension


class TraceAnalysisQuery(AnalysisQuery):
    limit: int = 5


class WindowCompareQuery(ContractModel):
    dataset_id: UUID
    service: ServiceName
    current_start: datetime
    current_end: datetime
    previous_start: datetime
    previous_end: datetime


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title=settings.app_name, version="0.1.0")

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/services/{service}/context", response_model=ServiceContext)
    def service_context(
        service: ServiceName,
        window: Annotated[TimeWindow, Query()],
    ) -> ServiceContext:
        instances, versions, dependencies = SERVICE_CATALOG[service]
        return ServiceContext(
            environment=settings.environment,
            dataset_id=DATASET_ID,
            service=service,
            window=window,
            instances=instances,
            versions=versions,
            dependencies=dependencies,
        )

    @app.get("/api/v1/metrics/query")
    def metrics_query(
        query: Annotated[MetricsQuery, Query()],
    ) -> dict:
        result = PrometheusBackend(settings.prometheus_url).query_metrics(
            query.service, query.start, query.end
        )
        return json.loads(result)

    @app.get("/api/v1/analysis/window")
    def analysis_window(query: Annotated[WindowCompareQuery, Query()]) -> dict:
        backend = ClickHouseBackend(dataset_id=str(query.dataset_id))
        try:
            return json.loads(
                compare_windows(
                    backend,
                    query.service,
                    TimeWindow(start=query.current_start, end=query.current_end),
                    TimeWindow(start=query.previous_start, end=query.previous_end),
                )
            )
        finally:
            backend.close()

    @app.get("/api/v1/analysis/groups")
    def analysis_groups(query: Annotated[GroupAnalysisQuery, Query()]) -> dict:
        backend = ClickHouseBackend(dataset_id=str(query.dataset_id))
        try:
            return json.loads(
                analyze_groups(
                    backend, query.service, query.start, query.end, query.dimension
                )
            )
        finally:
            backend.close()

    @app.get("/api/v1/analysis/traces")
    def analysis_traces(query: Annotated[TraceAnalysisQuery, Query()]) -> dict:
        backend = ClickHouseBackend(dataset_id=str(query.dataset_id))
        try:
            return json.loads(
                analyze_traces(
                    backend, query.service, query.start, query.end, query.limit
                )
            )
        finally:
            backend.close()

    @app.get("/api/v1/analysis/resource-changes")
    def analysis_resource_changes(query: Annotated[AnalysisQuery, Query()]) -> dict:
        context = MySQLContext()
        try:
            return json.loads(
                analyze_resource_changes(
                    context, str(query.dataset_id), query.service, query
                )
            )
        finally:
            context.close()

    return app
