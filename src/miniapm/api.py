import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, Field

from miniapm.alerts import AlertmanagerNotification, resolve_alert
from miniapm.analysis import GroupDimension, analyze_groups, analyze_resource_changes, analyze_traces, compare_with_baseline, compare_windows
from miniapm.baseline import BaselineClickHouseBackend
from miniapm.clickhouse_backend import ClickHouseBackend
from miniapm.config import Settings
from miniapm.models import ContractModel, ServiceContext, ServiceName, TimeWindow
from miniapm.investigation_manager import InvestigationManager, InvestigationQueueFull
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


class BaselineCompareQuery(ContractModel):
    dataset_id: UUID
    service: ServiceName
    route: str = "POST /orders"
    current_start: datetime
    current_end: datetime
    baseline_start_date: str
    baseline_end_date: str


class InvestigationRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)
    dataset_id: UUID
    service: ServiceName
    start: datetime
    end: datetime


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        context = MySQLContext()
        context.mark_interrupted()
        manager = InvestigationManager(
            context,
            lambda dataset_id: ClickHouseBackend(dataset_id=dataset_id),
        )
        app.state.investigations = manager
        try:
            yield
        finally:
            await manager.close()
            context.close()

    app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)

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

    @app.get("/api/v1/analysis/baseline")
    def analysis_baseline(query: Annotated[BaselineCompareQuery, Query()]) -> dict:
        backend = ClickHouseBackend(dataset_id=str(query.dataset_id))
        baseline_backend = BaselineClickHouseBackend()
        try:
            return json.loads(
                compare_with_baseline(
                    backend,
                    baseline_backend,
                    str(query.dataset_id),
                    query.service,
                    query.route,
                    TimeWindow(start=query.current_start, end=query.current_end),
                    query.baseline_start_date,
                    query.baseline_end_date,
                )
            )
        finally:
            backend.close()
            baseline_backend.close()

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

    @app.post("/api/v1/alerts/webhook", status_code=202)
    async def alert_webhook(http_request: Request, notification: AlertmanagerNotification) -> dict:
        manager = http_request.app.state.investigations
        results = [
            await manager.submit_alert(alert, DATASET_ID)
            if alert.status == "firing"
            else resolve_alert(manager.context, alert)
            for alert in notification.alerts
        ]
        return {"accepted": len(results), "alerts": results}

    @app.post("/api/v1/investigations", status_code=202)
    async def create_investigation(
        http_request: Request, request: InvestigationRequest
    ) -> dict:
        manager = http_request.app.state.investigations
        try:
            investigation_id = await manager.submit(
                request.question,
                str(request.dataset_id),
                request.service,
                request.start,
                request.end,
            )
        except InvestigationQueueFull as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"investigation_id": investigation_id, "status": "queued"}

    @app.get("/api/v1/investigations/{investigation_id}")
    def investigation_status(http_request: Request, investigation_id: str) -> dict:
        record = http_request.app.state.investigations.context.investigation(investigation_id)
        if record is None:
            raise HTTPException(status_code=404, detail="investigation not found")
        return record

    @app.get("/api/v1/investigations/{investigation_id}/report")
    def investigation_report(http_request: Request, investigation_id: str) -> dict:
        record = http_request.app.state.investigations.context.investigation(investigation_id)
        if record is None:
            raise HTTPException(status_code=404, detail="investigation not found")
        if record["report_json"] is None:
            raise HTTPException(status_code=409, detail="report is not ready")
        return record["report_json"]

    return app
