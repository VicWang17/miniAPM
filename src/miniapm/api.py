import json
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from miniapm.heterogeneous import HeterogeneousContext, HeterogeneousPipeline
from miniapm.rabbitmq_publisher import RabbitMQPublisher

from miniapm.alerts import AlertmanagerNotification, resolve_alert
from miniapm.analysis import GroupDimension, analyze_groups, analyze_resource_changes, analyze_traces, compare_with_baseline, compare_windows
from miniapm.baseline_cache import CachedBaselineBackend
from miniapm.clickhouse_backend import ClickHouseBackend
from miniapm.config import Settings
from miniapm.models import ContractModel, ServiceContext, ServiceName, TimeWindow
from miniapm.investigation_manager import InvestigationManager, InvestigationQueueFull
from miniapm.mysql_context import MySQLContext
from miniapm.prometheus_backend import PrometheusBackend
from miniapm.resource_backend import PrometheusResourceBackend

DATASET_ID = UUID("018f2f70-0000-7000-8000-000000000001")


class AdaptationRequest(ContractModel):
    dataset_id: UUID
    environment: str
    source_id: str
    batch_id: str
    rule_version: str
    content: str = Field(min_length=1, max_length=1_000_000)
    event_time: datetime | None = None


class ApprovalRequest(ContractModel):
    proposal_path: str
    confirmed: bool


class ReplayRequest(ContractModel):
    dataset_id: UUID
    batch_id: str
    rule_version: str
    event_time: datetime | None = None


class ServiceContextQuery(TimeWindow):
    dataset_id: UUID = DATASET_ID


class MetricsQuery(TimeWindow):
    service: ServiceName


class AnalysisQuery(TimeWindow):
    dataset_id: UUID
    service: ServiceName


class LogAnalysisQuery(AnalysisQuery):
    environment: str | None = None
    instance: str | None = None
    version: str | None = None
    source_kind: str | None = None
    limit: int = 50


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


class CandidateGroupRequest(ContractModel):
    anchor_event_key: str


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
        context = HeterogeneousContext()
        context.mark_interrupted()
        publisher = RabbitMQPublisher()
        manager = InvestigationManager(
            context,
            lambda dataset_id: ClickHouseBackend(dataset_id=dataset_id),
        )
        app.state.investigations = manager
        app.state.adaptation = HeterogeneousPipeline(context, publisher)
        app.state.experiment_notifications = []
        try:
            yield
        finally:
            await manager.close()
            publisher.close()
            context.close()

    app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/services/{service}/context", response_model=ServiceContext)
    def service_context(
        service: ServiceName,
        query: Annotated[ServiceContextQuery, Query()],
    ) -> ServiceContext:
        window = TimeWindow(start=query.start, end=query.end)
        dataset_id = query.dataset_id
        record = app.state.adaptation.context.service(str(dataset_id), service)
        if record is None:
            raise HTTPException(status_code=404, detail="service context not found")
        instances = [item["instance"] for item in record["instances"]]
        versions = sorted({item["version"] for item in record["instances"]})
        return ServiceContext(
            environment=record["environment"], dataset_id=dataset_id, service=service,
            window=window, instances=instances, versions=versions,
            dependencies=record["dependencies"],
        )

    @app.get("/api/v1/metrics/query")
    def metrics_query(
        query: Annotated[MetricsQuery, Query()],
    ) -> dict:
        result = PrometheusBackend(settings.prometheus_url).query_metrics(
            query.service, query.start, query.end
        )
        return json.loads(result)

    @app.get("/api/v1/logs/query")
    def logs_query(query: Annotated[LogAnalysisQuery, Query()]) -> dict:
        backend = ClickHouseBackend(dataset_id=str(query.dataset_id))
        try:
            return json.loads(backend.query_logs(query.service, query.start, query.end, None,
                query.version, None, query.limit, query.environment, query.instance, query.source_kind))
        finally:
            backend.close()

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
        baseline_backend = CachedBaselineBackend()
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
                    context, str(query.dataset_id), query.service, query,
                    PrometheusResourceBackend(settings.prometheus_url),
                )
            )
        finally:
            context.close()

    @app.post("/api/v1/alerts/experiments", status_code=202)
    async def experiment_alert(http_request: Request) -> dict:
        notification = await http_request.json()
        records = http_request.app.state.experiment_notifications
        records.append(notification)
        del records[:-100]
        return {"accepted": len(notification.get("alerts", [])), "investigations_created": 0}

    @app.get("/api/v1/alerts/experiments")
    def experiment_notifications(http_request: Request) -> dict:
        return {"notifications": http_request.app.state.experiment_notifications}

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

    @app.post("/api/v1/adaptation/{source_format}")
    def adapt(source_format: str, request: AdaptationRequest, http_request: Request) -> dict:
        if source_format not in {"nginx", "otlp", "prometheus"}:
            raise HTTPException(status_code=422, detail="unsupported source format")
        return http_request.app.state.adaptation.process(source_format=source_format,
            **request.model_dump())

    @app.get("/api/v1/adaptation/pending")
    def pending(dataset_id: UUID, limit: int = 5) -> dict:
        return app.state.adaptation.context.pending_summary(str(dataset_id), limit)

    @app.get("/metrics/heterogeneous")
    def heterogeneous_metrics() -> Response:
        return Response(app.state.adaptation.metrics.render(), media_type="text/plain; version=0.0.4")

    @app.post("/api/v1/adaptation/rules/approve")
    def approve_rule(request: ApprovalRequest) -> dict:
        path = Path(request.proposal_path).resolve()
        root = Path("data/generated/adaptation-proposals").resolve()
        if root not in path.parents:
            raise HTTPException(status_code=422, detail="proposal must be in the proposal directory")
        return app.state.adaptation.approve_rule(str(path), request.confirmed)

    @app.post("/api/v1/adaptation/replay")
    def replay(request: ReplayRequest) -> dict:
        return app.state.adaptation.replay(**request.model_dump())

    @app.post("/api/v1/alert-groups", status_code=201)
    def create_alert_group(request: CandidateGroupRequest) -> dict:
        try:
            group = app.state.adaptation.context.create_candidate_group(request.anchor_event_key)
            return group.model_dump(mode="json")
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/v1/alert-groups/{group_id}")
    def alert_group(group_id: str) -> dict:
        group = app.state.adaptation.context.candidate_group(group_id)
        if group is None:
            raise HTTPException(status_code=404, detail="candidate group not found")
        return group.model_dump(mode="json")

    @app.post("/api/v1/alert-groups/{group_id}/investigations", status_code=202)
    async def investigate_alert_group(group_id: str, http_request: Request) -> dict:
        try:
            investigation_id = await http_request.app.state.investigations.submit_group(group_id)
            return {"investigation_id": investigation_id, "status": "queued"}
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except InvestigationQueueFull as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    return app
