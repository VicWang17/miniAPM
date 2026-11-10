import json
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, Query

from miniapm.config import Settings
from miniapm.models import ServiceContext, ServiceName, TimeWindow
from miniapm.prometheus_backend import PrometheusBackend

DATASET_ID = UUID("018f2f70-0000-7000-8000-000000000001")
SERVICE_CATALOG = {
    "order": (("order-1",), ("v1",), ("inventory",)),
    "inventory": (("inventory-1",), ("v1",), ()),
}


class MetricsQuery(TimeWindow):
    service: ServiceName


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

    return app
