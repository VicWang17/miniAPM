from fastapi import FastAPI, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from miniapm.generation import RequestEvents
from miniapm.realtime import ResourceSample
from miniapm.topology import RequestPlan

LABELS = ("service", "instance", "version", "route")


class MetricsPublisher:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.requests_total = Counter(
            "miniapm_requests_total",
            "Completed simulated requests",
            (*LABELS, "status"),
            registry=self.registry,
        )
        self.requests_in_flight = Gauge(
            "miniapm_requests_in_flight",
            "Currently processing simulated requests",
            LABELS,
            registry=self.registry,
        )
        self.request_duration = Histogram(
            "miniapm_request_duration_seconds",
            "Simulated request duration in seconds",
            LABELS,
            buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5),
            registry=self.registry,
        )

    def _labels(self, service: str, instance: str, version: str, route: str) -> dict:
        return {
            "service": service,
            "instance": instance,
            "version": version,
            "route": route,
        }

    async def request_started(self, plan: RequestPlan) -> None:
        root = plan.spans[0]
        labels = self._labels(root.service, root.instance, root.version, plan.route)
        self.requests_in_flight.labels(**labels).inc()

    async def request_finished(self, events: RequestEvents) -> None:
        request = events.request
        labels = self._labels(
            request.service, request.instance, request.version, request.route
        )
        self.requests_in_flight.labels(**labels).dec()
        self.requests_total.labels(**labels, status=request.status).inc()
        self.request_duration.labels(**labels).observe(request.duration_seconds)

    async def publish_request(self, events: RequestEvents) -> None:
        return None

    async def publish_resource(self, sample: ResourceSample) -> None:
        return None


def create_metrics_app(registry: CollectorRegistry) -> FastAPI:
    app = FastAPI(title="miniAPM simulator metrics")

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app
