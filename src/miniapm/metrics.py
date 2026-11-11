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
    def __init__(
        self,
        registry: CollectorRegistry | None = None,
        experimental_trace_label: bool = False,
    ) -> None:
        self.registry = registry or CollectorRegistry()
        self.experimental_trace_label = experimental_trace_label
        counter_labels = (*LABELS, "status")
        if experimental_trace_label:
            counter_labels = (*counter_labels, "trace_id")
        self.requests_total = Counter(
            "miniapm_requests_total",
            "Completed simulated requests",
            counter_labels,
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
        for span in (plan.spans[0], plan.spans[2]):
            labels = self._labels(span.service, span.instance, span.version, plan.route)
            self.requests_in_flight.labels(**labels).inc()

    async def request_finished(self, events: RequestEvents) -> None:
        request = events.request
        labels = self._labels(
            request.service, request.instance, request.version, request.route
        )
        self.requests_in_flight.labels(**labels).dec()
        counter_labels = {**labels, "status": request.status}
        if self.experimental_trace_label:
            counter_labels["trace_id"] = request.trace_id
        self.requests_total.labels(**counter_labels).inc()
        self.request_duration.labels(**labels).observe(request.duration_seconds)

        inventory = next(span for span in events.spans if span.kind == "server" and span.service == "inventory")
        inventory_labels = self._labels(
            inventory.service, inventory.instance, inventory.version, inventory.route
        )
        duration = (inventory.ended_at - inventory.started_at).total_seconds()
        self.requests_in_flight.labels(**inventory_labels).dec()
        inventory_counter_labels = {**inventory_labels, "status": inventory.status}
        if self.experimental_trace_label:
            inventory_counter_labels["trace_id"] = request.trace_id
        self.requests_total.labels(**inventory_counter_labels).inc()
        self.request_duration.labels(**inventory_labels).observe(duration)

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
