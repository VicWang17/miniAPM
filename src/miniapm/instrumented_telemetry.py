import logging
import os
import time
from datetime import UTC, datetime
from uuid import UUID, uuid5

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import SpanKind, StatusCode, format_span_id, format_trace_id
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from miniapm.models import LogEvent, SpanEvent
from miniapm.rabbitmq_publisher import RabbitMQPublisher


def _datetime(nanoseconds: int) -> datetime:
    return datetime.fromtimestamp(nanoseconds / 1_000_000_000, UTC)


class RabbitSpanExporter(SpanExporter):
    def __init__(self, publisher: RabbitMQPublisher, dataset_id: UUID) -> None:
        self.publisher = publisher
        self.dataset_id = dataset_id

    def export(self, spans) -> SpanExportResult:
        try:
            for span in spans:
                context = span.context
                trace_id = format_trace_id(context.trace_id)
                span_id = format_span_id(context.span_id)
                resource = span.resource
                parent_id = (
                    format_span_id(span.parent.span_id) if span.parent is not None else None
                )
                event = SpanEvent(
                    event_id=uuid5(self.dataset_id, f"otel:{trace_id}:{span_id}"),
                    event_time=_datetime(span.start_time),
                    ingested_at=datetime.now(UTC),
                    environment="linux-container",
                    dataset_id=self.dataset_id,
                    service=resource.attributes["service.name"],
                    instance=resource.attributes["service.instance.id"],
                    version=resource.attributes["service.version"],
                    route=span.attributes.get("http.route") or span.attributes.get("url.path") or span.name,
                    source_kind="instrumented",
                    trace_id=trace_id,
                    span_id=span_id,
                    parent_span_id=parent_id,
                    kind="server" if span.kind == SpanKind.SERVER else "client" if span.kind == SpanKind.CLIENT else "internal",
                    started_at=_datetime(span.start_time),
                    ended_at=_datetime(span.end_time),
                    status="error" if span.status.status_code == StatusCode.ERROR else "ok",
                )
                self.publisher.publish("spans", event.model_dump(mode="json"))
            return SpanExportResult.SUCCESS
        except Exception:
            logging.exception("failed to export OpenTelemetry spans")
            return SpanExportResult.FAILURE


class RabbitLogHandler(logging.Handler):
    def __init__(self, publisher: RabbitMQPublisher, dataset_id: UUID) -> None:
        super().__init__()
        self.publisher = publisher
        self.dataset_id = dataset_id

    def emit(self, record: logging.LogRecord) -> None:
        context = trace.get_current_span().get_span_context()
        trace_id = format_trace_id(context.trace_id) if context.trace_id else None
        span_id = format_span_id(context.span_id) if context.span_id else None
        event = LogEvent(
            event_id=uuid5(self.dataset_id, f"log:{trace_id}:{span_id}:{record.created}:{record.lineno}"),
            event_time=datetime.fromtimestamp(record.created, UTC),
            ingested_at=datetime.now(UTC),
            environment="linux-container",
            dataset_id=self.dataset_id,
            service=os.environ["SERVICE_NAME"],
            instance=os.environ["INSTANCE"],
            version=os.environ["VERSION"],
            route=getattr(record, "route", "application"),
            source_kind="instrumented",
            trace_id=trace_id,
            span_id=span_id,
            severity=record.levelname,
            message=record.getMessage(),
            error_code=getattr(record, "error_code", None),
        )
        self.publisher.publish("logs", event.model_dump(mode="json"))


def create_tracer_provider(resource) -> tuple[TracerProvider, RabbitMQPublisher]:
    dataset_id = UUID(os.environ["DATASET_ID"])
    publisher = RabbitMQPublisher()
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(RabbitSpanExporter(publisher, dataset_id)))
    handler = RabbitLogHandler(publisher, dataset_id)
    handler.setLevel(logging.INFO)
    logging.getLogger("miniapm.service").addHandler(handler)
    logging.getLogger("miniapm.service").setLevel(logging.INFO)
    logging.getLogger("miniapm.service").propagate = False
    return provider, publisher


class InstrumentedMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        labels = ("dataset_id", "service", "instance", "version", "route", "source_kind")
        self.requests = Counter("miniapm_requests_total", "Instrumented HTTP requests", (*labels, "status"), registry=self.registry)
        self.in_flight = Gauge("miniapm_requests_in_flight", "Instrumented requests in flight", labels, registry=self.registry)
        self.duration = Histogram(
            "miniapm_request_duration_seconds",
            "Instrumented HTTP request duration",
            labels,
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
            registry=self.registry,
        )

    def labels(self, route: str) -> dict:
        return {
            "dataset_id": os.environ["DATASET_ID"],
            "service": os.environ["SERVICE_NAME"],
            "instance": os.environ["INSTANCE"],
            "version": os.environ["VERSION"],
            "route": route,
            "source_kind": "instrumented",
        }

    def started(self, route: str) -> dict:
        labels = self.labels(route)
        self.in_flight.labels(**labels).inc()
        return labels

    def finished(self, labels: dict, status: str, started: float) -> None:
        self.in_flight.labels(**labels).dec()
        self.requests.labels(**labels, status=status).inc()
        self.duration.labels(**labels).observe(time.monotonic() - started)
