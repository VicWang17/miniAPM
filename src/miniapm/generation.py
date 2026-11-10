from datetime import timedelta
from uuid import UUID, uuid5

from pydantic import Field

from miniapm.models import ContractModel, LogEvent, RequestEvent, SpanEvent
from miniapm.topology import RequestPlan, SpanPlan


class RequestParameters(ContractModel):
    inventory_duration_seconds: float = Field(default=0.08, gt=0, le=60)
    error: bool = False
    missing_inventory: bool = False


class RequestEvents(ContractModel):
    request: RequestEvent
    spans: tuple[SpanEvent, ...]
    logs: tuple[LogEvent, ...]


class RequestTruth(ContractModel):
    request_id: UUID
    parameters: RequestParameters
    expected_span_count: int
    expected_log_count: int


def _span_event(
    plan: RequestPlan,
    span: SpanPlan,
    ended_at,
    status: str,
) -> SpanEvent:
    started_at = plan.event_time if span.kind == "server" and span.service == "order" else plan.event_time + timedelta(seconds=0.02)
    return SpanEvent(
        event_id=span.event_id,
        event_time=started_at,
        ingested_at=ended_at,
        environment=plan.environment,
        dataset_id=plan.dataset_id,
        service=span.service,
        instance=span.instance,
        version=span.version,
        route=plan.route,
        source_kind=plan.source_kind,
        trace_id=plan.trace_id,
        span_id=span.span_id,
        parent_span_id=span.parent_span_id,
        kind=span.kind,
        started_at=started_at,
        ended_at=ended_at,
        status=status,
    )


def _log_event(
    plan: RequestPlan,
    span: SpanPlan,
    message: str,
    severity: str,
    error_code: str | None,
    event_time,
) -> LogEvent:
    return LogEvent(
        event_id=uuid5(plan.request_id, f"log:{span.service}"),
        event_time=event_time,
        ingested_at=event_time,
        environment=plan.environment,
        dataset_id=plan.dataset_id,
        service=span.service,
        instance=span.instance,
        version=span.version,
        route=plan.route,
        source_kind=plan.source_kind,
        trace_id=plan.trace_id,
        span_id=span.span_id,
        severity=severity,
        message=message,
        error_code=error_code,
    )


def generate_request(
    plan: RequestPlan,
    parameters: RequestParameters | None = None,
) -> tuple[RequestEvents, RequestTruth]:
    parameters = parameters or RequestParameters()
    if plan.event_time.tzinfo is None:
        raise ValueError("request event_time must include a timezone")

    order_server, order_client, inventory_server = plan.spans
    inventory_started_at = plan.event_time + timedelta(seconds=0.02)
    inventory_ended_at = inventory_started_at + timedelta(
        seconds=parameters.inventory_duration_seconds
    )
    order_ended_at = inventory_ended_at + timedelta(seconds=0.04)
    status = "error" if parameters.error else "ok"

    spans = [
        _span_event(plan, order_server, order_ended_at, status),
        _span_event(plan, order_client, inventory_ended_at, status),
    ]
    if not parameters.missing_inventory:
        spans.append(_span_event(plan, inventory_server, inventory_ended_at, status))

    order_log = _log_event(
        plan,
        order_server,
        "order failed" if parameters.error else "order completed",
        "ERROR" if parameters.error else "INFO",
        "ORDER_FAILED" if parameters.error else None,
        order_ended_at,
    )
    logs = [order_log]
    if not parameters.missing_inventory:
        logs.append(
            _log_event(
                plan,
                inventory_server,
                "inventory reservation failed" if parameters.error else "inventory reservation completed",
                "ERROR" if parameters.error else "INFO",
                "INVENTORY_RESERVATION_FAILED" if parameters.error else None,
                inventory_ended_at,
            )
        )

    order_instance = order_server
    request = RequestEvent(
        event_id=plan.request_id,
        event_time=plan.event_time,
        ingested_at=order_ended_at,
        environment=plan.environment,
        dataset_id=plan.dataset_id,
        service="order",
        instance=order_instance.instance,
        version=order_instance.version,
        route=plan.route,
        source_kind=plan.source_kind,
        trace_id=plan.trace_id,
        status=status,
        duration_seconds=(order_ended_at - plan.event_time).total_seconds(),
    )
    events = RequestEvents(request=request, spans=tuple(spans), logs=tuple(logs))
    truth = RequestTruth(
        request_id=plan.request_id,
        parameters=parameters,
        expected_span_count=len(events.spans),
        expected_log_count=len(events.logs),
    )
    return events, truth
