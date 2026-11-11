import json
import re
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID, uuid5

from prometheus_client.parser import text_string_to_metric_families
from pydantic import Field

from miniapm.models import ContractModel, LogEvent, SpanEvent


class AdaptationRule(ContractModel):
    version: str
    source_format: Literal["nginx", "otlp", "prometheus"]
    log_format: str | None = None
    duration_field: str | None = None
    duration_unit: Literal["seconds", "milliseconds"] | None = None
    route_from_request: bool = False
    time_unit: Literal["nanoseconds"] | None = None
    kind_map: dict[str, Literal["server", "client", "internal"]] = Field(default_factory=dict)
    metric_name: str | None = None
    source_metric: str | None = None


class PendingInput(ContractModel):
    event_id: UUID
    dataset_id: UUID
    environment: str
    source_format: str
    source_id: str
    batch_id: str
    record_locator: str
    raw_text: str = Field(max_length=1_000_000)
    reason: str = Field(max_length=500)
    rule_version: str


class AdaptedRecord(ContractModel):
    locator: str
    event: LogEvent | SpanEvent | None = None
    metric: dict | None = None
    pending: PendingInput | None = None


def stable_event_id(dataset_id: UUID, source_id: str, batch_id: str, locator: str) -> UUID:
    encoded = json.dumps([source_id, batch_id, locator], ensure_ascii=False, separators=(",", ":"))
    return uuid5(dataset_id, f"heterogeneous:{encoded}")


def pending(
    dataset_id: UUID,
    environment: str,
    source_format: str,
    source_id: str,
    batch_id: str,
    locator: str,
    raw_text: str,
    reason: str,
    rule_version: str,
) -> PendingInput:
    return PendingInput(
        event_id=stable_event_id(dataset_id, source_id, batch_id, locator),
        dataset_id=dataset_id,
        environment=environment,
        source_format=source_format,
        source_id=source_id,
        batch_id=batch_id,
        record_locator=locator,
        raw_text=raw_text[:1_000_000],
        reason=reason[:500],
        rule_version=rule_version,
    )


def _nginx_fields(log_format: str) -> list[str]:
    return re.findall(r"\$([a-zA-Z_][a-zA-Z0-9_]*)", log_format)


def adapt_nginx(
    content: str,
    rule: AdaptationRule,
    dataset_id: UUID,
    environment: str,
    source_id: str,
    batch_id: str,
    resolve_identity,
) -> list[AdaptedRecord]:
    if not rule.log_format or not rule.duration_field or not rule.duration_unit:
        raise ValueError("nginx rule requires log_format, duration_field and duration_unit")
    names = _nginx_fields(rule.log_format)
    pattern = re.escape(rule.log_format)
    pattern = pattern.replace(r'\"', '"')
    for name in names:
        value = r"(?P<%s>[^\"]*)" % name if name == "request" else r"(?P<%s>\S*)" % name
        if name == "time_local":
            value = r"(?P<time_local>[^\]]+)"
        pattern = re.sub(r"\\\$" + name + r"(?![a-zA-Z0-9_])", lambda _: value, pattern)
    records = []
    lines = content.splitlines()
    if len(lines) > 1000:
        raise ValueError("nginx batch exceeds 1000 records")
    for index, line in enumerate(lines):
        locator = f"line:{index + 1}"
        try:
            match = re.fullmatch(pattern, line)
            if not match:
                raise ValueError("unknown nginx log_format")
            fields = match.groupdict()
            if fields.get("source_id") != source_id:
                raise ValueError("source identity conflicts with declared source_id")
            event_time = datetime.strptime(fields["time_local"], "%d/%b/%Y:%H:%M:%S %z").astimezone(UTC)
            identity = resolve_identity(dataset_id, environment, source_id, event_time)
            if identity["status"] != "matched":
                raise ValueError(f"identity_{identity['status']}")
            value = float(fields[rule.duration_field])
            seconds = value / 1000 if rule.duration_unit == "milliseconds" else value
            method, route, _ = fields["request"].split(" ", 2)
            status_code = int(fields["status"])
            severity = "ERROR" if status_code >= 500 else "INFO"
            event = LogEvent(
                event_id=stable_event_id(dataset_id, source_id, batch_id, locator),
                event_time=event_time,
                ingested_at=datetime.now(UTC),
                environment=environment,
                dataset_id=dataset_id,
                service=identity["service"],
                instance=identity["instance"],
                version=identity["version"],
                route=f"{method} {route}",
                source_kind="simulated",
                severity=severity,
                message=f"nginx status={status_code} duration_seconds={seconds}",
                error_code=f"http_{status_code}" if status_code >= 500 else None,
            )
            records.append(AdaptedRecord(locator=locator, event=event))
        except Exception as exc:
            records.append(AdaptedRecord(locator=locator, pending=pending(
                dataset_id, environment, "nginx", source_id, batch_id, locator, line,
                str(exc), rule.version,
            )))
    return records


def _nano_time(value: str | int) -> datetime:
    nanoseconds = int(value)
    if nanoseconds <= 0:
        raise ValueError("invalid OTLP timestamp")
    return datetime.fromtimestamp(nanoseconds / 1_000_000_000, UTC)


def adapt_otlp(
    content: str,
    rule: AdaptationRule,
    dataset_id: UUID,
    environment: str,
    source_id: str,
    batch_id: str,
    resolve_identity,
) -> list[AdaptedRecord]:
    document = json.loads(content)
    records = []
    count = 0
    for resource_index, resource_span in enumerate(document.get("resourceSpans", [])):
        attributes = {
            item["key"]: item.get("value", {}).get("stringValue")
            for item in resource_span.get("resource", {}).get("attributes", [])
        }
        for scope_index, scope_span in enumerate(resource_span.get("scopeSpans", [])):
            for span_index, span in enumerate(scope_span.get("spans", [])):
                count += 1
                if count > 1000:
                    raise ValueError("OTLP batch exceeds 1000 spans")
                locator = f"resourceSpans/{resource_index}/scopeSpans/{scope_index}/spans/{span_index}"
                raw = json.dumps(span, ensure_ascii=False, separators=(",", ":"))
                record_source_id = attributes.get("miniapm.source_id") or source_id
                try:
                    trace_id, span_id = span["traceId"], span["spanId"]
                    if not re.fullmatch(r"[0-9a-f]{32}", trace_id) or trace_id == "0" * 32:
                        raise ValueError("invalid trace_id")
                    if not re.fullmatch(r"[0-9a-f]{16}", span_id) or span_id == "0" * 16:
                        raise ValueError("invalid span_id")
                    started = _nano_time(span["startTimeUnixNano"])
                    ended = _nano_time(span["endTimeUnixNano"])
                    identity = resolve_identity(dataset_id, environment, record_source_id, started)
                    if identity["status"] != "matched":
                        raise ValueError(f"identity_{identity['status']}")
                    native_service = attributes.get("service.name")
                    if native_service and native_service != identity["service"]:
                        raise ValueError("raw service identity conflicts with identity mapping")
                    status_code = span.get("status", {}).get("code", 0)
                    event = SpanEvent(
                        event_id=uuid5(dataset_id, f"otel:{trace_id}:{span_id}"),
                        event_time=started,
                        ingested_at=datetime.now(UTC),
                        environment=environment,
                        dataset_id=dataset_id,
                        service=identity["service"],
                        instance=identity["instance"],
                        version=identity["version"],
                        route=span.get("name") or "unknown",
                        source_kind="simulated",
                        trace_id=trace_id,
                        span_id=span_id,
                        parent_span_id=span.get("parentSpanId") or None,
                        kind=rule.kind_map.get(str(span.get("kind", 1)), "internal"),
                        started_at=started,
                        ended_at=ended,
                        status="error" if status_code == 2 else "ok",
                    )
                    records.append(AdaptedRecord(locator=locator, event=event))
                except Exception as exc:
                    records.append(AdaptedRecord(locator=locator, pending=pending(
                        dataset_id, environment, "otlp", record_source_id, batch_id, locator, raw,
                        str(exc), rule.version,
                    )))
    return records


def adapt_prometheus(
    content: str,
    rule: AdaptationRule,
    dataset_id: UUID,
    environment: str,
    source_id: str,
    batch_id: str,
    resolve_identity,
    event_time: datetime,
) -> list[AdaptedRecord]:
    identity = resolve_identity(dataset_id, environment, source_id, event_time)
    if identity["status"] != "matched":
        return [AdaptedRecord(locator="snapshot", pending=pending(
            dataset_id, environment, "prometheus", source_id, batch_id, "snapshot", content,
            f"identity_{identity['status']}", rule.version,
        ))]
    families = list(text_string_to_metric_families(content))
    records = []
    for family in families:
        if family.type not in {"counter", "gauge", "histogram"}:
            records.append(AdaptedRecord(locator=f"metric:{family.name}", pending=pending(
                dataset_id, environment, "prometheus", source_id, batch_id,
                f"metric:{family.name}", content, f"unsupported metric type {family.type}", rule.version,
            )))
            continue
        if family.name != rule.source_metric:
            records.append(AdaptedRecord(locator=f"metric:{family.name}", pending=pending(
                dataset_id, environment, "prometheus", source_id, batch_id,
                f"metric:{family.name}", content, "unknown metric name or semantics", rule.version,
            )))
            continue
        samples = []
        for sample in family.samples:
            value = sample.value
            if family.type == "histogram" and sample.name.endswith("_bucket"):
                boundary = sample.labels["le"]
                if boundary != "+Inf":
                    boundary = str(float(boundary) / 1000)
                sample = sample._replace(labels={**sample.labels, "le": boundary})
            elif family.type == "histogram" and sample.name.endswith("_sum"):
                value /= 1000
            samples.append({"name": sample.name.replace(rule.source_metric, rule.metric_name),
                            "labels": {k: v for k, v in sample.labels.items() if k != "source_id"},
                            "value": value})
        records.append(AdaptedRecord(locator=f"metric:{family.name}", metric={
            "name": rule.metric_name, "type": family.type, "samples": samples,
            "dataset_id": str(dataset_id), "environment": environment, **identity,
        }))
    return records
