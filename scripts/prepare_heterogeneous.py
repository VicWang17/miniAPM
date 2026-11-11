import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from miniapm.heterogeneous import HeterogeneousContext
from miniapm.topology import default_topology

DATASET_ID = UUID("018f2f70-0000-7000-8000-000000000901")
START = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
END = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)


def span(trace_id, span_id, parent, name, seconds, kind=2):
    started = int((START.timestamp() + seconds) * 1_000_000_000)
    return {"traceId": trace_id, "spanId": span_id, **({"parentSpanId": parent} if parent else {}),
            "name": name, "kind": kind, "startTimeUnixNano": str(started),
            "endTimeUnixNano": str(started + 100_000_000), "status": {"code": 0}}


def main():
    root = Path("data/hetero")
    raw, truth = root / "raw", root / "truth"
    raw.mkdir(parents=True, exist_ok=True)
    truth.mkdir(parents=True, exist_ok=True)
    prefix = '127.0.0.1 - - [{time}] "POST /orders HTTP/1.1" 200'
    good = prefix.format(time="10/Nov/2026:12:00:00 +0000") + " rt=0.125 svc=edge-order\n" + \
        prefix.format(time="11/Nov/2026:12:00:00 +0000") + " rt=0.250 svc=edge-order\n"
    unknown = prefix.format(time="10/Nov/2026:12:00:00 +0000") + " duration_ms=125 svc=edge-order\n"
    (raw / "nginx.log").write_text(good + unknown)
    (raw / "nginx-unknown.log").write_text(unknown)
    trace_id = "1" * 32
    document = {"resourceSpans": [
        {"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "order"}},
            {"key": "miniapm.source_id", "value": {"stringValue": "edge-order"}}]},
         "scopeSpans": [{"spans": [span(trace_id, "1" * 16, None, "POST /orders", 0)]}]},
        {"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "inventory"}},
            {"key": "miniapm.source_id", "value": {"stringValue": "edge-inventory"}}]},
         "scopeSpans": [{"spans": [span(trace_id, "2" * 16, "1" * 16, "reserve", 0.1)]}]},
    ]}
    (raw / "otlp.json").write_text(json.dumps(document))
    invalid = {"resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [{"spans": [
        span("0" * 32, "3" * 16, None, "invalid", 0)]}]}]}
    (raw / "otlp-invalid.json").write_text(json.dumps(invalid))
    metrics = """# HELP http_request_duration_milliseconds request duration
# TYPE http_request_duration_milliseconds histogram
http_request_duration_milliseconds_bucket{source_id=\"prom-source\",le=\"100\"} 1
http_request_duration_milliseconds_bucket{source_id=\"prom-source\",le=\"500\"} 2
http_request_duration_milliseconds_bucket{source_id=\"prom-source\",le=\"+Inf\"} 2
http_request_duration_milliseconds_sum{source_id=\"prom-source\"} 600
http_request_duration_milliseconds_count{source_id=\"prom-source\"} 2
"""
    (raw / "metrics.prom").write_text(metrics)
    (truth / "expected.json").write_text(json.dumps({
        "dataset_id": str(DATASET_ID), "nginx": {"passed": 2, "pending": 1,
            "durations_seconds": [0.125, 0.25], "versions": ["v1", "v2"]},
        "otlp": {"passed": 2, "trace_id": trace_id, "services": ["order", "inventory"]},
        "prometheus": {"buckets_seconds": [0.1, 0.5], "sum_seconds": 0.6, "count": 2},
        "invalid": {"expected_pending": ["unknown nginx log_format", "invalid trace_id"]},
    }, indent=2))
    context = HeterogeneousContext()
    try:
        schema = Path("deploy/mysql/init/002-create-adaptation-tables.sql").read_text()
        with context.connection.cursor() as cursor:
            for statement in schema.split(";"):
                if statement.strip():
                    cursor.execute(statement)
        context.connection.commit()
        context.import_context(default_topology(DATASET_ID), [])
        mappings = [
            ("edge-order", "order", "order-nginx-1", "v1", START, END),
            ("edge-order", "order", "order-nginx-2", "v2", END, None),
            ("edge-inventory", "inventory", "inventory-otel-1", "v1", START, None),
            ("prom-source", "order", "order-prom-1", "v1", START, None),
        ]
        with context.connection.cursor() as cursor:
            cursor.execute("DELETE FROM pending_records WHERE dataset_id=%s", (str(DATASET_ID),))
            cursor.execute("DELETE FROM identity_mappings WHERE dataset_id=%s", (str(DATASET_ID),))
        context.connection.commit()
        for source_id, service, instance, version, valid_from, valid_to in mappings:
            context.import_identity(DATASET_ID, "hetero", source_id, service, instance, version, valid_from, valid_to)
        print(json.dumps({"dataset_id": str(DATASET_ID), "raw": str(raw), "truth": str(truth),
                          "identity_mappings": len(mappings)}))
    finally:
        context.close()


if __name__ == "__main__":
    main()
