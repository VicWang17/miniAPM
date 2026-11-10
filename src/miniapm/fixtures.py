from datetime import UTC, datetime

FIXTURE_START = datetime(2026, 11, 9, 0, 0, tzinfo=UTC)
FIXTURE_END = datetime(2026, 11, 9, 0, 15, tzinfo=UTC)
FIXTURE_TRACE_ID = "1af0d3a32f344a7a9182f52db19f8301"

METRICS = {
    "order": {
        "request_count": 1200,
        "error_count": 8,
        "p95_seconds": 1.82,
        "previous_p95_seconds": 0.24,
    },
    "inventory": {
        "request_count": 1180,
        "error_count": 3,
        "p95_seconds": 1.55,
        "previous_p95_seconds": 0.12,
    },
}

TRACES = [
    {
        "trace_id": FIXTURE_TRACE_ID,
        "root_service": "order",
        "route": "POST /orders",
        "duration_seconds": 1.74,
        "spans": [
            {
                "span_id": "4d2b7b4f7d394d01",
                "parent_span_id": None,
                "service": "order",
                "kind": "server",
                "duration_seconds": 1.74,
            },
            {
                "span_id": "7f2d767f631a4c85",
                "parent_span_id": "4d2b7b4f7d394d01",
                "service": "inventory",
                "kind": "client",
                "duration_seconds": 1.61,
            },
        ],
    }
]

LOGS = [
    {
        "trace_id": FIXTURE_TRACE_ID,
        "span_id": "4d2b7b4f7d394d01",
        "service": "order",
        "severity": "INFO",
        "message": "order completed slowly",
        "duration_seconds": 1.74,
    },
    {
        "trace_id": FIXTURE_TRACE_ID,
        "span_id": "7f2d767f631a4c85",
        "service": "inventory",
        "severity": "INFO",
        "message": "inventory reserve completed slowly",
        "duration_seconds": 1.61,
    },
]
