from datetime import UTC, datetime
from typing import Literal

FIXTURE_START = datetime(2026, 11, 9, 0, 0, tzinfo=UTC)
FIXTURE_END = datetime(2026, 11, 9, 0, 15, tzinfo=UTC)
FIXTURE_TRACE_ID = "1af0d3a32f344a7a9182f52db19f8301"
ERROR_TRACE_ID = "2bf0d3a32f344a7a9182f52db19f8302"
DatasetName = Literal["slow-downstream", "version-errors", "empty", "tool-error"]

SLOW_TRACES = [
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

SLOW_LOGS = [
    {
        "trace_id": FIXTURE_TRACE_ID,
        "span_id": "4d2b7b4f7d394d01",
        "service": "order",
        "version": "v1",
        "severity": "INFO",
        "message": "order completed slowly",
        "error_code": None,
        "duration_seconds": 1.74,
    },
    {
        "trace_id": FIXTURE_TRACE_ID,
        "span_id": "7f2d767f631a4c85",
        "service": "inventory",
        "version": "v1",
        "severity": "INFO",
        "message": "inventory reserve completed slowly",
        "error_code": None,
        "duration_seconds": 1.61,
    },
]

ERROR_TRACES = [
    {
        "trace_id": ERROR_TRACE_ID,
        "root_service": "order",
        "route": "POST /orders",
        "duration_seconds": 0.31,
        "spans": [
            {
                "span_id": "8d2b7b4f7d394d01",
                "parent_span_id": None,
                "service": "order",
                "kind": "server",
                "duration_seconds": 0.31,
                "status": "error",
            }
        ],
    }
]

ERROR_LOGS = [
    {
        "trace_id": ERROR_TRACE_ID,
        "span_id": "8d2b7b4f7d394d01",
        "service": "order",
        "version": "v2",
        "severity": "ERROR",
        "message": "order rejected while validating inventory reservation",
        "error_code": "INVENTORY_RESERVATION_INVALID",
    },
    {
        "trace_id": FIXTURE_TRACE_ID,
        "span_id": "4d2b7b4f7d394d01",
        "service": "order",
        "version": "v1",
        "severity": "INFO",
        "message": "order completed",
        "error_code": None,
    },
]

FIXTURES = {
    "slow-downstream": {
        "metrics": {
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
        },
        "traces": SLOW_TRACES,
        "logs": SLOW_LOGS,
    },
    "version-errors": {
        "metrics": {
            "order": {
                "request_count": 1200,
                "error_count": 144,
                "p95_seconds": 0.4,
                "previous_p95_seconds": 0.2,
                "by_version": {
                    "v1": {"request_count": 800, "error_count": 8},
                    "v2": {"request_count": 400, "error_count": 136},
                },
            },
            "inventory": {
                "request_count": 1180,
                "error_count": 3,
                "p95_seconds": 0.12,
                "previous_p95_seconds": 0.12,
            },
        },
        "traces": ERROR_TRACES,
        "logs": ERROR_LOGS,
    },
    "empty": {"metrics": {}, "traces": [], "logs": []},
    "tool-error": {"metrics": {}, "traces": [], "logs": []},
}
