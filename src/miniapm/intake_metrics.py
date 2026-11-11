from datetime import UTC, datetime

from prometheus_client import CollectorRegistry, Counter, Histogram


class IntakeMetrics:
    def __init__(self, worker_id: str, registry: CollectorRegistry | None = None) -> None:
        self.worker_id = worker_id
        self.registry = registry or CollectorRegistry()
        self.received = Counter(
            "miniapm_intake_received_total",
            "Telemetry messages received by consumers",
            ("worker_id", "queue"),
            registry=self.registry,
        )
        self.acknowledged = Counter(
            "miniapm_intake_acknowledged_total",
            "Telemetry messages acknowledged after ClickHouse writes",
            ("worker_id", "queue"),
            registry=self.registry,
        )
        self.write_duration = Histogram(
            "miniapm_clickhouse_write_duration_seconds",
            "ClickHouse telemetry insert duration",
            ("worker_id", "table"),
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
            registry=self.registry,
        )
        self.ingestion_delay = Histogram(
            "miniapm_ingestion_delay_seconds",
            "Delay from telemetry event time to consumer receipt",
            ("worker_id", "queue"),
            buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 3600, 14400, 43200, 86400),
            registry=self.registry,
        )

    def observe_received(self, queue: str, event_time: datetime) -> None:
        labels = {"worker_id": self.worker_id, "queue": queue}
        self.received.labels(**labels).inc()
        delay = (datetime.now(UTC) - event_time).total_seconds()
        self.ingestion_delay.labels(**labels).observe(max(0, delay))

    def observe_acknowledged(self, queue: str, count: int) -> None:
        self.acknowledged.labels(worker_id=self.worker_id, queue=queue).inc(count)

    def observe_write(self, table: str, duration_seconds: float) -> None:
        self.write_duration.labels(worker_id=self.worker_id, table=table).observe(
            duration_seconds
        )
