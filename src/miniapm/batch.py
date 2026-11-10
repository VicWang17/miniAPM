import random
from datetime import datetime, timedelta
from typing import Iterator

from pydantic import Field

from miniapm.generation import RequestEvents, RequestParameters, RequestTruth, generate_request
from miniapm.models import ContractModel
from miniapm.topology import Topology, build_request_plan


class BatchGenerationSettings(ContractModel):
    seed: int = 42
    batch_size: int = Field(default=100, ge=1, le=10000)
    error_rate: float = Field(default=0.01, ge=0, le=1)
    slow_rate: float = Field(default=0.05, ge=0, le=1)
    missing_rate: float = Field(default=0.02, ge=0, le=1)
    slow_inventory_seconds: float = Field(default=1.6, gt=0, le=60)


class GenerationStats(ContractModel):
    request_count: int = 0
    span_count: int = 0
    log_count: int = 0
    byte_count: int = 0

    def add(self, other: "GenerationStats") -> "GenerationStats":
        return GenerationStats(
            request_count=self.request_count + other.request_count,
            span_count=self.span_count + other.span_count,
            log_count=self.log_count + other.log_count,
            byte_count=self.byte_count + other.byte_count,
        )


class RequestBatch(ContractModel):
    batch_number: int
    events: tuple[RequestEvents, ...]
    truths: tuple[RequestTruth, ...]
    stats: GenerationStats


def summarize_events(events: tuple[RequestEvents, ...]) -> GenerationStats:
    stats = GenerationStats(request_count=len(events))
    for item in events:
        serialized_size = len(item.request.model_dump_json().encode("utf-8"))
        serialized_size += sum(len(span.model_dump_json().encode("utf-8")) for span in item.spans)
        serialized_size += sum(len(log.model_dump_json().encode("utf-8")) for log in item.logs)
        stats = stats.add(
            GenerationStats(
                span_count=len(item.spans),
                log_count=len(item.logs),
                byte_count=serialized_size,
            )
        )
    return stats


def iter_request_batches(
    topology: Topology,
    request_count: int,
    start: datetime,
    interval: timedelta = timedelta(seconds=1),
    settings: BatchGenerationSettings | None = None,
) -> Iterator[RequestBatch]:
    if request_count < 0:
        raise ValueError("request_count must be non-negative")
    settings = settings or BatchGenerationSettings()
    random_source = random.Random(settings.seed)

    for batch_number, offset in enumerate(range(0, request_count, settings.batch_size)):
        size = min(settings.batch_size, request_count - offset)
        events = []
        truths = []
        for request_number in range(offset, offset + size):
            plan = build_request_plan(
                topology,
                request_number,
                start + request_number * interval,
            )
            parameters = RequestParameters(
                inventory_duration_seconds=(
                    settings.slow_inventory_seconds
                    if random_source.random() < settings.slow_rate
                    else 0.08
                ),
                error=random_source.random() < settings.error_rate,
                missing_inventory=random_source.random() < settings.missing_rate,
            )
            request_events, truth = generate_request(plan, parameters)
            events.append(request_events)
            truths.append(truth)
        batch_events = tuple(events)
        yield RequestBatch(
            batch_number=batch_number,
            events=batch_events,
            truths=tuple(truths),
            stats=summarize_events(batch_events),
        )
