import asyncio
from datetime import UTC, datetime
from typing import Callable, Protocol

from pydantic import Field

from miniapm.generation import RequestEvents, RequestParameters, generate_request
from miniapm.models import ContractModel, ServiceName
from miniapm.topology import Topology, build_request_plan


class RealtimeSettings(ContractModel):
    requests_per_second: float = Field(default=2, gt=0, le=100)
    max_in_flight: int = Field(default=100, ge=1, le=1000)
    shutdown_timeout_seconds: float = Field(default=10, gt=0, le=60)


class ResourceSample(ContractModel):
    event_time: datetime
    service: ServiceName
    instance: str
    version: str
    metric: str = "simulated_cpu_percent"
    value: float = Field(ge=0, le=100)
    source_kind: str = "simulated"


class RealtimePublisher(Protocol):
    async def publish_request(self, events: RequestEvents) -> None: ...

    async def publish_resource(self, sample: ResourceSample) -> None: ...


class RecordingPublisher:
    def __init__(self, request_capacity: int) -> None:
        self.request_capacity = request_capacity
        self.requests: list[RequestEvents] = []
        self.resources: list[ResourceSample] = []

    async def publish_request(self, events: RequestEvents) -> None:
        if len(self.requests) >= self.request_capacity:
            raise OverflowError("recording publisher capacity exhausted")
        self.requests.append(events)

    async def publish_resource(self, sample: ResourceSample) -> None:
        self.resources.append(sample)


class RealtimeSummary(ContractModel):
    requested_count: int
    completed_count: int
    failed_count: int
    resource_sample_count: int
    elapsed_seconds: float


def _at_ingestion(events: RequestEvents, ingested_at: datetime) -> RequestEvents:
    return RequestEvents(
        request=events.request.model_copy(update={"ingested_at": ingested_at}),
        spans=tuple(
            span.model_copy(update={"ingested_at": ingested_at}) for span in events.spans
        ),
        logs=tuple(
            log.model_copy(update={"ingested_at": ingested_at}) for log in events.logs
        ),
    )


async def run_realtime(
    topology: Topology,
    request_count: int,
    publisher: RealtimePublisher,
    settings: RealtimeSettings | None = None,
    parameters_for_request: Callable[[int], RequestParameters] | None = None,
) -> RealtimeSummary:
    if request_count < 0:
        raise ValueError("request_count must be non-negative")
    settings = settings or RealtimeSettings()
    parameters_for_request = parameters_for_request or (lambda _: RequestParameters())
    semaphore = asyncio.Semaphore(settings.max_in_flight)
    tasks: set[asyncio.Task[bool]] = set()
    outcomes: list[bool] = []
    loop = asyncio.get_running_loop()
    started = loop.time()

    async def run_request(request_number: int) -> bool:
        async with semaphore:
            request_started_at = datetime.now(UTC)
            plan = build_request_plan(topology, request_number, request_started_at)
            parameters = parameters_for_request(request_number)
            events, _ = generate_request(plan, parameters)
            inventory = plan.spans[2]
            is_slow = parameters.inventory_duration_seconds > 0.08
            await publisher.publish_resource(
                ResourceSample(
                    event_time=request_started_at,
                    service="inventory",
                    instance=inventory.instance,
                    version=inventory.version,
                    value=85 if is_slow else 20,
                )
            )
            await asyncio.sleep(events.request.duration_seconds)
            finished_at = datetime.now(UTC)
            await publisher.publish_request(_at_ingestion(events, finished_at))
            await publisher.publish_resource(
                ResourceSample(
                    event_time=finished_at,
                    service="inventory",
                    instance=inventory.instance,
                    version=inventory.version,
                    value=20,
                )
            )
            return True

    for request_number in range(request_count):
        if request_number:
            await asyncio.sleep(1 / settings.requests_per_second)
        task = asyncio.create_task(run_request(request_number))
        tasks.add(task)

        def record_result(completed_task: asyncio.Task[bool]) -> None:
            tasks.discard(completed_task)
            try:
                outcomes.append(completed_task.result())
            except BaseException:
                outcomes.append(False)

        task.add_done_callback(record_result)

    if tasks:
        _, pending = await asyncio.wait(tasks, timeout=settings.shutdown_timeout_seconds)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    await asyncio.sleep(0)
    completed = sum(outcomes)
    failed = request_count - completed
    return RealtimeSummary(
        requested_count=request_count,
        completed_count=completed,
        failed_count=failed,
        resource_sample_count=completed * 2,
        elapsed_seconds=round(loop.time() - started, 3),
    )
