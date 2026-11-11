import asyncio
from datetime import datetime
from uuid import UUID, uuid4

from miniapm.investigation import arun_investigation
from miniapm.models import ServiceName, TimeWindow
from miniapm.mysql_context import MySQLContext


class InvestigationQueueFull(Exception):
    pass


class InvestigationManager:
    def __init__(
        self,
        context: MySQLContext,
        backend_factory,
        max_concurrent: int = 2,
        max_queue: int = 10,
    ) -> None:
        self.context = context
        self.backend_factory = backend_factory
        self.max_concurrent = max_concurrent
        self.max_queue = max_queue
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.tasks: dict[str, asyncio.Task] = {}

    @property
    def pending_count(self) -> int:
        return len(self.tasks)

    async def submit(
        self,
        question: str,
        dataset_id: str,
        service: ServiceName,
        start: datetime,
        end: datetime,
        model=None,
    ) -> str:
        if self.pending_count >= self.max_queue + self.max_concurrent:
            raise InvestigationQueueFull("investigation capacity exhausted")
        investigation_id = str(uuid4())
        window = TimeWindow(start=start, end=end)
        self.context.create_investigation(
            investigation_id, dataset_id, question, service, window, status="queued"
        )
        task = asyncio.create_task(
            self._run(investigation_id, question, dataset_id, service, window, model)
        )
        self.tasks[investigation_id] = task
        task.add_done_callback(lambda _: self.tasks.pop(investigation_id, None))
        return investigation_id

    async def _run(
        self,
        investigation_id: str,
        question: str,
        dataset_id: str,
        service: ServiceName,
        window: TimeWindow,
        model,
    ) -> None:
        backend = self.backend_factory(dataset_id)
        try:
            async with self.semaphore:
                self.context.update_investigation_status(investigation_id, "running")
                result = await arun_investigation(
                    question,
                    service,
                    window.start,
                    window.end,
                    model=model,
                    backend=backend,
                    deployment_context=self.context,
                    dataset_id=dataset_id,
                )
                report = result["report"].model_copy(
                    update={"investigation_id": UUID(investigation_id)}
                )
                self.context.complete_investigation(report)
        except asyncio.CancelledError:
            self.context.update_investigation_status(
                investigation_id, "interrupted", "application stopped"
            )
            raise
        except Exception as exc:
            self.context.update_investigation_status(
                investigation_id, "failed", f"{type(exc).__name__}: {exc}"[:1000]
            )
        finally:
            close = getattr(backend, "close", None)
            if close:
                close()

    async def wait(self, investigation_id: str) -> None:
        task = self.tasks.get(investigation_id)
        if task is not None:
            await task

    async def close(self) -> None:
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.context.mark_interrupted()
