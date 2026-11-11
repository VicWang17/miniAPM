import asyncio
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from miniapm.alerts import AlertmanagerAlert, record_firing_alert
from miniapm.correlation import parse_correlation_result, validate_correlation_result
from miniapm.heterogeneous import HeterogeneousPipeline
from miniapm.investigation import SharedInvestigationBudget
from miniapm.multiagent import arun_multiagent_investigation
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
        self._schedule(investigation_id, question, dataset_id, service, window, model)
        return investigation_id

    async def submit_group(self, group_id: str, model=None) -> str:
        group = self.context.candidate_group(group_id)
        if group is None:
            raise LookupError("candidate group not found")
        if self.pending_count >= self.max_queue + self.max_concurrent:
            raise InvestigationQueueFull("investigation capacity exhausted")
        investigation_id = str(uuid4())
        anchor = next(member for member in group.members if member.relation == "anchor")
        question = (
            "Jointly investigate the candidate alert group; candidate grouping is not proof of shared root cause.\n"
            f"Group snapshot: {group.model_dump_json()}\n"
            "Return only JSON with members[{alert_id,judgment,evidence_refs,rationale}], "
            "root_cause_candidates[{service,summary,evidence_refs,counter_evidence}], observed_impact, "
            "rejected_explanations, missing_information. Every field must be an array; use [] when empty; "
            "counter_evidence and observed_impact are arrays of strings, never scalar strings. "
            "evidence_refs must contain only exact top-level subagent names actually called, such as telemetry_investigation; never tool names, dotted names, or services. "
            "Use supported only with current evidence; use insufficient when traces or causal evidence are missing. "
            "For HighLatency alerts, a shared trace with slow inventory spans inside slower order spans supports propagation without errors or a historical baseline. "
            "That propagation does not prove inventory's internal root cause; keep that root unknown without direct evidence. "
            "When evidence is insufficient, use root_cause_candidates=[]; do not create speculative or 'unknown' root entries. "
            "Observed impact must be measured, not merely possible through topology."
        )
        self.context.create_investigation(investigation_id, str(group.dataset_id), question,
            anchor.service, group.window, status="queued")
        self.context.attach_group_investigation(group_id, investigation_id)
        self._schedule(investigation_id, question, str(group.dataset_id), anchor.service, group.window, model, group)
        return investigation_id

    async def submit_alert(
        self, alert: AlertmanagerAlert, default_dataset_id: UUID, model=None
    ) -> dict:
        record = record_firing_alert(self.context, alert, default_dataset_id)
        if record["result"] == "duplicate":
            return record
        investigation_id = record["investigation_id"]
        if self.pending_count >= self.max_queue + self.max_concurrent:
            self.context.update_investigation_status(
                investigation_id, "failed", "investigation capacity exhausted"
            )
            return {**record, "result": "capacity_exhausted"}
        service = alert.labels.get("service", "order")
        dataset_id = alert.labels.get("dataset_id", str(default_dataset_id))
        summary = alert.annotations.get("summary") or alert.labels.get("alertname", "APM alert")
        window = TimeWindow(
            start=alert.starts_at, end=alert.starts_at + timedelta(minutes=15)
        )
        self._schedule(
            investigation_id, f"Alert: {summary}", dataset_id, service, window, model
        )
        return record

    def _schedule(self, investigation_id, question, dataset_id, service, window, model, group=None) -> None:
        task = asyncio.create_task(
            self._run(investigation_id, question, dataset_id, service, window, model, group)
        )
        self.tasks[investigation_id] = task
        task.add_done_callback(lambda _: self.tasks.pop(investigation_id, None))

    async def _run(
        self,
        investigation_id: str,
        question: str,
        dataset_id: str,
        service: ServiceName,
        window: TimeWindow,
        model,
        group=None,
    ) -> None:
        backend = self.backend_factory(dataset_id)
        try:
            async with self.semaphore:
                self.context.update_investigation_status(investigation_id, "running")
                result = await arun_multiagent_investigation(
                    question,
                    service,
                    window.start,
                    window.end,
                    model=model,
                    backend=backend,
                    deployment_context=self.context,
                    dataset_id=dataset_id,
                    adaptation_pipeline=HeterogeneousPipeline(self.context),
                    shared_budget=SharedInvestigationBudget(max_tool_calls=20) if group else None,
                )
                update = {"investigation_id": UUID(investigation_id)}
                if group is not None:
                    correlation = parse_correlation_result(result["final_answer"])
                    available_refs = {run.agent_name for run in result["report"].subagent_runs}
                    validate_correlation_result(correlation, group, available_refs)
                    update.update(correlation_group_id=group.group_id, correlation=correlation)
                report = result["report"].model_copy(update=update)
                self.context.connection.ping(reconnect=True)
                self.context.complete_investigation(report)
        except asyncio.CancelledError:
            self.context.update_investigation_status(
                investigation_id, "interrupted", "application stopped"
            )
            raise
        except Exception as exc:
            self.context.connection.ping(reconnect=True)
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
