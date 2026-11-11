import asyncio
import json
import time
from typing import Protocol

import httpx

from miniapm.intake_metrics import IntakeMetrics
from miniapm.models import LogEvent, SpanEvent


class TelemetryWriter(Protocol):
    async def add_log(self, event: LogEvent) -> None: ...

    async def add_span(self, event: SpanEvent) -> None: ...

    async def flush(self) -> None: ...

    async def close(self) -> None: ...


class ClickHouseHttpClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8123") -> None:
        self.client = httpx.Client(base_url=base_url, timeout=5, trust_env=False)

    def insert(self, table: str, rows: list[dict]) -> None:
        if table not in {"miniapm.logs", "miniapm.spans"}:
            raise ValueError("unsupported table")
        if not rows:
            return
        payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
        response = self.client.post(
            "/",
            params={
                "query": f"INSERT INTO {table} FORMAT JSONEachRow",
                "date_time_input_format": "best_effort",
            },
            content=payload,
        )
        response.raise_for_status()

    def close(self) -> None:
        self.client.close()


class BatchingClickHouseWriter:
    def __init__(
        self,
        client: ClickHouseHttpClient | None = None,
        batch_size: int = 500,
        flush_interval_seconds: float = 1,
        max_pending_events: int = 2000,
        metrics: IntakeMetrics | None = None,
    ) -> None:
        if batch_size < 1 or max_pending_events < batch_size:
            raise ValueError("invalid batch bounds")
        self.client = client or ClickHouseHttpClient()
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.max_pending_events = max_pending_events
        self.metrics = metrics
        self.buffers = {"miniapm.logs": [], "miniapm.spans": []}
        self.lock = asyncio.Lock()
        self.flush_task = None
        self.failure = None

    async def __aenter__(self) -> "BatchingClickHouseWriter":
        self.flush_task = asyncio.create_task(self._flush_loop())
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    def _check_failure(self) -> None:
        if self.failure is not None:
            raise RuntimeError("batch writer is in a failed state") from self.failure

    def _pending_count(self) -> int:
        return sum(len(rows) for rows in self.buffers.values())

    async def _insert(self, table: str, rows: list[dict]) -> None:
        started = time.monotonic()
        try:
            await asyncio.to_thread(self.client.insert, table, rows)
        finally:
            if self.metrics is not None:
                self.metrics.observe_write(table, time.monotonic() - started)

    async def _flush_locked(self) -> None:
        pending = {table: rows for table, rows in self.buffers.items() if rows}
        self.buffers = {"miniapm.logs": [], "miniapm.spans": []}
        try:
            for table, rows in pending.items():
                await self._insert(table, rows)
        except Exception as exc:
            self.failure = exc
            for table, rows in pending.items():
                self.buffers[table] = rows + self.buffers[table]
            raise

    async def add(self, table: str, event: LogEvent | SpanEvent) -> None:
        async with self.lock:
            self._check_failure()
            row = event.model_dump(mode="json")
            if table == "miniapm.logs":
                row["trace_id"] = row["trace_id"] or ""
                row["span_id"] = row["span_id"] or ""
            self.buffers[table].append(row)
            if len(self.buffers[table]) >= self.batch_size or self._pending_count() > self.max_pending_events:
                await self._flush_locked()

    async def add_log(self, event: LogEvent) -> None:
        await self.add("miniapm.logs", event)

    async def add_span(self, event: SpanEvent) -> None:
        await self.add("miniapm.spans", event)

    async def flush(self) -> None:
        async with self.lock:
            self._check_failure()
            await self._flush_locked()

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self.flush_interval_seconds)
            try:
                await self.flush()
            except Exception:
                return

    async def close(self) -> None:
        if self.flush_task is not None:
            self.flush_task.cancel()
            await asyncio.gather(self.flush_task, return_exceptions=True)
        if self.failure is None:
            await self.flush()
        self.client.close()
