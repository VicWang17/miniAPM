import json
import math
from datetime import datetime
from pathlib import Path
from typing import Iterator, TypeVar

from miniapm.models import LogEvent, RequestEvent, SpanEvent, TimeWindow, ToolResult

EventT = TypeVar("EventT", RequestEvent, SpanEvent, LogEvent)


class FileBackend:
    def __init__(self, dataset_directory: Path) -> None:
        self.observations = dataset_directory / "observations"

    def _read(
        self,
        filename: str,
        model: type[EventT],
        window: TimeWindow,
    ) -> Iterator[EventT]:
        seen = set()
        with (self.observations / filename).open(encoding="utf-8") as source:
            for line in source:
                event = model.model_validate_json(line)
                if event.event_id in seen:
                    continue
                seen.add(event.event_id)
                if window.start <= event.event_time < window.end:
                    yield event

    def _result(self, window: TimeWindow, data: object) -> str:
        return ToolResult[object](
            status="ok" if data else "empty",
            window=window,
            source_kind="simulated",
            source_name="jsonl",
            data=data,
            warnings=["FILE: generated historical observations; no ground truth loaded"],
        ).model_dump_json()

    def query_metrics(self, service: str, start: datetime, end: datetime) -> str:
        window = TimeWindow(start=start, end=end)
        if service == "order":
            events = list(self._read("requests.jsonl", RequestEvent, window))
            values = [
                {
                    "version": event.version,
                    "status": event.status,
                    "duration_seconds": event.duration_seconds,
                }
                for event in events
            ]
        else:
            spans = self._read("spans.jsonl", SpanEvent, window)
            values = [
                {
                    "version": span.version,
                    "status": span.status,
                    "duration_seconds": (span.ended_at - span.started_at).total_seconds(),
                }
                for span in spans
                if span.service == service and span.kind == "server"
            ]
        if not values:
            return self._result(window, None)
        durations = sorted(value["duration_seconds"] for value in values)
        versions = {}
        for value in values:
            group = versions.setdefault(value["version"], {"request_count": 0, "error_count": 0})
            group["request_count"] += 1
            group["error_count"] += value["status"] == "error"
        data = {
            "request_count": len(values),
            "error_count": sum(value["status"] == "error" for value in values),
            "p95_seconds": durations[math.ceil(0.95 * len(durations)) - 1],
            "by_version": versions,
        }
        return self._result(window, data)

    def query_traces(
        self, service: str, start: datetime, end: datetime, limit: int
    ) -> str:
        window = TimeWindow(start=start, end=end)
        grouped = {}
        for span in self._read("spans.jsonl", SpanEvent, window):
            grouped.setdefault(span.trace_id, []).append(span)
        traces = []
        for trace_id, spans in grouped.items():
            if service not in {span.service for span in spans}:
                continue
            ordered = sorted(spans, key=lambda span: span.started_at)
            root = next((span for span in ordered if span.parent_span_id is None), ordered[0])
            traces.append(
                {
                    "trace_id": trace_id,
                    "root_service": root.service,
                    "route": root.route,
                    "duration_seconds": (root.ended_at - root.started_at).total_seconds(),
                    "spans": [
                        {
                            "span_id": span.span_id,
                            "parent_span_id": span.parent_span_id,
                            "service": span.service,
                            "instance": span.instance,
                            "version": span.version,
                            "kind": span.kind,
                            "duration_seconds": (span.ended_at - span.started_at).total_seconds(),
                            "status": span.status,
                        }
                        for span in ordered
                    ],
                }
            )
        traces.sort(key=lambda trace: trace["duration_seconds"], reverse=True)
        return self._result(window, traces[:limit])

    def query_logs(
        self,
        service: str,
        start: datetime,
        end: datetime,
        trace_id: str | None,
        version: str | None,
        error_code: str | None,
        limit: int,
    ) -> str:
        window = TimeWindow(start=start, end=end)
        logs = []
        for log in self._read("logs.jsonl", LogEvent, window):
            if log.service != service:
                continue
            if trace_id is not None and log.trace_id != trace_id:
                continue
            if version is not None and log.version != version:
                continue
            if error_code is not None and log.error_code != error_code:
                continue
            logs.append(log.model_dump(mode="json"))
            if len(logs) >= limit:
                break
        return self._result(window, logs)
