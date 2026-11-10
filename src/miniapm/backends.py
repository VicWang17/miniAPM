from datetime import datetime
from typing import Protocol

from miniapm.fixtures import DatasetName, FIXTURE_END, FIXTURE_START, FIXTURES
from miniapm.models import TimeWindow, ToolResult

FIXTURE_WARNING = "FIXTURE: fixed sample data, not a live monitoring backend"


class TelemetryBackend(Protocol):
    def query_metrics(self, service: str, start: datetime, end: datetime) -> str: ...

    def query_traces(
        self, service: str, start: datetime, end: datetime, limit: int
    ) -> str: ...

    def query_logs(
        self,
        service: str,
        start: datetime,
        end: datetime,
        trace_id: str | None,
        version: str | None,
        error_code: str | None,
        limit: int,
    ) -> str: ...


class FixtureBackend:
    def __init__(self, dataset: DatasetName = "slow-downstream") -> None:
        self.dataset = dataset
        self.data = FIXTURES[dataset]

    def _window(self, start: datetime, end: datetime) -> TimeWindow:
        return TimeWindow(start=start, end=end)

    def _matches(self, window: TimeWindow) -> bool:
        return window.start == FIXTURE_START and window.end == FIXTURE_END

    def _result(self, window: TimeWindow, data: object) -> str:
        return ToolResult[object](
            status="ok" if data else "empty",
            window=window,
            source_kind="simulated",
            source_name="fixture",
            data=data,
            warnings=[FIXTURE_WARNING, f"dataset={self.dataset}"],
        ).model_dump_json()

    def query_metrics(self, service: str, start: datetime, end: datetime) -> str:
        window = self._window(start, end)
        if self.dataset == "tool-error":
            raise RuntimeError("fixture metrics backend is unavailable")
        data = self.data["metrics"].get(service) if self._matches(window) else None
        return self._result(window, data)

    def query_traces(
        self, service: str, start: datetime, end: datetime, limit: int
    ) -> str:
        window = self._window(start, end)
        traces = [
            trace
            for trace in self.data["traces"]
            if service in {trace["root_service"], *(span["service"] for span in trace["spans"])}
        ][:limit]
        return self._result(window, traces if self._matches(window) else [])

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
        window = self._window(start, end)
        logs = [
            log
            for log in self.data["logs"]
            if log["service"] == service
            and (trace_id is None or log["trace_id"] == trace_id)
            and (version is None or log["version"] == version)
            and (error_code is None or log["error_code"] == error_code)
        ][:limit]
        return self._result(window, logs if self._matches(window) else [])
