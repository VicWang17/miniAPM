from datetime import UTC, datetime, timedelta
from typing import Annotated, Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ServiceName = Literal["order", "inventory"]
SourceKind = Literal["simulated", "instrumented", "platform"]
ToolStatus = Literal["ok", "empty", "partial", "error"]
DataT = TypeVar("DataT")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timezone is required")
    return value.astimezone(UTC)


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TimeWindow(ContractModel):
    start: datetime
    end: datetime

    @field_validator("start", "end")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_bounds(self) -> "TimeWindow":
        if self.start >= self.end:
            raise ValueError("start must be earlier than end")
        if self.end - self.start > timedelta(hours=24):
            raise ValueError("window must not exceed 24 hours")
        return self


class TelemetryEvent(ContractModel):
    event_id: UUID
    event_time: datetime
    ingested_at: datetime
    environment: str = Field(min_length=1)
    dataset_id: UUID
    service: ServiceName
    instance: str = Field(min_length=1)
    version: str = Field(min_length=1)
    route: str = Field(min_length=1)
    source_kind: SourceKind

    @field_validator("event_time", "ingested_at")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        return _utc(value)


class RequestEvent(TelemetryEvent):
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    status: Literal["ok", "error"]
    duration_seconds: float = Field(ge=0)


class LogEvent(TelemetryEvent):
    trace_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    span_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    severity: Literal["DEBUG", "INFO", "WARN", "ERROR"]
    message: str = Field(min_length=1)
    error_code: str | None = None


class SpanEvent(TelemetryEvent):
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    span_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    parent_span_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    kind: Literal["server", "client", "internal"]
    started_at: datetime
    ended_at: datetime
    status: Literal["ok", "error"]

    @field_validator("started_at", "ended_at")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def validate_duration(self) -> "SpanEvent":
        if self.started_at > self.ended_at:
            raise ValueError("started_at must not be later than ended_at")
        return self


class ToolResult(ContractModel, Generic[DataT]):
    status: ToolStatus
    window: TimeWindow
    source_kind: SourceKind
    source_name: str = Field(min_length=1)
    data: DataT | None = None
    warnings: list[str] = Field(default_factory=list, max_length=20)


class ServiceContext(ContractModel):
    environment: str
    dataset_id: UUID
    service: ServiceName
    window: TimeWindow
    instances: tuple[str, ...]
    versions: tuple[str, ...]
    dependencies: tuple[ServiceName, ...]
