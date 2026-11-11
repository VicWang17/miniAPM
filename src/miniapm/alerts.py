import hashlib
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from pymysql.err import IntegrityError

from miniapm.models import TimeWindow


class AlertmanagerAlert(BaseModel):
    status: Literal["firing", "resolved"]
    labels: dict[str, str]
    annotations: dict[str, str] = Field(default_factory=dict)
    starts_at: datetime = Field(alias="startsAt")
    ends_at: datetime = Field(alias="endsAt")
    fingerprint: str = Field(min_length=1)


class AlertmanagerNotification(BaseModel):
    alerts: list[AlertmanagerAlert] = Field(min_length=1, max_length=100)


def alert_event_key(alert: AlertmanagerAlert) -> str:
    started_at = alert.starts_at.astimezone(UTC).isoformat()
    return hashlib.sha256(f"{alert.fingerprint}:{started_at}".encode()).hexdigest()


def record_firing_alert(context, alert: AlertmanagerAlert, default_dataset_id: UUID) -> dict:
    event_key = alert_event_key(alert)
    existing = context.alert(event_key)
    if existing is not None:
        return {"event_key": event_key, "result": "duplicate", **existing}
    service = alert.labels.get("service", "order")
    if service not in {"order", "inventory"}:
        raise ValueError("alert service must be order or inventory")
    dataset_id = UUID(alert.labels.get("dataset_id", str(default_dataset_id)))
    investigation_id = str(uuid4())
    summary = alert.annotations.get("summary") or alert.labels.get("alertname", "APM alert")
    try:
        context.create_alert_investigation(
            investigation_id=investigation_id,
            dataset_id=str(dataset_id),
            question=f"Alert: {summary}",
            service=service,
            window=TimeWindow(
                start=alert.starts_at,
                end=alert.starts_at + timedelta(minutes=15),
            ),
            event_key=event_key,
            fingerprint=alert.fingerprint,
            started_at=alert.starts_at,
            labels=alert.labels,
        )
    except IntegrityError:
        existing = context.alert(event_key)
        if existing is None:
            raise
        return {"event_key": event_key, "result": "duplicate", **existing}
    return {
        "event_key": event_key,
        "result": "created",
        "status": "firing",
        "investigation_id": investigation_id,
    }
