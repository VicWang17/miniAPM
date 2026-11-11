from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class AlertmanagerAlert(BaseModel):
    status: Literal["firing", "resolved"]
    labels: dict[str, str]
    annotations: dict[str, str] = Field(default_factory=dict)
    starts_at: datetime = Field(alias="startsAt")
    ends_at: datetime = Field(alias="endsAt")
    fingerprint: str = Field(min_length=1)


class AlertmanagerNotification(BaseModel):
    alerts: list[AlertmanagerAlert] = Field(min_length=1, max_length=100)
