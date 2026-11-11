from datetime import datetime
from typing import Literal, Protocol

GroupDimension = Literal["version", "instance", "route", "error_code"]


class GroupAnalysisBackend(Protocol):
    def group_analysis(
        self, service: str, start: datetime, end: datetime, dimension: str
    ) -> str: ...


def analyze_groups(
    backend: GroupAnalysisBackend,
    service: str,
    start: datetime,
    end: datetime,
    dimension: GroupDimension,
) -> str:
    return backend.group_analysis(service, start, end, dimension)
