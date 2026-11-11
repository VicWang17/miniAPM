import json
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

from pydantic import Field

from miniapm.models import ContractModel, TimeWindow
from miniapm.mysql_context import MySQLContext


class CandidateMember(ContractModel):
    alert_id: str
    alert_name: str = "unknown"
    service: str
    starts_at: datetime
    relation: Literal["anchor", "same_service", "upstream", "downstream"]
    reasons: list[str]


class CandidateGroup(ContractModel):
    group_id: UUID
    dataset_id: UUID
    anchor_event_key: str
    window: TimeWindow
    members: list[CandidateMember] = Field(max_length=20)
    excluded: list[dict] = Field(default_factory=list, max_length=50)
    truncated: bool = False
    investigation_id: UUID | None = None


class MemberAssessment(ContractModel):
    alert_id: str
    judgment: Literal["supported", "insufficient", "rejected"]
    evidence_refs: list[str] = Field(max_length=10)
    rationale: str = Field(max_length=2000)


class RootCandidate(ContractModel):
    service: str
    summary: str = Field(max_length=2000)
    evidence_refs: list[str] = Field(max_length=10)
    counter_evidence: list[str] = Field(default_factory=list, max_length=10)


class CorrelationResult(ContractModel):
    members: list[MemberAssessment]
    root_cause_candidates: list[RootCandidate] = Field(default_factory=list)
    observed_impact: list[str] = Field(default_factory=list, max_length=20)
    rejected_explanations: list[str] = Field(default_factory=list, max_length=20)
    missing_information: list[str] = Field(default_factory=list, max_length=20)


def parse_correlation_result(answer: str) -> CorrelationResult:
    text = answer.strip()
    if "```" in text:
        text = text.split("```", 1)[1]
        text = text.split("\n", 1)[1].rsplit("```", 1)[0]
    return CorrelationResult.model_validate_json(text)


def validate_correlation_result(result: CorrelationResult, group: CandidateGroup, evidence_refs: set[str]) -> None:
    member_ids = {member.alert_id for member in group.members}
    assessed = [member.alert_id for member in result.members]
    if len(assessed) != len(set(assessed)) or not set(assessed).issubset(member_ids):
        raise ValueError("correlation assessments contain unknown or duplicate members")
    for member in result.members:
        if member.judgment == "supported" and not member.evidence_refs:
            raise ValueError("supported correlation requires evidence references")
        if not set(member.evidence_refs).issubset(evidence_refs):
            raise ValueError("member assessment references unavailable evidence")
    for candidate in result.root_cause_candidates:
        if candidate.service not in {member.service for member in group.members}:
            raise ValueError("root cause candidate is outside the candidate group")
        if not candidate.evidence_refs or not set(candidate.evidence_refs).issubset(evidence_refs):
            raise ValueError("root cause candidate requires available evidence references")


class CorrelationContext(MySQLContext):
    def _alert(self, event_key: str) -> dict | None:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT JSON_UNQUOTE(JSON_EXTRACT(labels_json, '$.dataset_id')), started_at, labels_json FROM alerts WHERE event_key=%s", (event_key,))
            row = cursor.fetchone()
        if row is None:
            return None
        return {"event_key": event_key, "dataset_id": row[0], "started_at": row[1], "labels": json.loads(row[2])}

    def create_candidate_group(self, anchor_event_key: str, max_members: int = 20) -> CandidateGroup:
        anchor = self._alert(anchor_event_key)
        if anchor is None:
            raise LookupError("anchor alert not found")
        started = anchor["started_at"].replace(tzinfo=anchor["started_at"].tzinfo or UTC)
        window = TimeWindow(start=started - timedelta(minutes=5), end=started + timedelta(minutes=5))
        with self.connection.cursor() as cursor:
            cursor.execute(
                """SELECT event_key, started_at, labels_json FROM alerts
                WHERE JSON_UNQUOTE(JSON_EXTRACT(labels_json, '$.dataset_id'))=%s
                  AND started_at >= %s AND started_at < %s
                ORDER BY started_at, event_key LIMIT %s""",
                (anchor["dataset_id"], self._mysql_time(window.start), self._mysql_time(window.end), max_members + 1),
            )
            rows = cursor.fetchall()
        truncated = len(rows) > max_members
        rows = rows[:max_members]
        anchor_labels = anchor["labels"]
        anchor_service = anchor_labels.get("service")
        environment = anchor_labels.get("environment", "local")
        source_kind = anchor_labels.get("source_kind", "simulated")
        members, excluded = [], []
        for event_key, alert_time, raw_labels in rows:
            labels = json.loads(raw_labels)
            service = labels.get("service")
            reasons = [f"time within [{window.start.isoformat()}, {window.end.isoformat()})"]
            if labels.get("environment", "local") != environment or labels.get("source_kind", "simulated") != source_kind:
                excluded.append({"alert_id": event_key, "reasons": ["environment or source_kind is not comparable"]})
                continue
            relation = None
            if event_key == anchor_event_key:
                relation = "anchor"
                reasons.append("specified anchor")
            elif service == anchor_service and service:
                relation = "same_service"
                reasons.append("same service")
            elif service and anchor_service:
                anchor_context = self.service(anchor["dataset_id"], anchor_service)
                candidate_context = self.service(anchor["dataset_id"], service)
                if anchor_context and service in anchor_context.get("dependencies", []):
                    relation = "downstream"
                    reasons.append(f"{anchor_service} directly depends on {service}")
                elif candidate_context and anchor_service in candidate_context.get("dependencies", []):
                    relation = "upstream"
                    reasons.append(f"{service} directly depends on {anchor_service}")
            if relation is None:
                excluded.append({"alert_id": event_key, "reasons": ["unknown identity or no direct topology relation"]})
                continue
            members.append(CandidateMember(alert_id=event_key, alert_name=labels.get("alertname", "unknown"), service=service,
                starts_at=alert_time.replace(tzinfo=alert_time.tzinfo or UTC),
                relation=relation, reasons=reasons))
        if not any(member.relation == "anchor" for member in members):
            raise ValueError("anchor was excluded by identity or source comparison")
        group = CandidateGroup(group_id=uuid4(), dataset_id=UUID(anchor["dataset_id"]),
            anchor_event_key=anchor_event_key, window=window, members=members, excluded=excluded, truncated=truncated)
        with self.connection.cursor() as cursor:
            cursor.execute("""INSERT INTO alert_candidate_groups
                (group_id,dataset_id,anchor_event_key,window_start,window_end,snapshot_json)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (str(group.group_id), anchor["dataset_id"], anchor_event_key,
                 self._mysql_time(window.start), self._mysql_time(window.end), group.model_dump_json()))
        self.connection.commit()
        return group

    def candidate_group(self, group_id: str) -> CandidateGroup | None:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT snapshot_json FROM alert_candidate_groups WHERE group_id=%s", (group_id,))
            row = cursor.fetchone()
        return CandidateGroup.model_validate_json(row[0]) if row else None

    def attach_group_investigation(self, group_id: str, investigation_id: str) -> None:
        group = self.candidate_group(group_id)
        if group is None:
            raise LookupError("candidate group not found")
        updated = group.model_copy(update={"investigation_id": UUID(investigation_id)})
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE alert_candidate_groups SET investigation_id=%s, snapshot_json=%s WHERE group_id=%s",
                (investigation_id, updated.model_dump_json(), group_id))
        self.connection.commit()
