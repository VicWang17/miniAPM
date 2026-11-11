"""P13 alert noise review: deterministic sampling, review subagent and findings store.

Deterministic code owns sampling, classification, deduplication and notification
selection. The model only inspects a bounded sample and returns a judgement with
evidence. It never reads evaluation truth, never edits rules and never silences
alerts by itself.
"""

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import httpx
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from pydantic import BaseModel, ConfigDict, Field

from miniapm.investigation import (
    InvestigationBudget,
    SharedInvestigationBudget,
    build_chat_model,
    create_investigation_agent,
)
from miniapm.models import TimeWindow, ToolResult
from miniapm.prometheus_backend import PrometheusBackend

KNOWN_TYPES = {
    "duplicate_delivery",
    "short_spike",
    "sustained_repeat",
    "recovery_flap",
    "maintenance",
    "low_traffic",
    "normal",
    "unclassified",
}
STRATUM_LIMITS = {"unclassified": 12, "suppressed": 4, "normal": 4}
MAX_EVENTS_SCANNED = 500


class SampleEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_key: str
    fingerprint: str
    alertname: str
    service: str
    status: str
    started_at: str
    resolved_at: str | None = None
    duration_seconds: float | None = None
    investigation_id: str | None = None
    known_type: str
    stratum: str


class ReviewSample(BaseModel):
    model_config = ConfigDict(extra="forbid")

    window: TimeWindow
    events: list[SampleEvent]
    availability: dict[str, int]
    selected: dict[str, int]
    coverage: list[str]
    alertmanager_reachable: bool


class ReviewFinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target: str = Field(min_length=1, max_length=200)
    alert_fingerprint: str | None = Field(default=None, max_length=200)
    problem_type: str = Field(min_length=1, max_length=100)
    category: Literal["actionable", "no_action", "inconclusive"]
    evidence: str = Field(min_length=1, max_length=1000)
    counter_evidence: str | None = Field(default=None, max_length=1000)
    missing_information: str | None = Field(default=None, max_length=1000)
    suggestion: str | None = Field(default=None, max_length=1000)
    related_rule: str | None = Field(default=None, max_length=200)


class ReviewOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    findings: list[ReviewFinding] = Field(default_factory=list)
    coverage: str | None = Field(default=None, max_length=1000)


def _duration_seconds(started_at: str | None, resolved_at: str | None) -> float | None:
    if not started_at or not resolved_at:
        return None
    try:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(resolved_at)
    except ValueError:
        return None
    return round((end - start).total_seconds(), 3)


def _silence_matches(silence: dict, labels: dict) -> bool:
    for matcher in silence.get("matchers") or []:
        name = matcher.get("name")
        value = matcher.get("value", "")
        is_regex = matcher.get("isRegex", False)
        actual = labels.get(name)
        if actual is None:
            return False
        if is_regex:
            import re

            if not re.fullmatch(value, actual):
                return False
        elif actual != value:
            return False
    return True


def _fetch_rule_names(prometheus_url: str) -> tuple[set[str], bool]:
    try:
        response = httpx.get(
            f"{prometheus_url.rstrip('/')}/api/v1/rules",
            params={"type": "alert"},
            timeout=5,
            trust_env=False,
        )
        response.raise_for_status()
        groups = response.json()["data"]["groups"]
        names = {
            rule.get("name")
            for group in groups
            for rule in group.get("rules", [])
            if rule.get("name")
        }
        return names, True
    except Exception:
        return set(), False


def classify_event(
    event: dict,
    stats: dict[str, dict],
    silences: list[dict],
    known_rules: set[str],
    now: datetime,
) -> str:
    if any(_silence_matches(silence, event["labels"]) for silence in silences):
        return "maintenance"
    alertname = event.get("alertname") or ""
    if not alertname or alertname == "unknown" or alertname not in known_rules:
        # A record without a matching current rule cannot be explained by the
        # deterministic handlers and must go to human/model review.
        return "unclassified"
    if event.get("investigation_id") is None:
        return "unclassified"
    duration = _duration_seconds(event.get("started_at"), event.get("resolved_at"))
    if event.get("status") == "firing":
        started = event.get("started_at")
        if started:
            try:
                age = (now - datetime.fromisoformat(started)).total_seconds()
            except ValueError:
                age = 0
            if age > 1800:
                return "sustained_repeat"
    if duration is not None and duration < 30:
        return "short_spike"
    fingerprint_stats = stats.get(event["fingerprint"]) or {}
    if int(fingerprint_stats.get("events") or 0) > 1:
        return "recovery_flap"
    return "normal"


def _fetch_silences(alertmanager_url: str) -> tuple[list[dict], bool]:
    try:
        response = httpx.get(
            f"{alertmanager_url.rstrip('/')}/api/v2/silences",
            timeout=5,
            trust_env=False,
        )
        response.raise_for_status()
        payload = response.json()
        return [item for item in payload if item.get("status", {}).get("state") in {"active", "pending"}], True
    except Exception:
        return [], False


def collect_sample(
    context,
    window: TimeWindow,
    alertmanager_url: str = "http://127.0.0.1:9093",
    limits: dict[str, int] | None = None,
    prometheus_url: str = "http://127.0.0.1:9090",
) -> ReviewSample:
    """Deterministic stratified sample of alerts in the window.

    Unclassified records dominate; suppressed and normal records act as controls.
    Ordering is by started_at then event_key so the same window yields the same sample.
    """
    limits = {**STRATUM_LIMITS, **(limits or {})}
    rows = context.alerts_between(window.start, window.end, limit=MAX_EVENTS_SCANNED)
    stats = context.alert_fingerprint_stats(window.start, window.end)
    silences, alertmanager_reachable = _fetch_silences(alertmanager_url)
    known_rules, rules_reachable = _fetch_rule_names(prometheus_url)
    if not rules_reachable:
        known_rules = {
            "MiniapmHighLatency",
            "MiniapmHighErrorRatio",
            "MiniapmIntakeBacklog",
            "MiniapmAvailabilityFastBurn",
            "MiniapmAvailabilitySlowBurn",
            "MiniapmInFlightSigma",
            "MiniapmInFlightFixed",
            "MiniapmLatencySLO",
            "MiniapmLatencyP95Comparison",
        }

    events: list[SampleEvent] = []
    for row in rows:
        known_type = classify_event(row, stats, silences, known_rules, window.end)
        if known_type == "maintenance":
            stratum = "suppressed"
        elif known_type == "unclassified":
            stratum = "unclassified"
        elif known_type == "normal":
            stratum = "normal"
        else:
            stratum = "known"
        events.append(
            SampleEvent(
                event_key=row["event_key"],
                fingerprint=row["fingerprint"],
                alertname=row["alertname"],
                service=row["service"],
                status=row["status"],
                started_at=row["started_at"] or "",
                resolved_at=row["resolved_at"],
                duration_seconds=_duration_seconds(row["started_at"], row["resolved_at"]),
                investigation_id=row["investigation_id"],
                known_type=known_type,
                stratum=stratum,
            )
        )

    availability: dict[str, int] = {}
    selected: dict[str, int] = {}
    chosen: list[SampleEvent] = []
    for stratum in ("unclassified", "suppressed", "normal"):
        candidates = [event for event in events if event.stratum == stratum]
        availability[stratum] = len(candidates)
        take = min(limits.get(stratum, 0), len(candidates))
        selected[stratum] = take
        chosen.extend(candidates[:take])

    coverage = [
        f"scanned={len(rows)} of at most {MAX_EVENTS_SCANNED} alerts in window",
        "unclassified={}/{}".format(selected.get("unclassified", 0), availability.get("unclassified", 0)),
        "suppressed={}/{}".format(selected.get("suppressed", 0), availability.get("suppressed", 0)),
        "normal={}/{}".format(selected.get("normal", 0), availability.get("normal", 0)),
    ]
    if not alertmanager_reachable:
        coverage.append("alertmanager unreachable: suppressed stratum is incomplete")
    coverage.append("kn-1: alerts suppressed before reaching the webhook may not exist in MySQL")

    return ReviewSample(
        window=window,
        events=chosen,
        availability=availability,
        selected=selected,
        coverage=coverage,
        alertmanager_reachable=alertmanager_reachable,
    )


REVIEW_PROMPT = """You are the alert noise review subagent.
Inspect the bounded alert sample with the provided tools and judge whether each record is
low-value noise, a rule-design problem, or something that still needs a human.

Return ONLY a JSON object:
{"findings": [{"target": "...", "alert_fingerprint": "...", "problem_type": "...",
  "category": "actionable|no_action|inconclusive", "evidence": "...", "counter_evidence": "...",
  "missing_information": "...", "suggestion": "...", "related_rule": "..."}], "coverage": "..."}

For every finding, copy `alert_fingerprint` verbatim from list_alert_sample or alert_history.
Use one of these problem_type values when applicable: duplicate_events, flapping, short_spike,
sustained_noise, rule_misconfiguration, low_traffic_metrics, unknown_provenance.

Rules:
- Budget: at most 10 tool calls in total. Group your work: one list_alert_sample, then the few
  rule_config / alert_history / list_silences calls you actually need, then answer.
- Base every claim on tool output. Quote concrete counts, durations, rule fields or silence IDs.
- A long or repeated alert is not automatically noise. A suppressed alert is not automatically harmless.
- Use inconclusive when data is missing or contradictory; use no_action only when the sample was actually examined.
- Suggest rule improvements; never claim you changed a rule or a silence.
- Never invent alerts, services, rules or numbers outside the sample.
"""

REVIEW_SUPERVISOR_PROMPT = """You are the noise review lead.
Call noise_review_investigation exactly once with the review task, then return the subagent's
JSON object unchanged. Do not add prose outside the JSON. Treat subagent output as evidence,
not ground truth, and keep any coverage gaps it reports.
"""


class ReviewToolsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


def create_review_tools(
    context,
    sample: ReviewSample,
    prometheus_url: str,
    alertmanager_url: str,
):
    window = sample.window
    sample_by_fingerprint: dict[str, list[SampleEvent]] = {}
    for event in sample.events:
        sample_by_fingerprint.setdefault(event.fingerprint, []).append(event)

    @tool(args_schema=ReviewToolsInput)
    def list_alert_sample() -> str:
        """Return the bounded alert sample under review, grouped by stratum."""
        events = [
            {
                "event_key": event.event_key[:16],
                "fingerprint": event.fingerprint,
                "alertname": event.alertname,
                "service": event.service,
                "status": event.status,
                "started_at": event.started_at,
                "resolved_at": event.resolved_at,
                "duration_seconds": event.duration_seconds,
                "has_investigation": event.investigation_id is not None,
                "known_type": event.known_type,
                "stratum": event.stratum,
            }
            for event in sample.events
        ]
        return ToolResult[object](
            status="ok" if events else "empty",
            window=window,
            source_kind="platform",
            source_name="mysql_alerts",
            data={"events": events[:20], "truncated": len(events) > 20},
            warnings=list(sample.coverage),
        ).model_dump_json()

    @tool
    def alert_history(fingerprint: str) -> str:
        """Return firing/resolved counts and sample records for one fingerprint."""
        stats = context.alert_fingerprint_stats(window.start, window.end)
        record = stats.get(fingerprint)
        events = sample_by_fingerprint.get(fingerprint, [])
        if record is None and not events:
            return ToolResult[object](
                status="empty",
                window=window,
                source_kind="platform",
                source_name="mysql_alerts",
                data=None,
                warnings=["fingerprint was not observed in this window"],
            ).model_dump_json()
        return ToolResult[object](
            status="ok",
            window=window,
            source_kind="platform",
            source_name="mysql_alerts",
            data={
                "fingerprint": fingerprint,
                "stats": record or {"events": 0, "firing": 0, "resolved": 0},
                "sample_records": [event.model_dump() for event in events[:5]],
            },
            warnings=[],
        ).model_dump_json()

    @tool
    def rule_config(alertname: str) -> str:
        """Return the Prometheus rule definition (expr, for, keep_firing_for) by alertname."""
        try:
            response = httpx.get(
                f"{prometheus_url.rstrip('/')}/api/v1/rules",
                params={"type": "alert"},
                timeout=5,
                trust_env=False,
            )
            response.raise_for_status()
            groups = response.json()["data"]["groups"]
        except Exception as exc:
            return ToolResult[object](
                status="error",
                window=window,
                source_kind="platform",
                source_name="prometheus_rules",
                data=None,
                warnings=[f"prometheus rules unavailable: {type(exc).__name__}"],
            ).model_dump_json()
        for group in groups:
            for rule in group.get("rules", []):
                if rule.get("name") == alertname:
                    return ToolResult[object](
                        status="ok",
                        window=window,
                        source_kind="platform",
                        source_name="prometheus_rules",
                        data={
                            "group": group.get("name"),
                            "name": rule.get("name"),
                            "expr": (rule.get("query") or rule.get("expr") or "")[:800],
                            "for": rule.get("duration"),
                            "keep_firing_for": rule.get("keepFiringFor"),
                            "labels": rule.get("labels"),
                            "state": rule.get("state"),
                            "alerts": len(rule.get("alerts") or []),
                        },
                        warnings=[],
                    ).model_dump_json()
        return ToolResult[object](
            status="empty",
            window=window,
            source_kind="platform",
            source_name="prometheus_rules",
            data=None,
            warnings=[f"no rule named {alertname}"],
        ).model_dump_json()

    @tool(args_schema=ReviewToolsInput)
    def list_silences() -> str:
        """Return active and pending Alertmanager silences with matchers and expiry."""
        silences, reachable = _fetch_silences(alertmanager_url)
        if not reachable:
            return ToolResult[object](
                status="error",
                window=window,
                source_kind="platform",
                source_name="alertmanager",
                data=None,
                warnings=["alertmanager unreachable"],
            ).model_dump_json()
        return ToolResult[object](
            status="ok" if silences else "empty",
            window=window,
            source_kind="platform",
            source_name="alertmanager",
            data={
                "silences": [
                    {
                        "id": silence.get("id"),
                        "matchers": silence.get("matchers"),
                        "starts_at": silence.get("startsAt"),
                        "ends_at": silence.get("endsAt"),
                        "state": silence.get("status", {}).get("state"),
                        "comment": (silence.get("comment") or "")[:200],
                    }
                    for silence in silences[:10]
                ]
            },
            warnings=[],
        ).model_dump_json()

    @tool
    def service_metrics(service: str) -> str:
        """Return QPS, error ratio and P95 at the window end for one service."""
        if service not in {"order", "inventory"}:
            return ToolResult[object](
                status="error",
                window=window,
                source_kind="platform",
                source_name="prometheus",
                data=None,
                warnings=["unsupported service"],
            ).model_dump_json()
        return PrometheusBackend(prometheus_url).query_metrics(
            service, window.start, window.end
        )

    return [list_alert_sample, alert_history, rule_config, list_silences, service_metrics]


def _as_supervisor_tool(name: str, agent, deadline: float):
    @tool(name)
    async def invoke(task: str) -> str:
        """Invoke the bounded noise review subagent with a structured task."""
        from miniapm.multiagent import _initial_state

        result = await agent.ainvoke(
            _initial_state(task, deadline), config={"recursion_limit": 60}
        )
        return json.dumps(
            {
                "agent": name,
                "end_reason": result["end_reason"],
                "answer": result["final_answer"],
            },
            ensure_ascii=False,
        )

    return invoke


def create_review_agent(
    model: BaseChatModel | None,
    sample: ReviewSample,
    context,
    prometheus_url: str = "http://127.0.0.1:9090",
    alertmanager_url: str = "http://127.0.0.1:9093",
    shared_budget: SharedInvestigationBudget | None = None,
):
    shared_budget = shared_budget or SharedInvestigationBudget(max_tool_calls=18, timeout_seconds=240)
    deadline = shared_budget.deadline
    review_tools = create_review_tools(context, sample, prometheus_url, alertmanager_url)
    subagent = create_investigation_agent(
        model,
        # Tools share one MySQL connection, so review calls must not run concurrently.
        InvestigationBudget(max_tool_calls=12, tool_concurrency=1),
        sample.window,
        None,
        shared_budget=shared_budget,
        tools_override=tuple(review_tools),
        system_prompt=REVIEW_PROMPT,
    )
    supervisor = create_investigation_agent(
        model,
        InvestigationBudget(max_tool_calls=2, timeout_seconds=180),
        sample.window,
        None,
        shared_budget=shared_budget,
        tools_override=(_as_supervisor_tool("noise_review_investigation", subagent, deadline),),
        system_prompt=REVIEW_SUPERVISOR_PROMPT,
    )
    return supervisor, shared_budget


def _review_task(sample: ReviewSample) -> str:
    return (
        "Review the alert sample for noise and rule-design problems.\n"
        f"Window: {sample.window.start.isoformat()} to {sample.window.end.isoformat()}\n"
        f"Sample size: {len(sample.events)}; strata: {sample.selected}\n"
        f"Coverage: {'; '.join(sample.coverage)}\n"
        "Call noise_review_investigation once with this task."
    )


def parse_review_output(text: str) -> ReviewOutput:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        import re

        cleaned = re.sub(r"^```[a-zA-Z]*\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("review output did not contain a JSON object")
    return ReviewOutput(**json.loads(cleaned[start : end + 1]))


async def arun_review_once(
    context,
    window: TimeWindow,
    model: BaseChatModel | None = None,
    prometheus_url: str = "http://127.0.0.1:9090",
    alertmanager_url: str = "http://127.0.0.1:9093",
    limits: dict[str, int] | None = None,
) -> dict:
    started = time.monotonic()
    sample = collect_sample(context, window, alertmanager_url, limits)
    if not sample.events:
        return {
            "run_id": hashlib.sha256(f"{window.start}{window.end}empty".encode()).hexdigest()[:16],
            "window": window.model_dump(mode="json"),
            "sample": sample.model_dump(mode="json"),
            "category": "inconclusive",
            "findings": [],
            "coverage": sample.coverage + ["no alert matched any stratum; nothing was reviewed"],
            "end_reason": None,
            "parse_fallback": False,
            "duration_seconds": round(time.monotonic() - started, 3),
        }

    agent, shared_budget = create_review_agent(
        model, sample, context, prometheus_url, alertmanager_url
    )
    from miniapm.multiagent import _initial_state

    result = await agent.ainvoke(
        _initial_state(_review_task(sample), shared_budget.deadline),
        config={"recursion_limit": 60},
    )
    end_reason = result["end_reason"]
    answer = result["final_answer"]
    parse_fallback = False
    output: ReviewOutput | None = None
    error: str | None = None

    if end_reason == "completed":
        try:
            output = parse_review_output(answer)
        except (ValueError, json.JSONDecodeError):
            # The supervisor may have returned prose; try the subagent tool result instead.
            for message in reversed(result["messages"]):
                if isinstance(message, ToolMessage) and message.name == "noise_review_investigation":
                    try:
                        inner = json.loads(message.content)
                        output = parse_review_output(inner.get("answer", ""))
                        parse_fallback = True
                    except (ValueError, json.JSONDecodeError):
                        pass
                    break
            if output is None:
                error = "model output was not valid review JSON"

    if output is None:
        findings: list[ReviewFinding] = []
        category = "error" if end_reason in {"model_error", "timeout"} or error else "inconclusive"
        coverage = sample.coverage + [
            f"review did not produce findings: end_reason={end_reason}"
            + (f", {error}" if error else "")
        ]
    else:
        findings = output.findings
        coverage = sample.coverage + ([output.coverage] if output.coverage else [])
        actionable = [item for item in findings if item.category == "actionable"]
        category = "actionable" if actionable else (
            "no_action" if findings and all(item.category == "no_action" for item in findings) else "inconclusive"
        )

    return {
        "run_id": hashlib.sha256(
            f"{window.start.isoformat()}{window.end.isoformat()}{end_reason}".encode()
        ).hexdigest()[:16],
        "window": window.model_dump(mode="json"),
        "sample": sample.model_dump(mode="json"),
        "category": category,
        "findings": [item.model_dump() for item in findings],
        "coverage": coverage,
        "end_reason": end_reason,
        "parse_fallback": parse_fallback,
        "shared_tool_attempts": shared_budget.attempts,
        "duration_seconds": round(time.monotonic() - started, 3),
    }


def run_review_once(*args, **kwargs) -> dict:
    return asyncio.run(arun_review_once(*args, **kwargs))


class FindingStore:
    """Bounded local JSONL store with a fixed dedupe key per finding."""

    def __init__(self, path: Path, max_entries: int = 500) -> None:
        self.path = path
        self.max_entries = max_entries

    PROBLEM_BUCKETS = (
        ("duplicate", ("duplicate", "重复", "storm")),
        ("flapping", ("flap", "抖动", "recovery")),
        ("short", ("short", "spike", "毛刺")),
        ("sustained", ("sustained", "持续")),
        ("rule", ("rule", "threshold", "misconfig", "规则")),
        ("unknown", ("unknown", "provenance", "unverifiable", "无法", "inconclusive")),
    )

    @classmethod
    def problem_bucket(cls, problem_type: str | None) -> str:
        text = (problem_type or "").lower()
        for bucket, needles in cls.PROBLEM_BUCKETS:
            if any(needle in text for needle in needles):
                return bucket
        return "other"

    @classmethod
    def dedupe_key(cls, finding: dict) -> str:
        """Stable key: alert fingerprint (or normalized target) + problem bucket + rule.

        The model may paraphrase problem_type or target between runs, so the key keeps
        only deterministic identifiers and a coarse problem bucket.
        """
        subject = (finding.get("alert_fingerprint") or finding.get("target") or "").strip().lower()
        subject = "".join(ch for ch in subject if ch.isalnum())
        bucket = cls.problem_bucket(finding.get("problem_type"))
        rule = (finding.get("related_rule") or "").split("(")[0].strip().lower()
        raw = f"{subject}|{bucket}|{rule}"
        return hashlib.sha256(raw.encode()).hexdigest()[:24]

    def load(self) -> list[dict]:
        if not self.path.exists():
            return []
        entries = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return entries[-self.max_entries :]

    def record_run(self, run: dict) -> dict:
        """Merge one review run; return only newly actionable findings plus run counts."""
        entries = self.load()
        by_key = {entry["dedupe_key"]: entry for entry in entries}
        new_actionable = []
        updated = 0
        stored_briefs = 0
        for finding in run.get("findings") or []:
            key = self.dedupe_key(finding)
            existing = by_key.get(key)
            if existing is not None:
                existing["last_seen"] = run["window"]["end"]
                existing["evidence"] = finding.get("evidence")
                existing["category"] = finding.get("category")
                existing["seen_count"] = int(existing.get("seen_count") or 1) + 1
                updated += 1
                continue
            entry = {
                "dedupe_key": key,
                "first_seen": run["window"]["end"],
                "last_seen": run["window"]["end"],
                "seen_count": 1,
                "run_id": run["run_id"],
                **finding,
            }
            entries.append(entry)
            by_key[key] = entry
            if finding.get("category") == "actionable":
                new_actionable.append(entry)
            else:
                stored_briefs += 1
        entries = entries[-self.max_entries :]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            "\n".join(json.dumps(entry, ensure_ascii=False) for entry in entries) + "\n",
            encoding="utf-8",
        )
        return {
            "run_id": run["run_id"],
            "category": run.get("category"),
            "new_actionable": new_actionable,
            "updated_findings": updated,
            "stored_briefs": stored_briefs,
            "total_entries": len(entries),
            "coverage": run.get("coverage", []),
        }


@dataclass(slots=True)
class NoiseReviewScheduler:
    """Single-process periodic entry: no overlap, no catch-up after downtime."""

    runner: object
    interval_minutes: int = 10
    logger: object = print
    busy: bool = False

    async def run_once(self) -> dict:
        if self.busy:
            self.logger("[noise-review] previous run still active; skipping this tick")
            return {"skipped": "busy"}
        self.busy = True
        try:
            return await self.runner()
        finally:
            self.busy = False

    async def run_forever(self, max_ticks: int | None = None) -> list[dict]:
        records = []
        ticks = 0
        while True:
            if max_ticks is not None and ticks >= max_ticks:
                return records
            tick_started = time.monotonic()
            records.append(await self.run_once())
            ticks += 1
            elapsed = time.monotonic() - tick_started
            await asyncio.sleep(max(0.0, self.interval_minutes * 60 - elapsed))
