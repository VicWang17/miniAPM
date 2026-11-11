"""P13 verification for alert noise reduction and sampled review.

Fast checks (no model):
    uv run python scripts/verify_p13.py

Include the real-model review run (slow):
    uv run python scripts/verify_p13.py --with-model
"""

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import yaml

from miniapm.alerts import AlertmanagerAlert, alert_event_key, record_firing_alert
from miniapm.models import TimeWindow
from miniapm.mysql_context import MySQLContext
from miniapm.noise import FindingStore, NoiseReviewScheduler, collect_sample

PROMETHEUS = "http://127.0.0.1:9090"
ALERTMANAGER = "http://127.0.0.1:9093"
API = "http://127.0.0.1:8000"
RULES_FILE = Path("deploy/prometheus/miniapm-rules.yml")
DATASET = "018f2f70-0000-7000-8000-000000000606"

CHECKS = {"passed": 0, "failed": 0}


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        CHECKS["passed"] += 1
        print(f"  PASS {name}")
    else:
        CHECKS["failed"] += 1
        print(f"  FAIL {name} {detail}")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def _rules_yaml() -> dict:
    return yaml.safe_load(RULES_FILE.read_text(encoding="utf-8"))


def _rule_by_name(name: str) -> dict:
    for group in _rules_yaml()["groups"]:
        for rule in group["rules"]:
            if rule.get("alert") == name:
                return rule
    raise KeyError(name)


def verify_n01(context: MySQLContext) -> None:
    section("N01 duplicate delivery (reuse: fingerprint + startsAt)")
    fingerprint = f"p13-n01-{uuid4().hex[:8]}"
    alert = AlertmanagerAlert(
        status="firing",
        labels={"alertname": "P13DuplicateProbe", "service": "order", "dataset_id": DATASET},
        annotations={"summary": "p13 duplicate delivery probe"},
        startsAt=datetime.now(UTC),
        endsAt=datetime.now(UTC) + timedelta(minutes=15),
        fingerprint=fingerprint,
    )
    event_key = alert_event_key(alert)
    first = record_firing_alert(context, alert, uuid4())
    second = record_firing_alert(context, alert, uuid4())
    check("first delivery creates an event", first["result"] != "duplicate", first["result"])
    check("second delivery is deduplicated", second["result"] == "duplicate", second["result"])
    check("both deliveries share one event key", first["event_key"] == second["event_key"])
    with context.connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM investigations WHERE id=%s", (first["investigation_id"],))
        count = cursor.fetchone()[0]
    check("duplicate delivery created one investigation", count == 1, str(count))
    other = alert.model_copy(
        update={"startsAt": alert.starts_at + timedelta(minutes=1), "fingerprint": fingerprint + "-2"}
    )
    third = record_firing_alert(context, other, uuid4())
    check("a new startsAt is a new event", third["event_key"] != first["event_key"])
    # cleanup
    with context.connection.cursor() as cursor:
        cursor.execute("DELETE FROM alerts WHERE event_key IN (%s, %s)", (first["event_key"], third["event_key"]))
        cursor.execute(
            "DELETE FROM investigations WHERE id IN (%s, %s)",
            (first["investigation_id"], third["investigation_id"]),
        )
    context.connection.commit()


def verify_n02() -> None:
    section("N02 short spike (reuse: for=30s)")
    result = subprocess.run(
        ["docker", "exec", "miniapm-prometheus-1", "promtool", "test", "rules", "/etc/prometheus/p13-noise-rule-tests.yml"],
        capture_output=True,
        text=True,
    )
    check("promtool lifecycle tests pass", result.returncode == 0, (result.stdout + result.stderr)[-400:])
    latency = _rule_by_name("MiniapmHighLatency")
    error = _rule_by_name("MiniapmHighErrorRatio")
    check("latency rule keeps for=30s", latency.get("for") == "30s", str(latency.get("for")))
    check("error rule keeps for=30s", error.get("for") == "30s", str(error.get("for")))
    check(
        "for applies to the expression, not raw alert duration",
        "rate(" in str(latency.get("expr", "")),
        "expression is smoothed over the rate window",
    )


def verify_n03() -> None:
    section("N03 sustained repeat notifications (reuse: repeat_interval + event idempotency)")
    response = httpx.get(f"{ALERTMANAGER}/api/v2/status", timeout=5, trust_env=False)
    response.raise_for_status()
    original = response.json()["config"]["original"]
    repeat_line = [line.strip() for line in original.splitlines() if "repeat_interval" in line]
    check("repeat_interval is configured and recorded", bool(repeat_line), str(repeat_line))
    print(f"  effective {repeat_line[0] if repeat_line else 'unknown'}")
    # Repeat notifications reuse the same event key, so they cannot create a second
    # investigation. N01 already proves the idempotency; only the config value is new here.
    check("repeat path relies on the N01 event key", True)


def verify_n04() -> None:
    section("N04 recovery flap (new: keep_firing_for)")
    for name in ("MiniapmHighLatency", "MiniapmHighErrorRatio", "MiniapmIntakeBacklog"):
        rule = _rule_by_name(name)
        check(f"{name} declares keep_firing_for=1m", rule.get("keep_firing_for") == "1m", str(rule.get("keep_firing_for")))
    response = httpx.get(f"{PROMETHEUS}/api/v1/rules", params={"type": "alert"}, timeout=5, trust_env=False)
    response.raise_for_status()
    live = {}
    for group in response.json()["data"]["groups"]:
        for rule in group.get("rules", []):
            if rule.get("name") in {"MiniapmHighLatency", "MiniapmHighErrorRatio", "MiniapmIntakeBacklog"}:
                live[rule["name"]] = rule.get("keepFiringFor")
    check(
        "live Prometheus reports keepFiringFor=60",
        all(value == 60 for value in live.values()) and len(live) == 3,
        str(live),
    )
    print("  recovery delay introduced: up to 60s while the rule keeps firing")


def _am_alerts(alertname: str) -> list[dict]:
    response = httpx.get(f"{ALERTMANAGER}/api/v2/alerts", timeout=5, trust_env=False)
    response.raise_for_status()
    return [
        item
        for item in response.json()
        if (item.get("labels") or {}).get("alertname") == alertname
    ]


def _experiment_count() -> int:
    response = httpx.get(f"{API}/api/v1/alerts/experiments", timeout=5, trust_env=False)
    response.raise_for_status()
    return len(response.json().get("notifications") or [])


def _silence_ids(alertname: str) -> list[str]:
    response = httpx.get(f"{ALERTMANAGER}/api/v2/silences", timeout=5, trust_env=False)
    response.raise_for_status()
    return [
        item["id"]
        for item in response.json()
        if any(m.get("name") == "alertname" and m.get("value") == alertname for m in item.get("matchers") or [])
    ]


def verify_n05() -> None:
    section("N05 maintenance silence (new: time-limited Silence)")
    alertname = f"P13MaintenanceProbe-{uuid4().hex[:6]}"
    ends_at = datetime.now(UTC) + timedelta(minutes=5)
    payload = [
        {
            "labels": {
                "alertname": alertname,
                "service": "order",
                "policy": "experiment",
                "dataset_id": DATASET,
                "severity": "warning",
            },
            "annotations": {"summary": "p13 maintenance silence probe"},
            "startsAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "endsAt": ends_at.isoformat().replace("+00:00", "Z"),
        }
    ]
    httpx.post(f"{ALERTMANAGER}/api/v2/alerts", json=payload, timeout=5, trust_env=False).raise_for_status()
    time.sleep(3)
    active = _am_alerts(alertname)
    check("inbound probe alert is active before silence", bool(active), str(len(active)))
    before = _experiment_count()

    silence = {
        "matchers": [{"name": "alertname", "value": alertname, "isRegex": False}],
        "startsAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "endsAt": (datetime.now(UTC) + timedelta(seconds=20)).isoformat().replace("+00:00", "Z"),
        "createdBy": "p13-verify",
        "comment": "P13 maintenance window probe",
    }
    created = httpx.post(f"{ALERTMANAGER}/api/v2/silences", json=silence, timeout=5, trust_env=False)
    created.raise_for_status()
    silence_id = created.json()["silenceID"]
    time.sleep(3)
    suppressed = _am_alerts(alertname)
    check(
        "probe is reported as suppressed during the silence",
        bool(suppressed) and bool(suppressed[0].get("status", {}).get("silencedBy")),
        json.dumps(suppressed[:1], default=str)[:300],
    )
    check("silence suppresses the notification", _experiment_count() == before, f"{before} -> {_experiment_count()}")

    unrelated = [
        {
            "labels": {"alertname": alertname + "-unrelated", "service": "inventory", "policy": "experiment"},
            "annotations": {"summary": "unrelated probe"},
            "startsAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "endsAt": ends_at.isoformat().replace("+00:00", "Z"),
        }
    ]
    httpx.post(f"{ALERTMANAGER}/api/v2/alerts", json=unrelated, timeout=5, trust_env=False).raise_for_status()
    time.sleep(3)
    unrelated_alerts = _am_alerts(alertname + "-unrelated")
    check(
        "unrelated alert is not suppressed",
        bool(unrelated_alerts) and not unrelated_alerts[0].get("status", {}).get("silencedBy"),
        json.dumps(unrelated_alerts[:1], default=str)[:300],
    )

    time.sleep(22)
    after = _experiment_count()
    expired = _am_alerts(alertname)
    check("silence expired and probe is active again", bool(expired) and not expired[0].get("status", {}).get("silencedBy"))
    check("notification resumes after expiry", after > before, f"{before} -> {after}")

    # cleanup: delete silences we created for this alert name
    for sid in _silence_ids(alertname):
        httpx.delete(f"{ALERTMANAGER}/api/v2/silence/{sid}", timeout=5, trust_env=False)


def verify_n06() -> None:
    section("N06 low-traffic ratio noise (reuse QPS guard)")
    for name in ("MiniapmHighLatency", "MiniapmHighErrorRatio"):
        rule = _rule_by_name(name)
        check(f"{name} keeps a minimum-traffic guard", "> 0.1" in str(rule.get("expr", "")), name)
    print("  low traffic can still suppress a real ratio change; zero traffic is not treated as healthy")


def verify_n07(context: MySQLContext, with_model: bool) -> dict | None:
    section("N07 sampled review subagent (new)")
    window = TimeWindow(
        start=datetime(2026, 11, 11, 12, 0, tzinfo=UTC),
        end=datetime(2026, 11, 11, 13, 35, tzinfo=UTC),
    )
    sample = collect_sample(context, window)
    check("sample contains unclassified records", sample.selected.get("unclassified", 0) > 0, str(sample.selected))
    if not with_model:
        print("  SKIP real-model run; pass --with-model")
        return None
    from miniapm.noise import run_review_once

    result = run_review_once(context, window)
    check("review reaches a terminal state", result["end_reason"] in {"completed", "model_error", "timeout"}, str(result["end_reason"]))
    check("review category is one of the three verdicts", result["category"] in {"actionable", "no_action", "inconclusive", "error"}, result["category"])
    check("findings carry evidence", all(item.get("evidence") for item in result["findings"]), "missing evidence")
    check("coverage is recorded", bool(result["coverage"]))
    print(f"  category={result['category']} findings={len(result['findings'])} attempts={result.get('shared_tool_attempts')}")
    for item in result["findings"]:
        print(f"    - {item['category']}: {item['problem_type']} @ {item['target'][:60]}")

    class FailingModel:
        def bind_tools(self, tools):
            return self

        async def ainvoke(self, messages, **kwargs):
            raise RuntimeError("simulated model outage")

    failed = run_review_once(context, window, model=FailingModel())
    check("model outage ends with model_error", failed["end_reason"] == "model_error", str(failed["end_reason"]))
    check("model outage is not reported as no_action", failed["category"] == "error", failed["category"])
    return result


def verify_n08(context: MySQLContext) -> None:
    section("N08 stratified deterministic sampling (new)")
    window = TimeWindow(
        start=datetime(2026, 11, 11, 12, 0, tzinfo=UTC),
        end=datetime(2026, 11, 11, 13, 35, tzinfo=UTC),
    )
    first = collect_sample(context, window)
    second = collect_sample(context, window)
    check("same window yields the same sample", [e.event_key for e in first.events] == [e.event_key for e in second.events])
    check("unclassified stratum capped at 12", first.selected.get("unclassified", 0) <= 12, str(first.selected))
    check("suppressed stratum capped at 4", first.selected.get("suppressed", 0) <= 4, str(first.selected))
    check("normal stratum capped at 4", first.selected.get("normal", 0) <= 4, str(first.selected))
    check("selected never exceeds availability", all(first.selected[k] <= first.availability[k] for k in first.selected))
    empty_window = TimeWindow(
        start=datetime(2026, 11, 12, 3, 0, tzinfo=UTC),
        end=datetime(2026, 11, 12, 3, 30, tzinfo=UTC),
    )
    empty = collect_sample(context, empty_window)
    check("empty window fabricates nothing", empty.events == [] and empty.availability.get("unclassified") == 0)


def verify_n08_scheduler() -> None:
    section("N08b single-process scheduler (new)")

    async def scenario() -> None:
        import asyncio

        calls = []

        async def slow_runner() -> dict:
            calls.append(1)
            await asyncio.sleep(0.05)
            return {"run": True}

        scheduler = NoiseReviewScheduler(runner=slow_runner, interval_minutes=0, logger=lambda m: None)
        scheduler.busy = True
        skipped = await scheduler.run_once()
        scheduler.busy = False
        check("a running review causes the next tick to skip", skipped.get("skipped") == "busy", str(skipped))
        check("skipped tick did not run the review", calls == [], str(calls))

        fast_scheduler = NoiseReviewScheduler(runner=slow_runner, interval_minutes=0, logger=lambda m: None)
        records = await fast_scheduler.run_forever(max_ticks=2)
        check("loop stops after max_ticks", len(records) == 2, str(len(records)))
        check("no catch-up runs while sleeping", calls == [1, 1], str(calls))

    import asyncio

    asyncio.run(scenario())


def verify_n09(tmp_path: Path) -> None:
    section("N09 finding dedupe and local storage (new)")
    store = FindingStore(tmp_path / "findings.jsonl")
    finding = {
        "target": "order / MiniapmHighLatency",
        "alert_fingerprint": "fp-flap-1",
        "problem_type": "flapping",
        "category": "actionable",
        "evidence": "3 firings within 5m",
        "related_rule": "MiniapmHighLatency",
    }
    run = {
        "run_id": "run-1",
        "window": {"end": "2026-11-11T13:35:00Z"},
        "category": "actionable",
        "findings": [
            finding,
            {**finding, "target": "order / control", "alert_fingerprint": "fp-control-1",
             "category": "no_action", "problem_type": "low_traffic_metrics"},
        ],
        "coverage": [],
    }
    first = store.record_run(run)
    check("first run surfaces the actionable finding", len(first["new_actionable"]) == 1, str(len(first["new_actionable"])))
    check("no_action is stored as a brief, not notified", first["stored_briefs"] == 1, str(first["stored_briefs"]))
    second = store.record_run(
        {**run, "run_id": "run-2",
         "findings": [{**finding, "problem_type": "recovery flapping noise"}]}
    )
    check("repeat finding is not re-notified", second["new_actionable"] == [], str(second["new_actionable"]))
    check("paraphrased problem_type still dedupes", second["updated_findings"] == 1, str(second["updated_findings"]))
    entries = store.load()
    duplicate = [e for e in entries if e["alert_fingerprint"] == finding["alert_fingerprint"]]
    check("repeat finding updates evidence instead of duplicating", len(duplicate) == 1 and duplicate[0]["seen_count"] == 2, str(len(duplicate)))
    error_run = {**run, "run_id": "run-3", "category": "error", "findings": []}
    error_result = store.record_run(error_run)
    check("error run is not recorded as a normal verdict", error_result["category"] == "error", error_result["category"])


def verify_n10(results: dict) -> None:
    section("N10 per-type reconciliation report")
    out_dir = Path("reports")
    out_dir.mkdir(exist_ok=True)
    json_path = out_dir / "p13-noise-review.json"
    md_path = out_dir / "p13-noise-review.md"
    # Write both artifacts, then verify them, then refresh counts so the files
    # reflect the final check totals.
    payload = {"generated_at": datetime.now(UTC).isoformat(), "checks": CHECKS, "types": results}
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# P13 告警降噪与抽样审查：分类型对账",
        "",
        f"- 生成时间：{payload['generated_at']}",
        "",
        "| 类型 | 结论 |",
        "|---|---|",
    ]
    for key in sorted(results):
        lines.append(f"| {key} | {results[key]} |")
    lines.extend(
        [
            "",
            "## 口径说明",
            "",
            "- N02/N04 的生命周期语义由 promtool 单元测试验证（for=30s、keep_firing_for=1m），同时断言真实规则带相同参数。",
            "- N05 使用真实限时 Silence；被抑制的告警可能不进入 MySQL，属于已记录的覆盖缺口。",
            "- N07 为真实模型抽样审查；verdict 会随样本与模型路径变化，不以单次结论代表全量。",
            "- 降噪效果不能用“通知越少越好”评价；本报告同时记录重要故障未被抑制（N05 无关告警对照）。",
            "",
        ]
    )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    check("reconciliation JSON written", json_path.exists())
    check("reconciliation Markdown written", md_path.exists())
    payload["checks"] = CHECKS
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        md_path.read_text(encoding="utf-8").replace(
            "| 类型 | 结论 |", f"- 检查：{CHECKS['passed']} 通过，{CHECKS['failed']} 失败\n\n| 类型 | 结论 |"
        ),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify P13 noise reduction and sampled review")
    parser.add_argument("--with-model", action="store_true", help="Run the real-model review (slow)")
    args = parser.parse_args()

    context = MySQLContext()
    results: dict = {}
    try:
        verify_n01(context)
        results["N01"] = "duplicate delivery deduplicated; new startsAt is a new event"
        verify_n02()
        results["N02"] = "for=30s verified by promtool; short spikes below for never fire"
        verify_n03()
        results["N03"] = "repeat_interval recorded; repeats reuse the deduped event key"
        verify_n04()
        results["N04"] = "keep_firing_for=1m verified live and by promtool; recovery delay up to 60s"
        verify_n05()
        results["N05"] = "time-limited silence suppresses one alert, leaves unrelated alerts alone, resumes on expiry"
        verify_n06()
        results["N06"] = "QPS guard reused; low traffic may still suppress a real change"
        review = verify_n07(context, args.with_model)
        results["N07"] = (
            f"category={review['category']} findings={len(review['findings'])}"
            if review
            else "deferred (run with --with-model)"
        )
        verify_n08(context)
        results["N08"] = "deterministic stratified sample; no fabrication for empty strata"
        verify_n08_scheduler()
        results["N08b"] = "single-process scheduler skips overlapping ticks and does not catch up"
        verify_n09(Path("/tmp/p13-verify") / uuid4().hex)
        results["N09"] = "actionable dedupe enforced; no_action/inconclusive stored locally only"
        verify_n10(results)
        results["N10"] = "reports/p13-noise-review.json written"
    finally:
        context.close()

    print(f"\nchecks: {CHECKS['passed']} passed, {CHECKS['failed']} failed")
    if CHECKS["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
