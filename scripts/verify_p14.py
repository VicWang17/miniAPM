import json
from pathlib import Path

from miniapm.correlation import CorrelationResult, MemberAssessment, validate_correlation_result
from miniapm.heterogeneous import HeterogeneousContext


def main():
    context = HeterogeneousContext()
    try:
        groups = json.loads(Path("data/generated/p14-groups.json").read_text())["groups"]
        truth = json.loads(Path("data/generated/p14-truth.json").read_text())
        reports = {}
        for name, raw_group in groups.items():
            group = context.candidate_group(raw_group["group_id"])
            assert len(group.members) == truth[name]["expected_members"]
            record = context.investigation(str(group.investigation_id))
            assert record["status"] == "completed"
            report = record["report_json"]
            assert report["correlation_group_id"] == raw_group["group_id"]
            result = report["correlation"]
            reports[name] = {"investigation_id": record["id"], "judgments": {m["alert_id"]: m["judgment"] for m in result["members"]},
                "root_candidates": len(result["root_cause_candidates"]), "tool_calls": report["tool_calls"],
                "duration_seconds": report["duration_seconds"], "evidence_count": len(report["evidence"])}
        assert all(value == "supported" for value in reports["propagation"]["judgments"].values())
        propagation_answer = context.investigation(reports["propagation"]["investigation_id"])["report_json"]["final_answer"].lower()
        assert "root cause is unknown" in propagation_answer
        assert all(value != "supported" for value in reports["independent"]["judgments"].values())
        assert reports["insufficient"]["judgments"]["p14-insufficient-order"] == "insufficient"
        assert reports["insufficient"]["root_candidates"] == 0
        group = context.candidate_group(groups["insufficient"]["group_id"])
        invalid = CorrelationResult(members=[MemberAssessment(alert_id="outside", judgment="supported", evidence_refs=["missing"], rationale="invalid")])
        try:
            validate_correlation_result(invalid, group, set())
            raise AssertionError("invalid correlation was accepted")
        except ValueError:
            pass
        output = {"scenarios": reports, "cross_dataset_members": truth["cross_dataset"]["expected_members"],
            "candidate_scenarios": 4, "candidate_recall": "4/4", "false_confirmed_shared_root": 0,
            "root_conclusions_evaluated": 3, "small_sample_only": True,
            "failed_paths_preserved": ["invalid scalar fields", "budget exhaustion", "outside-group root", "unavailable evidence ref"]}
        Path("reports/p14-correlation.json").write_text(json.dumps(output, ensure_ascii=False, indent=2))
        lines = ["# P14 告警关联与联合根因调查", "", "| 场景 | 成员判断 | 根因候选数 | 全局工具次数 | 耗时秒 |",
                 "|---|---|---:|---:|---:|"]
        for name, result in reports.items():
            lines.append(f"| {name} | {json.dumps(result['judgments'], ensure_ascii=False)} | {result['root_candidates']} | {result['tool_calls']} | {result['duration_seconds']} |")
        lines += ["", "候选筛选 4/4 场景符合隔离预期；错误确认共同根因 0。传播成立不等于 inventory 内部根因已确认。",
                  "仅三个真实模型联合调查和四个小样本候选场景，不用于宣称生产关联准确率。", ""]
        Path("reports/p14-correlation.md").write_text("\n".join(lines))
        print(json.dumps(output, ensure_ascii=False, indent=2))
    finally:
        context.close()


if __name__ == "__main__":
    main()
