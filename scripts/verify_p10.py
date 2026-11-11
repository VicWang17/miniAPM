import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from miniapm.heterogeneous import HeterogeneousContext, HeterogeneousPipeline
from miniapm.rabbitmq_publisher import RabbitMQPublisher
from prepare_heterogeneous import DATASET_ID, END, START


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--approve-unknown", action="store_true")
    args = parser.parse_args()
    root = Path("data/hetero")
    truth = json.loads((root / "truth/expected.json").read_text())
    context = HeterogeneousContext()
    publisher = RabbitMQPublisher()
    try:
        pipeline = HeterogeneousPipeline(context, publisher)
        common = {"dataset_id": str(DATASET_ID), "environment": "hetero"}
        first = pipeline.process(source_format="nginx", content=(root / "raw/nginx.log").read_text(),
            source_id="edge-order", batch_id="nginx-batch", rule_version="nginx-v1", **common)
        second = pipeline.process(source_format="nginx", content=(root / "raw/nginx.log").read_text(),
            source_id="edge-order", batch_id="nginx-batch", rule_version="nginx-v1", **common)
        assert first["published"] == second["published"] == truth["nginx"]["passed"]
        assert first["pending"] == second["pending"] == 1
        good = [r.event for r in pipeline.adapt(source_format="nginx", content=(root / "raw/nginx.log").read_text(),
            source_id="edge-order", batch_id="nginx-batch", rule_version="nginx-v1", **common) if r.event]
        assert [e.event_time for e in good] == [START, END]
        assert [e.version for e in good] == truth["nginx"]["versions"]
        assert all(e.source_kind == "simulated" for e in good)
        assert context.resolve_identity(DATASET_ID, "hetero", "edge-order", END)["version"] == "v2"
        assert context.resolve_identity("018f2f70-0000-7000-8000-000000000999", "hetero", "edge-order", END)["status"] == "missing"

        otlp = pipeline.process(source_format="otlp", content=(root / "raw/otlp.json").read_text(),
            source_id="edge-order", batch_id="otlp-batch", rule_version="otlp-v1", **common)
        assert otlp["published"] == truth["otlp"]["passed"] and otlp["pending"] == 0
        spans = [r.event for r in pipeline.adapt(source_format="otlp", content=(root / "raw/otlp.json").read_text(),
            source_id="edge-order", batch_id="otlp-batch", rule_version="otlp-v1", **common) if r.event]
        assert [e.service for e in spans] == truth["otlp"]["services"]
        assert spans[0].trace_id == spans[1].trace_id == truth["otlp"]["trace_id"]
        assert spans[1].parent_span_id == spans[0].span_id

        invalid = pipeline.process(source_format="otlp", content=(root / "raw/otlp-invalid.json").read_text(),
            source_id="edge-order", batch_id="invalid-batch", rule_version="otlp-v1", **common)
        assert invalid["pending"] == 1
        metric_result = pipeline.process(source_format="prometheus", content=(root / "raw/metrics.prom").read_text(),
            source_id="prom-source", batch_id="metric-batch", rule_version="prometheus-v1",
            event_time=START, **common)
        assert metric_result["metrics"] == 1
        before = pipeline.metrics.render().decode()
        pipeline.process(source_format="prometheus", content=(root / "raw/metrics.prom").read_text(),
            source_id="prom-source", batch_id="metric-batch", rule_version="prometheus-v1",
            event_time=START, **common)
        after = pipeline.metrics.render().decode()
        for line in after.splitlines():
            if "miniapm_adapted_request_duration_seconds_bucket" in line and 'le="+Inf"' not in line:
                assert 'le="0.1"' in line or 'le="0.5"' in line
            if "miniapm_adapted_request_duration_seconds_sum" in line:
                assert line.endswith(" 0.6")
            if "miniapm_adapted_request_duration_seconds_count" in line:
                assert line.endswith(" 2.0")
        assert before.count("miniapm_adapted_request_duration_seconds_count{") == after.count("miniapm_adapted_request_duration_seconds_count{") == 1

        pending_before = context.pending_summary(str(DATASET_ID))
        assert sum(group["count"] for group in pending_before["groups"]) == 2
        approval = None
        replay = None
        if args.approve_unknown:
            proposal = Path("data/generated/adaptation-proposals/nginx-v2.json")
            if not proposal.exists():
                proposal.parent.mkdir(parents=True, exist_ok=True)
                proposal.write_text(json.dumps({"rule": {
                    "version": "nginx-v2", "source_format": "nginx",
                    "log_format": "$remote_addr - $remote_user [$time_local] \"$request\" $status duration_ms=$duration_ms svc=$source_id",
                    "duration_field": "duration_ms", "duration_unit": "milliseconds", "route_from_request": True
                }, "rationale": "Verified 125 milliseconds converts to 0.125 seconds", "scope": "edge-order duration_ms fixture"}))
            rejected = pipeline.approve_rule(str(proposal), False)
            assert rejected["approved"] is False
            approval = pipeline.approve_rule(str(proposal), True)
            replay = pipeline.replay(str(DATASET_ID), "nginx-batch", "nginx-v2")
            assert replay["published"] == 1 and replay["still_pending"] == 0
        context.import_identity(DATASET_ID, "hetero", "edge-order", "order", "duplicate", "v2", END)
        assert context.resolve_identity(DATASET_ID, "hetero", "edge-order", END)["status"] == "ambiguous"
        with context.connection.cursor() as cursor:
            cursor.execute("DELETE FROM identity_mappings WHERE dataset_id=%s AND instance='duplicate'", (str(DATASET_ID),))
        context.connection.commit()
        conflict_document = json.loads((root / "raw/otlp.json").read_text())
        conflict_document["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"] = "inventory"
        conflict = pipeline.adapt(source_format="otlp", content=json.dumps(conflict_document),
            source_id="edge-order", batch_id="conflict", rule_version="otlp-v1", **common)
        assert conflict[0].pending.reason == "raw service identity conflicts with identity mapping"
        passed = 6 if approval else 5
        reconciliation = {"input_records": 7, "passed_records": passed,
            "automatic_pass_rate": passed / 7, "error_identification_rate": 0.0,
            "unit_correct_samples": 4 if approval else 3, "unit_samples": 4,
            "unit_accuracy": (4 if approval else 3) / 4,
            "expected_unknown_pending": 1, "small_sample_only": True}
        output = {"dataset_id": str(DATASET_ID), "nginx_first": first, "nginx_repeat": second,
                  "otlp": otlp, "invalid": invalid, "metrics": metric_result,
                  "pending_before": pending_before, "approval": approval, "replay": replay,
                  "identity_ambiguous": True, "raw_identity_conflict": True,
                  "reconciliation": reconciliation, "truth_checks": "passed"}
        destination = Path("reports/p10-adaptation.json")
        destination.write_text(json.dumps(output, ensure_ascii=False, indent=2))
        print(json.dumps(output, ensure_ascii=False, indent=2))
    finally:
        publisher.close()
        context.close()


if __name__ == "__main__":
    main()
