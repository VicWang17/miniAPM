import json
from pathlib import Path

from langchain_core.tools import tool
from pydantic import BaseModel, ConfigDict, Field

from miniapm.adapters import AdaptationRule
from miniapm.heterogeneous import HeterogeneousPipeline


class TrialInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_format: str
    content: str = Field(max_length=20_000)
    dataset_id: str
    environment: str
    source_id: str
    batch_id: str
    rule_version: str


class ProposalInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rule: dict
    rationale: str = Field(max_length=2000)
    scope: str = Field(max_length=500)


def create_adaptation_tools(pipeline: HeterogeneousPipeline, proposal_dir: str = "data/generated/adaptation-proposals"):
    @tool
    def query_pending(dataset_id: str, limit: int = 5) -> str:
        """Read bounded pending groups and raw samples; never contains evaluation truth."""
        return json.dumps({"status": "ok", "data": pipeline.context.pending_summary(dataset_id, limit)}, ensure_ascii=False)

    @tool
    def list_approved_rules(source_format: str | None = None) -> str:
        """List deterministic rules already approved for use."""
        rules = [rule.model_dump(mode="json") for rule in pipeline.rules.values()
                 if source_format is None or rule.source_format == source_format]
        return json.dumps({"status": "ok", "data": rules}, ensure_ascii=False)

    @tool(args_schema=TrialInput)
    def trial_convert(source_format: str, content: str, dataset_id: str, environment: str,
                      source_id: str, batch_id: str, rule_version: str) -> str:
        """Trial approved conversion without publishing events or modifying pending records."""
        records = pipeline.adapt(source_format=source_format, content=content, dataset_id=dataset_id,
            environment=environment, source_id=source_id, batch_id="trial:" + batch_id,
            rule_version=rule_version)
        data = [{"locator": record.locator,
                 "event": record.event.model_dump(mode="json") if record.event else None,
                 "metric": record.metric,
                 "pending": record.pending.reason if record.pending else None} for record in records[:10]]
        return json.dumps({"status": "ok", "data": data}, ensure_ascii=False)

    @tool(args_schema=ProposalInput)
    def propose_rule(rule: dict, rationale: str, scope: str) -> str:
        """Save a candidate rule for human review; this never approves or publishes it."""
        parsed = AdaptationRule(**rule)
        if parsed.version in pipeline.rules:
            raise ValueError("proposal version must differ from approved versions")
        destination = Path(proposal_dir)
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / f"{parsed.version}.json"
        path.write_text(json.dumps({"rule": parsed.model_dump(mode="json"), "rationale": rationale,
                                    "scope": scope, "status": "proposed"}, ensure_ascii=False, indent=2))
        return json.dumps({"status": "ok", "data": {"proposal": str(path), "approved": False}})

    return (query_pending, list_approved_rules, trial_convert, propose_rule)
