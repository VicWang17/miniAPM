import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta

from miniapm.clickhouse_backend import ClickHouseBackend
from miniapm.heterogeneous import HeterogeneousContext, HeterogeneousPipeline
from miniapm.investigation import build_chat_model
from miniapm.multiagent import arun_multiagent_investigation


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("question")
    parser.add_argument("dataset_id")
    args = parser.parse_args()
    context = HeterogeneousContext()
    backend = ClickHouseBackend(dataset_id=args.dataset_id)
    try:
        end = datetime.now(UTC)
        result = await arun_multiagent_investigation(args.question, "order", end - timedelta(hours=1), end,
            backend, context, model=build_chat_model(), dataset_id=args.dataset_id,
            adaptation_pipeline=HeterogeneousPipeline(context))
        report = result["report"].model_dump(mode="json")
        print(json.dumps({"end_reason": result["end_reason"], "shared_tool_attempts": result["shared_tool_attempts"],
                          "subagents": [run["agent_name"] for run in report["subagent_runs"]],
                          "answer": result["final_answer"]}, ensure_ascii=False, indent=2))
    finally:
        backend.close()
        context.close()


if __name__ == "__main__":
    asyncio.run(main())
