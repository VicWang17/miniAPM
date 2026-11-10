import argparse
import json
from datetime import datetime

from langchain_core.messages import AIMessage, ToolMessage

from miniapm.fixtures import FIXTURE_END, FIXTURE_START
from miniapm.investigation import run_investigation


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one fixture-backed investigation")
    parser.add_argument("question")
    parser.add_argument("--service", choices=["order", "inventory"], default="order")
    parser.add_argument("--start", type=datetime.fromisoformat, default=FIXTURE_START)
    parser.add_argument("--end", type=datetime.fromisoformat, default=FIXTURE_END)
    args = parser.parse_args()

    result = run_investigation(args.question, args.service, args.start, args.end)
    calls = []
    final_answer = ""
    for message in result["messages"]:
        if isinstance(message, AIMessage):
            if message.content:
                final_answer = message.content
            calls.extend(
                {"call_id": call["id"], "name": call["name"], "args": call["args"]}
                for call in message.tool_calls
            )
        elif isinstance(message, ToolMessage):
            calls.append(
                {
                    "call_id": message.tool_call_id,
                    "name": message.name,
                    "result": json.loads(message.content),
                }
            )
    print(json.dumps({"final_answer": final_answer, "calls": calls}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
