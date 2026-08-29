from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from durable_workflow import workflow
from durable_workflow.workflow import CompleteWorkflow, SelectionResult, WorkflowContext, replay


@workflow.defn(name="durable-selection-cold-replay")
class DurableSelectionColdReplayWorkflow:
    def run(self, ctx: WorkflowContext) -> Any:
        selected: SelectionResult = yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        slow = yield selected.handles["slow"].await_result()

        return {
            "winner": selected.key,
            "winner_value": selected.result(),
            "slow": slow,
        }


def main() -> None:
    if len(sys.argv) != 2:
        raise RuntimeError("Expected one persisted selection-history path.")

    fixture = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    outcome = replay(DurableSelectionColdReplayWorkflow, fixture["history"], [])
    if len(outcome.commands) != 1 or not isinstance(outcome.commands[0], CompleteWorkflow):
        raise RuntimeError("Persisted selection history did not complete during cold replay.")

    print(json.dumps({"process_id": os.getpid(), **outcome.commands[0].result}))


if __name__ == "__main__":
    main()
