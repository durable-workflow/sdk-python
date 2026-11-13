from __future__ import annotations

import json
from pathlib import Path

from durable_workflow import workflow
from durable_workflow.errors import ActivityFailed
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, query_state, replay

FIXTURE = Path(__file__).parent / "fixtures/replay_regressions/legacy-parallel-failure-remains-pending.json"


@workflow.defn(name="tests.replay.parallel-failure-policy")
class ParallelFailurePolicyWorkflow:
    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def caught_failure(self) -> str | None:
        return self.caught

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [ctx.schedule_activity("slow-first", []), ctx.schedule_activity("fast-second", [])]
        except ActivityFailed as error:
            self.caught = error.activity_type
        return self.caught


def test_absent_policy_keeps_the_failure_handler_suspended_until_all_members_settle() -> None:
    history = json.loads(FIXTURE.read_text())["history"]
    outcome = replay(ParallelFailurePolicyWorkflow, history, [])
    assert not any(isinstance(command, CompleteWorkflow) for command in outcome.commands)
    assert query_state(ParallelFailurePolicyWorkflow, history, [], "caught") is None


def test_absent_policy_preserves_positional_failure_after_a_later_sibling_settles() -> None:
    history = json.loads(FIXTURE.read_text())["history"]
    first = dict(history[1]["payload"])
    first["message"] = "lower position failed later"
    history.append({"event_type": "ActivityFailed", "recorded_at": "2026-01-01T00:00:03Z", "payload": first})
    outcome = replay(ParallelFailurePolicyWorkflow, history, [])
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == "slow-first"
