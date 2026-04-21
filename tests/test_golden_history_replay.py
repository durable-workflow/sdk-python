from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import Replayer, workflow
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, WorkflowContext

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "golden_history" / "python-sdk-v0.json"


@workflow.defn(name="golden.single-activity")
class GoldenSingleActivityWorkflow:
    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        return (yield ctx.schedule_activity("golden.greet", [name]))


@workflow.defn(name="golden.signal-wait")
class GoldenSignalWaitWorkflow:
    def __init__(self) -> None:
        self.approved_by: str | None = None

    @workflow.signal("approve")
    def approve(self, approved_by: str) -> None:
        self.approved_by = approved_by

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        satisfied = yield ctx.wait_condition(
            lambda: self.approved_by is not None,
            key="approval",
            timeout=30,
        )
        if not satisfied:
            return "timed-out"

        activity_result = yield ctx.schedule_activity("golden.notify", [self.approved_by])
        return {
            "approved_by": self.approved_by,
            "activity_result": activity_result,
        }


@workflow.defn(name="golden.timeout-wait")
class GoldenTimeoutWaitWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        if not (yield ctx.wait_condition(lambda: False, key="approval", timeout=5)):
            return "timed-out"

        return "satisfied"


@workflow.defn(name="golden.version-marker")
class GoldenVersionMarkerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        version = yield ctx.get_version("golden-version", 1, 2)
        activity = "golden.version-new-path" if version >= 2 else "golden.version-old-path"
        return (yield ctx.schedule_activity(activity, []))


def _golden_cases() -> list[dict[str, Any]]:
    with FIXTURE_PATH.open() as handle:
        cases = json.load(handle)

    assert isinstance(cases, list)
    return cases


@pytest.mark.parametrize("case", _golden_cases(), ids=lambda case: str(case["name"]))
def test_golden_history_replay_contract(case: dict[str, Any]) -> None:
    replayer = Replayer(
        workflows=[
            GoldenSingleActivityWorkflow,
            GoldenSignalWaitWorkflow,
            GoldenTimeoutWaitWorkflow,
            GoldenVersionMarkerWorkflow,
        ],
    )

    outcome = replayer.replay(
        case["history"],
        case["start_input"],
        workflow_type=case["workflow_type"],
    )

    assert len(outcome.commands) == 1
    expected = case["expected"]
    command = outcome.commands[0]

    if expected["command_type"] == "CompleteWorkflow":
        assert isinstance(command, CompleteWorkflow)
        assert command.result == expected["result"]

        return

    if expected["command_type"] == "ScheduleActivity":
        assert isinstance(command, ScheduleActivity)
        assert command.activity_type == expected["activity_type"]
        assert command.arguments == expected["arguments"]

        return

    raise AssertionError(f"Unsupported golden expected command type: {expected['command_type']}")
