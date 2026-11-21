"""Parallel groups that follow a selection or applied update (#350)."""

from __future__ import annotations

import json
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import ActivityFailed
from durable_workflow.workflow import WorkflowContext, commands_to_server_commands, replay

FIXTURES = Path(__file__).parent / "fixtures" / "replay_regressions"


@workflow.defn(name="rv348_cancel")
class GroupAfterSelection:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.select({
            "t": ctx.start_timer(1),
            "slow": ctx.schedule_activity("slow", [], retry_policy={"max_attempts": 1}),
        })
        return (yield [ctx.schedule_activity(f"m{i}", [i]) for i in range(2)])


@workflow.defn(name="rv348_plain")
class GroupAfterUpdate:
    @workflow.update("report")
    def report(self, *_: Any) -> dict[str, str]:
        return {"compensating": "failed"}

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        with suppress(ActivityFailed):
            yield [ctx.schedule_activity(f"m{i}", [i], retry_policy={"max_attempts": 1}) for i in range(2)]
        return (yield [ctx.schedule_activity(f"undo{i}", [i]) for i in range(2)])


def fixture(name: str) -> dict[str, Any]:
    with (FIXTURES / name).open() as source:
        return json.load(source)  # type: ignore[no-any-return]


@pytest.mark.parametrize("record", [False, True])
def test_group_after_resolved_selection_schedules_despite_open_loser(record: bool) -> None:
    captured = fixture("failure-order-released-select-cancel.json")
    history = captured["history"]
    resolved = next(i for i, event in enumerate(history) if event["event_type"] == "SelectionResolved")
    state = replay(GroupAfterSelection, history[:resolved + 1], [], payload_codec=serializer.AVRO_CODEC,
                   record_parallel_failure_order=record)
    wire = commands_to_server_commands(state.commands, "q", payload_codec=serializer.AVRO_CODEC)
    groups = [command for command in wire if command["type"] == "schedule_activity"]
    assert [command["activity_type"] for command in groups] == ["m0", "m1"]
    assert [command["parallel_group_base_sequence"] for command in groups] == [3 + int(record)] * 2


def test_group_after_update_applied_schedules_at_the_next_command_sequence() -> None:
    captured = fixture("failure-order-released-update-before-new-worker.json")
    state = replay(GroupAfterUpdate, captured["history"], [], payload_codec=serializer.AVRO_CODEC)
    wire = commands_to_server_commands(state.commands, "q", payload_codec=serializer.AVRO_CODEC)
    groups = [command for command in wire if command["type"] == "schedule_activity"]
    assert [command["activity_type"] for command in groups] == ["undo0", "undo1"]
    assert [command["parallel_group_base_sequence"] for command in groups] == [3, 3]
