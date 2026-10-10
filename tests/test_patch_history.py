from __future__ import annotations

from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import CompleteWorkflow, RecordVersionMarker, ScheduleActivity, replay
from tests.test_cooperative_cancellation import marker as cancellation_marker
from tests.test_cooperative_cancellation import request


def version_marker(sequence: int, change_id: str = "same", version: int = 1) -> dict[str, Any]:
    return {"event_type": "VersionMarkerRecorded", "payload": {
        "sequence": sequence, "change_id": change_id, "version": version,
        "min_supported": -1, "max_supported": 1,
    }}


def old_activity(sequence: int, completed: bool = True) -> list[dict[str, Any]]:
    history = [{"event_type": "ActivityScheduled", "payload": {
        "sequence": sequence, "activity_type": "old",
    }}]
    if completed:
        history.append({"event_type": "ActivityCompleted", "payload": {
            "sequence": sequence, "activity_type": "old", "result": serializer.encode("recorded"),
            "payload_codec": "avro",
        }})
    return history


@workflow.defn(name="tests.patch.insertion")
class PatchInsertionWorkflow:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        if (yield ctx.patched("same")):
            yield ctx.schedule_activity("new", [])
        return (yield ctx.schedule_activity("old", []))


@workflow.defn(name="tests.patch.repeated")
class RepeatedPatchWorkflow:
    def run(self, ctx, repeat=True):  # type: ignore[no-untyped-def]
        first = yield ctx.patched("same")
        second = (yield ctx.patched("same")) if repeat else first
        result = yield ctx.schedule_activity("old", [])
        return [first, second, result]


@workflow.defn(name="tests.patch.interleaved")
class InterleavedPatchWorkflow:
    def run(self, ctx, repeat=True):  # type: ignore[no-untyped-def]
        first = yield ctx.patched("same")
        result = yield ctx.schedule_activity("old", [])
        second = (yield ctx.patched("same")) if repeat else first
        return [first, second, result]


@workflow.defn(name="tests.patch.ranges")
class VersionRangeWorkflow:
    def run(self, ctx, minimum=1, mixed=False):  # type: ignore[no-untyped-def]
        first = yield ctx.get_version("same", 1, 2)
        second = (yield ctx.patched("same")) if mixed else (yield ctx.get_version("same", minimum, 3))
        return [first, second]


@workflow.defn(name="tests.patch.cancellation")
class PatchCancellationWorkflow:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        decisions = [(yield ctx.patched("same")), (yield ctx.patched("same"))]
        try:
            yield ctx.schedule_activity("old", [])
        except WorkflowCancelled:
            return [decisions, "cancelled"]


@pytest.mark.parametrize("completed", [False, True])
def test_inserted_patch_preserves_unmarked_activity(completed: bool) -> None:
    outcome = replay(PatchInsertionWorkflow, old_activity(1, completed), [])
    if completed:
        assert len(outcome.commands) == 1
        command = outcome.commands[0]
        assert isinstance(command, CompleteWorkflow)
        assert command.result == "recorded"
    else:
        # The old activity is already scheduled. Await it without duplicating
        # that command or introducing the new patched activity.
        assert outcome.commands == []


def test_repeated_patch_emits_one_marker_on_new_history() -> None:
    commands = replay(RepeatedPatchWorkflow, [], []).commands
    assert [type(command) for command in commands] == [RecordVersionMarker, ScheduleActivity]


@pytest.mark.parametrize("repeat", [False, True])
@pytest.mark.parametrize("duplicates", [False, True])
def test_old_consistent_duplicate_markers_remain_replayable(repeat: bool, duplicates: bool) -> None:
    history = [version_marker(1)]
    if duplicates:
        history.append(version_marker(2))
    history += old_activity(3 if duplicates else 2)
    commands = replay(RepeatedPatchWorkflow, history, [repeat]).commands
    assert len(commands) == 1
    assert isinstance(commands[0], CompleteWorkflow)
    assert commands[0].result == [True, True, "recorded"]


@pytest.mark.parametrize("repeat", [False, True])
def test_duplicate_markers_after_an_activity_remain_replayable(repeat: bool) -> None:
    history = [version_marker(1)] + old_activity(2) + [version_marker(3)]
    commands = replay(InterleavedPatchWorkflow, history, [repeat]).commands
    assert len(commands) == 1
    assert isinstance(commands[0], CompleteWorkflow)
    assert commands[0].result == [True, True, "recorded"]


def test_repeated_version_reuses_first_decision() -> None:
    commands = replay(VersionRangeWorkflow, [], []).commands
    assert [type(command) for command in commands] == [RecordVersionMarker, CompleteWorkflow]
    assert commands[-1].result == [2, 2]


@pytest.mark.parametrize("arguments", [[3], [1, True]])
def test_repeated_version_rejects_incompatible_range_or_family(arguments: list[Any]) -> None:
    with pytest.raises(NonDeterministicReplayError):
        replay(VersionRangeWorkflow, [], arguments)


@pytest.mark.parametrize("version", [-1, 2, True])
def test_conflicting_duplicate_marker_is_rejected(version: Any) -> None:
    history = [version_marker(1), version_marker(2, version=version)] + old_activity(3)
    with pytest.raises(NonDeterministicReplayError):
        replay(RepeatedPatchWorkflow, history, [])


@pytest.mark.parametrize("marker_count", [0, 1, 2])
def test_patch_decisions_preserve_committed_cancellation_sequence(marker_count: int) -> None:
    history = [version_marker(sequence) for sequence in range(1, marker_count + 1)]
    history += old_activity(marker_count + 1, False)
    history += [request(), cancellation_marker(marker_count + 1)]
    commands = replay(PatchCancellationWorkflow, history, [], run_id="run-1").commands
    assert len(commands) == 1
    assert isinstance(commands[0], CompleteWorkflow)
    assert commands[0].result == [[marker_count > 0, marker_count > 0], "cancelled"]
