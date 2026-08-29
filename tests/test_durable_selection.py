from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, ClassVar

from durable_workflow import serializer, workflow
from durable_workflow.errors import ActivityFailed, NonDeterministicReplayError
from durable_workflow.workflow import (
    CancelDurableOperation,
    CompleteWorkflow,
    DurableOperationHandle,
    FailWorkflow,
    ScheduleActivity,
    SelectionResult,
    WorkflowContext,
    commands_to_server_commands,
    replay,
)


def _selection_metadata(index: int, key: str) -> dict[str, Any]:
    entry = {
        "parallel_group_id": "select-calls:1:2",
        "parallel_group_kind": "activity",
        "parallel_group_mode": "select",
        "parallel_group_base_sequence": 1,
        "parallel_group_size": 2,
        "parallel_group_index": index,
        "selection_member_key": key,
        "selection_member_index": index,
        "selection_member_base_sequence": index + 1,
        "selection_member_size": 1,
        "selection_member_kind": "activity",
    }
    return {**entry, "parallel_group_path": [entry]}


def _activity_completed(index: int, key: str, value: Any) -> dict[str, Any]:
    return {
        "id": f"event-{key}",
        "event_type": "ActivityCompleted",
        "payload": {
            "sequence": index + 1,
            "activity_type": f"{key}-activity",
            "activity_execution_id": f"activity-{key}",
            "result": serializer.envelope(value),
            "payload_codec": serializer.AVRO_CODEC,
            **_selection_metadata(index, key),
        },
    }


def _activity_scheduled(index: int, key: str) -> dict[str, Any]:
    return {
        "id": f"scheduled-{key}",
        "event_type": "ActivityScheduled",
        "payload": {
            "sequence": index + 1,
            "activity_type": f"{key}-activity",
            "activity_execution_id": f"activity-{key}",
            **_selection_metadata(index, key),
        },
    }


def _winner_marker() -> dict[str, Any]:
    return {
        "event_type": "SelectionResolved",
        "payload": {
            "selection_group_id": "select-calls:1:2",
            "selection_group_base_sequence": 1,
            "selection_group_size": 2,
            "member_key": "fast",
            "member_index": 1,
            "member_base_sequence": 2,
            "member_size": 1,
            "operation_kind": "activity",
            "operation_identity": "activity-fast",
            "outcome": "completed",
            "resolution_event_id": "event-fast",
            "resolution_event_type": "ActivityCompleted",
        },
    }


@workflow.defn(name="durable-selection-order")
class DurableSelectionOrderWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        successor = yield ctx.schedule_activity("successor", [])
        slow = yield selected.handles["slow"].await_result()
        return {
            "winner": selected.key,
            "winner_value": selected.result(),
            "successor": successor,
            "slow": slow,
        }


@workflow.defn(name="durable-selection-signal-order")
class DurableSelectionSignalOrderWorkflow:
    def __init__(self) -> None:
        self.signal_value = "before-signal"

    @workflow.signal("change-value")
    def change_value(self, value: str) -> None:
        self.signal_value = value

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        yield ctx.schedule_activity("successor", [self.signal_value])
        return self.signal_value


@workflow.defn(name="durable-selection-terminal")
class DurableSelectionTerminalWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        return {"winner": selected.key, "winner_value": selected.result()}


@workflow.defn(name="durable-selection-removed")
class DurableSelectionRemovedWorkflow:
    def run(self, ctx: WorkflowContext) -> str:
        return "selection-removed"


@workflow.defn(name="durable-consecutive-selection-replay")
class DurableConsecutiveSelectionReplayWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        return (
            yield ctx.select(
                {
                    "next-slow": ctx.schedule_activity("next-slow-activity", []),
                    "next-fast": ctx.schedule_activity("next-fast-activity", []),
                }
            )
        )


@workflow.defn(name="durable-selection-condition-terminal")
class DurableSelectionConditionTerminalWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "condition": ctx.wait_condition(lambda: False, key="ready", timeout=5),
                "work": ctx.schedule_activity("work", []),
            }
        )
        return {"winner": selected.key, "winner_value": selected.result()}


@workflow.defn(name="durable-selection-condition-signal-replay")
class DurableSelectionConditionSignalReplayWorkflow:
    def __init__(self) -> None:
        self.signal_value = "before-signal"

    @workflow.signal("change-value")
    def change_value(self, value: str) -> None:
        self.signal_value = value

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.select(
            {
                "condition": ctx.wait_condition(
                    lambda: self.signal_value == "after-signal",
                    key="ready",
                    timeout=5,
                ),
                "work": ctx.schedule_activity("work", []),
            }
        )
        yield ctx.schedule_activity("successor", [self.signal_value])
        return self.signal_value


@workflow.defn(name="durable-selection-two-condition-signal-replay")
class DurableSelectionTwoConditionSignalReplayWorkflow:
    def __init__(self) -> None:
        self.signal_value = "before-signal"

    @workflow.signal("change-value")
    def change_value(self, value: str) -> None:
        self.signal_value = value

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.select(
            {
                "earlier": ctx.wait_condition(
                    lambda: self.signal_value == "after-signal",
                    key="earlier",
                ),
                "later": ctx.wait_condition(lambda: False, key="later"),
            }
        )
        yield ctx.schedule_activity("successor", [self.signal_value])
        return self.signal_value


@workflow.defn(name="durable-selection-nested-condition-signal-replay")
class DurableSelectionNestedConditionSignalReplayWorkflow:
    def __init__(self) -> None:
        self.signal_values: list[str] = []

    @workflow.signal("change-value")
    def change_value(self, value: str) -> None:
        self.signal_values.append(value)

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.select(
            {
                "earlier-condition": ctx.wait_condition(
                    lambda: False,
                    key="earlier",
                ),
                "condition-group": [
                    ctx.wait_condition(
                        lambda: self.signal_values[-1:] == ["winner"],
                        key="ready",
                    ),
                    ctx.schedule_activity("nested-work", []),
                ],
                "other": ctx.schedule_activity("other-work", []),
            }
        )
        yield ctx.schedule_activity("successor", [list(self.signal_values)])
        return self.signal_values


@workflow.defn(name="durable-selection-await-condition-signal-replay")
class DurableSelectionAwaitConditionSignalReplayWorkflow:
    def __init__(self) -> None:
        self.signal_value = "before-signal"

    @workflow.signal("change-value")
    def change_value(self, value: str) -> None:
        self.signal_value = value

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "condition": ctx.wait_condition(
                    lambda: self.signal_value == "after-signal",
                    key="ready",
                ),
                "winner": ctx.schedule_activity("winner", []),
            }
        )
        condition_result = yield selected.handles["condition"].await_result()
        yield ctx.schedule_activity(
            "successor",
            [condition_result, self.signal_value],
        )
        return self.signal_value


@workflow.defn(name="durable-selection-await-nested-condition-signal-replay")
class DurableSelectionAwaitNestedConditionSignalReplayWorkflow:
    def __init__(self) -> None:
        self.signal_value = "before-signal"

    @workflow.signal("change-value")
    def change_value(self, value: str) -> None:
        self.signal_value = value

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "condition-group": [
                    ctx.wait_condition(
                        lambda: self.signal_value == "after-signal",
                        key="ready",
                    ),
                    ctx.schedule_activity("nested-work", []),
                ],
                "winner": ctx.schedule_activity("winner", []),
            }
        )
        group_result = yield selected.handles["condition-group"].await_result()
        yield ctx.schedule_activity(
            "successor",
            [group_result, self.signal_value],
        )
        return self.signal_value


@workflow.defn(name="durable-selection-interleaved-condition-signal-replay")
class DurableSelectionInterleavedConditionSignalReplayWorkflow:
    def __init__(self) -> None:
        self.signal_values: list[str] = []

    @workflow.signal("change-value")
    def change_value(self, value: str) -> None:
        self.signal_values.append(value)

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.select(
            {
                "earlier": ctx.wait_condition(lambda: False, key="earlier"),
                "winner": ctx.wait_condition(
                    lambda: self.signal_values[-1:] == ["winner-2"],
                    key="winner",
                ),
            }
        )
        yield ctx.schedule_activity("successor", [list(self.signal_values)])
        return self.signal_values


@workflow.defn(name="ordinary-parallel-condition-group")
class OrdinaryParallelConditionGroupWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (
            yield [
                ctx.wait_condition(lambda: False, key="unsupported"),
                ctx.schedule_activity("work", []),
            ]
        )


@workflow.defn(name="durable-selection-cancel")
class DurableSelectionCancelWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        cancelled = yield selected.handles["slow"].cancel()
        return {"winner": selected.key, "cancelled": cancelled}


@workflow.defn(name="durable-selection-cancel-then-await")
class DurableSelectionCancelThenAwaitWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        accepted = yield selected.handles["slow"].cancel()
        slow = yield selected.handles["slow"].await_result()
        return {"accepted": accepted, "slow": slow}


@workflow.defn(name="durable-selection-foreign-handle")
class DurableSelectionForeignHandleWorkflow:
    retained_handle: ClassVar[DurableOperationHandle | None] = None
    retained_action: ClassVar[str] = "await"

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            }
        )
        retained_handle = type(self).retained_handle
        if retained_handle is None:
            type(self).retained_handle = selected.handles["slow"]
            return selected.result()
        if type(self).retained_action == "cancel":
            yield retained_handle.cancel()
        else:
            yield retained_handle.await_result()
        return "foreign handle accepted"


@workflow.defn(name="durable-selection-member-kinds")
class DurableSelectionMemberKindsWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (
            yield ctx.select(
                {
                    "child": ctx.start_child_workflow("child-workflow", []),
                    "timer": ctx.start_timer(5),
                    "condition": ctx.wait_condition(lambda: False, key="ready"),
                    "nested": [
                        ctx.schedule_activity("nested-activity", []),
                        ctx.start_timer(10),
                    ],
                }
            )
        )


@workflow.defn(name="durable-selection-child-identity")
class DurableSelectionChildIdentityWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "child": ctx.start_child_workflow("child-workflow", []),
                "deadline": ctx.start_timer(5),
            }
        )
        yield selected.handles["child"].cancel()
        return selected.identity


@workflow.defn(name="durable-selection-nested-failure")
class DurableSelectionNestedFailureWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "nested": [
                    ctx.schedule_activity("nested-first", []),
                    ctx.schedule_activity("nested-second", []),
                ],
                "deadline": ctx.start_timer(0),
            }
        )
        yield selected.handles["nested"].cancel()
        try:
            yield selected.handles["nested"].await_result()
        except ActivityFailed as failure:
            return {"winner": selected.key, "failure": str(failure)}
        return {"winner": selected.key, "failure": None}


def test_selection_emits_stable_keys_and_starts_every_member() -> None:
    outcome = replay(DurableSelectionOrderWorkflow, [], [])

    assert [type(command) for command in outcome.commands] == [ScheduleActivity, ScheduleActivity]
    wire = commands_to_server_commands(outcome.commands, "default")
    assert [command["selection_member_key"] for command in wire] == ["slow", "fast"]
    assert {command["parallel_group_id"] for command in wire} == {"select-calls:1:2"}
    assert all(command["parallel_group_mode"] == "select" for command in wire)


def test_cold_reload_waits_for_exact_marker_without_reemitting_terminal_members() -> None:
    member_history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(0, "slow", "slow-value"),
        _activity_completed(1, "fast", "fast-value"),
    ]

    waiting = replay(DurableSelectionTerminalWorkflow, member_history, [])
    assert waiting.commands == []

    resolved = replay(DurableSelectionTerminalWorkflow, [*member_history, _winner_marker()], [])
    assert len(resolved.commands) == 1
    assert isinstance(resolved.commands[0], CompleteWorkflow)
    assert resolved.commands[0].result == {"winner": "fast", "winner_value": "fast-value"}


def test_cold_reload_rejects_history_for_a_removed_selection() -> None:
    history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
    ]

    try:
        replay(DurableSelectionRemovedWorkflow, history, [])
    except NonDeterministicReplayError as error:
        assert error.workflow_sequence == 1
        assert "ActivityScheduled" in error.recorded_event_types
    else:
        raise AssertionError("selection replay ignored member history removed from workflow code")


def test_cold_reload_rejects_an_extra_selection_marker() -> None:
    extra_marker = {
        "event_type": "SelectionResolved",
        "payload": {
            "selection_group_id": "select-calls:3:2",
            "selection_group_base_sequence": 3,
            "selection_group_size": 2,
            "member_key": "next-fast",
            "member_index": 1,
            "member_base_sequence": 4,
            "member_size": 1,
            "operation_kind": "activity",
            "operation_identity": "activity-next-fast",
            "outcome": "completed",
            "resolution_event_id": "event-next-fast",
            "resolution_event_type": "ActivityCompleted",
        },
    }
    history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "fast-value"),
        _winner_marker(),
        extra_marker,
    ]

    try:
        replay(DurableSelectionTerminalWorkflow, history, [])
    except NonDeterministicReplayError as error:
        assert error.workflow_sequence == 3
        assert error.recorded_event_types == ["SelectionResolved"]
    else:
        raise AssertionError("selection replay ignored an extra persisted selection marker")


def test_consecutive_selection_replay_advances_past_consumed_member_sequences() -> None:
    history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "fast-value"),
        _winner_marker(),
    ]

    outcome = replay(DurableConsecutiveSelectionReplayWorkflow, history, [])

    assert [type(command) for command in outcome.commands] == [ScheduleActivity, ScheduleActivity]
    wire = commands_to_server_commands(outcome.commands, "default")
    assert [command["activity_type"] for command in wire] == [
        "next-slow-activity",
        "next-fast-activity",
    ]
    assert {command["parallel_group_id"] for command in wire} == {"select-calls:3:2"}
    assert [command["selection_member_base_sequence"] for command in wire] == [3, 4]


def test_cold_reload_resumes_from_winner_before_dispatching_following_signal() -> None:
    history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "fast-value"),
        _winner_marker(),
        {
            "event_type": "SignalReceived",
            "payload": {
                "signal_name": "change-value",
                "value": serializer.envelope(["after-signal"]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
    ]

    outcome = replay(DurableSelectionSignalOrderWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], ScheduleActivity)
    assert outcome.commands[0].activity_type == "successor"
    assert outcome.commands[0].arguments == ["before-signal"]


def test_cold_reload_with_marker_rejects_malformed_selection_member_kind() -> None:
    for malformed_kind in (None, "timer"):
        terminal = _activity_completed(1, "fast", "fast-value")
        if malformed_kind is None:
            terminal["payload"]["parallel_group_path"][0].pop("selection_member_kind")
        else:
            terminal["payload"]["parallel_group_path"][0]["selection_member_kind"] = malformed_kind

        history = [
            _activity_scheduled(0, "slow"),
            _activity_scheduled(1, "fast"),
            terminal,
            _winner_marker(),
        ]
        try:
            replay(DurableSelectionTerminalWorkflow, history, [])
        except NonDeterministicReplayError as error:
            assert "parallel_group_path" in str(error)
            continue
        raise AssertionError(f"selection replay accepted malformed member kind {malformed_kind!r}")


def test_cold_reload_with_marker_rejects_corrupt_scheduled_member_history() -> None:
    for corruption, expected_detail in (
        ("missing member kind", "parallel_group_path"),
        ("wrong member kind", "parallel_group_path"),
        ("wrong operation identity", "canonical operation identity"),
    ):
        scheduled_fast = _activity_scheduled(1, "fast")
        if corruption == "missing member kind":
            scheduled_fast["payload"]["parallel_group_path"][0].pop("selection_member_kind")
        elif corruption == "wrong member kind":
            scheduled_fast["payload"]["parallel_group_path"][0]["selection_member_kind"] = "timer"
        else:
            scheduled_fast["payload"]["activity_execution_id"] = "forged-scheduled-identity"

        history = [
            _activity_scheduled(0, "slow"),
            scheduled_fast,
            _activity_completed(1, "fast", "fast-value"),
            _winner_marker(),
        ]
        try:
            replay(DurableSelectionTerminalWorkflow, history, [])
        except NonDeterministicReplayError as error:
            assert expected_detail in str(error)
            continue
        raise AssertionError(f"selection replay accepted {corruption}")


def _condition_terminal_selection_history(
    event_type: str,
    metadata_variant: str,
) -> list[dict[str, Any]]:
    condition_entry = {
        "parallel_group_id": "select-calls:1:2",
        "parallel_group_kind": "mixed",
        "parallel_group_mode": "select",
        "parallel_group_base_sequence": 1,
        "parallel_group_size": 2,
        "parallel_group_index": 0,
        "selection_member_key": "condition",
        "selection_member_index": 0,
        "selection_member_base_sequence": 1,
        "selection_member_size": 1,
        "selection_member_kind": "condition",
    }
    work_entry = {
        **condition_entry,
        "parallel_group_index": 1,
        "selection_member_key": "work",
        "selection_member_index": 1,
        "selection_member_base_sequence": 2,
        "selection_member_kind": "activity",
    }
    condition_metadata = {
        **condition_entry,
        "parallel_group_path": [condition_entry],
    }
    terminal_payload: dict[str, Any] = {
        "sequence": 1,
        "condition_wait_id": "condition-1",
        "condition_key": "ready",
        "timeout_seconds": 5,
    }
    if event_type == "TimerFired":
        terminal_payload.update(
            {
                "timer_id": "condition-timeout-1",
                "timer_kind": "condition_timeout",
            }
        )
    if metadata_variant != "missing path":
        terminal_payload.update(condition_metadata)
    if metadata_variant == "mismatched path":
        terminal_payload["parallel_group_path"] = [{**condition_entry, "selection_member_kind": "activity"}]
    elif metadata_variant == "mismatched identity":
        terminal_payload["condition_wait_id"] = "condition-forged"

    expected_value = event_type == "ConditionWaitSatisfied"
    return [
        {
            "id": "condition-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-1",
                "condition_key": "ready",
                "timeout_seconds": 5,
                **condition_metadata,
            },
        },
        {
            "id": "work-scheduled",
            "event_type": "ActivityScheduled",
            "payload": {
                "sequence": 2,
                "activity_type": "work",
                "activity_execution_id": "activity-2",
                **work_entry,
                "parallel_group_path": [work_entry],
            },
        },
        {
            "id": "condition-terminal",
            "event_type": event_type,
            "payload": terminal_payload,
        },
        {
            "id": "selection-resolved",
            "event_type": "SelectionResolved",
            "payload": {
                "selection_group_id": "select-calls:1:2",
                "selection_group_base_sequence": 1,
                "selection_group_size": 2,
                "member_key": "condition",
                "member_index": 0,
                "member_base_sequence": 1,
                "member_size": 1,
                "operation_kind": "condition",
                "operation_identity": "condition-1",
                "outcome": "completed",
                "resolution_event_id": "condition-terminal",
                "resolution_event_type": event_type,
                "result": expected_value,
            },
        },
    ]


def test_condition_terminal_selection_metadata_fails_closed_during_cold_replay() -> None:
    terminal_types = (
        "ConditionWaitSatisfied",
        "ConditionWaitTimedOut",
        "TimerFired",
    )
    invalid_variants = {
        "missing path": "terminal condition selection history is missing parallel_group_path",
        "mismatched path": "Recorded parallel_group_path",
        "mismatched identity": "the committed winner has no terminal member history",
    }

    for event_type in terminal_types:
        for metadata_variant, expected_detail in invalid_variants.items():
            try:
                replay(
                    DurableSelectionConditionTerminalWorkflow,
                    _condition_terminal_selection_history(event_type, metadata_variant),
                    [],
                )
            except NonDeterministicReplayError as error:
                assert expected_detail in str(error)
                continue
            raise AssertionError(f"selection replay accepted {event_type} with {metadata_variant}")

        valid = replay(
            DurableSelectionConditionTerminalWorkflow,
            _condition_terminal_selection_history(event_type, "valid"),
            [],
        )
        assert len(valid.commands) == 1
        assert isinstance(valid.commands[0], CompleteWorkflow)
        assert valid.commands[0].result == {
            "winner": "condition",
            "winner_value": event_type == "ConditionWaitSatisfied",
        }


def test_selected_condition_replay_drains_its_satisfying_signal_before_resuming() -> None:
    history = _condition_terminal_selection_history("ConditionWaitSatisfied", "valid")
    history.insert(
        2,
        {
            "event_type": "SignalReceived",
            "payload": {
                "sequence": 1,
                "signal_name": "change-value",
                "value": serializer.envelope(["after-signal"]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
    )

    outcome = replay(DurableSelectionConditionSignalReplayWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], ScheduleActivity)
    assert outcome.commands[0].activity_type == "successor"
    assert outcome.commands[0].arguments == ["after-signal"]


def test_selected_condition_signal_sequence_overrides_open_wait_position() -> None:
    def metadata(index: int, key: str) -> dict[str, Any]:
        entry = {
            "parallel_group_id": "select-calls:1:2",
            "parallel_group_kind": "condition",
            "parallel_group_mode": "select",
            "parallel_group_base_sequence": 1,
            "parallel_group_size": 2,
            "parallel_group_index": index,
            "selection_member_key": key,
            "selection_member_index": index,
            "selection_member_base_sequence": index + 1,
            "selection_member_size": 1,
            "selection_member_kind": "condition",
        }
        return {**entry, "parallel_group_path": [entry]}

    earlier = metadata(0, "earlier")
    later = metadata(1, "later")
    history = [
        {
            "id": "earlier-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-earlier",
                "condition_key": "earlier",
                **earlier,
            },
        },
        {
            "id": "later-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 2,
                "condition_wait_id": "condition-later",
                "condition_key": "later",
                **later,
            },
        },
        {
            "event_type": "SignalReceived",
            "payload": {
                "sequence": 1,
                "signal_name": "change-value",
                "value": serializer.envelope(["after-signal"]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
        {
            "id": "earlier-satisfied",
            "event_type": "ConditionWaitSatisfied",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-earlier",
                "condition_key": "earlier",
                **earlier,
            },
        },
        {
            "id": "selection-resolved",
            "event_type": "SelectionResolved",
            "payload": {
                "selection_group_id": "select-calls:1:2",
                "selection_group_base_sequence": 1,
                "selection_group_size": 2,
                "member_key": "earlier",
                "member_index": 0,
                "member_base_sequence": 1,
                "member_size": 1,
                "operation_kind": "condition",
                "operation_identity": "condition-earlier",
                "outcome": "completed",
                "resolution_event_id": "earlier-satisfied",
                "resolution_event_type": "ConditionWaitSatisfied",
                "result": True,
            },
        },
    ]

    outcome = replay(DurableSelectionTwoConditionSignalReplayWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], ScheduleActivity)
    assert outcome.commands[0].activity_type == "successor"
    assert outcome.commands[0].arguments == ["after-signal"]


def test_selected_nested_condition_replay_drains_receivers_through_winner_boundary() -> None:
    def outer(index: int, *, member: str, member_index: int, member_base: int, member_size: int) -> dict[str, Any]:
        return {
            "parallel_group_id": "select-calls:1:4",
            "parallel_group_kind": "mixed",
            "parallel_group_mode": "select",
            "parallel_group_base_sequence": 1,
            "parallel_group_size": 4,
            "parallel_group_index": index,
            "selection_member_key": member,
            "selection_member_index": member_index,
            "selection_member_base_sequence": member_base,
            "selection_member_size": member_size,
            "selection_member_kind": (
                "group" if member_size > 1 else "condition" if member == "earlier-condition" else "activity"
            ),
        }

    def inner(index: int) -> dict[str, Any]:
        return {
            "parallel_group_id": "parallel-calls:2:2",
            "parallel_group_kind": "mixed",
            "parallel_group_base_sequence": 2,
            "parallel_group_size": 2,
            "parallel_group_index": index,
        }

    earlier_outer = outer(0, member="earlier-condition", member_index=0, member_base=1, member_size=1)
    condition_outer = outer(
        1,
        member="condition-group",
        member_index=1,
        member_base=2,
        member_size=2,
    )
    activity_outer = outer(
        2,
        member="condition-group",
        member_index=1,
        member_base=2,
        member_size=2,
    )
    other_outer = outer(3, member="other", member_index=2, member_base=4, member_size=1)
    condition_inner = inner(0)
    activity_inner = inner(1)
    history = [
        {
            "id": "earlier-condition-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-earlier",
                "condition_key": "earlier",
                **earlier_outer,
                "parallel_group_path": [earlier_outer],
            },
        },
        {
            "event_type": "SignalReceived",
            "payload": {
                "sequence": 1,
                "signal_name": "change-value",
                "value": serializer.envelope(["earlier"]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
        {
            "id": "condition-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 2,
                "condition_wait_id": "condition-winner",
                "condition_key": "ready",
                **condition_inner,
                "parallel_group_path": [condition_outer, condition_inner],
            },
        },
        {
            "id": "nested-work-scheduled",
            "event_type": "ActivityScheduled",
            "payload": {
                "sequence": 3,
                "activity_type": "nested-work",
                "activity_execution_id": "activity-3",
                **activity_inner,
                "parallel_group_path": [activity_outer, activity_inner],
            },
        },
        {
            "id": "other-work-scheduled",
            "event_type": "ActivityScheduled",
            "payload": {
                "sequence": 4,
                "activity_type": "other-work",
                "activity_execution_id": "activity-4",
                **other_outer,
                "parallel_group_path": [other_outer],
            },
        },
        {
            "event_type": "SignalReceived",
            "payload": {
                "sequence": 2,
                "signal_name": "change-value",
                "value": serializer.envelope(["winner"]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
        {
            "id": "condition-satisfied",
            "event_type": "ConditionWaitSatisfied",
            "payload": {
                "sequence": 2,
                "condition_wait_id": "condition-winner",
                "condition_key": "ready",
                **condition_inner,
                "parallel_group_path": [condition_outer, condition_inner],
            },
        },
        {
            "id": "nested-work-completed",
            "event_type": "ActivityCompleted",
            "payload": {
                "sequence": 3,
                "activity_type": "nested-work",
                "activity_execution_id": "activity-3",
                "result": serializer.envelope("nested-result"),
                "payload_codec": serializer.AVRO_CODEC,
                **activity_inner,
                "parallel_group_path": [activity_outer, activity_inner],
            },
        },
        {
            "id": "selection-resolved",
            "event_type": "SelectionResolved",
            "payload": {
                "selection_group_id": "select-calls:1:4",
                "selection_group_base_sequence": 1,
                "selection_group_size": 4,
                "member_key": "condition-group",
                "member_index": 1,
                "member_base_sequence": 2,
                "member_size": 2,
                "operation_kind": "group",
                "operation_identity": "group:2:2",
                "outcome": "completed",
                "resolution_event_id": "nested-work-completed",
                "resolution_event_type": "ActivityCompleted",
            },
        },
        {
            "event_type": "SignalReceived",
            "payload": {
                "sequence": 5,
                "signal_name": "change-value",
                "value": serializer.envelope(["later"]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
    ]

    outcome = replay(DurableSelectionNestedConditionSignalReplayWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], ScheduleActivity)
    assert outcome.commands[0].activity_type == "successor"
    assert outcome.commands[0].arguments == [["earlier", "winner"]]


def test_awaited_non_winning_condition_drains_its_terminal_receiver() -> None:
    condition_entry = {
        "parallel_group_id": "select-calls:1:2",
        "parallel_group_kind": "mixed",
        "parallel_group_mode": "select",
        "parallel_group_base_sequence": 1,
        "parallel_group_size": 2,
        "parallel_group_index": 0,
        "selection_member_key": "condition",
        "selection_member_index": 0,
        "selection_member_base_sequence": 1,
        "selection_member_size": 1,
        "selection_member_kind": "condition",
    }
    winner_entry = {
        **condition_entry,
        "parallel_group_index": 1,
        "selection_member_key": "winner",
        "selection_member_index": 1,
        "selection_member_base_sequence": 2,
        "selection_member_kind": "activity",
    }
    history = [
        {
            "id": "condition-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-1",
                "condition_key": "ready",
                **condition_entry,
                "parallel_group_path": [condition_entry],
            },
        },
        {
            "id": "winner-scheduled",
            "event_type": "ActivityScheduled",
            "payload": {
                "sequence": 2,
                "activity_type": "winner",
                "activity_execution_id": "activity-winner",
                **winner_entry,
                "parallel_group_path": [winner_entry],
            },
        },
        {
            "id": "winner-completed",
            "event_type": "ActivityCompleted",
            "payload": {
                "sequence": 2,
                "activity_type": "winner",
                "activity_execution_id": "activity-winner",
                "result": serializer.envelope("winner-value"),
                "payload_codec": serializer.AVRO_CODEC,
                **winner_entry,
                "parallel_group_path": [winner_entry],
            },
        },
        {
            "event_type": "SelectionResolved",
            "payload": {
                "selection_group_id": "select-calls:1:2",
                "selection_group_base_sequence": 1,
                "selection_group_size": 2,
                "member_key": "winner",
                "member_index": 1,
                "member_base_sequence": 2,
                "member_size": 1,
                "operation_kind": "activity",
                "operation_identity": "activity-winner",
                "outcome": "completed",
                "resolution_event_id": "winner-completed",
                "resolution_event_type": "ActivityCompleted",
            },
        },
        {
            "event_type": "SignalReceived",
            "payload": {
                "sequence": 1,
                "signal_name": "change-value",
                "value": serializer.envelope(["after-signal"]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
        {
            "id": "condition-satisfied",
            "event_type": "ConditionWaitSatisfied",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-1",
                "condition_key": "ready",
                **condition_entry,
                "parallel_group_path": [condition_entry],
            },
        },
    ]

    outcome = replay(DurableSelectionAwaitConditionSignalReplayWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], ScheduleActivity)
    assert outcome.commands[0].activity_type == "successor"
    assert outcome.commands[0].arguments == [True, "after-signal"]


def test_awaited_non_winning_nested_condition_drains_its_terminal_receiver() -> None:
    condition_outer = {
        "parallel_group_id": "select-calls:1:3",
        "parallel_group_kind": "mixed",
        "parallel_group_mode": "select",
        "parallel_group_base_sequence": 1,
        "parallel_group_size": 3,
        "parallel_group_index": 0,
        "selection_member_key": "condition-group",
        "selection_member_index": 0,
        "selection_member_base_sequence": 1,
        "selection_member_size": 2,
        "selection_member_kind": "group",
    }
    nested_activity_outer = {**condition_outer, "parallel_group_index": 1}
    winner_outer = {
        **condition_outer,
        "parallel_group_index": 2,
        "selection_member_key": "winner",
        "selection_member_index": 1,
        "selection_member_base_sequence": 3,
        "selection_member_size": 1,
        "selection_member_kind": "activity",
    }
    condition_inner = {
        "parallel_group_id": "parallel-calls:1:2",
        "parallel_group_kind": "mixed",
        "parallel_group_base_sequence": 1,
        "parallel_group_size": 2,
        "parallel_group_index": 0,
    }
    nested_activity_inner = {**condition_inner, "parallel_group_index": 1}
    history = [
        {
            "id": "condition-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-1",
                "condition_key": "ready",
                **condition_inner,
                "parallel_group_path": [condition_outer, condition_inner],
            },
        },
        {
            "id": "nested-work-scheduled",
            "event_type": "ActivityScheduled",
            "payload": {
                "sequence": 2,
                "activity_type": "nested-work",
                "activity_execution_id": "activity-nested",
                **nested_activity_inner,
                "parallel_group_path": [nested_activity_outer, nested_activity_inner],
            },
        },
        {
            "id": "winner-scheduled",
            "event_type": "ActivityScheduled",
            "payload": {
                "sequence": 3,
                "activity_type": "winner",
                "activity_execution_id": "activity-winner",
                **winner_outer,
                "parallel_group_path": [winner_outer],
            },
        },
        {
            "id": "winner-completed",
            "event_type": "ActivityCompleted",
            "payload": {
                "sequence": 3,
                "activity_type": "winner",
                "activity_execution_id": "activity-winner",
                "result": serializer.envelope("winner-value"),
                "payload_codec": serializer.AVRO_CODEC,
                **winner_outer,
                "parallel_group_path": [winner_outer],
            },
        },
        {
            "event_type": "SelectionResolved",
            "payload": {
                "selection_group_id": "select-calls:1:3",
                "selection_group_base_sequence": 1,
                "selection_group_size": 3,
                "member_key": "winner",
                "member_index": 1,
                "member_base_sequence": 3,
                "member_size": 1,
                "operation_kind": "activity",
                "operation_identity": "activity-winner",
                "outcome": "completed",
                "resolution_event_id": "winner-completed",
                "resolution_event_type": "ActivityCompleted",
            },
        },
        {
            "event_type": "SignalReceived",
            "payload": {
                "sequence": 1,
                "signal_name": "change-value",
                "value": serializer.envelope(["after-signal"]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
        {
            "id": "condition-satisfied",
            "event_type": "ConditionWaitSatisfied",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-1",
                "condition_key": "ready",
                **condition_inner,
                "parallel_group_path": [condition_outer, condition_inner],
            },
        },
        {
            "id": "nested-work-completed",
            "event_type": "ActivityCompleted",
            "payload": {
                "sequence": 2,
                "activity_type": "nested-work",
                "activity_execution_id": "activity-nested",
                "result": serializer.envelope("nested-value"),
                "payload_codec": serializer.AVRO_CODEC,
                **nested_activity_inner,
                "parallel_group_path": [nested_activity_outer, nested_activity_inner],
            },
        },
    ]

    outcome = replay(DurableSelectionAwaitNestedConditionSignalReplayWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], ScheduleActivity)
    assert outcome.commands[0].activity_type == "successor"
    assert outcome.commands[0].arguments == [[True, "nested-value"], "after-signal"]


def test_selected_condition_drains_interleaved_receivers_through_terminal_boundary() -> None:
    def metadata(index: int, key: str) -> dict[str, Any]:
        entry = {
            "parallel_group_id": "select-calls:1:2",
            "parallel_group_kind": "condition",
            "parallel_group_mode": "select",
            "parallel_group_base_sequence": 1,
            "parallel_group_size": 2,
            "parallel_group_index": index,
            "selection_member_key": key,
            "selection_member_index": index,
            "selection_member_base_sequence": index + 1,
            "selection_member_size": 1,
            "selection_member_kind": "condition",
        }
        return {**entry, "parallel_group_path": [entry]}

    earlier = metadata(0, "earlier")
    winner = metadata(1, "winner")
    history = [
        {
            "id": "earlier-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 1,
                "condition_wait_id": "condition-earlier",
                "condition_key": "earlier",
                **earlier,
            },
        },
        {
            "id": "winner-opened",
            "event_type": "ConditionWaitOpened",
            "payload": {
                "sequence": 2,
                "condition_wait_id": "condition-winner",
                "condition_key": "winner",
                **winner,
            },
        },
    ]
    for sequence, value in (
        (2, "winner-1"),
        (1, "interleaved"),
        (2, "winner-2"),
    ):
        history.append(
            {
                "event_type": "SignalReceived",
                "payload": {
                    "sequence": sequence,
                    "signal_name": "change-value",
                    "value": serializer.envelope([value]),
                    "payload_codec": serializer.AVRO_CODEC,
                },
            }
        )
    history.extend(
        [
            {
                "id": "winner-satisfied",
                "event_type": "ConditionWaitSatisfied",
                "payload": {
                    "sequence": 2,
                    "condition_wait_id": "condition-winner",
                    "condition_key": "winner",
                    **winner,
                },
            },
            {
                "event_type": "SelectionResolved",
                "payload": {
                    "selection_group_id": "select-calls:1:2",
                    "selection_group_base_sequence": 1,
                    "selection_group_size": 2,
                    "member_key": "winner",
                    "member_index": 1,
                    "member_base_sequence": 2,
                    "member_size": 1,
                    "operation_kind": "condition",
                    "operation_identity": "condition-winner",
                    "outcome": "completed",
                    "resolution_event_id": "winner-satisfied",
                    "resolution_event_type": "ConditionWaitSatisfied",
                    "result": True,
                },
            },
            {
                "event_type": "SignalReceived",
                "payload": {
                    "sequence": 3,
                    "signal_name": "change-value",
                    "value": serializer.envelope(["later"]),
                    "payload_codec": serializer.AVRO_CODEC,
                },
            },
        ]
    )

    outcome = replay(DurableSelectionInterleavedConditionSignalReplayWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], ScheduleActivity)
    assert outcome.commands[0].activity_type == "successor"
    assert outcome.commands[0].arguments == [["winner-1", "interleaved", "winner-2"]]


def test_ordinary_parallel_group_rejects_wait_condition_locally() -> None:
    outcome = replay(OrdinaryParallelConditionGroupWorkflow, [], [])

    assert len(outcome.commands) == 1
    failure = outcome.commands[0]
    assert isinstance(failure, FailWorkflow)
    assert failure.exception_type == "TypeError"
    assert failure.message == (
        "ordinary parallel list groups do not support WaitCondition; "
        "use WorkflowContext.select() for durable condition selection"
    )


def test_selection_key_domain_rejects_empty_and_negative_keys_but_preserves_valid_keys() -> None:
    ctx = WorkflowContext()

    for key in ("", -1):
        try:
            ctx.select({key: ctx.schedule_activity("work", [])})
        except ValueError as error:
            assert "non-empty strings or non-negative integers" in str(error)
        else:
            raise AssertionError(f"selection accepted invalid member key {key!r}")

    selected = ctx.select(
        {
            0: ctx.schedule_activity("numeric", []),
            "named": ctx.start_timer(1),
        }
    )
    assert [key for key, _ in selected.operations] == [0, "named"]


def test_selection_replay_rejects_out_of_domain_recorded_member_keys() -> None:
    for key in ("", -1):
        malformed = _activity_scheduled(0, "slow")
        malformed["payload"]["selection_member_key"] = key
        malformed["payload"]["parallel_group_path"][0]["selection_member_key"] = key

        try:
            replay(DurableSelectionOrderWorkflow, [malformed], [])
        except NonDeterministicReplayError as error:
            assert "invalid member key" in str(error)
        else:
            raise AssertionError(f"selection replay accepted invalid member key {key!r}")


def test_selection_supports_child_timer_condition_and_nested_group_members() -> None:
    outcome = replay(DurableSelectionMemberKindsWorkflow, [], [])
    wire = commands_to_server_commands(outcome.commands, "default")

    assert [command["type"] for command in wire] == [
        "start_child_workflow",
        "start_timer",
        "open_condition_wait",
        "schedule_activity",
        "start_timer",
    ]
    assert [command["parallel_group_path"][0]["selection_member_key"] for command in wire] == [
        "child",
        "timer",
        "condition",
        "nested",
        "nested",
    ]
    nested = wire[-2:]
    assert all(command["parallel_group_path"][0]["selection_member_size"] == 2 for command in nested)
    assert all(command["parallel_group_path"][0]["selection_member_kind"] == "group" for command in nested)
    assert all(len(command["parallel_group_path"]) == 2 for command in nested)


def test_child_handle_uses_run_identity_when_instance_and_run_are_present() -> None:
    child_entry = {
        "parallel_group_id": "select-calls:1:2",
        "parallel_group_kind": "mixed",
        "parallel_group_mode": "select",
        "parallel_group_base_sequence": 1,
        "parallel_group_size": 2,
        "parallel_group_index": 0,
        "selection_member_key": "child",
        "selection_member_index": 0,
        "selection_member_base_sequence": 1,
        "selection_member_size": 1,
        "selection_member_kind": "child",
    }
    timer_entry = {
        **child_entry,
        "parallel_group_index": 1,
        "selection_member_key": "deadline",
        "selection_member_index": 1,
        "selection_member_base_sequence": 2,
        "selection_member_kind": "timer",
    }
    history = [
        {
            "id": "child-open",
            "event_type": "ChildWorkflowScheduled",
            "payload": {
                "sequence": 1,
                "child_workflow_type": "child-workflow",
                "child_workflow_instance_id": "child-instance",
                "child_workflow_run_id": "child-run",
                **child_entry,
                "parallel_group_path": [child_entry],
            },
        },
        {
            "id": "timer-open",
            "event_type": "TimerScheduled",
            "payload": {
                "sequence": 2,
                "timer_id": "deadline-timer",
                **timer_entry,
                "parallel_group_path": [timer_entry],
            },
        },
        {
            "id": "timer-fired",
            "event_type": "TimerFired",
            "payload": {
                "sequence": 2,
                "timer_id": "deadline-timer",
                **timer_entry,
                "parallel_group_path": [timer_entry],
            },
        },
        {
            "event_type": "SelectionResolved",
            "payload": {
                "selection_group_id": "select-calls:1:2",
                "selection_group_base_sequence": 1,
                "selection_group_size": 2,
                "member_key": "deadline",
                "member_index": 1,
                "member_base_sequence": 2,
                "member_size": 1,
                "operation_kind": "timer",
                "operation_identity": "deadline-timer",
                "outcome": "completed",
                "resolution_event_id": "timer-fired",
                "resolution_event_type": "TimerFired",
            },
        },
    ]

    outcome = replay(DurableSelectionChildIdentityWorkflow, history, [])
    wire = commands_to_server_commands(outcome.commands, "default")

    assert wire[0]["type"] == "cancel_selection_operation"
    assert wire[0]["operation_identity"] == "child-run"


def test_activity_selection_identity_requires_canonical_execution_id() -> None:
    scheduled_slow = _activity_scheduled(0, "slow")
    scheduled_fast = _activity_scheduled(1, "fast")
    completed_fast = _activity_completed(1, "fast", "winner-value")
    for event in (scheduled_fast, completed_fast):
        event["payload"].pop("activity_execution_id")
        event["payload"]["activity_id"] = "forged-activity-id"
    marker = _winner_marker()
    marker["payload"]["operation_identity"] = "forged-activity-id"

    try:
        replay(
            DurableSelectionOrderWorkflow,
            [scheduled_slow, scheduled_fast, completed_fast, marker],
            [],
        )
    except NonDeterministicReplayError as error:
        assert "canonical operation identity" in str(error)
    else:
        raise AssertionError("selection replay accepted noncanonical activity_id identity")


def test_loser_completion_does_not_reorder_successor_and_can_be_awaited_later() -> None:
    history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "winner-value"),
        _winner_marker(),
        _activity_completed(0, "slow", "loser-value"),
        {
            "id": "event-successor",
            "event_type": "ActivityCompleted",
            "payload": {
                "sequence": 3,
                "activity_type": "successor",
                "activity_execution_id": "activity-successor",
                "result": serializer.envelope("successor-value"),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
    ]

    outcome = replay(DurableSelectionOrderWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == {
        "winner": "fast",
        "winner_value": "winner-value",
        "successor": "successor-value",
        "slow": "loser-value",
    }


def test_fresh_process_replays_persisted_winner_and_awaits_loser() -> None:
    root = Path(__file__).resolve().parents[1]
    history_path = root / "tests/fixtures/durable_selection_runtime_history.json"
    fixture_bytes = history_path.read_bytes()
    assert (
        hashlib.sha256(fixture_bytes).hexdigest() == "51fd8b9c16e978dcef536a5c727b9fdc0ae724d9afc17d9a7837d219f41ee3ba"
    )
    fixture = json.loads(fixture_bytes)
    history = fixture["history"]
    fast_open = next(
        event for event in history if event["event_type"] == "ActivityScheduled" and event["payload"]["sequence"] == 2
    )
    fast_completion = next(
        event for event in history if event["event_type"] == "ActivityCompleted" and event["payload"]["sequence"] == 2
    )
    winner = next(event for event in history if event["event_type"] == "SelectionResolved")
    assert winner["payload"]["operation_identity"] == fast_open["payload"]["activity_execution_id"]
    assert winner["payload"]["resolution_event_id"] == fast_completion["id"]

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(root / "src"), env.get("PYTHONPATH")]))

    cold_replay = subprocess.run(
        [sys.executable, str(root / "tests/fixtures/durable_selection_cold_replay.py"), str(history_path)],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    observed = json.loads(cold_replay.stdout)

    assert observed == {"process_id": observed["process_id"], **fixture["expected"]}
    assert observed["process_id"] != os.getpid()


def test_selection_cancellation_is_explicit_and_idempotent_on_replay() -> None:
    history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "winner-value"),
        _winner_marker(),
    ]

    first = replay(DurableSelectionCancelWorkflow, history, [])
    assert isinstance(first.commands[0], CancelDurableOperation)
    assert isinstance(first.commands[1], CompleteWorkflow)
    wire = commands_to_server_commands(first.commands, "default")
    assert wire[0] == {
        "type": "cancel_selection_operation",
        "selection_group_id": "select-calls:1:2",
        "member_key": "slow",
        "member_index": 0,
        "member_base_sequence": 1,
        "member_size": 1,
        "operation_kind": "activity",
        "operation_identity": "activity-slow",
    }

    replayed = replay(
        DurableSelectionCancelWorkflow,
        history
        + [
            {
                "event_type": "SelectionOperationCancelled",
                "payload": {
                    "selection_group_id": "select-calls:1:2",
                    "member_key": "slow",
                    "member_index": 0,
                    "member_base_sequence": 1,
                    "member_size": 1,
                    "operation_kind": "activity",
                    "operation_identity": "activity-slow",
                },
            }
        ],
        [],
    )
    assert len(replayed.commands) == 1
    assert isinstance(replayed.commands[0], CompleteWorkflow)


def test_completion_before_cancellation_remains_awaitable() -> None:
    history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "winner-value"),
        _winner_marker(),
        _activity_completed(0, "slow", "completed-first"),
    ]

    outcome = replay(DurableSelectionCancelThenAwaitWorkflow, history, [])

    assert [type(command) for command in outcome.commands] == [CancelDurableOperation, CompleteWorkflow]
    assert outcome.commands[1].result == {"accepted": None, "slow": "completed-first"}


def test_selection_handles_are_bound_to_the_replay_that_authored_them() -> None:
    history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "winner-value"),
        _winner_marker(),
        _activity_completed(0, "slow", "current-run-slow-value"),
    ]

    for action in ("await", "cancel"):
        DurableSelectionForeignHandleWorkflow.retained_handle = None
        DurableSelectionForeignHandleWorkflow.retained_action = action

        authored = replay(DurableSelectionForeignHandleWorkflow, history, [])
        assert len(authored.commands) == 1
        assert isinstance(authored.commands[0], CompleteWorkflow)
        assert authored.commands[0].result == "winner-value"

        rejected = replay(DurableSelectionForeignHandleWorkflow, history, [])
        assert len(rejected.commands) == 1
        failure = rejected.commands[0]
        assert isinstance(failure, FailWorkflow)
        assert failure.exception_type == "ValueError"
        assert "current workflow replay" in failure.message


def test_cancellation_marker_is_bound_to_every_authored_handle_field() -> None:
    base_history = [
        _activity_scheduled(0, "slow"),
        _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "winner-value"),
        _winner_marker(),
    ]
    marker = {
        "selection_group_id": "select-calls:1:2",
        "member_key": "slow",
        "member_index": 0,
        "member_base_sequence": 1,
        "member_size": 1,
        "operation_kind": "activity",
        "operation_identity": "activity-slow",
    }

    for field_name, corrupt_value in [
        ("member_key", "fast"),
        ("member_index", 1),
        ("member_base_sequence", 3),
        ("member_size", 2),
        ("operation_kind", "timer"),
        ("operation_identity", "forged"),
    ]:
        corrupt = dict(marker)
        corrupt[field_name] = corrupt_value
        try:
            replay(
                DurableSelectionCancelWorkflow,
                base_history + [{"event_type": "SelectionOperationCancelled", "payload": corrupt}],
                [],
            )
        except NonDeterministicReplayError:
            continue
        raise AssertionError(f"corrupt cancellation field {field_name} was accepted")


def test_nested_later_failure_before_cancel_remains_the_awaited_failure() -> None:
    def outer(flat_index: int) -> dict[str, Any]:
        return {
            "parallel_group_id": "select-calls:1:3",
            "parallel_group_kind": "mixed",
            "parallel_group_mode": "select",
            "parallel_group_base_sequence": 1,
            "parallel_group_size": 3,
            "parallel_group_index": flat_index,
            "selection_member_key": "nested",
            "selection_member_index": 0,
            "selection_member_base_sequence": 1,
            "selection_member_size": 2,
            "selection_member_kind": "group",
        }

    def inner(index: int) -> dict[str, Any]:
        return {
            "parallel_group_id": "parallel-activities:1:2",
            "parallel_group_kind": "activity",
            "parallel_group_base_sequence": 1,
            "parallel_group_size": 2,
            "parallel_group_index": index,
        }

    deadline = {
        "parallel_group_id": "select-calls:1:3",
        "parallel_group_kind": "mixed",
        "parallel_group_mode": "select",
        "parallel_group_base_sequence": 1,
        "parallel_group_size": 3,
        "parallel_group_index": 2,
        "selection_member_key": "deadline",
        "selection_member_index": 1,
        "selection_member_base_sequence": 3,
        "selection_member_size": 1,
        "selection_member_kind": "timer",
    }
    histories: list[dict[str, Any]] = []
    for index, activity_type in enumerate(["nested-first", "nested-second"]):
        path = [outer(index), inner(index)]
        histories.append(
            {
                "id": f"scheduled-{index + 1}",
                "event_type": "ActivityScheduled",
                "payload": {
                    "sequence": index + 1,
                    "activity_type": activity_type,
                    "activity_execution_id": f"activity-{index + 1}",
                    **inner(index),
                    "parallel_group_path": path,
                },
            }
        )
    histories.extend(
        [
            {
                "id": "timer-open",
                "event_type": "TimerScheduled",
                "payload": {"sequence": 3, "timer_id": "timer-3", **deadline, "parallel_group_path": [deadline]},
            },
            {
                "id": "timer-fired",
                "event_type": "TimerFired",
                "payload": {"sequence": 3, "timer_id": "timer-3", **deadline, "parallel_group_path": [deadline]},
            },
            {
                "event_type": "SelectionResolved",
                "payload": {
                    "selection_group_id": "select-calls:1:3",
                    "selection_group_base_sequence": 1,
                    "selection_group_size": 3,
                    "member_key": "deadline",
                    "member_index": 1,
                    "member_base_sequence": 3,
                    "member_size": 1,
                    "operation_kind": "timer",
                    "operation_identity": "timer-3",
                    "outcome": "completed",
                    "resolution_event_id": "timer-fired",
                    "resolution_event_type": "TimerFired",
                },
            },
            {
                "id": "nested-failure",
                "event_type": "ActivityFailed",
                "payload": {
                    "sequence": 2,
                    "activity_type": "nested-second",
                    "activity_execution_id": "activity-2",
                    "message": "nested later failure",
                    "exception_type": "NestedFailure",
                    **inner(1),
                    "parallel_group_path": [outer(1), inner(1)],
                },
            },
        ]
    )

    outcome = replay(DurableSelectionNestedFailureWorkflow, histories, [])

    assert [type(command) for command in outcome.commands] == [CancelDurableOperation, CompleteWorkflow]
    assert outcome.commands[1].result == {"winner": "deadline", "failure": "nested later failure"}
