"""Replay of histories whose run close settled still-open work (server#1193).

Workflow 2.28.0 (workflow#1117, workflow#1121) and Server (server#1193) record
``ActivityCancelled`` with ``cancellation_reason: run_closed`` for each
activity a run never awaited when its own code completes, fails or continues
as new, in the closing transaction just before the terminal event. Pending
timers get ``TimerCancelled``. An activity scheduled with the abandon policy is
listed in the terminal event's ``abandoned_activities`` instead.

The workflow had already moved past those operations, so replay must never
treat such a cancellation as an outcome: it must not resolve a ``select()``
member and must not replace the failure an ``all()`` group raised.

The event shapes below are copied from workflow#1121: the corpus fixture
``selection-loser-settled-at-close.json`` and ``ActivityCancellation::record``.
"""

from __future__ import annotations

from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import ActivityFailed
from durable_workflow.workflow import (
    CompleteWorkflow,
    ContinueAsNew,
    FailWorkflow,
    SelectionResult,
    WorkflowContext,
    query_state,
    replay,
)

RUN_ID = "01m795h48k4a44yjrzbhx5hvaa"
INSTANCE_ID = "gen-1117"
CLOSED_AT = "2026-11-13T00:39:42.150118Z"


def _started(workflow_type: str) -> list[dict[str, Any]]:
    return [
        {
            "id": "01m795h48s87krk3abrcesqf39",
            "sequence": 1,
            "event_type": "StartAccepted",
            "payload": {
                "workflow_command_id": "01m795h48pxb9ks6zxaxxzcxkg",
                "workflow_instance_id": INSTANCE_ID,
                "workflow_run_id": RUN_ID,
                "workflow_class": workflow_type,
                "workflow_type": workflow_type,
                "business_key": None,
                "visibility_labels": None,
                "memo": None,
                "search_attributes": None,
                "outcome": "started_new",
            },
            "recorded_at": "2026-11-13T00:39:42.103872Z",
        },
        {
            "id": "01m795h48vrbq18nwm3g08va0x",
            "sequence": 2,
            "event_type": "WorkflowStarted",
            "payload": {
                "workflow_class": workflow_type,
                "workflow_type": workflow_type,
                "workflow_instance_id": INSTANCE_ID,
                "workflow_run_id": RUN_ID,
                "workflow_command_id": "01m795h48pxb9ks6zxaxxzcxkg",
                "business_key": None,
                "visibility_labels": None,
                "memo": None,
                "search_attributes": None,
                "execution_timeout_seconds": None,
                "run_timeout_seconds": None,
                "execution_deadline_at": None,
                "run_deadline_at": None,
                "declared_queries": [],
                "declared_query_contracts": [],
                "declared_signals": [],
                "declared_signal_contracts": [],
                "declared_updates": [],
                "declared_update_contracts": [],
                "declared_entry_method": "handle",
                "declared_entry_mode": "canonical",
            },
            "recorded_at": "2026-11-13T00:39:42.107521Z",
        },
    ]


def _member(
    *,
    group_id: str,
    kind: str,
    size: int,
    index: int,
    key: str | None = None,
    member_index: int | None = None,
    member_base: int | None = None,
    member_size: int = 1,
    member_kind: str | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "parallel_group_id": group_id,
        "parallel_group_kind": kind,
    }
    if key is not None:
        entry["parallel_group_mode"] = "select"
    entry.update(
        {
            "parallel_group_base_sequence": 1,
            "parallel_group_size": size,
            "parallel_group_index": index,
        }
    )
    if key is not None:
        entry.update(
            {
                "selection_member_key": key,
                "selection_member_index": member_index,
                "selection_member_base_sequence": member_base,
                "selection_member_size": member_size,
                "selection_member_kind": member_kind,
            }
        )
    return entry


def _with_path(path: list[dict[str, Any]]) -> dict[str, Any]:
    return {**path[-1], "parallel_group_path": path}


def _activity_snapshot(
    execution_id: str, activity_type: str, sequence: int, status: str, path: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "id": execution_id,
        "idempotency_key": execution_id,
        "sequence": sequence,
        "type": activity_type,
        "class": activity_type,
        "local_activity": False,
        "status": status,
        "payload_codec": "avro",
        "attempt_count": 0,
        "retry_policy": {
            "snapshot_version": 1,
            "max_attempts": 1,
            "backoff_seconds": [1, 2, 5, 10, 15, 30, 60, 120],
            "start_to_close_timeout": None,
            "schedule_to_start_timeout": None,
            "schedule_to_close_timeout": None,
            "heartbeat_timeout": None,
            "non_retryable_error_types": [],
        },
        "connection": "redis",
        "queue": "default",
        "created_at": "2026-11-13T00:39:42.143497Z",
        "arguments": serializer.envelope([])["blob"],
        **_with_path(path),
    }


def _activity_scheduled(
    event_id: str, sequence: int, execution_id: str, activity_type: str, path: list[dict[str, Any]],
    *, recorded_at: str = "2026-11-13T00:39:42.144915Z",
) -> dict[str, Any]:
    return {
        "id": event_id,
        "event_type": "ActivityScheduled",
        "payload": {
            "activity_execution_id": execution_id,
            "activity_class": activity_type,
            "activity_type": activity_type,
            "sequence": sequence,
            "activity": _activity_snapshot(execution_id, activity_type, sequence, "pending", path),
            **_with_path(path),
        },
        "recorded_at": recorded_at,
    }


def _activity_cancelled_at_close(
    event_id: str, sequence: int, execution_id: str, activity_type: str, path: list[dict[str, Any]],
    *, recorded_at: str = CLOSED_AT,
) -> dict[str, Any]:
    """``ActivityCancellation::record`` with ``cancellationReason: run_closed``."""
    return {
        "id": event_id,
        "event_type": "ActivityCancelled",
        "payload": {
            "workflow_command_id": None,
            "activity_execution_id": execution_id,
            "activity_attempt_id": None,
            "activity_class": activity_type,
            "activity_type": activity_type,
            "sequence": sequence,
            "attempt_number": 0,
            "cancelled_at": recorded_at,
            "activity": _activity_snapshot(execution_id, activity_type, sequence, "cancelled", path),
            "activity_attempt": None,
            "cancellation_reason": "run_closed",
            **_with_path(path),
        },
        "recorded_at": recorded_at,
    }


# --- select(): the loser is cancelled when the run closes -------------------

SELECT_WORK = [
    _member(
        group_id="select-calls:1:2", kind="mixed", size=2, index=0,
        key="work", member_index=0, member_base=1, member_kind="activity",
    )
]
SELECT_DEADLINE = [
    _member(
        group_id="select-calls:1:2", kind="mixed", size=2, index=1,
        key="deadline", member_index=1, member_base=2, member_kind="timer",
    )
]


@workflow.defn(name="test-selection-loser-at-close-workflow")
class SelectionLoserAtCloseWorkflow:
    """Python port of workflow#1121's ``TestSelectionLoserAtCloseWorkflow``."""

    def run(self, ctx: WorkflowContext, mode: str, round: int = 0):  # type: ignore[no-untyped-def]
        if round > 0:
            return "continued"
        selected: SelectionResult = yield ctx.select(
            {
                "work": ctx.schedule_activity("Tests\\Fixtures\\V2\\TestGreetingActivity", [mode]),
                "deadline": ctx.start_timer(0),
            }
        )
        if mode == "fail":
            raise RuntimeError("Closing with an open select() loser.")
        if mode == "continue":
            return (yield ctx.continue_as_new(mode, 1))
        return "closed-with-" + selected.key


def _selection_history_before_close() -> list[dict[str, Any]]:
    """The ``history`` of ``selection-loser-settled-at-close.json``."""
    timer_fields = {"timer_id": "01m795h4a2vty5vv2b0j103gpv", "sequence": 2, "delay_seconds": 0}
    return [
        *_started("test-selection-loser-at-close-workflow"),
        {
            **_activity_scheduled(
                "01m795h4a14c2beezexqk6zwqd", 1, "01m795h49zm0xftwdqam8nqj81",
                "Tests\\Fixtures\\V2\\TestGreetingActivity", SELECT_WORK,
            ),
            "sequence": 3,
        },
        {
            "id": "01m795h4a344awn80w7wnvztxr",
            "sequence": 4,
            "event_type": "TimerScheduled",
            "payload": {
                **timer_fields,
                "fire_at": "2026-11-13T00:39:42.146411Z",
                **_with_path(SELECT_DEADLINE),
            },
            "recorded_at": "2026-11-13T00:39:42.146930Z",
        },
        {
            "id": "01m795h4a466hy5ckc65z8bgwa",
            "sequence": 5,
            "event_type": "TimerFired",
            "payload": {
                **timer_fields,
                "fired_at": "2026-11-13T00:39:42.146411Z",
                **_with_path(SELECT_DEADLINE),
            },
            "recorded_at": "2026-11-13T00:39:42.147973Z",
        },
        {
            "id": "01m795h4a5xa436wzevyv0zqq2",
            "sequence": 6,
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
                "operation_identity": "01m795h4a2vty5vv2b0j103gpv",
                "outcome": "completed",
                "resolution_event_id": "01m795h4a466hy5ckc65z8bgwa",
                "resolution_event_type": "TimerFired",
            },
            "recorded_at": "2026-11-13T00:39:42.149382Z",
        },
    ]


def _loser_cancelled_at_close() -> dict[str, Any]:
    return {
        **_activity_cancelled_at_close(
            "01m795h4a6cancelledatclose0", 1, "01m795h49zm0xftwdqam8nqj81",
            "Tests\\Fixtures\\V2\\TestGreetingActivity", SELECT_WORK,
        ),
        "sequence": 7,
    }


def _terminal(mode: str) -> dict[str, Any]:
    if mode == "complete":
        return {
            "id": "01m795h4a7terminal00000000",
            "sequence": 8,
            "event_type": "WorkflowCompleted",
            "payload": {
                "output": serializer.envelope("closed-with-deadline")["blob"],
                "payload_codec": "avro",
            },
            "recorded_at": CLOSED_AT,
        }
    if mode == "fail":
        return {
            "id": "01m795h4a7terminal00000000",
            "sequence": 8,
            "event_type": "WorkflowFailed",
            "payload": {
                "failure_id": "01m795h4a7failure000000000",
                "source_kind": "workflow_run",
                "source_id": RUN_ID,
                "failure_category": "application",
                "non_retryable": False,
                "exception_type": "RuntimeError",
                "exception_class": "RuntimeError",
                "message": "Closing with an open select() loser.",
                "failed_step_kind": "workflow_code",
            },
            "recorded_at": CLOSED_AT,
        }
    return {
        "id": "01m795h4a7terminal00000000",
        "sequence": 8,
        "event_type": "WorkflowContinuedAsNew",
        "payload": {
            "sequence": 3,
            "continued_to_run_id": "01m795h4a8continuedrun0000",
            "continued_to_run_number": 2,
            "workflow_link_id": "01m795h4a8link000000000000",
            "closed_reason": "continued",
        },
        "recorded_at": CLOSED_AT,
    }


def _assert_close(outcome: Any, mode: str) -> None:
    assert len(outcome.commands) == 1
    command = outcome.commands[0]
    if mode == "complete":
        assert isinstance(command, CompleteWorkflow)
        assert command.result == "closed-with-deadline"
    elif mode == "fail":
        assert isinstance(command, FailWorkflow)
        assert command.message == "Closing with an open select() loser."
    else:
        assert isinstance(command, ContinueAsNew)
        assert command.arguments == ["continue", 1]


@pytest.mark.parametrize("mode", ["complete", "fail", "continue"])
def test_select_loser_cancelled_at_close_replays_to_the_same_close(mode: str) -> None:
    open_history = _selection_history_before_close()
    closed_history = [*open_history, _loser_cancelled_at_close(), _terminal(mode)]

    live = replay(SelectionLoserAtCloseWorkflow, open_history, [mode], run_id=RUN_ID, payload_codec="avro")
    cold = replay(SelectionLoserAtCloseWorkflow, closed_history, [mode], run_id=RUN_ID, payload_codec="avro")

    _assert_close(live, mode)
    _assert_close(cold, mode)


@pytest.mark.parametrize("mode", ["complete", "fail", "continue"])
def test_abandoned_activities_and_unknown_keys_on_the_terminal_event_are_tolerated(mode: str) -> None:
    terminal = _terminal(mode)
    terminal["payload"]["abandoned_activities"] = [
        {"activity_execution_id": "01m795h49zm0xftwdqam8nqj81", "sequence": 1},
    ]
    terminal["payload"]["a_key_from_a_later_server"] = {"nested": True}
    cancelled = _loser_cancelled_at_close()
    cancelled["payload"]["a_key_from_a_later_server"] = "ignored"

    outcome = replay(
        SelectionLoserAtCloseWorkflow,
        [*_selection_history_before_close(), cancelled, terminal],
        [mode],
        run_id=RUN_ID,
        payload_codec="avro",
    )

    _assert_close(outcome, mode)


def test_abandoned_loser_has_no_cancellation_and_replays_to_the_same_close() -> None:
    terminal = _terminal("complete")
    terminal["payload"]["abandoned_activities"] = [
        {"activity_execution_id": "01m795h49zm0xftwdqam8nqj81", "sequence": 1},
    ]

    outcome = replay(
        SelectionLoserAtCloseWorkflow,
        [*_selection_history_before_close(), terminal],
        ["complete"],
        run_id=RUN_ID,
        payload_codec="avro",
    )

    _assert_close(outcome, "complete")


# --- select() whose winner is a failed nested group ------------------------


def _nested_outer(flat_index: int) -> dict[str, Any]:
    return _member(
        group_id="select-calls:1:3", kind="mixed", size=3, index=flat_index,
        key="nested", member_index=0, member_base=1, member_size=2, member_kind="group",
    )


def _nested_inner(index: int) -> dict[str, Any]:
    return _member(group_id="parallel-activities:1:2", kind="activity", size=2, index=index)


NESTED_DEADLINE = _member(
    group_id="select-calls:1:3", kind="mixed", size=3, index=2,
    key="deadline", member_index=1, member_base=3, member_kind="timer",
)


@workflow.defn(name="run-closed-nested-selection")
class NestedSelectionClosedWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "nested": [
                    ctx.schedule_activity("nested-first", []),
                    ctx.schedule_activity("nested-second", []),
                ],
                "deadline": ctx.start_timer(3600),
            }
        )
        try:
            selected.result()
        except ActivityFailed as failure:
            return {"winner": selected.key, "failure": str(failure)}
        return {"winner": selected.key, "failure": None}


def test_cancellation_at_close_never_becomes_the_failed_nested_winner() -> None:
    """A run-close cancellation of the winning group's lower-index member in
    the same millisecond as its failure must not replace the failure."""
    failed_at = CLOSED_AT
    history: list[dict[str, Any]] = []
    for index, activity_type in enumerate(["nested-first", "nested-second"]):
        history.append(
            _activity_scheduled(
                f"scheduled-{index + 1}", index + 1, f"activity-{index + 1}", activity_type,
                [_nested_outer(index), _nested_inner(index)],
            )
        )
    history.extend(
        [
            {
                "id": "timer-open",
                "event_type": "TimerScheduled",
                "payload": {"sequence": 3, "timer_id": "timer-3", "delay_seconds": 3600,
                            **_with_path([NESTED_DEADLINE])},
                "recorded_at": "2026-11-13T00:39:42.146930Z",
            },
            {
                "id": "nested-failure",
                "event_type": "ActivityFailed",
                "payload": {
                    "activity_execution_id": "activity-2",
                    "activity_type": "nested-second",
                    "sequence": 2,
                    "message": "boom",
                    "exception_type": "NestedFailure",
                    **_with_path([_nested_outer(1), _nested_inner(1)]),
                },
                "recorded_at": failed_at,
            },
            {
                "event_type": "SelectionResolved",
                "payload": {
                    "selection_group_id": "select-calls:1:3",
                    "selection_group_base_sequence": 1,
                    "selection_group_size": 3,
                    "member_key": "nested",
                    "member_index": 0,
                    "member_base_sequence": 1,
                    "member_size": 2,
                    "operation_kind": "group",
                    "operation_identity": "group:1:2",
                    "outcome": "failed",
                    "resolution_event_id": "nested-failure",
                    "resolution_event_type": "ActivityFailed",
                },
                "recorded_at": failed_at,
            },
            _activity_cancelled_at_close(
                "first-cancelled-at-close", 1, "activity-1", "nested-first",
                [_nested_outer(0), _nested_inner(0)], recorded_at=failed_at,
            ),
            {
                "id": "timer-cancelled-at-close",
                "event_type": "TimerCancelled",
                "payload": {"sequence": 3, "timer_id": "timer-3", "delay_seconds": 3600,
                            **_with_path([NESTED_DEADLINE])},
                "recorded_at": failed_at,
            },
        ]
    )

    outcome = replay(NestedSelectionClosedWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == {"winner": "nested", "failure": "boom"}


# --- all(): the failure the workflow handled stays the failure --------------


@workflow.defn(name="run-closed-all-failure")
class AllFailureClosedWorkflow:
    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def caught_message(self) -> str | None:
        return self.caught

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [
                ctx.schedule_activity("still-running", []),
                ctx.schedule_activity("failing", []),
            ]
        except ActivityFailed as failure:
            self.caught = str(failure)
        return "caught: " + str(self.caught)


def _all_failure_history(*, cancelled_at_close: bool) -> list[dict[str, Any]]:
    """``testACancellationRecordedAtCloseNeverReplacesTheFailureTheWorkflowHandled``
    with frozen time: the failure and the close's cancellation share a timestamp."""
    frozen = CLOSED_AT

    def path(index: int) -> list[dict[str, Any]]:
        return [_member(group_id="parallel-activities:1:2", kind="activity", size=2, index=index)]

    history = [
        *_started("run-closed-all-failure"),
        _activity_scheduled("scheduled-1", 1, "activity-1", "still-running", path(0), recorded_at=frozen),
        _activity_scheduled("scheduled-2", 2, "activity-2", "failing", path(1), recorded_at=frozen),
        {
            "id": "failed-2",
            "event_type": "ActivityFailed",
            "payload": {
                "activity_execution_id": "activity-2",
                "activity_class": "failing",
                "activity_type": "failing",
                "sequence": 2,
                "attempt_number": 1,
                "failure_category": "application",
                "non_retryable": False,
                "exception_type": "RuntimeError",
                "exception_class": "RuntimeError",
                "message": "boom",
                **_with_path(path(1)),
            },
            "recorded_at": frozen,
        },
    ]
    if cancelled_at_close:
        history.append(
            _activity_cancelled_at_close(
                "cancelled-1", 1, "activity-1", "still-running", path(0), recorded_at=frozen,
            )
        )
        history.append(
            {
                "id": "completed",
                "event_type": "WorkflowCompleted",
                "payload": {"output": serializer.envelope("caught: boom")["blob"], "payload_codec": "avro"},
                "recorded_at": frozen,
            }
        )
    return history


def test_cancellation_at_close_never_replaces_the_failure_the_workflow_handled() -> None:
    """Core's all() vector. A Python list group resumes once every member is
    terminal, so the close's cancellation of the still-open lower-index member
    must leave the group exactly as the workflow saw it, never complete it with
    that cancellation as the group's first positional failure."""
    live = replay(AllFailureClosedWorkflow, _all_failure_history(cancelled_at_close=False), [])
    cold = replay(AllFailureClosedWorkflow, _all_failure_history(cancelled_at_close=True), [])

    assert cold.commands == live.commands
    assert not any(isinstance(command, CompleteWorkflow) for command in cold.commands)
    assert query_state(
        AllFailureClosedWorkflow, _all_failure_history(cancelled_at_close=True), [], "caught",
    ) is None


def test_ordinary_cancellation_is_still_an_activity_outcome() -> None:
    """Only ``cancellation_reason: run_closed`` is skipped."""
    history = _all_failure_history(cancelled_at_close=True)[:-1]
    del history[-1]["payload"]["cancellation_reason"]

    outcome = replay(AllFailureClosedWorkflow, history, [])

    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == "caught: activity failed"


@workflow.defn(name="test-selection-loser-at-close-query-workflow")
class SelectionLoserAtCloseQueryWorkflow(SelectionLoserAtCloseWorkflow):
    def __init__(self) -> None:
        self.winner: str | None = None

    @workflow.query("winner")
    def winner_key(self) -> str | None:
        return self.winner

    def run(self, ctx: WorkflowContext, mode: str, round: int = 0):  # type: ignore[no-untyped-def]
        selected: SelectionResult = yield ctx.select(
            {
                "work": ctx.schedule_activity("Tests\\Fixtures\\V2\\TestGreetingActivity", [mode]),
                "deadline": ctx.start_timer(0),
            }
        )
        self.winner = selected.key
        return "closed-with-" + selected.key


def test_query_replay_of_a_closed_run_keeps_the_selection_winner() -> None:
    history = [*_selection_history_before_close(), _loser_cancelled_at_close(), _terminal("complete")]

    assert query_state(
        SelectionLoserAtCloseQueryWorkflow, history, ["complete"], "winner", run_id=RUN_ID, payload_codec="avro",
    ) == "deadline"
