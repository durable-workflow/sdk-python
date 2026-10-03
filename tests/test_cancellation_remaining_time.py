from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest

from durable_workflow import CancellationContext, serializer
from durable_workflow.errors import ActivityFailed, DurableOperationCancelled, WorkflowCancelled
from durable_workflow.workflow import (
    CompleteWorkflow,
    RecordLocalActivity,
    RecordSideEffect,
    ScheduleActivity,
    WorkflowContext,
    replay,
)
from tests.test_cancellation_context import delivery, request, snapshot
from tests.test_cooperative_cancellation import completed_activity
from tests.test_durable_selection import _activity_completed, _activity_scheduled, _winner_marker


def history() -> list[dict[str, Any]]:
    return [
        {"event_type": "WorkflowStarted", "payload": {"timestamp": "2025-01-01T00:00:00Z"}},
        request(),
        {**delivery(), "timestamp": "2026-10-01T00:00:08Z"},
    ]


def test_detached_metadata_has_no_remaining_time_clock() -> None:
    context = CancellationContext.from_dict(snapshot())
    with pytest.raises(RuntimeError, match="active workflow replay"):
        context.remaining()


def test_first_execution_and_cold_replay_keep_the_same_budget_through_synchronous_results() -> None:
    observations: list[tuple[float, float]] = []
    contexts: list[CancellationContext] = []
    calls: list[str] = []

    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_timer(60)
            except WorkflowCancelled as error:
                assert error.context is ctx.cancellation_context
                assert error.context is not None
                contexts.append(error.context)
                before = error.context.remaining()
                with ctx.cancellation_shield():
                    yield ctx.side_effect(lambda: calls.append("once") or "recorded")
                    observations.append((before, error.context.remaining()))
                    yield ctx.schedule_activity("cleanup", [])
                assert ctx.now() == datetime(2025, 1, 1, tzinfo=timezone.utc)
                return {"remaining": error.context.remaining(), "context": error.context.to_dict()}

    events = history()
    first = replay(Cleanup, events, [], run_id="run-1")
    assert isinstance(first.commands[0], RecordSideEffect)
    assert isinstance(first.commands[1], ScheduleActivity)
    events.extend([
        {"event_type": "SideEffectRecorded", "timestamp": "2026-10-01T00:00:28Z", "payload": {
            "sequence": 2, "result": serializer.envelope("recorded"),
        }},
        {**completed_activity(3, "cleanup", "cleaned"), "timestamp": "2026-10-01T00:00:25Z"},
        {"event_type": "WorkflowTaskCompleted", "timestamp": "2026-10-01T00:00:29Z", "payload": {}},
    ])
    for _restart in range(2):
        assert replay(Cleanup, events, [], run_id="run-1").commands == [
            CompleteWorkflow({"remaining": 5.123456, "context": snapshot()}),
        ]
    assert observations == [(22.123456, 22.123456)] * 3
    assert calls == ["once"]
    for context in contexts:
        with pytest.raises(RuntimeError, match="active workflow replay"):
            context.remaining()


def test_completed_parallel_group_uses_consumed_members_without_regressing_on_clock_skew() -> None:
    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_timer(60)
            except WorkflowCancelled as error:
                assert error.context is not None
                with ctx.cancellation_shield():
                    yield [ctx.schedule_activity("one", []), ctx.schedule_activity("two", [])]
                return error.context.remaining()

    events = history() + [
        {**completed_activity(2, "one", "one"), "timestamp": "2026-10-01T00:00:25Z"},
        {**completed_activity(3, "two", "two"), "timestamp": "2026-10-01T00:00:20Z"},
    ]
    assert replay(Cleanup, events, [], run_id="run-1").commands == [CompleteWorkflow(5.123456)]


def test_parallel_failure_does_not_consume_a_future_sibling_completion() -> None:
    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_timer(60)
            except WorkflowCancelled as error:
                assert error.context is not None
                with ctx.cancellation_shield():
                    try:
                        yield [ctx.schedule_activity("one", []), ctx.schedule_activity("two", [])]
                    except ActivityFailed:
                        return error.context.remaining()

    events = history() + [
        {"event_type": "ActivityFailed", "timestamp": "2026-10-01T00:00:12Z", "payload": {
            "sequence": 2, "activity_type": "one", "message": "failed", "exception_class": "RuntimeError",
        }},
        {**completed_activity(3, "two", "two"), "timestamp": "2026-10-01T00:00:29Z"},
    ]
    assert replay(Cleanup, events, [], run_id="run-1").commands == [CompleteWorkflow(18.123456)]


@pytest.mark.parametrize("timestamp", [None, "invalid", "2026-10-01T00:00:08", "2026-02-30T00:00:08Z"])
def test_missing_or_invalid_delivery_time_refuses_a_host_clock(timestamp: str | None) -> None:
    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_timer(60)
            except WorkflowCancelled as error:
                assert error.context is not None
                with pytest.raises(RuntimeError, match="recorded timestamp"):
                    error.context.remaining()
                return "refused"

    events = history()
    events[-1]["timestamp"] = timestamp
    assert replay(Cleanup, events, [], run_id="run-1").commands == [CompleteWorkflow("refused")]


def test_recorded_expiry_clamps_remaining_to_zero_and_accepts_timezone_offsets() -> None:
    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_timer(60)
            except WorkflowCancelled as error:
                assert error.context is not None
                with ctx.cancellation_shield():
                    yield ctx.start_timer(10)
                return error.context.remaining()

    events = history() + [{"event_type": "TimerFired", "recorded_at": "2026-09-30T20:00:31-04:00", "payload": {
        "sequence": 2, "timer_kind": "durable_timer", "duration_ms": 10000,
    }}]
    assert replay(Cleanup, events, [], run_id="run-1").commands == [CompleteWorkflow(0.0)]


def selection_event(event: dict[str, Any], timestamp: str) -> dict[str, Any]:
    shifted = deepcopy(event)
    shifted["timestamp"] = f"2026-10-01T00:00:{timestamp}Z"
    payload = shifted["payload"]
    for item in [payload, *payload.get("parallel_group_path", [])]:
        for key in ("sequence", "parallel_group_base_sequence", "selection_member_base_sequence",
                    "selection_group_base_sequence", "member_base_sequence"):
            if key in item:
                item[key] += 1
        for key in ("parallel_group_id", "selection_group_id"):
            if key in item:
                item[key] = "select-calls:2:2"
    return shifted


def selection_history() -> list[dict[str, Any]]:
    return history() + [
        selection_event(_activity_scheduled(0, "slow"), "09"),
        selection_event(_activity_scheduled(1, "fast"), "09"),
        selection_event(_activity_completed(1, "fast", "fast"), "10"),
        selection_event(_winner_marker(), "12"),
    ]


def test_selection_advances_at_the_winner_and_awaited_handle_without_future_loser_lookahead() -> None:
    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_timer(60)
            except WorkflowCancelled as error:
                assert error.context is not None
                with ctx.cancellation_shield():
                    selected = yield ctx.select({
                        "slow": ctx.schedule_activity("slow-activity", []),
                        "fast": ctx.schedule_activity("fast-activity", []),
                    })
                    at_winner = error.context.remaining()
                    yield selected.winner.await_result()
                    at_old_result = error.context.remaining()
                    yield selected.handles["slow"].await_result()
                return [at_winner, at_old_result, error.context.remaining()]

    events = selection_history() + [selection_event(_activity_completed(0, "slow", "slow"), "20")]
    assert replay(Cleanup, events, [], run_id="run-1").commands == [
        CompleteWorkflow([18.123456, 18.123456, 10.123456]),
    ]


def test_cancelled_handle_uses_the_first_durable_receipt_despite_identical_redelivery() -> None:
    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_timer(60)
            except WorkflowCancelled as error:
                assert error.context is not None
                with ctx.cancellation_shield():
                    selected = yield ctx.select({
                        "slow": ctx.schedule_activity("slow-activity", []),
                        "fast": ctx.schedule_activity("fast-activity", []),
                    })
                    yield selected.handles["slow"].cancel()
                    try:
                        yield selected.handles["slow"].await_result()
                    except DurableOperationCancelled:
                        return error.context.remaining()

    cancelled = {"event_type": "SelectionOperationCancelled", "timestamp": "2026-10-01T00:00:15Z",
                 "payload": {"selection_group_id": "select-calls:2:2", "member_key": "slow", "member_index": 0,
                             "member_base_sequence": 2, "member_size": 1, "operation_kind": "activity",
                             "operation_identity": "activity-slow"}}
    events = selection_history() + [cancelled, {**cancelled, "timestamp": "2026-10-01T00:00:28Z"}]
    assert replay(Cleanup, events, [], run_id="run-1").commands == [CompleteWorkflow(15.123456)]


def test_inline_local_result_persistence_preserves_the_first_execution_budget() -> None:
    observations: list[float] = []
    calls: list[str] = []

    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.start_timer(60)
            except WorkflowCancelled as error:
                assert error.context is not None
                with ctx.cancellation_shield():
                    yield ctx.local_activity("inline", [])
                    observations.append(error.context.remaining())
                    yield ctx.schedule_activity("cleanup", [])
                return error.context.remaining()

    def execute(command: RecordLocalActivity) -> str:
        calls.append("once")
        command.arguments_envelope = serializer.envelope(command.arguments)
        command.result_envelope = serializer.envelope("local")
        command.outcome = {"outcome": "completed", "attempts": []}
        return "local"

    events = history()
    first = replay(Cleanup, events, [], run_id="run-1", local_activity_executor=execute)
    assert isinstance(first.commands[0], RecordLocalActivity)
    events.extend([
        {"event_type": "ActivityCompleted", "timestamp": "2026-10-01T00:00:28Z", "payload": {
            "sequence": 2, "activity_type": "inline", "execution_mode": "local", "result": serializer.envelope("local"),
        }},
        {**completed_activity(3, "cleanup", "cleaned"), "timestamp": "2026-10-01T00:00:25Z"},
    ])
    assert replay(Cleanup, events, [], run_id="run-1", local_activity_executor=execute).commands == [
        CompleteWorkflow(5.123456),
    ]
    assert observations == [22.123456, 22.123456]
    assert calls == ["once"]
