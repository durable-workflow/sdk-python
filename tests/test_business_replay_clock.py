from __future__ import annotations

import asyncio
import gc
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Any

import pytest

from durable_workflow import ReplayClockUnavailable, serializer, workflow
from durable_workflow.errors import ActivityFailed, NonDeterministicReplayError, QueryFailed
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, _prepare_query_state, query_state, replay
from tests.business_clock_workflows import NativeRedriveBusinessClockWorkflow, ScalarBusinessClockWorkflow, clock_text


def history() -> list[dict[str, Any]]:
    return [
        {"event_type": "WorkflowStarted", "recorded_at": "2026-11-01T00:00:00.123456Z", "payload": {}},
        {"event_type": "ActivityScheduled", "payload": {
            "sequence": 1, "activity_execution_id": "clock-activity", "activity_type": "clock.first",
        }},
        {"event_type": "ActivityCompleted", "recorded_at": "2026-11-01T00:00:03.123456Z", "payload": {
            "sequence": 1, "activity_execution_id": "clock-activity", "activity_type": "clock.first",
            "result": serializer.envelope("first"),
        }},
        {"event_type": "TimerScheduled", "payload": {
            "sequence": 2, "timer_id": "clock-timer", "timer_kind": "durable_timer", "delay_seconds": 2,
        }},
        {"event_type": "TimerFired", "recorded_at": "2026-11-01T00:00:05.654321Z", "payload": {
            "sequence": 2, "timer_id": "clock-timer", "timer_kind": "durable_timer", "delay_seconds": 2,
        }},
    ]


def result(events: list[dict[str, Any]], workflow: type = ScalarBusinessClockWorkflow) -> Any:
    outcome = replay(workflow, events, [], payload_codec="avro")
    try:
        assert len(outcome.commands) == 1
        command = outcome.commands[0]
        assert isinstance(command, CompleteWorkflow), command
        return command.result
    finally:
        outcome.close()


def test_exact_consumed_times_and_legacy_clock_survive_cold_replay_without_history_maximum() -> None:
    events = history()
    events.append({"event_type": "WorkflowTaskCompleted", "recorded_at": "2026-12-01T00:00:00Z", "payload": {}})
    original = deepcopy(events)
    first = result(events)
    assert first == result(events)
    assert first == {
        "clock": [None, "2026-11-01T00:00:03.123456+00:00", "2026-11-01T00:00:05.654321+00:00"],
        "legacy": ["2026-11-01T00:00:00.123456+00:00"] * 3,
        "elapsed_seconds": 2.530865,
    }
    assert events == original


@pytest.mark.parametrize("timer_time,elapsed", [
    ("2026-11-01T00:00:03.123456Z", 0),
    ("2026-11-01T00:00:02.123455Z", -1.000001),
    ("2026-11-01T02:00:05.654321+02:00", 2.530865),
])
def test_equal_backward_and_offset_times_retain_the_exact_consumed_instant(timer_time: str, elapsed: float) -> None:
    events = history()
    events[-1]["recorded_at"] = timer_time
    value = result(events)
    assert value["elapsed_seconds"] == elapsed
    assert datetime.fromisoformat(value["clock"][2]).utcoffset() == timedelta(0)


@pytest.mark.parametrize("bad", [None, False, 17, "", "not-a-time", "2026-11-01T00:00:03",
                                       "2026-02-30T00:00:00Z", "2026-11-01T24:00:00Z",
                                       "2026-11-01T00:00:03+01:99"])
def test_invalid_required_time_is_unavailable_then_a_new_proved_boundary_can_recover(bad: Any) -> None:
    events = history()
    events[2]["recorded_at"] = bad
    assert result(events)["clock"] == [None, None, "2026-11-01T00:00:05.654321+00:00"]


def test_invalid_present_timestamp_does_not_fall_back_to_recorded_at() -> None:
    events = history()
    events[2]["timestamp"] = "invalid"
    assert result(events)["clock"][1] is None


def test_identical_duplicate_terminal_retains_legacy_result_but_has_ambiguous_time() -> None:
    events = history()
    events.insert(3, deepcopy(events[2]))
    assert result(events)["clock"][1] is None


@pytest.mark.parametrize("field,value", [("activity_execution_id", "other"), ("sequence", True)])
def test_unproved_resource_or_counter_keeps_time_unavailable(field: str, value: Any) -> None:
    events = history()
    events[2]["payload"][field] = value
    assert result(events)["clock"][1] is None


@pytest.mark.parametrize("terminal", ["ActivityFailed", "ActivityTimedOut", "ActivityCancelled"])
def test_consumed_failure_uses_its_own_time_and_preserves_failure_delivery(terminal: str) -> None:
    class Failed:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.schedule_activity("clock.first", [])
            except ActivityFailed:
                return clock_text(ctx)
            raise AssertionError("failure was not thrown")

    events = history()[:3]
    events[-1]["event_type"] = terminal
    events[-1]["payload"].update(message="failed", reused_recorded_at="2020-01-01T00:00:00Z")
    assert result(events, Failed) == "2026-11-01T00:00:03.123456+00:00"


def test_existing_command_mismatch_remains_the_original_replay_error() -> None:
    events = history()
    events[1]["payload"]["activity_type"] = "changed"
    events[2]["payload"]["activity_type"] = "changed"
    with pytest.raises(NonDeterministicReplayError):
        result(events)


def test_accessor_is_unavailable_outside_execution_even_with_an_explicit_legacy_clock() -> None:
    ctx = WorkflowContext(current_time=datetime.fromisoformat("2026-11-01T00:00:00+00:00"))
    with pytest.raises(ReplayClockUnavailable):
        ctx.replay_now()


def test_supported_query_prefix_is_immutable_and_unknown_final_prefix_is_unavailable() -> None:
    events = history()
    original = deepcopy(events)
    for _ in range(2):
        assert query_state(ScalarBusinessClockWorkflow, events, [], "clock", payload_codec="avro") == (
            "2026-11-01T00:00:05.654321+00:00"
        )
    assert events == original
    events.append({"event_type": "WorkflowTaskCompleted", "recorded_at": "2026-12-01T00:00:00Z", "payload": {}})
    assert query_state(ScalarBusinessClockWorkflow, events, [], "clock", payload_codec="avro") is None


@pytest.mark.asyncio
async def test_async_queries_restore_scope_and_cannot_lend_time_to_a_child_task_or_thread() -> None:
    first, second = history(), history()
    second[-1]["recorded_at"] = "2026-11-02T00:00:00Z"
    values = await asyncio.gather(*(
            _prepare_query_state(ScalarBusinessClockWorkflow, rows, [], "async_clock", payload_codec="avro")()
        for rows in (first, second)
    ))
    assert values == [
        ("2026-11-01T00:00:05.654321+00:00", None, None, "2026-11-01T00:00:05.654321+00:00"),
        ("2026-11-02T00:00:00.000000+00:00", None, None, "2026-11-02T00:00:00.000000+00:00"),
    ]


def test_deadline_loop_advances_after_a_consumed_timer_and_repeated_reads_are_equal() -> None:
    class Deadline:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.schedule_activity("clock.first", [])
            before = ctx.replay_now()
            deadline = before + timedelta(seconds=2)
            steps = 0
            while ctx.replay_now() < deadline:
                assert ctx.replay_now() == ctx.replay_now()
                yield ctx.start_timer(2)
                steps += 1
            return steps, (ctx.replay_now() - before).total_seconds()

    assert result(history(), Deadline) == (1, 2.530865)


def test_nested_reconstruction_and_callback_error_cannot_borrow_time_and_restore_the_parent_scope() -> None:
    class Outer:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.schedule_activity("clock.first", [])
            before = ctx.replay_now()

            @workflow.defn(name="tests.business.inner")
            class Inner:
                def run(self, nested: WorkflowContext) -> str:
                    with pytest.raises(ReplayClockUnavailable):
                        ctx.replay_now()
                    return "inner"

                @workflow.query("fail")
                def fail(self) -> None:
                    with pytest.raises(ReplayClockUnavailable):
                        ctx.replay_now()
                    raise RuntimeError("query failed")

            nested_history = history()[:1]
            with pytest.raises(QueryFailed, match="query failed"):
                query_state(Inner, nested_history, [], "fail", payload_codec="avro")
            assert ctx.replay_now() == before
            return clock_text(ctx)

    assert result(history()[:3], Outer) == "2026-11-01T00:00:03.123456+00:00"


def _redrive_observations(missing: str | None = None) -> list[str | None]:
    from tests.test_replay_clock import check_redrive_successor, redrive_fixture

    fixture = redrive_fixture("redrive-successor-reused-timer")
    check_redrive_successor(fixture["history"], fixture["workflow"])
    observations: list[str | None] = []

    class Redrive:
        def run(self, ctx: WorkflowContext, name: str, seconds: int):  # type: ignore[no-untyped-def]
            observations.append(clock_text(ctx))
            greeting = yield ctx.schedule_activity("tests.redrive.first", [name])
            observations.append(clock_text(ctx))
            yield ctx.start_timer(seconds)
            observations.append(clock_text(ctx))
            return (yield ctx.schedule_activity("tests.redrive.second", [greeting]))

    if missing is not None:
        fixture["history"][0]["payload"].pop(missing)
    task = fixture["workflow"]
    outcome = replay(Redrive, fixture["history"], task["input"], workflow_id=task["workflow_id"],
                     run_id=task["run_id"], payload_codec="avro")
    try:
        assert len(outcome.commands) == 1
        assert outcome.commands[0].activity_type == "tests.redrive.second"
    finally:
        outcome.close()
    return observations


def test_canonical_redrive_start_and_reused_results_keep_their_original_times() -> None:
    expected = ["2026-11-03T00:00:00.000000+00:00"] * 2 + ["2026-11-03T00:00:06.000000+00:00"]
    assert _redrive_observations() == expected
    assert _redrive_observations() == expected


@pytest.mark.parametrize("missing", ["workflow_command_id", "continued_from_run_id", "workflow_link_id",
                                      "workflow_run_id", "resume_step_sequence"])
def test_partial_redrive_authority_is_unavailable_without_changing_legacy_commands(missing: str) -> None:
    assert _redrive_observations(missing) == [None] * 3


def test_unpersisted_side_effect_is_unavailable_on_first_authoring_and_cold_replay() -> None:
    class Inline:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.schedule_activity("clock.first", [])
            before = clock_text(ctx)
            yield ctx.side_effect(lambda: "recorded")
            return before, clock_text(ctx)

    events = history()[:3]
    first = replay(Inline, events, [], payload_codec="avro")
    try:
        assert isinstance(first.commands[-1], CompleteWorkflow)
        assert first.commands[-1].result == ("2026-11-01T00:00:03.123456+00:00", None)
    finally:
        first.close()
    events.append({"event_type": "SideEffectRecorded", "recorded_at": "2026-11-01T00:00:04Z",
                   "payload": {"sequence": 2, "result": serializer.envelope("recorded")}})
    assert result(events, Inline) == ("2026-11-01T00:00:03.123456+00:00", None)


def test_a_retained_context_is_unavailable_after_success_and_discard_cleanup() -> None:
    retained: list[WorkflowContext] = []

    class Retain:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            retained.append(ctx)
            yield ctx.schedule_activity("clock.first", [])
            return clock_text(ctx)

    assert result(history()[:3], Retain) == "2026-11-01T00:00:03.123456+00:00"
    with pytest.raises(ReplayClockUnavailable):
        retained[0].replay_now()


def test_cyclic_discard_cleanup_restores_parent_time_and_preserves_original_replay_errors() -> None:
    cleanup: list[str | None] = []

    class Discard:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield ctx.schedule_activity("discard", [])
            finally:
                cleanup.append(clock_text(ctx))

    class Parent:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.schedule_activity("clock.first", [])
            before = ctx.replay_now()
            for _ in range(32):
                outcome = replay(Discard, [], [], payload_codec="avro")
                cycle: list[Any] = [outcome]
                cycle.append(cycle)
            del outcome, cycle
            gc.collect()
            assert cleanup == [None] * 32
            assert ctx.replay_now() == before
            mismatch = history()
            mismatch[2]["payload"]["activity_type"] = "changed"
            with pytest.raises(NonDeterministicReplayError):
                replay(ScalarBusinessClockWorkflow, mismatch, [], payload_codec="avro")
            assert ctx.replay_now() == before
            return clock_text(ctx)

    assert result(history()[:3], Parent) == "2026-11-01T00:00:03.123456+00:00"


def _native_fixture() -> dict[str, Any]:
    from tests.native_redrive_fixture import check_native_redrive_successor
    from tests.test_replay_clock import redrive_fixture

    fixture = redrive_fixture("business-clock-native-redrive-entry")
    check_native_redrive_successor(fixture["history"], fixture["workflow"])
    return fixture


@pytest.mark.parametrize("public", [False, True])
def test_native_same_identity_acceptance_prefix_and_both_time_representations_are_supported(public: bool) -> None:
    fixture = _native_fixture()
    events = fixture["history"]
    if public:
        for event in events:
            event["timestamp"] = event.pop("recorded_at")
    assert result(events, NativeRedriveBusinessClockWorkflow) == fixture["expected"]["command_sequence"][0]["result"]


@pytest.mark.parametrize("mutation", ["prefix", "instance", "run", "command", "outcome", "extra_prefix",
                                      "duplicate_start", "order", "self_source", "start_time"])
def test_native_entry_refuses_unrelated_prefixes_conflicting_identity_and_ambiguous_start(mutation: str) -> None:
    events = _native_fixture()["history"]
    if mutation == "prefix":
        events[0]["event_type"] = "WorkflowTaskCompleted"
    elif mutation in {"instance", "run", "command"}:
        key = {"instance": "workflow_instance_id", "run": "workflow_run_id", "command": "workflow_command_id"}[mutation]
        events[0]["payload"][key] = "foreign"
    elif mutation == "outcome":
        events[0]["payload"]["outcome"] = "started"
    elif mutation == "extra_prefix":
        events.insert(0, {"event_type": "WorkflowTaskCompleted", "payload": {}})
    elif mutation == "duplicate_start":
        events.insert(2, deepcopy(events[1]))
    elif mutation == "self_source":
        events[1]["payload"]["continued_from_run_id"] = events[1]["payload"]["workflow_run_id"]
        events[-1]["payload"]["reused_from_run_id"] = events[1]["payload"]["workflow_run_id"]
    elif mutation == "start_time":
        events[1]["timestamp"] = None
    for index, event in enumerate(events, start=1):
        event["sequence"] = index
    if mutation == "order":
        events[-1]["sequence"] = 9
    assert result(events, NativeRedriveBusinessClockWorkflow)["clock"] == [None, None]


def test_an_unproved_reused_identity_does_not_poison_the_earlier_proved_start() -> None:
    fixture = _native_fixture()
    events = fixture["history"]
    events[-1]["payload"]["activity_execution_id"] = events[-1]["payload"]["reused_activity_execution_id"]
    first = fixture["expected"]["command_sequence"][0]["result"]["clock"][0]
    assert result(events, NativeRedriveBusinessClockWorkflow)["clock"] == [first, None]


def test_complete_core_acceptance_prefix_retains_reused_timer_time() -> None:
    from tests.test_replay_clock import redrive_fixture

    fixture = redrive_fixture("redrive-successor-reused-timer")
    events = fixture["history"]
    accepted = _native_fixture()["history"][0]
    accepted["payload"].update(workflow_type=events[0]["payload"]["workflow_type"],
                               workflow_class=events[0]["payload"]["workflow_type"])
    events.insert(0, accepted)
    for index, event in enumerate(events, start=1):
        event["sequence"] = index

    class CoreTimer:
        def run(self, ctx: WorkflowContext, name: str, seconds: int):  # type: ignore[no-untyped-def]
            yield ctx.schedule_activity("tests.redrive.first", [name])
            yield ctx.start_timer(seconds)
            return clock_text(ctx)

    task = fixture["workflow"]
    outcome = replay(CoreTimer, events, task["input"], workflow_id=task["workflow_id"],
                     run_id=task["run_id"], payload_codec="avro")
    try:
        assert outcome.commands == [CompleteWorkflow("2026-11-03T00:00:06.000000+00:00")]
    finally:
        outcome.close()
