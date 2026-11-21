"""sdk-python#307: which member failure an all() group raises.

With recording enabled, a run records ``PARALLEL_FAILURE_ORDER_MARKER``
directly before scheduling the first all-group that can fail. A marked
run raises the first member failure recorded in history, as soon as it is
recorded, whether or not the other members have settled. Every worker follows a
recorded marker, with recording on or off. A run whose first all-group was
recorded without the marker keeps the rule of the workers that started it for
its whole life: on every later task, query and update, a settled group raises
its lowest-position failure.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from durable_workflow import Client, serializer, workflow
from durable_workflow.errors import ActivityFailed, NonDeterministicReplayError
from durable_workflow.worker import RECORD_PARALLEL_FAILURE_ORDER_ENV, Worker
from durable_workflow.workflow import (
    PARALLEL_FAILURE_DECISION,
    CompleteUpdate,
    CompleteWorkflow,
    FailWorkflow,
    RecordSideEffect,
    ScheduleActivity,
    StartTimer,
    WorkflowContext,
    apply_update,
    commands_to_server_commands,
    query_state,
    replay,
)
from tests.failure_order import FAILURE_ORDER_MARKER, failure_order_marker_event
from tests.test_cooperative_cancellation import request

SIZE = 4


def _entry(base: int, index: int) -> dict[str, Any]:
    return {
        "parallel_group_id": f"parallel-activities:{base}:{SIZE}",
        "parallel_group_kind": "activity",
        "parallel_group_base_sequence": base,
        "parallel_group_size": SIZE,
        "parallel_group_index": index,
    }


def _member(event_type: str, index: int, base: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sequence": base + index, "activity_execution_id": f"member-{index}", "activity_type": f"member-{index}",
        **_entry(base, index), "parallel_group_path": [_entry(base, index)],
    }
    if event_type == "ActivityCompleted":
        payload["result"] = serializer.envelope(index, codec=serializer.AVRO_CODEC)
    if event_type == "ActivityFailed":
        payload["message"] = f"member-{index} failed"
    return {"event_type": event_type, "payload": payload}


def _activity(event_type: str, sequence: int, activity_type: str, result: Any = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"sequence": sequence, "activity_execution_id": f"{activity_type}-{sequence}",
                               "activity_type": activity_type}
    if event_type == "ActivityCompleted":
        payload["result"] = serializer.envelope(result, codec=serializer.AVRO_CODEC)
    return {"event_type": event_type, "payload": payload}


def _run(*, marked: bool) -> tuple[list[dict[str, Any]], int]:
    """A run with its group scheduled: with the marker (new) or without (legacy)."""
    base = 2 if marked else 1
    history: list[dict[str, Any]] = [{"event_type": "WorkflowStarted", "payload": {}}]
    if marked:
        history.append(failure_order_marker_event(1))
    history.extend(_member("ActivityScheduled", index, base) for index in range(SIZE))
    return history, base


def _settled_out_of_order(base: int) -> list[dict[str, Any]]:
    """Member 2 fails first; member 0, the lower position, fails later."""
    return [
        _member("ActivityFailed", 2, base),
        _member("ActivityCompleted", 1, base),
        _member("ActivityFailed", 0, base),
        _member("ActivityCompleted", 3, base),
    ]


@workflow.defn(name="tests.parallel-failure-order")
class FailureOrder:
    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def caught_member(self) -> str | None:
        return self.caught

    @workflow.update("report")
    def report(self) -> dict[str, str | None]:
        return {"compensating": self.caught}

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [ctx.schedule_activity(f"member-{index}", []) for index in range(SIZE)]
        except ActivityFailed as failure:
            self.caught = failure.activity_type
            return (yield ctx.schedule_activity("compensate", [self.caught]))
        return "no failure"


@workflow.defn(name="tests.parallel-failure-order-uncaught")
class FailureOrderUncaught:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield [ctx.schedule_activity(f"member-{index}", []) for index in range(SIZE)])


def _commands(history: list[dict[str, Any]], definition: type = FailureOrder, *, record: bool = True) -> list[Any]:
    return replay(definition, history, [], payload_codec=serializer.AVRO_CODEC,
                  record_parallel_failure_order=record).commands


def test_recording_disabled_keeps_the_released_commands_for_new_runs() -> None:
    started = [{"event_type": "WorkflowStarted", "payload": {}}]
    commands = _commands(started, record=False)
    assert FAILURE_ORDER_MARKER not in commands
    wire = commands_to_server_commands(commands, "queue", payload_codec=serializer.AVRO_CODEC)
    assert [command["parallel_group_base_sequence"] for command in wire] == [1] * SIZE
    # The group was recorded without the marker, so the run keeps the
    # released rule even for a worker with recording enabled.
    history, base = _run(marked=False)
    history += _settled_out_of_order(base)
    assert _commands(history, record=True) == [ScheduleActivity("compensate", ["member-0"])]


def _caught(history: list[dict[str, Any]]) -> Any:
    return query_state(FailureOrder, history, [], "caught", payload_codec=serializer.AVRO_CODEC)


def test_a_new_run_records_the_marker_directly_before_its_first_group() -> None:
    commands = _commands([{"event_type": "WorkflowStarted", "payload": {}}])
    assert commands[0] == FAILURE_ORDER_MARKER
    wire = commands_to_server_commands(commands, "queue", payload_codec=serializer.AVRO_CODEC)
    assert wire[0] == {"type": "record_version_marker", "change_id": "durable_workflow.parallel_failure_order",
                       "version": 1, "min_supported": 1, "max_supported": 1}
    assert [command["parallel_group_base_sequence"] for command in wire[1:]] == [2] * SIZE
    history, _ = _run(marked=True)
    assert _commands(history) == []


def test_the_marker_is_recorded_once_per_run() -> None:
    @workflow.defn(name="tests.parallel-failure-order-two-groups")
    class TwoGroups:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield [ctx.schedule_activity("first", []), ctx.schedule_activity("second", [])]
            yield [ctx.schedule_activity("third", []), ctx.schedule_activity("fourth", [])]
            return "done"

    def entry(base: int, index: int) -> dict[str, Any]:
        return {"parallel_group_id": f"parallel-activities:{base}:2", "parallel_group_kind": "activity",
                "parallel_group_base_sequence": base, "parallel_group_size": 2, "parallel_group_index": index}

    history: list[dict[str, Any]] = [{"event_type": "WorkflowStarted", "payload": {}}, failure_order_marker_event(1)]
    for index, name in enumerate(["first", "second"]):
        payload = {"sequence": 2 + index, "activity_type": name, **entry(2, index),
                   "parallel_group_path": [entry(2, index)]}
        history.append({"event_type": "ActivityScheduled", "payload": dict(payload)})
        history.append({"event_type": "ActivityCompleted", "payload": {
            **payload, "result": serializer.envelope(name, codec=serializer.AVRO_CODEC)}})
    commands = replay(TwoGroups, history, [], payload_codec=serializer.AVRO_CODEC,
                      record_parallel_failure_order=True).commands
    assert [type(command) for command in commands] == [ScheduleActivity, ScheduleActivity]
    wire = commands_to_server_commands(commands, "queue", payload_codec=serializer.AVRO_CODEC)
    assert [command["parallel_group_base_sequence"] for command in wire] == [4, 4]


def test_a_timer_group_does_not_decide_the_rule() -> None:
    @workflow.defn(name="tests.parallel-failure-order-timers-first")
    class TimersFirst:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield [ctx.start_timer(1), ctx.start_timer(2)]
            yield [ctx.schedule_activity("work", []), ctx.schedule_activity("more", [])]
            return "done"

    commands = replay(TimersFirst, [], [], record_parallel_failure_order=True).commands
    assert [type(command) for command in commands] == [StartTimer, StartTimer]
    fired = [{"event_type": "TimerFired", "payload": {"sequence": sequence}} for sequence in (1, 2)]
    commands = replay(TimersFirst, fired, [], record_parallel_failure_order=True).commands
    assert commands[0] == FAILURE_ORDER_MARKER
    assert [type(command) for command in commands[1:]] == [ScheduleActivity, ScheduleActivity]


def test_a_run_started_by_an_earlier_worker_records_the_marker_at_its_first_group() -> None:
    """No all-group of the run has an outcome yet, so no task has chosen a
    failure under the earlier rule."""
    @workflow.defn(name="tests.parallel-failure-order-late-group")
    class LateGroup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.schedule_activity("prepare", [])
            return (yield [ctx.schedule_activity("a", []), ctx.schedule_activity("b", [])])

    history = [{"event_type": "WorkflowStarted", "payload": {}}, _activity("ActivityScheduled", 1, "prepare"),
               _activity("ActivityCompleted", 1, "prepare", "ready")]
    commands = replay(LateGroup, history, [], payload_codec=serializer.AVRO_CODEC,
                      record_parallel_failure_order=True).commands
    assert commands[0] == FAILURE_ORDER_MARKER
    wire = commands_to_server_commands(commands[1:], "queue", payload_codec=serializer.AVRO_CODEC)
    assert [command["parallel_group_base_sequence"] for command in wire] == [3, 3]


def test_a_cancellation_request_keeps_the_earlier_rule() -> None:
    """A request may replace the group with its cancellation, so the run
    records no marker and its group is recorded under the earlier rule."""
    outcome = replay(FailureOrder, [request()], [], run_id="run-1", payload_codec=serializer.AVRO_CODEC,
                     record_parallel_failure_order=True)
    assert FAILURE_ORDER_MARKER not in outcome.commands
    assert outcome.cancellation_delivery is not None


@pytest.mark.parametrize("record", [False, True])
def test_a_marked_run_raises_the_first_recorded_failure_of_a_settled_group(record: bool) -> None:
    history, base = _run(marked=True)
    history += _settled_out_of_order(base)
    assert _commands(history, record=record) == [ScheduleActivity("compensate", ["member-2"])]
    assert _caught(history) == "member-2"
    uncaught = _commands(history, FailureOrderUncaught, record=record)
    assert [type(command) for command in uncaught] == [FailWorkflow]
    assert "member-2" in str(cast(FailWorkflow, uncaught[0]).message)


@pytest.mark.parametrize("record", [False, True])
def test_a_marked_run_raises_at_once_without_recording_a_decision(record: bool) -> None:
    """History is append-only: the first failure this task sees is the first
    failure every later replay sees, so nothing more needs recording."""
    history, base = _run(marked=True)
    history += [_member("ActivityCompleted", 0, base), _member("ActivityFailed", 2, base)]
    assert _commands(history, record=record) == [ScheduleActivity("compensate", ["member-2"])]
    # Member 3 fails and member 1 completes after the compensation was
    # scheduled; replay still raises member 2.
    later = [*history, _activity("ActivityScheduled", base + SIZE, "compensate"),
             _member("ActivityFailed", 3, base), _member("ActivityCompleted", 1, base),
             _activity("ActivityCompleted", base + SIZE, "compensate", "undone")]
    for end in range(len(history) + 1, len(later) + 1):
        expected = [CompleteWorkflow("undone")] if end == len(later) else []
        assert _commands(later[:end], record=record) == expected, end
        assert _caught(later[:end]) == "member-2", end


@pytest.mark.parametrize("record", [False, True])
def test_a_marked_run_keeps_its_choice_when_a_lower_position_fails_before_the_completion(record: bool) -> None:
    """The deciding task saw only member 2's failure. Member 0 failed and the
    others completed before its completion was written."""
    history, base = _run(marked=True)
    history += [
        _member("ActivityFailed", 2, base), _member("ActivityFailed", 0, base),
        _member("ActivityCompleted", 1, base), _member("ActivityCompleted", 3, base),
        _activity("ActivityScheduled", base + SIZE, "compensate"),
        _activity("ActivityCompleted", base + SIZE, "compensate", "undone"),
    ]
    assert _commands(history, record=record) == [CompleteWorkflow("undone")]
    assert _caught(history) == "member-2"


@pytest.mark.parametrize("record", [False, True])
def test_an_unmarked_run_keeps_the_position_rule_at_every_task(record: bool) -> None:
    """The group was scheduled without the marker, by a released worker."""
    history, base = _run(marked=False)
    history += _settled_out_of_order(base)
    assert _commands(history, record=record) == [ScheduleActivity("compensate", ["member-0"])]
    assert _caught(history) == "member-0"
    moved_on = [*history, _activity("ActivityScheduled", base + SIZE, "compensate"),
                _activity("ActivityCompleted", base + SIZE, "compensate", "undone")]
    assert _commands(moved_on, record=record) == [CompleteWorkflow("undone")]
    assert _caught(moved_on) == "member-0"


@pytest.mark.parametrize("record", [False, True])
def test_an_unmarked_run_whose_task_recorded_nothing_after_the_group_keeps_the_position_rule(record: bool) -> None:
    """A released worker caught member 0 and then waited without recording a
    command (for example on a select() handle it already held). A later task
    must not take that for a new decision."""
    history, base = _run(marked=False)
    history += _settled_out_of_order(base)
    history.append({"event_type": "SignalReceived", "payload": {"signal_name": "nudge", "arguments": None}})
    assert _caught(history) == "member-0"
    commands = _commands(history, record=record)
    assert not any(isinstance(command, RecordSideEffect) for command in commands)
    assert commands == [ScheduleActivity("compensate", ["member-0"])]


def test_an_update_in_an_unmarked_run_reports_the_position_rule() -> None:
    history, base = _run(marked=False)
    history += _settled_out_of_order(base)
    history.append({"event_type": "UpdateAccepted", "payload": {
        "update_id": "update-1", "update_name": "report", "arguments": None}})
    update = apply_update(FailureOrder, history, [], "update-1", payload_codec=serializer.AVRO_CODEC)
    assert update == CompleteUpdate("update-1", {"compensating": "member-0"})
    marked, base = _run(marked=True)
    marked += _settled_out_of_order(base)
    marked.append(history[-1])
    update = apply_update(FailureOrder, marked, [], "update-1", payload_codec=serializer.AVRO_CODEC)
    assert update == CompleteUpdate("update-1", {"compensating": "member-2"})


@pytest.mark.parametrize("record", [False, True])
def test_an_unmarked_run_still_honours_a_recorded_decision(record: bool) -> None:
    """2.19 records which member it raised while siblings were open (#338)."""
    history, base = _run(marked=False)
    decision = {PARALLEL_FAILURE_DECISION: {
        "parallel_group_id": f"parallel-activities:{base}:{SIZE}", "parallel_group_base_sequence": base,
        "parallel_group_size": SIZE, "member_sequence": base + 2,
    }}
    history += [
        _member("ActivityFailed", 2, base), _member("ActivityFailed", 0, base),
        _member("ActivityCompleted", 1, base), _member("ActivityCompleted", 3, base),
        {"event_type": "SideEffectRecorded", "payload": {
            "sequence": base + SIZE, "result": serializer.envelope(decision, codec=serializer.AVRO_CODEC)}},
    ]
    assert _commands(history, record=record) == [ScheduleActivity("compensate", ["member-2"])]


def test_an_unknown_marker_version_is_nondeterministic() -> None:
    history, base = _run(marked=True)
    history[1]["payload"].update({"version": 2, "max_supported": 2})
    history += _settled_out_of_order(base)
    with pytest.raises(NonDeterministicReplayError):
        _commands(history)


# Reproducers for the Server histories in
# tests/fixtures/replay_regressions/failure-order-*.json, recorded on a Server
# built from server main 52e3ec00 by published 2.19.1 and by this version
# (sdk-python#307 review). Two members: m1 fails first, then m0.
_ONCE = {"max_attempts": 1}


def _review_group(ctx: WorkflowContext) -> list[Any]:
    return [ctx.schedule_activity(f"m{i}", [i], retry_policy=_ONCE) for i in range(2)]


@workflow.defn(name="rv348_handle")
class ReviewZeroCommandCatch:
    """Catch the group failure, then await a select() handle started earlier:
    the task that catches it records no command."""

    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def q(self) -> str | None:
        return self.caught

    @workflow.update("report")
    def report(self, *_: Any) -> dict[str, str | None]:
        return {"compensating": self.caught}

    def run(self, ctx: WorkflowContext, *_: Any):  # type: ignore[no-untyped-def]
        sel = yield ctx.select({"t": ctx.start_timer(1), "slow": ctx.schedule_activity("slow", [], retry_policy=_ONCE)})
        slow = sel.handles["slow"]
        yield ctx.start_timer(1)
        try:
            yield _review_group(ctx)
        except ActivityFailed as e:
            self.caught = e.message if hasattr(e, "message") else str(e)
        yield slow.await_result()
        yield ctx.schedule_activity("compensate", [self.caught], retry_policy=_ONCE)
        return self.caught


@workflow.defn(name="rv348_cancel")
class ReviewSelectCancel:
    """Catch the group failure, then cancel a select() loser started earlier."""

    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def q(self) -> str | None:
        return self.caught

    @workflow.update("report")
    def report(self, *_: Any) -> dict[str, str | None]:
        return {"compensating": self.caught}

    def run(self, ctx: WorkflowContext, *_: Any):  # type: ignore[no-untyped-def]
        sel = yield ctx.select({"t": ctx.start_timer(1), "slow": ctx.schedule_activity("slow", [], retry_policy=_ONCE)})
        slow = sel.handles["slow"]
        yield ctx.start_timer(1)
        try:
            yield _review_group(ctx)
        except ActivityFailed as e:
            self.caught = str(e)
        yield slow.cancel()
        yield ctx.schedule_activity("compensate", [self.caught], retry_policy=_ONCE)
        return self.caught


@workflow.defn(name="rv348_plain")
class ReviewPlain:
    """Catch the group failure, then schedule a compensation."""

    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.update("report")
    def report(self, *_: Any) -> dict[str, str | None]:
        return {"compensating": self.caught}

    @workflow.query("caught")
    def q(self) -> str | None:
        return self.caught

    def run(self, ctx: WorkflowContext, *_: Any):  # type: ignore[no-untyped-def]
        try:
            yield _review_group(ctx)
        except ActivityFailed as e:
            self.caught = str(e)
        yield ctx.schedule_activity("compensate", [self.caught], retry_policy=_ONCE)
        return self.caught


@pytest.mark.parametrize("raw,expected", [(None, True), ("", False), ("false", False), ("0", False),
                                          ("true", True), ("1", True), (" TRUE ", True)])
def test_the_worker_setting_reads_its_environment_variable(
    monkeypatch: pytest.MonkeyPatch, raw: str | None, expected: bool,
) -> None:
    if raw is None:
        monkeypatch.delenv(RECORD_PARALLEL_FAILURE_ORDER_ENV, raising=False)
    else:
        monkeypatch.setenv(RECORD_PARALLEL_FAILURE_ORDER_ENV, raw)
    worker = Worker(AsyncMock(spec=Client), task_queue="q", workflows=[FailureOrder])
    assert worker.record_parallel_failure_order is expected
    assert Worker(AsyncMock(spec=Client), task_queue="q", workflows=[FailureOrder],
                  record_parallel_failure_order=not expected).record_parallel_failure_order is (not expected)


@pytest.mark.parametrize("raw", ["yes", "on", "2", "v2"])
def test_a_malformed_worker_setting_is_refused(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv(RECORD_PARALLEL_FAILURE_ORDER_ENV, raw)
    with pytest.raises(ValueError, match=RECORD_PARALLEL_FAILURE_ORDER_ENV):
        Worker(AsyncMock(spec=Client), task_queue="q", workflows=[FailureOrder])
    monkeypatch.delenv(RECORD_PARALLEL_FAILURE_ORDER_ENV)
    with pytest.raises(ValueError, match="record_parallel_failure_order"):
        Worker(AsyncMock(spec=Client), task_queue="q", workflows=[FailureOrder],
               record_parallel_failure_order="true")  # type: ignore[arg-type]


@pytest.mark.parametrize("record", [None, False, True])
@pytest.mark.asyncio
async def test_the_worker_writes_the_marker_only_when_enabled(
    monkeypatch: pytest.MonkeyPatch, record: bool | None,
) -> None:
    monkeypatch.delenv(RECORD_PARALLEL_FAILURE_ORDER_ENV, raising=False)
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="q", workflows=[FailureOrder], record_parallel_failure_order=record)
    await worker._run_workflow_task({
        "task_id": "t-307", "workflow_type": "tests.parallel-failure-order", "workflow_task_attempt": 1,
        "history_events": [{"event_type": "WorkflowStarted", "payload": {}}],
        "arguments": serializer.encode([], codec="avro"), "payload_codec": "avro",
    })
    commands = client.complete_workflow_task.call_args.kwargs["commands"]
    types = [command["type"] for command in commands]
    assert types == (["record_version_marker"] if record is not False else []) + ["schedule_activity"] * SIZE
