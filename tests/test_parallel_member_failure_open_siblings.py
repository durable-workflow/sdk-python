"""sdk-python#338: an all() member fails while its siblings are still open.

Server wakes the workflow once for the failure. A later successful member never
wakes it again, because the group can no longer complete successfully. The
worker used to send the whole scheduling batch again on that wake, which
Server refused with ``409 parallel_sequence_mismatch``, and the run blocked.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import ActivityFailed, NonDeterministicReplayError
from durable_workflow.workflow import (
    PARALLEL_FAILURE_DECISION,
    ActivityRetryPolicy,
    CompleteWorkflow,
    FailWorkflow,
    RecordSideEffect,
    ScheduleActivity,
    WorkflowContext,
    commands_to_server_commands,
    query_state,
    replay,
)

SIZE = 4


def _entry(index: int, size: int = SIZE) -> dict[str, Any]:
    return {
        "parallel_group_id": f"parallel-activities:1:{size}",
        "parallel_group_kind": "activity",
        "parallel_group_base_sequence": 1,
        "parallel_group_size": size,
        "parallel_group_index": index,
    }


def _member(event_type: str, index: int, *, size: int = SIZE, **details: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sequence": index + 1,
        "activity_execution_id": f"member-{index}",
        "activity_type": f"member-{index}",
        **_entry(index, size),
        "parallel_group_path": [_entry(index, size)],
        **details,
    }
    if event_type == "ActivityCompleted":
        payload["result"] = serializer.envelope(index, codec=serializer.AVRO_CODEC)
    if event_type == "ActivityFailed":
        payload.setdefault("message", f"member-{index} failed")
    return {"event_type": event_type, "payload": payload}


def _scheduled(size: int = SIZE) -> list[dict[str, Any]]:
    return [{"event_type": "WorkflowStarted", "payload": {}},
            *(_member("ActivityScheduled", index, size=size) for index in range(size))]


def _decision(index: int) -> dict[str, Any]:
    """The side effect a task records when it raises member ``index`` early."""
    value = {PARALLEL_FAILURE_DECISION: {
        "parallel_group_id": f"parallel-activities:1:{SIZE}", "parallel_group_base_sequence": 1,
        "parallel_group_size": SIZE, "member_sequence": index + 1,
    }}
    return {"event_type": "SideEffectRecorded", "payload": {
        "sequence": SIZE + 1, "result": serializer.envelope(value, codec=serializer.AVRO_CODEC)}}


def _compensation(event_type: str, result: Any = None, *, sequence: int = SIZE + 2) -> dict[str, Any]:
    payload: dict[str, Any] = {"sequence": sequence, "activity_execution_id": "compensation",
                               "activity_type": "compensate"}
    if event_type == "ActivityCompleted":
        payload["result"] = serializer.envelope(result, codec=serializer.AVRO_CODEC)
    return {"event_type": event_type, "payload": payload}


@workflow.defn(name="tests.all-failure-open-siblings")
class AllFailureOpenSiblings:
    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def caught_failure(self) -> str | None:
        return self.caught

    @workflow.signal("poke")
    def poke(self) -> None:
        pass

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            values = yield [ctx.schedule_activity(f"member-{index}", []) for index in range(SIZE)]
        except ActivityFailed as failure:
            self.caught = failure.activity_type
            compensated = yield ctx.schedule_activity("compensate", [self.caught])
            return {"caught": self.caught, "compensated": compensated}
        return {"values": values}


@workflow.defn(name="tests.all-failure-open-siblings-uncaught")
class AllFailureUncaught:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield [ctx.schedule_activity(f"member-{index}", []) for index in range(SIZE)])


def _wire(history: list[dict[str, Any]], definition: type = AllFailureOpenSiblings) -> list[dict[str, Any]]:
    outcome = replay(definition, history, [], payload_codec=serializer.AVRO_CODEC)
    return commands_to_server_commands(outcome.commands, "queue", payload_codec=serializer.AVRO_CODEC)


def test_an_open_group_is_never_scheduled_again() -> None:
    history = _scheduled()
    assert _wire(history) == []
    history.append(_member("ActivityCompleted", 2))
    assert _wire(history) == []
    history.append({"event_type": "SignalReceived", "payload": {"signal_name": "poke", "arguments": None}})
    assert _wire(history) == []


def test_a_new_group_is_still_scheduled_once() -> None:
    commands = _wire([{"event_type": "WorkflowStarted", "payload": {}}])
    assert [command["type"] for command in commands] == ["schedule_activity"] * SIZE
    assert [command["parallel_group_index"] for command in commands] == list(range(SIZE))


def test_the_failure_wake_raises_and_records_the_member_instead_of_resending_the_group() -> None:
    history = [*_scheduled(), _member("ActivityCompleted", 0), _member("ActivityFailed", 2)]
    commands = _wire(history)
    assert [command["type"] for command in commands] == ["record_side_effect", "schedule_activity"]
    outcome = replay(AllFailureOpenSiblings, history, [], payload_codec=serializer.AVRO_CODEC)
    assert outcome.commands[0] == RecordSideEffect({PARALLEL_FAILURE_DECISION: {
        "parallel_group_id": f"parallel-activities:1:{SIZE}", "parallel_group_base_sequence": 1,
        "parallel_group_size": SIZE, "member_sequence": 3,
    }})
    assert commands[1]["activity_type"] == "compensate"
    assert "parallel_group_id" not in commands[1]
    assert query_state(AllFailureOpenSiblings, history, [], "caught") == "member-2"


def test_an_uncaught_failure_records_the_member_and_fails_the_run() -> None:
    history = [*_scheduled(), _member("ActivityFailed", 1)]
    outcome = replay(AllFailureUncaught, history, [], payload_codec=serializer.AVRO_CODEC)
    assert [type(command) for command in outcome.commands] == [RecordSideEffect, FailWorkflow]
    assert "member-1" in str(cast(FailWorkflow, outcome.commands[1]).message)


def test_a_handler_that_returns_at_once_still_records_the_member() -> None:
    @workflow.defn(name="tests.all-failure-open-siblings-return")
    class ReturnAtOnce:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield [ctx.schedule_activity(f"member-{index}", []) for index in range(SIZE)]
            except ActivityFailed as failure:
                return failure.activity_type
            return None

    history = [*_scheduled(), _member("ActivityFailed", 3)]
    outcome = replay(ReturnAtOnce, history, [], payload_codec=serializer.AVRO_CODEC)
    assert [type(command) for command in outcome.commands] == [RecordSideEffect, CompleteWorkflow]
    assert outcome.commands[1] == CompleteWorkflow("member-3")
    closed = [*history, _decision(3), {"event_type": "WorkflowCompleted", "payload": {}},
              _member("ActivityFailed", 0)]
    assert replay(ReturnAtOnce, closed, [], payload_codec=serializer.AVRO_CODEC).commands == [
        CompleteWorkflow("member-3"),
    ]


def _fail_fast_history() -> list[dict[str, Any]]:
    """Member 2 fails first and the workflow moves on, recording that decision.
    Then member 0, which has the lower position, fails too, and the others
    complete, some of them after the compensation completed."""
    return [
        *_scheduled(),
        _member("ActivityCompleted", 1),
        _member("ActivityFailed", 2),
        _decision(2),
        _compensation("ActivityScheduled"),
        _member("ActivityFailed", 0),
        _compensation("ActivityCompleted", "undone"),
        _member("ActivityCompleted", 3),
    ]


def test_every_later_task_replays_the_failure_the_workflow_handled() -> None:
    history = _fail_fast_history()
    frontier = len(_scheduled()) + 2
    completed = history.index(_compensation("ActivityCompleted", "undone")) + 1
    for end in [frontier, *range(frontier + 2, len(history) + 1)]:
        outcome = replay(AllFailureOpenSiblings, history[:end], [], payload_codec=serializer.AVRO_CODEC)
        if end == frontier:
            assert [type(command) for command in outcome.commands] == [RecordSideEffect, ScheduleActivity]
            assert outcome.commands[1] == ScheduleActivity("compensate", ["member-2"])
        elif end < completed:
            # The compensation is scheduled and still open.
            assert outcome.commands == [], end
        else:
            assert outcome.commands == [
                CompleteWorkflow({"caught": "member-2", "compensated": "undone"}),
            ], end
        assert query_state(AllFailureOpenSiblings, history[:end], [], "caught") == "member-2", end


def test_the_race_replays_the_recorded_member_not_the_lowest_position() -> None:
    """The woken task saw only member 2's failure and raised it. Before its
    completion was written, member 0 failed and every other member completed.
    Event order alone now shows a group that settled before the workflow moved
    on; the recorded decision keeps replay on member 2."""
    history = [
        *_scheduled(),
        _member("ActivityFailed", 2),
        _member("ActivityFailed", 0),
        _member("ActivityCompleted", 1),
        _member("ActivityCompleted", 3),
        _decision(2),
        _compensation("ActivityScheduled"),
        _compensation("ActivityCompleted", "undone"),
    ]
    outcome = replay(AllFailureOpenSiblings, history, [], payload_codec=serializer.AVRO_CODEC)
    assert outcome.commands == [CompleteWorkflow({"caught": "member-2", "compensated": "undone"})]
    assert query_state(AllFailureOpenSiblings, history, [], "caught") == "member-2"


@pytest.mark.parametrize("decision", [
    {"member_sequence": 2},
    {"member_sequence": 9},
    {"member_sequence": "3"},
    {"parallel_group_size": SIZE + 1},
    {"parallel_group_id": "parallel-activities:9:4"},
])
def test_a_decision_that_does_not_match_the_group_is_nondeterministic(decision: dict[str, Any]) -> None:
    recorded = _decision(2)
    value = serializer.decode_envelope(recorded["payload"]["result"])
    value[PARALLEL_FAILURE_DECISION].update(decision)
    recorded["payload"]["result"] = serializer.envelope(value, codec=serializer.AVRO_CODEC)
    history = [*_scheduled(), _member("ActivityCompleted", 1), _member("ActivityFailed", 2), recorded,
               _compensation("ActivityScheduled")]
    with pytest.raises(NonDeterministicReplayError):
        replay(AllFailureOpenSiblings, history, [], payload_codec=serializer.AVRO_CODEC)


def test_an_ordinary_side_effect_after_a_settled_group_is_not_a_decision() -> None:
    @workflow.defn(name="tests.all-failure-then-side-effect")
    class SideEffectAfterGroup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield [ctx.schedule_activity(f"member-{index}", []) for index in range(SIZE)]
            except ActivityFailed as failure:
                return [failure.activity_type, (yield ctx.side_effect(lambda: "noted"))]
            return None

    history = [
        *_scheduled(),
        _member("ActivityFailed", 2),
        _member("ActivityFailed", 0),
        _member("ActivityCompleted", 1),
        _member("ActivityCompleted", 3),
        {"event_type": "SideEffectRecorded", "payload": {
            "sequence": SIZE + 1, "result": serializer.envelope("noted", codec=serializer.AVRO_CODEC)}},
    ]
    assert replay(SideEffectAfterGroup, history, [], payload_codec=serializer.AVRO_CODEC).commands == [
        CompleteWorkflow(["member-0", "noted"]),
    ]


@pytest.mark.parametrize("late_first", [True, False])
def test_a_late_sibling_never_takes_the_compensation_result(late_first: bool) -> None:
    history = [*_scheduled(), _member("ActivityFailed", 0), _decision(0), _compensation("ActivityScheduled")]
    late = [_member("ActivityCompleted", index) for index in range(1, SIZE)]
    done = _compensation("ActivityCompleted", "undone")
    history.extend([*late, done] if late_first else [done, *late])
    outcome = replay(AllFailureOpenSiblings, history, [], payload_codec=serializer.AVRO_CODEC)
    assert outcome.commands == [CompleteWorkflow({"caught": "member-0", "compensated": "undone"})]


def test_a_group_that_settled_before_the_workflow_moved_on_keeps_positional_selection() -> None:
    """A history recorded while the worker waited for every member: the
    lower-position failure is the one raised, as before, and no decision is
    recorded because the task saw every outcome."""
    history = [
        *_scheduled(),
        _member("ActivityFailed", 2),
        _member("ActivityCompleted", 1),
        _member("ActivityFailed", 0),
        _member("ActivityCompleted", 3),
    ]
    commands = _wire(history)
    assert [(command["type"], command.get("activity_type")) for command in commands] == [
        ("schedule_activity", "compensate"),
    ]
    assert query_state(AllFailureOpenSiblings, history, [], "caught") == "member-0"


def test_a_history_from_another_runtime_raises_its_first_recorded_failure() -> None:
    """Workflow's embedded engine records no decision. A run it closed while a
    member was open raised that run's first recorded failure."""
    history = [
        *_scheduled(),
        _member("ActivityFailed", 3),
        {"event_type": "WorkflowFailed", "payload": {"message": "member-3 failed"}},
        _member("ActivityFailed", 0),
    ]
    outcome = replay(AllFailureUncaught, history, [], payload_codec=serializer.AVRO_CODEC)
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], FailWorkflow)
    assert "member-3" in str(outcome.commands[0].message)


# Reproducers for the native Server 3.13.0 histories in
# tests/fixtures/replay_regressions/all-member-failure-*.json: seven members,
# the second fails at once, the others run two at a time.
def _native_group(ctx: WorkflowContext) -> list[Any]:
    calls = [ctx.schedule_activity("py338.slow", [2], start_to_close_timeout=60) for _ in range(6)]
    calls.insert(1, ctx.schedule_activity("py338.boom", [0], start_to_close_timeout=60,
                                          retry_policy=ActivityRetryPolicy(max_attempts=1)))
    return calls


@workflow.defn(name="py338.uncaught")
class NativeUncaughtMemberFailure:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        results = yield _native_group(ctx)
        return len(results)


@workflow.defn(name="py338.caught")
class NativeCaughtMemberFailure:
    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def caught_member(self) -> str | None:
        return self.caught

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield _native_group(ctx)
        except ActivityFailed as failure:
            self.caught = f"{failure.activity_type}#{failure.step_sequence}"
            return (yield ctx.schedule_activity("py338.compensate", [failure.activity_type],
                                                start_to_close_timeout=60))
        return "no failure"


# Reproducer for the native Server histories in
# tests/fixtures/replay_regressions/settled-group-*.json (sdk-python#307).
# Member 1 fails at once and member 0 fails later; the others complete. The
# workflow worker was stopped while they ran, so the woken task saw the whole
# group settled. Published 2.19.1 raised member 0 by position; a run that
# recorded the failure-order marker raises member 1, whose failure was recorded
# first.
PY307_ACTIVITIES = "py307-activities"


def _py307_group(ctx: WorkflowContext) -> list[Any]:
    once = ActivityRetryPolicy(max_attempts=1)
    return [
        ctx.schedule_activity("py307.fail", [1.5], queue=PY307_ACTIVITIES, start_to_close_timeout=60,
                              retry_policy=once),
        ctx.schedule_activity("py307.fail", [0], queue=PY307_ACTIVITIES, start_to_close_timeout=60,
                              retry_policy=once),
        ctx.schedule_activity("py307.ok", [0], queue=PY307_ACTIVITIES, start_to_close_timeout=60),
        ctx.schedule_activity("py307.ok", [0.5], queue=PY307_ACTIVITIES, start_to_close_timeout=60),
    ]


@workflow.defn(name="py307.caught")
class NativeSettledGroupCaught:
    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def caught_member(self) -> str | None:
        return self.caught

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield _py307_group(ctx)
        except ActivityFailed as failure:
            self.caught = f"{failure.activity_type}#{failure.step_sequence}"
            return (yield ctx.schedule_activity("py307.compensate", [self.caught], queue=PY307_ACTIVITIES,
                                                start_to_close_timeout=60))
        return "no failure"
