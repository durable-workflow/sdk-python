"""sdk-python#338: an all() member fails while its siblings are still open.

Server wakes the workflow once for the failure. A later successful member never
wakes it again, because the group can no longer complete successfully. The
worker used to send the whole scheduling batch again on that wake, which
Server refused with ``409 parallel_sequence_mismatch``, and the run blocked.
"""

from __future__ import annotations

from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import ActivityFailed
from durable_workflow.workflow import (
    ActivityRetryPolicy,
    CompleteWorkflow,
    FailWorkflow,
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


def _compensation(event_type: str, result: Any = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"sequence": SIZE + 1, "activity_execution_id": "compensation",
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


def test_the_failure_wake_raises_instead_of_resending_the_group() -> None:
    history = [*_scheduled(), _member("ActivityCompleted", 0), _member("ActivityFailed", 2)]
    commands = _wire(history)
    assert len(commands) == 1
    assert commands[0]["type"] == "schedule_activity"
    assert commands[0]["activity_type"] == "compensate"
    assert "parallel_group_id" not in commands[0]
    assert query_state(AllFailureOpenSiblings, history, [], "caught") == "member-2"


def test_an_uncaught_failure_fails_the_run() -> None:
    history = [*_scheduled(), _member("ActivityFailed", 1)]
    outcome = replay(AllFailureUncaught, history, [], payload_codec=serializer.AVRO_CODEC)
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], FailWorkflow)
    assert outcome.commands[0].exception_type is not None or outcome.commands[0].message


def _fail_fast_history() -> list[dict[str, Any]]:
    """Member 2 fails first and the workflow moves on. Then member 0, which
    has the lower position, fails too, and the others complete, some of them
    after the compensation completed."""
    return [
        *_scheduled(),
        _member("ActivityCompleted", 1),
        _member("ActivityFailed", 2),
        _compensation("ActivityScheduled"),
        _member("ActivityFailed", 0),
        _compensation("ActivityCompleted", "undone"),
        _member("ActivityCompleted", 3),
    ]


def test_every_later_task_replays_the_failure_the_workflow_handled() -> None:
    history = _fail_fast_history()
    frontier = len(_scheduled()) + 2
    completed = history.index(_compensation("ActivityCompleted", "undone")) + 1
    for end in range(frontier, len(history) + 1):
        outcome = replay(AllFailureOpenSiblings, history[:end], [], payload_codec=serializer.AVRO_CODEC)
        if end == frontier:
            assert outcome.commands == [ScheduleActivity("compensate", ["member-2"])]
        elif end < completed:
            # The compensation is scheduled and still open.
            assert outcome.commands == [], end
        else:
            assert outcome.commands == [
                CompleteWorkflow({"caught": "member-2", "compensated": "undone"}),
            ], end
        assert query_state(AllFailureOpenSiblings, history[:end], [], "caught") == "member-2", end


@pytest.mark.parametrize("late_first", [True, False])
def test_a_late_sibling_never_takes_the_compensation_result(late_first: bool) -> None:
    history = [*_scheduled(), _member("ActivityFailed", 0), _compensation("ActivityScheduled")]
    late = [_member("ActivityCompleted", index) for index in range(1, SIZE)]
    done = _compensation("ActivityCompleted", "undone")
    history.extend([*late, done] if late_first else [done, *late])
    outcome = replay(AllFailureOpenSiblings, history, [], payload_codec=serializer.AVRO_CODEC)
    assert outcome.commands == [CompleteWorkflow({"caught": "member-0", "compensated": "undone"})]


def test_a_group_that_settled_before_the_workflow_moved_on_keeps_positional_selection() -> None:
    """A history recorded while the worker waited for every member: the
    lower-position failure is the one raised, as before."""
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


def test_a_closed_run_replays_the_failure_it_handled() -> None:
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
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield _native_group(ctx)
        except ActivityFailed as failure:
            return (yield ctx.schedule_activity("py338.compensate", [failure.activity_type],
                                                start_to_close_timeout=60))
        return "no failure"
