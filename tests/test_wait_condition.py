from __future__ import annotations

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import NonDeterministicReplayError, QueryFailed
from durable_workflow.workflow import (
    CompleteWorkflow,
    ScheduleActivity,
    WaitCondition,
    WorkflowContext,
    query_state,
    replay,
)


def _signal_received_event(name: str, args: list, workflow_sequence: int | None = None) -> dict:
    payload = {
        "signal_name": name,
        "value": serializer.envelope(args),
        "payload_codec": serializer.AVRO_CODEC,
    }
    if workflow_sequence is not None:
        payload["workflow_sequence"] = workflow_sequence

    return {
        "event_type": "SignalReceived",
        "payload": payload,
    }


@workflow.defn(name="wait-until-approved")
class WaitUntilApproved:
    def __init__(self) -> None:
        self.approved: bool = False

    @workflow.signal("approve")
    def on_approve(self) -> None:
        self.approved = True

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.approved, key="approved")
        return "approved"


@workflow.defn(name="wait-with-timeout")
class WaitWithTimeout:
    def __init__(self) -> None:
        self.approved: bool = False

    @workflow.signal("approve")
    def on_approve(self) -> None:
        self.approved = True

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        satisfied = yield ctx.wait_condition(
            lambda: self.approved,
            key="approved",
            timeout=30,
        )
        return "approved" if satisfied else "timed_out"


@workflow.defn(name="wait-immediate-true")
class WaitImmediateTrue:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        satisfied = yield ctx.wait_condition(lambda: True, key="always")
        return "satisfied" if satisfied else "no"


@workflow.defn(name="activity-then-wait-for-approval")
class ActivityThenWaitForApproval:
    def __init__(self) -> None:
        self.activity_result: str | None = None
        self.approved_by: str | None = None

    @workflow.signal("approve")
    def approve(self, approved_by: str) -> None:
        self.approved_by = approved_by

    @workflow.query("state")
    def state(self) -> dict[str, str | None]:
        return {
            "activity_result": self.activity_result,
            "approved_by": self.approved_by,
        }

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        self.activity_result = yield ctx.schedule_activity("load-state", [])
        self.approved_by = None
        yield ctx.wait_condition(
            lambda: self.approved_by == "alice",
            key="approval",
            timeout=30,
        )

        return {
            "activity_result": self.activity_result,
            "approved_by": self.approved_by,
        }


@workflow.defn(name="two-stage-signal-wait")
class TwoStageSignalWait:
    def __init__(self) -> None:
        self.stage = "booting"
        self.name: str | None = None
        self.finished = False
        self.events: list[str] = []

    @workflow.signal("advance")
    def advance(self, name: str) -> None:
        self.name = name
        self.events.append(f"signal:{name}")

    @workflow.signal("finish")
    def finish(self) -> None:
        self.finished = True
        self.events.append("signal:finish")

    @workflow.query("state")
    def state(self) -> dict[str, object]:
        return {
            "stage": self.stage,
            "name": self.name,
            "finished": self.finished,
            "events": list(self.events),
        }

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        self.stage = "waiting-for-advance"
        self.events.append("started")
        yield ctx.wait_condition(lambda: self.name is not None, key="advance")

        self.stage = "waiting-for-finish"
        self.finished = False
        self.events.append(f"advanced:{self.name}")
        yield ctx.wait_condition(lambda: self.finished, key="finish")

        self.stage = "completed"
        self.events.append("finish")
        return self.state()


@workflow.defn(name="signal-counter-until-finished")
class SignalCounterUntilFinished:
    def __init__(self) -> None:
        self.count = 0
        self.done = False
        self.events: list[dict[str, object]] = []

    @workflow.signal("increment")
    def increment(self, amount: int) -> None:
        self.count += amount
        self.events.append({"signal": "increment", "amount": amount, "count": self.count})

    @workflow.signal("finish")
    def finish(self) -> None:
        self.done = True
        self.events.append({"signal": "finish", "count": self.count})

    @workflow.query("current")
    def current(self) -> dict[str, object]:
        return {
            "count": self.count,
            "done": self.done,
            "events": list(self.events),
        }

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.done, key="done")
        return self.current()


@workflow.defn(name="signal-counter-single-predicate-per-reopen")
class SignalCounterSinglePredicatePerReopen:
    def __init__(self) -> None:
        self.count = 0
        self.done = False
        self.receiver_generation = 0
        self.last_predicate_generation: int | None = None
        self.predicate_calls = 0

    @workflow.signal("increment")
    def increment(self, amount: int) -> None:
        self.count += amount
        self.receiver_generation += 1

    @workflow.signal("finish")
    def finish(self) -> None:
        self.done = True
        self.receiver_generation += 1

    def done_once_per_receiver(self) -> bool:
        self.predicate_calls += 1
        if self.last_predicate_generation == self.receiver_generation:
            raise RuntimeError("predicate re-evaluated before replay advanced")
        self.last_predicate_generation = self.receiver_generation
        return self.done

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        satisfied = yield ctx.wait_condition(self.done_once_per_receiver, key="done")
        return {
            "count": self.count,
            "satisfied": satisfied,
            "predicate_calls": self.predicate_calls,
        }


@workflow.defn(name="two-same-key-signal-waits")
class TwoSameKeySignalWaits:
    def __init__(self) -> None:
        self.ready = False
        self.stage = "booting"
        self.events: list[str] = []

    @workflow.signal("release")
    def release(self, label: str) -> None:
        self.ready = True
        self.events.append(f"signal:{label}")

    @workflow.query("state")
    def state(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "stage": self.stage,
            "events": list(self.events),
        }

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        self.stage = "first"
        yield ctx.wait_condition(lambda: self.ready, key="gate")
        self.events.append("passed:first")

        self.ready = False
        self.stage = "second"
        yield ctx.wait_condition(lambda: self.ready, key="gate")
        self.events.append("passed:second")

        self.stage = "completed"
        return self.state()


class TestCtxWaitCondition:
    def test_wait_condition_returns_dataclass_with_predicate_and_key(self) -> None:
        ctx = WorkflowContext(run_id="x")
        cmd = ctx.wait_condition(lambda: False, key="k", timeout=5.0)

        assert isinstance(cmd, WaitCondition)
        assert cmd.condition_key == "k"
        assert cmd.timeout_seconds == 5
        assert cmd.condition_definition_fingerprint is not None
        assert cmd.condition_definition_fingerprint.startswith("sha256:")
        assert callable(cmd.predicate)
        assert cmd.predicate() is False

    def test_wait_condition_rounds_timeout_up_to_whole_second(self) -> None:
        ctx = WorkflowContext(run_id="x")
        cmd = ctx.wait_condition(lambda: False, timeout=1.2)

        assert cmd.timeout_seconds == 2

    def test_wait_condition_zero_timeout_preserved(self) -> None:
        ctx = WorkflowContext(run_id="x")
        cmd = ctx.wait_condition(lambda: False, timeout=0)

        assert cmd.timeout_seconds == 0

    def test_wait_condition_no_timeout_remains_none(self) -> None:
        ctx = WorkflowContext(run_id="x")
        cmd = ctx.wait_condition(lambda: False)

        assert cmd.timeout_seconds is None
        assert cmd.condition_key is None


class TestWaitConditionToServerCommand:
    def test_emits_open_condition_wait_with_optional_fields(self) -> None:
        cmd = WaitCondition(
            predicate=lambda: True,
            condition_key="order",
            condition_definition_fingerprint="sha256:condition",
            timeout_seconds=60,
        )

        server_cmd = cmd.to_server_command("default")

        assert server_cmd == {
            "type": "open_condition_wait",
            "condition_key": "order",
            "condition_definition_fingerprint": "sha256:condition",
            "timeout_seconds": 60,
        }

    def test_emits_minimal_open_condition_wait_when_optional_fields_absent(self) -> None:
        cmd = WaitCondition(predicate=lambda: True)

        assert cmd.to_server_command("default") == {"type": "open_condition_wait"}


class TestReplayWaitCondition:
    def test_predicate_immediately_true_advances_without_emitting_command(self) -> None:
        outcome = replay(WaitImmediateTrue, [], [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "satisfied"

    def test_predicate_false_with_no_history_emits_open_condition_wait(self) -> None:
        outcome = replay(WaitUntilApproved, [], [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, WaitCondition)
        assert cmd.condition_key == "approved"
        assert cmd.timeout_seconds is None

    def test_signal_received_after_open_resolves_predicate_true_and_completes(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-1", "condition_key": "approved"},
            },
            _signal_received_event("approve", []),
        ]

        outcome = replay(WaitUntilApproved, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "approved"

    def test_condition_timeout_timer_fired_resolves_wait_as_timed_out(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-1", "condition_key": "approved"},
            },
            {
                "event_type": "TimerFired",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-1",
                },
            },
        ]

        outcome = replay(WaitWithTimeout, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "timed_out"

    def test_condition_wait_satisfied_event_resolves_wait_even_if_predicate_false(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-1", "condition_key": "approved"},
            },
            {
                "event_type": "ConditionWaitSatisfied",
                "payload": {"condition_wait_id": "wait-1"},
            },
        ]

        outcome = replay(WaitUntilApproved, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "approved"

    def test_signal_after_activity_result_applies_after_result_is_consumed(self) -> None:
        history = [
            {
                "event_type": "ActivityCompleted",
                "payload": {"result": serializer.envelope("loaded")},
            },
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-1", "condition_key": "approval"},
            },
            _signal_received_event("approve", ["alice"]),
        ]

        outcome = replay(ActivityThenWaitForApproval, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == {
            "activity_result": "loaded",
            "approved_by": "alice",
        }
        assert query_state(ActivityThenWaitForApproval, history, [], "state") == {
            "activity_result": "loaded",
            "approved_by": "alice",
        }

    def test_signals_scope_to_the_condition_wait_open_when_they_arrived(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-advance", "condition_key": "advance"},
            },
            _signal_received_event("advance", ["Ada"]),
            {
                "event_type": "ConditionWaitSatisfied",
                "payload": {"condition_wait_id": "wait-advance", "condition_key": "advance"},
            },
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-finish", "condition_key": "finish"},
            },
            _signal_received_event("finish", []),
        ]

        expected = {
            "stage": "completed",
            "name": "Ada",
            "finished": True,
            "events": ["started", "signal:Ada", "advanced:Ada", "signal:finish", "finish"],
        }

        outcome = replay(TwoStageSignalWait, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(TwoStageSignalWait, history, [], "state") == expected

    def test_repeated_physical_waits_for_one_logical_condition_replay_all_signals(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-count-3", "condition_key": "done"},
            },
            _signal_received_event("increment", [3]),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-count-8", "condition_key": "done"},
            },
            _signal_received_event("increment", [5]),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-finish", "condition_key": "done"},
            },
            _signal_received_event("finish", []),
        ]

        expected = {
            "count": 8,
            "done": True,
            "events": [
                {"signal": "increment", "amount": 3, "count": 3},
                {"signal": "increment", "amount": 5, "count": 8},
                {"signal": "finish", "count": 8},
            ],
        }

        outcome = replay(SignalCounterUntilFinished, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(SignalCounterUntilFinished, history, [], "current") == expected

    def test_repeated_physical_waits_keep_query_state_current_before_finish(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-count-3", "condition_key": "done"},
            },
            _signal_received_event("increment", [3]),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-count-8", "condition_key": "done"},
            },
            _signal_received_event("increment", [5]),
        ]

        outcome = replay(SignalCounterUntilFinished, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], WaitCondition)
        assert query_state(SignalCounterUntilFinished, history, [], "current") == {
            "count": 8,
            "done": False,
            "events": [
                {"signal": "increment", "amount": 3, "count": 3},
                {"signal": "increment", "amount": 5, "count": 8},
            ],
        }

    def test_satisfied_reopened_wait_reuses_current_predicate_result(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-count", "condition_key": "done"},
            },
            _signal_received_event("increment", [3]),
            {
                "event_type": "ConditionWaitSatisfied",
                "payload": {"condition_wait_id": "wait-count", "condition_key": "done"},
            },
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-finish", "condition_key": "done"},
            },
            _signal_received_event("finish", []),
            {
                "event_type": "ConditionWaitSatisfied",
                "payload": {"condition_wait_id": "wait-finish", "condition_key": "done"},
            },
        ]

        outcome = replay(SignalCounterSinglePredicatePerReopen, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == {
            "count": 3,
            "satisfied": True,
            "predicate_calls": 2,
        }

    def test_signal_received_before_next_wait_uses_declared_workflow_sequence(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-count-3",
                    "condition_key": "done",
                    "sequence": 1,
                },
            },
            _signal_received_event("increment", [3], workflow_sequence=1),
            _signal_received_event("finish", [], workflow_sequence=2),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-finish",
                    "condition_key": "done",
                    "sequence": 2,
                },
            },
            {
                "event_type": "ConditionWaitSatisfied",
                "payload": {
                    "condition_wait_id": "wait-finish",
                    "condition_key": "done",
                    "sequence": 2,
                },
            },
        ]

        expected = {
            "count": 3,
            "done": True,
            "events": [
                {"signal": "increment", "amount": 3, "count": 3},
                {"signal": "finish", "count": 3},
            ],
        }

        outcome = replay(SignalCounterUntilFinished, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(SignalCounterUntilFinished, history, [], "current") == expected

    @pytest.mark.parametrize("receiver_sequence", [None, 1])
    @pytest.mark.parametrize("changed_identity", [None, "condition_key", "condition_definition_fingerprint"])
    def test_signal_during_condition_completion_keeps_its_physical_reopen(
        self, receiver_sequence: int | None, changed_identity: str | None,
    ) -> None:
        history = [
            {"event_type": "ConditionWaitOpened", "payload": {
                "condition_wait_id": "wait-first", "condition_key": "done", "sequence": 1,
            }},
            _signal_received_event("increment", [3], workflow_sequence=receiver_sequence),
            _signal_received_event("finish", [], workflow_sequence=receiver_sequence),
            {"event_type": "SignalApplied", "payload": {"signal_name": "increment"}},
            {"event_type": "ConditionWaitSatisfied", "payload": {
                "condition_wait_id": "wait-first", "condition_key": "done", "sequence": 1,
            }},
            {"event_type": "ConditionWaitOpened", "payload": {
                "condition_wait_id": "wait-reopened", "condition_key": "done", "sequence": 2,
            }},
        ]

        if changed_identity is not None:
            history[-1]["payload"][changed_identity] = "changed-definition"
            with pytest.raises(NonDeterministicReplayError, match="ConditionWaitOpened") as captured:
                replay(SignalCounterUntilFinished, history, [])
            assert captured.value.workflow_sequence == 2
            with pytest.raises(QueryFailed, match="ConditionWaitOpened") as query_error:
                query_state(SignalCounterUntilFinished, history, [], "current")
            assert isinstance(query_error.value.__cause__, NonDeterministicReplayError)
            assert query_error.value.__cause__.workflow_sequence == 2
            return

        expected = {"count": 3, "done": True, "events": [
            {"signal": "increment", "amount": 3, "count": 3}, {"signal": "finish", "count": 3},
        ]}
        outcome = replay(SignalCounterUntilFinished, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(SignalCounterUntilFinished, history, [], "current") == expected

    def test_signal_received_before_reopened_wait_uses_next_wait_when_sequence_absent(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-count-3",
                    "condition_key": "done",
                    "sequence": 21,
                },
            },
            _signal_received_event("increment", [3]),
            _signal_received_event("finish", []),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "delay_seconds": 30,
                },
            },
        ]

        expected = {
            "count": 3,
            "done": True,
            "events": [
                {"signal": "increment", "amount": 3, "count": 3},
                {"signal": "finish", "count": 3},
            ],
        }

        outcome = replay(SignalCounterUntilFinished, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(SignalCounterUntilFinished, history, [], "current") == expected

    def test_signal_received_before_reopened_wait_uses_next_wait_when_sequence_is_stale(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-count-3",
                    "condition_key": "done",
                    "sequence": 21,
                },
            },
            _signal_received_event("increment", [3], workflow_sequence=21),
            _signal_received_event("finish", [], workflow_sequence=21),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "delay_seconds": 30,
                },
            },
        ]

        expected = {
            "count": 3,
            "done": True,
            "events": [
                {"signal": "increment", "amount": 3, "count": 3},
                {"signal": "finish", "count": 3},
            ],
        }

        outcome = replay(SignalCounterUntilFinished, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(SignalCounterUntilFinished, history, [], "current") == expected

    def test_stale_sequence_after_condition_resolution_binds_to_next_wait(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-advance",
                    "condition_key": "advance",
                    "sequence": 21,
                },
            },
            _signal_received_event("advance", ["Ada"], workflow_sequence=21),
            {
                "event_type": "ConditionWaitSatisfied",
                "payload": {
                    "condition_wait_id": "wait-advance",
                    "condition_key": "advance",
                    "sequence": 21,
                },
            },
            _signal_received_event("finish", [], workflow_sequence=21),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-finish",
                    "condition_key": "finish",
                    "sequence": 22,
                },
            },
        ]

        expected = {
            "stage": "completed",
            "name": "Ada",
            "finished": True,
            "events": ["started", "signal:Ada", "advanced:Ada", "signal:finish", "finish"],
        }

        outcome = replay(TwoStageSignalWait, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(TwoStageSignalWait, history, [], "state") == expected

    def test_terminal_pending_same_key_wait_without_prior_reopen_remains_history(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-before-finish",
                    "condition_key": "done",
                    "sequence": 21,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-before-finish",
                    "condition_key": "done",
                    "sequence": 21,
                    "delay_seconds": 30,
                },
            },
            _signal_received_event("finish", [], workflow_sequence=21),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "delay_seconds": 30,
                },
            },
        ]

        with pytest.raises(NonDeterministicReplayError) as exc_info:
            replay(SignalCounterUntilFinished, history, [])

        assert exc_info.value.workflow_sequence == 22
        assert exc_info.value.expected_shape == "complete workflow"
        assert "ConditionWaitOpened" in exc_info.value.recorded_event_types
        assert "TimerScheduled" in exc_info.value.recorded_event_types

    def test_terminal_stale_reopened_wait_after_false_reopen_is_consumed(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-count-3",
                    "condition_key": "done",
                    "sequence": 20,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-count-3",
                    "condition_key": "done",
                    "sequence": 20,
                    "delay_seconds": 30,
                },
            },
            _signal_received_event("increment", [3], workflow_sequence=20),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-before-finish",
                    "condition_key": "done",
                    "sequence": 21,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-before-finish",
                    "condition_key": "done",
                    "sequence": 21,
                    "delay_seconds": 30,
                },
            },
            _signal_received_event("finish", [], workflow_sequence=21),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "delay_seconds": 30,
                },
            },
        ]

        expected = {
            "count": 3,
            "done": True,
            "events": [
                {"signal": "increment", "amount": 3, "count": 3},
                {"signal": "finish", "count": 3},
            ],
        }

        outcome = replay(SignalCounterUntilFinished, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(SignalCounterUntilFinished, history, [], "current") == expected

    @pytest.mark.parametrize(
        ("resolution_event_type", "resolution_payload"),
        [
            (
                "ConditionWaitTimedOut",
                {
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                },
            ),
            (
                "ConditionWaitSatisfied",
                {
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                },
            ),
            (
                "TimerFired",
                {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                },
            ),
        ],
    )
    def test_terminal_reopened_wait_with_resolution_remains_replay_history(
        self,
        resolution_event_type: str,
        resolution_payload: dict[str, object],
    ) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-before-finish",
                    "condition_key": "done",
                    "sequence": 21,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-before-finish",
                    "condition_key": "done",
                    "sequence": 21,
                    "delay_seconds": 30,
                },
            },
            _signal_received_event("finish", [], workflow_sequence=21),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "timeout_seconds": 30,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-after-finish",
                    "condition_key": "done",
                    "sequence": 22,
                    "delay_seconds": 30,
                },
            },
            {
                "event_type": resolution_event_type,
                "payload": resolution_payload,
            },
        ]

        with pytest.raises(NonDeterministicReplayError) as exc_info:
            replay(SignalCounterUntilFinished, history, [])

        assert exc_info.value.workflow_sequence == 22
        assert exc_info.value.expected_shape == "complete workflow"
        assert "ConditionWaitOpened" in exc_info.value.recorded_event_types
        assert resolution_event_type in exc_info.value.recorded_event_types

    def test_sequential_same_key_waits_do_not_consume_second_wait_as_reopen(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-first",
                    "condition_key": "gate",
                    "sequence": 21,
                },
            },
            _signal_received_event("release", ["first"], workflow_sequence=21),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-second",
                    "condition_key": "gate",
                    "sequence": 22,
                },
            },
            _signal_received_event("release", ["second"], workflow_sequence=22),
        ]

        expected = {
            "ready": True,
            "stage": "completed",
            "events": [
                "signal:first",
                "passed:first",
                "signal:second",
                "passed:second",
            ],
        }

        outcome = replay(TwoSameKeySignalWaits, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == expected
        assert query_state(TwoSameKeySignalWaits, history, [], "state") == expected

    def test_pending_sequential_same_key_wait_replays_as_pending_wait(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-first",
                    "condition_key": "gate",
                    "sequence": 21,
                },
            },
            _signal_received_event("release", ["first"], workflow_sequence=21),
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-second",
                    "condition_key": "gate",
                    "sequence": 22,
                },
            },
        ]

        outcome = replay(TwoSameKeySignalWaits, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], WaitCondition)
        assert outcome.commands[0].condition_key == "gate"
        assert query_state(TwoSameKeySignalWaits, history, [], "state") == {
            "ready": False,
            "stage": "second",
            "events": ["signal:first", "passed:first"],
        }

    def test_repeated_wait_after_activity_can_be_satisfied_by_later_signal(self) -> None:
        history = [
            {
                "event_type": "ActivityCompleted",
                "payload": {"result": serializer.envelope("loaded")},
            },
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-before-query", "condition_key": "approval"},
            },
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-after-query", "condition_key": "approval"},
            },
            _signal_received_event("approve", ["alice"]),
        ]

        outcome = replay(ActivityThenWaitForApproval, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == {
            "activity_result": "loaded",
            "approved_by": "alice",
        }
        assert query_state(ActivityThenWaitForApproval, history, [], "state") == {
            "activity_result": "loaded",
            "approved_by": "alice",
        }

    def test_open_with_no_resolution_and_predicate_false_re_emits_wait_condition(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-1", "condition_key": "approved"},
            },
        ]

        outcome = replay(WaitUntilApproved, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], WaitCondition)

    def test_replay_rejects_changed_condition_key(self) -> None:
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-1", "condition_key": "old-key"},
            },
        ]

        with pytest.raises(NonDeterministicReplayError, match="key changed") as captured:
            replay(WaitUntilApproved, history, [])

        assert captured.value.workflow_sequence == 1
        assert "old-key" in str(captured.value)
        assert "approved" in str(captured.value)

    def test_replay_rejects_changed_condition_fingerprint(self) -> None:
        initial = replay(WaitUntilApproved, [], [])
        assert isinstance(initial.commands[0], WaitCondition)
        fingerprint = initial.commands[0].condition_definition_fingerprint
        assert fingerprint is not None
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-1",
                    "condition_key": "approved",
                    "condition_definition_fingerprint": "sha256:different",
                },
            },
        ]

        with pytest.raises(NonDeterministicReplayError, match="predicate fingerprint changed") as captured:
            replay(WaitUntilApproved, history, [])

        assert captured.value.workflow_sequence == 1
        assert "sha256:different" in str(captured.value)
        assert fingerprint in str(captured.value)

    def test_replay_accepts_matching_condition_fingerprint(self) -> None:
        initial = replay(WaitUntilApproved, [], [])
        assert isinstance(initial.commands[0], WaitCondition)
        fingerprint = initial.commands[0].condition_definition_fingerprint
        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "condition_wait_id": "wait-1",
                    "condition_key": "approved",
                    "condition_definition_fingerprint": fingerprint,
                },
            },
            _signal_received_event("approve", []),
        ]

        outcome = replay(WaitUntilApproved, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "approved"

    def test_condition_timeout_does_not_pollute_start_timer_resolved_results(self) -> None:
        @workflow.defn(name="wait-then-sleep")
        class WaitThenSleep:
            def __init__(self) -> None:
                self.approved = False

            @workflow.signal("approve")
            def on_approve(self) -> None:
                self.approved = True

            def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
                yield ctx.wait_condition(lambda: self.approved, timeout=10)
                yield ctx.sleep(60)
                return "done"

        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {"condition_wait_id": "wait-1"},
            },
            {
                "event_type": "TimerFired",
                "payload": {
                    "timer_kind": "condition_timeout",
                    "condition_wait_id": "wait-1",
                },
            },
        ]

        outcome = replay(WaitThenSleep, history, [])

        # condition_timeout should not satisfy ctx.sleep — that would advance
        # the workflow past the sleep too. We expect to land at the sleep yield.
        assert len(outcome.commands) == 1
        from durable_workflow.workflow import StartTimer

        assert isinstance(outcome.commands[0], StartTimer)
        assert outcome.commands[0].delay_seconds == 60

    def test_metadata_poor_condition_timeout_does_not_hide_following_timer(self) -> None:
        @workflow.defn(name="timed-wait-then-sleep")
        class TimedWaitThenSleep:
            def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
                yield ctx.wait_condition(lambda: False, timeout=10)
                yield ctx.sleep(60)
                return "done"

        history = [
            {
                "event_type": "ConditionWaitOpened",
                "payload": {
                    "sequence": 8,
                    "condition_wait_id": "wait-8",
                    "timeout_seconds": 10,
                },
            },
            {
                "event_type": "TimerScheduled",
                "payload": {"sequence": 9, "timer_id": "condition-timer", "delay_seconds": 10},
            },
            {
                "event_type": "TimerFired",
                "payload": {"sequence": 9, "timer_id": "condition-timer", "delay_seconds": 10},
            },
            {
                "event_type": "TimerScheduled",
                "payload": {"sequence": 10, "timer_id": "ordinary-timer"},
            },
            {
                "event_type": "TimerFired",
                "payload": {"sequence": 10, "timer_id": "ordinary-timer"},
            },
        ]

        outcome = replay(TimedWaitThenSleep, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "done"


# #245: a reopened wait keeps the deadline fixed at its first open.
@workflow.defn(name="tests.replay.condition-remaining-timeout")
class RemainingTimeoutApproval:
    def __init__(self) -> None:
        self.approved = False

    @workflow.signal("note")
    def note(self) -> None:
        return None

    @workflow.signal("approve")
    def approve(self) -> None:
        self.approved = True

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield ctx.wait_condition(lambda: self.approved, key="approval", timeout=300))


def _timed_open(wait_id: str, sequence: int, at: str, timeout: int = 300) -> list[dict]:
    return [
        {"event_type": "ConditionWaitOpened", "recorded_at": at, "payload": {
            "condition_wait_id": wait_id, "condition_key": "approval", "sequence": sequence,
            "timeout_seconds": timeout}},
        {"event_type": "TimerScheduled", "recorded_at": at, "payload": {
            "timer_kind": "condition_timeout", "condition_wait_id": wait_id, "condition_key": "approval",
            "sequence": sequence, "delay_seconds": timeout}},
    ]


def _timed_signal(name: str, sequence: int, at: str) -> dict:
    return {**_signal_received_event(name, [], workflow_sequence=sequence), "recorded_at": at}


START = {"event_type": "WorkflowStarted", "recorded_at": "2026-11-03T00:00:00+00:00", "payload": {}}


def _reopened_timeout(history: list[dict]) -> int | None:
    (command,) = replay(RemainingTimeoutApproval, history, []).commands
    assert isinstance(command, WaitCondition)
    return command.timeout_seconds


class TestReopenedWaitRemainingTimeout:
    def test_a_non_satisfying_signal_reopens_with_the_remaining_time(self) -> None:
        history = [START, *_timed_open("w1", 1, "2026-11-03T00:00:00+00:00"),
                   _timed_signal("note", 1, "2026-11-03T00:02:00.250000+00:00")]
        # Deadline 00:05:00 less 00:02:00.25, rounded up.
        assert _reopened_timeout(history) == 180

    def test_a_second_reopen_keeps_the_first_deadline(self) -> None:
        history = [START, *_timed_open("w1", 1, "2026-11-03T00:00:00+00:00"),
                   _timed_signal("note", 1, "2026-11-03T00:02:00+00:00"),
                   *_timed_open("w2", 2, "2026-11-03T00:02:01+00:00", timeout=180),
                   _timed_signal("note", 2, "2026-11-03T00:04:30+00:00")]
        assert _reopened_timeout(history) == 30

    def test_a_reopen_after_the_deadline_carries_zero(self) -> None:
        history = [START, *_timed_open("w1", 1, "2026-11-03T00:00:00+00:00"),
                   _timed_signal("note", 1, "2026-11-03T00:06:00+00:00")]
        assert _reopened_timeout(history) == 0

    def test_history_recorded_with_full_reopen_timeouts_still_replays(self) -> None:
        # Earlier SDKs reopened with the full timeout; the open is not compared.
        history = [START, *_timed_open("w1", 1, "2026-11-03T00:00:00+00:00"),
                   _timed_signal("note", 1, "2026-11-03T00:02:00+00:00"),
                   *_timed_open("w2", 2, "2026-11-03T00:02:01+00:00", timeout=300),
                   _timed_signal("note", 2, "2026-11-03T00:03:00+00:00")]
        assert _reopened_timeout(history) == 120

    def test_a_satisfying_signal_completes_without_a_reopen(self) -> None:
        history = [START, *_timed_open("w1", 1, "2026-11-03T00:00:00+00:00"),
                   _timed_signal("approve", 1, "2026-11-03T00:02:00+00:00")]
        (command,) = replay(RemainingTimeoutApproval, history, []).commands
        assert command == CompleteWorkflow(True)

    def test_a_first_open_keeps_the_authored_timeout(self) -> None:
        (command,) = replay(RemainingTimeoutApproval, [START], []).commands
        assert isinstance(command, WaitCondition) and command.timeout_seconds == 300

    def test_history_without_recorded_times_keeps_the_authored_timeout(self) -> None:
        history = [{"event_type": "WorkflowStarted", "payload": {"timestamp": "2026-11-03T00:00:00+00:00"}},
                   *[{key: value for key, value in event.items() if key != "recorded_at"}
                     for event in _timed_open("w1", 1, "2026-11-03T00:00:00+00:00")],
                   _signal_received_event("note", [], workflow_sequence=1)]
        assert _reopened_timeout(history) == 300

    @pytest.mark.parametrize(("signalled_at", "expected"), [
        ("2026-11-04T02:38:05.345000+00:00", 180),
        ("2026-11-04T02:40:05+00:00", 61),
        ("2026-11-04T02:41:06+00:00", 0),
    ])
    def test_sdk_php_false_wake_deadline_vectors(self, signalled_at: str, expected: int) -> None:
        # The same vectors as sdk-php ReplayerTest::falseWakeDeadlines (#269).
        start = {**START, "recorded_at": "2026-11-04T02:36:05+00:00"}
        history = [start, *_timed_open("w1", 1, "2026-11-04T02:36:05.295000+00:00"),
                   _timed_signal("note", 1, signalled_at)]
        assert _reopened_timeout(history) == expected


# A separate wait after a timed-out one is a new logical wait: it measures from
# its own open (General's sdk-php#271 review vectors).
@workflow.defn(name="tests.replay.condition-grace-after-timeout")
class GraceAfterTimeout:
    grace_seconds = 60

    def __init__(self) -> None:
        self.approved = False

    @workflow.signal("note")
    def note(self) -> None:
        return None

    @workflow.signal("approve")
    def approve(self) -> None:
        self.approved = True

    def ready(self) -> bool:
        return self.approved

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        if (yield ctx.wait_condition(self.ready, key="approval", timeout=300)):
            return "approved"
        late = yield ctx.wait_condition(self.ready, key="approval", timeout=type(self).grace_seconds)
        return "late" if late else "expired"


@workflow.defn(name="tests.replay.condition-equal-grace-after-timeout")
class EqualGraceAfterTimeout(GraceAfterTimeout):
    grace_seconds = 300


def _timed_out_then_grace(grace: int, signal: str) -> list[dict]:
    fired = {"event_type": "TimerFired", "recorded_at": "2026-11-03T00:05:00+00:00", "payload": {
        "timer_kind": "condition_timeout", "condition_wait_id": "w1", "condition_key": "approval", "sequence": 1}}
    return [START, *_timed_open("w1", 1, "2026-11-03T00:00:00+00:00"), fired,
            *_timed_open("w2", 2, "2026-11-03T00:05:00+00:00", timeout=grace),
            _timed_signal(signal, 2, "2026-11-03T00:05:29+00:00")]


class TestWaitAfterATimedOutWait:
    def test_approval_during_the_grace_wait_is_late(self) -> None:
        (command,) = replay(GraceAfterTimeout, _timed_out_then_grace(60, "approve"), []).commands
        assert command == CompleteWorkflow("late")

    def test_a_wrong_signal_reopens_the_grace_wait_from_its_own_open(self) -> None:
        (command,) = replay(GraceAfterTimeout, _timed_out_then_grace(60, "note"), []).commands
        assert isinstance(command, WaitCondition) and command.timeout_seconds == 31

    def test_an_equal_timeout_grace_wait_is_still_a_new_wait(self) -> None:
        (command,) = replay(EqualGraceAfterTimeout, _timed_out_then_grace(300, "note"), []).commands
        assert isinstance(command, WaitCondition) and command.timeout_seconds == 271


def test_a_selection_condition_member_is_not_reopened_by_a_wrong_signal() -> None:
    # Ordinary parallel lists refuse WaitCondition. A condition inside
    # ctx.select is never re-sent while open, so its original timer keeps its
    # deadline and the remaining-time rule does not apply to it (#245).
    from tests.test_durable_selection import (
        DurableSelectionConditionSignalReplayWorkflow,
        _condition_terminal_selection_history,
    )

    opens = [dict(event, recorded_at="2026-11-03T00:00:00+00:00")
             for event in _condition_terminal_selection_history("ConditionWaitSatisfied", "valid")
             if event["event_type"] in ("ConditionWaitOpened", "ActivityScheduled")]
    wrong = {"event_type": "SignalReceived", "recorded_at": "2026-11-03T00:00:03+00:00", "payload": {
        "sequence": 1, "signal_name": "change-value", "value": serializer.envelope(["wrong"]),
        "payload_codec": serializer.AVRO_CODEC}}
    outcome = replay(DurableSelectionConditionSignalReplayWorkflow, [START, *opens, wrong], [])
    assert list(outcome.commands) == []


# #253 (sdk-php#283): an orphaned duplicate open whose timer fires later does
# not end the logical wait. Server 3.0.x can record a second open of the same
# wait while the first is still open (server#1015); the first's timer stays live.
@workflow.defn(name="tests.replay.condition-orphaned-duplicate")
class OrphanedDuplicateDecision:
    def __init__(self) -> None:
        self.decided = False

    @workflow.signal("decision")
    def decision(self, approved: bool) -> None:
        self.decided = bool(approved)

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        decided = yield ctx.wait_condition(lambda: self.decided, key="decision-1", timeout=300)
        if not decided:
            yield ctx.schedule_activity("expire-proposal", [])
            return "expired"
        return "decided"


def _orphaned_duplicate_history(reopen: int, orphan_fired: bool) -> list[dict]:
    def open_wait(wait_id: str, sequence: int, at: str, timeout: int) -> list[dict]:
        return [
            {"event_type": "ConditionWaitOpened", "recorded_at": at, "payload": {
                "condition_wait_id": wait_id, "condition_key": "decision-1", "sequence": sequence,
                "timeout_seconds": timeout}},
            {"event_type": "TimerScheduled", "recorded_at": at, "payload": {
                "timer_kind": "condition_timeout", "condition_wait_id": wait_id, "condition_key": "decision-1",
                "sequence": sequence, "delay_seconds": timeout}},
        ]

    history = [
        {"event_type": "WorkflowStarted", "recorded_at": "2026-11-04T04:07:36.890000+00:00", "payload": {}},
        *open_wait("w3", 1, "2026-11-04T04:07:36.890000+00:00", 300),
        *open_wait("w4", 2, "2026-11-04T04:07:36.908000+00:00", 300),
        {"event_type": "SignalReceived", "recorded_at": "2026-11-04T04:09:36.979000+00:00", "payload": {
            "signal_name": "decision", "value": serializer.envelope([False]), "payload_codec": serializer.AVRO_CODEC,
            "workflow_sequence": 2}},
        {"event_type": "ConditionWaitSatisfied", "recorded_at": "2026-11-04T04:09:37.008000+00:00", "payload": {
            "condition_wait_id": "w4", "condition_key": "decision-1", "sequence": 2}},
        {"event_type": "TimerCancelled", "recorded_at": "2026-11-04T04:09:37.008000+00:00", "payload": {
            "timer_kind": "condition_timeout", "condition_wait_id": "w4", "sequence": 2}},
        *open_wait("w5", 3, "2026-11-04T04:09:37.010000+00:00", reopen),
    ]
    if orphan_fired:
        history.append({"event_type": "TimerFired", "recorded_at": "2026-11-04T04:12:36.956000+00:00", "payload": {
            "timer_kind": "condition_timeout", "condition_wait_id": "w3", "condition_key": "decision-1",
            "sequence": 1}})
    return history


class TestOrphanedDuplicateWait:
    @pytest.mark.parametrize("reopen", [300, 180])
    def test_the_orphans_timer_does_not_end_the_reopened_wait(self, reopen: int) -> None:
        (command,) = replay(OrphanedDuplicateDecision, _orphaned_duplicate_history(reopen, True), []).commands
        # The logical wait is still the reopened one; its first deadline has passed.
        assert isinstance(command, WaitCondition) and command.timeout_seconds == 0

    @pytest.mark.parametrize("reopen", [300, 180])
    def test_before_the_orphan_fires_the_wait_reopens_with_the_remaining_time(self, reopen: int) -> None:
        (command,) = replay(OrphanedDuplicateDecision, _orphaned_duplicate_history(reopen, False), []).commands
        assert isinstance(command, WaitCondition) and command.timeout_seconds == 180

    def test_the_reopened_waits_own_timeout_expires_the_decision(self) -> None:
        history = _orphaned_duplicate_history(180, True) + [
            {"event_type": "TimerFired", "recorded_at": "2026-11-04T04:12:37.216000+00:00", "payload": {
                "timer_kind": "condition_timeout", "condition_wait_id": "w5", "condition_key": "decision-1",
                "sequence": 3}},
        ]
        (command,) = replay(OrphanedDuplicateDecision, history, []).commands
        assert isinstance(command, ScheduleActivity) and command.activity_type == "expire-proposal"
