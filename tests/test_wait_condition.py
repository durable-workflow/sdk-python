from __future__ import annotations

from durable_workflow import serializer, workflow
from durable_workflow.workflow import (
    CompleteWorkflow,
    FailWorkflow,
    WaitCondition,
    WorkflowContext,
    replay,
)


def _signal_received_event(name: str, args: list) -> dict:
    return {
        "event_type": "SignalReceived",
        "payload": {
            "signal_name": name,
            "value": serializer.envelope(args),
            "payload_codec": serializer.AVRO_CODEC,
        },
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

        outcome = replay(WaitUntilApproved, history, [])

        assert len(outcome.commands) == 1
        assert not isinstance(outcome.commands[0], CompleteWorkflow)
        cmd = outcome.commands[0]
        assert isinstance(cmd, FailWorkflow)
        assert cmd.exception_type == "NonDeterministicWaitCondition"
        assert "key changed" in cmd.message

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

        outcome = replay(WaitUntilApproved, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, FailWorkflow)
        assert cmd.exception_type == "NonDeterministicWaitCondition"
        assert "predicate fingerprint changed" in cmd.message
        assert fingerprint in cmd.message

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
