from __future__ import annotations

from durable_workflow import workflow
from durable_workflow.workflow import CompleteWorkflow, StartTimer, WorkflowContext, replay


@workflow.defn(name="sleep-int")
class SleepInt:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.sleep(30)
        return "done"


@workflow.defn(name="sleep-float")
class SleepFloat:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.sleep(1.5)
        return "done"


@workflow.defn(name="sleep-zero")
class SleepZero:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.sleep(0)
        return "done"


class TestCtxSleep:
    def test_sleep_integer_seconds_emits_start_timer(self) -> None:
        outcome = replay(SleepInt, [], [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], StartTimer)
        assert outcome.commands[0].delay_seconds == 30

    def test_sleep_fractional_seconds_rounds_up(self) -> None:
        outcome = replay(SleepFloat, [], [])

        assert isinstance(outcome.commands[0], StartTimer)
        assert outcome.commands[0].delay_seconds == 2

    def test_sleep_zero_still_emits_zero_delay_timer(self) -> None:
        outcome = replay(SleepZero, [], [])

        assert isinstance(outcome.commands[0], StartTimer)
        assert outcome.commands[0].delay_seconds == 0

    def test_sleep_clamps_negative_input_to_zero(self) -> None:
        ctx = WorkflowContext(run_id="x")
        cmd = ctx.sleep(-5)

        assert isinstance(cmd, StartTimer)
        assert cmd.delay_seconds == 0

    def test_sleep_followed_by_return_completes_after_timer_fires(self) -> None:
        outcome = replay(
            SleepInt,
            [{"event_type": "TimerFired", "payload": {}}],
            [],
        )

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "done"
