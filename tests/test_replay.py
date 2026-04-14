from __future__ import annotations

from durable_workflow import workflow
from durable_workflow.workflow import (
    CompleteWorkflow,
    FailWorkflow,
    ScheduleActivity,
    StartTimer,
    WorkflowContext,
    replay,
)


@workflow.defn(name="simple-return")
class SimpleReturn:
    def run(self, ctx: WorkflowContext) -> str:
        return "done"


@workflow.defn(name="one-activity")
class OneActivity:
    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("greet", [name])
        return {"greeting": result}


@workflow.defn(name="two-activities")
class TwoActivities:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        a = yield ctx.schedule_activity("step1", [])
        b = yield ctx.schedule_activity("step2", [a])
        return [a, b]


@workflow.defn(name="timer-workflow")
class TimerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.start_timer(5)
        result = yield ctx.schedule_activity("greet", ["after-timer"])
        return result


@workflow.defn(name="failing-workflow")
class FailingWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("step1", [])
        raise ValueError("something went wrong")


class TestSimpleReturn:
    def test_non_generator_completes(self) -> None:
        outcome = replay(SimpleReturn, [], [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == "done"


class TestOneActivity:
    def test_first_replay_schedules(self) -> None:
        outcome = replay(OneActivity, [], ["world"])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "greet"
        assert cmd.arguments == ["world"]

    def test_completed_activity_triggers_completion(self) -> None:
        history = [{"event_type": "ActivityCompleted", "details": {"result": '"hello, world"'}}]
        outcome = replay(OneActivity, history, ["world"])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {"greeting": "hello, world"}

    def test_server_command_shape(self) -> None:
        outcome = replay(OneActivity, [], ["world"])
        cmd = outcome.commands[0]
        server_cmd = cmd.to_server_command("default-queue")
        assert server_cmd["type"] == "schedule_activity"
        assert server_cmd["activity_type"] == "greet"
        assert server_cmd["queue"] == "default-queue"


class TestTwoActivities:
    def test_first_schedules(self) -> None:
        outcome = replay(TwoActivities, [], [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "step1"

    def test_one_completed_schedules_next(self) -> None:
        history = [{"event_type": "ActivityCompleted", "details": {"result": '"val1"'}}]
        outcome = replay(TwoActivities, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "step2"
        assert outcome.commands[0].arguments == ["val1"]

    def test_both_completed(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "details": {"result": '"val1"'}},
            {"event_type": "ActivityCompleted", "details": {"result": '"val2"'}},
        ]
        outcome = replay(TwoActivities, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == ["val1", "val2"]


class TestTimerWorkflow:
    def test_first_replay_starts_timer(self) -> None:
        outcome = replay(TimerWorkflow, [], [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, StartTimer)
        assert cmd.delay_seconds == 5

    def test_timer_fired_schedules_activity(self) -> None:
        history = [{"event_type": "TimerFired", "details": {}}]
        outcome = replay(TimerWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "greet"

    def test_timer_and_activity_completed(self) -> None:
        history = [
            {"event_type": "TimerFired", "details": {}},
            {"event_type": "ActivityCompleted", "details": {"result": '"hi"'}},
        ]
        outcome = replay(TimerWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "hi"

    def test_timer_server_command_shape(self) -> None:
        outcome = replay(TimerWorkflow, [], [])
        server_cmd = outcome.commands[0].to_server_command("q")
        assert server_cmd["type"] == "start_timer"
        assert server_cmd["delay_seconds"] == 5


class TestFailingWorkflow:
    def test_exception_produces_fail_command(self) -> None:
        history = [{"event_type": "ActivityCompleted", "details": {"result": '"ok"'}}]
        outcome = replay(FailingWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, FailWorkflow)
        assert "something went wrong" in cmd.message
        assert cmd.exception_type == "ValueError"

    def test_fail_server_command_shape(self) -> None:
        history = [{"event_type": "ActivityCompleted", "details": {"result": '"ok"'}}]
        outcome = replay(FailingWorkflow, history, [])
        server_cmd = outcome.commands[0].to_server_command("q")
        assert server_cmd["type"] == "fail_workflow"
        assert "something went wrong" in server_cmd["message"]


class TestCompleteWorkflowCommand:
    def test_server_command(self) -> None:
        cmd = CompleteWorkflow(result={"key": "val"})
        server_cmd = cmd.to_server_command("q")
        assert server_cmd["type"] == "complete_workflow"
        assert '"key"' in server_cmd["result"]


class TestWorkflowRegistry:
    def test_registered(self) -> None:
        reg = workflow.registry()
        assert "simple-return" in reg
        assert "one-activity" in reg
        assert "timer-workflow" in reg
