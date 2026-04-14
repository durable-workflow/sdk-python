from __future__ import annotations

from durable_workflow import workflow
from durable_workflow.workflow import (
    CompleteWorkflow,
    ContinueAsNew,
    FailWorkflow,
    RecordSideEffect,
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


@workflow.defn(name="continue-as-new-wf")
class ContinueAsNewWorkflow:
    def run(self, ctx: WorkflowContext, counter: int):  # type: ignore[no-untyped-def]
        if counter > 0:
            return ctx.continue_as_new(counter - 1)
        return "done"


@workflow.defn(name="continue-as-new-yield-wf")
class ContinueAsNewYieldWorkflow:
    def run(self, ctx: WorkflowContext, counter: int):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("step", [])
        return ctx.continue_as_new(counter - 1)


@workflow.defn(name="side-effect-wf")
class SideEffectWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        val = yield ctx.side_effect(lambda: 42)
        result = yield ctx.schedule_activity("use-val", [val])
        return result


@workflow.defn(name="context-wf")
class ContextWorkflow:
    def run(self, ctx: WorkflowContext) -> dict:  # type: ignore[type-arg]
        t = ctx.now().isoformat()
        r = ctx.random().random()
        u = str(ctx.uuid4())
        return {"time": t, "rand": r, "uuid": u}


class TestContinueAsNew:
    def test_non_generator_continue(self) -> None:
        outcome = replay(ContinueAsNewWorkflow, [], [3])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ContinueAsNew)
        assert cmd.arguments == [2]

    def test_non_generator_complete(self) -> None:
        outcome = replay(ContinueAsNewWorkflow, [], [0])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "done"

    def test_generator_return_continue(self) -> None:
        history = [{"event_type": "ActivityCompleted", "details": {"result": '"ok"'}}]
        outcome = replay(ContinueAsNewYieldWorkflow, history, [5])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ContinueAsNew)
        assert cmd.arguments == [4]

    def test_server_command_shape(self) -> None:
        cmd = ContinueAsNew(workflow_type="other", arguments=[1, 2], task_queue="q2")
        sc = cmd.to_server_command("default-q")
        assert sc["type"] == "continue_as_new"
        assert sc["workflow_type"] == "other"
        assert sc["task_queue"] == "q2"

    def test_server_command_defaults(self) -> None:
        cmd = ContinueAsNew(arguments=[1])
        sc = cmd.to_server_command("default-q")
        assert sc["task_queue"] == "default-q"
        assert "workflow_type" not in sc


class TestSideEffect:
    def test_first_replay_records(self) -> None:
        outcome = replay(SideEffectWorkflow, [], [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, RecordSideEffect)
        assert cmd.result == 42

    def test_replayed_side_effect_skips_fn(self) -> None:
        history = [{"event_type": "SideEffectRecorded", "details": {"result": "99"}}]
        outcome = replay(SideEffectWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.arguments == [99]

    def test_full_replay(self) -> None:
        history = [
            {"event_type": "SideEffectRecorded", "details": {"result": "42"}},
            {"event_type": "ActivityCompleted", "details": {"result": '"final"'}},
        ]
        outcome = replay(SideEffectWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "final"

    def test_server_command_shape(self) -> None:
        cmd = RecordSideEffect(result={"key": "val"})
        sc = cmd.to_server_command("q")
        assert sc["type"] == "record_side_effect"
        assert '"key"' in sc["result"]


class TestWorkflowContext:
    def test_now_returns_deterministic_time(self) -> None:
        from datetime import datetime, timezone
        t = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ctx = WorkflowContext(run_id="r1", current_time=t)
        assert ctx.now() == t

    def test_random_seeded_by_run_id(self) -> None:
        ctx1 = WorkflowContext(run_id="same-run")
        ctx2 = WorkflowContext(run_id="same-run")
        assert ctx1.random().random() == ctx2.random().random()

    def test_random_differs_by_run_id(self) -> None:
        ctx1 = WorkflowContext(run_id="run-a")
        ctx2 = WorkflowContext(run_id="run-b")
        assert ctx1.random().random() != ctx2.random().random()

    def test_uuid4_deterministic(self) -> None:
        ctx1 = WorkflowContext(run_id="run-x")
        ctx2 = WorkflowContext(run_id="run-x")
        assert ctx1.uuid4() == ctx2.uuid4()

    def test_uuid4_is_version_4(self) -> None:
        ctx = WorkflowContext(run_id="run-y")
        u = ctx.uuid4()
        assert u.version == 4

    def test_logger_silent_during_replay(self) -> None:
        import logging
        import logging.handlers
        ctx = WorkflowContext(run_id="r1")
        ctx.logger._set_replaying(True)
        logger = logging.getLogger("durable_workflow.workflow.replay")
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.MemoryHandler(capacity=100)
        logger.addHandler(handler)
        try:
            ctx.logger.info("should not appear")
            assert len(handler.buffer) == 0
        finally:
            logger.removeHandler(handler)

    def test_logger_active_when_not_replaying(self) -> None:
        import logging
        import logging.handlers
        ctx = WorkflowContext(run_id="r1")
        ctx.logger._set_replaying(False)
        logger = logging.getLogger("durable_workflow.workflow.replay")
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.MemoryHandler(capacity=100)
        logger.addHandler(handler)
        try:
            ctx.logger.info("should appear")
            assert len(handler.buffer) == 1
        finally:
            logger.removeHandler(handler)


class TestReplayWithRunId:
    def test_run_id_passed_to_context(self) -> None:
        outcome = replay(ContextWorkflow, [], [], run_id="test-run-123")
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result["uuid"]
        assert cmd.result["time"]

    def test_timestamp_from_history(self) -> None:
        history = [
            {"event_type": "WorkflowStarted", "details": {"timestamp": "2026-06-01T12:00:00Z"}},
        ]
        outcome = replay(ContextWorkflow, history, [], run_id="r1")
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert "2026-06-01" in cmd.result["time"]


class TestWorkflowRegistry:
    def test_registered(self) -> None:
        reg = workflow.registry()
        assert "simple-return" in reg
        assert "one-activity" in reg
        assert "timer-workflow" in reg
        assert "continue-as-new-wf" in reg
        assert "side-effect-wf" in reg
