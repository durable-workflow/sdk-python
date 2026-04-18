from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.errors import WorkflowFailed
from durable_workflow.testing import (
    WorkflowEnvironment,
    replay_history,
    replay_history_file,
)
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext


@workflow.defn(name="simple-greeter")
class SimpleGreeter:
    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        greeting = yield ctx.schedule_activity("greet", [name])
        return greeting


@workflow.defn(name="three-activity-pipeline")
class ThreeActivityPipeline:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        a = yield ctx.schedule_activity("alpha", [])
        b = yield ctx.schedule_activity("beta", [a])
        c = yield ctx.schedule_activity("gamma", [b])
        return {"a": a, "b": b, "c": c}


@workflow.defn(name="failing-workflow-for-test")
class FailingWorkflowForTest:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("step", [])
        raise RuntimeError("boom")


@workflow.defn(name="timer-then-activity")
class TimerThenActivity:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.sleep(5)
        result = yield ctx.schedule_activity("after_timer", [])
        return result


@workflow.defn(name="signal-approval-harness")
class SignalApprovalHarness:
    def __init__(self) -> None:
        self.approved_by: str | None = None

    @workflow.signal("approve")
    def on_approve(self, by: str) -> None:
        self.approved_by = by

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("wait", [])
        return {"approved_by": self.approved_by}


class TestExecuteWorkflow:
    def test_drives_single_activity_workflow_to_completion(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("greet", "hello, world")

        result = env.execute_workflow(SimpleGreeter, "world")

        assert result == "hello, world"

    def test_drives_multi_activity_pipeline_with_canned_results(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("alpha", 1)
        env.register_activity_result("beta", 2)
        env.register_activity_result("gamma", 3)

        result = env.execute_workflow(ThreeActivityPipeline)

        assert result == {"a": 1, "b": 2, "c": 3}

    def test_callable_activity_mock_receives_arguments(self) -> None:
        captured: list[Any] = []

        def record_call(name: str) -> str:
            captured.append(name)
            return f"greeted:{name}"

        env = WorkflowEnvironment()
        env.register_activity("greet", record_call)

        result = env.execute_workflow(SimpleGreeter, "alice")

        assert result == "greeted:alice"
        assert captured == ["alice"]

    def test_timers_auto_fire_so_workflow_does_not_block(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("after_timer", "timer-done")

        result = env.execute_workflow(TimerThenActivity)

        assert result == "timer-done"

    def test_failing_workflow_raises_workflow_failed(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("step", None)

        with pytest.raises(WorkflowFailed) as exc_info:
            env.execute_workflow(FailingWorkflowForTest)

        assert "boom" in str(exc_info.value)

    def test_missing_activity_mock_raises_loudly(self) -> None:
        env = WorkflowEnvironment()
        # No mock registered for 'greet'.

        with pytest.raises(KeyError) as exc_info:
            env.execute_workflow(SimpleGreeter, "world")

        assert "greet" in str(exc_info.value)

    def test_iteration_limit_stops_runaway_workflow(self) -> None:
        @workflow.defn(name="infinite-loop")
        class InfiniteLoop:
            def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
                while True:
                    yield ctx.schedule_activity("spin", [])

        env = WorkflowEnvironment(iteration_limit=5)
        env.register_activity_result("spin", None)

        with pytest.raises(RuntimeError, match="did not terminate"):
            env.execute_workflow(InfiniteLoop)


class TestSignalInjection:
    def test_queued_signal_is_dispatched_to_handler_before_first_iteration(self) -> None:
        env = WorkflowEnvironment()
        env.signal("approve", ["alice"])
        env.register_activity_result("wait", None)

        result = env.execute_workflow(SignalApprovalHarness)

        assert result == {"approved_by": "alice"}

    def test_no_signal_leaves_state_at_default(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("wait", None)

        result = env.execute_workflow(SignalApprovalHarness)

        assert result == {"approved_by": None}


class TestReplayHistory:
    def test_replay_history_accepts_event_list(self) -> None:
        outcome = replay_history(
            SimpleGreeter,
            [
                {
                    "event_type": "ActivityCompleted",
                    "payload": {"result": {"codec": "json", "blob": '"recorded"'}},
                }
            ],
            ["world"],
            payload_codec="json",
        )

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "recorded"

    def test_replay_history_file_loads_json_list(self, tmp_path: Path) -> None:
        history_path = tmp_path / "history.json"
        history_path.write_text(
            json.dumps(
                [
                    {
                        "event_type": "ActivityCompleted",
                        "payload": {"result": {"codec": "json", "blob": '"from-file"'}},
                    }
                ]
            )
        )

        outcome = replay_history_file(
            SimpleGreeter,
            history_path,
            ["world"],
            payload_codec="json",
        )

        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "from-file"

    def test_replay_history_file_accepts_dict_with_events_key(self, tmp_path: Path) -> None:
        history_path = tmp_path / "history.json"
        history_path.write_text(
            json.dumps(
                {
                    "events": [
                        {
                            "event_type": "ActivityCompleted",
                            "payload": {"result": {"codec": "json", "blob": '"wrapped"'}},
                        }
                    ]
                }
            )
        )

        outcome = replay_history_file(
            SimpleGreeter,
            history_path,
            ["world"],
            payload_codec="json",
        )

        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "wrapped"
