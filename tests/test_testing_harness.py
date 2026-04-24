from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.errors import WorkflowFailed
from durable_workflow.testing import (
    WorkflowEnvironment,
    WorkflowRunRecord,
    replay_history,
    replay_history_file,
)
from durable_workflow.workflow import (
    CompleteWorkflow,
    ContinueAsNew,
    WorkflowContext,
)


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


@workflow.defn(name="countdown")
class CountdownContinueAsNewWorkflow:
    def run(self, ctx: WorkflowContext, counter: int):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("emit", [counter])
        if counter > 0:
            return ctx.continue_as_new(counter - 1)
        return {"final_counter": counter}


@workflow.defn(name="continue-signal-aware")
class ContinueSignalAwareWorkflow:
    def __init__(self) -> None:
        self.seen: list[str] = []

    @workflow.signal("note")
    def on_note(self, message: str) -> None:
        self.seen.append(message)

    def run(self, ctx: WorkflowContext, counter: int):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("wait", [counter])
        if counter > 0:
            return ctx.continue_as_new(counter - 1)
        return {"counter": counter, "seen": list(self.seen)}


@workflow.defn(name="continue-after-timer")
class ContinueAfterTimerWorkflow:
    def run(self, ctx: WorkflowContext, counter: int):  # type: ignore[no-untyped-def]
        yield ctx.sleep(60)
        if counter > 0:
            return ctx.continue_as_new(counter - 1)
        return "done"


@workflow.defn(name="continue-with-side-effect-and-search")
class ContinueWithSideEffectsWorkflow:
    def run(self, ctx: WorkflowContext, counter: int):  # type: ignore[no-untyped-def]
        val = yield ctx.side_effect(lambda: f"side-{counter}")
        yield ctx.upsert_search_attributes({"counter": counter})
        if counter > 0:
            return ctx.continue_as_new(counter - 1)
        return val


@workflow.defn(name="continue-first-stage")
class ContinueFirstStageWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("stage_one", [])
        return ctx.continue_as_new(workflow_type="continue-second-stage")


@workflow.defn(name="continue-second-stage")
class ContinueSecondStageWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("stage_two", [])
        return {"stage": "two", "activity": result}


class TestContinueAsNew:
    def test_chain_returns_final_run_result(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("emit", None)

        result = env.execute_workflow(CountdownContinueAsNewWorkflow, 3)

        assert result == {"final_counter": 0}
        assert env.run_count == 4

    def test_run_records_capture_input_and_terminal_per_link(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("emit", None)

        env.execute_workflow(CountdownContinueAsNewWorkflow, 2)

        runs = env.runs
        assert [r.input for r in runs] == [[2], [1], [0]]
        assert all(isinstance(r, WorkflowRunRecord) for r in runs)
        assert all(r.workflow_type == "countdown" for r in runs)
        assert isinstance(runs[0].terminal, ContinueAsNew)
        assert runs[0].terminal.arguments == [1]
        assert isinstance(runs[1].terminal, ContinueAsNew)
        assert isinstance(runs[2].terminal, CompleteWorkflow)
        assert runs[2].terminal.result == {"final_counter": 0}

    def test_non_final_run_history_records_continue_as_new_event(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("emit", None)

        env.execute_workflow(CountdownContinueAsNewWorkflow, 1)

        first_run_events = env.runs[0].history
        event_types = [e["event_type"] for e in first_run_events]
        assert event_types[-1] == "WorkflowContinuedAsNew"
        payload = first_run_events[-1]["payload"]
        assert payload["payload_codec"] == "avro"
        assert "arguments" in payload
        # Non-final run captures the activity event that ran before continue.
        assert "ActivityCompleted" in event_types

    def test_continue_as_new_limit_raises(self) -> None:
        env = WorkflowEnvironment(continue_as_new_limit=3)
        env.register_activity_result("emit", None)

        with pytest.raises(RuntimeError, match="continue-as-new past the 3"):
            env.execute_workflow(CountdownContinueAsNewWorkflow, 100)

    def test_signal_queued_for_specific_run_targets_that_link(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("wait", None)
        env.signal("note", ["hello-first"], run=1)
        env.signal("note", ["hello-last"], run=3)

        result = env.execute_workflow(ContinueSignalAwareWorkflow, 2)

        assert result == {"counter": 0, "seen": ["hello-last"]}
        first_events = env.runs[0].history
        assert any(
            e["event_type"] == "SignalReceived"
            and e["payload"]["signal_name"] == "note"
            for e in first_events
        )
        middle_events = env.runs[1].history
        assert not any(
            e["event_type"] == "SignalReceived" for e in middle_events
        )

    def test_timer_auto_fires_across_continuations(self) -> None:
        env = WorkflowEnvironment()

        result = env.execute_workflow(ContinueAfterTimerWorkflow, 2)

        assert result == "done"
        assert env.run_count == 3
        for record in env.runs[:-1]:
            assert any(
                e["event_type"] == "TimerFired" for e in record.history
            )

    def test_side_effects_and_search_attributes_work_across_continuations(
        self,
    ) -> None:
        env = WorkflowEnvironment()

        result = env.execute_workflow(ContinueWithSideEffectsWorkflow, 2)

        # Final link gets its own side-effect value.
        assert result == "side-0"
        for record in env.runs:
            event_types = [e["event_type"] for e in record.history]
            assert "SideEffectRecorded" in event_types
            assert "SearchAttributesUpserted" in event_types

    def test_continue_can_switch_workflow_type(self) -> None:
        env = WorkflowEnvironment()
        env.register_workflow(ContinueSecondStageWorkflow)
        env.register_activity_result("stage_one", None)
        env.register_activity_result("stage_two", "stage-two-result")

        result = env.execute_workflow(ContinueFirstStageWorkflow)

        assert result == {"stage": "two", "activity": "stage-two-result"}
        assert [r.workflow_type for r in env.runs] == [
            "continue-first-stage",
            "continue-second-stage",
        ]

    def test_continue_to_unregistered_workflow_raises(self) -> None:
        @workflow.defn(name="continue-to-unknown")
        class ContinueToUnknownWorkflow:
            def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
                return ctx.continue_as_new(workflow_type="never-registered-xyz")

        env = WorkflowEnvironment()

        with pytest.raises(KeyError, match="never-registered-xyz"):
            env.execute_workflow(ContinueToUnknownWorkflow)

    def test_re_running_execute_workflow_resets_runs(self) -> None:
        env = WorkflowEnvironment()
        env.register_activity_result("emit", None)

        env.execute_workflow(CountdownContinueAsNewWorkflow, 2)
        assert env.run_count == 3

        env.execute_workflow(CountdownContinueAsNewWorkflow, 0)
        assert env.run_count == 1


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
