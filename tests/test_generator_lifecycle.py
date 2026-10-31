"""Keep suspended replay state observable without finalizer side effects."""
from __future__ import annotations

import asyncio
import gc
import logging
import weakref
from contextlib import contextmanager
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import QueryFailed
from durable_workflow.workflow import (
    CompleteUpdate,
    CompleteWorkflow,
    FailWorkflow,
    RecordSideEffect,
    ScheduleActivity,
    _prepare_query_state,
    _prepare_update_validation,
    _UpdateValidationReplayOptions,
    apply_update,
    query_state,
    replay,
    validate_update,
)
from tests.test_cancellation_context import delivery, request
from tests.test_cooperative_cancellation import completed_activity


def pending(ctx: Any, kind: str) -> Any:
    if kind == "activity":
        return ctx.schedule_activity("work", [])
    if kind == "timer":
        return ctx.start_timer(1)
    if kind == "condition":
        return ctx.wait_condition(lambda: False, key="ready")
    return ctx.start_child_workflow("child", [])


@workflow.defn(name="tests.generator.pending-finally")
class PendingFinally:
    def run(self, ctx: Any, kind: str = "activity") -> Any:
        self.value = {"phase": "waiting", "items": ["waiting"]}
        try:
            yield pending(ctx, kind)
        finally:
            self.value["phase"] = "cleanup"
            self.value["items"].append("cleanup")
            ctx.logger.info("generator-lifecycle-cleanup")

    @workflow.query("state")
    def state(self) -> Any:
        return self.value

    @workflow.query("async_state")
    async def async_state(self) -> Any:
        await asyncio.sleep(0)
        return self.value


@contextmanager
def guard(value: dict[str, Any]) -> Any:
    try:
        yield
    finally:
        value["exits"] += 1


@workflow.defn(name="tests.generator.nested-contexts")
class NestedContexts:
    def run(self, ctx: Any, kind: str) -> Any:
        self.value = {"exits": 0}
        with guard(self.value), guard(self.value), ctx.cancellation_shield():
            yield pending(ctx, kind)

    @workflow.query("state")
    def state(self) -> Any:
        return self.value


@pytest.mark.parametrize("kind", ["activity", "timer", "condition", "child"])
@pytest.mark.parametrize("prepared", [False, True], ids=["public-query", "prepared-query"])
def test_query_sees_suspension_and_detaches_mutable_result(kind: str, prepared: bool) -> None:
    if prepared:
        invoke = _prepare_query_state(PendingFinally, [], [kind], "state")
        gc.collect()
        value = invoke()
        del invoke
    else:
        value = query_state(PendingFinally, [], [kind], "state")
    gc.collect()
    assert value == {"phase": "waiting", "items": ["waiting"]}


@pytest.mark.parametrize("kind", ["activity", "timer", "condition", "child"])
async def test_async_query_keeps_suspension_until_consumed_and_detaches_alias(kind: str) -> None:
    invoke = _prepare_query_state(PendingFinally, [], [kind], "async_state")
    await asyncio.sleep(0)
    value = await invoke()
    del invoke
    gc.collect()
    assert value == {"phase": "waiting", "items": ["waiting"]}


@pytest.mark.parametrize("kind", ["activity", "timer", "condition", "child"])
def test_nested_context_and_cancellation_shield_do_not_mutate_query_state(kind: str) -> None:
    value = query_state(NestedContexts, [], [kind], "state")
    gc.collect()
    assert value == {"exits": 0}


@pytest.mark.parametrize("kind", ["activity", "timer", "condition", "child"])
def test_discarded_pending_replay_does_not_emit_cleanup_logs(kind: str, caplog: Any) -> None:
    caplog.set_level(logging.INFO, logger="durable_workflow.workflow.replay")
    outcome = replay(PendingFinally, [], [kind])
    assert len(outcome.commands) == 1
    del outcome
    gc.collect()
    assert [r for r in caplog.records if r.message == "generator-lifecycle-cleanup"] == []


@workflow.defn(name="tests.generator.pending-receivers")
class PendingReceivers(PendingFinally):
    @workflow.update("touch")
    def touch(self) -> Any:
        self.value["items"].append("updated")
        return self.value

    @workflow.update_validator("touch")
    def check(self) -> Any:
        assert self.value["phase"] == "waiting"
        return self.value

    @workflow.update("async-touch")
    def async_touch(self) -> None:
        pass

    @workflow.update_validator("async-touch")
    async def async_check(self) -> Any:
        await asyncio.sleep(0)
        assert self.value["phase"] == "waiting"
        return self.value

    @workflow.query("broken")
    def broken(self) -> None:
        raise RuntimeError("query-body-failed")


@pytest.mark.parametrize("kind", ["activity", "timer", "condition", "child"])
def test_update_uses_suspended_state_and_detaches_its_result(kind: str) -> None:
    history = [{"event_type": "UpdateAccepted", "payload": {
        "update_id": "update-1", "update_name": "touch",
        "arguments": serializer.encode([], codec="avro"), "payload_codec": "avro",
    }}]
    result = apply_update(PendingReceivers, history, [kind], "update-1")
    gc.collect()
    assert isinstance(result, CompleteUpdate)
    assert result.result == {"phase": "waiting", "items": ["waiting", "updated"]}


@pytest.mark.parametrize("kind", ["activity", "timer", "condition", "child"])
def test_validator_uses_suspended_state_and_detaches_its_result(kind: str) -> None:
    result = validate_update(PendingReceivers, [], [kind], "touch", [])
    gc.collect()
    assert result == {"phase": "waiting", "items": ["waiting"]}


@pytest.mark.parametrize("kind", ["activity", "timer", "condition", "child"])
async def test_async_validator_owns_replay_through_await(kind: str) -> None:
    invoke = _prepare_update_validation(
        PendingReceivers, [], [kind], "async-touch", [], replay_options=_UpdateValidationReplayOptions(),
    )
    await asyncio.sleep(0)
    result = await invoke()
    del invoke
    gc.collect()
    assert result == {"phase": "waiting", "items": ["waiting"]}


@pytest.mark.parametrize("consume", [False, True], ids=["abandoned", "handler-error"])
def test_prepared_query_releases_instance_and_silences_discard(consume: bool, caplog: Any) -> None:
    refs: list[Any] = []

    @workflow.defn(name="tests.generator.owned-query")
    class OwnedQuery(PendingReceivers):
        def __init__(self) -> None:
            refs.append(weakref.ref(self))

    caplog.set_level(logging.INFO, logger="durable_workflow.workflow.replay")
    invoke = _prepare_query_state(OwnedQuery, [], [], "broken" if consume else "state")
    assert refs[0]() is not None
    if consume:
        with pytest.raises(QueryFailed, match="query-body-failed"):
            invoke()
    del invoke
    gc.collect()
    assert refs[0]() is None
    assert [r for r in caplog.records if r.message == "generator-lifecycle-cleanup"] == []


def test_unstarted_async_query_can_be_discarded_without_coroutine_warning(recwarn: Any, caplog: Any) -> None:
    caplog.set_level(logging.INFO, logger="durable_workflow.workflow.replay")
    invoke = _prepare_query_state(PendingFinally, [], [], "async_state")
    result = invoke()
    result.close()
    del result, invoke
    gc.collect()
    assert not [warning for warning in recwarn if "never awaited" in str(warning.message)]
    assert [r for r in caplog.records if r.message == "generator-lifecycle-cleanup"] == []


@workflow.defn(name="tests.generator.actual-cleanup")
class ActualCleanup:
    def run(self, ctx: Any, finish: str) -> Any:
        self.phase = "waiting"
        try:
            yield ctx.start_timer(10)
            if finish == "failure":
                raise ValueError("actual-workflow-failure")
        finally:
            self.phase = "cleanup"
            ctx.logger.info("actual-generator-cleanup")
            with ctx.cancellation_shield():
                yield ctx.schedule_activity("cleanup", [])
        return "done"

    @workflow.query("phase")
    def current_phase(self) -> str:
        return self.phase


@pytest.mark.parametrize("finish", ["success", "failure", "cancellation"])
def test_real_cleanup_survives_pending_discard_and_cold_replay(finish: str, caplog: Any) -> None:
    caplog.set_level(logging.INFO, logger="durable_workflow.workflow.replay")
    history = [request(), delivery()] if finish == "cancellation" else [{"event_type": "TimerFired", "payload": {}}]
    first = replay(ActualCleanup, history, [finish], run_id="run-1")
    assert len(first.commands) == 1
    assert isinstance(first.commands[0], ScheduleActivity)
    assert first.commands[0].activity_type == "cleanup"
    first.close()
    assert len([r for r in caplog.records if r.message == "actual-generator-cleanup"]) == 1
    caplog.clear()

    history.append(completed_activity(2, "cleanup", "cleaned"))
    for _restart in range(2):
        final = replay(ActualCleanup, history, [finish], run_id="run-1")
        if finish == "success":
            assert final.commands == [CompleteWorkflow("done")]
        else:
            assert len(final.commands) == 1
            assert isinstance(final.commands[0], FailWorkflow)
            assert final.commands[0].exception_type == ("ValueError" if finish == "failure" else "WorkflowCancelled")
        final.close()
    assert [r for r in caplog.records if r.message == "actual-generator-cleanup"] == []


@pytest.mark.parametrize("finish", ["success", "failure", "cancellation"])
def test_query_reconstruction_does_not_log_real_cleanup(finish: str, caplog: Any) -> None:
    caplog.set_level(logging.INFO, logger="durable_workflow.workflow.replay")
    history = [request(), delivery()] if finish == "cancellation" else [{"event_type": "TimerFired", "payload": {}}]
    assert query_state(ActualCleanup, history, [finish], "phase", run_id="run-1") == "cleanup"
    assert [r for r in caplog.records if r.message == "actual-generator-cleanup"] == []


@workflow.defn(name="tests.generator.cleanup-side-effect")
class CleanupSideEffect:
    calls = 0

    def cleanup(self) -> str:
        type(self).calls += 1
        return "cleaned"

    def run(self, ctx: Any) -> Any:
        self.phase = "waiting"
        try:
            yield ctx.start_timer(1)
        finally:
            self.phase = "cleanup"
            yield ctx.side_effect(self.cleanup)
        return "done"

    @workflow.query("phase")
    def phase_query(self) -> str:
        return self.phase

    @workflow.update("observe")
    def observe(self) -> str:
        return self.phase

    @observe.validator
    def validate_observe(self) -> None:
        if self.phase != "cleanup":
            raise ValueError("Cleanup has not started.")


def cleanup_side_effect_history() -> list[dict[str, Any]]:
    return [
        {"event_type": "WorkflowStarted", "payload": {"timestamp": "2026-10-31T00:00:00Z"}},
        {"event_type": "TimerScheduled", "payload": {"sequence": 1, "timer_id": "wait", "delay_seconds": 1}},
        {"event_type": "TimerFired", "payload": {"sequence": 1, "timer_id": "wait"}},
    ]


@pytest.mark.parametrize("operation", ["query", "validation", "update"])
def test_read_only_reconstruction_does_not_execute_cleanup_side_effect(operation: str) -> None:
    CleanupSideEffect.calls = 0
    history = cleanup_side_effect_history()
    if operation == "query":
        assert query_state(CleanupSideEffect, history, [], "phase") == "cleanup"
    elif operation == "validation":
        assert validate_update(CleanupSideEffect, history, [], "observe", []) is None
    else:
        history.append({"event_type": "UpdateAccepted", "payload": {
            "update_id": "update-1", "update_name": "observe",
            "arguments": serializer.envelope([], codec="avro"), "payload_codec": "avro",
        }})
        assert apply_update(CleanupSideEffect, history, [], "update-1") == CompleteUpdate("update-1", "cleanup")
    assert CleanupSideEffect.calls == 0


def test_real_cleanup_side_effect_runs_once_and_replays_its_recorded_result() -> None:
    CleanupSideEffect.calls = 0
    history = cleanup_side_effect_history()
    first = replay(CleanupSideEffect, history, [])
    assert len(first.commands) == 2
    assert isinstance(first.commands[0], RecordSideEffect)
    assert first.commands[0].result == "cleaned"
    assert first.commands[1] == CompleteWorkflow("done")
    first.close()
    assert CleanupSideEffect.calls == 1
    history.append({"event_type": "SideEffectRecorded", "payload": {
        "sequence": 2, "result": serializer.envelope("cleaned", codec="avro"), "payload_codec": "avro",
    }})
    for _restart in range(2):
        cold = replay(CleanupSideEffect, history, [])
        assert cold.commands == [CompleteWorkflow("done")]
        cold.close()
        assert query_state(CleanupSideEffect, history, [], "phase") == "cleanup"
    assert CleanupSideEffect.calls == 1


@pytest.mark.parametrize("delegated", [False, True])
def test_yielding_discarded_finally_is_quiet_and_does_not_add_commands(
    delegated: bool, caplog: Any, capfd: Any, recwarn: Any,
) -> None:
    @workflow.defn(name="tests.generator.yielded-finally")
    class YieldingFinally:
        def cleanup(self, ctx: Any) -> Any:
            try:
                yield ctx.schedule_activity("discarded-cleanup", [])
            finally:
                ctx.logger.info("discarded-finally")
                yield ctx.start_timer(2)

        def run(self, ctx: Any) -> Any:
            try:
                yield ctx.schedule_activity("work", [])
            finally:
                if delegated:
                    yield from self.cleanup(ctx)
                else:
                    ctx.logger.info("discarded-finally")
                    yield ctx.schedule_activity("discarded-cleanup", [])

    caplog.set_level(logging.INFO, logger="durable_workflow.workflow.replay")
    result = replay(YieldingFinally, [], [])
    commands = result.commands
    del result
    gc.collect()
    assert len(commands) == 1
    assert isinstance(commands[0], ScheduleActivity)
    assert commands[0].activity_type == "work"
    assert [r for r in caplog.records if r.message == "discarded-finally"] == []
    assert "ignored" not in capfd.readouterr().err
    assert not [warning for warning in recwarn if "unraisable" in str(warning.message).lower()]


@workflow.defn(name="tests.generator.command-alias")
class CommandAlias:
    def run(self, ctx: Any, local: bool = False) -> Any:
        value = {"phase": "waiting"}
        try:
            yield ctx.local_activity("work", [value]) if local else ctx.schedule_activity("work", [value])
        finally:
            value["phase"] = "cleanup"


@pytest.mark.parametrize("local", [False, True], ids=["remote-command", "prepared-local-command"])
def test_authored_command_payload_survives_releasing_replay_owner(local: bool) -> None:
    outcome = replay(CommandAlias, [], [local], prepare_local_activities=local)
    if local:
        assert outcome.prepared_local_activity is not None
        command = outcome.prepared_local_activity.command
    else:
        command = outcome.commands[0]
    del outcome
    gc.collect()
    assert command.arguments == [{"phase": "waiting"}]
