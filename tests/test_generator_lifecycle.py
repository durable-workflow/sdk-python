"""Keep suspended replay state observable without finalizer side effects."""
from __future__ import annotations

import asyncio
import gc
import logging
from contextlib import contextmanager
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.workflow import _prepare_query_state, query_state, replay


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
