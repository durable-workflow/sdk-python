"""One corpus consumer that records the old API's absence as an observation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from durable_workflow import workflow
from durable_workflow.errors import ActivityFailed, ChildWorkflowFailed, DurableWorkflowError
from durable_workflow.workflow import WorkflowContext


def clock_text(ctx: WorkflowContext) -> str | None:
    accessor = getattr(ctx, "replay_now", None)
    if accessor is None:
        # Allows the identical corpus consumer to measure the pre-accessor base.
        return None
    try:
        return str(accessor().isoformat(timespec="microseconds"))
    except DurableWorkflowError as error:
        if type(error).__name__ != "ReplayClockUnavailable":
            raise
        return None


@workflow.defn(name="tests.replay.business-clock")
class ScalarBusinessClockWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        self.ctx = ctx
        observations = [clock_text(ctx)]
        legacy = [ctx.now().isoformat(timespec="microseconds")]
        yield ctx.schedule_activity("clock.first", [])
        observations.append(clock_text(ctx))
        legacy.append(ctx.now().isoformat(timespec="microseconds"))
        yield ctx.start_timer(2)
        observations.append(clock_text(ctx))
        legacy.append(ctx.now().isoformat(timespec="microseconds"))
        elapsed = None
        if observations[1] is not None and observations[2] is not None:
            before = datetime.fromisoformat(observations[1])
            elapsed = (datetime.fromisoformat(observations[2]) - before).total_seconds()
        return {"clock": observations, "legacy": legacy, "elapsed_seconds": elapsed}

    @workflow.query("clock")
    def clock(self) -> str | None:
        return clock_text(self.ctx)

    @workflow.query("async_clock")
    async def async_clock(self) -> Any:
        import asyncio

        before = clock_text(self.ctx)

        async def child_read() -> str | None:
            return clock_text(self.ctx)

        child = await asyncio.create_task(child_read())
        thread = await asyncio.to_thread(clock_text, self.ctx)
        await asyncio.sleep(0)
        return before, child, thread, clock_text(self.ctx)


@workflow.defn(name="tests.replay.business-clock-native-redrive")
class NativeRedriveBusinessClockWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        before = clock_text(ctx)
        yield ctx.schedule_activity("clock.first", [])
        return {"clock": [before, clock_text(ctx)], "legacy": ctx.now().isoformat(timespec="microseconds")}


@workflow.defn(name="tests.replay.business-clock-core-prefix-timer")
class PrefixedCoreTimerBusinessClockWorkflow:
    def run(self, ctx: WorkflowContext, name: str, seconds: int):  # type: ignore[no-untyped-def]
        observations = [clock_text(ctx)]
        yield ctx.schedule_activity("tests.redrive.first", [name])
        observations.append(clock_text(ctx))
        yield ctx.start_timer(seconds)
        observations.append(clock_text(ctx))
        return {"clock": observations, "legacy": ctx.now().isoformat(timespec="microseconds")}


@workflow.defn(name="tests.replay.business-clock-child")
class ChildBusinessClockWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        self.ctx = ctx
        observations = [clock_text(ctx)]
        legacy = [ctx.now().isoformat(timespec="microseconds")]
        value = yield ctx.start_child_workflow("clock.child", [])
        observations.append(clock_text(ctx))
        repeated = clock_text(ctx) == observations[-1]
        legacy.append(ctx.now().isoformat(timespec="microseconds"))
        failure = None
        try:
            yield ctx.start_child_workflow("clock.child", [])
        except ChildWorkflowFailed as error:
            failure = {"kind": error.failure_kind, "run": error.child_workflow_run_id, "message": str(error)}
        observations.append(clock_text(ctx))
        legacy.append(ctx.now().isoformat(timespec="microseconds"))
        return {"clock": observations, "legacy": legacy, "repeated": repeated, "result": value, "failure": failure}

    @workflow.query("clock")
    def clock(self) -> str | None:
        return clock_text(self.ctx)

    @workflow.query("explode")
    def explode(self) -> None:
        raise ValueError("child-clock-query-error")


@workflow.defn(name="tests.replay.business-clock-parallel-activity")
class ParallelActivityBusinessClockWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        self.ctx = ctx
        observed = [clock_text(ctx)]
        legacy = [ctx.now().isoformat(timespec="microseconds")]
        values = yield [ctx.schedule_activity("group.0", []), ctx.schedule_activity("group.1", [])]
        observed.append(clock_text(ctx))
        legacy.append(ctx.now().isoformat(timespec="microseconds"))
        failure = None
        try:
            yield [ctx.schedule_activity("group.0", []), ctx.schedule_activity("group.1", [])]
        except ActivityFailed as error:
            failure = {"activity_type": error.activity_type, "message": str(error)}
        observed.append(clock_text(ctx))
        legacy.append(ctx.now().isoformat(timespec="microseconds"))
        return {"values": values, "clock": observed, "legacy": legacy, "failure": failure}

    @workflow.query("clock")
    def clock(self) -> str | None:
        return clock_text(self.ctx)


@workflow.defn(name="tests.replay.business-clock-parallel-failure-gap")
class ParallelFailureBusinessClockWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [ctx.schedule_activity("group.0", []), ctx.schedule_activity("group.1", [])]
        except ActivityFailed as error:
            return {"activity_type": error.activity_type, "message": str(error), "clock": clock_text(ctx),
                    "legacy": ctx.now().isoformat(timespec="microseconds")}
        raise AssertionError("expected the recorded group failure")
