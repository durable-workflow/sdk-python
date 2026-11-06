"""One corpus consumer that records the old API's absence as an observation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from durable_workflow import workflow
from durable_workflow.errors import DurableWorkflowError
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
