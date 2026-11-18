"""One unchanged workflow definition for the native timer-redrive cohort.

The first callback succeeds and the second callback deliberately fails in the
source run and first successor. A later fresh Worker lets it succeed. The outer
acceptance adapter owns those callback outcomes and process boundaries.
"""

from __future__ import annotations

from typing import Any

from durable_workflow import workflow
from durable_workflow.workflow import ActivityRetryPolicy, WorkflowContext


@workflow.defn(name="tests.python-redrive-timer-clock")
class RedriveTimerClockWorkflow:
    def run(self, ctx: WorkflowContext, name: str, seconds: int) -> Any:
        greeting = yield ctx.schedule_activity("tests.redrive.first", [name])
        yield ctx.start_timer(seconds)
        timer_clock = ctx.replay_now().isoformat(timespec="microseconds")
        resumed = yield ctx.schedule_activity(
            "tests.redrive.second",
            [greeting, timer_clock],
            retry_policy=ActivityRetryPolicy(max_attempts=1),
        )
        return {"greeting": greeting, "timer_clock": timer_clock, "resumed": resumed}
