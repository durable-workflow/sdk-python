"""Workflows shared by bounded-failure replay and published-runtime controls."""
from __future__ import annotations

import hashlib
from typing import Any

from durable_workflow import ActivityFailed, NonRetryableError, activity, workflow


@workflow.defn(name="tests.local.failure-observation")
class LocalFailureObservation:
    def run(self, ctx: Any, non_retryable: bool = False) -> Any:
        try:
            yield ctx.local_activity("tests.local.large-failure", [non_retryable], retry_policy={"max_attempts": 1})
        except ActivityFailed as error:
            message = str(error)
            return {
                "bytes": len(message.encode()), "digest": hashlib.sha256(message.encode()).hexdigest(),
                "marked": "[truncated" in message, "type": error.exception_type,
                "non_retryable": error.non_retryable,
            }
        raise AssertionError("local activity unexpectedly succeeded")


class OriginalFailure(Exception):
    pass


@activity.defn(name="tests.local.large-failure")
async def large_local_failure(non_retryable: bool) -> None:
    error_type = NonRetryableError if non_retryable else OriginalFailure
    raise error_type("🌈" * (3 * 1024 * 1024 // 4))

