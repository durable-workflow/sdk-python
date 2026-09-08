"""HTTP transport retry policy used inside :class:`~durable_workflow.Client`.

.. warning::

   :class:`TransportRetryPolicy` covers **only client-side HTTP retries** for
   transient transport errors (connection failures, timeouts, 5xx responses,
   429 rate-limiting). It is **not** the activity retry policy. Activity-level
   retry and timeout configuration lives on
   :class:`durable_workflow.workflow.ActivityRetryPolicy` and is passed to
   ``ctx.schedule_activity(..., retry_policy=...)``.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

import httpx

from .errors import ServerError

T = TypeVar("T")
log = logging.getLogger("durable_workflow.worker")

# Task-local so sharing a Client never changes unrelated client/control requests.
_worker_storage_admission_stop: contextvars.ContextVar[Callable[[], bool] | None] = contextvars.ContextVar(
    "worker_storage_admission_stop", default=None,
)


def _storage_refusal(exc: Exception) -> tuple[ServerError, str | None] | None:
    if not isinstance(exc, httpx.HTTPStatusError):
        return None
    if "X-Durable-Workflow-Protocol-Version" not in exc.request.headers:
        return None
    try:
        body = exc.response.json()
    except ValueError:
        return None
    # A payload upload is content-addressed and precedes completion submission.
    # Even a late pressure refusal can retry those same bytes. This local retry
    # classification does not alter the original response exposed to callers.
    if (isinstance(body, dict) and "request_admitted" not in body and exc.request.method == "POST"
            and exc.request.url.path.endswith("/api/external-payloads/v1")):
        body = {**body, "request_admitted": False}
    error = ServerError(exc.response.status_code, body)
    if error.reason() not in ("storage_pressure", "storage_admission_unavailable"):
        return None
    poll_id = None
    if exc.request.url.path.endswith("/poll"):
        try:
            request = json.loads(exc.request.content)
            poll_id = request.get("poll_request_id") if isinstance(request, dict) else None
        except ValueError:
            pass
        # An invalid submitted ID must not fall through to the non-poll contract.
        if not isinstance(poll_id, str) or not poll_id:
            poll_id = ""
    return error, poll_id


@dataclass
class TransportRetryPolicy:
    """
    Retry policy for transient HTTP transport errors.

    Retries requests that fail with transient errors (connection errors,
    timeouts, 5xx server errors, 429 rate limit). Does not retry client
    errors (4xx except 429).

    This policy runs inside :class:`~durable_workflow.Client` around HTTP
    requests. It does not retry workflow runs, workflow tasks, activity
    executions, child workflows, or any user code. Configure durable activity
    retries with :class:`durable_workflow.workflow.ActivityRetryPolicy` and
    child workflow retries with
    :class:`durable_workflow.workflow.ChildWorkflowRetryPolicy`.

    Uses exponential backoff with jitter to avoid thundering herd.
    """

    max_attempts: int = 3
    initial_backoff_seconds: float = 0.1
    max_backoff_seconds: float = 5.0
    backoff_multiplier: float = 2.0
    jitter: bool = True

    def should_retry(self, exc: Exception, attempt: int) -> bool:
        """Check if the error is retryable and we haven't exceeded max attempts."""
        if attempt >= self.max_attempts:
            return False

        # Retry connection errors and timeouts
        if isinstance(exc, httpx.ConnectError | httpx.TimeoutException | httpx.NetworkError):
            return True

        # Retry 5xx server errors and 429 rate limit
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code >= 500 or exc.response.status_code == 429

        return False

    def backoff_seconds(self, attempt: int) -> float:
        """Calculate backoff duration for the given attempt number (0-indexed)."""
        backoff = min(
            self.initial_backoff_seconds * (self.backoff_multiplier**attempt),
            self.max_backoff_seconds,
        )
        if self.jitter:
            # Add ±25% jitter
            backoff *= random.uniform(0.75, 1.25)
        return backoff

    async def execute(self, fn: Callable[[], Awaitable[T]]) -> T:
        """
        Execute the given async function with retries.

        Raises the last exception if all retries are exhausted.
        """
        attempt = 0
        storage_attempt = 0
        last_exc: Exception | None = None

        while attempt < self.max_attempts:
            try:
                result = await fn()
                return result
            except Exception as exc:
                last_exc = exc
                stop = _worker_storage_admission_stop.get()
                refusal = _storage_refusal(exc) if stop is not None else None
                if refusal is not None and stop is not None:
                    error, poll_id = refusal
                    if not error.is_storage_admission_failure(poll_id) or stop():
                        raise
                    storage_attempt += 1
                    assert isinstance(error.body, dict)
                    delay = min(
                        5.0,
                        max(
                            self.backoff_seconds(min(storage_attempt - 1, 6)),
                            error.body["retry_after_seconds"],
                        ),
                    )
                    log.warning("storage admission paused; retrying the same worker request in %.2fs", delay)
                    # Do not consume the finite transport budget or repeat serialization/uploads.
                    while delay > 0:
                        if stop():
                            raise
                        interval = min(0.1, delay)
                        await asyncio.sleep(interval)
                        delay -= interval
                    if stop():
                        raise
                    continue
                if not self.should_retry(exc, attempt):
                    raise

                if attempt + 1 < self.max_attempts:
                    backoff = self.backoff_seconds(attempt)
                    await asyncio.sleep(backoff)

                attempt += 1

        # All retries exhausted
        if last_exc:
            raise last_exc
        raise RuntimeError("retry loop exhausted with no exception")


# Backward-compatible alias for earlier 0.x releases. Prefer
# TransportRetryPolicy in new code so it is not confused with workflow-level
# activity retry policy.
RetryPolicy = TransportRetryPolicy
