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


def _backend_unavailable_refusal(exc: Exception) -> tuple[bool, int | None]:
    if not isinstance(exc, httpx.HTTPStatusError) or exc.response.status_code != 503:
        return False, None
    request = exc.request
    if request.method != "POST" or "X-Durable-Workflow-Protocol-Version" not in request.headers:
        return False, None
    operations = {
        "/api/worker/workflow-tasks/poll": "poll_workflow_task",
        "/api/worker/activity-tasks/poll": "poll_activity_task",
        "/api/worker/query-tasks/poll": "poll_query_task",
        "/api/worker/update-validation-tasks/poll": "poll_update_validation_task",
        "/api/worker/register": "register_worker",
        "/api/worker/heartbeat": "heartbeat_worker",
    }
    operation = next((name for path, name in operations.items() if request.url.path.endswith(path)), None)
    _, task_heartbeat_marker, task_heartbeat_tail = request.url.path.rpartition(
        "/api/worker/workflow-tasks/"
    )
    task_id = task_heartbeat_tail.removesuffix("/heartbeat")
    task_heartbeat = (
        bool(task_heartbeat_marker)
        and task_heartbeat_tail.endswith("/heartbeat")
        and bool(task_id)
        and "/" not in task_id
    )
    if task_heartbeat:
        operation = "heartbeat_workflow_task"
    if operation is None:
        return False, None
    try:
        body = exc.response.json()
    except ValueError:
        return False, None
    if not isinstance(body, dict) or body.get("reason") != "backend_unavailable":
        return False, None
    try:
        submitted = json.loads(request.content)
    except ValueError:
        return True, None
    if not isinstance(submitted, dict):
        return True, None
    worker_id = submitted.get("worker_id")
    queue = submitted.get("task_queue")
    delay = body.get("retry_after_seconds")
    if task_heartbeat:
        lease_owner = submitted.get("lease_owner")
        attempt = submitted.get("workflow_task_attempt")
        if (
            not isinstance(lease_owner, str) or not lease_owner
            or type(attempt) is not int or attempt <= 0
            or body.get("operation") != operation
            or body.get("outcome") != "unknown"
            or body.get("worker_id") != lease_owner
            or body.get("task_queue") is not None
            or body.get("task_id") != task_id
            or body.get("lease_owner") != lease_owner
            or body.get("workflow_task_attempt") != attempt
            or body.get("retryable") is not True
            or type(delay) is not int or delay <= 0
        ):
            return True, None
        return True, delay
    if (
        not isinstance(worker_id, str) or not worker_id
        or body.get("operation") != operation
        or body.get("outcome") != "unknown"
        or body.get("worker_id") != worker_id
        or body.get("task_queue") != queue
        or body.get("retryable") is not True
        or type(delay) is not int or delay <= 0
        or (operation != "heartbeat_worker" and (not isinstance(queue, str) or not queue))
    ):
        return True, None
    if operation.startswith("poll_"):
        poll_id = submitted.get("poll_request_id")
        if (
            not isinstance(poll_id, str) or not poll_id
            or "task" not in body or body["task"] is not None
            or body.get("poll_status") != "backend_unavailable"
            or body.get("poll_request_id") != poll_id
            or body.get("retry_same_poll_request_id") is not True
        ):
            return True, None
    return True, delay


def _poll_capacity_refusal(exc: Exception) -> bool:
    if not isinstance(exc, httpx.HTTPStatusError) or exc.response.status_code != 429:
        return False
    request = exc.request
    if request.method != "POST" or "X-Durable-Workflow-Protocol-Version" not in request.headers:
        return False
    task_kinds = {
        "/api/worker/workflow-tasks/poll": "workflow_task",
        "/api/worker/activity-tasks/poll": "activity_task",
        "/api/worker/query-tasks/poll": "query_task",
    }
    task_kind = next((kind for path, kind in task_kinds.items() if request.url.path.endswith(path)), None)
    if task_kind is None:
        return False
    try:
        body = exc.response.json()
        submitted = json.loads(request.content)
    except ValueError:
        return False
    if not isinstance(submitted, dict) or not isinstance(submitted.get("task_queue"), str):
        return False
    return ServerError(429, body).poll_capacity_backpressure_delay(
        task_kind, submitted["task_queue"],
    ) is not None


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
            if _poll_capacity_refusal(exc):
                return False
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
        worker_pause_attempt = 0
        last_exc: Exception | None = None

        while attempt < self.max_attempts:
            try:
                result = await fn()
                return result
            except Exception as exc:
                last_exc = exc
                stop = _worker_storage_admission_stop.get()
                refusal = _storage_refusal(exc) if stop is not None else None
                backend_refusal, backend_delay = _backend_unavailable_refusal(exc)
                if backend_refusal and backend_delay is None:
                    raise
                pause: tuple[str, int] | None = None
                if refusal is not None:
                    error, poll_id = refusal
                    if not error.is_storage_admission_failure(poll_id):
                        raise
                    assert isinstance(error.body, dict)
                    pause = ("storage admission paused", error.body["retry_after_seconds"])
                elif backend_delay is not None:
                    pause = ("worker backend unavailable", backend_delay)
                if pause is not None and stop is not None:
                    if stop():
                        raise
                    worker_pause_attempt += 1
                    delay = min(
                        5.0,
                        max(
                            self.backoff_seconds(min(worker_pause_attempt - 1, 6)),
                            pause[1],
                        ),
                    )
                    log.warning("%s; retrying the same worker request in %.2fs", pause[0], delay)
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
