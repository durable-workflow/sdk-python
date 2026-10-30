"""Activity decorator, registry, and execution context."""
from __future__ import annotations

import asyncio
import contextvars
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import InitVar, dataclass, field
from typing import TYPE_CHECKING, Any

from ._activity_lease import current_activity_authority
from .errors import ActivityCancelled, ActivityLeaseAuthorityLost

if TYPE_CHECKING:
    from .client import Client

_REGISTRY: dict[str, Callable[..., Any]] = {}

_current_context: contextvars.ContextVar[ActivityContext | None] = contextvars.ContextVar(
    "activity_context", default=None
)


@dataclass(frozen=True, init=False)
class ActivityRetryInfo:
    """Original attempt and immutable received retry snapshot, without defaults."""

    attempt_number: int
    policy_provided: bool
    # Immutable JSON also survives the supervised callback's spawn serialization.
    _policy_json: str = field(repr=False)

    def __init__(self, attempt_number: int, policy_provided: bool = False, policy: Any = None) -> None:
        object.__setattr__(self, "attempt_number", attempt_number)
        object.__setattr__(self, "policy_provided", policy_provided)
        object.__setattr__(self, "_policy_json", json.dumps(policy))

    @classmethod
    def from_task(cls, attempt_number: int, task: Mapping[str, Any]) -> ActivityRetryInfo:
        return cls(attempt_number, "retry_policy" in task, task.get("retry_policy"))

    def policy(self) -> Any:
        """Return a fresh copy, including unknown fields and explicit legacy null."""
        return json.loads(self._policy_json)

    def max_attempts(self) -> int | None:
        """A positive configured limit. An unknown limit does not mean unlimited."""
        policy = self.policy()
        maximum = policy.get("max_attempts") if isinstance(policy, dict) else None
        return maximum if type(maximum) is int and maximum > 0 else None

    def is_unlimited(self) -> bool:
        """Only an explicit max_attempts:null inside a policy means unlimited."""
        policy = self.policy()
        return isinstance(policy, dict) and "max_attempts" in policy and policy["max_attempts"] is None

    def is_last_configured_attempt(self) -> bool | None:
        """Check the budget, not whether another execution will actually occur.

        Cancellation, non-retryable failure, workflow closure and deadlines can
        end execution before the configured budget is exhausted.
        """
        if self.attempt_number < 1:
            return None
        maximum = self.max_attempts()
        if maximum is not None:
            return self.attempt_number >= maximum
        return False if self.is_unlimited() else None


@dataclass(frozen=True)
class ActivityInfo:
    """Metadata attached to the currently running activity attempt."""

    task_id: str
    activity_type: str
    activity_attempt_id: str
    attempt_number: int
    task_queue: str
    worker_id: str
    task_metadata: InitVar[Mapping[str, Any] | None] = None
    retry_info: ActivityRetryInfo = field(init=False)
    workflow_id: str | None = field(init=False)
    run_id: str | None = field(init=False)

    def __post_init__(self, task_metadata: Mapping[str, Any] | None) -> None:
        task = task_metadata if task_metadata is not None else {}
        object.__setattr__(self, "retry_info", ActivityRetryInfo.from_task(self.attempt_number, task))
        for name in ("workflow_id", "run_id"):
            value = task.get(name)
            object.__setattr__(self, name, value if isinstance(value, str) and value else None)


class ActivityContext:
    """Per-attempt activity context exposed by :func:`durable_workflow.activity.context`."""

    def __init__(
        self,
        *,
        info: ActivityInfo,
        client: Client,
        heartbeat_callback: Callable[[dict[str, Any] | None], Awaitable[None]] | None = None,
    ) -> None:
        self._info = info
        self._client = client
        self._heartbeat_callback = heartbeat_callback
        self._cancel_requested = False
        self._cannot_continue_reason: str | None = None
        try:
            self._event_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            self._event_loop = None

    @property
    def info(self) -> ActivityInfo:
        """Metadata for the currently running activity attempt."""
        return self._info

    @property
    def is_cancelled(self) -> bool:
        """``True`` once the server has signalled that the owning workflow cancelled this activity."""
        return self._cancel_requested

    def heartbeat_sync(self, details: dict[str, Any] | None = None) -> None:
        """Heartbeat from a synchronous activity thread and check attempt authority.

        The request runs on the worker's event loop using the original attempt
        and client. Async handlers must use ``await heartbeat()`` instead.
        """
        loop = self._event_loop
        if loop is None or not loop.is_running():
            raise RuntimeError("heartbeat_sync requires a running worker event loop")
        try:
            caller_loop = asyncio.get_running_loop()
        except RuntimeError:
            caller_loop = None
        if caller_loop is loop:
            raise RuntimeError("use await activity.context().heartbeat() on the worker event loop")
        heartbeat = self.heartbeat(details)
        try:
            pending = asyncio.run_coroutine_threadsafe(heartbeat, loop)
        except RuntimeError:
            heartbeat.close()
            raise
        pending.result()

    async def heartbeat(self, details: dict[str, Any] | None = None) -> None:
        """Report progress and check cancellation and continued attempt authority.

        Long-running activities should call ``heartbeat()`` periodically so the
        server can distinguish a slow-but-alive attempt from a dead worker.
        Optional ``details`` are attached to the heartbeat and surface as the
        activity's last-known progress on failure.

        Raises :class:`~durable_workflow.errors.ActivityCancelled` when the
        owning workflow has requested cancellation, so the activity can exit
        cleanly at its next natural break point.

        Raises :class:`~durable_workflow.errors.ActivityLeaseAuthorityLost`
        with the original Server reason when the attempt cannot continue,
        including an elapsed deadline, closed attempt or closed run.
        """
        self._raise_if_cannot_continue()
        if self._heartbeat_callback is not None:
            try:
                await self._heartbeat_callback(details)
            except ActivityCancelled:
                self._cancel_requested = True
                raise
            return
        resp = await self._client.heartbeat_activity_task(
            task_id=self._info.task_id,
            activity_attempt_id=self._info.activity_attempt_id,
            lease_owner=self._info.worker_id,
            details=details,
        )
        if isinstance(resp, dict) and resp.get("cancel_requested"):
            self._cancel_requested = True
            raise ActivityCancelled()
        if isinstance(resp, dict) and resp.get("can_continue") is False:
            reason = resp.get("reason")
            self._cannot_continue_reason = reason if isinstance(reason, str) and reason else "activity_cannot_continue"
            self._raise_if_cannot_continue()

    def _raise_if_cannot_continue(self) -> None:
        if self._cannot_continue_reason is not None:
            authority = current_activity_authority.get()
            if (authority is not None and authority.client is self._client
                    and authority.task_id == self._info.task_id
                    and authority.identity["activity_attempt_id"] == self._info.activity_attempt_id
                    and authority.identity["attempt_number"] == self._info.attempt_number
                    and authority.identity["lease_owner"] == self._info.worker_id):
                authority.lost_reason = authority.lost_reason or self._cannot_continue_reason
            raise ActivityLeaseAuthorityLost(self._info.task_id, self._cannot_continue_reason)


def context() -> ActivityContext:
    """Return the current activity attempt context."""
    ctx = _current_context.get()
    if ctx is None:
        raise RuntimeError("activity.context() called outside of an activity execution")
    return ctx


def _set_context(ctx: ActivityContext | None) -> contextvars.Token[ActivityContext | None]:
    return _current_context.set(ctx)


def defn(*, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Register a callable as an activity type under a language-neutral name."""

    def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
        fn.__activity_name__ = name  # type: ignore[attr-defined]
        _REGISTRY[name] = fn
        return fn

    return wrap


def registry() -> dict[str, Callable[..., Any]]:
    """Return a copy of activity callables registered in this process."""
    return dict(_REGISTRY)
