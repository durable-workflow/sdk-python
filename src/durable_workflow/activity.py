"""Activity decorator, registry, and execution context."""
from __future__ import annotations

import contextvars
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .errors import ActivityCancelled

if TYPE_CHECKING:
    from .client import Client

_REGISTRY: dict[str, Callable[..., Any]] = {}

_current_context: contextvars.ContextVar[ActivityContext | None] = contextvars.ContextVar(
    "activity_context", default=None
)


@dataclass(frozen=True)
class ActivityInfo:
    task_id: str
    activity_type: str
    activity_attempt_id: str
    attempt_number: int
    task_queue: str
    worker_id: str


class ActivityContext:
    def __init__(
        self,
        *,
        info: ActivityInfo,
        client: Client,
    ) -> None:
        self._info = info
        self._client = client
        self._cancel_requested = False

    @property
    def info(self) -> ActivityInfo:
        return self._info

    @property
    def is_cancelled(self) -> bool:
        return self._cancel_requested

    async def heartbeat(self, details: dict[str, Any] | None = None) -> None:
        resp = await self._client.heartbeat_activity_task(
            task_id=self._info.task_id,
            activity_attempt_id=self._info.activity_attempt_id,
            lease_owner=self._info.worker_id,
            details=details,
        )
        if isinstance(resp, dict) and resp.get("cancel_requested"):
            self._cancel_requested = True
            raise ActivityCancelled()


def context() -> ActivityContext:
    ctx = _current_context.get()
    if ctx is None:
        raise RuntimeError("activity.context() called outside of an activity execution")
    return ctx


def _set_context(ctx: ActivityContext | None) -> contextvars.Token[ActivityContext | None]:
    return _current_context.set(ctx)


def defn(*, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
        fn.__activity_name__ = name  # type: ignore[attr-defined]
        _REGISTRY[name] = fn
        return fn

    return wrap


def registry() -> dict[str, Callable[..., Any]]:
    return dict(_REGISTRY)
