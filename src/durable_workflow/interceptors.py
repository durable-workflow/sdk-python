"""Worker interceptor protocols for SDK instrumentation hooks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class WorkflowTaskInterceptorContext:
    """Context passed to workflow task interceptor hooks."""

    worker_id: str
    task_queue: str
    task: dict[str, Any]


@dataclass(frozen=True)
class ActivityInterceptorContext:
    """Context passed to activity execution interceptor hooks."""

    worker_id: str
    task_queue: str
    task: dict[str, Any]
    activity_type: str
    args: tuple[Any, ...]


@dataclass(frozen=True)
class QueryTaskInterceptorContext:
    """Context passed to query task interceptor hooks."""

    worker_id: str
    task_queue: str
    task: dict[str, Any]


WorkflowTaskHandler = Callable[[WorkflowTaskInterceptorContext], Awaitable[list[dict[str, Any]] | None]]
ActivityHandler = Callable[[ActivityInterceptorContext], Awaitable[Any]]
QueryTaskHandler = Callable[[QueryTaskInterceptorContext], Awaitable[str]]


class WorkerInterceptor(Protocol):
    """Protocol for wrapping worker task execution.

    Interceptors run in the order passed to ``Worker(..., interceptors=[...])``.
    The first interceptor is the outermost wrapper and should call ``next`` to
    continue the chain.
    """

    async def execute_workflow_task(
        self,
        context: WorkflowTaskInterceptorContext,
        next: WorkflowTaskHandler,
    ) -> list[dict[str, Any]] | None:
        """Wrap workflow task execution."""
        ...

    async def execute_activity(
        self,
        context: ActivityInterceptorContext,
        next: ActivityHandler,
    ) -> Any:
        """Wrap the registered activity callable."""
        ...

    async def execute_query_task(
        self,
        context: QueryTaskInterceptorContext,
        next: QueryTaskHandler,
    ) -> str:
        """Wrap query task execution."""
        ...


class PassthroughWorkerInterceptor:
    """No-op interceptor useful as a base for partial custom interceptors."""

    async def execute_workflow_task(
        self,
        context: WorkflowTaskInterceptorContext,
        next: WorkflowTaskHandler,
    ) -> list[dict[str, Any]] | None:
        return await next(context)

    async def execute_activity(
        self,
        context: ActivityInterceptorContext,
        next: ActivityHandler,
    ) -> Any:
        return await next(context)

    async def execute_query_task(
        self,
        context: QueryTaskInterceptorContext,
        next: QueryTaskHandler,
    ) -> str:
        return await next(context)
