"""Bounded, payload-free diagnostics for the ordinary task-family integration test."""

from __future__ import annotations

import asyncio
import time
from collections import Counter, deque
from collections.abc import Callable, Coroutine
from copy import deepcopy
from typing import Any

import httpx

from durable_workflow.interceptors import (
    ActivityHandler,
    ActivityInterceptorContext,
    PassthroughWorkerInterceptor,
    WorkflowTaskHandler,
    WorkflowTaskInterceptorContext,
)


def route_name(request: httpx.Request) -> str:
    path = request.url.path.rstrip("/")
    for family in ("workflow", "activity", "query"):
        if path.endswith(f"/worker/{family}-tasks/poll"):
            return f"{family}_poll"
        if f"/worker/{family}-tasks/" in path:
            return f"{family}_submission"
    if path.endswith("/worker/register") or "/worker/registrations" in path:
        return "registration"
    if path.endswith("/cluster/info"):
        return "discovery"
    return "control"


def task_identity(task: Any) -> dict[str, str | int]:
    if not isinstance(task, dict):
        return {}
    fields = (
        "task_id", "workflow_id", "run_id", "workflow_run_id", "activity_id",
        "workflow_task_attempt", "activity_task_attempt", "attempt", "lease_owner",
        "namespace", "task_queue", "workflow_type",
        "lease_expires_at", "task_deadline_at",
    )
    return {
        key: value for key in fields
        if (value := task.get(key)) is not None
        and (type(value) is int or isinstance(value, str) and len(value) <= 160)
    }


class WorkerObservation:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic, limit: int = 32) -> None:
        if limit < 1:
            raise ValueError("observation limit must be positive")
        self.clock = clock
        self.started = clock()
        self.limit = limit
        self.recent: deque[dict[str, Any]] = deque(maxlen=limit)
        self.active: dict[int, dict[str, Any]] = {}
        self.active_count = 0
        self.total = 0
        self.counts: Counter[str] = Counter()
        self.successful: Counter[str] = Counter()
        self.max_elapsed: dict[str, float] = {}
        self.milestones: dict[str, dict[str, Any]] = {}
        self.clients: dict[int, str] = {}

    def note(self, phase: str, task: Any = None) -> None:
        if phase not in self.milestones and len(self.milestones) >= self.limit:
            return
        entry = {"at": self.clock() - self.started, **task_identity(task)}
        milestone = self.milestones.setdefault(phase, {"count": 0, "first": entry})
        milestone["count"] += 1
        milestone["last"] = entry

    def install_http(self, monkeypatch: Any) -> None:
        original_send = httpx.AsyncClient.send

        async def observed_send(
            client: httpx.AsyncClient, request: httpx.Request, *args: Any, **kwargs: Any,
        ) -> httpx.Response:
            self.total += 1
            request_id = self.total
            started = self.clock()
            route = route_name(request)
            method = request.method if request.method in {"GET", "POST", "PUT", "PATCH", "DELETE"} else "OTHER"
            self.counts[f"{method} {route}"] += 1
            entry: dict[str, Any] = {
                "request": request_id, "client": self.clients.get(id(client), "additional"),
                "method": method, "route": route, "started": started - self.started,
            }
            if len(self.active) == self.limit:
                self.active.pop(next(iter(self.active)))
            self.active[request_id] = entry
            self.active_count += 1
            try:
                response = await original_send(client, request, *args, **kwargs)
                entry["status"] = response.status_code
                if 200 <= response.status_code < 300:
                    self.successful[f"{method} {route}"] += 1
                if response.is_stream_consumed:
                    self._response_metadata(entry, response)
                return response
            except BaseException as error:
                entry["exception"] = type(error).__name__
                raise
            finally:
                entry["elapsed"] = self.clock() - started
                key = f"{method} {route}"
                self.max_elapsed[key] = max(self.max_elapsed.get(key, 0), entry["elapsed"])
                self.active_count -= 1
                self.active.pop(request_id, None)
                self.recent.append(entry)

        monkeypatch.setattr(httpx.AsyncClient, "send", observed_send)

    @staticmethod
    def _response_metadata(entry: dict[str, Any], response: httpx.Response) -> None:
        if not response.headers.get("content-type", "").startswith("application/json"):
            return
        try:
            if len(response.content) > 32_768:
                return
            body = response.json()
        except (ValueError, httpx.ResponseNotRead):
            return
        if not isinstance(body, dict):
            return
        if "task" in body:
            entry["task_state"] = (
                "present" if isinstance(body["task"], dict) else "null" if body["task"] is None else "invalid"
            )
            entry.update(task_identity(body["task"]))
        if type(body.get("recorded")) is bool:
            entry["recorded"] = body["recorded"]
        error = body.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            if isinstance(code, str) and len(code) <= 80 and code.replace("_", "").isalnum():
                entry["error_code"] = code
        for field in ("reason", "status", "poll_status", "outcome"):
            value = body.get(field)
            if isinstance(value, str) and len(value) <= 80 and value.replace("_", "").isalnum():
                entry[field] = value

    def report(self, *, include_recent: bool = True) -> dict[str, Any]:
        return deepcopy({
            "elapsed": self.clock() - self.started, "requests": self.total,
            "counts": dict(self.counts), "milestones": self.milestones,
            "successful": dict(self.successful),
            "max_elapsed": self.max_elapsed,
            "active_count": self.active_count, "active": list(self.active.values()),
            "omitted_active": self.active_count - len(self.active),
            "recent": list(self.recent) if include_recent else [],
        })


async def result_or_worker_error(result: Coroutine[Any, Any, Any], runners: list[asyncio.Task[Any]]) -> Any:
    """Surface a fatal worker error while the original bounded result wait is pending."""
    result_task = asyncio.create_task(result)
    pending = {*runners, result_task}
    try:
        while True:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for runner in done - {result_task}:
                if runner.cancelled():
                    raise asyncio.CancelledError
                error = runner.exception()
                if error is not None:
                    raise error
            if result_task in done:
                return await result_task
    finally:
        if not result_task.done():
            result_task.cancel()
        await asyncio.gather(result_task, return_exceptions=True)


async def stop_and_drain(workers: list[Any], runners: list[asyncio.Task[Any]]) -> None:
    outcomes = await asyncio.gather(*(worker.stop() for worker in workers), return_exceptions=True)
    try:
        outcomes += await asyncio.wait_for(asyncio.gather(*runners, return_exceptions=True), timeout=10)
    except Exception as error:
        outcomes.append(error)
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            raise outcome


class ObservationInterceptor(PassthroughWorkerInterceptor):
    def __init__(self, observation: WorkerObservation) -> None:
        self.observation = observation

    async def execute_workflow_task(
        self, context: WorkflowTaskInterceptorContext, next: WorkflowTaskHandler,
    ) -> list[dict[str, Any]] | None:
        self.observation.note("workflow_task_enter", context.task)
        try:
            result = await next(context)
            self.observation.note("workflow_task_return", context.task)
            return result
        except BaseException:
            self.observation.note("workflow_task_error", context.task)
            raise

    async def execute_activity(self, context: ActivityInterceptorContext, next: ActivityHandler) -> Any:
        self.observation.note("activity_handler_enter", context.task)
        try:
            result = await next(context)
            self.observation.note("activity_handler_return", context.task)
            return result
        except BaseException:
            self.observation.note("activity_handler_error", context.task)
            raise
