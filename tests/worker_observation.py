"""Bounded, payload-free diagnostics for the ordinary task-family integration test."""

from __future__ import annotations

import asyncio
import json
import threading
import time
import weakref
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
    if "/worker/workflow-tasks/" in path:
        task_path = path.rsplit("/worker/workflow-tasks/", 1)[1]
        task_id, _, operation = task_path.partition("/")
        if task_id and operation == "history":
            return "workflow_history"
    for family in ("workflow", "activity", "query"):
        if path.endswith(f"/worker/{family}-tasks/poll"):
            return f"{family}_poll"
        if f"/worker/{family}-tasks/" in path:
            return f"{family}_submission"
    if "/worker/registrations/" in path:
        if request.method == "DELETE" or request.method == "POST" and path.endswith("/deregister"):
            return "deregistration"
        return "registration"
    if path.endswith("/worker/register"):
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
        self.omitted_completed = 0
        self.evicted_active = 0
        self.omitted_milestone_events = 0
        self.response_metadata_states: Counter[str] = Counter()
        self._lock = threading.Lock()
        self._client_instances: dict[int, tuple[weakref.ReferenceType[httpx.AsyncClient], str]] = {}
        self.client_instances: dict[str, dict[str, Any]] = {}
        self.unattributed_client_requests = 0

    def note(self, phase: str, task: Any = None) -> None:
        with self._lock:
            if phase not in self.milestones and len(self.milestones) >= self.limit:
                self.omitted_milestone_events += 1
                return
            entry = {"at": self.clock() - self.started, **task_identity(task)}
            milestone = self.milestones.setdefault(phase, {"count": 0, "first": entry})
            if milestone["count"] >= 2:
                self.omitted_milestone_events += 1
            milestone["count"] += 1
            milestone["last"] = entry

    def _client_instance(self, client: httpx.AsyncClient, key: str) -> str | None:
        """Called under the lock; weak references never keep an HTTP client alive."""
        previous = self._client_instances.get(id(client))
        if previous is not None and previous[0]() is client:
            label = previous[1]
        elif len(self.client_instances) < self.limit:
            label = f"http-{len(self.client_instances) + 1}"
            self._client_instances[id(client)] = (weakref.ref(client), label)
            self.client_instances[label] = {"counts": Counter()}
        else:
            self.unattributed_client_requests += 1
            return None
        self.client_instances[label]["counts"][key] += 1
        return label

    def install_http(self, monkeypatch: Any) -> None:
        original_send = httpx.AsyncClient.send

        async def observed_send(
            client: httpx.AsyncClient, request: httpx.Request, *args: Any, **kwargs: Any,
        ) -> httpx.Response:
            started = self.clock()
            route = route_name(request)
            method = request.method if request.method in {"GET", "POST", "PUT", "PATCH", "DELETE"} else "OTHER"
            key = f"{method} {route}"
            with self._lock:
                self.total += 1
                request_id = self.total
                self.counts[key] += 1
                instance = self._client_instance(client, key)
                entry: dict[str, Any] = {
                    "request": request_id, "client": self.clients.get(id(client), "additional"),
                    "client_instance": instance,
                    "client_instance_state": "complete" if instance is not None else "truncated",
                    "method": method, "route": route, "started": started - self.started,
                    "response_metadata_state": "missing",
                }
                if len(self.active) == self.limit:
                    self.active.pop(next(iter(self.active)))
                    self.evicted_active += 1
                self.active[request_id] = entry
                self.active_count += 1
            try:
                response = await original_send(client, request, *args, **kwargs)
                metadata: dict[str, Any] = {"status": response.status_code}
                if response.is_stream_consumed:
                    metadata["response_metadata_state"] = self._response_metadata(metadata, response)
                with self._lock:
                    entry.update(metadata)
                    if 200 <= response.status_code < 300:
                        self.successful[key] += 1
                return response
            except BaseException as error:
                with self._lock:
                    entry["exception"] = type(error).__name__
                raise
            finally:
                with self._lock:
                    entry["elapsed"] = self.clock() - started
                    self.max_elapsed[key] = max(self.max_elapsed.get(key, 0), entry["elapsed"])
                    self.active_count -= 1
                    self.active.pop(request_id, None)
                    self.response_metadata_states[entry["response_metadata_state"]] += 1
                    if len(self.recent) == self.limit:
                        self.omitted_completed += 1
                    self.recent.append(entry)

        monkeypatch.setattr(httpx.AsyncClient, "send", observed_send)

    @staticmethod
    def _response_metadata(entry: dict[str, Any], response: httpx.Response) -> str:
        if not response.headers.get("content-type", "").startswith("application/json"):
            return "missing"
        try:
            if len(response.content) > 32_768:
                return "truncated"
            body = response.json()
        except httpx.ResponseNotRead:
            return "missing"
        except ValueError:
            return "invalid"
        if not isinstance(body, dict):
            return "invalid"
        state = "complete"
        if "task" in body:
            entry["task_state"] = (
                "present" if isinstance(body["task"], dict) else "null" if body["task"] is None else "invalid"
            )
            entry.update(task_identity(body["task"]))
            if entry["task_state"] == "invalid":
                state = "invalid"
        if type(body.get("recorded")) is bool:
            entry["recorded"] = body["recorded"]
        elif "recorded" in body:
            state = "invalid"
        error = body.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            if isinstance(code, str) and len(code) <= 80 and code.replace("_", "").isalnum():
                entry["error_code"] = code
        for field in ("reason", "status", "poll_status", "outcome"):
            value = body.get(field)
            if isinstance(value, str) and len(value) <= 80 and value.replace("_", "").isalnum():
                entry[field] = value
        return state

    def report(self, *, include_recent: bool = True) -> dict[str, Any]:
        with self._lock:
            return self._report(include_recent=include_recent)

    def _report(self, *, include_recent: bool) -> dict[str, Any]:
        return deepcopy({
            "elapsed": self.clock() - self.started, "requests": self.total,
            "counts": dict(self.counts), "milestones": self.milestones,
            "successful": dict(self.successful),
            "max_elapsed": self.max_elapsed,
            "active_count": self.active_count, "active": list(self.active.values()),
            "omitted_active": self.active_count - len(self.active),
            "recent": list(self.recent) if include_recent else [],
            "completeness": {
                "recent_http": "missing" if not include_recent and self.recent else (
                    "truncated" if self.omitted_completed else "complete"
                ),
                "active_http": "truncated" if self.evicted_active else "complete",
                "milestones": "truncated" if self.omitted_milestone_events else "complete",
                "client_attribution": "truncated" if self.unattributed_client_requests else "complete",
            },
            "omitted_completed": self.omitted_completed,
            "evicted_active": self.evicted_active,
            "omitted_milestone_events": self.omitted_milestone_events,
            "response_metadata_states": dict(self.response_metadata_states),
            "client_instances": self.client_instances,
            "unattributed_client_requests": self.unattributed_client_requests,
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


def print_observation(record: dict[str, Any]) -> None:
    print(json.dumps(record), flush=True)


async def observe_and_drain(
    observation: WorkerObservation, identity: dict[str, Any],
    workers: list[Any], runners: list[asyncio.Task[Any]], *,
    primary_error: BaseException | None = None,
    emit: Callable[[dict[str, Any]], None] = print_observation,
) -> None:
    """Bracket the existing shutdown with bounded reports, retaining the primary error.

    Capture availability is separate from report completeness and phase linkage.
    Capture is synchronous; API/SQL probes and timeout extensions are absent.
    """
    secondary_error: BaseException | None = None

    def capture(phase: str, error: BaseException | None) -> None:
        nonlocal secondary_error
        record: dict[str, Any] = {**identity, "phase": phase}
        if error is not None:
            record["exception"] = type(error).__name__
        try:
            record["monotonic_before"] = observation.clock()
            report = observation.report()
            record["monotonic_after"] = observation.clock()
            record["observation"] = report
            record["observation_capture_state"] = "complete"
        except BaseException as capture_error:
            record["observation_capture_state"] = "invalid"
            record["observation_exception"] = type(capture_error).__name__
            if secondary_error is None:
                secondary_error = capture_error
        try:
            emit(record)
        except BaseException as emit_error:
            if secondary_error is None:
                secondary_error = emit_error

    capture("before_shutdown", primary_error)
    shutdown_error: BaseException | None = None
    try:
        await stop_and_drain(workers, runners)
    except BaseException as error:
        shutdown_error = error
        if secondary_error is None:
            secondary_error = error
        capture("shutdown", error)
    finally:
        capture("after_shutdown", shutdown_error if shutdown_error is not None else primary_error)
    if primary_error is None and secondary_error is not None:
        raise secondary_error


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
