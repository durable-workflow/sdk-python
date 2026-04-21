"""Async client for the Durable Workflow server's control and worker planes.

The :class:`Client` wraps the server's HTTP/JSON protocol. Control-plane
methods (``start_workflow``, ``signal_workflow``, ``describe_workflow``,
schedule management, …) are what callers use to drive workflows from outside.
Worker-plane methods (``register_worker``, ``poll_workflow_task``,
``poll_query_task``, ``complete_activity_task``, …) are what the
:class:`~durable_workflow.Worker`
uses to run tasks; they are public so advanced users can build custom
workers, but most applications should not call them directly.

The module also defines the returned-value dataclasses (``WorkflowExecution``,
``WorkflowList``, ``ScheduleSpec``, ``ScheduleDescription``, …) and the
ergonomic handle classes (:class:`WorkflowHandle`, :class:`ScheduleHandle`)
that bind a workflow or schedule id to a :class:`Client` so you can call
methods without repeating the id on every call.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from typing import Any

import httpx

from . import serializer
from .errors import (
    ServerError,
    WorkflowCancelled,
    WorkflowFailed,
    WorkflowTerminated,
    _raise_for_status,
)
from .metrics import CLIENT_REQUEST_DURATION_SECONDS, CLIENT_REQUESTS, NOOP_METRICS, MetricsRecorder
from .retry_policy import TransportRetryPolicy

PROTOCOL_VERSION = "1.0"
CONTROL_PLANE_VERSION = "2"
CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA = "durable-workflow.v2.control-plane-request.contract"
CONTROL_PLANE_REQUEST_CONTRACT_VERSION = 1


def _default_sdk_version() -> str:
    try:
        return f"durable-workflow-python/{_pkg_version('durable-workflow')}"
    except PackageNotFoundError:
        return "durable-workflow-python/0.0.0+unknown"


DEFAULT_SDK_VERSION = _default_sdk_version()


def _route_for_metrics(path: str) -> str:
    clean_path = path.split("?", 1)[0]
    parts = [part for part in clean_path.strip("/").split("/") if part]
    if not parts:
        return "/"

    if parts[0] == "workflows" and len(parts) >= 2:
        parts[1] = "{workflow_id}"
        if len(parts) >= 4 and parts[2] in {"signal", "query", "update"}:
            parts[3] = "{name}"
        if len(parts) >= 4 and parts[2] == "runs":
            parts[3] = "{run_id}"
    elif parts[0] == "schedules" and len(parts) >= 2:
        parts[1] = "{schedule_id}"
    elif (
        parts[:2] == ["worker", "workflow-tasks"]
        or parts[:2] == ["worker", "activity-tasks"]
        or parts[:2] == ["worker", "query-tasks"]
    ) and len(parts) >= 3:
        parts[2] = "{task_id}"

    return "/" + "/".join(parts)


@dataclass
class WorkflowExecution:
    """Current server view of one workflow execution."""

    workflow_id: str
    run_id: str | None
    workflow_type: str
    status: str | None = None
    namespace: str | None = None
    task_queue: str | None = None
    input: Any = None
    output: Any = None
    payload_codec: str | None = None


@dataclass
class WorkflowList:
    """One page of workflow visibility results."""

    executions: list[WorkflowExecution]
    next_page_token: str | None = None


@dataclass
class ScheduleSpec:
    """Calendar or interval rules for a scheduled workflow."""

    cron_expressions: list[str] | None = None
    intervals: list[dict[str, str]] | None = None
    timezone: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.cron_expressions is not None:
            d["cron_expressions"] = self.cron_expressions
        if self.intervals is not None:
            d["intervals"] = self.intervals
        if self.timezone is not None:
            d["timezone"] = self.timezone
        return d


@dataclass
class ScheduleAction:
    """Workflow start request issued whenever a schedule fires."""

    workflow_type: str
    task_queue: str | None = None
    input: list[Any] | None = None
    execution_timeout_seconds: int | None = None
    run_timeout_seconds: int | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"workflow_type": self.workflow_type}
        if self.task_queue is not None:
            d["task_queue"] = self.task_queue
        if self.input is not None:
            d["input"] = serializer.envelope(self.input)
        if self.execution_timeout_seconds is not None:
            d["execution_timeout_seconds"] = self.execution_timeout_seconds
        if self.run_timeout_seconds is not None:
            d["run_timeout_seconds"] = self.run_timeout_seconds
        return d


@dataclass
class ScheduleDescription:
    """Current server view of a schedule and its recent execution state."""

    schedule_id: str
    status: str | None = None
    spec: dict[str, Any] | None = None
    action: dict[str, Any] | None = None
    overlap_policy: str | None = None
    note: str | None = None
    memo: dict[str, Any] | None = None
    search_attributes: dict[str, Any] | None = None
    jitter_seconds: int | None = None
    max_runs: int | None = None
    remaining_actions: int | None = None
    fires_count: int = 0
    failures_count: int = 0
    next_fire_at: str | None = None
    last_fired_at: str | None = None
    latest_workflow_instance_id: str | None = None
    paused_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    info: dict[str, Any] | None = None


@dataclass
class ScheduleList:
    """One page of schedule visibility results."""

    schedules: list[ScheduleDescription]
    next_page_token: str | None = None


@dataclass
class ScheduleTriggerResult:
    """Outcome returned after manually triggering a schedule."""

    schedule_id: str
    outcome: str
    workflow_id: str | None = None
    run_id: str | None = None
    reason: str | None = None
    buffer_depth: int | None = None


@dataclass
class ScheduleBackfillResult:
    """Outcome returned after asking a schedule to backfill missed fires."""

    schedule_id: str
    outcome: str
    fires_attempted: int = 0
    results: list[dict[str, Any]] | None = None


class WorkflowHandle:
    """Convenience wrapper for operating on one workflow ID."""

    def __init__(self, client: Client, workflow_id: str, run_id: str | None = None, workflow_type: str = "") -> None:
        self._client = client
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.workflow_type = workflow_type

    async def result(self, *, poll_interval: float = 0.5, timeout: float = 30.0) -> Any:
        """Block until this workflow terminates and return its result. See :meth:`Client.get_result`."""
        return await self._client.get_result(self, poll_interval=poll_interval, timeout=timeout)

    async def describe(self) -> WorkflowExecution:
        """Return the server's current view of this workflow. See :meth:`Client.describe_workflow`."""
        return await self._client.describe_workflow(self.workflow_id)

    async def signal(self, signal_name: str, args: list[Any] | None = None) -> None:
        """Deliver an external signal to this workflow. See :meth:`Client.signal_workflow`."""
        await self._client.signal_workflow(self.workflow_id, signal_name, args=args)

    async def query(self, query_name: str, args: list[Any] | None = None) -> Any:
        """Execute a read-only query against this workflow. See :meth:`Client.query_workflow`."""
        return await self._client.query_workflow(self.workflow_id, query_name, args=args)

    async def cancel(self, *, reason: str | None = None) -> None:
        """Request graceful cancellation of this workflow. See :meth:`Client.cancel_workflow`."""
        await self._client.cancel_workflow(self.workflow_id, reason=reason)

    async def terminate(self, *, reason: str | None = None) -> None:
        """Forcefully stop this workflow. See :meth:`Client.terminate_workflow`."""
        await self._client.terminate_workflow(self.workflow_id, reason=reason)

    async def update(
        self,
        update_name: str,
        args: list[Any] | None = None,
        *,
        wait_for: str | None = None,
        wait_timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> Any:
        """Send a synchronous update to this workflow and wait for the result. See :meth:`Client.update_workflow`."""
        return await self._client.update_workflow(
            self.workflow_id,
            update_name,
            args=args,
            wait_for=wait_for,
            wait_timeout_seconds=wait_timeout_seconds,
            request_id=request_id,
        )


class ScheduleHandle:
    """Convenience wrapper for operating on one schedule ID."""

    def __init__(self, client: Client, schedule_id: str) -> None:
        self._client = client
        self.schedule_id = schedule_id

    async def describe(self) -> ScheduleDescription:
        """Return the server's current view of this schedule. See :meth:`Client.describe_schedule`."""
        return await self._client.describe_schedule(self.schedule_id)

    async def update(
        self,
        *,
        spec: ScheduleSpec | None = None,
        action: ScheduleAction | None = None,
        overlap_policy: str | None = None,
        jitter_seconds: int | None = None,
        max_runs: int | None = None,
        memo: dict[str, Any] | None = None,
        search_attributes: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> None:
        """Update one or more fields of this schedule. See :meth:`Client.update_schedule`."""
        await self._client.update_schedule(
            self.schedule_id,
            spec=spec,
            action=action,
            overlap_policy=overlap_policy,
            jitter_seconds=jitter_seconds,
            max_runs=max_runs,
            memo=memo,
            search_attributes=search_attributes,
            note=note,
        )

    async def pause(self, *, note: str | None = None) -> None:
        """Pause this schedule so it stops firing. See :meth:`Client.pause_schedule`."""
        await self._client.pause_schedule(self.schedule_id, note=note)

    async def resume(self, *, note: str | None = None) -> None:
        """Resume this paused schedule. See :meth:`Client.resume_schedule`."""
        await self._client.resume_schedule(self.schedule_id, note=note)

    async def trigger(self, *, overlap_policy: str | None = None) -> ScheduleTriggerResult:
        """Fire this schedule immediately. See :meth:`Client.trigger_schedule`."""
        return await self._client.trigger_schedule(self.schedule_id, overlap_policy=overlap_policy)

    async def delete(self) -> None:
        """Delete this schedule. See :meth:`Client.delete_schedule`."""
        await self._client.delete_schedule(self.schedule_id)

    async def backfill(
        self,
        *,
        start_time: str,
        end_time: str,
        overlap_policy: str | None = None,
    ) -> ScheduleBackfillResult:
        """Fire this schedule for every moment in a past time range. See :meth:`Client.backfill_schedule`."""
        return await self._client.backfill_schedule(
            self.schedule_id, start_time=start_time, end_time=end_time, overlap_policy=overlap_policy,
        )


class Client:
    """Async HTTP client for Durable Workflow control-plane and worker APIs.

    The client owns one `httpx.AsyncClient` connection pool. Use it as an async
    context manager or call `aclose()` when finished.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        control_token: str | None = None,
        worker_token: str | None = None,
        namespace: str = "default",
        timeout: float = 60.0,
        retry_policy: TransportRetryPolicy | None = None,
        metrics: MetricsRecorder | None = None,
        payload_size_limit_bytes: int = serializer.DEFAULT_PAYLOAD_SIZE_BYTES,
        payload_size_warning_threshold_percent: int = serializer.DEFAULT_WARNING_THRESHOLD_PERCENT,
        payload_size_warnings: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.control_token = control_token
        self.worker_token = worker_token
        self.namespace = namespace
        self.retry_policy = retry_policy or TransportRetryPolicy()
        self.metrics = metrics or NOOP_METRICS
        self.payload_size_warning_config = (
            serializer.PayloadSizeWarningConfig(
                limit_bytes=payload_size_limit_bytes,
                threshold_percent=payload_size_warning_threshold_percent,
            )
            if payload_size_warnings
            else None
        )
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def aclose(self) -> None:
        """Close the underlying ``httpx`` connection pool.

        Equivalent to exiting the async-context-manager form of the client.
        Safe to call multiple times.
        """
        await self._http.aclose()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    def _headers(self, *, worker: bool = False) -> dict[str, str]:
        h: dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
        token = self._auth_token(worker=worker)
        if token:
            h["Authorization"] = f"Bearer {token}"
        h["X-Namespace"] = self.namespace
        if worker:
            h["X-Durable-Workflow-Protocol-Version"] = PROTOCOL_VERSION
        else:
            h["X-Durable-Workflow-Control-Plane-Version"] = CONTROL_PLANE_VERSION
        return h

    def _auth_token(self, *, worker: bool = False) -> str | None:
        if worker:
            return self.worker_token or self.token or self.control_token
        return self.control_token or self.token or self.worker_token

    def _payload_context(
        self,
        *,
        kind: str,
        workflow_id: str | None = None,
        run_id: str | None = None,
        activity_name: str | None = None,
        signal_name: str | None = None,
        update_name: str | None = None,
        query_name: str | None = None,
        schedule_id: str | None = None,
        task_queue: str | None = None,
    ) -> serializer.PayloadSizeWarningContext:
        return serializer.PayloadSizeWarningContext(
            kind=kind,
            workflow_id=workflow_id,
            run_id=run_id,
            activity_name=activity_name,
            signal_name=signal_name,
            update_name=update_name,
            query_name=query_name,
            schedule_id=schedule_id,
            task_queue=task_queue,
            namespace=self.namespace,
        )

    def _payload_envelope(
        self,
        value: Any,
        *,
        kind: str,
        codec: str = serializer.AVRO_CODEC,
        workflow_id: str | None = None,
        run_id: str | None = None,
        activity_name: str | None = None,
        signal_name: str | None = None,
        update_name: str | None = None,
        query_name: str | None = None,
        schedule_id: str | None = None,
        task_queue: str | None = None,
    ) -> dict[str, str]:
        return serializer.envelope(
            value,
            codec=codec,
            size_warning=self.payload_size_warning_config,
            warning_context=self._payload_context(
                kind=kind,
                workflow_id=workflow_id,
                run_id=run_id,
                activity_name=activity_name,
                signal_name=signal_name,
                update_name=update_name,
                query_name=query_name,
                schedule_id=schedule_id,
                task_queue=task_queue,
            ),
        )

    def _warn_json_payload_size(
        self,
        value: Any,
        *,
        kind: str,
        workflow_id: str | None = None,
        schedule_id: str | None = None,
        task_queue: str | None = None,
    ) -> None:
        serializer.warn_if_json_payload_near_limit(
            value,
            size_warning=self.payload_size_warning_config,
            warning_context=self._payload_context(
                kind=kind,
                workflow_id=workflow_id,
                schedule_id=schedule_id,
                task_queue=task_queue,
            ),
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        worker: bool = False,
        json: Any = None,
        timeout: float | None = None,
        context: str = "",
    ) -> Any:
        start = time.perf_counter()
        route = _route_for_metrics(path)
        plane = "worker" if worker else "control"
        status_code = "none"
        outcome = "pending"

        async def _do_request() -> httpx.Response:
            resp = await self._http.request(
                method,
                f"/api{path}",
                headers=self._headers(worker=worker),
                json=json,
                timeout=timeout,
            )
            # Raise HTTPStatusError for 4xx/5xx so retry policy can catch it
            resp.raise_for_status()
            return resp

        try:
            try:
                resp = await self.retry_policy.execute(_do_request)
            except httpx.HTTPStatusError as exc:
                status_code = str(exc.response.status_code)
                outcome = "http_error"
                # Convert to our custom exception types
                try:
                    body = exc.response.json()
                except ValueError:
                    body = exc.response.text
                _raise_for_status(exc.response.status_code, body, context=context)
                raise  # unreachable, but keeps type checker happy

            status_code = str(resp.status_code)
            if resp.status_code == 204 or not resp.content:
                outcome = "ok"
                return None
            result = resp.json()
            outcome = "ok"
            return result
        except Exception as exc:
            if outcome == "pending":
                outcome = type(exc).__name__
            raise
        finally:
            tags = {
                "method": method.upper(),
                "route": route,
                "plane": plane,
                "status_code": status_code,
                "outcome": outcome,
            }
            self.metrics.increment(CLIENT_REQUESTS, tags=tags)
            self.metrics.record(CLIENT_REQUEST_DURATION_SECONDS, time.perf_counter() - start, tags=tags)

    async def get_cluster_info(self) -> dict[str, Any]:
        """Fetch server build identity, capabilities, and protocol manifests."""
        result = await self._request("GET", "/cluster/info", worker=False, context="get_cluster_info")
        if not isinstance(result, dict):
            raise ServerError(
                200,
                {"reason": "invalid_cluster_info", "message": f"expected JSON object, got {type(result).__name__}"},
            )
        return result

    def get_workflow_handle(
        self, workflow_id: str, *, run_id: str | None = None, workflow_type: str = ""
    ) -> WorkflowHandle:
        """Return a :class:`WorkflowHandle` bound to an existing workflow instance.

        Does not round-trip to the server. Pass ``run_id`` to pin the handle to
        a specific run, otherwise the handle resolves to whichever run is
        current at the time each method is called. ``workflow_type`` is
        optional and used only in error messages.
        """
        return WorkflowHandle(self, workflow_id=workflow_id, run_id=run_id, workflow_type=workflow_type)

    # ── Health ─────────────────────────────────────────────────────────
    async def health(self) -> dict[str, Any]:
        """Call the server's ``/health`` endpoint and return the JSON response."""
        result = await self._request("GET", "/health")
        if not isinstance(result, dict):
            raise ServerError(
                200,
                {"reason": "invalid_health_response", "message": f"expected JSON object, got {type(result).__name__}"},
            )
        return result

    # ── Workflows ──────────────────────────────────────────────────────
    async def start_workflow(
        self,
        *,
        workflow_type: str,
        task_queue: str,
        workflow_id: str,
        input: list[Any] | None = None,
        execution_timeout_seconds: int = 3600,
        run_timeout_seconds: int = 600,
        duplicate_policy: str | None = None,
        memo: dict[str, Any] | None = None,
        search_attributes: dict[str, Any] | None = None,
    ) -> WorkflowHandle:
        """Start a new workflow instance and return a handle bound to it.

        ``workflow_type`` is the language-neutral type key registered via
        :func:`durable_workflow.workflow.defn`. ``task_queue`` selects which
        worker pool picks up the workflow. ``workflow_id`` is the
        caller-supplied instance id — if it collides with an existing
        workflow, behavior depends on ``duplicate_policy``
        (``reject`` | ``allow`` | ``terminate_existing``).

        ``input`` is a list of positional arguments passed to the workflow's
        ``run`` method; the SDK encodes the list with the default payload
        codec (Avro). ``execution_timeout_seconds`` covers the entire workflow
        execution across all runs (including continue-as-new), while
        ``run_timeout_seconds`` applies to this single run only.

        ``memo`` and ``search_attributes`` attach operator-facing metadata to
        the instance; see the main docs site for the key/value rules.
        """
        body: dict[str, Any] = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "task_queue": task_queue,
            "input": self._payload_envelope(
                input if input is not None else [],
                kind="workflow_input",
                workflow_id=workflow_id,
                task_queue=task_queue,
            ),
            "execution_timeout_seconds": execution_timeout_seconds,
            "run_timeout_seconds": run_timeout_seconds,
        }
        if duplicate_policy is not None:
            body["duplicate_policy"] = duplicate_policy
        if memo is not None:
            body["memo"] = memo
        if search_attributes is not None:
            self._warn_json_payload_size(
                search_attributes,
                kind="search_attributes",
                workflow_id=workflow_id,
                task_queue=task_queue,
            )
            body["search_attributes"] = search_attributes
        data = await self._request("POST", "/workflows", json=body, context=workflow_id)
        return WorkflowHandle(
            self,
            workflow_id=data["workflow_id"],
            run_id=data.get("run_id"),
            workflow_type=data["workflow_type"],
        )

    async def describe_workflow(self, workflow_id: str) -> WorkflowExecution:
        """Return the server's current view of a workflow instance.

        Resolves to the newest durable run in the instance's chain (including
        any continue-as-new runs). Decodes the recorded ``input`` and
        ``output`` envelopes when present.
        """
        data = await self._request("GET", f"/workflows/{workflow_id}", context=workflow_id)
        input_val = None
        output_val = None
        if data.get("input_envelope"):
            input_val = serializer.decode_envelope(data["input_envelope"])
        elif data.get("input") is not None:
            input_val = data["input"]
        if data.get("output_envelope"):
            output_val = serializer.decode_envelope(data["output_envelope"])
        elif data.get("output") is not None:
            output_val = data["output"]
        return WorkflowExecution(
            workflow_id=data.get("workflow_id", workflow_id),
            run_id=data.get("run_id"),
            workflow_type=data.get("workflow_type", ""),
            status=data.get("status"),
            namespace=data.get("namespace"),
            task_queue=data.get("task_queue"),
            input=input_val,
            output=output_val,
            payload_codec=data.get("payload_codec"),
        )

    async def list_workflows(
        self,
        *,
        workflow_type: str | None = None,
        status: str | None = None,
        query: str | None = None,
        page_size: int | None = None,
        next_page_token: str | None = None,
    ) -> WorkflowList:
        """Page through workflow instances, optionally filtered by type, status, or query string.

        Pass the returned :attr:`WorkflowList.next_page_token` as
        ``next_page_token`` on the next call to fetch the following page; the
        token is ``None`` when there are no more pages.
        """
        params: dict[str, str] = {}
        if workflow_type is not None:
            params["workflow_type"] = workflow_type
        if status is not None:
            params["status"] = status
        if query is not None:
            params["query"] = query
        if page_size is not None:
            params["page_size"] = str(page_size)
        if next_page_token is not None:
            params["next_page_token"] = next_page_token

        qs = "&".join(f"{k}={v}" for k, v in params.items())
        path = f"/workflows?{qs}" if qs else "/workflows"
        data = await self._request("GET", path)
        items = data.get("workflows", [])
        executions = [
            WorkflowExecution(
                workflow_id=item.get("workflow_id", ""),
                run_id=item.get("run_id"),
                workflow_type=item.get("workflow_type", ""),
                status=item.get("status"),
            )
            for item in items
        ]
        return WorkflowList(
            executions=executions,
            next_page_token=data.get("next_page_token"),
        )

    async def get_history(self, workflow_id: str, run_id: str) -> Any:
        """Fetch the full durable history for one specific run of a workflow."""
        return await self._request(
            "GET", f"/workflows/{workflow_id}/runs/{run_id}/history", context=workflow_id
        )

    async def signal_workflow(
        self, workflow_id: str, signal_name: str, *, args: list[Any] | None = None
    ) -> None:
        """Deliver an external signal to a running workflow.

        Signals are fire-and-forget: the server records the signal in durable
        history and returns immediately. They do not wait for the workflow to
        observe the signal. See the main docs for how to declare the allowed
        signal names on a workflow type.
        """
        body: dict[str, Any] = {}
        if args:
            body["input"] = self._payload_envelope(
                args,
                kind="signal",
                workflow_id=workflow_id,
                signal_name=signal_name,
            )
        await self._request("POST", f"/workflows/{workflow_id}/signal/{signal_name}", json=body, context=workflow_id)

    async def query_workflow(
        self, workflow_id: str, query_name: str, *, args: list[Any] | None = None
    ) -> Any:
        """Execute a named read-only query against a workflow and return the result.

        Queries are synchronous and non-mutating. The server runs the named
        query handler inside the workflow process and returns the decoded
        result. Raises :class:`~durable_workflow.errors.QueryFailed` if the
        query was rejected or the handler errored.
        """
        body: dict[str, Any] = {}
        if args:
            body["input"] = self._payload_envelope(
                args,
                kind="query",
                workflow_id=workflow_id,
                query_name=query_name,
            )
        return await self._request(
            "POST", f"/workflows/{workflow_id}/query/{query_name}", json=body, context=workflow_id
        )

    async def cancel_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        """Request graceful cancellation of a workflow's current run.

        Cancellation is cooperative: the server delivers a cancellation signal
        that the workflow can observe and handle (e.g. to roll back via a
        saga). Compare with :meth:`terminate_workflow`, which is forceful.
        """
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        await self._request("POST", f"/workflows/{workflow_id}/cancel", json=body, context=workflow_id)

    async def terminate_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        """Forcefully stop a workflow without giving it a chance to clean up.

        Prefer :meth:`cancel_workflow` when the workflow code can implement
        graceful shutdown. Termination is an operator escape hatch.
        """
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        await self._request("POST", f"/workflows/{workflow_id}/terminate", json=body, context=workflow_id)

    async def update_workflow(
        self,
        workflow_id: str,
        update_name: str,
        *,
        args: list[Any] | None = None,
        wait_for: str | None = None,
        wait_timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> Any:
        """Send a synchronous update to a running workflow and wait for the result.

        Updates are request/response calls to a named handler on the workflow;
        the handler may mutate durable workflow state and return a value.
        ``wait_for`` selects how long the server waits before returning —
        typically ``completed`` to block until the handler finishes, or
        ``accepted`` to return once the validator has approved it.

        ``request_id`` lets the caller deduplicate retries. Raises
        :class:`~durable_workflow.errors.UpdateRejected` when the workflow's
        validator rejects the update.
        """
        body: dict[str, Any] = {}
        if args:
            body["input"] = self._payload_envelope(
                args,
                kind="update",
                workflow_id=workflow_id,
                update_name=update_name,
            )
        if wait_for is not None:
            body["wait_for"] = wait_for
        if wait_timeout_seconds is not None:
            body["wait_timeout_seconds"] = wait_timeout_seconds
        if request_id is not None:
            body["request_id"] = request_id
        return await self._request(
            "POST", f"/workflows/{workflow_id}/update/{update_name}", json=body, context=workflow_id
        )

    async def get_result(
        self,
        handle: WorkflowHandle,
        *,
        poll_interval: float = 0.5,
        timeout: float = 30.0,
    ) -> Any:
        """Poll a workflow until it reaches a terminal state and return its result.

        Raises :class:`~durable_workflow.errors.WorkflowFailed`,
        :class:`~durable_workflow.errors.WorkflowCancelled`, or
        :class:`~durable_workflow.errors.WorkflowTerminated` if the workflow
        ended in a non-success state, or :class:`TimeoutError` if ``timeout``
        seconds elapse before the workflow terminates.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            desc = await self.describe_workflow(handle.workflow_id)
            status = desc.status
            if status in ("completed", "failed", "terminated", "canceled", "cancelled"):
                run_id = handle.run_id or desc.run_id
                if run_id is None:
                    raise WorkflowFailed("no run_id available to fetch history")
                history = await self.get_history(handle.workflow_id, run_id)
                events = history.get("events", [])
                for ev in reversed(events):
                    etype = ev.get("event_type")
                    payload = ev.get("payload") or {}
                    if etype == "WorkflowCompleted":
                        return serializer.decode_envelope(
                            payload.get("output"),
                            codec=payload.get("payload_codec") or desc.payload_codec,
                        )
                    if etype == "WorkflowFailed":
                        raise WorkflowFailed(
                            payload.get("message", "workflow failed"),
                            payload.get("exception_class"),
                        )
                    if etype == "WorkflowTerminated":
                        raise WorkflowTerminated(
                            payload.get("reason", "workflow was terminated")
                        )
                    if etype == "WorkflowCancelled":
                        raise WorkflowCancelled(
                            payload.get("reason", "workflow was cancelled")
                        )
                return None
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(
                    f"workflow {handle.workflow_id} not terminal after {timeout}s (status={status})"
                )
            await asyncio.sleep(poll_interval)

    # ── Schedules ─────────────────────────────────────────────────────
    def get_schedule_handle(self, schedule_id: str) -> ScheduleHandle:
        """Return a :class:`ScheduleHandle` bound to an existing schedule.

        Does not round-trip to the server. Use :meth:`describe_schedule` via
        the handle when you need to verify the schedule actually exists.
        """
        return ScheduleHandle(self, schedule_id=schedule_id)

    async def create_schedule(
        self,
        *,
        schedule_id: str | None = None,
        spec: ScheduleSpec,
        action: ScheduleAction,
        overlap_policy: str | None = None,
        jitter_seconds: int | None = None,
        max_runs: int | None = None,
        memo: dict[str, Any] | None = None,
        search_attributes: dict[str, Any] | None = None,
        paused: bool = False,
        note: str | None = None,
    ) -> ScheduleHandle:
        """Create a new schedule and return a handle bound to it.

        ``spec`` describes when the schedule fires (cron expressions,
        intervals, calendars); ``action`` describes what it starts (typically
        a workflow). ``overlap_policy`` controls what happens when a fire
        would overlap an already-running action — ``skip``, ``buffer_one``,
        ``buffer_all``, ``cancel_other``, or ``terminate_other``. Pass
        ``paused=True`` to create the schedule in a paused state and resume
        it later with :meth:`resume_schedule`.
        """
        body: dict[str, Any] = {
            "spec": spec.to_dict(),
            "action": action.to_dict(),
        }
        if schedule_id is not None:
            body["schedule_id"] = schedule_id
        if overlap_policy is not None:
            body["overlap_policy"] = overlap_policy
        if jitter_seconds is not None:
            body["jitter_seconds"] = jitter_seconds
        if max_runs is not None:
            body["max_runs"] = max_runs
        if memo is not None:
            body["memo"] = memo
        if search_attributes is not None:
            self._warn_json_payload_size(
                search_attributes,
                kind="schedule_search_attributes",
                schedule_id=schedule_id,
            )
            body["search_attributes"] = search_attributes
        if paused:
            body["paused"] = True
        if note is not None:
            body["note"] = note
        data = await self._request("POST", "/schedules", json=body)
        sid = data.get("schedule_id", schedule_id or "")
        return ScheduleHandle(self, schedule_id=sid)

    async def list_schedules(self) -> ScheduleList:
        """Return all schedules in the current namespace."""
        data = await self._request("GET", "/schedules")
        items = data.get("schedules", [])
        schedules = [
            ScheduleDescription(
                schedule_id=item.get("schedule_id", ""),
                status=item.get("status"),
                spec=item.get("spec"),
                action=item.get("action"),
                overlap_policy=item.get("overlap_policy"),
                note=item.get("note"),
                fires_count=item.get("fires_count", 0),
                next_fire_at=item.get("next_fire_at"),
                last_fired_at=item.get("last_fired_at"),
            )
            for item in items
        ]
        return ScheduleList(
            schedules=schedules,
            next_page_token=data.get("next_page_token"),
        )

    async def describe_schedule(self, schedule_id: str) -> ScheduleDescription:
        """Return the server's current view of a schedule, including status and fire counters."""
        data = await self._request("GET", f"/schedules/{schedule_id}", context=schedule_id)
        return ScheduleDescription(
            schedule_id=data.get("schedule_id", schedule_id),
            status=data.get("status"),
            spec=data.get("spec"),
            action=data.get("action"),
            overlap_policy=data.get("overlap_policy"),
            note=data.get("note"),
            memo=data.get("memo"),
            search_attributes=data.get("search_attributes"),
            jitter_seconds=data.get("jitter_seconds"),
            max_runs=data.get("max_runs"),
            remaining_actions=data.get("remaining_actions"),
            fires_count=data.get("fires_count", 0),
            failures_count=data.get("failures_count", 0),
            next_fire_at=data.get("next_fire_at"),
            last_fired_at=data.get("last_fired_at"),
            latest_workflow_instance_id=data.get("latest_workflow_instance_id"),
            paused_at=data.get("paused_at"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            info=data.get("info"),
        )

    async def update_schedule(
        self,
        schedule_id: str,
        *,
        spec: ScheduleSpec | None = None,
        action: ScheduleAction | None = None,
        overlap_policy: str | None = None,
        jitter_seconds: int | None = None,
        max_runs: int | None = None,
        memo: dict[str, Any] | None = None,
        search_attributes: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> None:
        """Update one or more fields of an existing schedule.

        Pass ``None`` for any field you don't want to change. Unknown fields
        are ignored by the server.
        """
        body: dict[str, Any] = {}
        if spec is not None:
            body["spec"] = spec.to_dict()
        if action is not None:
            body["action"] = action.to_dict()
        if overlap_policy is not None:
            body["overlap_policy"] = overlap_policy
        if jitter_seconds is not None:
            body["jitter_seconds"] = jitter_seconds
        if max_runs is not None:
            body["max_runs"] = max_runs
        if memo is not None:
            body["memo"] = memo
        if search_attributes is not None:
            self._warn_json_payload_size(
                search_attributes,
                kind="schedule_search_attributes",
                schedule_id=schedule_id,
            )
            body["search_attributes"] = search_attributes
        if note is not None:
            body["note"] = note
        await self._request("PUT", f"/schedules/{schedule_id}", json=body, context=schedule_id)

    async def pause_schedule(self, schedule_id: str, *, note: str | None = None) -> None:
        """Pause a schedule so it stops firing until resumed.

        Optional ``note`` is recorded as operator metadata on the pause
        event. Pausing does not cancel workflows that are already running.
        """
        body: dict[str, Any] = {}
        if note is not None:
            body["note"] = note
        await self._request("POST", f"/schedules/{schedule_id}/pause", json=body, context=schedule_id)

    async def resume_schedule(self, schedule_id: str, *, note: str | None = None) -> None:
        """Resume a paused schedule so it begins firing again."""
        body: dict[str, Any] = {}
        if note is not None:
            body["note"] = note
        await self._request("POST", f"/schedules/{schedule_id}/resume", json=body, context=schedule_id)

    async def trigger_schedule(
        self, schedule_id: str, *, overlap_policy: str | None = None
    ) -> ScheduleTriggerResult:
        """Fire a schedule immediately, outside its normal schedule.

        The ``overlap_policy`` override applies only to this one manual fire.
        The returned :class:`ScheduleTriggerResult` reports whether the fire
        was accepted or skipped (e.g. due to overlap).
        """
        body: dict[str, Any] = {}
        if overlap_policy is not None:
            body["overlap_policy"] = overlap_policy
        data = await self._request(
            "POST", f"/schedules/{schedule_id}/trigger", json=body, context=schedule_id,
        )
        return ScheduleTriggerResult(
            schedule_id=data.get("schedule_id", schedule_id),
            outcome=data.get("outcome", ""),
            workflow_id=data.get("workflow_id"),
            run_id=data.get("run_id"),
            reason=data.get("reason"),
            buffer_depth=data.get("buffer_depth"),
        )

    async def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule. Running workflows the schedule already started are unaffected."""
        await self._request("DELETE", f"/schedules/{schedule_id}", context=schedule_id)

    async def backfill_schedule(
        self,
        schedule_id: str,
        *,
        start_time: str,
        end_time: str,
        overlap_policy: str | None = None,
    ) -> ScheduleBackfillResult:
        """Fire a schedule for every would-have-been moment in ``[start_time, end_time]``.

        Times are ISO-8601 strings. Useful to replay a period the schedule
        was paused or to seed historical runs. The returned
        :class:`ScheduleBackfillResult` reports how many fires were attempted
        and the outcome of each.
        """
        body: dict[str, Any] = {
            "start_time": start_time,
            "end_time": end_time,
        }
        if overlap_policy is not None:
            body["overlap_policy"] = overlap_policy
        data = await self._request(
            "POST", f"/schedules/{schedule_id}/backfill", json=body, context=schedule_id,
        )
        return ScheduleBackfillResult(
            schedule_id=data.get("schedule_id", schedule_id),
            outcome=data.get("outcome", ""),
            fires_attempted=data.get("fires_attempted", 0),
            results=data.get("results"),
        )

    # ── Worker protocol ────────────────────────────────────────────────
    async def register_worker(
        self,
        *,
        worker_id: str,
        task_queue: str,
        supported_workflow_types: list[str] | None = None,
        workflow_definition_fingerprints: dict[str, str] | None = None,
        supported_activity_types: list[str] | None = None,
        runtime: str = "python",
        sdk_version: str | None = None,
    ) -> Any:
        """Register this process with the server as a worker for ``task_queue``.

        Called by :class:`~durable_workflow.Worker` at startup. Most
        applications should not call this directly — create a
        :class:`~durable_workflow.Worker` instead.
        """
        if sdk_version is None:
            sdk_version = DEFAULT_SDK_VERSION
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "task_queue": task_queue,
            "runtime": runtime,
            "sdk_version": sdk_version,
            "supported_workflow_types": supported_workflow_types or [],
            "supported_activity_types": supported_activity_types or [],
        }
        if workflow_definition_fingerprints is not None:
            body["workflow_definition_fingerprints"] = workflow_definition_fingerprints
        return await self._request("POST", "/worker/register", worker=True, json=body)

    async def poll_workflow_task(
        self, *, worker_id: str, task_queue: str, timeout: float = 35.0
    ) -> Any:
        """Long-poll for the next workflow task on ``task_queue``.

        Returns the task payload, or ``None`` on poll timeout. Worker-plane
        endpoint — most applications use :class:`~durable_workflow.Worker`
        rather than calling this directly.
        """
        body: dict[str, Any] = {"worker_id": worker_id, "task_queue": task_queue}
        try:
            data = await self._request(
                "POST", "/worker/workflow-tasks/poll", worker=True, json=body, timeout=timeout
            )
        except httpx.TimeoutException:
            return None
        return (data or {}).get("task")

    async def complete_workflow_task(
        self,
        *,
        task_id: str,
        lease_owner: str,
        workflow_task_attempt: int,
        commands: list[dict[str, Any]],
    ) -> Any:
        """Report successful execution of a workflow task with its emitted commands.

        Worker-plane endpoint, called by :class:`~durable_workflow.Worker`.
        ``commands`` is the list of serialized commands the workflow yielded
        for this task.
        """
        body: dict[str, Any] = {
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "commands": commands,
        }
        return await self._request(
            "POST", f"/worker/workflow-tasks/{task_id}/complete", worker=True, json=body
        )

    async def fail_workflow_task(
        self,
        *,
        task_id: str,
        lease_owner: str,
        workflow_task_attempt: int,
        message: str,
        failure_type: str | None = None,
        stack_trace: str | None = None,
    ) -> Any:
        """Report a workflow task failure so the server can schedule a retry.

        Worker-plane endpoint. Task failures (e.g. non-determinism) are
        distinct from workflow failures (``FailWorkflow`` commands).
        """
        failure: dict[str, Any] = {"message": message}
        if failure_type is not None:
            failure["type"] = failure_type
        if stack_trace is not None:
            failure["stack_trace"] = stack_trace
        body: dict[str, Any] = {
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "failure": failure,
        }
        return await self._request(
            "POST", f"/worker/workflow-tasks/{task_id}/fail", worker=True, json=body
        )

    async def workflow_task_history(
        self,
        *,
        task_id: str,
        page_token: str,
        lease_owner: str,
        workflow_task_attempt: int,
    ) -> Any:
        """Page through extra history events while the worker is executing a long task.

        Worker-plane endpoint. The first page of history is delivered inline
        with the workflow task; this endpoint fetches subsequent pages.
        """
        body: dict[str, Any] = {
            "page_token": page_token,
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
        }
        return await self._request(
            "POST", f"/worker/workflow-tasks/{task_id}/history", worker=True, json=body
        )

    async def poll_query_task(
        self, *, worker_id: str, task_queue: str, timeout: float = 35.0
    ) -> Any:
        """Long-poll for the next workflow query task on ``task_queue``.

        Query tasks are ephemeral worker-plane requests created when the server
        must route a control-plane query to a non-PHP workflow runtime.
        """
        body: dict[str, Any] = {"worker_id": worker_id, "task_queue": task_queue}
        try:
            data = await self._request(
                "POST", "/worker/query-tasks/poll", worker=True, json=body, timeout=timeout
            )
        except httpx.TimeoutException:
            return None
        return (data or {}).get("task")

    async def complete_query_task(
        self,
        *,
        query_task_id: str,
        lease_owner: str,
        query_task_attempt: int,
        result: Any,
        codec: str = serializer.AVRO_CODEC,
        workflow_id: str | None = None,
        run_id: str | None = None,
        query_name: str | None = None,
    ) -> Any:
        """Submit the successful result for a worker-routed query task."""
        body: dict[str, Any] = {
            "lease_owner": lease_owner,
            "query_task_attempt": query_task_attempt,
            "result": result,
            "result_envelope": self._payload_envelope(
                result,
                codec=codec,
                kind="query_result",
                workflow_id=workflow_id,
                run_id=run_id,
                query_name=query_name,
            ),
        }
        return await self._request(
            "POST", f"/worker/query-tasks/{query_task_id}/complete", worker=True, json=body
        )

    async def fail_query_task(
        self,
        *,
        query_task_id: str,
        lease_owner: str,
        query_task_attempt: int,
        message: str,
        reason: str = "query_rejected",
        failure_type: str | None = None,
        stack_trace: str | None = None,
    ) -> Any:
        """Report a failed worker-routed query task."""
        failure: dict[str, Any] = {"message": message, "reason": reason}
        if failure_type is not None:
            failure["type"] = failure_type
        if stack_trace is not None:
            failure["stack_trace"] = stack_trace
        body: dict[str, Any] = {
            "lease_owner": lease_owner,
            "query_task_attempt": query_task_attempt,
            "failure": failure,
        }
        return await self._request(
            "POST", f"/worker/query-tasks/{query_task_id}/fail", worker=True, json=body
        )

    async def poll_activity_task(
        self, *, worker_id: str, task_queue: str, timeout: float = 35.0
    ) -> Any:
        """Long-poll for the next activity task on ``task_queue``.

        Returns the task payload, or ``None`` on poll timeout. Worker-plane
        endpoint — typically used by :class:`~durable_workflow.Worker`.
        """
        body: dict[str, Any] = {"worker_id": worker_id, "task_queue": task_queue}
        try:
            data = await self._request(
                "POST", "/worker/activity-tasks/poll", worker=True, json=body, timeout=timeout
            )
        except httpx.TimeoutException:
            return None
        return (data or {}).get("task")

    async def complete_activity_task(
        self,
        *,
        task_id: str,
        activity_attempt_id: str,
        lease_owner: str,
        result: Any,
        codec: str = serializer.AVRO_CODEC,
        activity_name: str | None = None,
    ) -> Any:
        """Report successful activity execution and submit the encoded result."""
        body: dict[str, Any] = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "result": self._payload_envelope(
                result,
                codec=codec,
                kind="activity_result",
                activity_name=activity_name,
            ),
        }
        return await self._request(
            "POST", f"/worker/activity-tasks/{task_id}/complete", worker=True, json=body
        )

    async def fail_activity_task(
        self,
        *,
        task_id: str,
        activity_attempt_id: str,
        lease_owner: str,
        message: str,
        failure_type: str | None = None,
        stack_trace: str | None = None,
        non_retryable: bool = False,
        details: Any | None = None,
        codec: str = serializer.AVRO_CODEC,
        activity_name: str | None = None,
    ) -> Any:
        """Report a failed activity attempt.

        Pass ``non_retryable=True`` to signal that this class of error will
        not be fixed by retrying — the server then surfaces the failure to
        the workflow immediately instead of scheduling another attempt.
        """
        failure: dict[str, Any] = {"message": message}
        if failure_type is not None:
            failure["type"] = failure_type
        if stack_trace is not None:
            failure["stack_trace"] = stack_trace
        if non_retryable:
            failure["non_retryable"] = True
        if details is not None:
            failure["details"] = self._payload_envelope(
                details,
                codec=codec,
                kind="activity_failure_details",
                activity_name=activity_name,
            )
        body: dict[str, Any] = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "failure": failure,
        }
        return await self._request(
            "POST", f"/worker/activity-tasks/{task_id}/fail", worker=True, json=body
        )

    async def heartbeat_activity_task(
        self,
        *,
        task_id: str,
        activity_attempt_id: str,
        lease_owner: str,
        details: dict[str, Any] | None = None,
    ) -> Any:
        """Send a liveness heartbeat for a running activity attempt.

        Worker-plane endpoint. Most code calls
        :meth:`~durable_workflow.ActivityContext.heartbeat` instead, which
        additionally raises :class:`~durable_workflow.errors.ActivityCancelled`
        when the server reports the activity should stop.
        """
        body: dict[str, Any] = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
        }
        if details is not None:
            body["details"] = details
        return await self._request(
            "POST", f"/worker/activity-tasks/{task_id}/heartbeat", worker=True, json=body
        )
