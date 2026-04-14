from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from . import serializer
from .errors import (
    WorkflowCancelled,
    WorkflowFailed,
    WorkflowTerminated,
    _raise_for_status,
)

PROTOCOL_VERSION = "1.0"
CONTROL_PLANE_VERSION = "2"


@dataclass
class WorkflowExecution:
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
    executions: list[WorkflowExecution]
    next_page_token: str | None = None


class WorkflowHandle:
    def __init__(self, client: Client, workflow_id: str, run_id: str | None = None, workflow_type: str = "") -> None:
        self._client = client
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.workflow_type = workflow_type

    async def result(self, *, poll_interval: float = 0.5, timeout: float = 30.0) -> Any:
        return await self._client.get_result(self, poll_interval=poll_interval, timeout=timeout)

    async def describe(self) -> WorkflowExecution:
        return await self._client.describe_workflow(self.workflow_id)

    async def signal(self, signal_name: str, args: list[Any] | None = None) -> None:
        await self._client.signal_workflow(self.workflow_id, signal_name, args=args)

    async def query(self, query_name: str, args: list[Any] | None = None) -> Any:
        return await self._client.query_workflow(self.workflow_id, query_name, args=args)

    async def cancel(self, *, reason: str | None = None) -> None:
        await self._client.cancel_workflow(self.workflow_id, reason=reason)

    async def terminate(self, *, reason: str | None = None) -> None:
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
        return await self._client.update_workflow(
            self.workflow_id,
            update_name,
            args=args,
            wait_for=wait_for,
            wait_timeout_seconds=wait_timeout_seconds,
            request_id=request_id,
        )


class Client:
    """HTTP client for the Durable Workflow server."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        namespace: str = "default",
        timeout: float = 60.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.namespace = namespace
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    def _headers(self, *, worker: bool = False) -> dict[str, str]:
        h: dict[str, str] = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        h["X-Namespace"] = self.namespace
        if worker:
            h["X-Durable-Workflow-Protocol-Version"] = PROTOCOL_VERSION
        else:
            h["X-Durable-Workflow-Control-Plane-Version"] = CONTROL_PLANE_VERSION
        return h

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
        resp = await self._http.request(
            method,
            f"/api{path}",
            headers=self._headers(worker=worker),
            json=json,
            timeout=timeout,
        )
        if resp.status_code >= 400:
            try:
                body = resp.json()
            except ValueError:
                body = resp.text
            _raise_for_status(resp.status_code, body, context=context)
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    def get_workflow_handle(
        self, workflow_id: str, *, run_id: str | None = None, workflow_type: str = ""
    ) -> WorkflowHandle:
        return WorkflowHandle(self, workflow_id=workflow_id, run_id=run_id, workflow_type=workflow_type)

    # ── Health ─────────────────────────────────────────────────────────
    async def health(self) -> dict[str, Any]:
        resp = await self._http.get("/api/health")
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
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
        encoded_input = serializer.encode(input if input is not None else [])
        body: dict[str, Any] = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "task_queue": task_queue,
            "input": {"codec": "json", "blob": encoded_input},
            "execution_timeout_seconds": execution_timeout_seconds,
            "run_timeout_seconds": run_timeout_seconds,
        }
        if duplicate_policy is not None:
            body["duplicate_policy"] = duplicate_policy
        if memo is not None:
            body["memo"] = memo
        if search_attributes is not None:
            body["search_attributes"] = search_attributes
        data = await self._request("POST", "/workflows", json=body, context=workflow_id)
        return WorkflowHandle(
            self,
            workflow_id=data["workflow_id"],
            run_id=data.get("run_id"),
            workflow_type=data["workflow_type"],
        )

    async def describe_workflow(self, workflow_id: str) -> WorkflowExecution:
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
        return await self._request(
            "GET", f"/workflows/{workflow_id}/runs/{run_id}/history", context=workflow_id
        )

    async def signal_workflow(
        self, workflow_id: str, signal_name: str, *, args: list[Any] | None = None
    ) -> None:
        body: dict[str, Any] = {}
        if args:
            body["input"] = args
        await self._request("POST", f"/workflows/{workflow_id}/signal/{signal_name}", json=body, context=workflow_id)

    async def query_workflow(
        self, workflow_id: str, query_name: str, *, args: list[Any] | None = None
    ) -> Any:
        body: dict[str, Any] = {}
        if args:
            body["input"] = args
        return await self._request(
            "POST", f"/workflows/{workflow_id}/query/{query_name}", json=body, context=workflow_id
        )

    async def cancel_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        await self._request("POST", f"/workflows/{workflow_id}/cancel", json=body, context=workflow_id)

    async def terminate_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
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
        body: dict[str, Any] = {}
        if args:
            body["input"] = args
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
                    if etype in ("WorkflowCompleted", "workflow_completed"):
                        return serializer.decode(payload.get("result"))
                    if etype in ("WorkflowFailed", "workflow_failed"):
                        raise WorkflowFailed(
                            payload.get("message", "workflow failed"),
                            payload.get("exception_class"),
                        )
                    if etype in ("WorkflowTerminated", "workflow_terminated"):
                        raise WorkflowTerminated(
                            payload.get("reason", "workflow was terminated")
                        )
                    if etype in ("WorkflowCancelled", "workflow_cancelled", "WorkflowCanceled", "workflow_canceled"):
                        raise WorkflowCancelled(
                            payload.get("reason", "workflow was cancelled")
                        )
                return None
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(
                    f"workflow {handle.workflow_id} not terminal after {timeout}s (status={status})"
                )
            await asyncio.sleep(poll_interval)

    # ── Worker protocol ────────────────────────────────────────────────
    async def register_worker(
        self,
        *,
        worker_id: str,
        task_queue: str,
        supported_workflow_types: list[str] | None = None,
        supported_activity_types: list[str] | None = None,
        runtime: str = "python",
        sdk_version: str = "durable-workflow-python/0.1.0",
    ) -> Any:
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "task_queue": task_queue,
            "runtime": runtime,
            "sdk_version": sdk_version,
            "supported_workflow_types": supported_workflow_types or [],
            "supported_activity_types": supported_activity_types or [],
        }
        return await self._request("POST", "/worker/register", worker=True, json=body)

    async def poll_workflow_task(
        self, *, worker_id: str, task_queue: str, timeout: float = 35.0
    ) -> Any:
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

    async def poll_activity_task(
        self, *, worker_id: str, task_queue: str, timeout: float = 35.0
    ) -> Any:
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
    ) -> Any:
        body: dict[str, Any] = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "result": serializer.envelope(result),
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
    ) -> Any:
        failure: dict[str, Any] = {"message": message}
        if failure_type is not None:
            failure["type"] = failure_type
        if stack_trace is not None:
            failure["stack_trace"] = stack_trace
        if non_retryable:
            failure["non_retryable"] = True
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
        body: dict[str, Any] = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
        }
        if details is not None:
            body["details"] = details
        return await self._request(
            "POST", f"/worker/activity-tasks/{task_id}/heartbeat", worker=True, json=body
        )
