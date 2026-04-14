from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from . import serializer
from .errors import ServerError, WorkflowFailed

PROTOCOL_VERSION = "1.0"
CONTROL_PLANE_VERSION = "2"


@dataclass
class WorkflowHandle:
    workflow_id: str
    run_id: str | None
    workflow_type: str


class Client:
    """HTTP client for the Durable Workflow server.

    Wraps the worker and control-plane endpoints. Methods map 1:1 to the
    server's REST routes; parameters are JSON-serializable primitives.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        namespace: str = "default",
        timeout: float = 60.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.namespace = namespace
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
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
        self, method: str, path: str, *, worker: bool = False, json: Any = None, timeout: float | None = None,
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
            raise ServerError(resp.status_code, body)
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()

    # ── Health ─────────────────────────────────────────────────────────
    async def health(self) -> dict:
        resp = await self._http.get("/api/health")
        resp.raise_for_status()
        return resp.json()

    # ── Workflows ──────────────────────────────────────────────────────
    async def start_workflow(
        self,
        *,
        workflow_type: str,
        task_queue: str,
        workflow_id: str,
        input: list | None = None,
        execution_timeout_seconds: int = 3600,
        run_timeout_seconds: int = 600,
    ) -> WorkflowHandle:
        body = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "task_queue": task_queue,
            "input": input or [],
            "execution_timeout_seconds": execution_timeout_seconds,
            "run_timeout_seconds": run_timeout_seconds,
        }
        data = await self._request("POST", "/workflows", json=body)
        return WorkflowHandle(
            workflow_id=data["workflow_id"],
            run_id=data.get("run_id"),
            workflow_type=data["workflow_type"],
        )

    async def describe_workflow(self, workflow_id: str) -> dict:
        return await self._request("GET", f"/workflows/{workflow_id}")

    async def get_history(self, workflow_id: str, run_id: str) -> dict:
        return await self._request(
            "GET", f"/workflows/{workflow_id}/runs/{run_id}/history"
        )

    async def get_result(
        self,
        handle: WorkflowHandle,
        *,
        poll_interval: float = 0.5,
        timeout: float = 30.0,
    ) -> Any:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            data = await self.describe_workflow(handle.workflow_id)
            status = data.get("status") or data.get("run_status")
            if status in ("completed", "failed", "terminated", "canceled"):
                # Pull the terminal result from the history (server stores the
                # completion command on the last WorkflowCompleted event).
                run_id = handle.run_id or data.get("run_id")
                history = await self.get_history(handle.workflow_id, run_id)
                events = history.get("events") or history.get("history_events") or []
                for ev in reversed(events):
                    etype = ev.get("event_type") or ev.get("type")
                    details = ev.get("details") or {}
                    if etype in ("WorkflowCompleted", "workflow_completed"):
                        return serializer.decode(
                            details.get("result")
                            or ev.get("result")
                        )
                    if etype in ("WorkflowFailed", "workflow_failed"):
                        raise WorkflowFailed(
                            details.get("message") or ev.get("message", "workflow failed"),
                            details.get("exception_class") or ev.get("exception_class"),
                        )
                return None
            if asyncio.get_event_loop().time() > deadline:
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
    ) -> dict:
        body = {
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
    ) -> dict | None:
        body = {"worker_id": worker_id, "task_queue": task_queue}
        data = await self._request(
            "POST", "/worker/workflow-tasks/poll", worker=True, json=body, timeout=timeout
        )
        return (data or {}).get("task")

    async def complete_workflow_task(
        self,
        *,
        task_id: str,
        lease_owner: str,
        workflow_task_attempt: int,
        commands: list[dict],
    ) -> dict:
        body = {
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "commands": commands,
        }
        return await self._request(
            "POST", f"/worker/workflow-tasks/{task_id}/complete", worker=True, json=body
        )

    async def fail_workflow_task(
        self, *, task_id: str, lease_owner: str, workflow_task_attempt: int, message: str
    ) -> dict:
        body = {
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "message": message,
        }
        return await self._request(
            "POST", f"/worker/workflow-tasks/{task_id}/fail", worker=True, json=body
        )

    async def poll_activity_task(
        self, *, worker_id: str, task_queue: str, timeout: float = 35.0
    ) -> dict | None:
        body = {"worker_id": worker_id, "task_queue": task_queue}
        data = await self._request(
            "POST", "/worker/activity-tasks/poll", worker=True, json=body, timeout=timeout
        )
        return (data or {}).get("task")

    async def complete_activity_task(
        self,
        *,
        task_id: str,
        activity_attempt_id: str,
        lease_owner: str,
        result: Any,
    ) -> dict:
        body = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "result": serializer.encode(result),
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
    ) -> dict:
        body = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "message": message,
        }
        return await self._request(
            "POST", f"/worker/activity-tasks/{task_id}/fail", worker=True, json=body
        )
