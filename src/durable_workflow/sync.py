"""Synchronous facade over the async Client, for scripts and Jupyter."""
from __future__ import annotations

import asyncio
from typing import Any

from .client import Client as AsyncClient
from .client import WorkflowExecution, WorkflowHandle, WorkflowList


def _run(coro: Any) -> Any:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None:
        raise RuntimeError(
            "durable_workflow.sync.Client cannot be used inside an already-running "
            "event loop. Use the async durable_workflow.Client instead."
        )
    return asyncio.run(coro)


class SyncWorkflowHandle:
    def __init__(self, async_handle: WorkflowHandle) -> None:
        self._handle = async_handle
        self.workflow_id = async_handle.workflow_id
        self.run_id = async_handle.run_id
        self.workflow_type = async_handle.workflow_type

    def result(self, *, poll_interval: float = 0.5, timeout: float = 30.0) -> Any:
        return _run(self._handle.result(poll_interval=poll_interval, timeout=timeout))

    def describe(self) -> WorkflowExecution:
        result: WorkflowExecution = _run(self._handle.describe())
        return result

    def signal(self, signal_name: str, args: list[Any] | None = None) -> None:
        _run(self._handle.signal(signal_name, args=args))

    def query(self, query_name: str, args: list[Any] | None = None) -> Any:
        return _run(self._handle.query(query_name, args=args))

    def cancel(self, *, reason: str | None = None) -> None:
        _run(self._handle.cancel(reason=reason))

    def terminate(self, *, reason: str | None = None) -> None:
        _run(self._handle.terminate(reason=reason))


class Client:
    """Blocking wrapper around the async Client. Each call opens and closes its own event loop."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        namespace: str = "default",
        timeout: float = 60.0,
    ) -> None:
        self._async = AsyncClient(base_url, token=token, namespace=namespace, timeout=timeout)

    def close(self) -> None:
        _run(self._async.aclose())

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def health(self) -> dict[str, Any]:
        result: dict[str, Any] = _run(self._async.health())
        return result

    def start_workflow(
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
    ) -> SyncWorkflowHandle:
        handle = _run(self._async.start_workflow(
            workflow_type=workflow_type,
            task_queue=task_queue,
            workflow_id=workflow_id,
            input=input,
            execution_timeout_seconds=execution_timeout_seconds,
            run_timeout_seconds=run_timeout_seconds,
            duplicate_policy=duplicate_policy,
            memo=memo,
            search_attributes=search_attributes,
        ))
        return SyncWorkflowHandle(handle)

    def describe_workflow(self, workflow_id: str) -> WorkflowExecution:
        result: WorkflowExecution = _run(self._async.describe_workflow(workflow_id))
        return result

    def list_workflows(
        self,
        *,
        workflow_type: str | None = None,
        status: str | None = None,
        query: str | None = None,
        page_size: int | None = None,
        next_page_token: str | None = None,
    ) -> WorkflowList:
        result: WorkflowList = _run(self._async.list_workflows(
            workflow_type=workflow_type,
            status=status,
            query=query,
            page_size=page_size,
            next_page_token=next_page_token,
        ))
        return result

    def get_history(self, workflow_id: str, run_id: str) -> Any:
        return _run(self._async.get_history(workflow_id, run_id))

    def signal_workflow(
        self, workflow_id: str, signal_name: str, *, args: list[Any] | None = None
    ) -> None:
        _run(self._async.signal_workflow(workflow_id, signal_name, args=args))

    def query_workflow(
        self, workflow_id: str, query_name: str, *, args: list[Any] | None = None
    ) -> Any:
        return _run(self._async.query_workflow(workflow_id, query_name, args=args))

    def cancel_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        _run(self._async.cancel_workflow(workflow_id, reason=reason))

    def terminate_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        _run(self._async.terminate_workflow(workflow_id, reason=reason))

    def get_result(
        self,
        handle: SyncWorkflowHandle,
        *,
        poll_interval: float = 0.5,
        timeout: float = 30.0,
    ) -> Any:
        return _run(self._async.get_result(
            handle._handle, poll_interval=poll_interval, timeout=timeout
        ))
