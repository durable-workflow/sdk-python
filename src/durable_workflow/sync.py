"""Synchronous facade over the async Client, for scripts and Jupyter."""

from __future__ import annotations

import asyncio
from typing import Any

from .client import Client as AsyncClient
from .client import (
    NamespaceDescription,
    NamespaceList,
    ScheduleAction,
    ScheduleBackfillResult,
    ScheduleDescription,
    ScheduleHandle,
    ScheduleList,
    ScheduleSpec,
    ScheduleTriggerResult,
    StorageTestResult,
    TaskQueueDescription,
    TaskQueueList,
    WorkflowCommandResult,
    WorkflowExecution,
    WorkflowHandle,
    WorkflowList,
    WorkflowRun,
    WorkflowRunList,
)
from .metrics import MetricsRecorder
from .retry_policy import TransportRetryPolicy


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
    """Blocking wrapper around an async workflow handle."""

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

    def get_history(self) -> Any:
        return _run(self._handle.get_history())

    def export_history(self) -> Any:
        return _run(self._handle.export_history())

    def list_runs(self) -> WorkflowRunList:
        result: WorkflowRunList = _run(self._handle.list_runs())
        return result

    def describe_run(self, run_id: str | None = None) -> WorkflowRun:
        result: WorkflowRun = _run(self._handle.describe_run(run_id))
        return result

    def signal(self, signal_name: str, args: list[Any] | None = None) -> None:
        _run(self._handle.signal(signal_name, args=args))

    def query(self, query_name: str, args: list[Any] | None = None) -> Any:
        return _run(self._handle.query(query_name, args=args))

    def cancel(self, *, reason: str | None = None) -> None:
        _run(self._handle.cancel(reason=reason))

    def terminate(self, *, reason: str | None = None) -> None:
        _run(self._handle.terminate(reason=reason))

    def repair(self) -> WorkflowCommandResult:
        result: WorkflowCommandResult = _run(self._handle.repair())
        return result

    def archive(self, *, reason: str | None = None) -> WorkflowCommandResult:
        result: WorkflowCommandResult = _run(self._handle.archive(reason=reason))
        return result

    def update(
        self,
        update_name: str,
        args: list[Any] | None = None,
        *,
        wait_for: str | None = None,
        wait_timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> Any:
        return _run(
            self._handle.update(
                update_name,
                args=args,
                wait_for=wait_for,
                wait_timeout_seconds=wait_timeout_seconds,
                request_id=request_id,
            )
        )


class SyncScheduleHandle:
    """Blocking wrapper around an async schedule handle."""

    def __init__(self, async_handle: ScheduleHandle) -> None:
        self._handle = async_handle
        self.schedule_id = async_handle.schedule_id

    def describe(self) -> ScheduleDescription:
        result: ScheduleDescription = _run(self._handle.describe())
        return result

    def update(
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
        _run(
            self._handle.update(
                spec=spec,
                action=action,
                overlap_policy=overlap_policy,
                jitter_seconds=jitter_seconds,
                max_runs=max_runs,
                memo=memo,
                search_attributes=search_attributes,
                note=note,
            )
        )

    def pause(self, *, note: str | None = None) -> None:
        _run(self._handle.pause(note=note))

    def resume(self, *, note: str | None = None) -> None:
        _run(self._handle.resume(note=note))

    def trigger(self, *, overlap_policy: str | None = None) -> ScheduleTriggerResult:
        result: ScheduleTriggerResult = _run(self._handle.trigger(overlap_policy=overlap_policy))
        return result

    def delete(self) -> None:
        _run(self._handle.delete())

    def backfill(
        self,
        *,
        start_time: str,
        end_time: str,
        overlap_policy: str | None = None,
    ) -> ScheduleBackfillResult:
        result: ScheduleBackfillResult = _run(
            self._handle.backfill(
                start_time=start_time,
                end_time=end_time,
                overlap_policy=overlap_policy,
            )
        )
        return result


class Client:
    """Blocking wrapper around the async client.

    Each call opens and closes its own event loop, so this facade is best for
    scripts, notebooks, and command-line tools rather than high-throughput
    services.
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
    ) -> None:
        self._async = AsyncClient(
            base_url,
            token=token,
            control_token=control_token,
            worker_token=worker_token,
            namespace=namespace,
            timeout=timeout,
            retry_policy=retry_policy,
            metrics=metrics,
        )

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
        handle = _run(
            self._async.start_workflow(
                workflow_type=workflow_type,
                task_queue=task_queue,
                workflow_id=workflow_id,
                input=input,
                execution_timeout_seconds=execution_timeout_seconds,
                run_timeout_seconds=run_timeout_seconds,
                duplicate_policy=duplicate_policy,
                memo=memo,
                search_attributes=search_attributes,
            )
        )
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
        result: WorkflowList = _run(
            self._async.list_workflows(
                workflow_type=workflow_type,
                status=status,
                query=query,
                page_size=page_size,
                next_page_token=next_page_token,
            )
        )
        return result

    def list_task_queues(self) -> TaskQueueList:
        result: TaskQueueList = _run(self._async.list_task_queues())
        return result

    def describe_task_queue(self, name: str) -> TaskQueueDescription:
        result: TaskQueueDescription = _run(self._async.describe_task_queue(name))
        return result

    def list_namespaces(self) -> NamespaceList:
        result: NamespaceList = _run(self._async.list_namespaces())
        return result

    def describe_namespace(self, name: str) -> NamespaceDescription:
        result: NamespaceDescription = _run(self._async.describe_namespace(name))
        return result

    def create_namespace(
        self,
        name: str,
        *,
        description: str | None = None,
        retention_days: int = 30,
    ) -> NamespaceDescription:
        result: NamespaceDescription = _run(
            self._async.create_namespace(
                name,
                description=description,
                retention_days=retention_days,
            )
        )
        return result

    def update_namespace(
        self,
        name: str,
        *,
        description: str | None = None,
        retention_days: int | None = None,
    ) -> NamespaceDescription:
        result: NamespaceDescription = _run(
            self._async.update_namespace(
                name,
                description=description,
                retention_days=retention_days,
            )
        )
        return result

    def set_namespace_external_storage(
        self,
        namespace: str,
        *,
        driver: str,
        enabled: bool = True,
        threshold_bytes: int | None = None,
        config: dict[str, Any] | None = None,
    ) -> NamespaceDescription:
        result: NamespaceDescription = _run(
            self._async.set_namespace_external_storage(
                namespace,
                driver=driver,
                enabled=enabled,
                threshold_bytes=threshold_bytes,
                config=config,
            )
        )
        return result

    def test_external_storage(
        self,
        *,
        driver: str | None = None,
        small_payload_bytes: int | None = None,
        large_payload_bytes: int | None = None,
    ) -> StorageTestResult:
        result: StorageTestResult = _run(
            self._async.test_external_storage(
                driver=driver,
                small_payload_bytes=small_payload_bytes,
                large_payload_bytes=large_payload_bytes,
            )
        )
        return result

    def get_history(self, workflow_id: str, run_id: str) -> Any:
        return _run(self._async.get_history(workflow_id, run_id))

    def export_history(self, workflow_id: str, run_id: str) -> Any:
        return _run(self._async.export_history(workflow_id, run_id))

    def list_workflow_runs(self, workflow_id: str) -> WorkflowRunList:
        result: WorkflowRunList = _run(self._async.list_workflow_runs(workflow_id))
        return result

    def describe_workflow_run(self, workflow_id: str, run_id: str) -> WorkflowRun:
        result: WorkflowRun = _run(self._async.describe_workflow_run(workflow_id, run_id))
        return result

    def signal_workflow(self, workflow_id: str, signal_name: str, *, args: list[Any] | None = None) -> None:
        _run(self._async.signal_workflow(workflow_id, signal_name, args=args))

    def query_workflow(self, workflow_id: str, query_name: str, *, args: list[Any] | None = None) -> Any:
        return _run(self._async.query_workflow(workflow_id, query_name, args=args))

    def cancel_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        _run(self._async.cancel_workflow(workflow_id, reason=reason))

    def terminate_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        _run(self._async.terminate_workflow(workflow_id, reason=reason))

    def repair_workflow(self, workflow_id: str) -> WorkflowCommandResult:
        result: WorkflowCommandResult = _run(self._async.repair_workflow(workflow_id))
        return result

    def archive_workflow(self, workflow_id: str, *, reason: str | None = None) -> WorkflowCommandResult:
        result: WorkflowCommandResult = _run(self._async.archive_workflow(workflow_id, reason=reason))
        return result

    def update_workflow(
        self,
        workflow_id: str,
        update_name: str,
        *,
        args: list[Any] | None = None,
        wait_for: str | None = None,
        wait_timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> Any:
        return _run(
            self._async.update_workflow(
                workflow_id,
                update_name,
                args=args,
                wait_for=wait_for,
                wait_timeout_seconds=wait_timeout_seconds,
                request_id=request_id,
            )
        )

    def get_result(
        self,
        handle: SyncWorkflowHandle,
        *,
        poll_interval: float = 0.5,
        timeout: float = 30.0,
    ) -> Any:
        return _run(self._async.get_result(handle._handle, poll_interval=poll_interval, timeout=timeout))

    # ── Schedules ─────────────────────────────────────────────────────
    def get_schedule_handle(self, schedule_id: str) -> SyncScheduleHandle:
        return SyncScheduleHandle(self._async.get_schedule_handle(schedule_id))

    def create_schedule(
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
    ) -> SyncScheduleHandle:
        handle = _run(
            self._async.create_schedule(
                schedule_id=schedule_id,
                spec=spec,
                action=action,
                overlap_policy=overlap_policy,
                jitter_seconds=jitter_seconds,
                max_runs=max_runs,
                memo=memo,
                search_attributes=search_attributes,
                paused=paused,
                note=note,
            )
        )
        return SyncScheduleHandle(handle)

    def list_schedules(self) -> ScheduleList:
        result: ScheduleList = _run(self._async.list_schedules())
        return result

    def describe_schedule(self, schedule_id: str) -> ScheduleDescription:
        result: ScheduleDescription = _run(self._async.describe_schedule(schedule_id))
        return result

    def update_schedule(
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
        _run(
            self._async.update_schedule(
                schedule_id,
                spec=spec,
                action=action,
                overlap_policy=overlap_policy,
                jitter_seconds=jitter_seconds,
                max_runs=max_runs,
                memo=memo,
                search_attributes=search_attributes,
                note=note,
            )
        )

    def pause_schedule(self, schedule_id: str, *, note: str | None = None) -> None:
        _run(self._async.pause_schedule(schedule_id, note=note))

    def resume_schedule(self, schedule_id: str, *, note: str | None = None) -> None:
        _run(self._async.resume_schedule(schedule_id, note=note))

    def trigger_schedule(self, schedule_id: str, *, overlap_policy: str | None = None) -> ScheduleTriggerResult:
        result: ScheduleTriggerResult = _run(self._async.trigger_schedule(schedule_id, overlap_policy=overlap_policy))
        return result

    def delete_schedule(self, schedule_id: str) -> None:
        _run(self._async.delete_schedule(schedule_id))

    def backfill_schedule(
        self,
        schedule_id: str,
        *,
        start_time: str,
        end_time: str,
        overlap_policy: str | None = None,
    ) -> ScheduleBackfillResult:
        result: ScheduleBackfillResult = _run(
            self._async.backfill_schedule(
                schedule_id,
                start_time=start_time,
                end_time=end_time,
                overlap_policy=overlap_policy,
            )
        )
        return result
