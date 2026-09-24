"""Qualify Python local activity recording and cold replay against Server."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from durable_workflow import Client, Worker, activity, workflow
from durable_workflow.client import PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST
from durable_workflow.errors import NonRetryableError, WorkflowCancelled, WorkflowFailed
from durable_workflow.workflow import replay


@workflow.defn(name="tests.python-local-activity")
class LocalActivityWorkflow:
    def run(self, ctx: Any, name: str) -> Any:
        result = yield ctx.local_activity("tests.python-local-greet", [name])
        return {"greeting": result}


_executions = 0


@activity.defn(name="tests.python-local-greet")
async def local_greet(name: str) -> str:
    global _executions
    _executions += 1
    await activity.context().heartbeat({"phase": "greeting"})
    return f"hello, {name}"


@workflow.defn(name="tests.python-local-restart")
class LocalRestartWorkflow:
    def __init__(self) -> None:
        self.finished = False

    @workflow.signal("finish")
    def finish(self) -> None:
        self.finished = True

    def run(self, ctx: Any, name: str) -> Any:
        greeting = yield ctx.local_activity("tests.python-local-greet", [name])
        yield ctx.wait_condition(lambda: self.finished, key="local-activity-finished")
        return {"greeting": greeting}


@workflow.defn(name="tests.python-local-retry")
class LocalRetryWorkflow:
    def run(self, ctx: Any, name: str) -> Any:
        result = yield ctx.local_activity(
            "tests.python-local-retry-greet",
            [name],
            retry_policy={"max_attempts": 2, "backoff_seconds": [0]},
        )
        return {"greeting": result}


_retry_executions = 0


@activity.defn(name="tests.python-local-retry-greet")
async def local_retry_greet(name: str) -> str:
    global _retry_executions
    _retry_executions += 1
    if name == "Permanent":
        raise NonRetryableError("permanent")
    if _retry_executions == 1:
        raise RuntimeError("transient")
    return f"hello, {name}"


@workflow.defn(name="tests.python-local-timeout")
class LocalTimeoutWorkflow:
    def run(self, ctx: Any, name: str, timeout_kind: str) -> Any:
        result = yield ctx.local_activity(
            "tests.python-local-slow-greet",
            [name],
            **{f"{timeout_kind}_timeout": 1},
        )
        return {"greeting": result}


_slow_executions = 0


@activity.defn(name="tests.python-local-slow-greet")
async def local_slow_greet(name: str) -> str:
    global _slow_executions
    _slow_executions += 1
    await activity.context().heartbeat({"phase": "before-wait"})
    await asyncio.sleep(1.2)
    return f"hello, {name}"


@pytest.mark.asyncio
async def test_local_activity_completion_survives_cold_replay(
    server_url: str,
    server_token: str,
) -> None:
    global _executions
    _executions = 0
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-local-{suffix}"
    workflow_id = f"py-local-{suffix}"
    manifest = {
        **PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
        "local_activities": {
            "supported": True,
            "minimum_protocol_version": "1.18",
            "implementation": "record_local_activity",
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[LocalActivityWorkflow],
            activities=[local_greet],
            worker_id=f"py-local-worker-{suffix}",
        )
        await client.register_worker(
            worker_id=worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(worker.workflows),
            supported_activity_types=list(worker.activities),
            workflow_definition_fingerprints=worker.workflow_definition_fingerprints,
            workflow_command_contracts=worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-local-activity",
                task_queue=queue,
                workflow_id=workflow_id,
                input=["Ada"],
            )
            task = await client.poll_workflow_task(
                worker_id=worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert task is not None
            commands = await worker._run_workflow_task(task)
            assert commands is not None
            assert [command["type"] for command in commands] == [
                "record_local_activity",
                "complete_workflow",
            ]
            assert _executions == 1
            assert await handle.result(timeout=10.0) == {"greeting": "hello, Ada"}

            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            event_types = [event["event_type"] for event in events]
            assert "ActivityScheduled" in event_types
            assert "ActivityStarted" in event_types
            assert "ActivityHeartbeatRecorded" in event_types
            assert "ActivityCompleted" in event_types
            assert "WorkflowCompleted" in event_types

            outcome = replay(
                LocalActivityWorkflow,
                events,
                ["Ada"],
                workflow_id=workflow_id,
                run_id=handle.run_id or "",
            )
            assert [command.__class__.__name__ for command in outcome.commands] == ["CompleteWorkflow"]
            assert _executions == 1
        finally:
            await client.deregister_worker_registration(worker.worker_id)


@pytest.mark.asyncio
async def test_local_activity_completion_survives_lost_acknowledgement(
    server_url: str,
    server_token: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    global _executions
    _executions = 0
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-local-uncertain-{suffix}"
    workflow_id = f"py-local-uncertain-{suffix}"
    manifest = {
        **PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
        "local_activities": {
            "supported": True,
            "minimum_protocol_version": "1.18",
            "implementation": "record_local_activity",
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[LocalActivityWorkflow],
            activities=[local_greet],
            worker_id=f"py-local-uncertain-worker-{suffix}",
        )
        await client.register_worker(
            worker_id=worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(worker.workflows),
            supported_activity_types=list(worker.activities),
            workflow_definition_fingerprints=worker.workflow_definition_fingerprints,
            workflow_command_contracts=worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-local-activity",
                task_queue=queue,
                workflow_id=workflow_id,
                input=["Ada"],
            )
            task = await client.poll_workflow_task(
                worker_id=worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert task is not None

            original_complete = client.complete_workflow_task
            complete_attempts = 0
            fail_attempts = 0

            async def lose_first_acknowledgement(*args: Any, **kwargs: Any) -> Any:
                nonlocal complete_attempts
                complete_attempts += 1
                response = await original_complete(*args, **kwargs)
                if complete_attempts == 1:
                    raise TimeoutError("completion response lost after Server committed it")
                return response

            original_fail = client.fail_workflow_task

            async def record_failure(*args: Any, **kwargs: Any) -> Any:
                nonlocal fail_attempts
                fail_attempts += 1
                return await original_fail(*args, **kwargs)

            monkeypatch.setattr(client, "complete_workflow_task", lose_first_acknowledgement)
            monkeypatch.setattr(client, "fail_workflow_task", record_failure)
            commands = await worker._run_workflow_task(task)
            assert commands is not None
            assert [command["type"] for command in commands] == [
                "record_local_activity",
                "complete_workflow",
            ]
            assert complete_attempts == 2
            assert fail_attempts == 0
            assert await handle.result(timeout=10.0) == {"greeting": "hello, Ada"}
            assert _executions == 1

            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            event_types = [event["event_type"] for event in events]
            assert event_types.count("ActivityCompleted") == 1
            assert event_types.count("WorkflowCompleted") == 1
            assert event_types.count("WorkflowTaskFailed") == 0
        finally:
            await client.deregister_worker_registration(worker.worker_id)


@pytest.mark.asyncio
async def test_cancelled_run_fences_in_flight_local_activity(
    server_url: str,
    server_token: str,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-local-cancel-{suffix}"
    workflow_id = f"py-local-cancel-{suffix}"
    entered = asyncio.Event()
    release = asyncio.Event()
    manifest = {
        **PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
        "local_activities": {
            "supported": True,
            "minimum_protocol_version": "1.18",
            "implementation": "record_local_activity",
        },
    }

    async def blocked_greet(name: str) -> str:
        entered.set()
        await release.wait()
        await activity.context().heartbeat({"phase": "after-cancel"})
        return f"hello, {name}"

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[LocalActivityWorkflow],
            activities=[local_greet],
            worker_id=f"py-local-cancel-worker-{suffix}",
        )
        worker.activities["tests.python-local-greet"] = blocked_greet
        await client.register_worker(
            worker_id=worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(worker.workflows),
            supported_activity_types=list(worker.activities),
            workflow_definition_fingerprints=worker.workflow_definition_fingerprints,
            workflow_command_contracts=worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-local-activity",
                task_queue=queue,
                workflow_id=workflow_id,
                input=["Ada"],
            )
            task = await client.poll_workflow_task(
                worker_id=worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert task is not None
            execution = asyncio.create_task(worker._run_workflow_task(task))
            try:
                await asyncio.wait_for(entered.wait(), timeout=10.0)
                await handle.cancel(reason="cancel during local activity")
            finally:
                release.set()
            assert await asyncio.wait_for(execution, timeout=10.0) is None
            with pytest.raises(WorkflowCancelled):
                await handle.result(timeout=10.0)

            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            event_types = [event["event_type"] for event in events]
            assert event_types.count("WorkflowCancelled") == 1
            assert event_types.count("ActivityCompleted") == 0
        finally:
            await client.deregister_worker_registration(worker.worker_id)


@pytest.mark.asyncio
async def test_replacement_worker_replays_local_activity_before_signal(
    server_url: str,
    server_token: str,
) -> None:
    global _executions
    _executions = 0
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-local-restart-{suffix}"
    workflow_id = f"py-local-restart-{suffix}"
    manifest = {
        **PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
        "local_activities": {
            "supported": True,
            "minimum_protocol_version": "1.18",
            "implementation": "record_local_activity",
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as first_client:
        first_worker = Worker(
            first_client,
            task_queue=queue,
            workflows=[LocalRestartWorkflow],
            activities=[local_greet],
            worker_id=f"py-local-before-{suffix}",
        )
        await first_client.register_worker(
            worker_id=first_worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(first_worker.workflows),
            supported_activity_types=list(first_worker.activities),
            workflow_definition_fingerprints=first_worker.workflow_definition_fingerprints,
            workflow_command_contracts=first_worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = await first_client.start_workflow(
                workflow_type="tests.python-local-restart",
                task_queue=queue,
                workflow_id=workflow_id,
                input=["Ada"],
            )
            first_run_id = handle.run_id
            assert first_run_id is not None
            first_task = await first_client.poll_workflow_task(
                worker_id=first_worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert first_task is not None
            commands = await first_worker._run_workflow_task(first_task)
            assert commands is not None
            assert [command["type"] for command in commands] == [
                "record_local_activity",
                "open_condition_wait",
            ]
            assert _executions == 1
            assert (await handle.describe()).status.lower() == "waiting"
        finally:
            await first_client.deregister_worker_registration(first_worker.worker_id)

    async with Client(server_url, token=server_token, namespace="default") as replacement_client:
        replacement_worker = Worker(
            replacement_client,
            task_queue=queue,
            workflows=[LocalRestartWorkflow],
            activities=[local_greet],
            worker_id=f"py-local-after-{suffix}",
        )
        await replacement_client.register_worker(
            worker_id=replacement_worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(replacement_worker.workflows),
            supported_activity_types=list(replacement_worker.activities),
            workflow_definition_fingerprints=replacement_worker.workflow_definition_fingerprints,
            workflow_command_contracts=replacement_worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = replacement_client.get_workflow_handle(workflow_id, run_id=first_run_id)
            await handle.signal("finish")
            replacement_task = await replacement_client.poll_workflow_task(
                worker_id=replacement_worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert replacement_task is not None
            commands = await replacement_worker._run_workflow_task(replacement_task)
            assert commands is not None
            assert [command["type"] for command in commands] == ["complete_workflow"]
            assert await handle.result(timeout=10.0) == {"greeting": "hello, Ada"}
            assert _executions == 1

            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            event_types = [event["event_type"] for event in events]
            assert event_types.count("ActivityCompleted") == 1
            assert event_types.count("WorkflowCompleted") == 1
        finally:
            await replacement_client.deregister_worker_registration(replacement_worker.worker_id)


@pytest.mark.asyncio
async def test_local_activity_terminal_failure_is_recorded_and_replayed(
    server_url: str,
    server_token: str,
) -> None:
    global _retry_executions
    _retry_executions = 0
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-local-failure-{suffix}"
    workflow_id = f"py-local-failure-{suffix}"
    manifest = {
        **PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
        "local_activities": {
            "supported": True,
            "minimum_protocol_version": "1.18",
            "implementation": "record_local_activity",
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[LocalRetryWorkflow],
            activities=[local_retry_greet],
            worker_id=f"py-local-failure-worker-{suffix}",
        )
        await client.register_worker(
            worker_id=worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(worker.workflows),
            supported_activity_types=list(worker.activities),
            workflow_definition_fingerprints=worker.workflow_definition_fingerprints,
            workflow_command_contracts=worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-local-retry",
                task_queue=queue,
                workflow_id=workflow_id,
                input=["Permanent"],
            )
            task = await client.poll_workflow_task(
                worker_id=worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert task is not None
            commands = await worker._run_workflow_task(task)
            assert commands is not None
            assert [command["type"] for command in commands] == ["record_local_activity", "fail_workflow"]
            assert commands[0]["outcome"] == "failed"
            assert commands[0]["non_retryable"] is True
            assert len(commands[0]["attempts"]) == 1
            assert _retry_executions == 1
            with pytest.raises(WorkflowFailed):
                await handle.result(timeout=10.0)

            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            event_types = [event["event_type"] for event in events]
            assert event_types.count("ActivityFailed") == 1
            assert event_types.count("WorkflowFailed") == 1

            outcome = replay(
                LocalRetryWorkflow,
                events,
                ["Permanent"],
                workflow_id=workflow_id,
                run_id=handle.run_id or "",
            )
            assert [command.__class__.__name__ for command in outcome.commands] == ["FailWorkflow"]
            assert _retry_executions == 1
        finally:
            await client.deregister_worker_registration(worker.worker_id)


@pytest.mark.asyncio
async def test_local_activity_retries_commit_one_terminal_record_and_replay(
    server_url: str,
    server_token: str,
) -> None:
    global _retry_executions
    _retry_executions = 0
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-local-retry-{suffix}"
    workflow_id = f"py-local-retry-{suffix}"
    manifest = {
        **PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
        "local_activities": {
            "supported": True,
            "minimum_protocol_version": "1.18",
            "implementation": "record_local_activity",
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[LocalRetryWorkflow],
            activities=[local_retry_greet],
            worker_id=f"py-local-retry-worker-{suffix}",
        )
        await client.register_worker(
            worker_id=worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(worker.workflows),
            supported_activity_types=list(worker.activities),
            workflow_definition_fingerprints=worker.workflow_definition_fingerprints,
            workflow_command_contracts=worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-local-retry",
                task_queue=queue,
                workflow_id=workflow_id,
                input=["Ada"],
            )
            task = await client.poll_workflow_task(
                worker_id=worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert task is not None
            commands = await worker._run_workflow_task(task)
            assert commands is not None
            assert [command["type"] for command in commands] == [
                "record_local_activity",
                "complete_workflow",
            ]
            assert [attempt["outcome"] for attempt in commands[0]["attempts"]] == ["failed", "completed"]
            assert commands[0]["attempts"][0]["retry_reason"] == "failure"
            assert _retry_executions == 2
            assert await handle.result(timeout=10.0) == {"greeting": "hello, Ada"}

            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            event_types = [event["event_type"] for event in events]
            assert event_types.count("ActivityCompleted") == 1
            assert event_types.count("WorkflowCompleted") == 1

            outcome = replay(
                LocalRetryWorkflow,
                events,
                ["Ada"],
                workflow_id=workflow_id,
                run_id=handle.run_id or "",
            )
            assert [command.__class__.__name__ for command in outcome.commands] == ["CompleteWorkflow"]
            assert _retry_executions == 2
        finally:
            await client.deregister_worker_registration(worker.worker_id)


@pytest.mark.parametrize("timeout_kind", ["start_to_close", "heartbeat", "schedule_to_close"])
@pytest.mark.asyncio
async def test_local_activity_timeout_is_recorded_and_replayed(
    server_url: str,
    server_token: str,
    timeout_kind: str,
) -> None:
    global _slow_executions
    _slow_executions = 0
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-local-{timeout_kind}-{suffix}"
    workflow_id = f"py-local-{timeout_kind}-{suffix}"
    manifest = {
        **PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
        "local_activities": {
            "supported": True,
            "minimum_protocol_version": "1.18",
            "implementation": "record_local_activity",
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[LocalTimeoutWorkflow],
            activities=[local_slow_greet],
            worker_id=f"py-local-timeout-worker-{suffix}",
        )
        await client.register_worker(
            worker_id=worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(worker.workflows),
            supported_activity_types=list(worker.activities),
            workflow_definition_fingerprints=worker.workflow_definition_fingerprints,
            workflow_command_contracts=worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-local-timeout",
                task_queue=queue,
                workflow_id=workflow_id,
                input=["Ada", timeout_kind],
            )
            task = await client.poll_workflow_task(
                worker_id=worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert task is not None
            commands = await worker._run_workflow_task(task)
            assert commands is not None
            assert [command["type"] for command in commands] == ["record_local_activity", "fail_workflow"]
            assert commands[0]["outcome"] == "timed_out"
            assert commands[0]["timeout_kind"] == timeout_kind
            assert len(commands[0]["attempts"]) == 1
            assert _slow_executions == 1
            with pytest.raises(WorkflowFailed):
                await handle.result(timeout=10.0)

            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            event_types = [event["event_type"] for event in events]
            assert event_types.count("ActivityHeartbeatRecorded") == 1
            assert event_types.count("ActivityTimedOut") == 1
            assert event_types.count("WorkflowFailed") == 1

            outcome = replay(
                LocalTimeoutWorkflow,
                events,
                ["Ada", timeout_kind],
                workflow_id=workflow_id,
                run_id=handle.run_id or "",
            )
            assert [command.__class__.__name__ for command in outcome.commands] == ["FailWorkflow"]
            assert _slow_executions == 1
        finally:
            await client.deregister_worker_registration(worker.worker_id)
