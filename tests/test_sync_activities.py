from __future__ import annotations

import asyncio
import contextlib
import threading
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer
from durable_workflow.client import Client
from durable_workflow.worker import Worker


def activity_task(number: int = 1) -> dict[str, Any]:
    return {
        "task_id": f"sync-task-{number}",
        "activity_attempt_id": f"sync-attempt-{number}",
        "activity_type": "sync-probe",
        "arguments": serializer.encode([], codec="avro"),
        "payload_codec": "avro",
    }


async def wait_for_thread(event: threading.Event) -> None:
    async def observe() -> None:
        while not event.is_set():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(observe(), timeout=1)


@pytest.mark.asyncio
async def test_default_sync_handlers_run_concurrently_without_stalling_heartbeat() -> None:
    release = threading.Event()
    started: list[str] = []

    @activity.defn(name="sync-probe")
    def blocking() -> str:
        started.append(threading.current_thread().name)
        assert release.wait(1), "event loop did not release the activity"
        return activity.context().info.task_id

    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="sync-queue", activities=[blocking],
                    max_concurrent_activity_tasks=2, heartbeat_interval=0.01)
    heartbeat = asyncio.create_task(worker._heartbeat_loop())
    tasks = [asyncio.create_task(worker._run_activity_task(activity_task(n))) for n in (1, 2)]
    try:
        await asyncio.sleep(0.05)
        assert len(started) == 2
        assert all(name.startswith("dw-remote-activity") for name in started)
        assert not any(task.done() for task in tasks)
        assert client.heartbeat_worker.await_count >= 1
        assert worker._current_task_slots()["activity_available"] == 0
    finally:
        release.set()
        worker._stop.set()
        await heartbeat
        results = await asyncio.gather(*tasks)
        await worker._shutdown()
    assert results == ["completed", "completed"]
    assert {call.kwargs["result"] for call in client.complete_activity_task.await_args_list} == {
        "sync-task-1", "sync-task-2",
    }


@pytest.mark.asyncio
async def test_sync_activity_heartbeat_retains_owner_and_observes_cancellation() -> None:
    contexts: list[activity.ActivityContext] = []

    @activity.defn(name="sync-probe")
    def blocking() -> None:
        ctx = activity.context()
        contexts.append(ctx)
        ctx.heartbeat_sync({"processed": 7})
        pytest.fail("cancelled handler must not return a result")

    client = AsyncMock(spec=Client)
    client.heartbeat_activity_task.return_value = {"cancel_requested": True}
    worker = Worker(client, task_queue="sync-queue", worker_id="sync-owner", activities=[blocking])
    try:
        assert await worker._run_activity_task(activity_task()) == "cancelled"
        client.heartbeat_activity_task.assert_awaited_once_with(
            task_id="sync-task-1", activity_attempt_id="sync-attempt-1",
            lease_owner="sync-owner", details={"processed": 7},
        )
        assert contexts[0].is_cancelled
        client.complete_activity_task.assert_not_awaited()
        assert client.fail_activity_task.await_args.kwargs["failure_type"] == "ActivityCancelled"
    finally:
        await worker._shutdown()


@pytest.mark.asyncio
async def test_async_activity_remains_on_loop_and_sync_heartbeat_cannot_deadlock_it() -> None:
    owner = threading.current_thread()

    @activity.defn(name="sync-probe")
    async def nonblocking() -> str:
        assert threading.current_thread() is owner
        with pytest.raises(RuntimeError, match="await.*heartbeat"):
            activity.context().heartbeat_sync()
        await activity.context().heartbeat({"async": True})
        return "async-result"

    client = AsyncMock(spec=Client)
    client.heartbeat_activity_task.return_value = {"cancel_requested": False}
    worker = Worker(client, task_queue="sync-queue", activities=[nonblocking])
    assert await worker._run_activity_task(activity_task()) == "completed"
    assert client.complete_activity_task.await_args.kwargs["result"] == "async-result"
    assert worker._remote_activity_executor is None


@pytest.mark.asyncio
async def test_cancelled_awaiter_keeps_physical_thread_capacity_until_return() -> None:
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    @activity.defn(name="sync-probe")
    def blocking() -> None:
        started.set()
        try:
            release.wait(2)
        finally:
            finished.set()

    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="sync-queue", activities=[blocking], max_concurrent_activity_tasks=1)
    running = asyncio.create_task(worker._run_activity_task(activity_task()))
    try:
        await wait_for_thread(started)
        running.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await running
        assert not finished.is_set()
        assert worker._current_task_slots()["activity_available"] == 0
        client.complete_activity_task.assert_not_awaited()
    finally:
        release.set()
        await wait_for_thread(finished)
        await asyncio.sleep(0)
        await worker._shutdown()
    assert worker._current_task_slots()["activity_available"] == 1


@pytest.mark.asyncio
async def test_shutdown_retains_registration_if_activity_thread_has_not_stopped() -> None:
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    @activity.defn(name="sync-probe")
    def blocking() -> None:
        started.set()
        try:
            release.wait(2)
        finally:
            finished.set()

    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="sync-queue", activities=[blocking], shutdown_timeout=0.01)
    worker._registered = True
    running = worker._track(worker._run_activity_task(activity_task()))
    try:
        await wait_for_thread(started)
        with pytest.raises(RuntimeError, match="activity thread.*registration remains active"):
            await worker._shutdown()
        assert worker._registered
        client.deregister_worker_registration.assert_not_awaited()
        assert not finished.is_set()
        client.complete_activity_task.assert_not_awaited()
    finally:
        release.set()
        await wait_for_thread(finished)
        with contextlib.suppress(asyncio.CancelledError):
            await running
        await asyncio.sleep(0)
        await worker._shutdown()
    client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)
