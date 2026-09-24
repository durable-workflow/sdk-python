"""Exercise typed Python worker sessions against a real Server."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from durable_workflow import Client, Worker, WorkerSessionOptions, activity, workflow
from durable_workflow.client import PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST
from durable_workflow.errors import ServerError


@activity.defn(name="tests.python-session-greet")
async def session_greet(name: str) -> str:
    await activity.context().heartbeat({"phase": "greeting"})
    return f"hello, {name}"


@workflow.defn(name="tests.python-session-workflow")
class SessionWorkflow:
    def run(self, ctx: Any, name: str, queue: str, session_id: str) -> Any:
        greeting = yield ctx.schedule_activity(
            "tests.python-session-greet",
            [name],
            queue=queue,
            worker_session=WorkerSessionOptions(
                session_id,
                queue=queue,
                requirements=("gpu:l4",),
                lease_seconds=5,
                ttl_seconds=60,
            ),
        )
        return {"greeting": greeting}


@pytest.mark.asyncio
async def test_session_lifecycle_routes_activity_and_closes_on_shutdown(
    server_url: str, server_token: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST["worker_sessions"]
    monkeypatch.setitem(manifest, "supported", True)
    monkeypatch.setitem(manifest, "implementation", "typed_worker_session")
    monkeypatch.delitem(manifest, "reason", raising=False)
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-session-{suffix}"
    session_id = f"py-session-{suffix}"
    options = WorkerSessionOptions(
        session_id, queue=queue, requirements=("gpu:l4",), lease_seconds=5, ttl_seconds=60
    )

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[SessionWorkflow],
            activities=[session_greet],
            capabilities=["gpu:l4"],
            worker_id=f"py-session-worker-{suffix}",
        )
        runner = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(worker._registration_done.wait(), timeout=20)
            if runner.done():
                await runner
            session = worker.worker_session(options)
            assert (await session.create())["outcome"] == "created"
            assert (await session.renew())["outcome"] == "heartbeat_recorded"
            handle = await client.start_workflow(
                workflow_type="tests.python-session-workflow",
                task_queue=queue,
                workflow_id=f"py-session-run-{suffix}",
                input=["Ada", queue, session_id],
            )
            assert await handle.result(timeout=30) == {"greeting": "hello, Ada"}
            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            assert "ActivityHeartbeatRecorded" in [event["event_type"] for event in events]
            assert session.active
        finally:
            await worker.stop()
            await asyncio.wait_for(runner, timeout=15)
        assert (await session.close())["outcome"] == "closed"


@pytest.mark.asyncio
async def test_expired_holder_can_be_reacquired_without_reusing_process_state(
    server_url: str, server_token: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST["worker_sessions"]
    monkeypatch.setitem(manifest, "supported", True)
    monkeypatch.setitem(manifest, "implementation", "typed_worker_session")
    monkeypatch.delitem(manifest, "reason", raising=False)
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-session-reclaim-{suffix}"
    options = WorkerSessionOptions(f"py-session-reclaim-{suffix}", queue=queue, lease_seconds=3, ttl_seconds=30)

    async with Client(server_url, token=server_token, namespace="default") as client:
        first = Worker(client, task_queue=queue, worker_id=f"py-holder-first-{suffix}")
        await first._register()
        replacement = Worker(client, task_queue=queue, worker_id=f"py-holder-replacement-{suffix}")
        await replacement._register()
        first_session = first.worker_session(options)
        assert (await first_session.create())["outcome"] == "created"

        try:
            replacement_session = replacement.worker_session(options)
            with pytest.raises(ServerError) as owned:
                await replacement_session.create()
            assert owned.value.reason() == "session_owned_by_another_worker"
            await client.deregister_worker_registration(first.worker_id)
            await asyncio.sleep(3.2)
            assert (await replacement_session.create())["outcome"] == "reacquired"
            assert replacement_session.rebuild_required_after_holder_loss()
            assert (await replacement_session.close())["outcome"] == "closed"
        finally:
            await replacement.stop()
