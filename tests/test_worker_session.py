from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from durable_workflow import Client, Worker, WorkerSessionOptions
from durable_workflow.workflow import ScheduleActivity, commands_to_server_commands


def test_options_are_canonical_and_validate_bounds() -> None:
    options = WorkerSessionOptions(
        session_id="  render  ",
        queue="gpu",
        requirements=(" gpu:l4 ", "gpu:l4"),
        max_concurrent_activities=2,
    )
    assert options.to_wire() == {
        "session_id": "render",
        "queue": "gpu",
        "requirements": ["gpu:l4"],
        "lease_seconds": 120,
        "ttl_seconds": 1800,
        "max_concurrent_activities": 2,
        "create_if_missing": True,
        "allow_reacquire_after_failure": True,
    }
    for invalid in ("", " \t"):
        with pytest.raises(ValueError, match="session id"):
            WorkerSessionOptions(invalid)
    with pytest.raises(ValueError, match="positive"):
        WorkerSessionOptions("render", lease_seconds=0)
    with pytest.raises(ValueError, match="non-empty"):
        WorkerSessionOptions("render", requirements=(" ",))


def test_activity_command_carries_session_routing_in_both_encoding_paths() -> None:
    options = WorkerSessionOptions("render", requirements=("gpu:l4",))
    command = ScheduleActivity("render.frame", [{"frame": 1}], worker_session=options)
    assert command.to_server_command("gpu")["worker_session"] == options.to_wire()
    assert commands_to_server_commands([command], "gpu")[0]["worker_session"] == options.to_wire()


@pytest.mark.asyncio
async def test_client_session_verbs_use_worker_auth_and_canonical_paths() -> None:
    client = Client("https://server.example", worker_token="worker-only", namespace="ns")
    options = WorkerSessionOptions("render/one")
    with patch.object(client, "_request", new_callable=AsyncMock, return_value={"outcome": "ok"}) as request:
        await client.create_worker_session("worker-1", options)
        await client.renew_worker_session("worker-1", options.session_id, 180)
        await client.close_worker_session("worker-1", options.session_id, "shutdown")
    assert request.await_count == 3
    assert request.await_args_list[0].args == ("POST", "/worker/sessions")
    assert request.await_args_list[0].kwargs == {
        "worker": True, "json": {"worker_id": "worker-1", **options.to_wire()}
    }
    assert request.await_args_list[1].args == ("POST", "/worker/sessions/render%2Fone/heartbeat")
    assert request.await_args_list[1].kwargs["json"]["lease_seconds"] == 180
    assert request.await_args_list[2].args == ("DELETE", "/worker/sessions/render%2Fone")
    assert request.await_args_list[2].kwargs["json"] == {"worker_id": "worker-1", "reason": "shutdown"}
    with pytest.raises(ValueError, match="positive"):
        await client.renew_worker_session("worker-1", "render", 0)
    await client.aclose()


@pytest.mark.asyncio
async def test_worker_session_lifecycle_and_shutdown_are_idempotent() -> None:
    client = Client("https://server.example", worker_token="worker-only", namespace="ns")
    worker = Worker(client, task_queue="gpu", worker_id="holder-1")
    options = WorkerSessionOptions("render", queue="gpu")
    session = worker.worker_session(options)
    assert worker.worker_session(options) is session
    assert worker._current_task_slots()["session_available"] == worker.max_concurrent_worker_sessions

    with (
        patch.object(client, "create_worker_session", new_callable=AsyncMock, return_value={"outcome": "created"}),
        patch.object(
            client, "renew_worker_session", new_callable=AsyncMock, return_value={"outcome": "renewed"}
        ) as renew,
        patch.object(
            client, "close_worker_session", new_callable=AsyncMock, return_value={"outcome": "closed"}
        ) as close,
    ):
        assert (await session.create())["outcome"] == "created"
        assert worker._current_task_slots()["session_available"] == worker.max_concurrent_worker_sessions - 1
        assert (await session.renew(180))["outcome"] == "renewed"
        renew.assert_awaited_once_with("holder-1", "render", 180)
        await worker.stop()
        await worker.stop()
        assert (await session.close())["outcome"] == "closed"
        close.assert_awaited_once_with("holder-1", "render", "worker_shutdown")
        assert worker._current_task_slots()["session_available"] == worker.max_concurrent_worker_sessions
        with pytest.raises(RuntimeError, match="closed"):
            await session.renew()

    await client.aclose()


@pytest.mark.asyncio
async def test_claimed_session_is_tracked_and_closed_without_explicit_create() -> None:
    client = Client("https://server.example", worker_token="worker-only", namespace="ns")
    worker = Worker(client, task_queue="gpu", worker_id="replacement-holder")
    worker._track_worker_session_from_task({
        "worker_session": {
            "session_id": "render",
            "queue": "gpu",
            "requirements": ["gpu:l4"],
        }
    })
    session = worker.worker_session(WorkerSessionOptions("render", queue="gpu", requirements=("gpu:l4",)))
    assert session.active
    assert session.rebuild_required_after_holder_loss()
    with patch.object(
        client, "close_worker_session", new_callable=AsyncMock, return_value={"outcome": "closed"}
    ) as close:
        await worker.stop()
    close.assert_awaited_once_with("replacement-holder", "render", "worker_shutdown")
    await client.aclose()
