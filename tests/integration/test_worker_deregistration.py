"""Qualify generation-fenced receipts against the connected real Server."""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import httpx
import pytest

from durable_workflow import Client, ServerError, Worker
from durable_workflow.workflow import replay
from tests.deregistration_recovery import DiscardCompletedResponse, RecoveredWorkflow, echo


def make_worker(client: Client, worker_id: str) -> Worker:
    return Worker(client, task_queue="python-deregistration", worker_id=worker_id)


@pytest.mark.asyncio
async def test_completed_receipt_replays_without_deleting_replacement(
    server_url: str, server_token: str,
) -> None:
    worker_id = "py-dereg-receipt-" + uuid.uuid4().hex
    async with Client(server_url, token=server_token) as client:
        first = make_worker(client, worker_id)
        await first._register()
        token = first._registration_token
        # This integration cohort must expose the contract being qualified;
        # SDK unit controls separately preserve capability-absent operation.
        assert first._deregistration_fenced
        assert token
        result = await client.deregister_worker_registration(worker_id, registration_token=token)
        assert result["outcome"] == "deregistered"
        replacement = make_worker(client, worker_id)
        await replacement._register()
        try:
            assert replacement._registration_token != token
            await first.stop()
            assert not first._registered
            assert (await client.heartbeat_worker(worker_id=worker_id))["acknowledged"] is True
        finally:
            await replacement.stop()


@pytest.mark.asyncio
async def test_superseded_generation_is_refused_without_touching_replacement(
    server_url: str, server_token: str,
) -> None:
    worker_id = "py-dereg-replaced-" + uuid.uuid4().hex
    async with Client(server_url, token=server_token) as client:
        first = make_worker(client, worker_id)
        await first._register()
        assert first._deregistration_fenced
        assert first._registration_token
        replacement = make_worker(client, worker_id)
        await replacement._register()
        try:
            with pytest.raises(ServerError) as failure:
                await first.stop()
            assert failure.value.status == 409
            assert failure.value.reason() == "worker_registration_lost_authority"
            assert first._registered
            assert (await client.heartbeat_worker(worker_id=worker_id))["acknowledged"] is True
        finally:
            await replacement.stop()


@pytest.mark.asyncio
async def test_unknown_registration_token_is_not_already_deregistered(
    server_url: str, server_token: str,
) -> None:
    worker_id = "py-dereg-unknown-" + uuid.uuid4().hex
    async with Client(server_url, token=server_token) as client:
        worker = make_worker(client, worker_id)
        await worker._register()
        assert worker._deregistration_fenced
        assert worker._registration_token
        try:
            with pytest.raises(ServerError) as failure:
                await client.deregister_worker_registration(worker_id, registration_token=uuid.uuid4().hex)
            assert failure.value.status == 404
            assert failure.value.reason() == "worker_registration_token_not_found"
            assert (await client.heartbeat_worker(worker_id=worker_id))["acknowledged"] is True
        finally:
            await worker.stop()


@pytest.mark.asyncio
async def test_lost_completed_response_retries_receipt_after_replacement_registration(
    server_url: str, server_token: str,
) -> None:
    worker_id = "py-dereg-lost-" + uuid.uuid4().hex
    async with (
        Client(server_url, token=server_token) as client,
        Client(server_url, token=server_token) as replacement_client,
    ):
        replacement = make_worker(replacement_client, worker_id)

        fault = DiscardCompletedResponse(replacement)
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url=client.base_url, transport=fault)
        first = make_worker(client, worker_id)
        await first._register()
        assert first._deregistration_fenced
        assert first._registration_token
        try:
            await first.stop()
            assert fault.discarded
            assert len(fault.deregistration_bodies) == 2
            assert fault.deregistration_bodies[0] == fault.deregistration_bodies[1]
            assert not first._registered
            assert (await replacement_client.heartbeat_worker(worker_id=worker_id))["acknowledged"] is True
        finally:
            await replacement.stop()


async def exercise_recovered_claim(client: Client, replacement_client: Client, identity: str) -> dict[str, Any]:
    first = Worker(client, task_queue=identity, worker_id=identity, workflows=[RecoveredWorkflow], activities=[echo])
    replacement = Worker(
        replacement_client, task_queue=identity, worker_id=identity, workflows=[RecoveredWorkflow], activities=[echo],
    )
    fault = DiscardCompletedResponse(replacement)
    await client._http.aclose()
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=fault)
    await first._register()
    handle = await client.start_workflow(
        workflow_type="tests.deregistration.cold-recovery", task_queue=identity,
        workflow_id=identity, input=["handoff"],
    )
    old = await client.poll_workflow_task(worker_id=identity, task_queue=identity, timeout=2)
    assert isinstance(old, dict)
    try:
        await first.stop()
        assert not first._registered
        assert fault.discarded
        assert fault.recovered_workflow_task_count == 1
        assert len(fault.deregistration_bodies) == 2
        assert fault.deregistration_bodies[0] == fault.deregistration_bodies[1]
        claimed = await replacement_client.poll_workflow_task(worker_id=identity, task_queue=identity, timeout=2)
        assert isinstance(claimed, dict)
        assert claimed["task_id"] == old["task_id"]
        assert claimed["workflow_task_attempt"] > old["workflow_task_attempt"]
        outcome = replay(RecoveredWorkflow, claimed["history_events"], ["handoff"], run_id=handle.run_id)
        assert len(outcome.commands) == 1
        await replacement_client.complete_workflow_task(
            task_id=claimed["task_id"], lease_owner=identity, workflow_task_attempt=claimed["workflow_task_attempt"],
            commands=[command.to_server_command(identity) for command in outcome.commands],
        )
        task = await replacement_client.poll_activity_task(worker_id=identity, task_queue=identity, timeout=2)
        assert isinstance(task, dict)
        await replacement_client.complete_activity_task(
            task_id=task["task_id"], activity_attempt_id=task["activity_attempt_id"],
            lease_owner=identity, result="handoff",
        )
        resumed = await replacement_client.poll_workflow_task(worker_id=identity, task_queue=identity, timeout=2)
        assert isinstance(resumed, dict)
        cold = replay(RecoveredWorkflow, resumed["history_events"], ["handoff"], run_id=handle.run_id)
        assert len(cold.commands) == 1
        assert cold.commands[0].to_server_command(identity)["type"] == "complete_workflow"
        await replacement_client.complete_workflow_task(
            task_id=resumed["task_id"], lease_owner=identity, workflow_task_attempt=resumed["workflow_task_attempt"],
            commands=[command.to_server_command(identity) for command in cold.commands],
        )
        result = await handle.result(timeout=3, poll_interval=.05)
        assert result == {"value": "handoff", "run": handle.run_id}
        return {"history": resumed["history_events"], "run_id": handle.run_id, "workflow_id": handle.workflow_id,
                "result": result, "old_attempt": old["workflow_task_attempt"],
                "replacement_attempt": claimed["workflow_task_attempt"], "recovered_claim_count": 1,
                "deregistration_attempts": 2, "same_request_bytes": True,
                "replacement_preserved": True, "cold_replay_did_not_reschedule_activity": True}
    finally:
        await replacement.stop()


@pytest.mark.asyncio
async def test_recovered_claim_cold_replays_after_lost_receipt_and_replacement(
    server_url: str, server_token: str,
) -> None:
    async with (
        Client(server_url, worker_token=server_token,
               control_token=os.environ.get("DURABLE_WORKFLOW_CONTROL_TOKEN", server_token)) as client,
        Client(server_url, worker_token=server_token) as replacement_client,
    ):
        await asyncio.wait_for(
            exercise_recovered_claim(client, replacement_client, "py-dereg-cold-" + uuid.uuid4().hex[:12]), timeout=30,
        )
