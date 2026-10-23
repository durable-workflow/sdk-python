"""Qualify generation-fenced receipts against the connected real Server."""

from __future__ import annotations

import uuid

import pytest

from durable_workflow import Client, ServerError, Worker


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
        assert first._deregistration_fenced and token
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
        assert first._deregistration_fenced and first._registration_token
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
        assert worker._deregistration_fenced and worker._registration_token
        try:
            with pytest.raises(ServerError) as failure:
                await client.deregister_worker_registration(worker_id, registration_token=uuid.uuid4().hex)
            assert failure.value.status == 404
            assert failure.value.reason() == "worker_registration_token_not_found"
            assert (await client.heartbeat_worker(worker_id=worker_id))["acknowledged"] is True
        finally:
            await worker.stop()
