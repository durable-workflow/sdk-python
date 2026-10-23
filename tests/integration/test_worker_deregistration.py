"""Qualify generation-fenced receipts against the connected real Server."""

from __future__ import annotations

import uuid

import httpx
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

        class DiscardCompletedResponse(httpx.AsyncBaseTransport):
            def __init__(self) -> None:
                self.inner = httpx.AsyncHTTPTransport()
                self.discarded = False
                self.deregistration_bodies: list[bytes] = []

            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                response = await self.inner.handle_async_request(request)
                if request.method != "POST" or not request.url.path.endswith("/deregister"):
                    return response
                self.deregistration_bodies.append(request.content)
                if self.discarded:
                    return response
                # The real Server has committed and answered. Only the caller's
                # response is discarded; no Server behavior is mocked here.
                await response.aread()
                assert response.status_code == 200
                receipt = response.json()
                assert receipt["outcome"] == "deregistered"
                await response.aclose()
                await replacement._register()
                assert replacement._registration_token != receipt["registration_token"]
                self.discarded = True
                raise httpx.RemoteProtocolError("test discarded a committed response", request=request)

            async def aclose(self) -> None:
                await self.inner.aclose()

        fault = DiscardCompletedResponse()
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
