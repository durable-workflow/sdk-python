from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from durable_workflow.client import Client
from durable_workflow.errors import RuntimeCapabilityUnsupported, RuntimeDiscoveryUnavailable


def renewal_client(supported: object = True) -> Client:
    client = Client("http://unused.invalid", worker_token="worker-secret", namespace="17")
    client._cluster_info = {
        "worker_protocol": {"server_capabilities": {"activity_lease_renewal": supported}},
    }
    return client


@pytest.mark.asyncio
async def test_renewal_uses_worker_credentials_and_exact_original_identity() -> None:
    client = renewal_client()
    reply = {"lease_renewed": True, "heartbeat_recorded": False, "server_time": "original-clock"}
    client._request = AsyncMock(return_value=reply)
    try:
        assert await client.renew_activity_task_lease(
            task_id="task/17", activity_attempt_id="attempt0", lease_owner="worker017",
        ) is reply
        client._request.assert_awaited_once_with(
            "POST", "/worker/activity-tasks/task%2F17/renew-lease", worker=True,
            json={"activity_attempt_id": "attempt0", "lease_owner": "worker017"}, timeout=5.0,
        )
    finally:
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("supported", [False, None, "true", 1])
async def test_renewal_requires_affirmative_capability_before_a_worker_request(supported: object) -> None:
    client = renewal_client(supported)
    client._request = AsyncMock()
    try:
        expected = RuntimeCapabilityUnsupported if supported is False else RuntimeDiscoveryUnavailable
        with pytest.raises(expected, match="activity_lease_renewal"):
            await client.renew_activity_task_lease(
                task_id="task", activity_attempt_id="attempt", lease_owner="worker",
            )
        client._request.assert_not_awaited()
    finally:
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["task_id", "activity_attempt_id", "lease_owner"])
async def test_empty_claim_identity_is_refused_before_discovery(field: str) -> None:
    client = renewal_client()
    client._runtime_discovery = AsyncMock()
    identity = {"task_id": "task", "activity_attempt_id": "attempt", "lease_owner": "worker", field: ""}
    try:
        with pytest.raises(ValueError, match=field):
            await client.renew_activity_task_lease(**identity)
        client._runtime_discovery.assert_not_awaited()
    finally:
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["discovery", "renewal"])
async def test_discovery_and_transport_share_one_bounded_request_window(phase: str) -> None:
    client = renewal_client()
    cancelled = asyncio.Event()

    async def held(*args: object, **kwargs: object) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    if phase == "discovery":
        client._runtime_discovery = AsyncMock(side_effect=held)
        client._request = AsyncMock()
    else:
        client._request = AsyncMock(side_effect=held)
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.renew_activity_task_lease(
                task_id="task", activity_attempt_id="attempt", lease_owner="worker", timeout=0.02,
            ), timeout=0.5)
        assert cancelled.is_set()
        if phase == "discovery":
            client._request.assert_not_awaited()
    finally:
        await client.aclose()
