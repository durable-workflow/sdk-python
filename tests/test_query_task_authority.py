from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from durable_workflow import Client, Worker, serializer, workflow
from durable_workflow._query_task_authority import QueryTaskAuthority, current_query_authority
from durable_workflow.errors import QueryTaskAuthorityLost


def timestamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def delivery(**changes: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "query_task_id": "query-1", "query_task_attempt": 1, "lease_owner": "w1", "task_queue": "q1",
        "query_admitted_at": timestamp(now - timedelta(seconds=2)),
        "query_deadline_at": timestamp(now + timedelta(seconds=30)),
        "lease_expires_at": timestamp(now + timedelta(seconds=20)), "server_time": timestamp(now),
        "workflow_id": "workflow-1", "run_id": "run-1", "workflow_type": "authority-query",
        "query_name": "current", "payload_codec": "avro", "history_events": [],
        "workflow_arguments": serializer.envelope([], codec="avro"),
        "query_arguments": serializer.envelope([], codec="avro"), **changes,
    }


def authority(task: dict[str, Any], *, client: object | None = None, observed: bool = True) -> QueryTaskAuthority:
    return QueryTaskAuthority.delivery(client or object(), "billing", task, owner="w1", task_queue="q1",
                                       observed=observed)


def observation(task: dict[str, Any], **changes: Any) -> dict[str, Any]:
    return {key: task[key] for key in ("query_task_id", "query_task_attempt", "lease_owner", "query_admitted_at",
                                      "query_deadline_at", "lease_expires_at")} | {
        "server_time": timestamp(datetime.now(timezone.utc)), "query_task_status": "leased",
        "can_continue": True, "reason": None, **changes,
    }


@pytest.mark.parametrize(("field", "value"), [
    ("query_task_attempt", True), ("lease_owner", "other"), ("task_queue", "other"),
    ("lease_expires_at", None), ("lease_expires_at", "2026-10-14T00:00:00"),
    ("query_admitted_at", None), ("query_deadline_at", "invalid"), ("server_time", "invalid"),
])
def test_invalid_claim_cannot_grant_authority(field: str, value: Any) -> None:
    with pytest.raises(QueryTaskAuthorityLost):
        authority(delivery(**{field: value}))


@pytest.mark.parametrize("reason", ["query_task_timed_out", "lease_expired", "stale_worker_registration"])
async def test_denied_observation_never_starts_operation(reason: str) -> None:
    task = delivery()
    budget = authority(task)
    budget.observer = AsyncMock(return_value=observation(task, can_continue=False, reason=reason))
    operation = AsyncMock()
    with pytest.raises(QueryTaskAuthorityLost, match=reason):
        await budget.run(operation)
    operation.assert_not_called()


async def test_observation_never_extends_original_budget_or_accepts_changed_deadline() -> None:
    task = delivery()
    budget = authority(task)
    original_until = budget.until
    budget.observer = AsyncMock(return_value=observation(task))
    await budget.confirm()
    assert budget.until <= original_until
    budget.observer.return_value = observation(task, query_deadline_at=timestamp(
        datetime.now(timezone.utc) + timedelta(seconds=60)))
    with pytest.raises(QueryTaskAuthorityLost):
        await budget.confirm()


async def test_expiry_cancels_loading_without_starting_another_operation() -> None:
    budget = authority(delivery(), observed=False)
    budget.until = time.monotonic() + 0.03
    cancelled = asyncio.Event()

    async def load() -> None:
        try:
            await asyncio.sleep(60)
        finally:
            cancelled.set()

    with pytest.raises(QueryTaskAuthorityLost, match="budget_expired"):
        await budget.run(load)
    assert cancelled.is_set()


@workflow.defn(name="authority-query")
class QueryWorkflow:
    calls = 0

    def run(self, ctx: workflow.WorkflowContext) -> int:
        return 7

    @workflow.query("current")
    def current(self) -> int:
        type(self).calls += 1
        return 7


@pytest.mark.parametrize("expire_after_replay", [False, True])
async def test_worker_checks_after_replay_before_handler(monkeypatch: pytest.MonkeyPatch,
                                                        expire_after_replay: bool) -> None:
    import durable_workflow.worker as worker_module

    client = AsyncMock(spec=Client)
    client.namespace = "billing"
    client.complete_query_task.return_value = {"outcome": "completed"}
    worker = Worker(client, worker_id="w1", task_queue="q1", workflows=[QueryWorkflow])
    task = delivery()
    budget = authority(task, client=client, observed=False)
    original = worker_module._prepare_query_state

    def prepare(*args: Any, **kwargs: Any) -> Any:
        invoke = original(*args, **kwargs)
        if expire_after_replay:
            budget.until = time.monotonic() - 1
        return invoke

    monkeypatch.setattr(worker_module, "_prepare_query_state", prepare)
    QueryWorkflow.calls = 0
    token = current_query_authority.set(budget)
    try:
        outcome = await worker._run_query_task(task)
    finally:
        current_query_authority.reset(token)
    assert outcome == ("expired" if expire_after_replay else "completed")
    assert QueryWorkflow.calls == (0 if expire_after_replay else 1)
    assert client.complete_query_task.await_count == (0 if expire_after_replay else 1)
    client.fail_query_task.assert_not_called()


async def test_dispatch_context_keeps_its_own_original_budget() -> None:
    first, second = authority(delivery()), authority(delivery(query_task_id="query-2"))
    token = current_query_authority.set(first)
    ready = asyncio.Event()

    async def dispatched() -> QueryTaskAuthority | None:
        await ready.wait()
        return current_query_authority.get()

    pending = asyncio.create_task(dispatched())
    current_query_authority.set(second)
    ready.set()
    try:
        assert await pending is first
        assert current_query_authority.get() is second
    finally:
        current_query_authority.reset(token)


async def test_query_loading_reuses_references_without_enlarging_persistent_cache() -> None:
    from durable_workflow.external_storage import ExternalPayloadCache
    from tests.test_runtime_external_payload_transport import FakeRuntimePayloadServer, runtime_client, runtime_envelope

    server = FakeRuntimePayloadServer()
    first, second = runtime_envelope(server, ["first"]), runtime_envelope(server, ["second"])
    task = delivery(workflow_arguments=first, query_arguments=second,
                    history_events=[{"event_type": "WorkflowStarted", "payload": {"arguments": first}}])
    server.responses[("POST", "/api/worker/query-tasks/poll")] = {"task": task}
    cache = ExternalPayloadCache(max_bytes=1)
    async with runtime_client(server, cache=cache) as client:
        token = current_query_authority.set(None)
        try:
            result = await client.poll_query_task(worker_id="w1", task_queue="q1")
        finally:
            current_query_authority.reset(token)
    assert server.fetch_count == 2
    assert result["workflow_arguments"] == result["history_events"][0]["payload"]["arguments"]
    assert result["workflow_arguments"] is not result["history_events"][0]["payload"]["arguments"]
    assert cache.max_bytes == 1


async def test_status_loss_cancels_active_query_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    import durable_workflow._query_task_authority as authority_module
    from tests.test_runtime_external_payload_transport import FakeRuntimePayloadServer, runtime_client, runtime_envelope

    monkeypatch.setattr(authority_module, "_OBSERVATION_INTERVAL_SECONDS", 0.01)
    server = FakeRuntimePayloadServer()
    task = delivery(workflow_arguments=runtime_envelope(server, ["large"]))
    info = server.cluster_info()
    info["worker_protocol"]["server_capabilities"]["query_task_authority"] = True
    server.cluster_info = lambda: info  # type: ignore[method-assign]
    server.responses[("POST", "/api/worker/query-tasks/poll")] = {"task": task}
    fetching, cancelled = asyncio.Event(), asyncio.Event()

    async def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/status"):
            assert request.method == "GET"
            assert request.url.params["lease_owner"] == "w1"
            assert request.url.params["query_task_attempt"] == "1"
            return httpx.Response(200, json=observation(task, can_continue=not fetching.is_set(),
                reason="stale_worker_registration" if fetching.is_set() else None))
        if request.method == "GET" and "/external-payloads/" in request.url.path:
            fetching.set()
            try:
                await asyncio.sleep(60)
            finally:
                cancelled.set()
        return server.handler(request)

    async with runtime_client(server) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(handle))
        await client.get_cluster_info()
        token = current_query_authority.set(None)
        try:
            with pytest.raises(QueryTaskAuthorityLost, match="stale_worker_registration"):
                await asyncio.wait_for(client.poll_query_task(worker_id="w1", task_queue="q1"), timeout=1)
        finally:
            current_query_authority.reset(token)
    assert fetching.is_set() and cancelled.is_set()


@pytest.mark.parametrize(("version", "supported", "accepted"), [
    ("1.20", False, False), ("1.19", True, False), ("1.20", True, True),
])
async def test_required_authority_is_checked_before_registration(monkeypatch: pytest.MonkeyPatch,
        version: str, supported: bool, accepted: bool) -> None:
    from tests.test_worker import compatible_cluster_info

    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", version)
    info = compatible_cluster_info()
    protocol = info["worker_protocol"]
    assert isinstance(protocol, dict)
    protocol["server_capabilities"]["query_task_authority"] = supported
    client = AsyncMock(spec=Client)
    client.get_cluster_info.return_value = info
    client.register_worker.return_value = {"worker_id": "w1", "registered": True}
    worker = Worker(client, worker_id="w1", task_queue="q1", workflows=[QueryWorkflow],
                    require_query_task_authority=True)
    if accepted:
        await worker._register()
        client.register_worker.assert_awaited_once()
    else:
        with pytest.raises(RuntimeError, match="query_task_authority_unavailable"):
            await worker._register()
        client.register_worker.assert_not_called()


async def test_explicit_legacy_status_does_not_send_a_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.19")
    async with Client("https://runtime.example", namespace="billing") as client:
        client._request = AsyncMock()  # type: ignore[method-assign]
        with pytest.raises(ValueError, match="protocol 1.20"):
            await client.query_task_status(query_task_id="query-1", lease_owner="w1", query_task_attempt=1)
        client._request.assert_not_called()
