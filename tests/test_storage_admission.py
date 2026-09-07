from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

import durable_workflow.retry_policy as retry_module
from durable_workflow import activity, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import ServerError, Unauthorized
from durable_workflow.retry_policy import TransportRetryPolicy, _worker_storage_admission_stop
from durable_workflow.worker import PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST, Worker
from tests.test_runtime_external_payload_transport import FakeRuntimePayloadServer, runtime_client
from tests.test_worker import ValidatedUpdateWorkflow, compatible_cluster_info


def pressure(poll_id: str | None = None, *, reason: str = "storage_pressure", entry: bool = True) -> dict[str, Any]:
    body: dict[str, Any] = {
        "reason": reason,
        "storage_state": "draining" if reason == "storage_pressure" else "fenced",
        "retryable": True,
        "retry_after_seconds": 1,
    }
    if entry:
        body["request_admitted"] = False
    if poll_id is not None:
        body.update({
            "task": None, "poll_status": reason, "poll_request_id": poll_id,
            "retry_same_poll_request_id": True, "claim_admitted": False,
        })
    return body


@contextmanager
def worker_scope(stop: Callable[[], bool] = lambda: False) -> Iterator[None]:
    token = _worker_storage_admission_stop.set(stop)
    try:
        yield
    finally:
        _worker_storage_admission_stop.reset(token)


@pytest.fixture
def retry_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        assert len(sleeps) < 1000, "The simulated storage outage must recover or be interrupted."
        await asyncio.sleep(0)

    monkeypatch.setattr(retry_module, "asyncio", SimpleNamespace(sleep=sleep))
    return sleeps


def client_for(handler: Callable[..., Any]) -> Client:
    client = Client(
        "https://runtime.example", token="test-runtime-token",
        retry_policy=TransportRetryPolicy(max_attempts=2, initial_backoff_seconds=0, jitter=False),
    )
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(handler))
    client._runtime_external_payload_transport_resolved = True
    return client


@pytest.mark.parametrize("kind", ["workflow", "activity", "query", "multiplexed"])
@pytest.mark.parametrize("reason", ["storage_pressure", "storage_admission_unavailable"])
@pytest.mark.parametrize("entry", [True, False])
async def test_poll_keeps_identity_after_ambiguous_response_and_pressure(
    kind: str, reason: str, entry: bool, retry_sleeps: list[float],
) -> None:
    requests: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        body = json.loads(request.content)
        if len(requests) == 1:
            raise httpx.ReadTimeout("Response lost after a possible claim.", request=request)
        if len(requests) < 7:
            return httpx.Response(503, json=pressure(body["poll_request_id"], reason=reason, entry=entry))
        return httpx.Response(200, json={"task": {"task_id": "same-claim"}})

    async with client_for(handler) as client:
        with worker_scope():
            if kind == "multiplexed":
                result = await client.poll_workflow_task(
                    worker_id="storage-worker", task_queue="orders",
                    task_kinds=("workflow", "update_validation"),
                )
            else:
                poll = getattr(client, f"poll_{kind}_task")
                result = await poll(worker_id="storage-worker", task_queue="orders")
    assert result == {"task_id": "same-claim"}
    assert len(requests) == 7
    assert len(set(requests)) == 1
    assert sum(retry_sleeps) == pytest.approx(5)


@pytest.mark.parametrize("method,kwargs", [
    ("register_worker", {
        "worker_id": "storage-worker", "task_queue": "orders",
        "capability_manifest": PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
    }),
    ("heartbeat_worker", {"worker_id": "storage-worker"}),
    ("complete_workflow_task", {"task_id": "task", "workflow_task_attempt": 7, "commands": []}),
    ("fail_workflow_task", {"task_id": "task", "workflow_task_attempt": 7, "message": "failed"}),
    ("complete_activity_task", {"task_id": "task", "activity_attempt_id": "attempt-7", "result": "done"}),
    ("fail_activity_task", {"task_id": "task", "activity_attempt_id": "attempt-7", "message": "failed"}),
    ("heartbeat_activity_task", {"task_id": "task", "activity_attempt_id": "attempt-7", "details": {"step": 2}}),
    ("complete_query_task", {"query_task_id": "task", "query_task_attempt": 7, "result": "done"}),
    ("fail_query_task", {"query_task_id": "task", "query_task_attempt": 7, "message": "failed"}),
    ("approve_update_validation_task", {"update_validation_task_id": "task", "update_validation_attempt": 7}),
    ("reject_update_validation_task", {
        "update_validation_task_id": "task", "update_validation_attempt": 7, "message": "rejected",
        "reason": "update_rejected",
    }),
])
async def test_worker_mutations_retry_the_prepared_request(
    method: str, kwargs: dict[str, Any], retry_sleeps: list[float],
) -> None:
    requests: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        if len(requests) <= 4:
            return httpx.Response(503, json=pressure(reason="storage_admission_unavailable"))
        return httpx.Response(200, json={"recorded": True})

    if method not in ("register_worker", "heartbeat_worker"):
        kwargs = {**kwargs, "lease_owner": "storage-worker"}
    async with client_for(handler) as client:
        with worker_scope():
            await getattr(client, method)(**kwargs)
    assert len(requests) == 5
    assert len(set(requests)) == 1
    assert sum(retry_sleeps) == pytest.approx(4)


@pytest.mark.parametrize("override", [
    {"task": {"task_id": "already-claimed"}}, {"poll_request_id": "another-poll"},
    {"poll_status": "empty"}, {"claim_admitted": True}, {"retry_same_poll_request_id": False},
    {"request_admitted": True}, {"retryable": False}, {"storage_state": "normal"},
    {"retry_after_seconds": 0}, {"retry_after_seconds": True}, {"retry_after_seconds": "1"},
    {"reason": "storage_admission_unavailable", "poll_status": "storage_admission_unavailable"},
])
async def test_invalid_storage_poll_contract_is_not_retried(
    override: dict[str, Any], retry_sleeps: list[float],
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={**pressure(json.loads(request.content)["poll_request_id"]), **override})

    async with client_for(handler) as client:
        with worker_scope(), pytest.raises(ServerError):
            await client.poll_activity_task(worker_id="storage-worker", task_queue="orders")
    assert calls == 1
    assert not retry_sleeps


@pytest.mark.parametrize("scoped,worker_plane", [(False, True), (True, False)])
async def test_direct_clients_and_control_plane_keep_bounded_retries(
    scoped: bool, worker_plane: bool, retry_sleeps: list[float],
) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json=pressure())

    async with client_for(handler) as client:
        async def call() -> None:
            with pytest.raises(ServerError):
                await client._request("POST", "/test", worker=worker_plane, json={})
        if scoped:
            with worker_scope():
                await call()
        else:
            await call()
    assert calls == 2


async def test_retry_context_is_task_local_and_restored(retry_sleeps: list[float]) -> None:
    async def check() -> None:
        assert _worker_storage_admission_stop.get() is None

    outside = asyncio.create_task(check())
    with worker_scope():
        await outside
        assert _worker_storage_admission_stop.get() is not None
    assert _worker_storage_admission_stop.get() is None


async def test_shutdown_interrupts_pressure_without_another_mutation(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    stopped = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json=pressure())

    async def stop_on_sleep(delay: float) -> None:
        nonlocal stopped
        stopped = True

    monkeypatch.setattr(retry_module, "asyncio", SimpleNamespace(sleep=stop_on_sleep))
    async with client_for(handler) as client:
        with worker_scope(lambda: stopped), pytest.raises(ServerError):
            await client.complete_activity_task(
                task_id="task", activity_attempt_id="attempt-7", lease_owner="worker", result="done",
            )
    assert calls == 1


@pytest.mark.parametrize("kind", ["activity", "query", "workflow"])
async def test_runtime_payload_upload_and_acknowledgement_are_not_repeated(
    kind: str, retry_sleeps: list[float],
) -> None:
    server = FakeRuntimePayloadServer()
    uploads: list[bytes] = []
    acknowledgements: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/api/external-payloads/v1":
            uploads.append(request.content)
            if len(uploads) <= 3:
                return httpx.Response(503, json=pressure())
        if request.url.path.endswith("/complete"):
            acknowledgements.append(request.content)
            if len(acknowledgements) <= 4:
                return httpx.Response(503, json=pressure())
            return httpx.Response(200, json={"completed": True})
        return server.handler(request)

    async with runtime_client(server) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(handler))
        with worker_scope():
            if kind == "activity":
                await client.complete_activity_task(
                    task_id="task", activity_attempt_id="attempt-7", lease_owner="worker", result="payload" * 30,
                )
            elif kind == "query":
                await client.complete_query_task(
                    query_task_id="task", query_task_attempt=7, lease_owner="worker", result="payload" * 30,
                )
            else:
                await client.complete_workflow_task(
                    task_id="task", workflow_task_attempt=7, lease_owner="worker",
                    commands=[{"type": "complete_workflow", "result": serializer.envelope("payload" * 30)}],
                )
    assert len(uploads) == 4
    assert len(set(uploads)) == 1
    assert server.upload_count == 1
    assert len(acknowledgements) == 5
    assert len(set(acknowledgements)) == 1


@pytest.mark.parametrize("fails", [False, True])
async def test_running_worker_preserves_activity_outcome_without_reexecution(
    fails: bool, retry_sleeps: list[float],
) -> None:
    calls = 0
    claims = 0
    heartbeats = 0
    acks: list[bytes] = []
    completed = asyncio.Event()

    @activity.defn(name="storage.activity")
    async def receipt() -> dict[str, bool]:
        nonlocal calls
        calls += 1
        await activity.context().heartbeat({"step": "receipt"})
        if fails:
            raise ValueError("intentional activity failure")
        return {"receipt": True}

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal claims, heartbeats
        path = request.url.path
        if path.endswith("/cluster/info"):
            info = compatible_cluster_info()
            info["worker_protocol"]["server_capabilities"]["query_tasks"] = False
            return httpx.Response(200, json=info)
        if path.endswith("/register") or request.method == "DELETE":
            return httpx.Response(200, json={"registered": True})
        if path.endswith("/activity-tasks/poll"):
            claims += 1
            if claims == 1:
                return httpx.Response(200, json={"task": {
                    "task_id": "storage-task", "activity_type": "storage.activity",
                    "activity_attempt_id": "attempt-7", "attempt_number": 7,
                    "payload_codec": "avro", "arguments": serializer.envelope([]),
                }})
        if path.endswith("/storage-task/heartbeat"):
            heartbeats += 1
            if heartbeats <= 3:
                return httpx.Response(503, json=pressure())
            return httpx.Response(200, json={"cancel_requested": False})
        if path.endswith("/storage-task/fail" if fails else "/storage-task/complete"):
            acks.append(request.content)
            if len(acks) <= 4:
                return httpx.Response(503, json=pressure())
            completed.set()
            return httpx.Response(200, json={"recorded": True})
        assert path.endswith("/poll"), f"Unexpected request: {request.method} {path}"
        return httpx.Response(200, json={"task": None})

    async with client_for(handler) as client:
        worker = Worker(client, task_queue="orders", activities=[receipt], max_concurrent_activity_tasks=1)
        run = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(completed.wait(), 2)
        finally:
            await worker.stop()
            await run
    assert calls == 1
    assert heartbeats == 4
    assert len(acks) == 5
    assert len(set(acks)) == 1
    assert worker._act_semaphore._value == 1


async def test_query_thread_uses_pressure_retries(retry_sleeps: list[float]) -> None:
    completed = threading.Event()
    calls = 0
    claims = 0
    acks: list[bytes] = []

    @workflow.defn(name="storage.query")
    class QueryWorkflow:
        def run(self, ctx):
            yield ctx.sleep(30)

        @workflow.query("status")
        def status(self) -> str:
            nonlocal calls
            calls += 1
            return "waiting"

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal claims
        path = request.url.path
        if path.endswith("/query-tasks/poll"):
            claims += 1
            if claims == 1:
                return httpx.Response(200, json={"task": {
                    "query_task_id": "query-task", "query_task_attempt": 7,
                    "workflow_type": "storage.query", "workflow_id": "workflow", "run_id": "run",
                    "query_name": "status", "payload_codec": "avro", "history_events": [],
                    "arguments": serializer.envelope([]),
                }})
            return httpx.Response(200, json={"task": None})
        if path.endswith("/query-task/complete"):
            acks.append(request.content)
            if len(acks) <= 4:
                return httpx.Response(503, json=pressure())
            completed.set()
            return httpx.Response(200, json={"completed": True})
        raise AssertionError(f"Unexpected query request: {path}")

    async with client_for(handler) as client:
        worker = Worker(client, task_queue="orders", workflows=[QueryWorkflow])
        worker._clone_client_for_query_tasks = lambda: client_for(handler)
        worker._start_query_task_thread()
        try:
            assert await asyncio.to_thread(completed.wait, 2)
        finally:
            worker._request_query_task_thread_stop()
            await worker._stop_query_task_thread(deadline=asyncio.get_running_loop().time() + 2)
    assert calls == 1
    assert len(acks) == 5
    assert len(set(acks)) == 1


@pytest.mark.parametrize("phase", ["registration", "poll", "completion"])
async def test_run_until_timeout_interrupts_storage_pressure(phase: str) -> None:
    refused = 0
    deregistered = 0

    @workflow.defn(name="storage.timeout")
    class TimeoutWorkflow:
        def run(self, ctx):
            return "done"

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal refused, deregistered
        path = request.url.path
        if path.endswith("/cluster/info"):
            info = compatible_cluster_info()
            info["worker_protocol"]["server_capabilities"]["query_tasks"] = False
            return httpx.Response(200, json=info)
        if request.method == "DELETE":
            deregistered += 1
            return httpx.Response(200, json={})
        if (
            (phase == "registration" and path.endswith("/register"))
            or (phase == "poll" and path.endswith("/poll"))
            or (phase == "completion" and path.endswith("/complete"))
        ):
            refused += 1
            poll_id = json.loads(request.content)["poll_request_id"] if phase == "poll" else None
            return httpx.Response(503, json=pressure(poll_id))
        if path.endswith("/register"):
            return httpx.Response(200, json={})
        if path.endswith("/workflows/storage-timeout"):
            return httpx.Response(200, json={"status": "running"})
        if path.endswith("/workflow-tasks/poll"):
            return httpx.Response(200, json={"task": {
                "task_id": "task", "workflow_task_attempt": 1, "workflow_type": "storage.timeout",
                "payload_codec": "avro", "arguments": serializer.envelope([]), "history_events": [],
            }})
        raise AssertionError(f"Unexpected request: {path}")

    async with client_for(handler) as client:
        worker = Worker(client, task_queue="orders", workflows=[TimeoutWorkflow], shutdown_timeout=0.2)
        with pytest.raises(TimeoutError, match="storage-timeout"):
            await asyncio.wait_for(worker.run_until(workflow_id="storage-timeout", timeout=0.05), 1)
    assert refused == 1
    assert deregistered == (0 if phase == "registration" else 1)
    assert worker._stop.is_set()
    assert worker._registration_done.is_set()
    assert worker._workflow_reserved == 0
    assert not worker._in_flight
    assert _worker_storage_admission_stop.get() is None


async def test_stop_interrupts_registration_pressure() -> None:
    refused = asyncio.Event()
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.url.path.endswith("/cluster/info"):
            return httpx.Response(200, json=compatible_cluster_info())
        assert request.url.path.endswith("/register")
        calls += 1
        refused.set()
        return httpx.Response(503, json=pressure())

    async with client_for(handler) as client:
        worker = Worker(client, task_queue="orders")
        run = asyncio.create_task(worker.run())
        try:
            await asyncio.wait_for(refused.wait(), 1)
        finally:
            await asyncio.wait_for(worker.stop(), 1)
        with pytest.raises(ServerError):
            await run
    assert calls == 1
    assert worker._registration_done.is_set()


@pytest.mark.parametrize("status,reason", [(401, "unauthenticated"), (409, "activity_attempt_expired")])
async def test_recovery_does_not_retry_auth_or_lease_rejection(
    status: int, reason: str, retry_sleeps: list[float],
) -> None:
    requests: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.content)
        if len(requests) <= 3:
            return httpx.Response(503, json=pressure())
        return httpx.Response(status, json={"reason": reason})

    async with client_for(handler) as client:
        with worker_scope(), pytest.raises(Unauthorized if status == 401 else ServerError) as error:
            await client.complete_activity_task(
                task_id="task", activity_attempt_id="attempt-7", lease_owner="worker", result="receipt",
            )
    if isinstance(error.value, ServerError):
        assert error.value.status == status
        assert error.value.reason() == reason
    assert len(requests) == 4
    assert len(set(requests)) == 1


async def test_legacy_validation_poll_without_identity_fails_closed(retry_sleeps: list[float]) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        body = pressure("")
        body["poll_request_id"] = None
        return httpx.Response(503, json=body)

    async with client_for(handler) as client:
        with worker_scope(), pytest.raises(ServerError):
            await client.poll_update_validation_task(worker_id="worker", task_queue="orders")
    assert calls == 1
    assert not retry_sleeps


@pytest.mark.parametrize("approved", [True, False])
async def test_update_validator_is_not_reexecuted_for_acknowledgement(
    approved: bool, retry_sleeps: list[float],
) -> None:
    ValidatedUpdateWorkflow.validator_calls = 0
    ValidatedUpdateWorkflow.handler_calls = 0
    requests: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/approve" if approved else "/reject")
        requests.append(request.content)
        if len(requests) <= 4:
            return httpx.Response(503, json=pressure())
        return httpx.Response(200, json={})

    async with client_for(handler) as client:
        worker = Worker(client, task_queue="orders", workflows=[ValidatedUpdateWorkflow])
        with worker_scope():
            outcome = await worker._run_update_validation_task({
                "update_validation_task_id": "validation-task", "update_validation_attempt": 7,
                "workflow_type": "validated-update-wf", "update_name": "approve",
                "history_events": [], "workflow_arguments": serializer.envelope([]),
                "update_arguments": serializer.envelope([approved]), "payload_codec": "avro",
            })
    assert outcome == ("approved" if approved else "rejected")
    assert ValidatedUpdateWorkflow.validator_calls == 1
    assert ValidatedUpdateWorkflow.handler_calls == 0
    assert len(requests) == 5
    assert len(set(requests)) == 1


async def test_cold_replay_completion_retries_only_its_prepared_acknowledgement(
    retry_sleeps: list[float], monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.test_golden_history_replay import GoldenSingleActivityWorkflow

    path = Path(__file__).parent / "fixtures/replay_regressions/storage-paused-cold-completion.json"
    fixture = json.loads(path.read_text())
    replays = 0
    original = GoldenSingleActivityWorkflow.run

    def counted_run(self, *args):
        nonlocal replays
        replays += 1
        return original(self, *args)

    monkeypatch.setattr(GoldenSingleActivityWorkflow, "run", counted_run)
    requests: list[bytes] = []
    completed = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal completed
        path = request.url.path
        if path.endswith("/cluster/info"):
            info = compatible_cluster_info()
            info["worker_protocol"]["server_capabilities"]["query_tasks"] = False
            return httpx.Response(200, json=info)
        if path.endswith("/register") or request.method == "DELETE":
            return httpx.Response(200, json={})
        if path.endswith("/workflows/cold-replay"):
            return httpx.Response(200, json={"status": "completed" if completed else "running"})
        if path.endswith("/workflow-tasks/poll"):
            return httpx.Response(200, json={"task": {
                "task_id": "task", "workflow_task_attempt": 7, "workflow_type": fixture["workflow"]["type"],
                "payload_codec": "avro", "arguments": serializer.envelope(fixture["workflow"]["input"]),
                "history_events": fixture["history"],
            }})
        assert path.endswith("/workflow-tasks/task/complete"), f"Unexpected request: {path}"
        requests.append(request.content)
        if len(requests) <= 7:
            return httpx.Response(503, json=pressure())
        completed = True
        return httpx.Response(200, json={})

    async with client_for(handler) as client:
        worker = Worker(client, task_queue="orders", workflows=[GoldenSingleActivityWorkflow])
        result = await worker.run_until(workflow_id="cold-replay", timeout=2)
    assert result.status == "completed"
    assert replays == 1
    assert len(requests) == 8
    assert len(set(requests)) == 1
    command, = json.loads(requests[0])["commands"]
    assert command["type"] == "complete_workflow"
    assert serializer.decode_envelope(command["result"]) == fixture["expected"]["result"]
