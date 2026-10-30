from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from durable_workflow import Client
from durable_workflow.interceptors import ActivityInterceptorContext, WorkflowTaskInterceptorContext
from tests.worker_observation import (
    ObservationInterceptor,
    WorkerObservation,
    result_or_worker_error,
    stop_and_drain,
)


@pytest.mark.asyncio
async def test_http_observation_covers_additional_clients_without_exposing_payloads(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    requests = []

    async def handle(request: httpx.Request) -> httpx.Response:
        requests.append(await request.aread())
        return httpx.Response(503, json={
            "task": {"task_id": "task-original", "workflow_task_attempt": 0, "task_token": "SECRET"},
            "reason": "backend_lock_pressure", "error": {"code": "backend_lock_pressure", "message": "SECRET"},
            "recorded": False, "input": "SECRET",
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as first:
        observation.clients[id(first)] = "workflow"
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as additional:
            for client, family in ((first, "workflow"), (additional, "query")):
                response = await client.post(
                    f"https://example.test/api/worker/{family}-tasks/poll?token=SECRET",
                    headers={"Authorization": "Bearer SECRET"}, content=b"SECRET",
                )
                assert response.json()["input"] == "SECRET"

    report = observation.report()
    assert report["counts"] == {"POST workflow_poll": 1, "POST query_poll": 1}
    assert requests == [b"SECRET", b"SECRET"]
    assert report["active_count"] == 0
    assert report["recent"][1]["client"] == "additional"
    assert report["recent"][1]["workflow_task_attempt"] == 0
    assert report["recent"][1]["reason"] == "backend_lock_pressure"
    assert report["recent"][1]["recorded"] is False
    assert "SECRET" not in json.dumps(report)


@pytest.mark.asyncio
async def test_observer_follows_the_actual_client_registration_route(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    async with Client("https://example.test", token="SECRET", namespace="default") as client:
        original = client._http
        client._http = httpx.AsyncClient(
            base_url=original.base_url, headers=original.headers,
            transport=httpx.MockTransport(lambda request: httpx.Response(201, json={"worker_id": "worker"})),
        )
        await original.aclose()
        await client.register_worker(worker_id="worker", task_queue="queue", capability_manifest={})
    assert observation.report()["counts"].get("POST registration") == 1


@pytest.mark.asyncio
async def test_snapshot_keeps_inflight_registration_before_cancellation(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    started = asyncio.Event()

    async def held(request: httpx.Request) -> httpx.Response:
        started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(held)) as client:
        pending = asyncio.create_task(client.post("https://example.test/api/worker/register"))
        await asyncio.wait_for(started.wait(), 5)
        before = observation.report()
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    assert before["active_count"] == 1
    assert before["active"][0]["route"] == "registration"
    assert "exception" not in before["active"][0]
    after = observation.report()
    assert after["active_count"] == 0
    assert after["recent"][0]["exception"] == "CancelledError"


@pytest.mark.asyncio
async def test_registration_timing_survives_recent_record_eviction(monkeypatch: Any) -> None:
    now = [0.0]
    observation = WorkerObservation(clock=lambda: now[0], limit=1)
    observation.install_http(monkeypatch)

    async def reply(request: httpx.Request) -> httpx.Response:
        now[0] += 3.5 if request.method == "POST" else 0.25
        return httpx.Response(200, json={"status": "completed", "outcome": "completed"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
        await client.post("https://example.test/api/worker/register")
        await client.get("https://example.test/result")
    report = observation.report(include_recent=False)
    assert report["recent"] == []
    assert report["max_elapsed"]["POST registration"] == 3.5
    assert report["max_elapsed"]["GET control"] == 0.25


@pytest.mark.asyncio
async def test_streamed_response_is_not_consumed_by_observer(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    reads = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            reads.append(True)
            yield b'{"task":null}'

    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, stream=Stream(), headers={"Content-Type": "application/json"}),
    )) as client:
        response = await client.send(client.build_request("GET", "https://example.test/stream"), stream=True)
        assert reads == []
        assert "task_state" not in observation.report()["recent"][0]
        assert await response.aread() == b'{"task":null}'
        await response.aclose()


@pytest.mark.asyncio
async def test_observation_bounds_completed_records_and_milestones(monkeypatch: Any) -> None:
    observation = WorkerObservation(limit=2)
    observation.install_http(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as client:
        for index in range(5):
            await client.get(f"https://example.test/record/{index}")
            observation.note(f"phase{index}", {"task_id": f"task{index}", "task_token": "SECRET"})
    report = observation.report()
    assert report["requests"] == 5
    assert len(report["recent"]) == 2
    assert len(report["milestones"]) == 2
    assert "SECRET" not in json.dumps(report)


@pytest.mark.asyncio
async def test_active_record_limit_keeps_the_actual_inflight_count(monkeypatch: Any) -> None:
    observation = WorkerObservation(limit=2)
    observation.install_http(monkeypatch)
    started = asyncio.Event()
    entered = 0

    async def held(request: httpx.Request) -> httpx.Response:
        nonlocal entered
        entered += 1
        if entered == 5:
            started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(held)) as client:
        runners = [asyncio.create_task(client.get("https://example.test/held")) for _ in range(5)]
        try:
            await asyncio.wait_for(started.wait(), 5)
            report = observation.report()
            assert report["active_count"] == 5
            assert len(report["active"]) == 2
            assert report["omitted_active"] == 3
        finally:
            for runner in runners:
                runner.cancel()
            await asyncio.gather(*runners, return_exceptions=True)
    assert observation.report()["active_count"] == 0


@pytest.mark.asyncio
async def test_fatal_worker_is_reported_before_result_wait_and_cancels_wait() -> None:
    cancelled = asyncio.Event()
    started = asyncio.Event()
    failure = RuntimeError("registration failed")

    async def result():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def worker():
        await started.wait()
        raise failure

    runner = asyncio.create_task(worker())
    pending_result = asyncio.wait_for(result_or_worker_error(result(), [runner]), 5)
    with pytest.raises(RuntimeError) as raised:
        await pending_result
    assert raised.value is failure
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_normal_worker_return_does_not_replace_original_result() -> None:
    worker_returned = asyncio.Event()
    expected = object()

    async def result():
        await worker_returned.wait()
        return expected

    async def worker():
        worker_returned.set()

    runner = asyncio.create_task(worker())
    assert await result_or_worker_error(result(), [runner]) is expected


@pytest.mark.asyncio
async def test_stop_failure_still_drains_original_runner() -> None:
    stopped = asyncio.Event()
    drained = []
    failure = RuntimeError("registration delete failed")

    class Worker:
        async def stop(self):
            stopped.set()
            raise failure

    async def run():
        await stopped.wait()
        drained.append(True)

    workers = [Worker()]
    runners = [asyncio.create_task(run())]
    with pytest.raises(RuntimeError) as raised:
        await stop_and_drain(workers, runners)
    assert raised.value is failure
    assert drained == [True]


@pytest.mark.asyncio
async def test_failed_runner_does_not_skip_drain_of_its_peer() -> None:
    release = asyncio.Event()
    drained = []
    failure = RuntimeError("worker startup failed")

    async def failed():
        raise failure

    async def peer():
        await release.wait()
        drained.append(True)

    runners = [asyncio.create_task(failed()), asyncio.create_task(peer())]
    cleanup = asyncio.create_task(stop_and_drain([], runners))
    try:
        for _ in range(10):
            await asyncio.sleep(0)
        release.set()
        with pytest.raises(RuntimeError) as raised:
            await cleanup
        assert raised.value is failure
        assert drained == [True]
    finally:
        release.set()
        await asyncio.gather(*runners, cleanup, return_exceptions=True)


@pytest.mark.asyncio
async def test_interceptors_preserve_results_and_exceptions() -> None:
    observation = WorkerObservation()
    interceptor = ObservationInterceptor(observation)
    task = {"task_id": "original", "workflow_task_attempt": 1, "task_token": "SECRET"}
    context = WorkflowTaskInterceptorContext(worker_id="worker", task_queue="queue", task=task)
    expected = [{"type": "complete_workflow"}]

    async def execute(context):
        return expected

    assert await interceptor.execute_workflow_task(context, execute) is expected
    activity = ActivityInterceptorContext(
        worker_id="worker", task_queue="queue", task=task, activity_type="echo", args=("SECRET",),
    )
    failure = RuntimeError("SECRET")

    async def fail(context):
        raise failure

    with pytest.raises(RuntimeError) as raised:
        await interceptor.execute_activity(activity, fail)
    assert raised.value is failure
    report = observation.report()
    assert report["milestones"]["activity_handler_error"]["count"] == 1
    assert report["milestones"]["workflow_task_enter"]["first"]["task_id"] == "original"
    assert "SECRET" not in json.dumps(report)


@pytest.mark.parametrize(("method", "path", "expected"), [
    ("POST", "/api/worker/register", "registration"),
    ("POST", "/api/worker/registrations/worker/deregister", "deregistration"),
    ("DELETE", "/api/worker/registrations/worker", "deregistration"),
    ("GET", "/api/worker/registrations/worker", "registration"),
])
def test_registration_and_deregistration_are_counted_separately(method: str, path: str, expected: str) -> None:
    from tests.worker_observation import route_name

    assert route_name(httpx.Request(method, "http://runtime" + path)) == expected
