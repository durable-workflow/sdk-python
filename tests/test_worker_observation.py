from __future__ import annotations

import asyncio
import gc
import itertools
import json
import threading
import weakref
from typing import Any

import httpx
import pytest

from durable_workflow import Client
from durable_workflow.interceptors import ActivityInterceptorContext, WorkflowTaskInterceptorContext
from tests.worker_observation import (
    ObservationInterceptor,
    WorkerObservation,
    observe_and_drain,
    request_task_identity,
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
async def test_additional_clients_have_stable_distinct_bounded_instance_labels(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"task": None}))
    async with httpx.AsyncClient(transport=transport) as first, httpx.AsyncClient(transport=transport) as second:
        await first.get("https://example.test/api/cluster/info?token=SECRET")
        await first.post("https://example.test/api/worker/query-tasks/poll")
        await second.post("https://example.test/api/worker/query-tasks/poll")
    report = observation.report()
    assert [entry["client"] for entry in report["recent"]] == ["additional"] * 3
    assert [entry["client_instance"] for entry in report["recent"]] == ["http-1", "http-1", "http-2"]
    assert report["client_instances"] == {
        "http-1": {"counts": {"GET discovery": 1, "POST query_poll": 1}},
        "http-2": {"counts": {"POST query_poll": 1}},
    }
    assert report["completeness"]["client_attribution"] == "complete"
    assert "SECRET" not in json.dumps(report)


@pytest.mark.asyncio
async def test_client_registry_does_not_retain_clients_or_reuse_their_labels(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as first:
        await first.get("https://example.test/api/cluster/info")
    original = weakref.ref(first)
    del first
    gc.collect()
    assert original() is None
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as second:
        # Reproduce a reused integer ID without relying on allocator timing.
        observation._client_instances[id(second)] = (original, "http-1")
        await second.get("https://example.test/api/cluster/info")
    report = observation.report()
    assert [entry["client_instance"] for entry in report["recent"]] == ["http-1", "http-2"]
    assert len(report["client_instances"]) == 2


@pytest.mark.asyncio
async def test_client_attribution_limit_never_groups_new_clients_as_a_known_instance(monkeypatch: Any) -> None:
    observation = WorkerObservation(limit=1)
    observation.install_http(monkeypatch)
    transport = httpx.MockTransport(lambda request: httpx.Response(200))
    async with httpx.AsyncClient(transport=transport) as first, httpx.AsyncClient(transport=transport) as second:
        await first.get("https://example.test/api/cluster/info")
        await second.get("https://example.test/api/cluster/info")
        overflow = observation.report()
        assert overflow["recent"][0]["client_instance"] is None
        assert overflow["recent"][0]["client_instance_state"] == "truncated"
        await first.get("https://example.test/api/cluster/info")
    report = observation.report()
    assert report["recent"][0]["client_instance"] == "http-1"
    assert report["recent"][0]["client_instance_state"] == "complete"
    assert report["completeness"]["client_attribution"] == "truncated"
    assert len(report["client_instances"]) == 1 and report["unattributed_client_requests"] == 1
    assert overflow["client_instances"]["http-1"]["counts"]["GET discovery"] == 1


def test_query_thread_and_workflow_thread_share_consistent_snapshots(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    entered = [threading.Event(), threading.Event()]
    release = threading.Event()
    errors: list[BaseException] = []

    def run(index: int, family: str) -> None:
        async def execute():
            def reply(request: httpx.Request) -> httpx.Response:
                entered[index].set()
                assert release.wait(5)
                return httpx.Response(200, json={"task": None})
            async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
                await client.post(f"https://example.test/api/worker/{family}-tasks/poll")
                observation.note("request_returned")
        try:
            asyncio.run(execute())
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=run, args=(index, family))
               for index, family in enumerate(("workflow", "query"))]
    try:
        for thread in threads:
            thread.start()
        assert all(event.wait(5) for event in entered)
        before = observation.report()
        assert before["active_count"] == len(before["active"]) == 2
        assert len({entry["client_instance"] for entry in before["active"]}) == 2
    finally:
        release.set()
        for thread in threads:
            thread.join(5)
    assert not errors and not any(thread.is_alive() for thread in threads)
    after = observation.report()
    assert after["requests"] == 2 and after["active_count"] == 0
    assert after["counts"] == {"POST workflow_poll": 1, "POST query_poll": 1}
    assert after["milestones"]["request_returned"]["count"] == 2
    assert {entry["request"] for entry in after["recent"]} == {1, 2}
    assert before["active_count"] == 2 and not before["milestones"]


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
        assert observation.report()["recent"][0]["response_metadata_state"] == "missing"
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
    assert report["omitted_completed"] == 3
    assert report["omitted_milestone_events"] == 3
    assert report["completeness"]["recent_http"] == "truncated"
    assert report["completeness"]["milestones"] == "truncated"
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
            assert report["evicted_active"] == 3
            assert report["completeness"]["active_http"] == "truncated"
        finally:
            for runner in runners:
                runner.cancel()
            await asyncio.gather(*runners, return_exceptions=True)
    assert observation.report()["active_count"] == 0
    assert observation.report()["completeness"]["active_http"] == "truncated"


@pytest.mark.asyncio
@pytest.mark.parametrize(("body", "content_type", "expected"), [
    (b'{"task":null,"recorded":false,"input":"SECRET"}', "application/json", "complete"),
    (b"SECRET", "text/plain", "missing"),
    (b'{"input":"' + b"x" * 32_768 + b'"}', "application/json", "truncated"),
    (b'{"input":"SECRET"', "application/json", "invalid"),
    (b'["SECRET"]', "application/json", "invalid"),
    (b'{"task":"SECRET"}', "application/json", "invalid"),
    (b'{"recorded":"SECRET"}', "application/json", "invalid"),
])
async def test_http_metadata_omissions_have_explicit_states(
    monkeypatch: Any, body: bytes, content_type: str, expected: str,
) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body, headers={"Content-Type": content_type}),
    )) as client:
        response = await client.post("https://example.test/api/worker/workflow-tasks/original/history")
        assert response.content == body
    report = observation.report()
    assert report["counts"] == {"POST workflow_history": 1}
    assert report["recent"][0]["response_metadata_state"] == expected
    assert report["response_metadata_states"] == {expected: 1}
    assert "SECRET" not in json.dumps(report)


def test_middle_milestone_loss_and_summary_omission_are_explicit() -> None:
    observation = WorkerObservation()
    for task_id in ("first", "middle", "last"):
        observation.note("workflow_task_enter", {"task_id": task_id})
    report = observation.report()
    assert report["milestones"]["workflow_task_enter"]["first"]["task_id"] == "first"
    assert report["milestones"]["workflow_task_enter"]["last"]["task_id"] == "last"
    assert report["omitted_milestone_events"] == 1
    assert report["completeness"]["milestones"] == "truncated"


@pytest.mark.asyncio
async def test_recent_records_suppressed_in_report_are_missing_not_complete(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json={}),
    )) as client:
        await client.get("https://example.test/result")
    report = observation.report(include_recent=False)
    assert report["recent"] == []
    assert report["omitted_completed"] == 0
    assert report["completeness"]["recent_http"] == "missing"
    assert report["response_metadata_states"] == {"complete": 1}


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
@pytest.mark.parametrize("primary_kind", [None, RuntimeError, asyncio.CancelledError])
@pytest.mark.parametrize("failure_phase", [None, "report", "emit", "shutdown"])
async def test_boundary_observation_always_drains_and_preserves_primary_error(
    monkeypatch: pytest.MonkeyPatch, primary_kind: type[BaseException] | None, failure_phase: str | None,
) -> None:
    observation = WorkerObservation()
    primary = None if primary_kind is None else primary_kind("SECRET primary")
    secondary = ValueError("SECRET secondary")
    stopped = asyncio.Event()
    drained = []
    records: list[dict[str, Any]] = []
    original_report = observation.report
    report_calls = 0

    def report():
        nonlocal report_calls
        report_calls += 1
        if failure_phase == "report" and report_calls == 1:
            raise secondary
        return original_report()

    def emit(record):
        records.append(record)
        if failure_phase == "emit" and len(records) == 1:
            raise secondary

    class Worker:
        async def stop(self):
            observation.note("stop_requested")
            stopped.set()
            if failure_phase == "shutdown":
                raise secondary

    async def run():
        await stopped.wait()
        drained.append(True)

    monkeypatch.setattr(observation, "report", report)
    runner = asyncio.create_task(run())

    async def case():
        try:
            if primary is not None:
                raise primary
        finally:
            await observe_and_drain(observation, {"run_id": "original-run"}, [Worker()], [runner],
                                    primary_error=primary, emit=emit)

    expected = primary if primary is not None else secondary if failure_phase is not None else None
    if expected is None:
        await case()
    else:
        with pytest.raises(type(expected)) as raised:
            await case()
        assert raised.value is expected
    assert stopped.is_set() and runner.done() and drained == [True]
    assert records[0]["phase"] == "before_shutdown" and records[-1]["phase"] == "after_shutdown"
    assert "stop_requested" not in records[0].get("observation", {}).get("milestones", {})
    assert records[-1]["observation"]["milestones"]["stop_requested"]["count"] == 1
    assert all(record["run_id"] == "original-run" for record in records)
    assert records[0]["observation_capture_state"] == ("invalid" if failure_phase == "report" else "complete")
    assert "SECRET" not in json.dumps(records)


@pytest.mark.asyncio
async def test_shutdown_snapshots_bracket_cleanup_and_do_not_share_mutable_state() -> None:
    ticks = itertools.count()
    observation = WorkerObservation(clock=lambda: float(next(ticks)))
    records: list[dict[str, Any]] = []

    class Worker:
        async def stop(self):
            observation.note("stop_requested")

    await observe_and_drain(observation, {}, [Worker()], [], emit=records.append)
    before, after = records
    stop_at = after["observation"]["milestones"]["stop_requested"]["first"]["at"] + observation.started
    before_report_at = before["observation"]["elapsed"] + observation.started
    assert before["monotonic_before"] <= before_report_at <= before["monotonic_after"]
    assert before["monotonic_after"] <= stop_at <= after["monotonic_before"] <= after["monotonic_after"]
    assert "stop_requested" not in before["observation"]["milestones"]
    assert before["observation_capture_state"] == after["observation_capture_state"] == "complete"


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


@pytest.mark.asyncio
async def test_handler_and_http_keep_scoped_task_and_request_identities_separate(monkeypatch: Any) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    interceptor = ObservationInterceptor(observation)
    task = {"task_id": "original", "run_id": "run-original", "workflow_task_attempt": 0,
            "task_token": "SECRET", "input": "SECRET"}
    context = WorkflowTaskInterceptorContext(worker_id="worker", task_queue="queue", task=task)
    expected = [{"type": "complete_workflow"}]
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))) as client:
        async def execute(context):
            observation.note("workflow_handler_enter")
            await client.post("https://example.test/api/worker/workflow-tasks/different/history?token=SECRET")
            return expected

        assert await interceptor.execute_workflow_task(context, execute) is expected
        observation.note("outside_interceptor")
        await client.post("https://example.test/api/worker/workflow-tasks/original/complete")
    report = observation.report()
    linked = report["milestones"]["workflow_handler_enter"]["first"]
    assert {key: linked[key] for key in ("task_id", "run_id", "workflow_task_attempt")} == {
        "task_id": "original", "run_id": "run-original", "workflow_task_attempt": 0,
    }
    history, completion = report["recent"]
    assert history["request_task"] == {"family": "workflow", "task_id": "different"}
    assert history["interceptor_task"] == {"task_id": "original", "run_id": "run-original", "workflow_task_attempt": 0}
    assert completion["request_task"] == {"family": "workflow", "task_id": "original"}
    assert "interceptor_task" not in completion, "a completed scope must not supply the last attempt to a later request"
    assert "task_id" not in report["milestones"]["outside_interceptor"]["first"]
    assert "SECRET" not in json.dumps(report)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", [None, RuntimeError, asyncio.CancelledError])
async def test_task_context_isolated_across_concurrent_tasks_and_reset_on_exit(
    failure_kind: type[BaseException] | None,
) -> None:
    observation = WorkerObservation()
    interceptor = ObservationInterceptor(observation)
    failure = None if failure_kind is None else failure_kind("SECRET")
    caught = []
    returned = []

    async def run(index: int):
        task = {"task_id": f"original-{index}", "run_id": f"run-{index}", "workflow_task_attempt": index}
        context = WorkflowTaskInterceptorContext(worker_id="worker", task_queue="queue", task=task)

        async def execute(context):
            await asyncio.sleep(0)
            observation.note("workflow_handler_enter")
            if index == 0 and failure is not None:
                raise failure
            return [{"type": "complete_workflow"}]

        try:
            returned.append(await interceptor.execute_workflow_task(context, execute))
        except BaseException as error:
            caught.append(error)
        observation.note("outside_interceptor")

    await asyncio.gather(run(0), run(1))
    assert caught == ([] if failure is None else [failure])
    assert len(returned) == (2 if failure is None else 1)
    report = observation.report()
    handler = report["milestones"]["workflow_handler_enter"]
    assert {handler[where]["task_id"] for where in ("first", "last")} == {"original-0", "original-1"}
    for where in ("first", "last"):
        identity = handler[where]
        assert identity["run_id"] == f"run-{identity['workflow_task_attempt']}"
        assert "task_id" not in report["milestones"]["outside_interceptor"][where]
    assert "SECRET" not in json.dumps(report)


@pytest.mark.parametrize(("path", "expected"), [
    ("/api/worker/activity-tasks/original/complete", {"family": "activity", "task_id": "original"}),
    ("/api/worker/query-tasks/original/complete", {"family": "query", "task_id": "original"}),
    ("/api/worker/workflow-tasks/poll", {}),
    ("/api/worker/workflow-tasks//history", {}),
    ("/api/worker/workflow-tasks/original/other/history", {}),
    ("/api/worker/workflow-tasks/original%2Fother/history", {}),
    ("/api/worker/workflow-tasks/" + "x" * 161 + "/history", {}),
])
def test_request_task_identity_is_bounded_and_ignores_non_task_routes(path: str, expected: dict[str, str]) -> None:
    assert request_task_identity(httpx.Request("POST", "https://example.test" + path + "?token=SECRET")) == expected


@pytest.mark.parametrize(("method", "path", "expected"), [
    ("POST", "/api/worker/register", "registration"),
    ("POST", "/api/worker/registrations/worker/deregister", "deregistration"),
    ("DELETE", "/api/worker/registrations/worker", "deregistration"),
    ("GET", "/api/worker/registrations/worker", "registration"),
    ("POST", "/api/worker/workflow-tasks/original/history", "workflow_history"),
    ("GET", "/api/worker/workflow-tasks/original/history", "workflow_history"),
    ("POST", "/api/worker/workflow-tasks/original/events", "workflow_submission"),
    ("POST", "/api/worker/workflow-tasks/original/other/history", "workflow_submission"),
    ("POST", "/api/worker/workflow-tasks//history", "workflow_submission"),
])
def test_registration_and_deregistration_are_counted_separately(method: str, path: str, expected: str) -> None:
    from tests.worker_observation import route_name

    assert route_name(httpx.Request(method, "http://runtime" + path)) == expected
