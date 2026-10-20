from __future__ import annotations

import json
import logging
from typing import Any

import httpx
import pytest
from opentelemetry import baggage
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from durable_workflow import activity, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import ServerError
from durable_workflow.telemetry import ExecutionTelemetry, current_log_context
from durable_workflow.telemetry_carrier import CAPABILITY
from durable_workflow.worker import Worker

ORIGIN = "00-12345678901234567890123456789012-1234567890123456-01"
DISPATCH = "00-abcdef78901234567890123456789012-abcdef7890123456-01"
observed: list[Any] = []


def envelope(parent: str = ORIGIN, **headers: str) -> dict[str, Any]:
    return {"schema": "durable-workflow.trace-context", "version": 1,
            "carrier": {"traceparent": parent, **headers}}


@workflow.defn(name="wire.workflow")
class WireWorkflow:
    def run(self, ctx: Any, value: Any) -> Any:
        observed.append((current_log_context(), dict(baggage.get_all()), value))
        return "original-result"
        yield  # pragma: no cover


@activity.defn(name="wire.activity")
def wire_activity() -> str:
    observed.append((current_log_context(), dict(baggage.get_all())))
    return "original-activity-result"


async def fixture(task: dict[str, Any], *, refuse_history: bool = False) -> tuple[Client, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def transport(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/cluster/info":
            return httpx.Response(200, json={"worker_protocol": {
                "server_capabilities": {"observability_context": CAPABILITY}}})
        if request.url.path.endswith("/poll"):
            return httpx.Response(200, json={"task": task, "poll_status": "leased"})
        if "/history" in request.url.path and refuse_history:
            return httpx.Response(403, json={"reason": "original_claim_refused", "message": "original refusal"})
        if request.url.path.endswith("/complete"):
            return httpx.Response(200, json={"recorded": True})
        raise AssertionError(f"Unexpected request {request.method} {request.url.path}")

    client = Client("https://server.example", token="test-token", namespace="tenant")
    await client._http.aclose()
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(transport))
    await client.get_cluster_info()
    return client, requests


def original_task() -> dict[str, Any]:
    return {"task_id": "task", "workflow_id": "workflow", "run_id": "run", "workflow_type": "wire.workflow",
            "workflow_task_attempt": 1, "lease_owner": "worker", "task_queue": "orders", "payload_codec": "avro",
            "arguments": serializer.envelope([{"observability_context": {"business": "preserved"}}]),
            "history_events": [], "observability_context": envelope(baggage="tenant=public,credential=private"),
            "dispatch_observability_context": {"schema": "durable-workflow.queue-trace", "version": 1,
                                                "task_id": "task", "context": envelope(DISPATCH)},
            "observability_links": {"schema": "durable-workflow.trace-links", "version": 1,
                "task_id": "task", "run_id": "run", "namespace": "tenant", "links": [
                    {"kind": "command", "command_id": "accepted", "context": envelope(DISPATCH)},
                    {"kind": "child_outcome", "workflow_link_id": "link", "child_run_id": "child",
                     "child_task_id": None, "event_id": "terminal", "event_type": "WorkflowCancelled",
                     "sequence": 7, "context": envelope(DISPATCH)}]}}


async def test_actual_polled_workflow_recovers_parent_links_and_allowlisted_baggage_without_extra_rpc(
    caplog: Any,
) -> None:
    observed.clear()
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    telemetry = ExecutionTelemetry(tracer_provider=provider, baggage_allowlist=("tenant",))
    client, requests = await fixture(original_task())
    worker = Worker(client, worker_id="worker", task_queue="orders", workflows=[WireWorkflow], interceptors=[telemetry])
    caplog.set_level(logging.DEBUG, logger="durable_workflow.worker")
    try:
        task = await client.poll_workflow_task(worker_id="worker", task_queue="orders", timeout=0)
        commands = await worker._run_workflow_task(task)
        assert commands is not None and commands[0]["type"] == "complete_workflow"
        body = json.loads(requests[-1].content)
        assert serializer.decode_envelope(body["commands"][0]["result"]) == "original-result"
        span, = exporter.get_finished_spans()
        assert span.context.trace_id == int(ORIGIN[3:35], 16) and span.parent.span_id == int(ORIGIN[36:52], 16)
        assert len(span.links) == 3
        assert all(link.context.trace_id == int(DISPATCH[3:35], 16) for link in span.links)
        assert observed[0][0]["namespace"] == "tenant" and observed[0][1] == {"tenant": "public"}
        assert observed[0][2] == {"observability_context": {"business": "preserved"}}
        assert len(requests) == 3  # existing discovery, original poll, original completion
        assert "private" not in caplog.text and "traceparent" not in caplog.text
        assert current_log_context() == {}
    finally:
        await client.aclose()


async def test_refused_history_never_extracts_observation_or_invokes_workflow() -> None:
    observed.clear()
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    task = original_task()
    task["next_history_page_token"] = "original-token"
    client, _ = await fixture(task, refuse_history=True)
    worker = Worker(client, worker_id="worker", task_queue="orders", workflows=[WireWorkflow],
                    interceptors=[ExecutionTelemetry(tracer_provider=provider)])
    try:
        work = await client.poll_workflow_task(worker_id="worker", task_queue="orders", timeout=0)
        with pytest.raises(ServerError) as failure:
            await worker._run_workflow_task(work)
        assert failure.value.reason() == "original_claim_refused"
        assert not observed and not exporter.get_finished_spans() and current_log_context() == {}
    finally:
        await client.aclose()


async def test_remote_activity_uses_real_attempt_and_task_parent_without_extra_authority_rpc() -> None:
    observed.clear()
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    task = original_task()
    task.update(activity_type="wire.activity", activity_execution_id="execution", activity_attempt_id="attempt",
                attempt_number=3, arguments=serializer.envelope([]))
    client, requests = await fixture(task)
    worker = Worker(client, worker_id="worker", task_queue="orders", workflows=[], activities=[wire_activity],
                    interceptors=[ExecutionTelemetry(tracer_provider=provider)])
    try:
        work = await client.poll_activity_task(worker_id="worker", task_queue="orders", timeout=0)
        assert await worker._run_activity_task(work) == "completed"
        spans = {span.name: span for span in exporter.get_finished_spans()}
        assert set(spans) == {"durable.activity.task", "durable.activity.attempt"}
        assert spans["durable.activity.task"].parent.span_id == int(ORIGIN[36:52], 16)
        assert spans["durable.activity.attempt"].parent.span_id == spans["durable.activity.task"].context.span_id
        assert len(spans["durable.activity.task"].links) == 3
        assert observed[0][0]["activity_attempt_id"] == "attempt"
        assert observed[0][0]["activity_attempt_number"] == 3 and not observed[0][1]
        assert len(requests) == 3
        assert serializer.decode_envelope(json.loads(requests[-1].content)["result"]) == "original-activity-result"
        assert current_log_context() == {}
    finally:
        await client.aclose()


@pytest.mark.parametrize("malformed", [False, True])
async def test_insufficient_claim_omits_telemetry_and_malformed_carriers_preserve_original_result(
    malformed: bool,
) -> None:
    observed.clear()
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    task = original_task()
    if malformed:
        task["observability_context"] = envelope("private-header")
        task["dispatch_observability_context"]["task_id"] = "other-task"
        task["observability_links"]["namespace"] = "other-namespace"
    else:
        del task["lease_owner"]
    client, _ = await fixture(task)
    worker = Worker(client, worker_id="worker", task_queue="orders", workflows=[WireWorkflow],
                    interceptors=[ExecutionTelemetry(tracer_provider=provider)])
    try:
        work = await client.poll_workflow_task(worker_id="worker", task_queue="orders", timeout=0)
        commands = await worker._run_workflow_task(work)
        assert commands is not None and commands[0]["type"] == "complete_workflow" and len(observed) == 1
        if malformed:
            span, = exporter.get_finished_spans()
            assert span.parent is None and not span.links
            assert span.attributes["durable.observability_context"] == "discarded"
            assert span.attributes["durable.dispatch_context"] == "discarded"
            assert span.attributes["durable.observability_links"] == "discarded"
            assert "private-header" not in repr(span.attributes)
        else:
            assert not exporter.get_finished_spans() and observed[0][0] == {}
        assert current_log_context() == {}
    finally:
        await client.aclose()
