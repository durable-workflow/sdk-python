from __future__ import annotations

import copy
import json
from typing import Any

import httpx
import pytest
from opentelemetry import propagate
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from durable_workflow import serializer
from durable_workflow.client import Client
from durable_workflow.telemetry import ExecutionTelemetry
from durable_workflow.telemetry_carrier import CAPABILITY


async def test_real_client_discovery_captures_before_http_preserving_commands_and_business_properties() -> None:
    provider = TracerProvider()
    telemetry = ExecutionTelemetry(tracer_provider=provider)
    previous = propagate.get_global_textmap()
    propagate.set_global_textmap(TraceContextTextMapPropagator())
    requests: list[httpx.Request] = []
    capability: Any = dict(CAPABILITY)
    fail_discovery = False

    def transport(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        with provider.get_tracer("http-client").start_as_current_span("HTTP transport"):
            if request.url.path == "/api/cluster/info":
                if fail_discovery:
                    raise httpx.ConnectError("discovery unavailable", request=request)
                return httpx.Response(200, json={
                    "worker_protocol": {"server_capabilities": {"observability_context": capability}},
                    "control_plane": {"request_contract": {"operations": {"update": {
                        "fields": {"wait_for": {"canonical_values": ["accepted", "completed"]}},
                    }}}},
                })
            return httpx.Response(200, json={
                "workflow_id": "order", "run_id": "run", "workflow_type": "orders", "status": "pending",
            })

    client = Client("https://server.example", token="test-token", telemetry=telemetry)
    await client._http.aclose()
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(transport))
    try:
        with provider.get_tracer("application").start_as_current_span("caller") as caller:
            input = [{"observability_context": {"business": "preserved"}}]
            await client.start_workflow(workflow_type="orders", workflow_id="order", task_queue="orders", input=input)
            initial = [request for request in requests if request.method == "POST"]
            assert len(initial) == 1
            assert 'observability_context' not in json.loads(initial[0].content)
            requests.clear()
            await client.get_cluster_info()
            await client.start_workflow(workflow_type="orders", workflow_id="order", task_queue="orders", input=input)
            await client.signal_workflow("order", "ready", args=input, request_id="signal-original")
            await client.update_workflow("order", "status", args=input, request_id="update-original")
            await client.cancel_workflow("order", reason="requested")
            bodies = [json.loads(request.content) for request in requests[1:]]
            assert len(bodies) == 4
            for body in bodies:
                carrier = body["observability_context"]["carrier"]
                assert carrier["traceparent"][36:52] == format(caller.get_span_context().span_id, "016x")
                assert "baggage" not in carrier
            assert serializer.decode_envelope(bodies[0]["input"], codec="avro") == input
            assert bodies[1]['request_id'] == 'signal-original'
            assert bodies[2]['request_id'] == 'update-original'
            commands = [{"type": "start_child_workflow", "workflow_type": "child", "input": serializer.envelope(input)},
                        {"type": "continue_as_new", "input": serializer.envelope([])}]
            original = copy.deepcopy(commands)
            await client.complete_workflow_task(task_id="task", lease_owner="worker", workflow_task_attempt=1,
                                                commands=commands)
            emitted = json.loads(requests[-1].content)
            assert emitted["commands"][0]["observability_context"] == bodies[0]["observability_context"]
            assert emitted["commands"][1]["observability_context"] == bodies[0]["observability_context"]
            assert commands == original
            capability = {**CAPABILITY, "version": 99}
            await client.get_cluster_info()
            await client.start_workflow(workflow_type="orders", workflow_id="order", task_queue="orders")
            assert "observability_context" not in json.loads(requests[-1].content)
            capability = dict(CAPABILITY)
            await client.get_cluster_info()
            fail_discovery = True
            with pytest.raises(httpx.ConnectError):
                await client.get_cluster_info()
            before = len(requests)
            await client.start_workflow(workflow_type="orders", workflow_id="order", task_queue="orders")
            assert len(requests) == before + 1
            assert "observability_context" not in json.loads(requests[-1].content)
    finally:
        await client.aclose()
        propagate.set_global_textmap(previous)
