from __future__ import annotations

import hashlib
import json
from typing import Any

import httpx
import pytest

from durable_workflow import Replayer, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import (
    ExternalPayloadError,
    ExternalPayloadExpired,
    ExternalPayloadIntegrityMismatch,
    ExternalPayloadNotFound,
    ExternalPayloadOversized,
    ExternalPayloadUnauthorized,
    ExternalPayloadUnavailable,
    ExternalPayloadUnsupported,
    RuntimeCapabilityUnsupported,
)
from durable_workflow.external_storage import (
    RUNTIME_EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
    ExternalPayloadCache,
)
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext


@workflow.defn(name="runtime-payload-replay")
class RuntimePayloadReplayWorkflow:
    def run(self, ctx: WorkflowContext, value: str):  # type: ignore[no-untyped-def]
        return value


class FakeRuntimePayloadServer:
    def __init__(self, *, threshold_bytes: int = 1, max_payload_bytes: int = 1024 * 1024) -> None:
        self.threshold_bytes = threshold_bytes
        self.max_payload_bytes = max_payload_bytes
        self.payloads: dict[str, bytes] = {}
        self.references: dict[str, dict[str, Any]] = {}
        self.requests: list[tuple[str, str, Any]] = []
        self.fetch_count = 0
        self.upload_count = 0
        self.responses: dict[tuple[str, str], object] = {}
        self.fetch_error: tuple[int, str, bool] | None = None
        self.fetch_bytes: bytes | None = None

    def cluster_info(self) -> dict[str, Any]:
        return {
            "namespace": {
                "name": "billing",
                "external_payload_storage": {
                    "schema": RUNTIME_EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
                    "configured": True,
                    "enabled": True,
                    "status": "available",
                    "threshold_bytes": self.threshold_bytes,
                    "provider_details_exposed": False,
                    "transport": {
                        "schema": "durable-workflow.v2.runtime-external-payload-transport.v1",
                        "version": 1,
                        "reference_schema": RUNTIME_EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
                        "mode": "authenticated_namespace_runtime",
                        "upload": {
                            "method": "POST",
                            "path": "/api/external-payloads/v1",
                        },
                        "fetch": {
                            "method": "GET",
                            "path_template": "/api/external-payloads/v1/{referenceId}",
                        },
                        "limits": {
                            "max_payload_bytes": self.max_payload_bytes,
                            "request_timeout_seconds": 5,
                        },
                    },
                },
            },
            "worker_protocol": {
                "server_capabilities": {
                    "runtime_external_payload_transport": True,
                    "query_tasks": True,
                },
            },
            "control_plane": {
                "request_contract": {
                    "operations": {
                        "update": {
                            "fields": {
                                "wait_for": {
                                    "canonical_values": ["accepted", "completed"],
                                },
                            },
                        },
                    },
                },
            },
        }

    def seed(self, blob: str) -> dict[str, Any]:
        data = blob.encode("utf-8")
        sha256 = hashlib.sha256(data).hexdigest()
        reference_id = f"ep_{len(self.references) + 1:026d}"
        reference = {
            "schema": RUNTIME_EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
            "reference_id": reference_id,
            "codec": "avro",
            "size_bytes": len(data),
            "sha256": sha256,
        }
        self.payloads[reference_id] = data
        self.references[reference_id] = reference
        return reference

    def handler(self, request: httpx.Request) -> httpx.Response:
        method = request.method
        path = request.url.path
        if path == "/api/cluster/info":
            return httpx.Response(200, json=self.cluster_info())

        if method == "POST" and path == "/api/external-payloads/v1":
            self.upload_count += 1
            data = request.content
            assert request.headers["x-namespace"] == "billing"
            assert request.headers["authorization"] == "Bearer runtime-role-token"
            assert request.headers["content-type"] == "application/octet-stream"
            assert int(request.headers["x-durable-workflow-payload-size"]) == len(data)
            assert request.headers["x-durable-workflow-payload-sha256"] == hashlib.sha256(data).hexdigest()
            reference = self.seed(data.decode("utf-8"))
            return httpx.Response(
                201,
                json={
                    "schema": "durable-workflow.v2.runtime-external-payload-upload.v1",
                    "transport_version": 1,
                    "reference": reference,
                },
            )

        if method == "GET" and path.startswith("/api/external-payloads/v1/"):
            self.fetch_count += 1
            if self.fetch_error is not None:
                status, reason, retryable = self.fetch_error
                return httpx.Response(
                    status,
                    json={
                        "schema": "durable-workflow.v2.runtime-external-payload-error.v1",
                        "reason": reason,
                        "message": reason.replace("_", " "),
                        "retryable": retryable,
                        "status": status,
                    },
                )
            reference_id = path.rsplit("/", 1)[1]
            reference = self.references[reference_id]
            data = self.fetch_bytes if self.fetch_bytes is not None else self.payloads[reference_id]
            return httpx.Response(
                200,
                content=data,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Durable-Workflow-Payload-Codec": reference["codec"],
                    "X-Durable-Workflow-Payload-Size": str(reference["size_bytes"]),
                    "X-Durable-Workflow-Payload-SHA256": reference["sha256"],
                },
            )

        body = json.loads(request.content) if request.content else None
        self.requests.append((method, path, body))
        response = self.responses.get((method, path), {"ok": True})
        if callable(response):
            response = response(request)
        return httpx.Response(200, json=response)


def runtime_client(
    server: FakeRuntimePayloadServer,
    *,
    cache: ExternalPayloadCache | None = None,
    retry_policy: TransportRetryPolicy | None = None,
) -> Client:
    client = Client(
        "https://runtime.example",
        token="runtime-role-token",
        namespace="billing",
        external_storage_cache=cache,
        retry_policy=retry_policy,
    )
    client._http = httpx.AsyncClient(
        base_url=client.base_url,
        transport=httpx.MockTransport(server.handler),
    )
    return client


def runtime_envelope(server: FakeRuntimePayloadServer, value: Any) -> dict[str, Any]:
    blob = serializer.encode(value)
    return {"codec": "avro", "external_payload": server.seed(blob)}


@pytest.mark.asyncio
async def test_client_start_and_result_use_runtime_credentials_without_provider_setup() -> None:
    server = FakeRuntimePayloadServer()
    result_envelope = runtime_envelope(server, {"result": "x" * 128})
    server.responses[("POST", "/api/workflows")] = {
        "workflow_id": "runtime-payload-workflow",
        "run_id": "run-1",
        "workflow_type": "runtime-payload-replay",
    }
    server.responses[("GET", "/api/workflows/runtime-payload-workflow")] = {
        "workflow_id": "runtime-payload-workflow",
        "run_id": "run-1",
        "workflow_type": "runtime-payload-replay",
        "status": "completed",
        "payload_codec": "avro",
    }
    server.responses[("GET", "/api/workflows/runtime-payload-workflow/runs/run-1/history")] = {
        "events": [
            {
                "event_type": "WorkflowCompleted",
                "payload": {"payload_codec": "avro", "output": result_envelope},
            },
        ],
    }
    client = runtime_client(server)

    try:
        handle = await client.start_workflow(
            workflow_type="runtime-payload-replay",
            task_queue="python",
            workflow_id="runtime-payload-workflow",
            input=["x" * 128],
        )
        result = await handle.result(poll_interval=0, timeout=1)
    finally:
        await client.aclose()

    start_body = next(body for method, path, body in server.requests if method == "POST" and path == "/api/workflows")
    assert start_body["input"]["external_payload"]["schema"] == RUNTIME_EXTERNAL_PAYLOAD_REFERENCE_SCHEMA
    assert "blob" not in start_body["input"]
    assert "external_storage" not in start_body["input"]
    assert result == {"result": "x" * 128}
    assert server.upload_count == 1
    assert server.fetch_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("worker", "path", "body"),
    [
        (False, "/workflows/wf/signal/change", {"input": serializer.envelope(["signal" * 32])}),
        (False, "/workflows/wf/query/state", {"input": serializer.envelope(["query" * 32])}),
        (False, "/workflows/wf/update/change", {"input": serializer.envelope(["update" * 32])}),
        (False, "/schedules", {"action": {"input": serializer.envelope(["schedule" * 32])}}),
        (False, "/workflows/wf/runs/run-1/streams/items", {"payload": serializer.envelope("stream" * 32)}),
        (
            True,
            "/worker/workflow-tasks/task-1/complete",
            {"commands": [{"result": serializer.envelope("workflow" * 32)}]},
        ),
        (True, "/worker/activity-tasks/task-2/complete", {"result": serializer.envelope("activity" * 32)}),
        (True, "/worker/query-tasks/task-3/complete", {"result_envelope": serializer.envelope("query-result" * 32)}),
        (
            True,
            "/worker/workflow-tasks/task-4/complete",
            {
                "commands": [
                    {
                        "type": "record_side_effect",
                        "result": serializer.encode("side-effect" * 32),
                    },
                ],
            },
        ),
    ],
)
async def test_payload_bearing_operations_externalize_at_the_runtime_boundary(
    worker: bool,
    path: str,
    body: dict[str, Any],
) -> None:
    server = FakeRuntimePayloadServer()
    client = runtime_client(server)
    try:
        await client._request("POST", path, worker=worker, json=body)
    finally:
        await client.aclose()

    sent_body = server.requests[-1][2]
    rendered = json.dumps(sent_body)
    assert '"external_payload"' in rendered
    assert '"external_storage"' not in rendered
    assert '"blob"' not in rendered


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("worker", "path"),
    [
        (True, "/worker/workflow-tasks/poll"),
        (True, "/worker/activity-tasks/poll"),
        (True, "/worker/workflow-tasks/task-1/history"),
        (False, "/workflows/wf/runs/run-1/history"),
        (False, "/workflows/wf/runs/run-1/history/export"),
        (False, "/workflows/wf/runs/run-1/streams/items"),
    ],
)
async def test_polls_history_replay_and_streams_fetch_references_before_return(
    worker: bool,
    path: str,
) -> None:
    server = FakeRuntimePayloadServer()
    expected = {"path": path, "value": "x" * 128}
    server.responses[("GET", f"/api{path}")] = {"payload": runtime_envelope(server, expected)}
    client = runtime_client(server)
    try:
        result = await client._request("GET", path, worker=worker)
    finally:
        await client.aclose()

    assert serializer.decode_envelope(result["payload"]) == expected
    assert "external_payload" not in json.dumps(result)


@pytest.mark.asyncio
async def test_history_fetched_from_runtime_can_replay_offline() -> None:
    server = FakeRuntimePayloadServer()
    history = {
        "events": [
            {
                "event_type": "WorkflowStarted",
                "payload": {
                    "workflow_type": "runtime-payload-replay",
                    "arguments": runtime_envelope(server, ["replayed"]),
                },
            },
        ],
    }
    server.responses[("GET", "/api/workflows/wf/runs/run-1/history")] = history
    client = runtime_client(server)
    try:
        resolved_history = await client.get_history("wf", "run-1")
    finally:
        await client.aclose()

    outcome = Replayer(workflows=[RuntimePayloadReplayWorkflow]).replay(resolved_history)
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == "replayed"


@pytest.mark.asyncio
async def test_externalized_query_completion_does_not_duplicate_large_raw_result() -> None:
    server = FakeRuntimePayloadServer()
    client = runtime_client(server)
    result = {"query": "x" * 128}
    try:
        await client.complete_query_task(
            query_task_id="query-1",
            lease_owner="worker-1",
            query_task_attempt=1,
            result=result,
        )
    finally:
        await client.aclose()

    completion = next(
        body
        for method, path, body in server.requests
        if method == "POST" and path == "/api/worker/query-tasks/query-1/complete"
    )
    assert completion["result"] is None
    assert "external_payload" in completion["result_envelope"]


@pytest.mark.asyncio
async def test_query_response_hydrates_user_result_from_externalized_envelope() -> None:
    server = FakeRuntimePayloadServer()
    expected = {"query": "x" * 128}
    server.responses[("POST", "/api/workflows/wf/query/state")] = {
        "result": None,
        "result_envelope": runtime_envelope(server, expected),
    }
    client = runtime_client(server)
    try:
        result = await client.query_workflow("wf", "state")
    finally:
        await client.aclose()

    assert result["result"] == expected


@pytest.mark.asyncio
async def test_worker_restart_uses_a_new_bounded_cache_and_fetches_again() -> None:
    server = FakeRuntimePayloadServer()
    envelope = runtime_envelope(server, {"restart": "safe"})
    server.responses[("GET", "/api/worker/activity-tasks/poll")] = {"task": {"arguments": envelope}}

    first = runtime_client(server, cache=ExternalPayloadCache(max_entries=1, max_bytes=1024))
    try:
        first_poll = await first._request("GET", "/worker/activity-tasks/poll", worker=True)
    finally:
        await first.aclose()

    second = runtime_client(server, cache=ExternalPayloadCache(max_entries=1, max_bytes=1024))
    try:
        second_poll = await second._request("GET", "/worker/activity-tasks/poll", worker=True)
    finally:
        await second.aclose()

    assert serializer.decode_envelope(first_poll["task"]["arguments"]) == {"restart": "safe"}
    assert serializer.decode_envelope(second_poll["task"]["arguments"]) == {"restart": "safe"}
    assert server.fetch_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "reason", "retryable", "error_type"),
    [
        (404, "external_payload_not_found", False, ExternalPayloadNotFound),
        (410, "external_payload_expired", False, ExternalPayloadExpired),
        (403, "external_payload_unauthorized", False, ExternalPayloadUnauthorized),
        (413, "external_payload_oversized", False, ExternalPayloadOversized),
        (415, "external_payload_unsupported", False, ExternalPayloadUnsupported),
        (422, "external_payload_integrity_mismatch", False, ExternalPayloadIntegrityMismatch),
    ],
)
async def test_runtime_fetch_failures_are_typed_and_non_retryable(
    status: int,
    reason: str,
    retryable: bool,
    error_type: type[ExternalPayloadError],
) -> None:
    server = FakeRuntimePayloadServer()
    server.fetch_error = (status, reason, retryable)
    server.responses[("GET", "/api/reference")] = {"payload": runtime_envelope(server, "typed")}
    client = runtime_client(
        server,
        retry_policy=TransportRetryPolicy(max_attempts=3, initial_backoff_seconds=0, jitter=False),
    )
    try:
        with pytest.raises(error_type) as raised:
            await client._request("GET", "/reference")
    finally:
        await client.aclose()

    assert raised.value.retryable is retryable
    assert server.fetch_count == 1


@pytest.mark.asyncio
async def test_runtime_unavailable_fetch_retries_then_raises_typed_failure() -> None:
    server = FakeRuntimePayloadServer()
    server.fetch_error = (503, "external_payload_unavailable", True)
    server.responses[("GET", "/api/reference")] = {"payload": runtime_envelope(server, "retry")}
    client = runtime_client(
        server,
        retry_policy=TransportRetryPolicy(max_attempts=2, initial_backoff_seconds=0, jitter=False),
    )
    try:
        with pytest.raises(ExternalPayloadUnavailable) as raised:
            await client._request("GET", "/reference")
    finally:
        await client.aclose()

    assert raised.value.retryable is True
    assert server.fetch_count == 2


@pytest.mark.asyncio
async def test_malformed_runtime_reference_is_rejected_without_fetch() -> None:
    server = FakeRuntimePayloadServer()
    malformed = runtime_envelope(server, "malformed")
    malformed["external_payload"]["reference_id"] = "s3://provider-secret/bucket/key"
    server.responses[("GET", "/api/reference")] = {"payload": malformed}
    client = runtime_client(server)
    try:
        with pytest.raises(ExternalPayloadUnsupported):
            await client._request("GET", "/reference")
    finally:
        await client.aclose()

    assert server.fetch_count == 0


@pytest.mark.asyncio
async def test_fetch_verifies_size_and_sha256_before_avro_decode() -> None:
    server = FakeRuntimePayloadServer()
    envelope = runtime_envelope(server, {"trusted": True})
    original = server.payloads[envelope["external_payload"]["reference_id"]]
    server.fetch_bytes = b"x" * len(original)
    server.responses[("GET", "/api/reference")] = {"payload": envelope}
    client = runtime_client(server)
    try:
        with pytest.raises(ExternalPayloadIntegrityMismatch, match="hash"):
            await client._request("GET", "/reference")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_runtime_limit_rejects_oversized_payload_before_upload() -> None:
    server = FakeRuntimePayloadServer(max_payload_bytes=64)
    client = runtime_client(server)
    try:
        with pytest.raises(ExternalPayloadOversized):
            await client._request("POST", "/workflows", json={"input": serializer.envelope("x" * 1024)})
    finally:
        await client.aclose()

    assert server.upload_count == 0


def test_serializer_never_exposes_an_unresolved_runtime_reference_as_user_data() -> None:
    reference = {
        "schema": RUNTIME_EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
        "reference_id": "ep_00000000000000000000000001",
        "codec": "avro",
        "size_bytes": 10,
        "sha256": "0" * 64,
    }
    with pytest.raises(ExternalPayloadUnsupported, match="resolved by Client"):
        serializer.decode_envelope({"codec": "avro", "external_payload": reference})


@pytest.mark.asyncio
async def test_legacy_provider_discovery_does_not_select_a_direct_adapter() -> None:
    seen_body: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/cluster/info":
            return httpx.Response(
                200,
                json={
                    "namespace": {
                        "external_payload_storage": {
                            "enabled": True,
                            "driver": "s3",
                            "threshold_bytes": 1,
                            "config": {"bucket": "must-not-be-used"},
                        },
                    },
                    "worker_protocol": {"server_capabilities": {}},
                },
            )
        seen_body.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "workflow_id": "wf",
                "run_id": "run",
                "workflow_type": "runtime-payload-replay",
            },
        )

    client = Client("https://runtime.example", token="runtime-role-token", namespace="billing")
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(handler))
    try:
        await client.start_workflow(
            workflow_type="runtime-payload-replay",
            task_queue="python",
            workflow_id="wf",
            input=["x" * 128],
        )
    finally:
        await client.aclose()

    assert "blob" in seen_body["input"]
    assert "external_payload" not in seen_body["input"]
    assert "external_storage" not in seen_body["input"]


@pytest.mark.asyncio
async def test_runtime_requires_explicit_capability_for_direct_provider_references() -> None:
    server = FakeRuntimePayloadServer()
    client = runtime_client(server)
    direct_reference = {
        "codec": "avro",
        "external_storage": {
            "schema": "durable-workflow.v2.external-payload-reference.v1",
            "uri": "s3://application-owned/payload",
            "codec": "avro",
            "size_bytes": 7,
            "sha256": "0" * 64,
        },
    }
    try:
        with pytest.raises(RuntimeCapabilityUnsupported, match="runtime-mediated"):
            await client._request(
                "POST",
                "/workflows",
                json={"input": direct_reference},
            )
    finally:
        await client.aclose()

    assert server.requests == []
