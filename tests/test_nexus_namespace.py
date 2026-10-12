import asyncio
import json

import httpx
import pytest

from durable_workflow import Client, serializer, workflow
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.nexus import NexusOperationResult, stable_nexus_idempotency_key
from durable_workflow.workflow import CompleteWorkflow, NexusServiceCall, WorkflowContext, replay
from tests.test_runtime_external_payload_transport import FakeRuntimePayloadServer


@pytest.mark.asyncio
async def test_target_requests_and_concurrent_worker_heartbeat_keep_separate_scopes():
    requests = []

    async def handler(request):
        requests.append(request)
        await asyncio.sleep(0)
        if request.url.path == "/api/cluster/info":
            return httpx.Response(200, json={"namespace": {"name": request.headers["x-namespace"]}})
        return httpx.Response(200, json={"accepted": True, "id": "call-1", "status": "started"})

    async with Client(
        "https://runtime.example", namespace="tenant-b", control_token="control", worker_token="worker",
    ) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(handler))
        client._cluster_info = {"namespace": {"name": "tenant-b"}}
        result, _ = await asyncio.gather(
            client.execute_nexus_operation("greeter", "shared", "greet", ["Ada"], target_namespace="shared",
                                          caller_workflow_instance_id="caller", caller_workflow_run_id="original-run"),
            client.heartbeat_worker(worker_id="caller-worker"),
        )
        assert client.namespace == "tenant-b"
        assert client._cluster_info == {"namespace": {"name": "tenant-b"}}

    nexus = next(request for request in requests if request.url.path.endswith("/execute"))
    heartbeat = next(request for request in requests if request.url.path == "/api/worker/heartbeat")
    assert nexus.headers["x-namespace"] == "shared"
    assert nexus.headers["authorization"] == "Bearer control"
    assert heartbeat.headers["x-namespace"] == "tenant-b"
    assert heartbeat.headers["authorization"] == "Bearer worker"
    body = json.loads(nexus.content)
    assert body["caller_namespace"] == "tenant-b"
    assert body["caller_workflow_instance_id"] == "caller"
    assert body["caller_workflow_run_id"] == "original-run"
    assert result.target_namespace == "shared"
    assert result.to_recorded_payload()["target_namespace"] == "shared"


@pytest.mark.asyncio
async def test_cross_namespace_payload_discovery_upload_and_admission_use_target_scope():
    server = FakeRuntimePayloadServer(threshold_bytes=1)
    requests = []

    def handler(request):
        requests.append(request)
        assert request.headers["x-namespace"] == "shared"
        assert request.headers["authorization"] == "Bearer control"
        if request.url.path == "/api/cluster/info":
            info = server.cluster_info()
            info["namespace"]["name"] = "shared"
            return httpx.Response(200, json=info)
        if request.url.path == "/api/external-payloads/v1":
            reference = server.seed(request.content.decode())
            assert int(request.headers["x-durable-workflow-payload-size"]) == reference["size_bytes"]
            assert request.headers["x-durable-workflow-payload-sha256"] == reference["sha256"]
            return httpx.Response(201, json={
                "schema": "durable-workflow.v2.runtime-external-payload-upload.v1",
                "transport_version": 1, "reference": reference,
            })
        return httpx.Response(200, json={"accepted": True, "id": "call-1", "status": "started"})

    async with Client(
        "https://runtime.example", namespace="tenant-b", control_token="control", worker_token="worker",
    ) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(handler))
        client._cluster_info = {"namespace": {"name": "tenant-b"}}
        client._runtime_external_payload_transport_resolved = True
        result = await client.execute_nexus_operation(
            "greeter", "shared", "greet", ["Ada" * 128], target_namespace="shared",
        )
        assert client.namespace == "tenant-b"
        assert client._runtime_external_payload_transport_cache is None

    assert [request.url.path for request in requests] == [
        "/api/cluster/info", "/api/external-payloads/v1",
        "/api/service-endpoints/greeter/services/shared/operations/greet/execute",
    ]
    body = json.loads(requests[-1].content)
    reference = body["arguments"]["external_payload"]
    assert serializer.decode(server.payloads[reference["reference_id"]].decode(), codec="avro") == ["Ada" * 128]
    assert body["caller_namespace"] == "tenant-b"
    assert serializer.decode(result.request_payload["arguments"], codec="avro") == ["Ada" * 128]


@pytest.mark.asyncio
async def test_target_scope_discards_caller_discovery_payload_cache_and_provider():
    client = Client("https://runtime.example", namespace="tenant-b", token="shared-token")
    client._cluster_info = {"namespace": {"name": "tenant-b"}}
    client._runtime_external_payload_transport_resolved = True
    target = client._nexus_target_client("shared")
    assert target._http is client._http
    assert target.namespace == "shared"
    assert target._cluster_info is None
    assert target._cluster_info_lock is not client._cluster_info_lock
    assert not target._runtime_external_payload_transport_resolved
    assert target.external_storage is None
    assert target.external_storage_cache is not client.external_storage_cache
    await client.aclose()


@workflow.defn(name="nexus-target-caller")
class NexusTargetCaller:
    def run(self, ctx: WorkflowContext, target: str | None):
        result = yield ctx.call_nexus_service("greeter", "shared", "greet", ["Ada"],
                                             target_namespace=target, idempotency_key="original-call")
        return result.service_call_id


@pytest.mark.parametrize("target", [None, "shared"])
def test_original_target_replays_and_changed_target_is_refused(target):
    command = replay(NexusTargetCaller, [], [target], workflow_id="caller", run_id="original-run").commands[0]
    assert isinstance(command, NexusServiceCall)
    result = NexusOperationResult(accepted=True, service_call_id="call-1", endpoint_name="greeter",
                                  service_name="shared", operation_name="greet", target_namespace=target,
                                  request_payload={"idempotency_key": command.idempotency_key}, status="completed")
    history = [{"event_type": "SideEffectRecorded", "payload": {
        "sequence": 1, "payload_codec": "avro", "result": serializer.encode(result.to_recorded_payload(), codec="avro"),
    }}]
    outcome = replay(
        NexusTargetCaller, history, [target], workflow_id="caller", run_id="original-run", payload_codec="avro",
    )
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == "call-1"
    requested = {
        "event_type": "CooperativeCancellationRequested", "recorded_at": "2026-10-12T07:00:00Z",
        "payload": {"workflow_command_id": "original-request", "workflow_run_id": "original-run",
                    "cleanup_deadline_at": "2026-10-12T07:00:30Z"},
    }
    cancelled_replay = replay(
        NexusTargetCaller, [*history, requested], [target], workflow_id="caller", run_id="original-run",
        payload_codec="avro",
    )
    assert cancelled_replay.cancellation_delivery is None
    assert isinstance(cancelled_replay.commands[0], CompleteWorkflow)
    assert cancelled_replay.commands[0].result == "call-1"
    changed = "other" if target else "shared"
    with pytest.raises(NonDeterministicReplayError, match="nexus_target_namespace_changed"):
        replay(NexusTargetCaller, history, [changed], workflow_id="caller", run_id="original-run", payload_codec="avro")


def test_explicit_target_participates_in_call_identity_and_default_stays_compatible():
    identity = dict(workflow_id="caller", run_id="original-run", ordinal=0, endpoint_name="greeter",
                    service_name="shared", operation_name="greet", arguments=["Ada"])
    legacy = stable_nexus_idempotency_key(**identity)
    assert stable_nexus_idempotency_key(**identity, target_namespace=None) == legacy
    assert stable_nexus_idempotency_key(**identity, target_namespace="shared") != legacy
    assert stable_nexus_idempotency_key(**identity, target_namespace="shared") != stable_nexus_idempotency_key(
        **identity, target_namespace="other")


@pytest.mark.asyncio
async def test_empty_target_is_refused_before_network_and_worker_credentials_are_not_reused():
    async with Client("https://runtime.example", namespace="tenant-b", worker_token="worker-only") as client:
        with pytest.raises(ValueError, match="nonempty"):
            await client.execute_nexus_operation("greeter", "shared", "greet", target_namespace=" ")
        with pytest.raises(ValueError, match="control-plane requests require"):
            await client.execute_nexus_operation("greeter", "shared", "greet", target_namespace="shared")
