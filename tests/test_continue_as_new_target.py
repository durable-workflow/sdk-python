from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from durable_workflow import Client, ContinueAsNewTargetUnsupported, Worker, serializer, workflow
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.workflow import ContinueAsNew, WorkflowContext, commands_to_server_commands, replay
from tests.test_worker import compatible_cluster_info


@workflow.defn(name="promoted-continuation")
class PromotedContinuation:
    def run(self, ctx: WorkflowContext, state: str) -> ContinueAsNew | str:
        if state == "before":
            return ctx.continue_as_new("after", target_build="promoted")
        return state


@workflow.defn(name="inherited-continuation")
class InheritedContinuation:
    def run(self, ctx: WorkflowContext) -> ContinueAsNew:
        return ctx.continue_as_new("after")


def test_replay_respects_the_recorded_target_and_legacy_history() -> None:
    recorded = {
        "event_type": "WorkflowContinuedAsNew",
        "payload": {
            "sequence": 1,
            "build_transition": {
                "from_compatibility": None, "to_compatibility": "build-b", "resolution": "promoted",
            },
        },
    }
    assert replay(PromotedContinuation, [recorded], ["before"]).commands == []
    with pytest.raises(NonDeterministicReplayError, match="continue_as_new_target_changed"):
        replay(InheritedContinuation, [recorded], [])
    legacy = {"event_type": "WorkflowContinuedAsNew", "payload": {"sequence": 1}}
    assert replay(InheritedContinuation, [legacy], []).commands == [ContinueAsNew(arguments=["after"])]

    successor = {"event_type": "WorkflowStarted", "payload": recorded["payload"]}
    assert replay(PromotedContinuation, [successor], ["after"]).commands[0].to_server_command("queue")["type"] == (
        "complete_workflow"
    )
    with pytest.raises(ContinueAsNewTargetUnsupported, match="unsupported_target_build"):
        replay(PromotedContinuation, [successor], ["before"])


def test_direct_and_batch_serialization_use_the_same_target() -> None:
    result = replay(PromotedContinuation, [], ["before"], allow_continue_as_new_target_build=True)
    assert result.commands[0].to_server_command("queue")["target_build"] == "promoted"
    assert commands_to_server_commands(result.commands, "queue")[0]["target_build"] == "promoted"
    assert "target_build" not in ContinueAsNew().to_server_command("queue")
    assert "target_build" not in commands_to_server_commands([ContinueAsNew()], "queue")[0]


@pytest.mark.parametrize("supported", [True, False])
async def test_worker_reports_the_refusal_on_the_current_attempt_and_can_retry(supported: bool) -> None:
    writes: list[tuple[str, str, dict[str, Any]]] = []

    def request(req: httpx.Request) -> httpx.Response:
        path = req.url.path
        if path == "/api/cluster/info":
            return httpx.Response(200, json=compatible_cluster_info())
        if path == "/api/worker/register":
            assert req.headers["x-durable-workflow-protocol-version"] == "1.20"
            assert json.loads(req.content)["capability_manifest"]["continue_as_new_target_build"] == {
                "supported": True, "minimum_protocol_version": "1.21",
            }
            return httpx.Response(201, json={
                "registered": True, "worker_id": "worker", "namespace": "default", "task_queue": "queue",
                "protocol_version": "1.21" if supported else "1.20",
                "server_capabilities": {"continue_as_new_target_build": supported},
            })
        if path.endswith(("/complete", "/fail")):
            body = json.loads(req.content)
            writes.append((path, req.headers["x-durable-workflow-protocol-version"], body))
            if path.endswith("/first/complete"):
                return httpx.Response(409, json={
                    "reason": "promoted_build_unavailable", "retryable": True, "request_admitted": False,
                })
            return httpx.Response(200, json={"recorded": True})
        raise AssertionError(f"unexpected request {req.method} {path}")

    async with Client("http://fixture.invalid") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://fixture.invalid", transport=httpx.MockTransport(request))
        worker = Worker(client, task_queue="queue", worker_id="worker", workflows=[PromotedContinuation])
        await worker._register()
        task = {
            "task_id": "first", "workflow_id": "workflow", "run_id": "run",
            "workflow_type": "promoted-continuation", "workflow_task_attempt": 1, "lease_owner": "worker",
            "payload_codec": "avro", "arguments": serializer.envelope(["before"]),
            "history_events": [{"event_type": "WorkflowStarted"}],
        }
        assert await worker._run_workflow_task(task) is None
        if not supported:
            assert writes == []
            return
        result = await worker._run_workflow_task({**task, "task_id": "second", "workflow_task_attempt": 2})
        assert result is not None and result[0]["target_build"] == "promoted"

    assert [path.rsplit("/", 2)[-2:] for path, _, _ in writes] == [
        ["first", "complete"], ["first", "fail"], ["second", "complete"],
    ]
    assert [version for _, version, _ in writes] == ["1.21"] * 3
    assert writes[1][2]["failure"]["reason"] == "promoted_build_unavailable"
    assert writes[1][2]["failure"]["message"] == "The promoted build is unavailable for the successor workflow."
    assert "stack_trace" not in writes[1][2]["failure"]
    assert [(body["lease_owner"], body["workflow_task_attempt"]) for _, _, body in writes] == [
        ("worker", 1), ("worker", 1), ("worker", 2),
    ]


async def test_client_requires_exact_value_and_current_registration() -> None:
    registrations = iter([
        ("1.20", True, "default", "queue"),
        ("1.21", "true", "default", "queue"),
        ("1.21", True, "different-namespace", "queue"),
        ("1.21", True, "default", "different-queue"),
        ("1.21", True, "default", "queue"),
        ("1.21", False, "default", "queue"),
    ])
    completions = []

    def request(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/api/worker/register":
            version, flag, namespace, queue = next(registrations)
            return httpx.Response(201, json={
                "registered": True, "worker_id": "worker", "namespace": namespace, "task_queue": queue,
                "protocol_version": version,
                "server_capabilities": {"continue_as_new_target_build": flag},
            })
        if req.url.path == "/api/cluster/info":
            return httpx.Response(200, json=compatible_cluster_info())
        completions.append((req.headers["x-durable-workflow-protocol-version"], json.loads(req.content)))
        return httpx.Response(200, json={"recorded": True})

    async with Client("http://fixture.invalid") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://fixture.invalid", transport=httpx.MockTransport(request))

        async def complete(command: dict[str, Any], *, owner: str = "worker") -> None:
            await client.complete_workflow_task(
                task_id="task", lease_owner=owner, workflow_task_attempt=1, commands=[command],
            )

        explicit = {"type": "continue_as_new", "target_build": "inherited"}
        for _ in range(4):
            await client.register_worker(worker_id="worker", task_queue="queue", capability_manifest={})
            with pytest.raises(ContinueAsNewTargetUnsupported):
                await complete(explicit)
        await client.register_worker(worker_id="worker", task_queue="queue", capability_manifest={})
        with pytest.raises(ContinueAsNewTargetUnsupported):
            await complete(explicit, owner="unregistered")
        for invalid in (None, False, [], 1, "Promoted", "promoted "):
            with pytest.raises(ValueError, match="invalid_target_build"):
                await complete({"type": "continue_as_new", "target_build": invalid})
        assert completions == []

        await complete(explicit)
        await complete({"type": "continue_as_new"})
        await complete({"type": "complete_workflow"})
        await client.register_worker(worker_id="worker", task_queue="queue", capability_manifest={})
        with pytest.raises(ContinueAsNewTargetUnsupported):
            await complete(explicit)

    assert [protocol for protocol, _ in completions] == ["1.21", "1.21", "1.20"]
    assert completions[0][1]["commands"][0]["target_build"] == "inherited"
    assert "target_build" not in completions[1][1]["commands"][0]
