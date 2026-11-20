"""Server capabilities digest (server#1262) and the history-free completed result."""
from __future__ import annotations

import asyncio
import copy
import json
from typing import Any

import httpx
import pytest

from durable_workflow import Client, serializer
from durable_workflow._capabilities_digest import HEADER, ServerCapabilitiesDigest
from durable_workflow.errors import ServerCapabilitiesDigestError, ServerError, WorkflowFailed

D1, D2 = "sha256:" + "1" * 64, "sha256:" + "2" * 64
CAPS1 = {"activity_lease_renewal": True, "supported_workflow_task_commands": ["complete_workflow"], "v": 1}
CAPS2 = {"activity_lease_renewal": True, "supported_workflow_task_commands": ["complete_workflow"], "v": 2}


def tagged(capabilities: dict[str, Any], digest: str) -> dict[str, Any]:
    return {**copy.deepcopy(capabilities), "digest": digest}


def test_header_is_none_until_a_tagged_copy_is_learned() -> None:
    held = ServerCapabilitiesDigest()
    assert held.header(held.capture()) == "none"
    answer = {"task": None, "server_capabilities": tagged(CAPS1, D1)}
    held.restore(answer, held.capture())
    assert answer["server_capabilities"] == CAPS1, "the digest member is stripped"
    assert held.capture() == (D1, CAPS1)
    assert held.header(held.capture()) == D1


def test_elided_copies_are_restored_up_to_three_levels_deep() -> None:
    held = ServerCapabilitiesDigest()
    held.restore({"server_capabilities": tagged(CAPS1, D1)}, held.capture())
    captured = held.capture()
    answer = {
        "server_capabilities": D1,
        "activity_poll": {"answer": {"server_capabilities": D1, "task": {"server_capabilities": D1}}},
    }
    held.restore(answer, captured)
    assert answer["server_capabilities"] == CAPS1
    assert answer["activity_poll"]["answer"]["server_capabilities"] == CAPS1
    # Deeper than the Server ever elides: left untouched.
    assert answer["activity_poll"]["answer"]["task"]["server_capabilities"] == D1
    answer["server_capabilities"]["v"] = 99
    assert held.capture()[1] == CAPS1, "callers cannot mutate the held pair"


def test_a_digest_string_resolves_against_a_tagged_copy_in_the_same_answer() -> None:
    held = ServerCapabilitiesDigest()
    answer = {"server_capabilities": D2, "workflow_poll": {"answer": {"server_capabilities": tagged(CAPS2, D2)}}}
    held.restore(answer, (None, None))
    assert answer["server_capabilities"] == CAPS2
    assert answer["workflow_poll"]["answer"]["server_capabilities"] == CAPS2


@pytest.mark.parametrize("captured", [(None, None), (D1, CAPS1)])
def test_unknown_digest_is_a_protocol_failure(captured: tuple[Any, Any]) -> None:
    with pytest.raises(ServerCapabilitiesDigestError):
        ServerCapabilitiesDigest().restore({"server_capabilities": D2}, captured)


def test_a_differing_full_object_is_kept_as_is() -> None:
    held = ServerCapabilitiesDigest()
    held.restore({"server_capabilities": tagged(CAPS1, D1)}, held.capture())
    answer = {"server_capabilities": CAPS2}
    held.restore(answer, held.capture())
    assert answer["server_capabilities"] == CAPS2
    assert held.capture() == (D1, CAPS1)


class DigestServer:
    """Elides with the digest each request sent, as server#1262 does."""

    def __init__(self) -> None:
        self.current: tuple[str, dict[str, Any]] = (D1, CAPS1)
        self.sent: list[tuple[str, str | None]] = []
        self.gates: dict[str, asyncio.Event] = {}
        self.status = 200

    def answer_capabilities(self, sent: str | None) -> Any:
        digest, capabilities = self.current
        if sent is None:
            return copy.deepcopy(capabilities)
        return digest if sent == digest else tagged(capabilities, digest)

    async def respond(self, request: httpx.Request) -> httpx.Response:
        sent = request.headers.get(HEADER)
        self.sent.append((request.url.path, sent))
        # Elision is decided when the request is received, before any wait.
        body = {"ok": True, "server_capabilities": self.answer_capabilities(sent)}
        gate = self.gates.get(request.url.path)
        if gate is not None:
            await gate.wait()
        if request.url.path.endswith("/refused"):
            return httpx.Response(409, json={**body, "reason": "stale_attempt", "message": "refused"})
        return httpx.Response(self.status, json=body)


def client_for(server: DigestServer) -> Client:
    client = Client("http://digest.invalid", namespace="17", worker_token="worker-token", token="control-token")
    client._http = httpx.AsyncClient(base_url="http://digest.invalid", transport=httpx.MockTransport(server.respond))
    return client


async def test_worker_requests_opt_in_and_callers_always_see_full_capabilities() -> None:
    server = DigestServer()
    async with client_for(server) as client:
        first = await client._request("POST", "/worker/a", worker=True, json={})
        second = await client._request("POST", "/worker/b", worker=True, json={})
        await client._request("GET", "/workflows/w", json=None)
    assert server.sent == [("/api/worker/a", "none"), ("/api/worker/b", D1), ("/api/workflows/w", None)]
    assert first["server_capabilities"] == CAPS1
    assert second["server_capabilities"] == CAPS1


async def test_error_bodies_are_restored() -> None:
    server = DigestServer()
    async with client_for(server) as client:
        await client._request("POST", "/worker/a", worker=True, json={})
        with pytest.raises(ServerError) as refusal:
            await client._request("POST", "/worker/refused", worker=True, json={})
    assert server.sent[-1] == ("/api/worker/refused", D1)
    assert refusal.value.body["server_capabilities"] == CAPS1


async def test_rolling_deploy_restores_each_answer_against_the_pair_its_request_sent() -> None:
    server = DigestServer()
    async with client_for(server) as client:
        await client._request("POST", "/worker/learn", worker=True, json={})
        server.gates["/api/worker/slow"] = asyncio.Event()
        # In flight under D1, elided with D1 by the Server, answered late.
        slow = asyncio.create_task(client._request("POST", "/worker/slow", worker=True, json={}))
        while len(server.sent) < 2:
            await asyncio.sleep(0)
        server.current = (D2, CAPS2)  # the deploy changes the capabilities
        moved = await client._request("POST", "/worker/fast", worker=True, json={})
        assert moved["server_capabilities"] == CAPS2
        assert client._capabilities_digest.capture() == (D2, CAPS2)
        server.gates["/api/worker/slow"].set()
        late = await slow
        after = await client._request("POST", "/worker/next", worker=True, json={})
    assert late["server_capabilities"] == CAPS1, "the late D1 answer restores the D1 object"
    assert after["server_capabilities"] == CAPS2
    assert [sent for _, sent in server.sent] == ["none", D1, D1, D2]


async def test_poll_transport_envelope_is_restored() -> None:
    async def respond(request: httpx.Request) -> httpx.Response:
        body = {"task": None, "server_capabilities": tagged(CAPS1, D1)}
        envelope = {"schema": "durable-workflow.poll-transport.v1", "status": 200, "headers": {}, "body": body}
        return httpx.Response(200, headers={"content-type": "application/vnd.durable-workflow.poll.v1+json"},
                              content=json.dumps(envelope).encode())

    async with Client("http://digest.invalid", namespace="17", worker_token="worker-token") as client:
        client._http = httpx.AsyncClient(base_url="http://digest.invalid", transport=httpx.MockTransport(respond))
        result = await client._request("POST", "/worker/query-tasks/poll", worker=True, json={"worker_id": "w"})
        assert result["server_capabilities"] == CAPS1
        assert client._capabilities_digest.capture() == (D1, CAPS1)


class ResultServer:
    def __init__(self, status: str, *, run_id: str = "run-2", envelope: bool = True) -> None:
        self.status, self.run_id, self.envelope = status, run_id, envelope
        self.paths: list[str] = []

    async def respond(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        if request.url.path.endswith("/history"):
            if self.status == "failed":
                event = {"event_type": "WorkflowFailed", "payload": {"message": "boom", "exception_class": "E"}}
            else:
                event = {"event_type": "WorkflowCompleted",
                         "payload": {"output": serializer.envelope({"from": "history"})}}
            return httpx.Response(200, json={"events": [event], "next_page_token": None})
        body: dict[str, Any] = {"workflow_id": "wf", "run_id": self.run_id, "workflow_type": "t", "status": self.status}
        if self.envelope and self.status == "completed":
            body["output_envelope"] = serializer.envelope({"from": "status"})
        return httpx.Response(200, json=body)


async def run_result(server: ResultServer, run_id: str | None = None) -> Any:
    async with Client("http://result.invalid", namespace="17", token="control-token") as client:
        transport = httpx.MockTransport(server.respond)
        client._http = httpx.AsyncClient(base_url="http://result.invalid", transport=transport)
        return await client.get_workflow_handle("wf", run_id=run_id).result(timeout=1)


@pytest.mark.parametrize("run_id", [None, "run-2"])
async def test_completed_result_comes_from_the_status_read(run_id: str | None) -> None:
    server = ResultServer("completed")
    assert await run_result(server, run_id) == {"from": "status"}
    assert server.paths == ["/api/workflows/wf"], "no history read"


@pytest.mark.parametrize(("server", "run_id"), [
    (ResultServer("completed", envelope=False), None),
    (ResultServer("completed"), "run-1"),  # the status read names a newer run
])
async def test_completed_result_falls_back_to_history(server: ResultServer, run_id: str | None) -> None:
    assert await run_result(server, run_id) == {"from": "history"}
    assert any(path.endswith("/history") for path in server.paths)


async def test_failed_result_still_reads_history() -> None:
    server = ResultServer("failed")
    with pytest.raises(WorkflowFailed, match="boom"):
        await run_result(server)
    assert server.paths[-1] == "/api/workflows/wf/runs/run-2/history"


async def test_requests_advertise_gzip() -> None:
    # Native Server compresses JSON answers when asked (server#1261); httpx
    # decodes them transparently.
    seen: list[str] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("accept-encoding", ""))
        return httpx.Response(200, json={"ok": True})

    async with Client("http://gzip.invalid", namespace="17", token="control-token") as client:
        default = client._http
        client._http = httpx.AsyncClient(base_url="http://gzip.invalid", transport=httpx.MockTransport(respond),
                                         headers=default.headers)
        await default.aclose()
        await client._request("GET", "/workflows/w")
    assert "gzip" in seen[0]
