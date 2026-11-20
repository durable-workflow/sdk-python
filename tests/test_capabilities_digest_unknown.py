"""Recovery from an answer naming an unknown capabilities digest (PHP and Rust parity).

A non-idempotent request (completion, failure, heartbeat) is never resent: its
answer is kept and the capabilities are read from cluster discovery. Only task
polls, which are safe to repeat, are resent without the digest.
"""
from __future__ import annotations

import asyncio
import copy
import logging
from typing import Any

import httpx
import pytest

from durable_workflow import Client, Worker, activity, serializer
from durable_workflow._capabilities_digest import HEADER, ServerCapabilitiesDigest
from durable_workflow.errors import ServerCapabilitiesDigestError, ServerError
from durable_workflow.retry_policy import TransportRetryPolicy

from .test_worker import compatible_cluster_info

D1, D2, GARBLED = "sha256:" + "1" * 64, "sha256:" + "2" * 64, "sha256:" + "f" * 64
CAPS = {"activity_lease_renewal": False, "v": 1}
DISCOVERED = {"activity_lease_renewal": False, "v": "discovered"}


def tagged(digest: str, capabilities: dict[str, Any] | None = None) -> dict[str, Any]:
    return {**copy.deepcopy(capabilities or CAPS), "digest": digest}


def test_drop_unknown_removes_the_field_and_forget_resets_the_header() -> None:
    held = ServerCapabilitiesDigest()
    held.restore({"server_capabilities": tagged(D1)}, held.capture())
    body = {"reason": "conflict", "server_capabilities": GARBLED, "answer": {"server_capabilities": D1}}
    assert held.restore(body, held.capture(), drop_unknown=True) is True
    assert "server_capabilities" not in body
    assert body["answer"]["server_capabilities"] == CAPS
    assert held.restore({"server_capabilities": D1}, held.capture(), drop_unknown=True) is False
    held.forget()
    assert held.header(held.capture()) == "none"


class GarblingServer:
    """Answers worker requests per a script of (status, capabilities) after learning D1."""

    def __init__(self, script: list[tuple[int, Any]] | None = None) -> None:
        self.script = list(script or [])
        self.sent: list[tuple[str, str | None]] = []
        self.current = D1
        self.discovery = 0

    async def respond(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/cluster/info"):
            self.discovery += 1
            assert request.method == "GET" and HEADER not in request.headers
            return httpx.Response(200, json={"worker_protocol": {"server_capabilities": DISCOVERED}})
        sent = request.headers.get(HEADER)
        self.sent.append((request.url.path, sent))
        capabilities: Any
        if self.script and sent != "none":
            status, capabilities = self.script.pop(0)
        else:
            status, capabilities = 200, self.current if sent == self.current else tagged(self.current)
        body = {"ok": True, "path": request.url.path, "server_capabilities": capabilities}
        if status >= 400:
            body = {"reason": "stale_attempt", "message": "refused", "server_capabilities": capabilities}
        return httpx.Response(status, json=body)


def client_for(server: GarblingServer) -> Client:
    client = Client("http://digest.invalid", namespace="17", worker_token="worker-token",
                    retry_policy=TransportRetryPolicy(max_attempts=1))
    client._http = httpx.AsyncClient(base_url="http://digest.invalid", transport=httpx.MockTransport(server.respond))
    return client


async def learned(client: Client) -> None:
    await client._request("POST", "/worker/learn", worker=True, json={})
    assert client._capabilities_digest.capture()[0] == D1


async def test_a_garbled_answer_to_a_completion_is_kept_and_not_resent() -> None:
    server = GarblingServer()
    async with client_for(server) as client:
        await learned(client)
        server.script = [(200, GARBLED)]
        answer = await client._request("POST", "/worker/activity-tasks/t/complete", worker=True, json={})
        after = await client._request("POST", "/worker/b", worker=True, json={})
    assert server.sent[1:] == [("/api/worker/activity-tasks/t/complete", D1), ("/api/worker/b", "none")], (
        "the completion is sent once; the held pair is forgotten afterwards"
    )
    assert answer["path"] == "/api/worker/activity-tasks/t/complete", "the completion's own answer is used"
    assert answer["server_capabilities"] == DISCOVERED, "capabilities come from cluster discovery"
    assert server.discovery == 1
    assert after["server_capabilities"] == CAPS


async def test_without_discovery_the_answer_is_kept_without_its_capabilities() -> None:
    async def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/cluster/info"):
            return httpx.Response(503, json={"reason": "storage_unavailable"})
        return httpx.Response(200, json={"recorded": True, "server_capabilities": GARBLED})

    async with Client("http://digest.invalid", worker_token="w",
                      retry_policy=TransportRetryPolicy(max_attempts=1)) as client:
        client._http = httpx.AsyncClient(base_url="http://digest.invalid", transport=httpx.MockTransport(respond))
        client._capabilities_digest.restore({"server_capabilities": tagged(D1)}, (None, None))
        answer = await client._request("POST", "/worker/heartbeat", worker=True, json={})
    assert answer == {"recorded": True}


async def test_a_garbled_poll_answer_is_resent_once_without_the_digest() -> None:
    server = GarblingServer()
    async with client_for(server) as client:
        await learned(client)
        server.script = [(200, GARBLED)]
        server.current = D2  # the capabilities changed; the garbled answer names neither
        poll = {"worker_id": "w", "task_queue": "q"}
        answer = await client._request("POST", "/worker/query-tasks/poll", worker=True, json=poll)
        after = await client._request("POST", "/worker/b", worker=True, json={})
    assert server.sent[1:] == [
        ("/api/worker/query-tasks/poll", D1), ("/api/worker/query-tasks/poll", "none"), ("/api/worker/b", D2),
    ]
    assert answer["server_capabilities"] == CAPS, "the resend's full copy, not the garbled digest"
    assert server.discovery == 0
    assert after["server_capabilities"] == CAPS


async def test_an_unknown_digest_on_the_poll_resend_is_a_protocol_error() -> None:
    async def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"task": None, "server_capabilities": GARBLED})

    poll = {"worker_id": "w", "task_queue": "q"}
    async with Client("http://digest.invalid", worker_token="w") as client:
        client._http = httpx.AsyncClient(base_url="http://digest.invalid", transport=httpx.MockTransport(respond))
        client._capabilities_digest.restore({"server_capabilities": tagged(D1)}, (None, None))
        with pytest.raises(ServerCapabilitiesDigestError):
            await client._request("POST", "/worker/query-tasks/poll", worker=True, json=poll)
        # Sent with ``none`` already: no resend, the answer is a protocol error.
        with pytest.raises(ServerCapabilitiesDigestError):
            await client._request("POST", "/worker/query-tasks/poll", worker=True, json=poll)


async def test_a_garbled_error_body_surfaces_the_http_error_unchanged() -> None:
    server = GarblingServer()
    async with client_for(server) as client:
        await learned(client)
        server.script = [(409, GARBLED)]
        with pytest.raises(ServerError) as refusal:
            await client._request("POST", "/worker/refused", worker=True, json={})
        await client._request("POST", "/worker/next", worker=True, json={})
    assert refusal.value.status == 409
    assert refusal.value.body == {"reason": "stale_attempt", "message": "refused"}
    assert server.sent[1:] == [("/api/worker/refused", D1), ("/api/worker/next", "none")], "no resend"


@activity.defn(name="digest.echo")
def echo(value: str) -> str:
    calls.append(value)
    return value


calls: list[str] = []


class Runtime:
    """A worker runtime whose first poll and first completion answers are garbled."""

    def __init__(self, *, garble_poll: bool, garble_completion: bool) -> None:
        info = compatible_cluster_info()
        info["worker_protocol"]["server_capabilities"].pop("worker_deregistration_fencing", None)  # type: ignore[index]
        self.info = info
        self.garble_poll, self.garble_completion = garble_poll, garble_completion
        self.polls: list[str | None] = []
        self.completions: list[str | None] = []
        self.failures = 0
        self.task_served = False

    def caps(self, sent: str | None) -> Any:
        return D1 if sent == D1 else tagged(D1)

    async def respond(self, request: httpx.Request) -> httpx.Response:
        path, sent = request.url.path, request.headers.get(HEADER)
        if path.endswith("/cluster/info"):
            return httpx.Response(200, json=self.info)
        if path.endswith("/register"):
            return httpx.Response(200, json={"worker_id": "worker-1", "registered": True,
                                             "server_capabilities": self.caps(sent)})
        if path.endswith("/activity-tasks/poll"):
            self.polls.append(sent)
            await asyncio.sleep(0.01)
            caps = self.caps(sent)
            if self.garble_poll and sent == D1:
                self.garble_poll = False
                caps = GARBLED
            task = None
            if not self.task_served and self.garble_completion and sent is not None and caps != GARBLED:
                self.task_served = True
                task = {
                    "task_id": "task-1", "activity_attempt_id": "attempt-1", "attempt_number": 1,
                    "activity_type": "digest.echo", "workflow_id": "wf-1", "run_id": "run-1",
                    "lease_owner": "worker-1", "task_queue": "q",
                    "arguments": serializer.envelope(["once"], codec="avro"), "payload_codec": "avro",
                }
            return httpx.Response(200, json={"task": task, "server_capabilities": caps})
        if path.endswith("/complete"):
            self.completions.append(sent)
            if len(self.completions) == 1:
                # Committed, but the answer names capabilities the worker never held.
                return httpx.Response(200, json={"outcome": "completed", "recorded": True,
                                                 "server_capabilities": GARBLED})
            # A resend would only get this duplicate; it must never be sent.
            return httpx.Response(409, json={
                "task_id": "task-1", "activity_attempt_id": "attempt-1", "outcome": "completed",
                "recorded": False, "reason": "stale_attempt", "next_task_id": None, "lease_owner": "worker-1",
                "activity_status": "completed", "attempt_status": "completed", "task_status": "completed",
                "server_capabilities": tagged(D2),
            })
        if path.endswith("/fail"):
            self.failures += 1
            return httpx.Response(200, json={"outcome": "failed", "recorded": True})
        if path.endswith("/registrations/worker-1"):
            return httpx.Response(200, json={"worker_id": "worker-1", "outcome": "deregistered",
                                             "recovered_workflow_task_count": 0})
        return httpx.Response(200, json={"worker_id": "worker-1", "acknowledged": True,
                                         "server_capabilities": self.caps(sent)})


async def run_worker_until(runtime: Runtime, done: Any) -> Worker:
    client = Client("http://runtime.invalid", token="token", retry_policy=TransportRetryPolicy(max_attempts=1))
    client._http = httpx.AsyncClient(base_url="http://runtime.invalid", transport=httpx.MockTransport(runtime.respond))
    worker = Worker(client, task_queue="q", workflows=[], activities=[echo], worker_id="worker-1")
    running = asyncio.create_task(worker.run())
    try:
        for _ in range(400):
            if done():
                break
            await asyncio.sleep(0.01)
        assert not running.done(), running.exception() if running.done() else None
        return worker
    finally:
        await worker.stop()
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await client.aclose()


async def test_a_garbled_poll_answer_is_resent_in_full_and_the_worker_keeps_polling() -> None:
    runtime = Runtime(garble_poll=True, garble_completion=False)
    await run_worker_until(runtime, lambda: len(runtime.polls) >= 6)
    garbled_at = runtime.polls.index(D1)
    assert runtime.polls[garbled_at + 1] == "none", "the garbled poll is resent without the digest"
    assert len(runtime.polls) >= 6, "the worker keeps polling"


async def test_a_garbled_completion_answer_is_kept_without_a_resend(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="durable_workflow.worker")
    calls.clear()
    runtime = Runtime(garble_poll=False, garble_completion=True)
    await run_worker_until(runtime, lambda: len(runtime.completions) >= 1 and len(runtime.polls) >= 4)
    assert calls == ["once"], "the activity ran once"
    assert runtime.completions == [D1], "the completion is never resent"
    assert runtime.failures == 0, "no failure report"
    assert len(runtime.polls) >= 4, "the worker keeps running"
    assert runtime.polls[-1] in ("none", D1), "later requests re-learn the capabilities"
    assert "failed to complete" not in caplog.text, "the completion's own recorded answer is used"
