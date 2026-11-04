from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest

from durable_workflow.client import Client
from durable_workflow.errors import ServerError
from durable_workflow.retry_policy import TransportRetryPolicy


def paged_client(handler: Callable[[httpx.Request], Any]) -> Client:
    client = Client("https://runtime.example", control_token="synthetic-control", namespace="tenant-a",
                    retry_policy=TransportRetryPolicy(max_attempts=1))
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(handler))
    return client


def page(kind: str, values: list[int], token: Any = None) -> dict[str, Any]:
    if kind == "workflows":
        items = [{"workflow_id": str(value), "run_id": "run", "workflow_type": "invoice"} for value in values]
        return {"workflows": items, "next_page_token": token}
    return {"events": [{"sequence": value, "event_type": "CustomEvent"} for value in values],
            "next_page_token": token}


def iterate(client: Client, kind: str, *, token: str | None = None) -> AsyncIterator[Any]:
    if kind == "workflows":
        return client.iter_workflows(workflow_type="invoice", status="running", query="customer = 'fixed'",
                                     page_size=2, next_page_token=token)
    return client.iter_history("wf-fixed", "run-fixed", page_size=2, next_page_token=token)


def value_of(kind: str, item: Any) -> int:
    return int(item.workflow_id) if kind == "workflows" else int(item["sequence"])


@pytest.mark.parametrize("kind", ["workflows", "history"])
async def test_lazy_pages_preserve_identity_filters_and_opaque_tokens(kind: str) -> None:
    requests: list[httpx.Request] = []
    pages = [page(kind, [1, 2], "opaque+/="), page(kind, [], "tail"), page(kind, [3])]

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=pages[len(requests) - 1])

    async with paged_client(respond) as client:
        iterator = iterate(client, kind)
        assert not requests
        assert value_of(kind, await anext(iterator)) == 1
        assert len(requests) == 1
        assert value_of(kind, await anext(iterator)) == 2
        assert len(requests) == 1
        assert value_of(kind, await anext(iterator)) == 3
        assert len(requests) == 3  # The empty continued page is followed.
        with pytest.raises(StopAsyncIteration):
            await anext(iterator)

    assert [r.url.params.get("next_page_token") for r in requests] == [None, "opaque+/=", "tail"]
    assert all(r.headers["X-Namespace"] == "tenant-a" for r in requests)
    assert all(r.url.params["page_size"] == "2" for r in requests)
    assert "opaque%2B%2F%3D" in str(requests[1].url)
    if kind == "history":
        assert {r.url.path for r in requests} == {"/api/workflows/wf-fixed/runs/run-fixed/history"}
    else:
        for request in requests:
            assert request.url.path == "/api/workflows"
            assert request.url.params["workflow_type"] == "invoice"
            assert request.url.params["status"] == "running"
            assert request.url.params["query"] == "customer = 'fixed'"


@pytest.mark.parametrize("kind", ["workflows", "history"])
async def test_closing_early_does_not_fetch_the_next_page(kind: str) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=page(kind, [1, 2], "next"))

    async with paged_client(respond) as client:
        iterator = iterate(client, kind)
        assert value_of(kind, await anext(iterator)) == 1
        await iterator.aclose()  # type: ignore[attr-defined]
        assert len(requests) == 1


@pytest.mark.parametrize("kind", ["workflows", "history"])
@pytest.mark.parametrize("final_token", [None, ""])
async def test_empty_final_page_terminates_without_another_request(kind: str, final_token: str | None) -> None:
    async with paged_client(lambda _: httpx.Response(200, json=page(kind, [], final_token))) as client:
        assert [item async for item in iterate(client, kind)] == []


@pytest.mark.parametrize("kind", ["workflows", "history"])
async def test_missing_continuation_is_a_final_page(kind: str) -> None:
    body = page(kind, [9])
    del body["next_page_token"]
    async with paged_client(lambda _: httpx.Response(200, json=body)) as client:
        items = [item async for item in iterate(client, kind)]
    assert [value_of(kind, item) for item in items] == [9]


@pytest.mark.parametrize("kind", ["workflows", "history"])
async def test_resume_token_is_sent_without_normalization(kind: str) -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=page(kind, [7]))

    async with paged_client(respond) as client:
        items = [item async for item in iterate(client, kind, token=" resume/+? ")]
    assert [value_of(kind, item) for item in items] == [7]
    assert requests[0].url.params["next_page_token"] == " resume/+? "


@pytest.mark.parametrize("kind", ["workflows", "history"])
@pytest.mark.parametrize("tokens", [["same"], ["a", "b"], ["a", "b", "c"]])
async def test_cyclic_continuations_fail_instead_of_looping(kind: str, tokens: list[str]) -> None:
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        token = tokens[calls % len(tokens)]
        calls += 1
        assert calls <= 8, "cyclic pagination did not terminate"
        return httpx.Response(200, json=page(kind, [], token))

    async with paged_client(respond) as client:
        with pytest.raises(RuntimeError, match="repeated a page token"):
            _ = [item async for item in iterate(client, kind)]


@pytest.mark.parametrize("kind", ["workflows", "history"])
@pytest.mark.parametrize("token", [False, 0, 1, [], {}])
async def test_malformed_continuations_are_not_silent_end_of_data(kind: str, token: Any) -> None:
    async with paged_client(lambda _: httpx.Response(200, json=page(kind, [1], token))) as client:
        with pytest.raises(RuntimeError, match="invalid page token"):
            _ = [item async for item in iterate(client, kind)]


@pytest.mark.parametrize("kind", ["workflows", "history"])
async def test_namespace_change_cannot_reuse_a_cursor_in_another_namespace(kind: str) -> None:
    calls = 0

    def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=page(kind, [1], "next"))

    async with paged_client(respond) as client:
        iterator = iterate(client, kind)
        await anext(iterator)
        client.namespace = "another-tenant"
        with pytest.raises(RuntimeError, match="namespace changed"):
            await anext(iterator)
        assert calls == 1


@pytest.mark.parametrize("kind", ["workflows", "history"])
@pytest.mark.parametrize("cancelled", [False, True], ids=["http-error", "cancellation"])
async def test_later_page_error_or_cancellation_propagates(kind: str, cancelled: bool) -> None:
    calls = 0

    async def respond(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json=page(kind, [1], "next"))
        if cancelled:
            raise asyncio.CancelledError()
        return httpx.Response(503, json={"message": "temporarily unavailable"})

    async with paged_client(respond) as client:
        iterator = iterate(client, kind)
        assert value_of(kind, await anext(iterator)) == 1
        with pytest.raises(asyncio.CancelledError if cancelled else ServerError):
            await anext(iterator)
        assert calls == 2


@pytest.mark.parametrize("events", [None, {}, [None], ["bad"]])
async def test_history_requires_an_event_array(events: Any) -> None:
    async with paged_client(lambda _: httpx.Response(200, json={"events": events})) as client:
        with pytest.raises(RuntimeError, match="invalid events"):
            _ = [event async for event in client.iter_history("workflow", "run")]


async def test_handle_iteration_requires_an_explicit_run_and_uses_it() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=page("history", [1]))

    async with paged_client(respond) as client:
        missing = client.get_workflow_handle("wf")
        with pytest.raises(ValueError, match="run_id is required"):
            _ = [event async for event in missing.iter_history()]
        assert not requests
        handle = client.get_workflow_handle("wf", run_id="original-run")
        assert [event["sequence"] async for event in handle.iter_history()] == [1]
    assert requests[0].url.path == "/api/workflows/wf/runs/original-run/history"
