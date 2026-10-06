from __future__ import annotations

import asyncio
import json
import time
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow.client import Client
from durable_workflow.errors import ServerError
from durable_workflow.retry_policy import TransportRetryPolicy

CLAIM = dict(task_id="task/one", run_id="run-one", lease_owner="original", workflow_task_attempt=4,
             sequence=2, parent_scope_id="scope-one", shield_parent=True)


@pytest.fixture(autouse=True)
def candidate_protocol(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")


def opening(sequence: int, scope_id: str, parent: str = "root", shield: bool = False) -> dict[str, Any]:
    return dict(id="event-" + scope_id, sequence=sequence + 1, namespace="tenant",
                event_type="CancellationScopeOpened", payload=dict(
                    schema="durable-workflow.cancellation-scope/v1", workflow_run_id="run-one",
                    sequence=sequence, scope_id=scope_id, parent_scope_id=parent, shield_parent=shield))


def fixture() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    receipt = dict(task_id="task/one", workflow_run_id="run-one", lease_owner="original",
                   workflow_task_attempt=4, sequence=2, scope_id="scope-two", parent_scope_id="scope-one",
                   shield_parent=True, opened=True, duplicate=False, claim_released=False,
                   created_task_ids=[], reason=None, history_event_id="event-scope-two",
                   history_refresh_page_token="opaque-start")
    history = [dict(id="start", sequence=1, namespace="tenant", event_type="WorkflowStarted", payload={}),
               opening(1, "scope-one"), opening(2, "scope-two", "scope-one", True)]
    return receipt, history


def page(history: list[dict[str, Any]], token: str | None = None) -> dict[str, Any]:
    return dict(task_id="task/one", workflow_task_attempt=4, history_events=history, next_history_page_token=token)


@pytest.mark.parametrize("duplicate,accepted_prefix", [(False, False), (True, True)])
async def test_original_claim_opening_is_proved_from_complete_paged_history(
    duplicate: bool, accepted_prefix: bool,
) -> None:
    receipt, history = fixture()
    receipt["duplicate"] = duplicate
    if accepted_prefix:
        history.insert(0, dict(id="accepted", sequence=0, namespace="tenant", event_type="StartAccepted", payload={}))
        for event in history:
            event["sequence"] += 1
    async with Client("http://server", namespace="tenant") as client:
        with patch.object(client, "_request", new_callable=AsyncMock,
                          side_effect=[receipt, page(history[:1], "opaque-next"), page(history[1:])]) as send:
            proof = await client.open_cancellation_scope_on_claim(**CLAIM)
    assert proof.scope_id == "scope-two" and proof.history_event_id == "event-scope-two"
    assert proof.parent_scope_id == "scope-one" and proof.shield_parent is True
    assert proof.sequence == 2 and proof.duplicate is duplicate and list(proof.history) == history
    assert send.await_args_list[0].args == ("POST", "/worker/workflow-tasks/task%2Fone/cancellation-scopes/open")
    for call in send.await_args_list:
        assert call.kwargs["worker"] is True
        assert call.kwargs["json"]["lease_owner"] == "original"
        assert call.kwargs["json"]["workflow_task_attempt"] == 4
    assert [call.kwargs["json"]["next_history_page_token"] for call in send.await_args_list[1:]] == [
        "opaque-start", "opaque-next",
    ]


@pytest.mark.parametrize("change", [
    {"task_id": "foreign"}, {"workflow_run_id": "foreign"}, {"lease_owner": "replacement"},
    {"workflow_task_attempt": 5}, {"sequence": 3}, {"parent_scope_id": "root"}, {"shield_parent": 1},
    {"opened": 1}, {"duplicate": 0}, {"claim_released": True}, {"created_task_ids": ["new"]},
    {"reason": "refused"}, {"scope_id": "root"}, {"history_event_id": ""}, {"history_refresh_page_token": None},
])
async def test_changed_receipt_cannot_supply_scope_authority(change: dict[str, Any]) -> None:
    receipt, _ = fixture()
    receipt.update(change)
    async with Client("http://server", namespace="tenant") as client:
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=receipt) as send:
            with pytest.raises(ServerError, match="invalid_cancellation_scope_opening"):
                await client.open_cancellation_scope_on_claim(**CLAIM)
            assert send.await_count == 1


@pytest.mark.parametrize("change,location", [
    ({"namespace": "foreign"}, "event"), ({"id": "start"}, "event"), ({"sequence": 2}, "event"),
    ({"sequence": True}, "event"), ({"event_type": ""}, "event"),
    ({"workflow_run_id": "foreign"}, "payload"), ({"scope_id": "scope-one"}, "payload"),
    ({"parent_scope_id": "unknown"}, "payload"), ({"parent_scope_id": "scope-two"}, "payload"),
    ({"parent_scope_id": "root"}, "payload"), ({"shield_parent": False}, "payload"),
    ({"shield_parent": 1}, "payload"), ({"sequence": 1}, "payload"), ({"sequence": True}, "payload"),
    ({"schema": "other"}, "payload"),
])
async def test_changed_committed_tree_cannot_prove_the_receipt(change: dict[str, Any], location: str) -> None:
    receipt, history = fixture()
    target = history[-1] if location == "event" else history[-1]["payload"]
    target.update(change)
    async with Client("http://server", namespace="tenant") as client:
        with (
            patch.object(client, "_request", new_callable=AsyncMock, side_effect=[receipt, page(history)]),
            pytest.raises(ServerError, match="invalid_cancellation_scope_opening"),
        ):
            await client.open_cancellation_scope_on_claim(**CLAIM)


@pytest.mark.parametrize("change", [
    {"task_id": "foreign"}, {"workflow_task_attempt": 5}, {"history_events": {}},
    {"history_events": [None]}, {"next_history_page_token": "opaque-start"},
    {"next_history_page_token": False}, {"next_history_page_token": " "},
])
async def test_history_page_cannot_change_claim_or_cycle_its_cursor(change: dict[str, Any]) -> None:
    receipt, history = fixture()
    result = page(history)
    result.update(change)
    async with Client("http://server", namespace="tenant") as client:
        with (
            patch.object(client, "_request", new_callable=AsyncMock, side_effect=[receipt, result]),
            pytest.raises(ServerError, match="invalid_cancellation_scope_history_page"),
        ):
            await client.open_cancellation_scope_on_claim(**CLAIM)


@pytest.mark.parametrize("history_transform", ["empty", "missing-opening", "missing-start", "truncated-start"])
async def test_incomplete_original_history_cannot_authorize_body_entry(history_transform: str) -> None:
    receipt, history = fixture()
    history = {"empty": [], "missing-opening": history[:-1], "missing-start": history[1:],
               "truncated-start": [dict(history[0], event_type="StartAccepted"), *history[1:]]}[history_transform]
    async with Client("http://server", namespace="tenant") as client:
        with (
            patch.object(client, "_request", new_callable=AsyncMock, side_effect=[receipt, page(history)]),
            pytest.raises(ServerError, match="invalid_cancellation_scope_opening"),
        ):
            await client.open_cancellation_scope_on_claim(**CLAIM)


@pytest.mark.parametrize("change", [
    {"task_id": ""}, {"run_id": " "}, {"lease_owner": "x" * 256}, {"parent_scope_id": "\ud800"},
    {"workflow_task_attempt": True}, {"sequence": 0}, {"sequence": 2**63}, {"shield_parent": 1},
    {"timeout_seconds": 0}, {"timeout_seconds": 6}, {"timeout_seconds": float("nan")},
])
async def test_invalid_authority_fails_before_io(change: dict[str, Any]) -> None:
    async with Client("http://server", namespace="tenant") as client:
        with patch.object(client, "_request", new_callable=AsyncMock) as send:
            with pytest.raises(ValueError):
                await client.open_cancellation_scope_on_claim(**{**CLAIM, **change})
            send.assert_not_awaited()


async def test_published_protocol_refuses_opening_before_io(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION")
    async with Client("http://server", namespace="tenant") as client:
        with patch.object(client, "_request", new_callable=AsyncMock) as send:
            with pytest.raises(ValueError, match="1.20"):
                await client.open_cancellation_scope_on_claim(**CLAIM)
            send.assert_not_awaited()


async def test_opening_and_history_share_one_budget() -> None:
    receipt, history = fixture()
    cancelled = False

    async def slow(method: str, path: str, **kwargs: Any) -> Any:
        nonlocal cancelled
        try:
            await asyncio.sleep(0.06)
            return deepcopy(receipt) if path.endswith("/open") else page(history)
        except asyncio.CancelledError:
            cancelled = True
            raise

    async with Client("http://server", namespace="tenant") as client:
        with patch.object(client, "_request", side_effect=slow) as send:
            started = time.monotonic()
            with pytest.raises(asyncio.TimeoutError):
                await client.open_cancellation_scope_on_claim(**CLAIM, timeout_seconds=0.1)
            assert send.await_count == 2 and cancelled
            assert time.monotonic() - started < 0.3


async def test_lost_ack_retries_original_opening_and_proves_duplicate_with_worker_credentials() -> None:
    receipt, history = fixture()
    receipt["duplicate"] = True
    requests: list[httpx.Request] = []

    async def transport(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ReadError("lost committed opening acknowledgement", request=request)
        return httpx.Response(200, json=receipt if request.url.path.endswith("/open") else page(history))

    retry = TransportRetryPolicy(max_attempts=2, initial_backoff_seconds=0.001, jitter=False)
    async with Client("http://server", namespace="tenant", control_token="control", worker_token="worker",
                      retry_policy=retry) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://server", transport=httpx.MockTransport(transport))
        proof = await client.open_cancellation_scope_on_claim(**CLAIM)
    assert proof.duplicate and proof.scope_id == "scope-two"
    assert len(requests) == 3 and requests[0].content == requests[1].content
    assert json.loads(requests[0].content)["workflow_task_attempt"] == 4
    for request in requests:
        assert request.headers["Authorization"] == "Bearer worker"
        assert request.headers["X-Namespace"] == "tenant"
        assert request.headers["X-Durable-Workflow-Protocol-Version"] == "1.20"
