from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow._cancellation_scope_history import CancellationScopeBudget, CommittedCancellationScopeHistory
from durable_workflow.client import Client
from durable_workflow.errors import ServerError, WorkflowCancelled
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import Worker
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext
from tests.test_committed_cancellation_scope_history import fixture


@pytest.fixture(autouse=True)
def protocol(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")


def exchange(
    delivering: bool = False, layout: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    value = (fixture("committed-scope-delivery.json", "unshielded") if layout is None
             else fixture("populated-scope-groups.json", layout))
    history = value["history"] if delivering else value["history"][:-1]
    event = history[-1]
    preparation = value["history"][-2]
    receipt = {
        **event["payload"], **{field: preparation["payload"][field] for field in (
            "activity_members", "timer_members", "wait_members", "child_members",
        )},
        "task_id": "task/one", "lease_owner": "original", "workflow_task_attempt": 4,
        "prepared": True, "delivered": delivering, "claim_released": False, "created_task_ids": [], "reason": None,
        "history_event_id": event["id"], "preparation_history_event_id": preparation["id"],
        "history_refresh_page_token": "opaque-start",
    }
    committed = CommittedCancellationScopeHistory.read(history, value["task"]["run_id"], value["task"]["workflow_id"])
    original = next(iter(committed.preparations.values()))
    arguments = {
        "task_id": "task/one", "run_id": value["task"]["run_id"], "workflow_id": value["task"]["workflow_id"],
        "lease_owner": "original", "workflow_task_attempt": 4, "scope_id": original.context.scope_id,
        "boundary": original.boundary, "phase": "deliver" if delivering else "prepare",
    }
    return receipt, history, arguments


def page(history: list[dict[str, Any]], token: str | None = None) -> dict[str, Any]:
    return {"task_id": "task/one", "workflow_task_attempt": 4, "history_events": history,
            "next_history_page_token": token}


def pending_stop(receipt: dict[str, Any]) -> dict[str, Any]:
    return {**{key: value for key, value in receipt.items() if key != "history_event_id"},
            "delivered": False, "reason": "cancellation_scope_activity_stop_not_acknowledged"}


async def test_pending_callback_stop_waits_for_delivery_on_the_original_claim_and_preparation() -> None:
    receipt, history, arguments = exchange()
    delivered, full_history, delivery_arguments = exchange(True)
    budget = CancellationScopeBudget.start()
    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=[
            receipt, page(history), pending_stop(receipt), page(history), delivered, page(full_history),
        ]) as send:
            prepared = await client.cancellation_scope_boundary_on_claim(**arguments, budget=budget)
            result = await client.cancellation_scope_boundary_on_claim(
                **delivery_arguments, preparation=prepared, budget=budget,
            )
    result.assert_original_preparation(prepared)
    assert result.delivery is not None
    assert send.await_args_list[2].kwargs["json"] == send.await_args_list[4].kwargs["json"]
    assert send.await_args_list[4].kwargs["timeout"] < send.await_args_list[2].kwargs["timeout"]


@pytest.mark.parametrize("change", [
    {"lease_owner": "replacement"}, {"preparation_history_event_id": "borrowed"},
    {"authority_deadline_at": "2026-10-04T00:00:31.123456Z"}, {"history_event_id": "invented-delivery"},
])
async def test_pending_stop_cannot_substitute_authority_or_invent_delivery(change: dict[str, Any]) -> None:
    receipt, history, arguments = exchange()
    _, _, delivery_arguments = exchange(True)
    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=[receipt, page(history)]):
            prepared = await client.cancellation_scope_boundary_on_claim(
                **arguments, budget=CancellationScopeBudget.start(),
            )
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=[
            {**pending_stop(receipt), **change}, page(history),
        ]) as send:
            with pytest.raises(ServerError, match="invalid_cancellation_scope_boundary"):
                await client.cancellation_scope_boundary_on_claim(
                    **delivery_arguments, preparation=prepared, budget=CancellationScopeBudget.start(),
                )
            assert sum(call.args[1].endswith("/deliver") for call in send.await_args_list) == 1


async def test_pending_stop_does_not_renew_the_original_budget() -> None:
    receipt, history, arguments = exchange()
    _, _, delivery_arguments = exchange(True)
    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=[receipt, page(history)]):
            prepared = await client.cancellation_scope_boundary_on_claim(
                **arguments, budget=CancellationScopeBudget.start(),
            )
        async def respond(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return pending_stop(receipt) if args[1].endswith("/deliver") else page(history)
        budget = CancellationScopeBudget.start(0.25)
        original_expiry = budget.expires_at
        with patch.object(client, "_request", side_effect=respond) as send, pytest.raises(TimeoutError):
            await client.cancellation_scope_boundary_on_claim(**delivery_arguments, preparation=prepared, budget=budget)
        assert budget.expires_at == original_expiry
        mutations = [call for call in send.await_args_list if call.args[1].endswith("/deliver")]
        assert len(mutations) >= 2
        assert all(call.kwargs["json"] == mutations[0].kwargs["json"] for call in mutations)


@pytest.mark.parametrize("layout", [None, "flat", "nested"])
async def test_preparation_and_delivery_prove_every_original_claim_page_on_one_budget(layout: str | None) -> None:
    receipt, history, arguments = exchange(layout=layout)
    delivered_receipt, delivered_history, delivered_arguments = exchange(True, layout)
    budget = CancellationScopeBudget.start()
    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=[
            receipt, page(history[:2], "opaque-next"), page(history[2:]),
            delivered_receipt, page(delivered_history),
        ]) as send:
            prepared = await client.cancellation_scope_boundary_on_claim(**arguments, budget=budget)
            delivered = await client.cancellation_scope_boundary_on_claim(
                **delivered_arguments, preparation=prepared, budget=budget,
            )
    assert prepared.delivery is None and delivered.delivery is not None
    delivered.assert_original_preparation(prepared)
    assert list(delivered.history) == delivered_history
    assert send.await_count == 5
    for call in send.await_args_list:
        assert call.kwargs["worker"] is True
        assert call.kwargs["json"]["lease_owner"] == "original"
        assert call.kwargs["json"]["workflow_task_attempt"] == 4
        assert 0 < call.kwargs["timeout"] <= 5
    assert send.await_args_list[0].args[1] == "/worker/workflow-tasks/task%2Fone/cancellation-scopes/prepare"
    assert send.await_args_list[3].args[1] == "/worker/workflow-tasks/task%2Fone/cancellation-scopes/deliver"
    if layout is not None:
        for index in (0, 3):
            assert send.await_args_list[index].kwargs["json"]["call_kind"] == "parallel"
            assert send.await_args_list[index].kwargs["json"]["sequence_span"] == 4


@pytest.mark.parametrize("change", [
    {"task_id": "other"}, {"lease_owner": "other"}, {"workflow_task_attempt": True}, {"scope_id": "other"},
    {"request_id": "other"}, {"sequence": 9}, {"claim_released": True}, {"created_task_ids": ["extra"]},
    {"reason": "cancellation_scope_activity_stop_not_acknowledged"}, {"delivered": True}, {"prepared": 1},
    {"authority_deadline_at": "2026-10-04T00:00:31.123456Z"}, {"operation_sequence": 1},
    {"history_event_id": "other"}, {"history_refresh_page_token": ""},
])
async def test_changed_or_unacknowledged_boundary_cannot_authorize_cleanup(change: dict[str, Any]) -> None:
    receipt, history, arguments = exchange()
    receipt.update(change)
    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=[receipt, page(history)]) as send:
            with pytest.raises(ServerError, match="invalid_cancellation_scope_boundary"):
                await client.cancellation_scope_boundary_on_claim(**arguments, budget=CancellationScopeBudget.start())
            assert send.await_count <= 2


@pytest.mark.parametrize("fault", ["missing_start", "foreign_namespace", "changed_request", "missing_preparation",
                                   "changed_projection", "malformed_range", "cyclic_page", "missing_terminal_cursor"])
async def test_incomplete_or_substituted_history_cannot_prove_the_receipt(fault: str) -> None:
    receipt, history, arguments = exchange()
    if fault == "missing_start":
        history = history[2:]
    elif fault == "foreign_namespace":
        history[-1]["namespace"] = "foreign"
    elif fault == "changed_request":
        history[-2]["payload"]["cancellation"]["root_context"]["reason"] = "changed"
    elif fault == "missing_preparation":
        history.pop()
    elif fault == "changed_projection":
        history[-1]["payload"]["timer_members"] = [{}]
    elif fault == "malformed_range":
        history[-1]["payload"]["sequence_span"] = False
    reply = page(history, "opaque-start" if fault == "cyclic_page" else None)
    if fault == "missing_terminal_cursor":
        reply.pop("next_history_page_token")
    async with Client("http://server", namespace="sdk-scope-fixture") as client:
        with (
            patch.object(client, "_request", new_callable=AsyncMock, side_effect=[receipt, reply]),
            pytest.raises(ServerError, match="invalid_cancellation_scope_boundary"),
        ):
            await client.cancellation_scope_boundary_on_claim(**arguments, budget=CancellationScopeBudget.start())


async def test_shared_budget_expiry_and_missing_original_preparation_refuse_before_io() -> None:
    _, history, arguments = exchange()
    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        with patch.object(client, "_request", new_callable=AsyncMock) as send:
            with pytest.raises(TimeoutError):
                await client.cancellation_scope_boundary_on_claim(
                    **arguments, budget=CancellationScopeBudget(asyncio.get_running_loop().time() - 1),
                )
            with pytest.raises(ValueError, match="budget"):
                await client.cancellation_scope_boundary_on_claim(**arguments)
            arguments["phase"] = "deliver"
            with pytest.raises(ValueError, match="preparation"):
                await client.cancellation_scope_boundary_on_claim(**arguments, budget=CancellationScopeBudget.start())
            send.assert_not_awaited()


async def test_all_history_pages_share_the_budget_even_when_transport_hangs() -> None:
    receipt, history, arguments = exchange()
    calls = 0

    async def send(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return receipt
        await asyncio.sleep(0.2)
        return page(history)

    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        with patch.object(client, "_request", side_effect=send), pytest.raises(asyncio.TimeoutError):
            await client.cancellation_scope_boundary_on_claim(**arguments, budget=CancellationScopeBudget.start(0.03))
    assert calls == 2


async def test_delivery_cannot_borrow_or_replace_an_earlier_preparation() -> None:
    receipt, history, arguments = exchange()
    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=[receipt, page(history)]):
            prepared = await client.cancellation_scope_boundary_on_claim(
                **arguments, budget=CancellationScopeBudget.start(),
            )
        delivered, full_history, arguments = exchange(True)
        full_history[-2]["payload"]["authority_deadline_at"] = "2026-10-04T00:00:28.123456Z"
        full_history[-1]["payload"]["authority_deadline_at"] = "2026-10-04T00:00:28.123456Z"
        delivered["authority_deadline_at"] = "2026-10-04T00:00:28.123456Z"
        with (
            patch.object(client, "_request", new_callable=AsyncMock,
                         side_effect=[deepcopy(delivered), page(full_history)]),
            pytest.raises(ServerError, match="invalid_cancellation_scope_boundary"),
        ):
            await client.cancellation_scope_boundary_on_claim(
                **arguments, preparation=prepared, budget=CancellationScopeBudget.start(),
            )


async def test_actual_worker_replays_proved_preparation_before_delivery_and_cleanup() -> None:
    receipt, history, arguments = exchange()
    delivered_receipt, delivered_history, _ = exchange(True)
    offset = datetime.now(timezone.utc) - datetime(2026, 10, 4, tzinfo=timezone.utc) - timedelta(seconds=10)

    def live(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: live(item) for key, item in value.items()}
        if isinstance(value, list):
            return [live(item) for item in value]
        if isinstance(value, str) and value.startswith("2026-10-04T"):
            return (datetime.fromisoformat(value.replace("Z", "+00:00")) + offset).isoformat(
                timespec="microseconds",
            ).replace("+00:00", "Z")
        return value

    receipt, history, delivered_receipt, delivered_history = live([
        receipt, history, delivered_receipt, delivered_history,
    ])
    cleanup: list[str] = []

    class Probe:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            def inner():  # type: ignore[no-untyped-def]
                try:
                    yield ctx.start_timer(10)
                except WorkflowCancelled as error:
                    cleanup.append(error.request_id or "")
                    return "done"
            return (yield from ctx.cancellation_scope(lambda: ctx.cancellation_scope(inner)))

    async with Client("http://server", namespace=history[0]["namespace"]) as client:
        worker = Worker(client, task_queue="queue", workflows=[Probe], worker_id="original")
        worker._cooperative_cancellation_supported = True
        worker._allow_cancellation_scope_authoring = True
        worker._allow_cancellation_scope_delivery = True
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=[
            receipt, page(history), delivered_receipt, page(delivered_history),
        ]) as send:
            outcome, proved_history = await worker._replay_workflow_claim(Probe, {
                "task_id": "task/one", "workflow_id": arguments["workflow_id"], "run_id": arguments["run_id"],
                "workflow_task_attempt": 4,
            }, history[:-1], [], payload_codec="avro", execute_local=lambda operation: None)
    assert outcome.commands == [CompleteWorkflow("done")]
    assert proved_history == delivered_history
    assert cleanup == [arguments["boundary"].request_id]
    assert [call.args[1].rsplit("/", 1)[-1] for call in send.await_args_list] == [
        "prepare", "history", "deliver", "history",
    ]
    assert [call.kwargs["timeout"] for call in send.await_args_list] == sorted(
        [call.kwargs["timeout"] for call in send.await_args_list], reverse=True,
    )


async def test_lost_acknowledgment_reuses_original_claim_and_worker_credentials() -> None:
    receipt, history, arguments = exchange()
    requests: list[httpx.Request] = []

    async def transport(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ReadError("lost original preparation acknowledgment", request=request)
        return httpx.Response(200, json=receipt if request.url.path.endswith("/prepare") else page(history))

    retry = TransportRetryPolicy(max_attempts=2, initial_backoff_seconds=0.001, jitter=False)
    async with Client(
        "http://server", namespace=history[0]["namespace"], control_token="control", worker_token="worker",
        retry_policy=retry,
    ) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://server", transport=httpx.MockTransport(transport))
        proof = await client.cancellation_scope_boundary_on_claim(**arguments, budget=CancellationScopeBudget.start())
    assert proof.preparation.context.scope_id == arguments["scope_id"]
    assert len(requests) == 3 and requests[0].content == requests[1].content
    assert json.loads(requests[0].content)["workflow_task_attempt"] == 4
    for request in requests:
        assert request.headers["Authorization"] == "Bearer worker"
        assert request.headers["X-Namespace"] == history[0]["namespace"]
        assert request.headers["X-Durable-Workflow-Protocol-Version"] == "1.20"
