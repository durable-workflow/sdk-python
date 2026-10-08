from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from durable_workflow import Client, Worker
from durable_workflow._sticky_workflow_cache import StickyWorkflowCache
from durable_workflow.errors import ServerError
from durable_workflow.workflow import LocalActivityExecutionAborted


def history(count: int = 6) -> list[dict[str, Any]]:
    return [{"id": f"event-{sequence}", "sequence": sequence,
             "event_type": "WorkflowStarted" if sequence == 1 else "SideEffectRecorded",
             "payload": {"value": {"number": sequence}}} for sequence in range(1, count + 1)]


def task(events: list[dict[str, Any]], *, mode: str = "sticky_hit_expected",
         last: int = 6, token: str | None = None) -> dict[str, Any]:
    return {"task_id": "current-task", "workflow_id": "workflow", "run_id": "run",
            "workflow_task_attempt": 3, "history_events": events,
            "sticky_replay_mode": mode, "last_history_sequence": last,
            "next_history_page_token": token}


@pytest.mark.parametrize(("capacity", "max_bytes", "ttl"), [
    (-1, 100, 1), (True, 100, 1), (1, 0, 1), (1, True, 1),
    (1, 100, 0), (1, 100, 3601), (1, 100, 1.5),
])
def test_cache_limits_reject_invalid_values(capacity: int, max_bytes: int, ttl: int) -> None:
    with pytest.raises(ValueError, match="sticky_cache"):
        StickyWorkflowCache(capacity, max_bytes, ttl)


def test_lru_entry_limit_and_build_run_isolation() -> None:
    cache = StickyWorkflowCache(2, 10_000, 300)
    a, b, c = ("wf", "run-a", "build-a"), ("wf", "run-b", "build-a"), ("wf", "run-a", "build-b")
    assert cache.remember(a, history())
    assert cache.remember(b, history())
    assert cache.lookup(a) is not None  # Keep A, evict B.
    assert cache.remember(c, history())
    assert cache.lookup(b) is None
    assert cache.lookup(a) is not None
    assert cache.lookup(c) is not None
    assert cache.metrics()["eviction"] == 1
    assert cache.metrics()["entries"] == 2


def test_byte_limit_evicts_and_rejects_oversized_history() -> None:
    sizing = StickyWorkflowCache(10, 100_000, 300)
    assert sizing.remember(("a", "run", "build"), history(2))
    one_size = sizing.metrics()["history_bytes"]
    cache = StickyWorkflowCache(10, one_size * 2 - 1, 300)
    assert cache.remember(("a", "run", "build"), history(2))
    assert cache.remember(("b", "run", "build"), history(2))
    assert cache.metrics()["entries"] == 1
    assert cache.metrics()["eviction"] == 1
    assert not cache.remember(("b", "run", "build"), history(10))
    assert cache.metrics()["entries"] == 0
    assert cache.metrics()["history_bytes"] == 0


def test_cache_snapshot_is_independent_of_input_and_replayed_mutation() -> None:
    cache = StickyWorkflowCache(1, 10_000, 300)
    original = history()
    key = ("wf", "run", "build")
    assert cache.remember(key, original)
    original[0]["payload"]["value"]["number"] = 999
    first = cache.lookup(key)
    assert first is not None
    first[0][0]["payload"]["value"]["number"] = 888
    second = cache.lookup(key)
    assert second is not None
    assert second[0] == history()


def test_expiry_disabled_cache_and_malformed_history_are_not_retained() -> None:
    cache = StickyWorkflowCache(1, 10_000, 1)
    key = ("wf", "run", "build")
    with patch("durable_workflow._sticky_workflow_cache.time.monotonic", return_value=10):
        assert cache.remember(key, history())
    with patch("durable_workflow._sticky_workflow_cache.time.monotonic", return_value=11):
        assert cache.lookup(key) is None
        assert cache.metrics()["history_bytes"] == 0
    assert not StickyWorkflowCache(0, 10_000, 300).remember(key, history())
    assert not cache.remember(key, history()[1:])
    broken = history()
    broken[2]["sequence"] = 9
    assert not cache.remember(key, broken)
    broken = history()
    broken[0]["payload"] = object()
    assert not cache.remember(key, broken)


@pytest.mark.asyncio
async def test_warm_history_reuses_server_cursor_on_current_lease_and_advances_boundary() -> None:
    client = AsyncMock(spec=Client)
    client.workflow_task_history.side_effect = [
        {"history_events": history()[2:4], "next_history_page_token": "server-cursor-four"},
        {"history_events": history()[4:], "next_history_page_token": None},
    ]
    worker = Worker(client, task_queue="q", worker_id="holder", build_id="build", sticky_cache_capacity=2)
    first = task(history()[:2], mode="cold_replay", token="server-cursor-two")
    loaded = await worker._load_workflow_claim_history(first)
    claim = worker._sticky_cache_claim(first, loaded, [{"type": "start_timer"}])
    assert claim is not None
    assert claim["build_id"] == "build"
    assert claim["metrics"]["miss"] == 1
    client.workflow_task_history.reset_mock(side_effect=True)
    client.workflow_task_history.side_effect = [
        {"history_events": history(8)[4:6], "next_history_page_token": "server-cursor-six"},
        {"history_events": history(8)[6:], "next_history_page_token": None},
    ]
    current = task(history(8)[:2], last=8, token="server-cursor-two")
    assert await worker._load_workflow_claim_history(current) == history(8)
    assert [call.kwargs["next_history_page_token"] for call in client.workflow_task_history.await_args_list] == [
        "server-cursor-four", "server-cursor-six",
    ]
    assert all(call.kwargs["task_id"] == "current-task" and call.kwargs["lease_owner"] == "holder"
               and call.kwargs["workflow_task_attempt"] == 3 for call in client.workflow_task_history.await_args_list)
    worker._sticky_cache_claim(current, history(8), [{"type": "start_timer"}])
    retained = worker._sticky_cache.lookup(("workflow", "run", "build"))
    assert retained is not None
    assert retained[1:] == ("server-cursor-six", 6)
    assert worker.sticky_cache_metrics()["hit"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["prefix", "tail", "shorter", "cursor"])
async def test_changed_history_or_rejected_cursor_fetches_complete_authoritative_history(change: str) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="q", build_id="build", sticky_cache_capacity=1)
    worker._sticky_cache.remember(("workflow", "run", "build"), history(),
                                 resume_token="old-cursor", resume_offset=4)
    authoritative = history()
    inline = authoritative[:2]
    last = 6
    if change == "prefix":
        authoritative[1]["payload"] = {"changed": True}
    elif change == "shorter":
        authoritative = history(4)
        inline, last = authoritative[:2], 4
    replies: list[Any] = [{"history_events": authoritative[2:], "next_history_page_token": None}]
    if change == "tail":
        authoritative[5]["payload"] = {"changed": True}
        replies.insert(0, {"history_events": authoritative[4:], "next_history_page_token": None})
    if change == "cursor":
        replies.insert(0, ServerError(400, {"reason": "invalid_page_token"}))
    client.workflow_task_history.side_effect = replies
    loaded = await worker._load_workflow_claim_history(task(inline, last=last, token="current-cursor"))
    assert loaded == authoritative
    assert client.workflow_task_history.await_args_list[-1].kwargs["next_history_page_token"] == "current-cursor"
    assert worker.sticky_cache_metrics()["hit"] == 0
    assert worker.sticky_cache_metrics()["forced_cold_replay"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["sticky_hit_expected", "forced_cold_replay"])
async def test_suffix_without_matching_prefix_uses_lease_scoped_cold_fetch(mode: str) -> None:
    client = AsyncMock(spec=Client)
    client.workflow_task_history.return_value = {"history_events": history(), "next_history_page_token": None}
    worker = Worker(client, task_queue="q", worker_id="replacement", sticky_cache_capacity=1)
    assert await worker._load_workflow_claim_history(task(history()[4:], mode=mode)) == history()
    client.workflow_task_history.assert_awaited_once_with(
        task_id="current-task", next_history_page_token="MA==", lease_owner="replacement", workflow_task_attempt=3,
    )
    assert worker.sticky_cache_metrics()["forced_cold_replay"] == 1


@pytest.mark.asyncio
async def test_build_mismatch_forced_replay_and_replacement_do_not_use_retained_history() -> None:
    client = AsyncMock(spec=Client)
    client.workflow_task_history.return_value = {"history_events": history()[2:], "next_history_page_token": None}
    worker = Worker(client, task_queue="q", build_id="new-build", sticky_cache_capacity=1)
    worker._sticky_cache.remember(("workflow", "run", "old-build"), history(),
                                 resume_token="old-cursor", resume_offset=4)
    assert await worker._load_workflow_claim_history(task(history()[:2], token="current-cursor")) == history()
    assert client.workflow_task_history.await_args.kwargs["next_history_page_token"] == "current-cursor"
    assert worker.sticky_cache_metrics()["hit"] == 0
    await worker.stop()
    assert worker.sticky_cache_metrics()["entries"] == 0


@pytest.mark.asyncio
async def test_incomplete_cold_history_and_repeated_tokens_never_replay_partial_data() -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="q", sticky_cache_capacity=1)
    client.workflow_task_history.return_value = {"history_events": history()[2:], "next_history_page_token": None}
    with pytest.raises(LocalActivityExecutionAborted, match="complete canonical"):
        await worker._load_workflow_claim_history(task(history()[4:]))
    client.workflow_task_history.return_value = {"history_events": history()[:2], "next_history_page_token": "repeat"}
    with pytest.raises(LocalActivityExecutionAborted, match="paging did not advance"):
        await worker._load_workflow_claim_history(task(history()[:2], mode="cold_replay", token="repeat"))
    client.complete_workflow_task.assert_not_awaited()


def test_oversized_and_terminal_histories_issue_no_affinity_claim() -> None:
    worker = Worker(AsyncMock(spec=Client), task_queue="q", build_id="build",
                    sticky_cache_capacity=2, sticky_cache_max_bytes=100)
    assert worker._sticky_cache_claim(task(history()), history(), [{"type": "start_timer"}]) is None
    worker = Worker(AsyncMock(spec=Client), task_queue="q", build_id="build", sticky_cache_capacity=2)
    assert worker._sticky_cache_claim(task(history()), history(), [{"type": "start_timer"}]) is not None
    assert worker._sticky_cache_claim(task(history()), history(), [{"type": "complete_workflow"}]) is None
    assert worker.sticky_cache_metrics()["entries"] == 0


@pytest.mark.asyncio
async def test_completion_transport_retries_identical_claim_and_omits_it_when_disabled() -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="q", worker_id="holder", build_id="build", sticky_cache_capacity=1)
    claim = worker._sticky_cache_claim(task(history()), history(), [{"type": "start_timer"}])
    client.complete_workflow_task.side_effect = [ServerError(503, {}), {}]
    with patch("durable_workflow.worker.asyncio.sleep", new_callable=AsyncMock):
        await worker._complete_workflow_task_with_retry(
            task_id="current-task", attempt=3, commands=[{"type": "start_timer"}], sticky_cache=claim,
        )
    assert client.complete_workflow_task.await_args_list[0] == client.complete_workflow_task.await_args_list[1]
    assert client.complete_workflow_task.await_args.kwargs["sticky_cache"] == claim
    client.complete_workflow_task.reset_mock(side_effect=True)
    await worker._complete_workflow_task_with_retry(task_id="next-task", attempt=1, commands=[{"type": "start_timer"}])
    assert "sticky_cache" not in client.complete_workflow_task.await_args.kwargs


@pytest.mark.asyncio
async def test_client_completion_serializes_claim_on_worker_plane() -> None:
    async with Client("https://server.example", worker_token="test") as client:
        with patch.object(client, "_request", new_callable=AsyncMock) as request:
            await client.complete_workflow_task(
                task_id="task", lease_owner="worker", workflow_task_attempt=1,
                commands=[{"type": "start_timer"}], sticky_cache={"worker_id": "worker"},
            )
        assert request.await_args.kwargs["worker"] is True
        assert request.await_args.kwargs["json"]["sticky_cache"] == {"worker_id": "worker"}
