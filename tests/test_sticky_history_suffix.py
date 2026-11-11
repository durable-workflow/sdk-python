"""sticky_history_suffix v1 (server#1017): accept a suffix only when it continues the cache."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import Client, Worker

KEY = ("workflow", "run", "build")


def history(count: int) -> list[dict[str, Any]]:
    return [{"id": f"event-{sequence}", "sequence": sequence,
             "event_type": "WorkflowStarted" if sequence == 1 else "SideEffectRecorded",
             "payload": {"value": {"number": sequence}}} for sequence in range(1, count + 1)]


def suffix_task(events: list[dict[str, Any]], *, after: int, last: int,
                mode: str = "sticky_hit_expected", token: str | None = None) -> dict[str, Any]:
    return {"task_id": "current-task", "workflow_id": "workflow", "run_id": "run",
            "workflow_task_attempt": 2, "history_events": events, "sticky_replay_mode": mode,
            "history_after_sequence": after, "history_last_sequence": last,
            "last_history_sequence": last, "next_history_page_token": token}


def warm_worker(client: Any, cached: int = 4) -> Worker:
    worker = Worker(client, task_queue="q", worker_id="holder", build_id="build", sticky_cache_capacity=2)
    worker._sticky_history_suffix_supported = True
    assert worker._sticky_cache.remember(KEY, history(cached))
    return worker


def cold_reply(count: int) -> dict[str, Any]:
    return {"history_events": history(count), "next_history_page_token": None}


@pytest.mark.asyncio
async def test_hit_appends_the_suffix_to_the_cached_prefix() -> None:
    client = AsyncMock(spec=Client)
    worker = warm_worker(client)
    loaded = await worker._load_workflow_claim_history(suffix_task(history(6)[4:], after=4, last=6))
    assert loaded == history(6)
    client.workflow_task_history.assert_not_awaited()
    assert worker.sticky_cache_metrics()["hit"] == 1
    assert worker.sticky_cache_metrics()["forced_cold_replay"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(("events", "after", "last"), [
    pytest.param(history(7)[5:], 4, 7, id="gap"),
    pytest.param(history(6)[3:], 4, 6, id="overlap"),
    pytest.param([history(7)[4], history(7)[6]], 4, 7, id="non-contiguous"),
    pytest.param(history(6)[4:5], 4, 6, id="ends-short-of-last"),
    pytest.param(history(6)[4:], 3, 6, id="boundary-differs-from-cache"),
])
async def test_suffix_that_does_not_continue_the_cache_refetches_full_history(
    events: list[dict[str, Any]], after: int, last: int,
) -> None:
    client = AsyncMock(spec=Client)
    client.workflow_task_history.return_value = cold_reply(last)
    worker = warm_worker(client)
    loaded = await worker._load_workflow_claim_history(suffix_task(events, after=after, last=last))
    assert loaded == history(last)
    client.workflow_task_history.assert_awaited_once_with(
        task_id="current-task", next_history_page_token="MA==", lease_owner="holder", workflow_task_attempt=2,
    )
    assert worker.sticky_cache_metrics()["hit"] == 0
    assert worker.sticky_cache_metrics()["forced_cold_replay"] == 1
    assert worker._sticky_cache.lookup(KEY) is None


@pytest.mark.asyncio
async def test_cold_suffix_without_a_cached_prefix_is_never_replayed_as_full_history() -> None:
    client = AsyncMock(spec=Client)
    client.workflow_task_history.return_value = cold_reply(6)
    worker = Worker(client, task_queue="q", worker_id="replacement", build_id="build", sticky_cache_capacity=1)
    worker._sticky_history_suffix_supported = True
    assert await worker._load_workflow_claim_history(suffix_task(history(6)[4:], after=4, last=6)) == history(6)
    assert client.workflow_task_history.await_args.kwargs["next_history_page_token"] == "MA=="


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["forced_cold_replay", "cold_replay", None])
async def test_suffix_outside_a_sticky_hit_refetches(mode: str | None) -> None:
    client = AsyncMock(spec=Client)
    client.workflow_task_history.return_value = cold_reply(6)
    worker = warm_worker(client)
    task = suffix_task(history(6)[4:], after=4, last=6)
    task["sticky_replay_mode"] = mode
    assert await worker._load_workflow_claim_history(task) == history(6)
    assert client.workflow_task_history.await_args.kwargs["next_history_page_token"] == "MA=="


@pytest.mark.asyncio
async def test_empty_suffix_is_a_hit_only_when_nothing_was_appended() -> None:
    client = AsyncMock(spec=Client)
    worker = warm_worker(client)
    assert await worker._load_workflow_claim_history(suffix_task([], after=4, last=4)) == history(4)
    client.workflow_task_history.return_value = cold_reply(5)
    assert await worker._load_workflow_claim_history(suffix_task([], after=4, last=5)) == history(5)
    assert worker.sticky_cache_metrics()["hit"] == 1
    assert worker.sticky_cache_metrics()["forced_cold_replay"] == 1


@pytest.mark.asyncio
async def test_paged_suffix_is_assembled_before_it_is_checked() -> None:
    client = AsyncMock(spec=Client)
    client.workflow_task_history.return_value = {"history_events": history(8)[6:], "next_history_page_token": None}
    worker = warm_worker(client)
    task = suffix_task(history(8)[4:6], after=4, last=8, token="suffix-page-two")
    assert await worker._load_workflow_claim_history(task) == history(8)
    assert client.workflow_task_history.await_args.kwargs["next_history_page_token"] == "suffix-page-two"
    assert worker.sticky_cache_metrics()["hit"] == 1


@pytest.mark.asyncio
async def test_server_without_suffix_support_never_uses_a_suffix() -> None:
    client = AsyncMock(spec=Client)
    client.workflow_task_history.return_value = cold_reply(6)
    worker = warm_worker(client)
    worker._sticky_history_suffix_supported = False
    assert await worker._load_workflow_claim_history(suffix_task(history(6)[4:], after=4, last=6)) == history(6)
    assert client.workflow_task_history.await_args.kwargs["next_history_page_token"] == "MA=="


@pytest.mark.asyncio
async def test_boundary_zero_keeps_the_full_history_path() -> None:
    client = AsyncMock(spec=Client)
    worker = warm_worker(client)
    task = suffix_task(history(6), after=0, last=6, mode="cold_replay")
    assert await worker._load_workflow_claim_history(task) == history(6)
    client.workflow_task_history.assert_not_awaited()


def test_claim_reports_the_cached_last_sequence_only_with_server_support() -> None:
    worker = Worker(AsyncMock(spec=Client), task_queue="q", build_id="build", sticky_cache_capacity=2)
    task = suffix_task(history(6), after=0, last=6)
    claim = worker._sticky_cache_claim(task, history(6), [{"type": "start_timer"}])
    assert claim is not None and "history_last_sequence" not in claim
    worker._sticky_history_suffix_supported = True
    claim = worker._sticky_cache_claim(task, history(6), [{"type": "start_timer"}])
    assert claim is not None and claim["history_last_sequence"] == 6
