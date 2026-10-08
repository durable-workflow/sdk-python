"""Qualify process-local history retention against Server's durable state."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from durable_workflow import Client, Worker, workflow


@workflow.defn(name="tests.python-sticky-history")
class StickyHistoryWorkflow:
    def __init__(self) -> None:
        self.stage = 0

    @workflow.signal("advance")
    def advance(self, stage: int) -> None:
        self.stage = max(self.stage, stage)

    def run(self, ctx: Any, batch_size: int) -> Any:
        total = 0
        for phase in range(1, 4):
            if phase <= 2:
                for index in range(batch_size):
                    total += yield ctx.side_effect(lambda value=index: value)
            yield ctx.upsert_memo({"sticky_phase": phase})
            yield ctx.wait_condition(lambda current=phase: self.stage >= current,
                                     key=f"sticky-phase-{phase}", timeout=300)
        return {"total": total, "stage": self.stage}


async def wait_phase(handle: Any, phase: int) -> None:
    async def wait() -> None:
        while True:
            description = await handle.describe()
            if (description.status or "").lower() == "waiting" and description.memo == {"sticky_phase": phase}:
                return
            await asyncio.sleep(0.1)
    await asyncio.wait_for(wait(), timeout=90)


async def start_worker(client: Client, queue: str, *, worker_id: str,
                       capacity: int = 2, max_bytes: int = 16 * 1024 * 1024,
                       build_id: str | None = None) -> tuple[Worker, asyncio.Task[None]]:
    worker = Worker(client, task_queue=queue, workflows=[StickyHistoryWorkflow], worker_id=worker_id,
                    sticky_cache_capacity=capacity, sticky_cache_max_bytes=max_bytes, build_id=build_id)
    runner = asyncio.create_task(worker.run())
    await asyncio.wait_for(worker._registration_done.wait(), timeout=20)
    if runner.done():
        await runner
    return worker, runner


async def stop_worker(worker: Worker, runner: asyncio.Task[None]) -> None:
    await worker.stop()
    await asyncio.wait_for(runner, timeout=20)


@pytest.mark.asyncio
async def test_warm_paged_history_skips_retained_middle_pages(server_url: str, server_token: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    queue = f"sticky-warm-{suffix}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        with patch.object(client, "workflow_task_history", new_callable=AsyncMock,
                          wraps=client.workflow_task_history) as pages:
            worker, runner = await start_worker(client, queue, worker_id=f"sticky-holder-{suffix}")
            load = AsyncMock(wraps=worker._load_workflow_claim_history)
            worker._load_workflow_claim_history = load
            try:
                handle = await client.start_workflow(workflow_type="tests.python-sticky-history", task_queue=queue,
                                                     workflow_id=f"sticky-run-{suffix}", input=[600])
                for phase in (1, 2):
                    await wait_phase(handle, phase)
                    await handle.signal("advance", [phase])
                await wait_phase(handle, 3)
                before = pages.await_count
                retained = worker._sticky_cache.lookup(worker._sticky_cache_key({
                    "workflow_id": handle.workflow_id, "run_id": handle.run_id,
                }))
                assert retained is not None
                assert retained[2] >= 1000
                await handle.signal("advance", [3])
                assert await handle.result(timeout=60) == {"total": 359400, "stage": 3}
                last_task = load.await_args.args[0]
                differences = [(index, sorted(set(event) | set(retained[0][index])))
                               for index, event in enumerate(last_task["history_events"])
                               if index < len(retained[0]) and event != retained[0][index]]
                assert pages.await_count - before == 1, json.dumps({
                    "new_cursors": [call.kwargs["next_history_page_token"]
                                    for call in pages.await_args_list[before:]],
                    "retained_cursor": retained[1], "retained_offset": retained[2],
                    "retained_events": len(retained[0]), "metrics": worker.sticky_cache_metrics(),
                    "mode": last_task.get("sticky_replay_mode"),
                    "last": last_task.get("last_history_sequence"), "count": last_task.get("total_history_events"),
                    "inline_differences": differences[:1], "inline_first": last_task["history_events"][0]["event_type"],
                })
                assert pages.await_args.kwargs["next_history_page_token"] == retained[1]
                assert worker.sticky_cache_metrics()["hit"] >= 3
                assert worker.sticky_cache_metrics()["entries"] == 0
            finally:
                await stop_worker(worker, runner)


@pytest.mark.asyncio
@pytest.mark.parametrize("max_bytes", [16 * 1024 * 1024, 1])
async def test_eviction_or_oversized_cache_uses_cold_history(
    server_url: str, server_token: str, max_bytes: int,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    queue = f"sticky-bounds-{suffix}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        worker, runner = await start_worker(client, queue, worker_id=f"sticky-small-{suffix}",
                                            capacity=1, max_bytes=max_bytes)
        try:
            first = await client.start_workflow(workflow_type="tests.python-sticky-history", task_queue=queue,
                                                workflow_id=f"sticky-first-{suffix}", input=[2])
            await wait_phase(first, 1)
            second = await client.start_workflow(workflow_type="tests.python-sticky-history", task_queue=queue,
                                                 workflow_id=f"sticky-second-{suffix}", input=[2])
            await wait_phase(second, 1)
            assert worker.sticky_cache_metrics()["entries"] <= 1
            assert worker.sticky_cache_metrics()["history_bytes"] <= max_bytes
            if max_bytes > 1:
                assert worker.sticky_cache_metrics()["eviction"] >= 1
            for phase in (1, 2, 3):
                await first.signal("advance", [phase])
                if phase < 3:
                    await wait_phase(first, phase + 1)
            assert await first.result(timeout=30) == {"total": 2, "stage": 3}
            assert worker.sticky_cache_metrics()["miss"] >= 3
            if max_bytes > 1:
                assert worker.sticky_cache_metrics()["forced_cold_replay"] >= 1
            await second.cancel()
        finally:
            await stop_worker(worker, runner)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement_build", [None, "build-after"])
async def test_replacement_replays_original_run_without_retained_state(
    server_url: str, server_token: str, replacement_build: str | None,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    queue = f"sticky-replace-{suffix}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        original, original_runner = await start_worker(
            client, queue, worker_id=f"sticky-before-{suffix}",
            build_id="build-before" if replacement_build is not None else None,
        )
        try:
            handle = await client.start_workflow(workflow_type="tests.python-sticky-history", task_queue=queue,
                                                 workflow_id=f"sticky-replace-run-{suffix}", input=[2])
            await wait_phase(handle, 1)
            run_id = handle.run_id
            assert original.sticky_cache_metrics()["entries"] == 1
        finally:
            await stop_worker(original, original_runner)
        replacement, runner = await start_worker(
            client, queue, worker_id=f"sticky-after-{suffix}", build_id=replacement_build,
        )
        try:
            assert replacement.sticky_cache_metrics()["entries"] == 0
            if replacement_build is not None:
                # Existing runs keep their build pin. A different build cannot take
                # their task or turn affinity into permission to change workflow code.
                await handle.signal("advance", [1])
                assert await client.poll_workflow_task(
                    worker_id=replacement.worker_id, task_queue=queue, build_id=replacement_build, timeout=0,
                ) is None
                assert (await handle.describe()).memo == {"sticky_phase": 1}
                assert replacement.sticky_cache_metrics()["miss"] == 0
                await stop_worker(replacement, runner)
                replacement, runner = await start_worker(
                    client, queue, worker_id=f"sticky-compatible-{suffix}", build_id="build-before",
                )
            for phase in (1, 2, 3):
                if phase != 1 or replacement_build is None:
                    await handle.signal("advance", [phase])
                if phase < 3:
                    await wait_phase(handle, phase + 1)
            assert await handle.result(timeout=30) == {"total": 2, "stage": 3}
            assert (await handle.describe()).run_id == run_id
            assert replacement.sticky_cache_metrics()["miss"] >= 1
        finally:
            await stop_worker(replacement, runner)
