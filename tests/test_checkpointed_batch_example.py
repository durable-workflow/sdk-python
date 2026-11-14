"""Synthetic provider/acknowledgement boundaries; no Server or worker invocation."""
from __future__ import annotations

import pickle
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from examples.checkpointed_batch import ACTIVITY, FAILURES, Batch, BatchActivity, Progress, ReceiptStore, process_batch

from durable_workflow import ActivityInfo, Client, Worker
from durable_workflow._activity_process import CallbackInvocation
from durable_workflow.errors import NonRetryableError


def batch(failure: str = "after-commit") -> Batch:
    return Batch("business-batch", ["first", "second", "third"], failure)


def inventory(store: ReceiptStore) -> list[tuple[str, bytes]]:
    return sorted((path.name, path.read_bytes()) for path in store.root.iterdir())


@pytest.mark.parametrize("point", FAILURES)
async def test_commit_boundaries_resume_only_acknowledged_items(tmp_path: Path, point: str) -> None:
    store = ReceiptStore.create(tmp_path / "provider")
    request = batch(point)
    persisted = None

    async def acknowledge(progress: Progress) -> None:
        nonlocal persisted
        persisted = progress

    with pytest.raises(RuntimeError, match="planned first-attempt failure"):
        await process_batch(store, request, None, 1, acknowledge)
    assert (persisted.next_item if persisted else None) == (1 if point == "after-checkpoint" else None)
    assert len(inventory(store)) == (0 if point == "before-commit" else 1)
    resumed = []

    async def acknowledge_replacement(progress: Progress) -> None:
        resumed.append(progress.next_item)

    assert await process_batch(store, request, persisted, 2, acknowledge_replacement) == {
        "business_id": "business-batch", "completed": 3,
    }
    assert resumed == ([2, 3] if point == "after-checkpoint" else [1, 2, 3])
    before = inventory(store)
    business_id, items, fingerprint = request.snapshot()
    for index, item in enumerate(items):
        assert store.commit_or_reconcile(business_id, fingerprint, index, item) is False
    assert inventory(store) == before


@pytest.mark.parametrize("committed", [False, True])
async def test_lost_ack_response_handles_both_commit_outcomes(tmp_path: Path, committed: bool) -> None:
    store = ReceiptStore.create(tmp_path / "provider")
    request = batch("after-checkpoint")
    persisted = None

    async def lost_response(progress: Progress) -> None:
        nonlocal persisted
        if committed:
            persisted = progress
        raise TimeoutError("synthetic acknowledgement response lost")

    with pytest.raises(TimeoutError):
        await process_batch(store, request, None, 1, lost_response)
    assert len(inventory(store)) == 1
    next_items = []

    async def acknowledge(progress: Progress) -> None:
        next_items.append(progress.next_item)

    assert (await process_batch(store, request, persisted, 2, acknowledge))["completed"] == 3
    assert next_items == ([2, 3] if committed else [1, 2, 3])
    assert len(inventory(store)) == 3


@pytest.mark.parametrize("items", [
    ["first", "second", "third", "fourth"], ["first", "second"], ["second", "first", "third"],
])
async def test_changed_input_without_checkpoint_preserves_provider_state(tmp_path: Path, items: list[str]) -> None:
    store = ReceiptStore.create(tmp_path / "provider")

    async def no_op(_progress: Progress) -> None:
        pass

    request = batch()
    await process_batch(store, request, None, 2, no_op)
    before = inventory(store)

    async def forbidden(_progress: Progress) -> None:
        raise AssertionError("conflicting business input was acknowledged")

    with pytest.raises(NonRetryableError, match="provider receipt conflicts"):
        await process_batch(store, replace(request, items=items), None, 2, forbidden)
    assert inventory(store) == before


@pytest.mark.parametrize("cursor", [Progress("foreign", 1), Progress("same", -1),
                                   Progress("same", 4), Progress("same", True)])
async def test_bad_checkpoint_refuses_before_provider_access(tmp_path: Path, cursor: Progress) -> None:
    store = ReceiptStore.create(tmp_path / "provider")
    request = batch()
    if cursor.batch_sha256 == "same":
        cursor = replace(cursor, batch_sha256=request.snapshot()[2])

    async def forbidden(_progress: Progress) -> None:
        raise AssertionError("invalid checkpoint was acknowledged")

    with pytest.raises(NonRetryableError, match="checkpoint belongs"):
        await process_batch(store, request, cursor, 2, forbidden)
    assert inventory(store) == []


@pytest.mark.parametrize("items", [[], ["item"] * 17, [""], ["x" * 129], ["\ud800"], [False], "not-a-list"])
async def test_invalid_batch_refuses_before_provider_access(tmp_path: Path, items: Any) -> None:
    store = ReceiptStore.create(tmp_path / "provider")

    async def forbidden(_progress: Progress) -> None:
        raise AssertionError("invalid batch was acknowledged")

    with pytest.raises(NonRetryableError):
        await process_batch(store, Batch("business", items, "after-commit"), None, 2, forbidden)
    assert inventory(store) == []


def test_existing_directory_and_partial_receipt_are_preserved(tmp_path: Path) -> None:
    store = ReceiptStore.create(tmp_path / "provider")
    with pytest.raises(FileExistsError):
        ReceiptStore.create(store.root)
    business_id, items, fingerprint = batch().snapshot()
    assert store.commit_or_reconcile(business_id, fingerprint, 0, items[0])
    path = next(store.root.iterdir())
    path.write_bytes(b"partial")
    with pytest.raises(NonRetryableError, match="partial"):
        store.commit_or_reconcile(business_id, fingerprint, 0, items[0])
    assert path.read_bytes() == b"partial"


def test_actual_handler_survives_cooperative_preflight_and_callback_serialization(tmp_path: Path) -> None:
    store = ReceiptStore.create(tmp_path / "provider")
    handler = BatchActivity(store).execute
    worker = Worker(AsyncMock(spec=Client), task_queue="queue", activities=[handler])
    worker._cooperative_cancellation_supported = True
    worker._validate_cooperative_activity_handlers()
    assert worker.activities[ACTIVITY] == handler
    invocation = CallbackInvocation(
        handler=worker.activities[ACTIVITY], args=(asdict(batch()),),
        info=ActivityInfo("task", ACTIVITY, "attempt", 2, "queue", "worker"), task={},
    )
    restored = pickle.loads(invocation.serialize())
    assert isinstance(restored.handler.__self__, BatchActivity)
    assert restored.handler.__self__.store == store
    assert restored.args == (asdict(batch()),)
    assert inventory(store) == []


def test_nested_handler_is_refused_by_the_same_preflight_and_serializer() -> None:
    async def legacy_nested_handler(_body: dict[str, Any]) -> None:
        pass

    worker = Worker(AsyncMock(spec=Client), task_queue="queue", activities=[legacy_nested_handler])
    worker._cooperative_cancellation_supported = True
    with pytest.raises(RuntimeError, match="spawn-compatible"):
        worker._validate_cooperative_activity_handlers()
    invocation = CallbackInvocation(
        handler=legacy_nested_handler, args=(),
        info=ActivityInfo("task", ACTIVITY, "attempt", 2, "queue", "worker"), task={},
    )
    with pytest.raises(ValueError, match="spawn-compatible"):
        invocation.serialize()
