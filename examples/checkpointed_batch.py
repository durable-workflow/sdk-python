"""Bounded source example using the unreleased checkpoint API. See CHECKPOINTED_BATCH.md."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from collections.abc import Awaitable, Callable, Generator
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from durable_workflow import ActivityRetryPolicy, Client, Worker, activity, workflow
from durable_workflow.errors import ActivityHeartbeatResumeUnavailable, NonRetryableError

FAILURES = ("before-commit", "after-commit", "after-checkpoint")
ACTIVITY = "examples.checkpointed-batch"


@dataclass(frozen=True)
class Batch:
    business_id: str
    items: list[str]
    failure: str

    def snapshot(self) -> tuple[str, tuple[str, ...], str]:
        if not isinstance(self.items, list) or not 1 <= len(self.items) <= 16 or self.failure not in FAILURES:
            raise NonRetryableError("use a business ID and one to sixteen bounded items")
        try:
            valid = all(isinstance(value, str) and 1 <= len(value.encode()) <= 128
                        for value in [self.business_id, *self.items])
        except UnicodeEncodeError:
            valid = False
        if not valid:
            raise NonRetryableError("use a business ID and one to sixteen bounded items")
        items = tuple(self.items)
        encoded = json.dumps([self.business_id, items], ensure_ascii=False, separators=(",", ":")).encode()
        return self.business_id, items, hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class Progress:
    batch_sha256: str
    next_item: int


@dataclass(frozen=True)
class ReceiptStore:
    """Synthetic durable provider state, independent of Server checkpoints."""

    root: Path

    @classmethod
    def create(cls, root: Path) -> ReceiptStore:
        root.mkdir(mode=0o700)
        return cls(root)

    def sync_directory(self) -> None:
        descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def commit_or_reconcile(self, business_id: str, fingerprint: str, index: int, item: str) -> bool:
        key = json.dumps([business_id, index], ensure_ascii=False, separators=(",", ":")).encode()
        path = self.root / (hashlib.sha256(key).hexdigest() + ".json")
        receipt = json.dumps({"key": [business_id, index], "batch_sha256": fingerprint, "item": item},
                             ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        try:
            with path.open("xb") as stream:
                stream.write(receipt)
                stream.flush()
                os.fsync(stream.fileno())
            created = True
        except FileExistsError:
            with path.open("rb") as stream:
                if stream.read(4097) != receipt:
                    raise NonRetryableError(
                        "provider receipt conflicts or is partial; reconcile before retrying",
                    ) from None
                os.fsync(stream.fileno())
            created = False
        self.sync_directory()
        return created


async def process_batch(
    store: ReceiptStore, batch: Batch, received: Progress | None, attempt: int,
    acknowledge: Callable[[Progress], Awaitable[None]],
) -> dict[str, Any]:
    business_id, items, fingerprint = batch.snapshot()
    if type(attempt) is not int or attempt < 1:
        raise NonRetryableError("the activity attempt must be positive")
    if received is not None and (
        received.batch_sha256 != fingerprint or type(received.next_item) is not int
        or not 0 <= received.next_item <= len(items)
    ):
        raise NonRetryableError("checkpoint belongs to different input or is out of range")
    start = received.next_item if received is not None else 0
    def fail(point: str, index: int) -> None:
        if attempt == 1 and index == 0 and batch.failure == point:
            raise RuntimeError("planned first-attempt failure at " + point)

    for index in range(start, len(items)):
        fail("before-commit", index)
        await asyncio.to_thread(store.commit_or_reconcile, business_id, fingerprint, index, items[index])
        fail("after-commit", index)
        await acknowledge(Progress(fingerprint, index + 1))
        fail("after-checkpoint", index)
    return {"business_id": business_id, "completed": len(items)}


@workflow.defn(name=ACTIVITY)
class BatchWorkflow:
    def run(self, ctx: workflow.WorkflowContext, batch: dict[str, Any]) -> Generator[Any, Any, Any]:
        return (yield ctx.schedule_activity(
            ACTIVITY, [batch], retry_policy=ActivityRetryPolicy(max_attempts=2, backoff_seconds=[1]),
            heartbeat_timeout=10, start_to_close_timeout=30, schedule_to_close_timeout=60,
        ))


async def run(root: Path, failure: str) -> None:
    store = await asyncio.to_thread(ReceiptStore.create, root)

    @activity.defn(name=ACTIVITY)
    async def execute(body: dict[str, Any]) -> dict[str, Any]:
        ctx = activity.context()
        try:
            received = ctx.last_heartbeat_details(result_type=Progress)
        except ActivityHeartbeatResumeUnavailable:
            raise NonRetryableError("this example requires activity_heartbeat_resume v1") from None

        async def acknowledge(progress: Progress) -> None:
            await ctx.heartbeat(asdict(progress))

        return await process_batch(store, Batch(**body), received, ctx.info.attempt_number, acknowledge)

    queue = "checkpointed-batch-" + uuid4().hex
    async with Client(os.environ.get("SERVER_URL", "http://127.0.0.1:8080"),
                      token=os.environ.get("WORKFLOW_TOKEN"), namespace="default", timeout=10) as client:
        worker = Worker(client, task_queue=queue, workflows=[BatchWorkflow], activities=[execute])
        handle = await client.start_workflow(
            workflow_type=ACTIVITY, task_queue=queue, workflow_id=queue,
            input=[asdict(Batch("demo-" + uuid4().hex, ["first", "second", "third"], failure))],
        )
        await worker.run_until(workflow_id=handle.workflow_id, timeout=70)
        print(json.dumps(await handle.result(timeout=3)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipts", type=Path, help="a new private synthetic-provider directory")
    parser.add_argument("failure", choices=FAILURES)
    args = parser.parse_args()
    asyncio.run(run(args.receipts, args.failure))


if __name__ == "__main__":
    main()
