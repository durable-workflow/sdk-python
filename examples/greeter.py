"""End-to-end demo: start a Python-authored workflow with one activity.

Known limitation (as of 2026-04-14): the server PHP-serializes workflow
*start inputs*. Non-PHP SDKs can't read them back. This demo therefore uses
a workflow with no start input — the activity arguments and workflow result
both round-trip through the Python SDK's JSON codec without the server
re-wrapping them.

Usage (from inside the server's docker network):

    SERVER_URL=http://server:8080 \
    WORKFLOW_TOKEN=dev-token-123 \
    python -m examples.greeter
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid

from durable_workflow import Client, Worker, activity, workflow


@activity.defn(name="greet")
def greet(name: str) -> str:
    return f"hello, {name}"


@workflow.defn(name="greeter")
class GreeterWorkflow:
    def run(self, ctx, *_ignored):
        greeting = yield ctx.schedule_activity("greet", ["world"])
        return {"greeting": greeting, "length": len(greeting)}


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    url = os.environ.get("SERVER_URL", "http://server:8080")
    token = os.environ.get("WORKFLOW_TOKEN", "dev-token-123")
    task_queue = "python-workers"
    wf_id = f"greet-{uuid.uuid4().hex[:6]}"

    async with Client(url, token=token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[GreeterWorkflow],
            activities=[greet],
        )
        handle = await client.start_workflow(
            workflow_type="greeter",
            task_queue=task_queue,
            workflow_id=wf_id,
            input=[],
        )
        print(f"started {handle.workflow_id} run={handle.run_id}", flush=True)

        worker_task = asyncio.create_task(worker.run())

        try:
            result = await client.get_result(handle, timeout=180.0)
            print(f"RESULT: {result!r}", flush=True)
            rc = 0
        except Exception as e:
            print(f"FAILED: {e}", flush=True)
            rc = 1
        finally:
            worker.stop()
            worker_task.cancel()
            try:
                await worker_task
            except (asyncio.CancelledError, Exception):
                pass
        return rc


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
