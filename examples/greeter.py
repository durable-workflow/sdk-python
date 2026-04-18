"""End-to-end demo: start a Python-authored workflow with one activity.

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
    log = logging.getLogger("greeter")
    url = os.environ.get("SERVER_URL", "http://server:8080")
    token = os.environ.get("WORKFLOW_TOKEN", "dev-token-123")
    task_queue = "python-workers"
    wf_id = f"greet-{uuid.uuid4().hex[:6]}"

    async with Client(url, token=token, namespace="default") as client:
        handle = await client.start_workflow(
            workflow_type="greeter",
            task_queue=task_queue,
            workflow_id=wf_id,
            input=[],
        )
        log.info("started %s run=%s", handle.workflow_id, handle.run_id)

        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[GreeterWorkflow],
            activities=[greet],
        )

        log.info("running worker until workflow completes...")
        await worker.run_until(workflow_id=wf_id, timeout=30.0)

    log.info("worker stopped, checking result...")
    async with Client(url, token=token, namespace="default") as client2:
        handle2 = client2.get_workflow_handle(handle.workflow_id, run_id=handle.run_id)
        try:
            result = await handle2.result(timeout=10.0, poll_interval=1.0)
            log.info("RESULT: %r", result)
            print(f"RESULT: {result!r}", flush=True)
            return 0
        except Exception as e:
            log.error("FAILED: %s", e)
            print(f"FAILED: {e}", flush=True)
            return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
