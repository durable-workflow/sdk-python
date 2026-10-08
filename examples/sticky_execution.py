"""Run a workflow with bounded, optional history retention.

    SERVER_URL=http://localhost:8080 WORKFLOW_TOKEN=dev-token python examples/sticky_execution.py
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any

from durable_workflow import Client, Worker, workflow


@workflow.defn(name="examples.python-sticky")
class StickyExampleWorkflow:
    def run(self, ctx: Any) -> Any:
        value = yield ctx.side_effect(lambda: "recorded once")
        yield ctx.start_timer(1)
        yield ctx.start_timer(1)
        return value


async def main() -> None:
    async with Client(
        os.environ.get("SERVER_URL", "http://localhost:8080"),
        token=os.environ["WORKFLOW_TOKEN"], namespace=os.environ.get("DW_NAMESPACE", "default"),
    ) as client:
        queue = f"sticky-example-{uuid.uuid4().hex[:8]}"
        worker = Worker(client, task_queue=queue, workflows=[StickyExampleWorkflow],
                        sticky_cache_capacity=100, sticky_cache_max_bytes=16 * 1024 * 1024,
                        sticky_cache_ttl_seconds=300)
        handle = await client.start_workflow(workflow_type="examples.python-sticky", task_queue=queue,
                                             workflow_id=f"sticky-{uuid.uuid4().hex}")
        await worker.run_until(workflow_id=handle.workflow_id, timeout=60)
        assert await handle.result(timeout=10) == "recorded once"
        print(json.dumps(worker.sticky_cache_metrics(), sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
