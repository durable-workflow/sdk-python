"""A real child worker for cooperative process-loss qualification."""

from __future__ import annotations

import asyncio
import json
import os
import sys

from durable_workflow import Client, activity
from tests.integration.test_cooperative_cancellation import candidate_worker, cooperative_cleanup, poll_claim


async def main() -> None:
    queue, worker_id, mode = sys.argv[1:]
    if mode not in {"hold", "finish", "remote"}:
        raise ValueError("unknown qualification mode")
    async with Client(
        os.environ["DURABLE_WORKFLOW_SERVER_URL"],
        token=os.environ.get("DURABLE_WORKFLOW_AUTH_TOKEN", "test-token"), namespace="default",
    ) as client:
        worker = candidate_worker(client, queue, worker_id=worker_id)

        async def cleanup(request_id: str) -> str:
            print(json.dumps({"phase": "cleanup", "request_id": request_id, "worker_id": worker_id}), flush=True)
            if mode == "hold":
                await asyncio.Event().wait()
            return await cooperative_cleanup(request_id)

        worker.activities["tests.python-cooperative-cleanup"] = cleanup
        if mode == "remote":
            async def blocked_remote() -> object:
                info = activity.context().info
                print(json.dumps({"phase": "remote-entered", "task_id": info.task_id,
                                  "activity_attempt_id": info.activity_attempt_id,
                                  "lease_owner": info.worker_id}), flush=True)
                await asyncio.Event().wait()
                return object()
            worker.activities["tests.python-cooperative-work"] = blocked_remote
            await worker.run()
            return
        await worker._register()
        try:
            task = await poll_claim(client, worker)
            observation = task["cancellation_request"]
            print(json.dumps({
                "phase": "claim", "attempt": task["workflow_task_attempt"], "worker_id": worker_id,
                "task_id": task["task_id"], "request_id": observation["request_id"],
                "cleanup_deadline_at": observation["cleanup_deadline_at"],
            }), flush=True)
            commands = await worker._run_workflow_task(task)
            print(json.dumps({"phase": "finished", "committed": commands is not None}), flush=True)
        finally:
            await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
