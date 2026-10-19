"""Run workflow and remote activity handlers in separate processes.

See docs/guides/separate-workers.md for installation and three-terminal usage.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from uuid import uuid4

from durable_workflow import ActivityRetryPolicy, Client, Worker, activity, workflow

WORKFLOW_TYPE = "examples.split-workers.greeter"
ACTIVITY_TYPE = "examples.split-workers.greet"


@activity.defn(name=ACTIVITY_TYPE)
async def greet(name: str, retry_once: bool) -> str:
    info = activity.context().info
    print(json.dumps({
        "event": "activity_attempt", "worker_id": info.worker_id,
        "attempt": info.attempt_number, "process_id": os.getpid(),
    }), flush=True)
    if retry_once and info.attempt_number == 1:
        raise RuntimeError("intentional first-attempt failure for the example")
    return f"hello, {name}"


@workflow.defn(name=WORKFLOW_TYPE)
class Greeter:
    def run(self, ctx, name: str, retry_once: bool, activity_queue: str | None):
        return (yield ctx.schedule_activity(
            ACTIVITY_TYPE,
            [name, retry_once],
            queue=activity_queue,
            retry_policy=ActivityRetryPolicy(max_attempts=2, initial_interval_seconds=1),
            start_to_close_timeout=5,
            schedule_to_close_timeout=20,
        ))


def positive_slots(value: str) -> int:
    slots = int(value)
    if slots < 1:
        raise argparse.ArgumentTypeError("slots must be at least 1")
    return slots


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    roles = parser.add_subparsers(dest="role", required=True)
    for role in ("workflow", "activity"):
        worker = roles.add_parser(role, help=f"run only {role} handlers")
        worker.add_argument("--worker-id", required=True, help="unique for each live worker process")
        worker.add_argument("--queue", default="python-split")
        worker.add_argument("--slots", type=positive_slots, default=4)
    start = roles.add_parser("start", help="start a workflow and wait up to 30 seconds")
    start.add_argument("--queue", default="python-split")
    start.add_argument("--name", default="world")
    start.add_argument("--retry-once", action="store_true")
    start.add_argument("--activity-queue", help="optional explicit queue for the remote activity")
    inspect = roles.add_parser("inspect", help="read the registered worker roster (operator credential)")
    inspect.add_argument("--queue", default="python-split")
    return parser.parse_args()


async def run_worker(client: Client, args: argparse.Namespace) -> None:
    workflow_role = args.role == "workflow"
    worker = Worker(
        client,
        task_queue=args.queue,
        worker_id=args.worker_id,
        workflows=[Greeter] if workflow_role else [],
        activities=[] if workflow_role else [greet],
        max_concurrent_workflow_tasks=args.slots if workflow_role else 1,
        max_concurrent_activity_tasks=1 if workflow_role else args.slots,
        poll_timeout=5,
        shutdown_timeout=10,
    )
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for requested_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(requested_signal, shutdown.set)
    run_task = asyncio.create_task(worker.run())
    shutdown_task = asyncio.create_task(shutdown.wait())
    try:
        done, _ = await asyncio.wait({run_task, shutdown_task}, return_when=asyncio.FIRST_COMPLETED)
        if run_task in done:
            await run_task
    finally:
        shutdown_task.cancel()
        await asyncio.gather(shutdown_task, return_exceptions=True)
        try:
            await worker.stop()
        finally:
            await run_task
            for requested_signal in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(requested_signal)
    print(json.dumps({"event": "worker_stopped", "worker_id": worker.worker_id}), flush=True)


async def main(args: argparse.Namespace) -> None:
    server_url = os.environ.get("SERVER_URL")
    token = os.environ.get("WORKFLOW_TOKEN")
    if not server_url or not token:
        raise SystemExit("set SERVER_URL and WORKFLOW_TOKEN; see the separate-workers guide")
    async with Client(
        server_url, token=token, namespace=os.environ.get("WORKFLOW_NAMESPACE", "default"),
    ) as client:
        if args.role in ("workflow", "activity"):
            await run_worker(client, args)
        elif args.role == "inspect":
            roster = await client.list_workers(task_queue=args.queue)
            print(json.dumps([{
                "worker_id": worker.worker_id,
                "task_queue": worker.task_queue,
                "status": worker.status,
                "workflow_types": worker.supported_workflow_types,
                "activity_types": worker.supported_activity_types,
                "workflow_slots": worker.max_concurrent_workflow_tasks,
                "activity_slots": worker.max_concurrent_activity_tasks,
            } for worker in roster.workers], indent=2))
        else:
            handle = await client.start_workflow(
                workflow_type=WORKFLOW_TYPE,
                workflow_id=f"python-split-{uuid4().hex}",
                task_queue=args.queue,
                input=[args.name, args.retry_once, args.activity_queue],
                execution_timeout_seconds=60,
                run_timeout_seconds=60,
            )
            print(json.dumps({"event": "started", "workflow_id": handle.workflow_id, "run_id": handle.run_id}))
            result = await handle.result(timeout=30, poll_interval=0.2)
            print(json.dumps({"event": "completed", "result": result}), flush=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main(parse_args()))
