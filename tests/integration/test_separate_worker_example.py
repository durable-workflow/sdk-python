"""Run the documented worker roles as independent processes against Server."""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest

from durable_workflow import Client

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "separate_workers.py"


@pytest.mark.asyncio
@pytest.mark.parametrize(("separate_queues", "retry_once"), [(False, False), (False, True), (True, True)])
async def test_separate_workers_retry_and_deregister(
    server_url: str, server_token: str, separate_queues: bool, retry_once: bool,
) -> None:
    suffix = uuid4().hex[:12]
    workflow_queue = f"split-workflow-{suffix}"
    activity_queue = f"split-activity-{suffix}" if separate_queues else workflow_queue
    worker_ids = [f"split-workflow-{suffix}", f"split-activity-{suffix}"]
    environment = {
        **os.environ, "SERVER_URL": server_url, "WORKFLOW_TOKEN": server_token,
        "WORKFLOW_NAMESPACE": "default", "PYTHONUNBUFFERED": "1",
    }
    processes: list[asyncio.subprocess.Process] = []
    output: list[str] = []
    try:
        for role, worker_id, queue in zip(
            ("workflow", "activity"), worker_ids, (workflow_queue, activity_queue), strict=True,
        ):
            processes.append(await asyncio.create_subprocess_exec(
                sys.executable, str(EXAMPLE), role, "--worker-id", worker_id,
                "--queue", queue, "--slots", "1", env=environment,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            ))
        async with Client(server_url, token=server_token, namespace="default") as client:
            deadline = time.monotonic() + 10
            while True:
                roster = await client.list_workers()
                workers = {worker.worker_id: worker for worker in roster.workers}
                if set(worker_ids) <= workers.keys():
                    break
                assert all(process.returncode is None for process in processes), "worker exited during registration"
                assert time.monotonic() < deadline, "separate worker registrations did not arrive"
                await asyncio.sleep(0.1)
            workflow_worker, activity_worker = (workers[worker_id] for worker_id in worker_ids)
            assert workflow_worker.task_queue == workflow_queue
            assert activity_worker.task_queue == activity_queue
            assert workflow_worker.supported_workflow_types == ["examples.split-workers.greeter"]
            assert workflow_worker.supported_activity_types == []
            assert activity_worker.supported_workflow_types == []
            assert activity_worker.supported_activity_types == ["examples.split-workers.greet"]
            assert processes[0].pid != processes[1].pid

        arguments = ["--queue", workflow_queue, "--name", suffix]
        if retry_once:
            arguments.append("--retry-once")
        if separate_queues:
            arguments.extend(["--activity-queue", activity_queue])
        caller = await asyncio.create_subprocess_exec(
            sys.executable, str(EXAMPLE), "start", *arguments, env=environment,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            result, _ = await asyncio.wait_for(caller.communicate(), timeout=35)
        finally:
            if caller.returncode is None:
                caller.kill()
                await caller.wait()
        assert caller.returncode == 0, result.decode()[-8000:]
        events = [json.loads(line) for line in result.decode().splitlines() if line.startswith("{")]
        assert events[-1] == {"event": "completed", "result": f"hello, {suffix}"}
    finally:
        for process in processes:
            if process.returncode is None:
                process.send_signal(signal.SIGTERM)
        for process in processes:
            try:
                stdout, _ = await asyncio.wait_for(process.communicate(), timeout=15)
            except asyncio.TimeoutError:
                process.kill()
                stdout, _ = await process.communicate()
            output.append(stdout.decode())
        print(json.dumps({"separate_queues": separate_queues, "retry_once": retry_once, "worker_output": output}),
              flush=True)

    assert all(process.returncode == 0 for process in processes), output
    assert '"event": "activity_attempt"' not in output[0], output
    activity_events = [json.loads(line) for line in output[1].splitlines() if line.startswith("{")]
    attempts = [event for event in activity_events if event["event"] == "activity_attempt"]
    assert [event["attempt"] for event in attempts] == ([1, 2] if retry_once else [1]), output
    assert all(event["worker_id"] == worker_ids[1] for event in attempts), output
    assert all(event["process_id"] == processes[1].pid for event in attempts), output
    async with Client(server_url, token=server_token, namespace="default") as client:
        remaining = await client.list_workers()
        assert not (set(worker_ids) & {worker.worker_id for worker in remaining.workers})
