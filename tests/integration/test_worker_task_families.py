"""Exercise ordinary workers and count their real HTTP poll requests."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import uuid
from collections import Counter
from contextvars import ContextVar
from typing import Any

import httpx
import pytest

from durable_workflow import Client, Worker, activity, workflow
from tests.worker_observation import (
    ObservationInterceptor,
    WorkerObservation,
    result_or_worker_error,
    stop_and_drain,
)

current_observation: ContextVar[WorkerObservation] = ContextVar("task_family_observation")


@activity.defn(name="tests.task-family.echo")
async def echo(value: str) -> str:
    return f"echo:{value}"


@workflow.defn(name="tests.task-family.activity")
class ActivityWorkflow:
    def run(self, ctx: Any, value: str) -> Any:
        if observation := current_observation.get(None):
            observation.note("workflow_handler_enter")
        return (yield ctx.schedule_activity("tests.task-family.echo", [value]))


@workflow.defn(name="tests.task-family.timer")
class TimerWorkflow:
    def run(self, ctx: Any, value: str) -> Any:
        if observation := current_observation.get(None):
            observation.note("workflow_handler_enter")
        yield ctx.sleep(2)
        return f"timer:{value}"


def record_polls(client: Client) -> Counter[str]:
    counts: Counter[str] = Counter()

    async def record(request: httpx.Request) -> None:
        for family in ("workflow", "activity", "query"):
            if request.url.path.endswith(f"/worker/{family}-tasks/poll"):
                counts[family] += 1

    client._http.event_hooks["request"].append(record)
    return counts


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["split", "mixed", "timer"])
@pytest.mark.parametrize("entry_point", ["run", "run_until"])
async def test_workers_execute_without_polling_empty_task_families(
    server_url: str, server_token: str, mode: str, entry_point: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation = WorkerObservation()
    observation.install_http(monkeypatch)
    suffix = uuid.uuid4().hex[:12]
    queue = f"task-family-{suffix}"
    async with (
        Client(server_url, token=server_token, namespace="default") as control,
        Client(server_url, token=server_token, namespace="default") as workflow_client,
        Client(server_url, token=server_token, namespace="default") as activity_client,
    ):
        observation.clients.update({
            id(control._http): "control", id(workflow_client._http): "workflow",
            id(activity_client._http): "activity",
        })
        workflow_polls = record_polls(workflow_client)
        activity_polls = record_polls(activity_client)
        workflow_definition = TimerWorkflow if mode == "timer" else ActivityWorkflow
        worker = Worker(
            workflow_client, task_queue=queue,
            workflows=[workflow_definition],
            activities=[echo] if mode == "mixed" else [],
            poll_timeout=0.5, shutdown_timeout=10,
            interceptors=[ObservationInterceptor(observation)],
        )
        workers = [worker]
        if mode == "split":
            workers.append(Worker(
                activity_client, task_queue=queue, workflows=[], activities=[echo],
                poll_timeout=0.5, shutdown_timeout=10,
                interceptors=[ObservationInterceptor(observation)],
            ))

        handle = await control.start_workflow(
            workflow_type="tests.task-family.timer" if mode == "timer" else "tests.task-family.activity",
            task_queue=queue, workflow_id=f"task-family-{suffix}", input=[suffix],
        )
        context_token = current_observation.set(observation)
        runners = [asyncio.create_task(
            selected.run() if entry_point == "run" else selected.run_until(
                workflow_id=handle.workflow_id, timeout=30, poll_interval=0.05,
            )
        ) for selected in workers]
        identity = {
            "sdk_version": importlib.metadata.version("durable-workflow"),
            "mode": mode, "entry_point": entry_point, "workflow_id": handle.workflow_id,
            "run_id": handle.run_id, "task_queue": queue,
            "workers": [selected.worker_id for selected in workers],
        }
        primary_error: BaseException | None = None
        try:
            expected = f"timer:{suffix}" if mode == "timer" else f"echo:{suffix}"
            assert await result_or_worker_error(handle.result(timeout=30, poll_interval=0.1), runners) == expected
            if entry_point == "run_until":
                terminal = await asyncio.wait_for(asyncio.gather(*runners), timeout=10)
                assert all((result.status or "").lower() == "completed" for result in terminal)
            assert observation.milestones["workflow_handler_enter"]["count"] > 0
            assert ("activity_handler_enter" in observation.milestones) is (mode != "timer")
        except BaseException as error:
            primary_error = error
            print(json.dumps({
                **identity, "phase": "before_shutdown", "exception": type(error).__name__,
                "observation": observation.report(),
            }), flush=True)
            raise
        finally:
            try:
                await stop_and_drain(workers, runners)
            except BaseException as error:
                print(json.dumps({
                    **identity, "phase": "shutdown", "exception": type(error).__name__,
                    "observation": observation.report(),
                }), flush=True)
                if primary_error is None:
                    raise
            finally:
                current_observation.reset(context_token)

        assert workflow_polls["workflow"] > 0, observation.report()
        assert (workflow_polls["activity"] > 0) is (mode == "mixed"), observation.report()
        assert observation.successful["POST registration"] >= len(workers), observation.report()
        assert observation.successful["DELETE registration"] >= len(workers), observation.report()
        assert observation.active_count == 0, observation.report()
        if mode == "split":
            assert activity_polls["activity"] > 0, observation.report()
            assert activity_polls["workflow"] == 0, observation.report()
            assert activity_polls["query"] == 0, observation.report()
        print(json.dumps({
            **identity,
            "workflow_worker_polls": dict(workflow_polls),
            "activity_worker_polls": dict(activity_polls), "outcome": "pass",
            "observation": observation.report(include_recent=False),
        }), flush=True)
