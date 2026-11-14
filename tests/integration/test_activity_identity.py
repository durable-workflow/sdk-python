"""Observe logical identity inside real activity callbacks across Server retry."""

from __future__ import annotations

import importlib.metadata
import json
import os
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import durable_workflow
from durable_workflow import ActivityRetryPolicy, Client, Worker, activity, workflow
from durable_workflow.workflow import WorkflowContext


@workflow.defn(name="tests.python-activity-retry-identity")
class ActivityRetryIdentityWorkflow:
    def run(self, ctx: WorkflowContext) -> Any:
        return (
            yield ctx.schedule_activity(
                "tests.python-retry-identity",
                [],
                retry_policy=ActivityRetryPolicy(max_attempts=2, backoff_seconds=[1]),
            )
        )


async def test_retry_preserves_received_execution_id_and_changes_attempt_id(
    server_url: str,
    server_token: str,
) -> None:
    installed_version = os.environ.get("DURABLE_WORKFLOW_EXPECT_INSTALLED_VERSION")
    if installed_version:
        assert importlib.metadata.version("durable-workflow") == installed_version
        assert Path(durable_workflow.__file__).is_relative_to(Path(sys.prefix))
    observed: list[dict[str, Any]] = []

    @activity.defn(name="tests.python-retry-identity")
    async def inspect_identity() -> dict[str, Any]:
        info = activity.context().info
        value = {
            "execution_id": info.activity_execution_id,
            "workflow_id": info.workflow_id,
            "run_id": info.run_id,
            "task_id": info.task_id,
            "attempt_id": info.activity_attempt_id,
            "attempt_number": info.attempt_number,
        }
        observed.append(value)
        if info.attempt_number == 1:
            raise RuntimeError("retry the first identity observation")
        return value

    queue = "python-activity-identity-" + uuid4().hex
    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[ActivityRetryIdentityWorkflow],
            activities=[inspect_identity],
        )
        handle = await client.start_workflow(
            workflow_type="tests.python-activity-retry-identity",
            workflow_id=queue,
            task_queue=queue,
            input=[],
        )
        try:
            await worker.run_until(workflow_id=handle.workflow_id, timeout=45)
            result = await handle.result(timeout=10)
        finally:
            await worker.stop()

    assert len(observed) == 2
    first, second = observed
    assert [first["attempt_number"], second["attempt_number"]] == [1, 2]
    assert isinstance(first["execution_id"], str)
    assert first["execution_id"].strip()
    assert first["execution_id"] == second["execution_id"]
    for value in observed:
        for name in ("task_id", "attempt_id"):
            assert isinstance(value[name], str)
            assert value[name].strip()
    assert first["attempt_id"] != second["attempt_id"]
    assert first["task_id"] != second["task_id"]
    assert {value["workflow_id"] for value in observed} == {handle.workflow_id}
    assert {value["run_id"] for value in observed} == {handle.run_id}
    assert first["execution_id"] not in {first["attempt_id"], second["attempt_id"], first["task_id"], second["task_id"]}
    assert result == second
    print(json.dumps({"scope": "activity callback retry identity", "observed": observed}, sort_keys=True))
