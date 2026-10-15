"""Actual query authority with a locally configured, isolated Server.

Run with DW_QUERY_TASK_TIMEOUT=4 on Server and
DURABLE_WORKFLOW_QUERY_AUTHORITY_QUALIFICATION=1 on pytest. The token must be
allowed to create disposable namespaces. Payload delay is injected after real
HTTP response headers; responses, claims and bytes are not rewritten.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import uuid
from datetime import datetime
from typing import Any

import httpx
import pytest

from durable_workflow import Client, Worker, workflow
from durable_workflow.errors import QueryFailed
from durable_workflow.external_storage import ExternalPayloadCache

pytestmark = pytest.mark.skipif(
    os.environ.get("DURABLE_WORKFLOW_QUERY_AUTHORITY_QUALIFICATION") != "1",
    reason="requires an isolated query-authority Server with a four-second original query budget",
)


@workflow.defn(name="tests.query-authority.http")
class QueryWorkflow:
    calls = 0
    cancelled = 0
    returned = 0

    def run(self, ctx: Any, value: Any) -> Any:
        self.value = value
        yield ctx.sleep(60)
        return 7

    @workflow.query("value")
    async def current(self, slow: bool = False) -> Any:
        type(self).calls += 1
        assert self.value == {"body": "x" * 65536}
        try:
            if slow:
                await asyncio.sleep(60)
            type(self).returned += 1
            return {"long": 9223372036854775807, "nil": None, "text": "λ"}
        except asyncio.CancelledError:
            type(self).cancelled += 1
            raise


class ObservedWorker(Worker):
    """Keep normal Worker.run/client cloning and observe its real HTTP path."""

    def __init__(self, *args: Any, delay_payload: bool, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.delay_payload = delay_payload
        self.claims: list[dict[str, Any]] = []
        self.status_reads = 0
        self.payload_reads = 0
        self.completions = 0
        self.payload_cancelled = threading.Event()

    def _clone_client_for_query_tasks(self) -> Client:
        client = super()._clone_client_for_query_tasks()

        async def observe(response: httpx.Response) -> None:
            path = response.request.url.path
            if path.endswith("/query-tasks/poll") and response.status_code == 200:
                await response.aread()
                task = response.json().get("task")
                if isinstance(task, dict):
                    admitted = datetime.fromisoformat(task["query_admitted_at"].replace("Z", "+00:00"))
                    deadline = datetime.fromisoformat(task["query_deadline_at"].replace("Z", "+00:00"))
                    assert (deadline - admitted).total_seconds() == 4, "configure the original Server budget"
                    self.claims.append(task)
            elif "/query-tasks/" in path and path.endswith("/status"):
                self.status_reads += 1
            elif "/query-tasks/" in path and path.endswith("/complete"):
                self.completions += 1
            elif response.request.method == "GET" and "/external-payloads/v1/" in path:
                self.payload_reads += 1
                if self.delay_payload:
                    try:
                        await asyncio.sleep(60)
                    except asyncio.CancelledError:
                        self.payload_cancelled.set()
                        raise

        client._http.event_hooks["response"].append(observe)
        return client


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["timely", "loading-expiry", "handler-expiry"])
async def test_installed_query_authority_http(server_url: str, server_token: str, mode: str) -> None:
    suffix = uuid.uuid4().hex[:12]
    namespace = f"query-authority-{suffix}"
    QueryWorkflow.calls = QueryWorkflow.cancelled = QueryWorkflow.returned = 0
    async with Client(server_url, token=server_token) as admin:
        await admin.create_namespace(namespace)
        try:
            await admin.set_namespace_external_storage(namespace, driver="local", threshold_bytes=1024)
            async with Client(server_url, token=server_token, namespace=namespace,
                              external_storage_cache=ExternalPayloadCache(max_bytes=1)) as client:
                info = await client.get_cluster_info()
                assert info["worker_protocol"]["server_capabilities"]["query_task_authority"] is True
                assert info["namespace"]["external_payload_storage"]["status"] == "available"
                worker = ObservedWorker(client, workflows=[QueryWorkflow], task_queue=namespace,
                    poll_timeout=0.5, shutdown_timeout=10, require_query_task_authority=True,
                    delay_payload=mode == "loading-expiry")
                runner = asyncio.create_task(worker.run())
                handle = None
                try:
                    await asyncio.wait_for(worker._registration_done.wait(), timeout=10)
                    assert not runner.done(), runner.exception() if runner.done() else ""
                    handle = await client.start_workflow(workflow_type="tests.query-authority.http",
                        task_queue=namespace, workflow_id=namespace, input=[{"body": "x" * 65536}])
                    for _ in range(100):
                        if (await handle.describe()).status.lower() == "waiting":
                            break
                        await asyncio.sleep(0.05)
                    else:
                        raise AssertionError("original workflow did not reach waiting")
                    before = (await handle.get_history())["events"]
                    if mode == "timely":
                        reply = await client.query_workflow(handle.workflow_id, "value", args=[False])
                        assert reply["result"] == {"long": 9223372036854775807, "nil": None, "text": "λ"}
                        assert type(reply["result"]["long"]) is int
                    else:
                        with pytest.raises(QueryFailed) as refused:
                            await client.query_workflow(handle.workflow_id, "value", args=[mode == "handler-expiry"])
                        assert refused.value.status == 504
                        assert refused.value.reason == "query_worker_execution_timeout"
                        for _ in range(100):
                            if worker.payload_cancelled.is_set() or QueryWorkflow.cancelled:
                                break
                            await asyncio.sleep(0.05)
                    assert (await handle.get_history())["events"] == before
                    assert len({task["query_task_id"] for task in worker.claims}) == 1
                    assert worker.status_reads > 0
                    assert worker.payload_reads == 1
                    assert worker.completions == (1 if mode == "timely" else 0)
                    assert QueryWorkflow.calls == (0 if mode == "loading-expiry" else 1)
                    assert QueryWorkflow.returned == (1 if mode == "timely" else 0)
                    assert QueryWorkflow.cancelled == (1 if mode == "handler-expiry" else 0)
                    assert worker.payload_cancelled.is_set() is (mode == "loading-expiry")
                    print(json.dumps({"outcome": "pass", "mode": mode, "server": info.get("version"),
                        "workflow_id": handle.workflow_id, "run_id": handle.run_id,
                        "query_task_id": worker.claims[0]["query_task_id"], "status_reads": worker.status_reads,
                        "payload_reads": worker.payload_reads, "completions": worker.completions}), flush=True)
                finally:
                    await worker.stop()
                    await asyncio.wait_for(runner, timeout=10)
                    if handle is not None:
                        await client.terminate_workflow(handle.workflow_id, reason="qualification complete")
        finally:
            await admin.delete_namespace(namespace)
