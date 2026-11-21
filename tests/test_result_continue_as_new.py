"""result() on a handle bound to an earlier run follows continue-as-new to the run that ended."""
from __future__ import annotations

from typing import Any

import httpx
import pytest

from durable_workflow import Client, serializer
from durable_workflow.errors import WorkflowFailed


def continued(to: str | None) -> dict[str, Any]:
    payload: dict[str, Any] = {"sequence": 3, "arguments": serializer.envelope([1])}
    if to is not None:
        payload["continued_to_run_id"] = to
    return {"event_type": "WorkflowContinuedAsNew", "payload": payload}


COMPLETED = {"event_type": "WorkflowCompleted", "payload": {"output": serializer.envelope({"final": True})}}
FAILED = {"event_type": "WorkflowFailed", "payload": {"message": "final run failed", "exception_class": "E"}}
STARTED = {"event_type": "WorkflowStarted", "payload": {}}


class ChainServer:
    def __init__(self, current: str, status: str, histories: dict[str, list[dict[str, Any]]]) -> None:
        self.current, self.status, self.histories = current, status, histories
        self.paths: list[str] = []

    async def respond(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.paths.append(path)
        _, _, run_path = path.partition("/runs/")
        run = run_path.split("/")[0] or self.current
        if path.endswith("/history"):
            return httpx.Response(200, json={"events": [STARTED, *self.histories[run]], "next_page_token": None})
        status = self.status if run == self.current else "continued"
        return httpx.Response(200, json={"workflow_id": "wf", "run_id": run, "workflow_type": "t", "status": status})


async def result_of(server: ChainServer, run_id: str | None) -> Any:
    async with Client("http://chain.invalid", token="t") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://chain.invalid", transport=httpx.MockTransport(server.respond))
        return await client.get_workflow_handle("wf", run_id=run_id).result(timeout=1)


def history_reads(server: ChainServer) -> list[str]:
    return [path.split("/runs/")[1].split("/")[0] for path in server.paths if path.endswith("/history")]


@pytest.mark.parametrize("run_id", ["run-1", None])
async def test_completed_after_continue_as_new_returns_the_final_result(run_id: str | None) -> None:
    server = ChainServer("run-2", "completed", {"run-1": [continued("run-2")], "run-2": [COMPLETED]})
    assert await result_of(server, run_id) == {"final": True}
    assert history_reads(server) == (["run-1", "run-2"] if run_id else ["run-2"])


async def test_failed_after_continue_as_new_raises_the_final_failure() -> None:
    server = ChainServer("run-2", "failed", {"run-1": [continued("run-2")], "run-2": [FAILED]})
    with pytest.raises(WorkflowFailed, match="final run failed"):
        await result_of(server, "run-1")
    assert history_reads(server) == ["run-1", "run-2"]


async def test_a_longer_chain_is_followed_run_by_run() -> None:
    server = ChainServer("run-3", "completed", {
        "run-1": [continued("run-2")], "run-2": [continued("run-3")], "run-3": [COMPLETED],
    })
    assert await result_of(server, "run-1") == {"final": True}
    assert history_reads(server) == ["run-1", "run-2", "run-3"]


async def test_without_a_recorded_successor_the_newest_run_is_used() -> None:
    server = ChainServer("run-2", "completed", {"run-1": [continued(None)], "run-2": [COMPLETED]})
    assert await result_of(server, "run-1") == {"final": True}


async def test_a_cycle_is_a_failure_not_a_silent_none() -> None:
    server = ChainServer("run-2", "completed", {"run-1": [continued("run-2")], "run-2": [continued("run-1")]})
    with pytest.raises(WorkflowFailed, match="successor run could not be followed"):
        await result_of(server, "run-1")
