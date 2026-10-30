"""Shared workflow and committed-response fault for deregistration controls."""
from __future__ import annotations

from typing import Any

import httpx

from durable_workflow import Worker, activity, workflow


@activity.defn(name="tests.deregistration.echo")
def echo(value: str) -> str:
    return value


@workflow.defn(name="tests.deregistration.cold-recovery")
class RecoveredWorkflow:
    def run(self, ctx: Any, value: str) -> Any:
        returned = yield ctx.schedule_activity("tests.deregistration.echo", [value], retry_policy={"max_attempts": 1})
        return {"value": returned, "run": ctx.run_id}


class DiscardCompletedResponse(httpx.AsyncBaseTransport):
    def __init__(self, replacement: Worker) -> None:
        self.inner = httpx.AsyncHTTPTransport()
        self.replacement = replacement
        self.discarded = False
        self.deregistration_bodies: list[bytes] = []
        self.recovered_workflow_task_count: int | None = None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.inner.handle_async_request(request)
        if request.method != "POST" or not request.url.path.endswith("/deregister"):
            return response
        self.deregistration_bodies.append(request.content)
        if self.discarded:
            return response
        # The real Server commits before only the caller's response is lost.
        await response.aread()
        assert response.status_code == 200
        receipt = response.json()
        assert receipt["outcome"] == "deregistered"
        self.recovered_workflow_task_count = receipt["recovered_workflow_task_count"]
        await response.aclose()
        await self.replacement._register()
        assert self.replacement._registration_token != receipt["registration_token"]
        self.discarded = True
        raise httpx.RemoteProtocolError("test discarded a committed response", request=request)

    async def aclose(self) -> None:
        await self.inner.aclose()
