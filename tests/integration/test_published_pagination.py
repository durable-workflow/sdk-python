"""Read real native pages through the unchanged published iterator APIs."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import os
from dataclasses import asdict
from typing import Any
from uuid import uuid4

import pytest

from durable_workflow import Client


class ObservedClient(Client):
    """Retain request/page evidence without replacing any Server response."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.pages: list[dict[str, Any]] = []

    async def list_workflows(self, **kwargs: Any) -> Any:
        page = await super().list_workflows(**kwargs)
        self.pages.append({
            "namespace": self.namespace, "request": kwargs,
            "items": [asdict(item) for item in page.executions], "next": page.next_page_token,
        })
        return page

    async def get_history(self, workflow_id: str, run_id: str, **kwargs: Any) -> Any:
        page = await super().get_history(workflow_id, run_id, **kwargs)
        assert page["workflow_id"] == workflow_id
        assert page["run_id"] == run_id
        self.pages.append({
            "namespace": self.namespace,
            "request": {"workflow_id": workflow_id, "run_id": run_id, **kwargs},
            "items": page["events"], "next": page.get("next_page_token"),
        })
        return page


def verify_pages(
    pages: list[dict[str, Any]], expected: list[Any], request: dict[str, Any],
    *, initial_token: str | None = None,
) -> None:
    """Check every request, opaque continuation, boundary and returned item."""
    assert pages
    token = initial_token
    flattened = []
    seen = set()
    for index, page in enumerate(pages):
        assert page["namespace"] == "default"
        assert page["request"] == {**request, "next_page_token": token}
        assert 0 < len(page["items"]) <= request["page_size"]
        flattened.extend(page["items"])
        token = page["next"]
        if index < len(pages) - 1:
            assert isinstance(token, str)
            assert token
            assert token not in seen
            seen.add(token)
        else:
            assert token is None
    assert flattened == expected


async def check_controls(client: ObservedClient, factory: Any, expected: list[Any], page_size: int) -> dict[str, Any]:
    client.pages.clear()
    iterator = factory()
    assert client.pages == []
    await anext(iterator)
    assert len(client.pages) == 1
    token = client.pages[0]["next"]
    assert isinstance(token, str)
    assert token
    await iterator.aclose()
    assert len(client.pages) == 1

    client.pages.clear()
    resumed = [item async for item in factory(next_page_token=token)]
    normalized = [asdict(item) if hasattr(item, "workflow_id") else item for item in resumed]
    assert normalized == expected[page_size:]
    resumed_pages = list(client.pages)

    client.pages.clear()
    iterator = factory()
    for _ in range(page_size):
        await anext(iterator)
    assert len(client.pages) == 1
    client.namespace = "must-not-be-requested"
    try:
        with pytest.raises(RuntimeError, match="namespace changed"):
            await anext(iterator)
    finally:
        client.namespace = "default"
        await iterator.aclose()
    assert len(client.pages) == 1
    return {"early_close_requests": 1, "namespace_refusal_requests": 1, "resume_pages": resumed_pages}


async def pagination_case(server_url: str, server_token: str) -> dict[str, Any]:
    prefix = "python-pagination-" + uuid4().hex
    kind = "tests.pagination." + uuid4().hex
    options = {"token": server_token, "namespace": "default"}
    async with Client(server_url, **options) as seed:
        targets = []
        for index in range(5):
            handle = await seed.start_workflow(
                workflow_type=kind, task_queue=prefix, workflow_id=f"{prefix}-{index}", input=[],
            )
            assert isinstance(handle.run_id, str)
            assert handle.run_id
            targets.append((handle.workflow_id, handle.run_id))
            await seed.cancel_workflow(handle.workflow_id, reason="finite pagination fixture")
        negatives = []
        for suffix, workflow_type, workflow_id, cancel in (
            ("other-type", kind + ".other", prefix + "-other-type", True),
            ("running", kind, prefix + "-running", False),
            ("other-query", kind, "outside-" + uuid4().hex, True),
        ):
            handle = await seed.start_workflow(
                workflow_type=workflow_type, task_queue=prefix, workflow_id=workflow_id, input=[],
            )
            if cancel:
                await seed.cancel_workflow(handle.workflow_id, reason="pagination filter control")
            negatives.append({"control": suffix, "workflow_id": handle.workflow_id, "run_id": handle.run_id})

        filters = {"workflow_type": kind, "status": "failed", "query": prefix}
        baseline = await seed.list_workflows(**filters, page_size=200)
        assert baseline.next_page_token is None
        expected_list = [asdict(item) for item in baseline.executions]
        assert len(expected_list) == 5
        assert {(item["workflow_id"], item["run_id"]) for item in expected_list} == set(targets)
        assert all(item["workflow_type"] == kind and item["status"] == "cancelled" for item in expected_list)
        workflow_id, run_id = targets[0]
        history = await seed.get_history(workflow_id, run_id, page_size=200)
        assert history.get("next_page_token") is None
        expected_history = history["events"]
        assert len(expected_history) >= 3
        assert {"StartAccepted", "WorkflowStarted", "WorkflowCancelled"} <= {
            event["event_type"] for event in expected_history
        }
        assert all(isinstance(event["timestamp"], str) for event in expected_history)

        async with ObservedClient(server_url, **options) as observed:
            actual_list = [asdict(item) async for item in observed.iter_workflows(**filters, page_size=2)]
            list_pages = list(observed.pages)
            list_request = {**filters, "page_size": 2}
            verify_pages(list_pages, expected_list, list_request)
            assert actual_list == expected_list
            assert [len(page["items"]) for page in list_pages] == [2, 2, 1]
            list_controls = await check_controls(
                observed, lambda **kw: observed.iter_workflows(**filters, page_size=2, **kw), expected_list, 2,
            )
            verify_pages(list_controls["resume_pages"], expected_list[2:], list_request,
                         initial_token=list_pages[0]["next"])

            observed.pages.clear()
            handle = observed.get_workflow_handle(workflow_id, run_id=run_id)
            actual_history = [event async for event in handle.iter_history(page_size=1)]
            history_pages = list(observed.pages)
            history_request = {"workflow_id": workflow_id, "run_id": run_id, "page_size": 1}
            verify_pages(history_pages, expected_history, history_request)
            assert actual_history == expected_history
            assert len(history_pages) == len(expected_history)
            assert len({event["sequence"] for event in actual_history}) == len(expected_history)
            history_controls = await check_controls(
                observed, lambda **kw: observed.iter_history(workflow_id, run_id, page_size=1, **kw),
                expected_history, 1,
            )
            verify_pages(history_controls["resume_pages"], expected_history[1:], history_request,
                         initial_token=history_pages[0]["next"])

        # The cancelled runs cannot change while the independent traversals run.
        assert (await seed.list_workflows(**filters, page_size=200)) == baseline
        assert (await seed.get_history(workflow_id, run_id, page_size=200)) == history
        await seed.cancel_workflow(negatives[1]["workflow_id"], reason="pagination fixture cleanup")
    return {
        "namespace": "default", "targets": targets, "negative_filters": negatives,
        "list_pages": list_pages, "history_pages": history_pages,
        "list_controls": list_controls, "history_controls": history_controls,
    }


async def test_published_iterators_cross_real_page_boundaries(server_url: str, server_token: str) -> None:
    if os.environ.get("DURABLE_WORKFLOW_PAGINATION_CELL") != "1":
        pytest.skip("select the reviewed published pagination cell")
    assert importlib.metadata.version("durable-workflow") == os.environ["DURABLE_WORKFLOW_EXPECT_INSTALLED_VERSION"]
    result = await asyncio.wait_for(pagination_case(server_url, server_token), timeout=60)
    print(json.dumps({"scope": "published pagination", **result}, sort_keys=True))
