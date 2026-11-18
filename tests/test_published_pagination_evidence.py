from __future__ import annotations

import importlib.util
import json
from copy import deepcopy
from pathlib import Path

import httpx
import pytest

PATH = Path(__file__).parent / "integration/test_published_pagination.py"
SPEC = importlib.util.spec_from_file_location("pagination_case", PATH)
assert SPEC is not None
assert SPEC.loader is not None
case = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(case)


def evidence():
    request = {"workflow_id": "fixed-workflow", "run_id": "fixed-run", "page_size": 1}
    events = [{"sequence": n, "timestamp": "2026-11-14T12:34:56.123456Z"} for n in range(3)]
    pages = [
        {"namespace": "default", "request": {**request, "next_page_token": previous},
         "items": [event], "next": following}
        for event, previous, following in zip(
            events, [None, "opaque+/=", "tail"], ["opaque+/=", "tail", None], strict=True,
        )
    ]
    return request, events, pages


def test_real_page_evidence_accepts_complete_ordered_traversal_and_resume():
    request, events, pages = evidence()
    case.verify_pages(pages, events, request)
    case.verify_pages(pages[1:], events[1:], request, initial_token="opaque+/=")


@pytest.mark.parametrize("mutation", ["namespace", "run", "cursor", "omission", "duplicate", "open-tail", "order"])
def test_incomplete_or_substituted_history_cannot_pass_evidence(mutation):
    request, events, pages = evidence()
    pages = deepcopy(pages)
    if mutation == "namespace":
        pages[1]["namespace"] = "other"
    elif mutation == "run":
        pages[1]["request"]["run_id"] = "other"
    elif mutation == "cursor":
        pages[1]["request"]["next_page_token"] = "decoded-or-replaced"
    elif mutation == "omission":
        pages[1]["items"] = []
    elif mutation == "duplicate":
        pages[1]["items"] = pages[0]["items"]
    elif mutation == "open-tail":
        pages[-1]["next"] = "unread"
    else:
        pages[1]["items"], pages[2]["items"] = pages[2]["items"], pages[1]["items"]
    with pytest.raises(AssertionError):
        case.verify_pages(pages, events, request)


@pytest.mark.parametrize("field", ["workflow_type", "status", "query", "page_size"])
def test_filtered_list_evidence_refuses_changed_filter(field):
    request = {"workflow_type": "fixed-type", "status": "failed", "query": "fixed-prefix", "page_size": 2}
    pages = [{"namespace": "default", "request": {**request, "next_page_token": None}, "items": [1], "next": None}]
    pages[0]["request"][field] = "changed"
    with pytest.raises(AssertionError):
        case.verify_pages(pages, [1], request)


@pytest.mark.parametrize("kind", ["workflows", "history"])
async def test_case_observer_preserves_public_responses_and_namespace_before_next_request(kind):
    requests = []

    def respond(request):
        requests.append(request)
        index = 0 if request.url.params.get("next_page_token") is None else 1
        token = "opaque+/=" if index == 0 else None
        if kind == "workflows":
            value = {"workflows": [{"workflow_id": str(index), "run_id": "run", "workflow_type": "type"}],
                     "next_page_token": token}
        else:
            value = {"workflow_id": "wf", "run_id": "run", "events": [{"sequence": index}], "next_page_token": token}
        return httpx.Response(200, json=value)

    client = case.ObservedClient("https://runtime.example", control_token="synthetic-control", namespace="default")
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(respond))
    async with client:
        factory = (lambda **kw: client.iter_workflows(page_size=1, **kw)) if kind == "workflows" else (
            lambda **kw: client.iter_history("wf", "run", page_size=1, **kw)
        )
        expected = [item async for item in factory()]
        if kind == "workflows":
            expected = [case.asdict(item) for item in expected]
        controls = await case.check_controls(client, factory, expected, 1)
        assert controls["resume_pages"][0]["request"]["next_page_token"] == "opaque+/="
    assert all(request.headers["X-Namespace"] == "default" for request in requests)


@pytest.mark.parametrize("field", ["workflow_id", "run_id"])
async def test_history_observer_refuses_foreign_response_identity(field):
    response = {"workflow_id": "wf", "run_id": "run", "events": []}
    response[field] = "foreign"
    client = case.ObservedClient("https://runtime.example", control_token="synthetic-control", namespace="default")
    client._http = httpx.AsyncClient(
        base_url=client.base_url, transport=httpx.MockTransport(lambda _: httpx.Response(200, json=response)),
    )
    async with client:
        with pytest.raises(AssertionError):
            await client.get_history("wf", "run", page_size=1)
    assert client.pages == []


async def test_complete_fixture_uses_timestamp_sequence_public_shape_without_worker(monkeypatch):
    runs = {}
    requests = []

    def respond(request):
        requests.append(request)
        path = request.url.path
        if path == "/api/cluster/info":
            return httpx.Response(200, json={"namespace": {"name": "default", "external_payload_storage": None}})
        if request.method == "POST" and path == "/api/workflows":
            body = json.loads(request.content)
            item = {key: body[key] for key in ("workflow_id", "workflow_type", "task_queue")}
            item.update(run_id="run-" + str(len(runs)), status="pending")
            runs[item["workflow_id"]] = item
            return httpx.Response(202, json=item)
        if request.method == "POST" and path.endswith("/cancel"):
            runs[path.split("/")[3]]["status"] = "cancelled"
            return httpx.Response(200, json={})
        size = int(request.url.params["page_size"])
        token = request.url.params.get("next_page_token")
        start = 0 if token is None else {"opaque+/=1": 1, "opaque+/=2": 2, "opaque+/=4": 4}[token]
        if path == "/api/workflows":
            items = [item for item in reversed(list(runs.values())) if (
                item["workflow_type"] == request.url.params["workflow_type"]
                and item["status"] == "cancelled"
                and request.url.params["query"] in item["workflow_id"]
            )]
            result = {"workflows": items[start:start + size]}
        else:
            parts = path.split("/")
            assert parts[4] == "runs"
            assert parts[6] == "history"
            assert runs[parts[3]]["run_id"] == parts[5]
            items = [{"sequence": index, "event_type": kind, "timestamp": "2026-11-14T12:34:56.123456Z",
                      "principal": None, "payload": {}} for index, kind in enumerate(
                          ["StartAccepted", "WorkflowStarted", "WorkflowCancelled"], start=1,
                      )]
            result = {"workflow_id": parts[3], "run_id": parts[5], "events": items[start:start + size]}
        following = start + size
        result["next_page_token"] = "opaque+/=" + str(following) if following < len(items) else None
        return httpx.Response(200, json=result)

    original = case.Client.__init__

    def initialize(self, *args, **kwargs):
        original(self, *args, **kwargs)
        self._http = httpx.AsyncClient(base_url=self.base_url, transport=httpx.MockTransport(respond))

    monkeypatch.setattr(case.Client, "__init__", initialize)
    result = await case.pagination_case("https://runtime.example", "synthetic-control")
    assert [len(page["items"]) for page in result["list_pages"]] == [2, 2, 1]
    assert len(result["history_pages"]) == 3
    assert len(runs) == 8
    assert all(item["status"] == "cancelled" for item in runs.values())
    assert all(request.headers["X-Namespace"] == "default" for request in requests)
    assert not any("worker" in request.url.path for request in requests)
