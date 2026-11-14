from __future__ import annotations

import importlib.util
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
