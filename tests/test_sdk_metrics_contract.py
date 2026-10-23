from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Coroutine, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

import durable_workflow.client as client_module
import durable_workflow.worker as worker_module
from durable_workflow.client import Client
from durable_workflow.errors import ServerError
from durable_workflow.metrics import NOOP_METRICS, PrometheusMetrics
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import Worker

CONTRACT_DIR = Path(__file__).parent / "fixtures/sdk_metrics"
CONTRACT = json.loads((CONTRACT_DIR / "contract.json").read_text())
SPECS = {metric["name"]: metric for metric in CONTRACT["metrics"]}


@dataclass
class Recorder:
    calls: list[tuple[str, str, float, dict[str, str]]] = field(default_factory=list)

    def increment(self, name: str, value: float = 1.0, tags: Mapping[str, str] | None = None) -> None:
        self.calls.append(("increment", name, value, dict(tags or {})))

    def record(self, name: str, value: float, tags: Mapping[str, str] | None = None) -> None:
        self.calls.append(("record", name, value, dict(tags or {})))


def assert_contract(recorder: Recorder, *, duration: float | None = None) -> set[str]:
    assert recorder.calls and len(recorder.calls) % 2 == 0
    scopes: set[str] = set()
    for counter, histogram in zip(recorder.calls[::2], recorder.calls[1::2], strict=True):
        for method, name, value, tags in (counter, histogram):
            spec = SPECS[name]
            assert method == spec["recorder_method"]
            assert set(tags) == set(spec["tags"])
            assert all(isinstance(value, str) for value in tags.values())
            assert math.isfinite(value) and value >= 0
        counter_spec, duration_spec = SPECS[counter[1]], SPECS[histogram[1]]
        assert counter_spec["type"] == "counter" and counter_spec["unit"] == "1"
        assert duration_spec["type"] == "histogram" and duration_spec["unit"] == "s"
        assert counter_spec["scope"] == duration_spec["scope"]
        assert counter[2] == 1
        assert counter[3] == histogram[3]
        if duration is not None:
            assert histogram[2] == duration
        scopes.add(counter_spec["scope"])
    return scopes


def test_contract_is_an_exact_pinned_copy() -> None:
    provenance = json.loads((CONTRACT_DIR / "provenance.json").read_text())
    assert hashlib.sha256((CONTRACT_DIR / "contract.json").read_bytes()).hexdigest() == provenance["sha256"]
    assert provenance["source"].startswith("https://github.com/durable-workflow/.github/blob/")
    assert re.fullmatch(r"[0-9a-f]{40}", provenance["source"].split("/blob/", 1)[1].split("/", 1)[0])
    assert CONTRACT["schema"] == "durable-workflow.sdk-metrics/v1"


@pytest.mark.parametrize("outcome", ["ok", "http_error", "ConnectError", "retry"])
async def test_actual_client_request_pair_matches_the_contract(
    outcome: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    now = 1.0
    attempts = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal now, attempts
        attempts += 1
        now += 0.125
        if outcome == "ConnectError":
            raise httpx.ConnectError("runtime unavailable", request=request)
        return httpx.Response(503 if outcome == "http_error" or outcome == "retry" and attempts == 1 else 200, json={})

    client = Client("https://runtime.example", token="synthetic-token", metrics=recorder,
                    retry_policy=TransportRetryPolicy(max_attempts=2, initial_backoff_seconds=0, jitter=False))
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(respond))
    monkeypatch.setattr(client_module, "time", SimpleNamespace(perf_counter=lambda: now, monotonic=time.monotonic))
    async with client:
        if outcome == "http_error":
            with pytest.raises(ServerError):
                await client.health()
        elif outcome == "ConnectError":
            with pytest.raises(httpx.ConnectError):
                await client.health()
        else:
            await client.health()
    assert_contract(recorder, duration=attempts * 0.125)
    assert len(recorder.calls) == 2  # One logical request, including all HTTP retries.
    assert attempts == (1 if outcome == "ok" else 2)
    assert recorder.calls[0][3]["outcome"] == ("ok" if outcome == "retry" else outcome)


@pytest.mark.parametrize("kind", ["workflow", "activity", "query"])
@pytest.mark.parametrize("outcome", ["empty", "task", "error", "backpressure"])
async def test_actual_worker_poll_pair_matches_the_contract(
    kind: str, outcome: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="orders", metrics=recorder)
    now = 1.0

    async def poll(**_: object) -> dict[str, str] | None:
        nonlocal now
        now = 1.25
        if outcome == "error":
            raise httpx.ReadError()
        if outcome == "backpressure":
            raise ServerError(429, {
                "task": None, "reason": "long_poll_capacity_exhausted",
                "poll_status": "long_poll_capacity_exhausted", "task_kind": f"{kind}_task",
                "task_queue": "orders", "retryable": True, "retry_after_seconds": 2,
            })
        if outcome == "task":
            return {"task_id": "synthetic-task"}
        worker._stop.set()
        return None

    async def sleep(_: float) -> None:
        worker._stop.set()

    def admit_workflow(*_: object) -> None:
        worker._release_workflow_capacity()
        worker._stop.set()

    def track(coro: Coroutine[Any, Any, Any]) -> None:
        coro.close()
        if kind == "activity":
            worker._act_semaphore.release()
        worker._stop.set()

    getattr(client, f"poll_{kind}_task").side_effect = poll
    monkeypatch.setattr(worker_module, "time", SimpleNamespace(perf_counter=lambda: now, monotonic=time.monotonic))
    monkeypatch.setattr(worker_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(worker, "_admit_workflow_work", admit_workflow)
    monkeypatch.setattr(worker, "_track", track)
    await getattr(worker, f"_poll_{kind}_tasks")()
    assert assert_contract(recorder, duration=0.25) == {"worker_poll"}
    assert len(recorder.calls) == 2
    assert recorder.calls[0][3] == {"task_kind": kind, "task_queue": "orders", "outcome": outcome}


@pytest.mark.parametrize("kind", ["workflow", "activity", "query", "update_validation"])
@pytest.mark.parametrize("failed", [False, True], ids=["success", "failure"])
async def test_actual_task_dispatch_pair_matches_the_contract(
    kind: str, failed: bool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = Recorder()
    worker = Worker(AsyncMock(spec=Client), task_queue="orders", metrics=recorder)
    now = 1.0
    expected = "failed" if failed else "approved" if kind == "update_validation" else "completed"

    async def execute(*_: object, **__: object) -> object:
        nonlocal now
        now = 1.25
        if kind == "workflow":
            return None if failed else []
        return expected

    monkeypatch.setattr(worker_module, "time", SimpleNamespace(perf_counter=lambda: now, monotonic=time.monotonic))
    monkeypatch.setattr(worker, f"_run_{kind}_task", execute)
    if kind == "activity":
        await worker._act_semaphore.acquire()
    await getattr(worker, f"_dispatch_{kind}_task")({"task_id": "synthetic-task"})
    assert assert_contract(recorder, duration=0.25) == {"worker_task"}
    assert recorder.calls[0][3] == {"task_kind": kind, "task_queue": "orders", "outcome": expected}


@pytest.mark.parametrize("override", [False, True], ids=["inherit", "override"])
async def test_actual_client_and_worker_respect_recorder_selection(override: bool) -> None:
    client_recorder, worker_recorder = Recorder(), Recorder()
    client = Client("https://runtime.example", worker_token="synthetic-token", metrics=client_recorder)
    worker = Worker(client, task_queue="orders", metrics=worker_recorder if override else None)

    def respond(_: httpx.Request) -> httpx.Response:
        worker._stop.set()
        return httpx.Response(200, json={"task": None})

    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(respond))
    client._runtime_external_payload_transport_resolved = True
    async with client:
        await worker._poll_workflow_tasks()
    if override:
        assert assert_contract(client_recorder) == {"client_request"}
        assert assert_contract(worker_recorder) == {"worker_poll"}
    else:
        assert assert_contract(client_recorder) == {"client_request", "worker_poll"}
        assert not worker_recorder.calls


async def test_default_noop_handles_real_client_and_worker_observations() -> None:
    client = Client("https://runtime.example", worker_token="synthetic-token")
    worker = Worker(client, task_queue="orders")

    def respond(_: httpx.Request) -> httpx.Response:
        worker._stop.set()
        return httpx.Response(200, json={"task": None})

    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(respond))
    client._runtime_external_payload_transport_resolved = True
    async with client:
        await worker._poll_workflow_tasks()
    assert client.metrics is NOOP_METRICS and worker.metrics is NOOP_METRICS


def test_prometheus_adapter_exports_counter_and_seconds_samples() -> None:
    prometheus = pytest.importorskip("prometheus_client")
    registry = prometheus.CollectorRegistry()
    recorder = PrometheusMetrics(registry=registry)
    for spec in CONTRACT["metrics"]:
        tags = {name: "bounded" for name in spec["tags"]}
        if spec["type"] == "counter":
            recorder.increment(spec["name"], tags=tags)
            assert registry.get_sample_value(spec["name"] + "_total", tags) == 1
        else:
            recorder.record(spec["name"], 0.25, tags)
            assert registry.get_sample_value(spec["name"] + "_sum", tags) == 0.25
            assert registry.get_sample_value(spec["name"] + "_count", tags) == 1
