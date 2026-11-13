"""Installed SDK adapter for shared #216's separately reviewed loopback fixture.

Run only with a reviewed proxy file/hash and synthetic runtime credentials.
No transport fault implementation or product source overlay lives here.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import importlib.util
import io
import json
import platform
import stat
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from uuid import uuid4
from zipfile import ZipFile

import httpx

from durable_workflow import Client, Worker, workflow
from durable_workflow.errors import SignalFailed, UpdateFailed
from durable_workflow.retry_policy import TransportRetryPolicy

WORKFLOW_TYPE = "sdk-python-command-response-loss"
SDK_VERSION = "2.17.2"
WHEEL_SHA256 = "7dc77d3882fa5cbddbeaee6a4cc63b05285e280523c8ea40dc31e03cc12a40bc"
PROXY_COMMIT = "117118c83093822469b3824ba9587ff4068ed29f"
PROXY_SHA256 = "bd1e8ccbbf76f63fc2eafa278a023eb181d405b68d9c30016392e395a1eff725"
PAYLOAD = {
    "number": 1,
    "flag": True,
    "large": 2**62 + 7,
    "unicode": "Ünïcødé ☃ 日本",
    "nested": [None, False, "text", 1.25, b"\x00\xff"],
}
IDENTITIES = ("command_id", "command_sequence", "run_id", "resolved_run_id", "update_id", "accepted_at")


def explicit_key(kind):
    prefix = kind + "-" + uuid4().hex
    # This is the corrected producer's 255-code-point boundary, deliberately
    # longer than 255 UTF-8 bytes. Caller keys are opaque Unicode strings.
    return prefix + "日" * (255 - len(prefix))


def installed_identity(wheel):
    """Compare installed code to the exact published wheel, without importing project source."""
    wheel_bytes = wheel if isinstance(wheel, bytes) else wheel.read_bytes()
    if hashlib.sha256(wheel_bytes).hexdigest() != WHEEL_SHA256:
        raise ValueError("published wheel identity mismatch")
    distribution = importlib.metadata.distribution("durable-workflow")
    if distribution.version != SDK_VERSION or distribution.read_text("direct_url.json") is not None:
        raise ValueError("require a registry-only installation of the bound SDK")
    with ZipFile(io.BytesIO(wheel_bytes)) as archive:
        names = sorted(
            name for name in archive.namelist() if name.startswith("durable_workflow/") and not name.endswith("/")
        )
        for name in names:
            if distribution.locate_file(name).read_bytes() != archive.read(name):
                raise ValueError("installed SDK file differs from published wheel")
    if len(names) != 46:
        raise ValueError("incomplete published SDK inventory")
    # The imported package must also come from that installation, even when a
    # caller accidentally runs without Python's documented isolated flag.
    import durable_workflow

    if Path(durable_workflow.__file__).resolve() != distribution.locate_file("durable_workflow/__init__.py").resolve():
        raise ValueError("SDK import is outside the verified installation")
    return {
        "version": SDK_VERSION,
        "wheel_sha256": WHEEL_SHA256,
        "runtime_files_verified": len(names),
        "python": platform.python_version(),
        "dependencies": {name: importlib.metadata.version(name) for name in ("httpx", "fastavro")},
    }


@workflow.defn(name=WORKFLOW_TYPE)
class Commands:
    def __init__(self):
        self.signals = 0
        self.updates = 0
        self.done = False
        self.rotate = False
        self.generation = 0

    @workflow.signal("add")
    def add(self, value):
        assert value == PAYLOAD and type(value["number"]) is int and type(value["flag"]) is bool
        self.signals += 1

    @workflow.signal("finish")
    def finish(self):
        self.done = True

    @workflow.signal("continue")
    def continue_run(self):
        self.rotate = True

    @workflow.update("value")
    def value(self, value):
        self.updates += 1
        return {"count": self.updates, "payload": value}

    @workflow.query("state")
    def state(self):
        return {"signals": self.signals, "updates": self.updates, "generation": self.generation}

    def run(self, ctx, generation=0):
        self.generation = generation
        yield ctx.wait_condition(lambda: self.done or self.rotate, key="control", timeout=600)
        if self.rotate:
            yield ctx.continue_as_new(generation + 1)
        return self.state()


async def history(client, handle):
    return [event async for event in client.iter_history(handle.workflow_id, handle.run_id)]


async def until(client, handle, event_type):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        events = await history(client, handle)
        if any(event["event_type"] == event_type for event in events):
            return events
        await asyncio.sleep(0.1)
    raise TimeoutError(event_type)


async def state(client, workflow_id, signals, updates, generation=0):
    deadline = time.monotonic() + 60
    expected = {"signals": signals, "updates": updates, "generation": generation}
    while time.monotonic() < deadline:
        receipt = await client.query_workflow(workflow_id, "state")
        assert receipt["workflow_id"] == workflow_id and receipt["query_name"] == "state"
        assert receipt["reason"] is None
        if receipt["result"] == expected:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("business state mismatch")


async def command(client, kind, handle, key=None, payload=PAYLOAD):
    if kind == "signal":
        return await client.signal_workflow(
            handle.workflow_id, "add", args=[payload], request_id=key, run_id=handle.run_id
        )
    return await client.update_workflow(
        handle.workflow_id, "value", args=[payload], request_id=key, wait_for="accepted"
    )


def route(kind, handle):
    if kind == "signal":
        return f"/api/workflows/{handle.workflow_id}/runs/{handle.run_id}/signal/add"
    return f"/api/workflows/{handle.workflow_id}/update/value"


def retained(original, retry):
    assert retry["request_id"] == original["request_id"]
    assert retry["retained"] is True and retry["intent_verified"] is True
    for key in IDENTITIES:
        if key in original:
            assert retry.get(key) == original[key], key


def sanitized_record(record):
    return {
        key: record[key]
        for key in (
            "method",
            "path",
            "request_body_bytes",
            "request_body_sha256",
            "upstream_status",
            "upstream_complete",
            "upstream_body_bytes",
            "upstream_body_sha256",
            "dropped",
        )
    }


def loss_records(proxy, path, count):
    records = [record for record in proxy.records if record["method"] == "POST" and record["path"] == path]
    assert len(records) == count, (path, len(records), count)
    assert records[0]["dropped"] is True and 200 <= records[0]["upstream_status"] < 300
    assert records[0]["upstream_body_bytes"] > 0
    assert all(record["upstream_complete"] is True for record in records)
    assert len(records[0]["upstream_body_sha256"]) == 64
    assert all(record["dropped"] is False and 200 <= record["upstream_status"] < 300 for record in records[1:])
    return [sanitized_record(record) for record in records]


def compare_requests(records, requests):
    """Independent sender and proxy observations must agree on every request byte."""
    assert len(records) == len(requests)
    assert len({item["sha256"] for item in requests}) == 1
    for record, request in zip(records, requests, strict=True):
        assert record["request_body_sha256"] == request["sha256"]
        assert record["request_body_bytes"] == request["bytes"]


async def main(args):
    if not __debug__:
        raise ValueError("acceptance assertions require Python without optimization")
    if args.output.exists():
        raise ValueError("refusing to overwrite an existing evidence file")
    proxy_bytes = args.proxy_data
    if hashlib.sha256(proxy_bytes).hexdigest() != PROXY_SHA256:
        raise ValueError("accepted proxy source hash mismatch")
    package = installed_identity(args.wheel_data)
    module = ModuleType("shared_committed_response_loss")
    exec(compile(proxy_bytes, "captured-committed-response-loss.py", "exec"), module.__dict__)
    mode = args.credential_file.lstat().st_mode
    if not stat.S_ISREG(mode) or mode & 0o077:
        raise ValueError("fixture credential file must be owner-only")
    token = dict(line.split("=", 1) for line in args.credential_file.read_text().splitlines())["DW_AUTH_TOKEN"]
    proxy = module.CommittedResponseLoss(args.upstream)
    thread = threading.Thread(target=proxy.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    report = {
        "sdk": package,
        "proxy_commit": PROXY_COMMIT,
        "proxy_sha256": PROXY_SHA256,
        "adapter_sha256": args.adapter_sha256,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "cells": [],
        "complete": False,
    }
    wire = []

    async def observe(request):
        path = request.url.path
        if request.method == "POST" and (path.endswith("/signal/add") or path.endswith("/update/value")):
            body = request.content
            wire.append(
                {
                    "path": path,
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "bytes": len(body),
                    "request_id": json.loads(body)["request_id"],
                }
            )

    def save():
        args.output.write_text(json.dumps(report, indent=2) + "\n")

    queue = "sdk-python-loss-" + uuid4().hex
    try:
        async with Client(args.upstream, token=token) as direct:
            info = await direct.get_cluster_info()
            if info["version"] != args.server_version:
                raise ValueError("runtime version differs from the externally verified tuple")
            report["server_version"] = info["version"]
            worker = Worker(direct, task_queue=queue, workflows=[Commands], poll_timeout=0.2)
            running = asyncio.create_task(worker.run())
            try:
                for automatic, explicit, continued, kinds in (
                    (True, False, False, ("signal", "update")),
                    (True, True, True, ("signal", "update")),
                    (False, False, False, ("signal",)),
                    (False, True, False, ("signal",)),
                    (False, False, False, ("update",)),
                    (False, True, False, ("update",)),
                ):
                    handle = await direct.start_workflow(
                        workflow_type=WORKFLOW_TYPE, workflow_id=queue + "-" + uuid4().hex, task_queue=queue, input=[0]
                    )
                    await until(direct, handle, "ConditionWaitOpened")
                    originals = {}
                    cell = {
                        "automatic_retry": automatic,
                        "explicit_key": explicit,
                        "continued": continued,
                        "commands": {},
                    }
                    policy = None if automatic else TransportRetryPolicy(max_attempts=1)
                    async with Client(proxy.url, token=token, retry_policy=policy) as lossy:
                        lossy._http.event_hooks["request"].append(observe)
                        for kind in kinds:
                            path = route(kind, handle)
                            key = explicit_key(kind) if explicit else None
                            proxy.arm("POST", path)
                            diagnostic = None
                            if automatic:
                                receipt = await command(lossy, kind, handle, key)
                                assert receipt["retained"] is True and receipt["intent_verified"] is True
                            else:
                                try:
                                    await command(lossy, kind, handle, key)
                                except httpx.TransportError as error:
                                    diagnostic = type(error).__name__
                                    sent_key = getattr(error, "request_id", None)
                                    assert isinstance(sent_key, str) and sent_key
                                    assert sent_key == key if explicit else sent_key.startswith(f"sdk-python-{kind}-")
                                else:
                                    raise AssertionError("selected committed response was not lost")
                                loss_records(proxy, path, 1)
                                receipt = await command(direct, kind, handle, sent_key)
                                assert receipt["retained"] is True and receipt["intent_verified"] is True
                            expected = 2 if automatic else 1
                            records = loss_records(proxy, path, expected)
                            requests = [item for item in wire if item["path"] == path]
                            assert len(requests) == expected
                            compare_requests(records, requests)
                            assert all(item["request_id"] == receipt["request_id"] for item in requests)
                            assert (
                                receipt["request_id"] == key
                                if explicit
                                else receipt["request_id"].startswith(f"sdk-python-{kind}-")
                            )
                            assert receipt["command_id"] and type(receipt["command_sequence"]) is int
                            if kind == "update":
                                result = await direct.update_outcome(
                                    handle.workflow_id, receipt["update_id"], wait_timeout_seconds=30
                                )
                                assert result["count"] == 1 and result["payload"] == PAYLOAD
                                assert (
                                    type(result["payload"]["number"]) is int and type(result["payload"]["flag"]) is bool
                                )
                            originals[kind] = receipt
                            assert receipt.get("resolved_run_id", receipt.get("run_id")) == handle.run_id
                            # Concurrent retries must all resolve to one retained
                            # command before any close/continuation control.
                            retries = await asyncio.gather(
                                *(command(direct, kind, handle, receipt["request_id"]) for _ in range(4))
                            )
                            for retry in retries:
                                retained(receipt, retry)
                            try:
                                await command(direct, kind, handle, receipt["request_id"], {"different": True})
                            except (SignalFailed, UpdateFailed) as error:
                                assert error.reason == "request_id_conflict"
                                assert error.request_id == receipt["request_id"]
                            else:
                                raise AssertionError("changed command intent was admitted")
                            cell["commands"][kind] = {
                                "key_characters": len(receipt["request_id"]),
                                "key_utf8_bytes": len(receipt["request_id"].encode()),
                                "wire": requests,
                                "loss": records,
                                "transport_exception": diagnostic,
                                "concurrent_identical_retries": len(retries),
                                "changed_intent": "request_id_conflict",
                                "identity": {key: receipt[key] for key in IDENTITIES if key in receipt},
                            }
                    await state(direct, handle.workflow_id, int("signal" in kinds), int("update" in kinds))
                    if continued:
                        await direct.signal_workflow(handle.workflow_id, "continue", run_id=handle.run_id)
                        await until(direct, handle, "WorkflowContinuedAsNew")
                        current = await direct.describe_workflow(handle.workflow_id)
                        assert current.run_id != handle.run_id
                        await state(direct, handle.workflow_id, 0, 0, 1)
                        for kind, original in originals.items():
                            retained(original, await command(direct, kind, handle, original["request_id"]))
                        await state(direct, handle.workflow_id, 0, 0, 1)
                        # The original selected-run identity remains authoritative for recovery.
                        before = len(await history(direct, handle))
                        await direct.signal_workflow(current.workflow_id, "finish", run_id=current.run_id)
                        await until(direct, current, "WorkflowCompleted")
                    else:
                        await direct.signal_workflow(handle.workflow_id, "finish", run_id=handle.run_id)
                        await until(direct, handle, "WorkflowCompleted")
                        before = len(await history(direct, handle))
                    for kind, original in originals.items():
                        retained(original, await command(direct, kind, handle, original["request_id"]))
                    events = await history(direct, handle)
                    assert len(events) == before, "retained closed-run reads wrote history"
                    counts = Counter(event["event_type"] for event in events)
                    assert counts["SignalReceived"] == int("signal" in kinds) + 1
                    for event_type in ("UpdateAccepted", "UpdateApplied", "UpdateCompleted"):
                        assert counts[event_type] == int("update" in kinds), (event_type, counts)
                    cell["event_counts"] = dict(counts)
                    cell["business_executions"] = {"signal": int("signal" in kinds), "update": int("update" in kinds)}
                    report["cells"].append(cell)
                    save()
                    print(f"completed response-loss cell {len(report['cells'])}", flush=True)
            finally:
                await worker.stop()
                await running
        report["business_complete"] = True
    except Exception as error:
        report["failure_type"] = type(error).__name__
        raise
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=5)
        report["proxy_thread_stopped"] = not thread.is_alive()
        save()
        assert not thread.is_alive()
    report["complete"] = True
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    save()
    return report


if __name__ == "__main__":
    raise SystemExit("Use the bound run_response_loss.py controller; standalone executable paths are refused")
