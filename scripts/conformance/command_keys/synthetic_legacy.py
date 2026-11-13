"""Installed Python API adapter for the accepted synthetic SQLite legacy cases."""

from __future__ import annotations

import asyncio
import hashlib
import json
import stat
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit
from uuid import uuid4

from durable_workflow import Client, Worker, workflow
from durable_workflow.errors import ServerError, SignalFailed, UpdateFailed

MANIFEST_SHA256 = "da1990000c611d7377690c7526487d6415fa4f9cb42d299f59585a7c2eebf769"
WORKFLOW_TYPE = "sdk-python-synthetic-legacy"
SCENARIOS = {"readback", "two-rows", "legacy-token", "modern-raw", "case-only"}
IDENTITIES = ("command_id", "run_id", "update_id", "accepted_at")


@workflow.defn(name=WORKFLOW_TYPE)
class LegacyCommands:
    def __init__(self):
        self.finish = False
        self.rotate = False

    @workflow.signal("touch")
    def touch(self, value):
        assert value == 1

    @workflow.update("mutate")
    def mutate(self, value):
        assert value == 1
        return None

    @workflow.signal("close")
    def close(self, rotate):
        self.rotate = rotate
        self.finish = True

    def run(self, ctx, generation=0):
        yield ctx.wait_condition(lambda: self.finish, key="control", timeout=600)
        if self.rotate:
            yield ctx.continue_as_new(generation + 1)
        return None


def packet(data):
    value = json.loads(data)
    if (
        value.get("schema") != "sdk-python.synthetic-legacy-input.v1"
        or value.get("manifest_sha256") != MANIFEST_SHA256
        or value.get("phase") not in {"prepare", "read", "case_distinct"}
        or value.get("scenario") not in SCENARIOS
        or value.get("state") not in {"open", "completed", "continued"}
        or (value["scenario"] != "readback" and value["state"] != "open")
    ):
        raise ValueError("require a declared synthetic legacy cell")
    if value["phase"] == "case_distinct" and value["scenario"] != "case-only":
        raise ValueError("distinct-key admissions require the separate case-only interval")
    if value["phase"] != "prepare":
        seed = value.get("seed", {})
        if set(seed.get("commands", {})) != {"signal", "update"}:
            raise ValueError("require both original API receipts")
        for kind, command in seed["commands"].items():
            original = command["original"]
            fields = IDENTITIES if kind == "update" else IDENTITIES[:2]
            if any(not isinstance(original.get(key), str) or not original[key] for key in fields):
                raise ValueError("original durable receipt identity is missing")
            if original["run_id"] != seed["run_id"] or original["workflow_id"] != seed["workflow_id"]:
                raise ValueError("original receipt target differs")
    return value


def selection(kind, workflow_id, original, caller_key, legacy_key):
    return {
        "command_id": original["command_id"],
        "command_type": kind,
        "workflow_instance_id": workflow_id,
        "workflow_run_id": original["run_id"],
        "caller_key": caller_key,
        "legacy_request_id": legacy_key,
    }


def retained(original, received, kind):
    if received.get("retained") is not True or received.get("intent_verified") is not False:
        raise ValueError("legacy receipt must be retained and explicitly unverified")
    for field in IDENTITIES if kind == "update" else IDENTITIES[:2]:
        if received.get(field) != original[field]:
            raise ValueError("legacy durable identity differs: " + field)


async def command(client, kind, seed, key, value=999, *, name=None, run_id=None):
    if kind == "signal":
        return await client.signal_workflow(
            seed["workflow_id"], name or "touch", args=[value], request_id=key, run_id=run_id or seed["run_id"]
        )
    return await client.update_workflow(
        seed["workflow_id"], name or "mutate", args=[value], request_id=key, wait_for="accepted"
    )


async def prepare(client, value):
    queue = "synthetic-legacy-" + uuid4().hex
    worker = Worker(client, task_queue=queue, workflows=[LegacyCommands], poll_timeout=0.2)
    task = asyncio.create_task(worker.run())
    try:
        handle = await client.start_workflow(
            workflow_type=WORKFLOW_TYPE, input=[0], workflow_id=queue, task_queue=queue
        )
        # The producer admits updates only after the first real workflow task
        # binds its contract. Stop before command admission so open updates stay pending.
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            events = [event async for event in client.iter_history(queue, handle.run_id)]
            if any(event["event_type"] == "ConditionWaitOpened" for event in events):
                break
            await asyncio.sleep(0.1)
        else:
            raise TimeoutError("first workflow task did not bind the run contract")
    finally:
        await worker.stop()
        await task
    seed = {"workflow_id": handle.workflow_id, "run_id": handle.run_id, "commands": {}, "selections": []}
    caller = "CaseSensitive" if value["scenario"] == "case-only" else "token-key"
    for kind in ("signal", "update"):
        original = await command(client, kind, seed, caller, 1)
        seed["commands"][kind] = {"original": original, "caller_key": caller}
        chosen = original
        chosen_key = caller
        legacy_key = caller
        if value["scenario"] == "two-rows":
            chosen_key = "second-key"
            chosen = await command(client, kind, seed, chosen_key, 1)
        elif value["scenario"] == "legacy-token":
            legacy_key = kind + "-v1:" + hashlib.sha256(caller.encode()).hexdigest()
        if value["scenario"] != "modern-raw":
            seed["selections"].append(selection(kind, seed["workflow_id"], chosen, chosen_key, legacy_key))
    if value["state"] != "open":
        worker = Worker(client, task_queue=queue, workflows=[LegacyCommands], poll_timeout=0.2)
        task = asyncio.create_task(worker.run())
        try:
            update = seed["commands"]["update"]["original"]
            assert await client.update_outcome(queue, update["update_id"], wait_timeout_seconds=30) is None
            await client.signal_workflow(queue, "close", args=[value["state"] == "continued"], run_id=handle.run_id)
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                description = await client.describe_workflow(queue)
                if value["state"] == "completed" and description.status == "completed":
                    break
                if value["state"] == "continued" and description.run_id != handle.run_id:
                    seed["successor_run_id"] = description.run_id
                    break
                await asyncio.sleep(0.1)
            else:
                raise TimeoutError("original workflow did not reach the declared state")
        finally:
            await worker.stop()
            await task
    seed["history"] = [event async for event in client.iter_history(queue, handle.run_id)]
    return seed


async def read(client, value):
    seed = value["seed"]
    observed = [event async for event in client.iter_history(seed["workflow_id"], seed["run_id"])]
    assert observed == seed["history"]
    cells = []
    for kind, original in seed["commands"].items():
        key = original["caller_key"]
        receipt = original["original"]
        cell = {"kind": kind, "original_command_id": receipt["command_id"], "original_run_id": receipt["run_id"]}
        if value["scenario"] in {"two-rows", "legacy-token", "modern-raw"}:
            if value["scenario"] == "modern-raw":
                key = kind + "-v1:" + hashlib.sha256(key.encode()).hexdigest()
            try:
                await command(client, kind, seed, key)
            except (ServerError, SignalFailed, UpdateFailed) as error:
                assert error.status == 409
                assert error.body["reason"] == "request_id_ambiguous"
                assert error.body["retained"] is True and error.body["intent_verified"] is False
                assert error.body["retryable"] is False
                cell["refusal"] = "request_id_ambiguous"
            else:
                raise AssertionError("ambiguous key selected a receipt")
        else:
            replies = await asyncio.gather(*(command(client, kind, seed, key) for _ in range(4)))
            for reply in replies:
                retained(receipt, reply, kind)
                if kind == "update" and value["state"] != "open":
                    assert reply["update_status"] == "completed" and reply["result"] is None
            try:
                await command(client, kind, seed, key, name="other")
            except (SignalFailed, UpdateFailed) as error:
                assert error.reason == "request_id_conflict"
                assert error.details["retained_command_id"] == receipt["command_id"]
            else:
                raise AssertionError("changed legacy name was admitted")
            if kind == "signal":
                try:
                    await command(client, kind, seed, key, run_id="01M00000000000000000000000")
                except SignalFailed as error:
                    assert error.reason == "request_id_conflict"
                else:
                    raise AssertionError("different explicit run selected the legacy receipt")
            if kind == "update":
                outcome = await client.update_outcome_receipt(seed["workflow_id"], receipt["update_id"])
                assert outcome["update_id"] == receipt["update_id"]
                assert outcome["update_status"] == ("accepted" if value["state"] == "open" else "completed")
                if value["state"] != "open":
                    assert outcome["result"] is None
                    assert await client.update_outcome(seed["workflow_id"], receipt["update_id"]) is None
            cell.update(concurrent_retries=4, retained=True, intent_verified=False, changed_name="request_id_conflict")
        cells.append(cell)
    return cells


async def case_distinct(client, value):
    seed = value["seed"]
    cells = []
    for kind, original in seed["commands"].items():
        admitted = await command(client, kind, seed, "casesensitive")
        assert admitted["command_id"] != original["original"]["command_id"]
        assert admitted["run_id"] == seed["run_id"]
        assert admitted["request_id"] == "casesensitive" and admitted.get("retained") is not True
        cells.append({"kind": kind, "new_command_id": admitted["command_id"], "distinct_key_admitted": True})
    return cells


async def main(args):
    if not __debug__:
        raise ValueError("run acceptance without optimization")
    value = packet(args.seed_data)
    if hashlib.sha256(args.seed_data).hexdigest() != args.seed_sha256 or args.output.exists():
        raise ValueError("captured packet identity or output differs")
    upstream = urlsplit(args.upstream)
    if (
        upstream.scheme != "http"
        or upstream.hostname != "127.0.0.1"
        or args.upstream != f"http://127.0.0.1:{upstream.port}"
    ):
        raise ValueError("require canonical numeric loopback")
    mode = args.credential_file.lstat().st_mode
    if not stat.S_ISREG(mode) or mode & 0o077:
        raise ValueError("fixture credential must be private")
    token = dict(line.split("=", 1) for line in args.credential_file.read_text().splitlines())["DW_AUTH_TOKEN"]
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "adapter_sha256": args.adapter_sha256,
        "seed_sha256": args.seed_sha256,
        "sdk": args.package_identity,
        "scope": "synthetic legacy " + value["scenario"] + " " + value["state"],
        "complete": False,
    }
    try:
        async with Client(args.upstream, token=token) as client:
            assert (await client.get_cluster_info())["version"] == args.server_version
            if value["phase"] == "prepare":
                report["seed"] = await prepare(client, value)
            elif value["phase"] == "case_distinct":
                report["cells"] = await case_distinct(client, value)
            else:
                report["cells"] = await read(client, value)
        report["complete"] = True
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
    except Exception as error:
        report["failure_type"] = type(error).__name__
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    return report
