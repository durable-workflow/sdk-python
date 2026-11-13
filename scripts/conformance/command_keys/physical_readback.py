"""Installed SDK readbacks for the producer-owned physical seed, after upgrade.

The outer runner owns the verified seed/database, exact image, upgrade, complete
zero-write snapshots and cleanup. This consumer neither seeds nor upgrades data.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import stat
from datetime import datetime, timezone
from urllib.parse import urlsplit

from durable_workflow import Client
from durable_workflow.errors import UpdateFailed

# Physical seed authority includes the producer's unkeyed-signal clarification.
# It is distinct from the older manifest bound to the synthetic-copy helper.
MANIFEST_SHA256 = "ceb4695643e48c7bfe3f81eea380c6ab9aa0b973da830cb552d070344047319f"


def seed_cases(document):
    if (
        document.get("schema") != "durable-workflow.command-key-physical-seed.v1"
        or document.get("manifest_sha256") != MANIFEST_SHA256
    ):
        raise ValueError("require the producer-owned physical seed contract")
    cases = document["cases"]
    expected = {(kind, state) for kind in ("signal", "update") for state in ("open", "completed", "continued")}
    if len(cases) != len(expected) or {(case["kind"], case["state"]) for case in cases} != expected:
        raise ValueError("physical seed must contain each original state/type exactly once")
    for case in cases:
        receipt = case["receipt"]
        fields = ("command_id", "run_id") + (("update_id", "accepted_at") if case["kind"] == "update" else ())
        if any(not isinstance(receipt.get(field), str) or not receipt[field] for field in fields):
            raise ValueError("original receipt has incomplete durable identities")
        if receipt["workflow_id"] != case["workflow_id"]:
            raise ValueError("original receipt names a different workflow")
        if case["kind"] == "update" and not case["original_body"].get("request_id"):
            raise ValueError("original keyed update identity is missing")
        if case["history"]["next_page_token"] is not None:
            raise ValueError("physical seed history was not fully captured")
    return cases


def legacy_receipt(original, received):
    if received.get("retained") is not True or received.get("intent_verified") is not False:
        raise ValueError("legacy readback must be retained and explicitly unverified")
    for field in ("command_id", "run_id", "update_id", "accepted_at"):
        if received.get(field) != original[field]:
            raise ValueError("legacy readback changed original " + field)


async def main(args):
    if not __debug__:
        raise ValueError("acceptance assertions require Python without optimization")
    if args.output.exists():
        raise ValueError("refusing to overwrite existing evidence")
    upstream = urlsplit(args.upstream)
    if (
        upstream.scheme != "http"
        or upstream.hostname != "127.0.0.1"
        or not upstream.port
        or args.upstream != f"http://127.0.0.1:{upstream.port}"
    ):
        raise ValueError("synthetic runtime must be canonical numeric loopback")
    raw_seed = args.seed_data
    if hashlib.sha256(raw_seed).hexdigest() != args.seed_sha256:
        raise ValueError("producer seed receipt hash mismatch")
    cases = seed_cases(json.loads(raw_seed))
    package = args.package_identity
    mode = args.credential_file.lstat().st_mode
    if not stat.S_ISREG(mode) or mode & 0o077:
        raise ValueError("fixture credential file must be owner-only")
    token = dict(line.split("=", 1) for line in args.credential_file.read_text().splitlines())["DW_AUTH_TOKEN"]
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "sdk": package,
        "seed_sha256": args.seed_sha256,
        "verifier_sha256": args.verifier_sha256,
        "adapter_sha256": args.adapter_sha256,
        "scope": "installed SDK physical-upgrade readbacks only",
        "cells": [],
        "complete": False,
    }
    try:
        async with Client(args.upstream, token=token) as client:
            info = await client.get_cluster_info()
            if info["version"] != args.server_version:
                raise ValueError("runtime differs from the outer verified upgrade tuple")
            report["server_version"] = info["version"]
            for case in cases:
                original = case["receipt"]
                workflow_id = case["workflow_id"]
                observed = [event async for event in client.iter_history(workflow_id, original["run_id"])]
                if observed != case["history"]["events"]:
                    raise ValueError("upgrade history differs from the original producer capture")
                cell = {
                    "kind": case["kind"],
                    "state": case["state"],
                    "command_id": original["command_id"],
                    "run_id": original["run_id"],
                    "original_history_preserved": True,
                }
                if case["kind"] == "signal":
                    # Published historical signals have NULL stored keys. Never
                    # manufacture keyed recovery from an unrelated current key.
                    cell["scope"] = "unkeyed historical signal compatibility only"
                else:
                    key = case["original_body"]["request_id"]
                    name = original["update_name"]
                    receipts = await asyncio.gather(
                        *(
                            client.update_workflow(workflow_id, name, args=[999], request_id=key, wait_for="accepted")
                            for _ in range(4)
                        )
                    )
                    for receipt in receipts:
                        legacy_receipt(original, receipt)
                        assert receipt["request_id"] == key
                        if case["state"] != "open":
                            assert receipt["update_status"] == "completed" and receipt["result"] is None
                    try:
                        await client.update_workflow(workflow_id, "other", args=[999], request_id=key)
                    except UpdateFailed as error:
                        assert error.reason == "request_id_conflict"
                        assert error.details["retained_command_id"] == original["command_id"]
                    else:
                        raise AssertionError("changed legacy command name was admitted")
                    outcome = await client.update_outcome_receipt(workflow_id, original["update_id"])
                    assert outcome["update_id"] == original["update_id"]
                    if case["state"] == "open":
                        assert outcome["update_status"] == "accepted"
                    else:
                        assert outcome["update_status"] == "completed" and outcome["result"] is None
                        assert await client.update_outcome(workflow_id, original["update_id"]) is None
                    cell.update(
                        {
                            "scope": "original keyed update; unscoped SDK route returns original run",
                            "update_id": original["update_id"],
                            "concurrent_retries": len(receipts),
                            "retained": True,
                            "intent_verified": False,
                            "changed_name": "request_id_conflict",
                        }
                    )
                report["cells"].append(cell)
        report["complete"] = True
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
    except Exception as error:
        report["failure_type"] = type(error).__name__
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    raise SystemExit("Use the captured-input run_physical_readback.py controller")
