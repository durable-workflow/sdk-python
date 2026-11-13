from __future__ import annotations

import copy
import hashlib
import importlib.util
import sqlite3
from pathlib import Path

import pytest

PATH = Path(__file__).parents[1] / "scripts/conformance/command_keys/physical_readback.py"
SPEC = importlib.util.spec_from_file_location("physical_readback", PATH)
assert SPEC is not None and SPEC.loader is not None
consumer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(consumer)
CONTROLLER_SPEC = importlib.util.spec_from_file_location(
    "physical_controller", PATH.with_name("run_physical_readback.py")
)
assert CONTROLLER_SPEC is not None and CONTROLLER_SPEC.loader is not None
controller = importlib.util.module_from_spec(CONTROLLER_SPEC)
CONTROLLER_SPEC.loader.exec_module(controller)


def seed():
    cases = []
    for kind in ("signal", "update"):
        for state in ("open", "completed", "continued"):
            identity = kind + "-" + state
            receipt = {"workflow_id": identity, "command_id": "command-" + identity, "run_id": "run-" + identity}
            body = {}
            if kind == "update":
                receipt.update({"update_id": "update-" + identity, "accepted_at": "2026-01-01T00:00:00Z"})
                body["request_id"] = "original-update-key"
            cases.append(
                {
                    "kind": kind,
                    "state": state,
                    "workflow_id": identity,
                    "receipt": receipt,
                    "original_body": body,
                    "history": {"events": [], "next_page_token": None},
                }
            )
    return {
        "schema": "durable-workflow.command-key-physical-seed.v1",
        "manifest_sha256": consumer.MANIFEST_SHA256,
        "cases": cases,
    }


def test_accepts_real_unkeyed_signal_compatibility_without_inventing_keys():
    cases = consumer.seed_cases(seed())
    assert all("request_id" not in case["original_body"] for case in cases if case["kind"] == "signal")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(manifest_sha256="unreviewed"),
        lambda value: value["cases"].pop(),
        lambda value: value["cases"].append(copy.deepcopy(value["cases"][0])),
        lambda value: value["cases"][0].update(state="unsupported"),
        lambda value: value["cases"][0]["receipt"].pop("command_id"),
        lambda value: value["cases"][0]["receipt"].update(workflow_id="other"),
        lambda value: value["cases"][3]["original_body"].pop("request_id"),
        lambda value: value["cases"][0]["history"].update(next_page_token="unfinished"),
    ],
)
def test_incomplete_or_wrong_seed_refuses_before_client_creation(mutation):
    document = seed()
    mutation(document)
    with pytest.raises(ValueError):
        consumer.seed_cases(document)


@pytest.mark.parametrize(
    "mutation",
    [
        {"retained": False},
        {"intent_verified": True},
        {"intent_verified": None},
        {"command_id": "other"},
        {"run_id": "successor"},
        {"update_id": "other"},
        {"accepted_at": "changed"},
    ],
)
def test_legacy_receipt_cannot_claim_verification_or_change_original_identities(mutation):
    original = seed()["cases"][3]["receipt"]
    received = {**original, "retained": True, "intent_verified": False}
    consumer.legacy_receipt(original, received)
    with pytest.raises(ValueError):
        consumer.legacy_receipt(original, {**received, **mutation})


def test_input_hash_and_regular_file_refusal(tmp_path):
    path = tmp_path / "synthetic"
    path.write_bytes(b"original")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    controller.verify_file(path, digest)
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="identity differs"):
        controller.verify_file(path, digest)
    link = tmp_path / "linked"
    link.symlink_to(path)
    with pytest.raises(ValueError, match="identity differs"):
        controller.verify_file(link, hashlib.sha256(path.read_bytes()).hexdigest())


def test_schema_authority_uses_native_marker_not_sqlite_user_version():
    with sqlite3.connect(":memory:") as connection:
        connection.execute("PRAGMA user_version=12")
        connection.execute("CREATE TABLE dw_server_schema(engine,version)")
        connection.execute("INSERT INTO dw_server_schema VALUES ('rust-development',13)")
        controller.native_marker(connection, 13)
        with pytest.raises(ValueError, match="marker differs"):
            controller.native_marker(connection, 12)
        connection.execute("INSERT INTO dw_server_schema VALUES ('rust-development',13)")
        with pytest.raises(ValueError, match="marker differs"):
            controller.native_marker(connection, 13)
