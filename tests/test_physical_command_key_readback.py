from __future__ import annotations

import copy
import hashlib
import importlib.util
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
GUARD_SPEC = importlib.util.spec_from_file_location("physical_input_guards", PATH.with_name("input_guards.py"))
assert GUARD_SPEC is not None and GUARD_SPEC.loader is not None
guards = importlib.util.module_from_spec(GUARD_SPEC)
GUARD_SPEC.loader.exec_module(guards)
BOOTSTRAP_SPEC = importlib.util.spec_from_file_location("physical_bootstrap", PATH.with_name("consumer_bootstrap.py"))
assert BOOTSTRAP_SPEC is not None and BOOTSTRAP_SPEC.loader is not None
bootstrap = importlib.util.module_from_spec(BOOTSTRAP_SPEC)
BOOTSTRAP_SPEC.loader.exec_module(bootstrap)


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
    with guards.CapturedInput(path, "baseline", maximum=100, expected=digest) as captured:
        assert captured.data == b"original"
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="input refused"):
        guards.CapturedInput(path, "baseline", maximum=100, expected=digest)
    link = tmp_path / "linked"
    link.symlink_to(path)
    with pytest.raises(ValueError, match="input refused"):
        guards.CapturedInput(link, "baseline", maximum=100, expected=hashlib.sha256(path.read_bytes()).hexdigest())


def test_physical_bundle_closes_seed_and_verifier_roles():
    import base64

    sources = {
        role: {"base64": base64.b64encode(b"original").decode(), "sha256": hashlib.sha256(b"original").hexdigest()}
        for role in bootstrap.PHYSICAL_ROLES
    }
    assert bootstrap.decode_inputs({"scenario": "physical_readback", "sources": sources})["seed"] == b"original"
    sources["proxy"] = sources["seed"]
    with pytest.raises(ValueError, match="roles differ"):
        bootstrap.decode_inputs({"scenario": "physical_readback", "sources": sources})
    with pytest.raises(ValueError, match="roles differ"):
        bootstrap.decode_inputs({"scenario": "unknown", "sources": sources})


def test_schema_authority_uses_native_marker_not_sqlite_user_version():
    marker = {"columns": ["engine", "version"], "rows": [[["text", "rust-development"], ["integer", 13]]]}
    snapshot = {"user_version": 12, "tables": {"dw_server_schema": marker}}
    controller.native_marker(snapshot, 13)
    with pytest.raises(ValueError, match="marker differs"):
        controller.native_marker(snapshot, 12)
    marker["rows"].append(copy.deepcopy(marker["rows"][0]))
    with pytest.raises(ValueError, match="marker shape differs"):
        controller.native_marker(snapshot, 13)


def upgrade_snapshots():
    columns = ["version", "description", "installed_on", "success", "checksum", "execution_time"]
    before = {
        "schema": [("table", "business", "business", "business schema")],
        "user_version": 0,
        "application_id": 0,
        "tables": {
            "business": {"columns": ["id"], "rows": [[["text", "preserve"]]]},
            "dw_server_schema": {
                "columns": ["engine", "version"],
                "rows": [[["text", "rust-development"], ["integer", 12]]],
            },
            "_sqlx_migrations": {"columns": columns, "rows": []},
        },
    }
    added = [("table", "dw_command_intents", "dw_command_intents", "tagged schema")]
    after = copy.deepcopy(before)
    after["schema"] = sorted(before["schema"] + added)
    after["tables"]["dw_server_schema"]["rows"][0][1] = ["integer", 13]
    after["tables"]["dw_command_intents"] = {"columns": ["command_id", "namespace", "intent_digest"], "rows": []}
    after["tables"]["_sqlx_migrations"]["rows"] = [
        [
            ["integer", 13],
            ["text", controller.JOURNAL_DESCRIPTION],
            ["text", "2026-01-01 00:00:01"],
            ["integer", 1],
            ["blob", controller.JOURNAL_SHA384],
            ["integer", 0],
        ]
    ]
    return before, after, added


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value["tables"]["business"]["rows"].clear(),
        lambda value: value.update(user_version=13),
        lambda value: value["tables"]["dw_command_intents"]["rows"].append([["text", "unexpected"]]),
        lambda value: value["tables"]["_sqlx_migrations"]["rows"][0].__setitem__(0, ["integer", 12]),
        lambda value: value["tables"]["_sqlx_migrations"]["rows"][0].__setitem__(1, ["text", "other description"]),
        lambda value: value["tables"]["_sqlx_migrations"]["rows"][0].__setitem__(2, ["text", "2025-01-01 00:00:00"]),
        lambda value: value["tables"]["_sqlx_migrations"]["rows"][0].__setitem__(3, ["integer", 0]),
        lambda value: value["tables"]["_sqlx_migrations"]["rows"][0].__setitem__(4, ["blob", "wrong checksum"]),
        lambda value: value["tables"]["_sqlx_migrations"]["rows"][0].__setitem__(5, ["integer", 1]),
    ],
)
def test_upgrade_preserves_all_business_rows_and_closes_every_journal_field(mutation):
    before, after, added = upgrade_snapshots()
    began, finished = 1767225600.5, 1767225602.5
    controller.validate_upgrade(before, after, added, began, finished)
    mutation(after)
    with pytest.raises(ValueError):
        controller.validate_upgrade(before, after, added, began, finished)


def test_unapproved_helper_bytes_cannot_execute(tmp_path):
    script = tmp_path / "helper.py"
    marker = tmp_path / "executed"
    script.write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
    with pytest.raises(ValueError, match="source identity differs"):
        controller.load_bytes(script.read_bytes(), "unapproved_fixture", "0" * 64)
    assert not marker.exists()
