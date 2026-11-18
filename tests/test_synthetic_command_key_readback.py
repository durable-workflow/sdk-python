from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parents[1] / "scripts/conformance/command_keys"


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    assert spec and spec.loader
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


consumer = module("synthetic_consumer", "synthetic_legacy.py")
controller = module("synthetic_controller", "run_synthetic_legacy.py")
bootstrap = module("synthetic_bootstrap", "consumer_bootstrap.py")


def packet():
    return {
        "schema": "sdk-python.synthetic-legacy-input.v1",
        "manifest_sha256": consumer.MANIFEST_SHA256,
        "phase": "read",
        "state": "open",
        "scenario": "readback",
        "seed": {
            "workflow_id": "workflow",
            "run_id": "run",
            "commands": {
                kind: {
                    "original": {
                        "workflow_id": "workflow",
                        "run_id": "run",
                        "command_id": kind,
                        "update_id": "update",
                        "accepted_at": "2026-01-01T00:00:00Z",
                    }
                }
                for kind in ("signal", "update")
            },
        },
    }


@pytest.mark.parametrize("state", ["open", "completed", "continued"])
def test_declared_readback_states_keep_original_api_receipts(state):
    document = packet()
    document["state"] = state
    assert consumer.packet(json.dumps(document))["state"] == state
    controller.validate_cell(state, "readback")


@pytest.mark.parametrize(
    "change",
    [
        lambda v: v.update(manifest_sha256="wrong"),
        lambda v: v.update(phase="execute-arbitrary"),
        lambda v: v.update(state="invented"),
        lambda v: v.update(scenario="invented"),
        lambda v: v.update(phase="case_distinct"),
        lambda v: v.update(state="continued", scenario="two-rows"),
        lambda v: v["seed"]["commands"].pop("signal"),
        lambda v: v["seed"]["commands"]["update"]["original"].pop("update_id"),
        lambda v: v["seed"]["commands"]["signal"]["original"].update(run_id="different"),
        lambda v: v["seed"]["commands"]["signal"]["original"].update(workflow_id="different"),
    ],
)
def test_wrong_or_incomplete_packet_refuses_before_api(change):
    document = packet()
    change(document)
    with pytest.raises(ValueError):
        consumer.packet(json.dumps(document))


@pytest.mark.parametrize(
    "change",
    [
        {"retained": False},
        {"intent_verified": True},
        {"intent_verified": None},
        {"command_id": "new"},
        {"run_id": "successor"},
        {"update_id": "new"},
        {"accepted_at": "new"},
    ],
)
def test_retained_receipt_cannot_fabricate_intent_or_target(change):
    original = packet()["seed"]["commands"]["update"]["original"]
    reply = {**original, "retained": True, "intent_verified": False, **change}
    with pytest.raises(ValueError):
        consumer.retained(original, reply, "update")


def test_helper_execution_is_refused_before_wrong_bytes_can_run():
    inputs = {
        "snapshot_helper": SimpleNamespace(data=b"raise RuntimeError('must not execute')"),
        "manifest": SimpleNamespace(data=b"{}"),
    }
    with pytest.raises(ValueError, match="accepted helper/manifest"):
        controller.accepted_helper(inputs)


@pytest.mark.parametrize(
    "state,scenario", [("completed", "case-only"), ("continued", "modern-raw"), ("open", "unknown")]
)
def test_controller_refuses_unsupported_cell_combinations(state, scenario):
    with pytest.raises(ValueError):
        controller.validate_cell(state, scenario)


def test_case_delta_preserves_all_prior_rows_and_rejects_unrelated_writes():
    delta = module("case_delta_control", "case_delta.py")
    before = {
        "schema": [],
        "user_version": 0,
        "application_id": 0,
        "tables": {
            "workflow_commands": {"columns": ["id"], "rows": [[["text", "old"]]]},
            "dw_command_intents": {"columns": ["command_id"], "rows": [[["text", "old"]]]},
            "unrelated": {"columns": ["value"], "rows": [[["text", "preserve"]]]},
        },
    }
    expected = copy.deepcopy(before)
    for name in ("workflow_commands", "dw_command_intents"):
        expected["tables"][name]["rows"] += [[["text", "new1"]], [["text", "new2"]]]
        expected["tables"][name]["rows"].sort()
    delta.stable_catalog(before, expected)
    delta.finish_expected(copy.deepcopy(expected), expected)
    unrelated = copy.deepcopy(expected)
    unrelated["tables"]["unrelated"]["rows"][0][0][1] = "changed"
    replaced = copy.deepcopy(expected)
    replaced["tables"]["workflow_commands"]["rows"].remove([["text", "old"]])
    replaced["tables"]["workflow_commands"]["rows"].append([["text", "third-new"]])
    for changed in (unrelated, replaced):
        # The predecessor count control accepts both corruptions.
        assert all(
            len(changed["tables"][name]["rows"]) == len(before["tables"][name]["rows"]) + 2
            for name in ("workflow_commands", "dw_command_intents")
        )
        with pytest.raises(ValueError):
            delta.finish_expected(copy.deepcopy(expected), changed)
    columns = copy.deepcopy(expected)
    columns["tables"]["unrelated"]["columns"] = ["different"]
    with pytest.raises(ValueError):
        delta.stable_catalog(before, columns)


def test_stopped_copy_must_equal_the_first_restart_before_later_zero_writes():
    delta = module("case_delta_restart", "case_delta.py")
    stopped = {"business": [["original"]]}
    restarted = {"business": [["substituted"]]}
    # The predecessor only compares the two later snapshots and accepts this.
    assert restarted == copy.deepcopy(restarted)
    with pytest.raises(ValueError):
        delta.validate_restart(stopped, restarted)
    delta.validate_restart(stopped, copy.deepcopy(stopped))


def test_synthetic_bundle_has_the_same_closed_roles_as_physical_not_the_proxy():
    with pytest.raises(ValueError, match="source roles"):
        bootstrap.decode_inputs({"scenario": "synthetic_legacy", "sources": {"adapter": {}}})
    with pytest.raises(ValueError, match="source roles"):
        bootstrap.decode_inputs({"scenario": "invented", "sources": {}})
