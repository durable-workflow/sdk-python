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


def test_case_distinct_control_requires_two_new_durable_commands_and_intents():
    before = {"tables": {"workflow_commands": {"rows": [[1]]}, "dw_command_intents": {"rows": []}}}
    after = copy.deepcopy(before)
    after["tables"]["workflow_commands"]["rows"] += [[2], [3]]
    after["tables"]["dw_command_intents"]["rows"] += [[2], [3]]
    controller.validate_distinct_counts(before, after)
    after["tables"]["dw_command_intents"]["rows"].pop()
    with pytest.raises(ValueError):
        controller.validate_distinct_counts(before, after)


def test_synthetic_bundle_has_the_same_closed_roles_as_physical_not_the_proxy():
    with pytest.raises(ValueError, match="source roles"):
        bootstrap.decode_inputs({"scenario": "synthetic_legacy", "sources": {"adapter": {}}})
    with pytest.raises(ValueError, match="source roles"):
        bootstrap.decode_inputs({"scenario": "invented", "sources": {}})
