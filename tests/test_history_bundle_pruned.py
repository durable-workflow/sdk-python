"""Retention-pruned history bundles fail verification, matching PHP (#228)."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from durable_workflow.history_bundle_verify import main, verify_bundle, verify_bundle_json
from durable_workflow.replay_verify import main as replay_main
from durable_workflow.replay_verify import simulate_bundles

FIXTURES = Path(__file__).parent / "fixtures" / "history_bundles" / "retention_pruned"
BUNDLE = FIXTURES / "pruned-bundle.json"


def provenance() -> dict[str, Any]:
    return json.loads((FIXTURES / "provenance.json").read_text())


def bundle() -> dict[str, Any]:
    return json.loads(BUNDLE.read_text())


def rechecksum(value: dict[str, Any]) -> dict[str, Any]:
    body = {key: item for key, item in value.items() if key != "integrity"}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    value["integrity"] = {
        "canonicalization": "json-recursive-ksort-v1",
        "checksum_algorithm": "sha256",
        "checksum": hashlib.sha256(canonical).hexdigest(),
    }
    return value


def rules(report: dict[str, Any]) -> list[str]:
    return [finding["rule"] for finding in report["findings"]]


def test_fixture_is_the_untouched_php_export() -> None:
    record = provenance()
    assert hashlib.sha256(BUNDLE.read_bytes()).hexdigest() == record["file_sha256"]


def test_pruned_php_export_fails_exactly_like_php() -> None:
    record = provenance()

    report = verify_bundle_json(BUNDLE.read_text())

    assert report["status"] == record["expected_php_verification"]["status"] == "failed"
    assert report["findings"] == record["expected_php_verification"]["findings"]
    assert report["integrity"]["checksum_matches"] is True
    assert report["integrity"]["recomputed_checksum"] == record["canonical_checksum"]
    assert report["summary"]["errors"] == 1


def test_pruned_dedupe_key_is_accepted_as_exported() -> None:
    value = bundle()
    assert ":details-pruned:" in value["dedupe_key"]

    report = verify_bundle(value)

    assert rules(report) == ["history.details_pruned"]


@pytest.mark.parametrize("pruned_at", [None, "", 7, "absent"])
def test_pruned_marker_without_a_timestamp_still_fails(pruned_at: object) -> None:
    value = bundle()
    if pruned_at == "absent":
        del value["workflow"]["details_pruned_at"]
    else:
        value["workflow"]["details_pruned_at"] = pruned_at

    report = verify_bundle(rechecksum(value))

    assert report["status"] == "failed"
    assert report["integrity"]["checksum_matches"] is True
    [finding] = report["findings"]
    assert finding["rule"] == "history.details_pruned"
    assert finding["message"] == (
        "Retention cleanup pruned this run's details; the bundle has no history to replay or import."
    )
    assert finding["context"] == {"details_pruned_at": None, "history_complete": False}


@pytest.mark.parametrize("state", ["retained", "absent"])
def test_retained_and_older_exports_are_unchanged(state: str) -> None:
    value = bundle()
    value["workflow"]["details_pruned_at"] = None
    if state == "absent":
        del value["workflow"]["details_state"]
        del value["workflow"]["details_pruned_at"]
    else:
        value["workflow"]["details_state"] = state

    report = verify_bundle(rechecksum(value))

    assert "history.details_pruned" not in rules(report)
    assert report["status"] == "ok", report["findings"]


@pytest.mark.parametrize("state", ["PRUNED", "pruned ", True, None])
def test_only_the_exact_pruned_marker_fails(state: object) -> None:
    value = bundle()
    value["workflow"]["details_state"] = state

    report = verify_bundle(rechecksum(value))

    assert "history.details_pruned" not in rules(report)


def test_simulation_and_both_cli_entry_points_block_a_pruned_bundle(tmp_path: Path) -> None:
    directory = tmp_path / "bundles"
    directory.mkdir()
    shutil.copyfile(BUNDLE, directory / "pruned-bundle.json")

    simulation = simulate_bundles(directory).to_dict()
    assert simulation["verdict"] == "failed"
    assert simulation["promotion_decision"] == "block_and_investigate"
    [entry] = simulation["bundles"]
    assert entry["evidence"]["integrity_status"] == "failed"

    single = tmp_path / "single.json"
    replayed = tmp_path / "simulation.json"
    assert main([str(BUNDLE), "--output", str(single)]) != 0
    assert replay_main([str(directory), "--simulate-bundles", "--output", str(replayed)]) != 0
    assert rules(json.loads(single.read_text())) == ["history.details_pruned"]
    assert json.loads(replayed.read_text())["verdict"] == "failed"
