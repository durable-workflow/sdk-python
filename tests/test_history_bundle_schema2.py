"""Unmodified published exports and negative controls for schema-2 integrity."""

from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer
from durable_workflow.history_bundle_verify import _canonicalize, main, verify_bundle, verify_bundle_json
from durable_workflow.replay_verify import main as replay_main
from durable_workflow.replay_verify import simulate_bundles

FIXTURES = Path(__file__).parent / "fixtures" / "history_bundles" / "schema2_published"


def _bundle(name: str = "completed") -> dict[str, Any]:
    return json.loads((FIXTURES / "bundles" / f"{name}.json").read_text())


def _resign_negative_control(bundle: dict[str, Any], key: str | None = None) -> dict[str, Any]:
    # Never write to a genuine exported fixture. A newly signed copy isolates
    # structural refusal from checksum refusal in the deliberately bad case.
    body = {k: v for k, v in bundle.items() if k != "integrity"}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    bundle["integrity"] = {
        "canonicalization": "json-recursive-ksort-v1", "checksum_algorithm": "sha256",
        "checksum": hashlib.sha256(canonical).hexdigest(),
    }
    if key is not None:
        bundle["integrity"].update({
            "signature_algorithm": "hmac-sha256", "key_id": "synthetic-test-key",
            "signature": hmac.new(key.encode(), canonical, hashlib.sha256).hexdigest(),
        })
    return bundle


def _rules(report: dict[str, Any]) -> set[str]:
    return {finding["rule"] for finding in report["findings"]}


@pytest.mark.parametrize("name", ["waiting", "completed", "unicode_waiting", "unicode_completed"])
def test_genuine_published_schema2_export_verifies_without_rewriting(name: str) -> None:
    raw = (FIXTURES / "bundles" / f"{name}.json").read_bytes()
    provenance = json.loads((FIXTURES / "provenance.json").read_text())["exports"][name]
    assert hashlib.sha256(raw).hexdigest() == provenance["file_sha256"]
    report = verify_bundle_json(raw.decode())
    assert report["status"] == "ok", report["findings"]
    assert report["findings"] == []
    assert report["integrity"]["checksum_matches"] is True
    assert report["integrity"]["recomputed_checksum"] == provenance["canonical_checksum"]
    assert report["bundle"]["history_event_count"] == provenance["history_event_count"]


def test_genuine_export_directory_is_integrity_only() -> None:
    report = simulate_bundles(FIXTURES / "bundles").to_dict()
    assert report["verdict"] == "ok"
    assert report["promotion_decision"] == "review_before_promote"
    assert report["evidence"]["replay_checked_count"] == 0
    assert report["evidence"]["replay_skipped"] is True


def test_both_actual_cli_entry_points_accept_unmodified_exports(tmp_path: Path) -> None:
    single = tmp_path / "single.json"
    simulation = tmp_path / "simulation.json"
    assert main([str(FIXTURES / "bundles" / "completed.json"), "--output", str(single)]) == 0
    assert replay_main([
        str(FIXTURES / "bundles"), "--simulate-bundles", "--output", str(simulation),
    ]) == 0
    assert json.loads(single.read_text())["integrity"]["checksum_matches"] is True
    assert json.loads(simulation.read_text())["promotion_decision"] == "review_before_promote"


@pytest.mark.parametrize("version", [True, False, 1.0, 2.0, "1", "2", 0, 3, -1, None, [], {}])
def test_malformed_or_unknown_schema_versions_fail_closed(version: Any) -> None:
    bundle = _bundle()
    bundle["schema_version"] = version
    report = verify_bundle(_resign_negative_control(bundle))
    assert report["status"] == "failed"
    assert "bundle.schema_version_unsupported" in _rules(report)
    assert report["integrity"]["checksum_matches"] is True


@pytest.mark.parametrize("mutation, rule", [
    ("missing", "workflow.memo_payload_missing"),
    ("wrong_codec", "workflow.memo_payload_invalid"),
    ("wrong_blob", "workflow.memo_payload_invalid"),
    ("not_map", "workflow.memo_payload_invalid"),
    ("unknown_writer", "workflow.memo_payload_invalid"),
    ("extra_field", "workflow.memo_payload_invalid"),
    ("bad_key", "workflow.memo_payload_invalid"),
    ("missing_projection", "workflow.memo_projection_mismatch"),
    ("changed_projection", "workflow.memo_projection_mismatch"),
])
def test_v2_memo_authority_and_projection_are_structurally_checked(mutation: str, rule: str) -> None:
    bundle = _bundle()
    workflow = bundle["workflow"]
    if mutation == "missing":
        del workflow["memo_payload"]
    elif mutation == "wrong_codec":
        workflow["memo_payload"]["codec"] = "json"
    elif mutation == "wrong_blob":
        workflow["memo_payload"]["blob"] = "not base64"
    elif mutation == "not_map":
        workflow["memo_payload"] = serializer.envelope([], codec="avro")
    elif mutation == "unknown_writer":
        workflow["memo_payload"]["blob"] = "wwEAAAAAAAAAAA4A"
    elif mutation == "extra_field":
        workflow["memo_payload"]["unrecognized"] = "must not pass"
    elif mutation == "bad_key":
        workflow["memo_payload"] = serializer.envelope({"12": "invalid memo name"}, codec="avro")
    elif mutation == "missing_projection":
        del workflow["memo"]
    else:
        workflow["memo"] = {"wrong": "projection"}
    report = verify_bundle(_resign_negative_control(bundle))
    assert report["status"] == "failed"
    assert rule in _rules(report)
    assert report["integrity"]["checksum_matches"] is True


def test_v2_typed_memos_preserve_bytes_maps_and_numeric_kinds() -> None:
    bundle = _bundle()
    reference = json.loads((FIXTURES / "typed-memo-reference.json").read_text())
    bundle["workflow"]["memo_payload"] = reference["memo_payload"]
    bundle["workflow"]["memo"] = reference["memo"]
    report = verify_bundle(_resign_negative_control(bundle))
    assert report["status"] == "ok", report["findings"]
    bundle["workflow"]["memo"]["values"][0] = 1
    report = verify_bundle(_resign_negative_control(bundle))
    assert "workflow.memo_projection_mismatch" in _rules(report)


@pytest.mark.parametrize("redacted", ["memo", "memo_payload", "both"])
def test_v2_declared_redaction_remains_supported(redacted: str) -> None:
    bundle = _bundle()
    names = ["memo", "memo_payload"] if redacted == "both" else [redacted]
    bundle["redaction"] = {"applied": True, "policy": "synthetic", "paths": []}
    for name in names:
        bundle["workflow"][name] = "[redacted]"
        bundle["redaction"]["paths"].append("workflow." + name)
    report = verify_bundle(_resign_negative_control(bundle))
    assert report["status"] == "ok", report["findings"]


def test_v2_checksum_and_signature_refusals_still_apply() -> None:
    bundle = _resign_negative_control(_bundle(), "test-secret")
    assert verify_bundle(bundle, "test-secret")["status"] == "ok"
    assert "integrity.signature_mismatch" in _rules(verify_bundle(bundle, "wrong-key"))
    bundle["workflow"]["memo"] = {"tampered": True}
    report = verify_bundle(bundle, "test-secret")
    assert report["status"] == "failed"
    assert "integrity.checksum_mismatch" in _rules(report)
    assert "integrity.signature_mismatch" in _rules(report)


def test_supported_schema1_does_not_require_v2_memos() -> None:
    bundle = _bundle()
    bundle["schema_version"] = 1
    del bundle["workflow"]["memo"]
    del bundle["workflow"]["memo_payload"]
    report = verify_bundle(_resign_negative_control(bundle))
    assert report["status"] == "ok", report["findings"]


def test_direct_api_reports_non_json_memo_projection_without_raising() -> None:
    bundle = _bundle()
    bundle["workflow"]["memo"] = b"not a JSON projection"
    report = verify_bundle(bundle)
    assert report["status"] == "failed"
    assert "workflow.memo_projection_mismatch" in _rules(report)
    assert "integrity.canonicalization_failed" in _rules(report)


def test_canonical_json_matches_independent_published_php_bytes() -> None:
    reference = json.loads((FIXTURES / "php-canonical-reference.json").read_text())
    for case in reference["cases"]:
        observed = _canonicalize(case["body"])
        assert observed == case["canonical_json"], case["name"]
        assert hashlib.sha256(observed.encode()).hexdigest() == case["sha256"]


def test_published_php_signed_unicode_control_verifies_and_rejects_a_wrong_key() -> None:
    reference = json.loads((FIXTURES / "php-canonical-reference.json").read_text())["unicode_signature_control"]
    report = verify_bundle(reference["bundle"], reference["test_signing_key"])
    assert report["status"] == "ok", report["findings"]
    assert report["integrity"]["signature_verified"] is True
    assert "integrity.signature_mismatch" in _rules(verify_bundle(reference["bundle"], "wrong-key"))
