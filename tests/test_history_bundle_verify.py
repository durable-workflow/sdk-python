"""Tests for the offline history-bundle integrity verifier."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

import pytest

from durable_workflow.history_bundle_verify import (
    BUNDLE_SCHEMA,
    BUNDLE_SCHEMA_VERSION,
    REPORT_SCHEMA,
    REPORT_SCHEMA_VERSION,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_WARNING,
    main,
    verify_bundle,
    verify_bundle_json,
)


def _base_bundle() -> dict[str, Any]:
    return {
        "schema": BUNDLE_SCHEMA,
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "exported_at": "2026-04-09T12:00:00.000000Z",
        "dedupe_key": "run-1:1:2026-04-09T12:00:00.000000Z",
        "history_complete": True,
        "workflow": {
            "instance_id": "instance-1",
            "run_id": "run-1",
            "run_number": 1,
            "workflow_type": "history.export",
            "workflow_class": "App\\Workflows\\HistoryExport",
            "last_history_sequence": 1,
        },
        "payloads": {
            "codec": "json",
            "arguments": {"available": True, "data": '"order-1"'},
            "output": {"available": True, "data": '{"ok":true}'},
        },
        "history_events": [
            {
                "id": "evt-1",
                "sequence": 1,
                "type": "WorkflowStarted",
                "workflow_task_id": None,
                "workflow_command_id": None,
                "recorded_at": "2026-04-09T12:00:00.000000Z",
                "payload": {},
            }
        ],
        "commands": [],
        "codec_schemas": {},
        "payload_manifest": {"version": 1, "entries": []},
        "redaction": {"applied": False, "policy": None, "paths": []},
    }


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def _sign(bundle: dict[str, Any], signing_key: str, key_id: str) -> dict[str, Any]:
    cloned = {k: v for k, v in bundle.items() if k != "integrity"}
    canonical = json.dumps(_canonicalize(cloned), separators=(",", ":"), ensure_ascii=False)
    bundle["integrity"] = {
        "canonicalization": "json-recursive-ksort-v1",
        "checksum_algorithm": "sha256",
        "checksum": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "signature_algorithm": "hmac-sha256",
        "signature": hmac.new(
            signing_key.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256
        ).hexdigest(),
        "key_id": key_id,
    }
    return bundle


def _rule_names(report: dict[str, Any]) -> list[str]:
    return [str(finding.get("rule")) for finding in report["findings"]]


def test_signed_bundle_reports_ok() -> None:
    signing_key = "history-export-secret"
    bundle = _sign(_base_bundle(), signing_key, "key-1")

    report = verify_bundle(bundle, signing_key=signing_key)

    assert report["schema"] == REPORT_SCHEMA
    assert report["schema_version"] == REPORT_SCHEMA_VERSION
    assert report["status"] == STATUS_OK
    assert report["findings"] == []
    assert report["integrity"]["checksum_ok"] is True
    assert report["integrity"]["signature_ok"] is True
    assert report["integrity"]["key_id"] == "key-1"


def test_tampered_bundle_reports_checksum_mismatch() -> None:
    signing_key = "history-export-secret"
    bundle = _sign(_base_bundle(), signing_key, "key-1")
    bundle["payloads"]["arguments"]["data"] = "tampered"

    report = verify_bundle(bundle, signing_key=signing_key)

    assert report["status"] == STATUS_FAILED
    rules = _rule_names(report)
    assert "integrity.checksum_mismatch" in rules
    assert "integrity.signature_mismatch" in rules
    assert report["integrity"]["checksum_ok"] is False
    assert report["integrity"]["signature_ok"] is False


def test_signed_bundle_without_key_warns() -> None:
    signing_key = "history-export-secret"
    bundle = _sign(_base_bundle(), signing_key, "key-1")

    report = verify_bundle(bundle, signing_key=None)

    assert report["status"] == STATUS_WARNING
    assert "integrity.signature_key_unavailable" in _rule_names(report)
    assert report["integrity"]["signature_ok"] is None


def test_unparseable_json_is_failed() -> None:
    report = verify_bundle_json("{not json")
    assert report["status"] == STATUS_FAILED
    assert "bundle.unparseable" in _rule_names(report)


def test_history_event_sequence_must_be_monotonic() -> None:
    signing_key = "history-export-secret"
    bundle = _base_bundle()
    bundle["history_events"] = [
        {
            "id": "evt-1",
            "sequence": 2,
            "type": "WorkflowStarted",
            "recorded_at": "2026-04-09T12:00:00.000000Z",
            "payload": {},
        },
        {
            "id": "evt-2",
            "sequence": 1,
            "type": "WorkflowCompleted",
            "recorded_at": "2026-04-09T12:00:01.000000Z",
            "payload": {},
        },
    ]
    bundle = _sign(bundle, signing_key, "key-1")

    report = verify_bundle(bundle, signing_key=signing_key)
    assert "history_events.sequence_not_monotonic" in _rule_names(report)
    assert report["status"] == STATUS_FAILED


def test_main_writes_report_and_returns_zero_for_ok(tmp_path) -> None:
    signing_key = "secret"
    bundle = _sign(_base_bundle(), signing_key, "key-1")
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
    output_path = tmp_path / "report.json"

    code = main(
        [
            str(bundle_path),
            "--signing-key",
            signing_key,
            "--output",
            str(output_path),
        ]
    )

    assert code == 0
    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["status"] == STATUS_OK


def test_main_returns_one_for_tampered_bundle(tmp_path) -> None:
    signing_key = "secret"
    bundle = _sign(_base_bundle(), signing_key, "key-1")
    bundle["payloads"]["arguments"]["data"] = "tampered"
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

    code = main([str(bundle_path), "--signing-key", signing_key])
    assert code == 1


def test_main_treats_warning_as_failure_under_strict(tmp_path, capsys) -> None:
    signing_key = "secret"
    bundle = _sign(_base_bundle(), signing_key, "key-1")
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

    # Without signing key, signed bundle yields a warning.
    code = main([str(bundle_path), "--strict-warnings"])
    assert code == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_WARNING


def test_history_event_id_duplicates_are_flagged() -> None:
    signing_key = "secret"
    bundle = _base_bundle()
    bundle["history_events"].append(
        {
            "id": "evt-1",
            "sequence": 5,
            "type": "WorkflowCompleted",
            "recorded_at": "2026-04-09T12:00:01.000000Z",
            "payload": {},
        }
    )
    bundle = _sign(bundle, signing_key, "key-1")

    report = verify_bundle(bundle, signing_key=signing_key)
    assert "history_events.id_duplicate" in _rule_names(report)


def test_writer_schema_fingerprint_mismatch_in_payload_manifest() -> None:
    signing_key = "secret"
    bundle = _base_bundle()
    bundle["payload_manifest"]["entries"].append(
        {
            "path": "payloads.output.data",
            "codec": "avro",
            "available": True,
            "redacted": False,
            "encoding": "base64-avro-binary",
            "avro_framing": "wrapper",
            "avro_prefix_hex": "00",
            "writer_schema": '{"type":"record","name":"X"}',
            "writer_schema_fingerprint": "sha256:deadbeef",
            "diagnostic": None,
        }
    )
    bundle = _sign(bundle, signing_key, "key-1")

    report = verify_bundle(bundle, signing_key=signing_key)
    assert "payload_manifest.writer_schema_fingerprint_mismatch" in _rule_names(report)
    assert report["status"] == STATUS_FAILED


@pytest.mark.parametrize(
    "broken_field, expected_rule",
    [
        ("schema", "bundle.schema_unexpected"),
        ("schema_version", "bundle.schema_version_unsupported"),
    ],
)
def test_envelope_drift_is_failed(broken_field: str, expected_rule: str) -> None:
    bundle = _base_bundle()
    bundle[broken_field] = "drift" if broken_field == "schema" else 99

    report = verify_bundle(bundle)
    assert expected_rule in _rule_names(report)
    assert report["status"] == STATUS_FAILED
