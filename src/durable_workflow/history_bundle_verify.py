"""Offline integrity verifier for ``durable-workflow.v2.history-export`` bundles.

This module is the Python counterpart of the workflow-php
``HistoryBundleVerifier`` class. It produces a
``durable-workflow.v2.history-bundle-verification`` report whose shape,
statuses, severities, and rule names match the
``replay_verification_contract`` published by the control plane.

Two surfaces are exposed:

- :func:`verify_bundle` — verify a parsed bundle dict.
- :func:`verify_bundle_json` — verify a JSON-encoded bundle string.

The verifier is pure: it only reads the bundle and (when supplied) an
HMAC signing key. No I/O, no network, no SDK runtime needed. CI runners
and operator shells can call it in a sandbox.

The pair (bundle verifier + replay verifier) is the cross-runtime
replay verification contract: a Python verdict is shaped the same as
a workflow-php verdict, so promotion gates can compare them directly.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

REPORT_SCHEMA = "durable-workflow.v2.history-bundle-verification"
REPORT_SCHEMA_VERSION = 1

BUNDLE_SCHEMA = "durable-workflow.v2.history-export"
BUNDLE_SCHEMA_VERSION = 1

STATUS_OK = "ok"
STATUS_WARNING = "warning"
STATUS_FAILED = "failed"

SEVERITY_INFO = "info"
SEVERITY_WARNING = "warning"
SEVERITY_ERROR = "error"

SUPPORTED_CHECKSUM_ALGORITHMS = frozenset({"sha256"})
SUPPORTED_SIGNATURE_ALGORITHMS = frozenset({"hmac-sha256"})
SUPPORTED_CANONICALIZATIONS = frozenset({"json-recursive-ksort-v1"})


def verify_bundle_json(payload: str, signing_key: str | None = None) -> dict[str, Any]:
    """Decode a JSON-encoded bundle and verify it."""

    try:
        bundle = json.loads(payload)
    except json.JSONDecodeError as exc:
        return _finalize(
            findings=[
                _finding(
                    "bundle.unparseable",
                    SEVERITY_ERROR,
                    f"Bundle JSON could not be decoded: {exc}",
                )
            ],
            bundle_summary=_empty_bundle_summary(),
            integrity_report=_integrity_report_for(None, None, None, None, None),
        )

    return verify_bundle(bundle, signing_key)


def verify_bundle(bundle: Any, signing_key: str | None = None) -> dict[str, Any]:
    """Verify a parsed bundle and emit a report."""

    findings: list[dict[str, Any]] = []

    if not isinstance(bundle, dict):
        findings.append(
            _finding(
                "bundle.unparseable",
                SEVERITY_ERROR,
                "History export bundle must be a JSON object.",
            )
        )
        return _finalize(
            findings=findings,
            bundle_summary=_empty_bundle_summary(),
            integrity_report=_integrity_report_for(None, None, None, None, None),
        )

    _check_envelope(bundle, findings)
    _check_workflow(bundle, findings)
    _check_history_events(bundle, findings)
    _check_commands(bundle, findings)
    _check_payload_manifest(bundle, findings)
    _check_redaction(bundle, findings)
    integrity_report = _check_integrity(bundle, signing_key, findings)

    workflow = bundle.get("workflow") if isinstance(bundle.get("workflow"), Mapping) else {}
    history_events = bundle.get("history_events")
    bundle_summary = {
        "schema": _string_or_none(bundle.get("schema")),
        "schema_version": _int_or_none(bundle.get("schema_version")),
        "instance_id": _string_or_none(workflow.get("instance_id")) if isinstance(workflow, Mapping) else None,
        "run_id": _string_or_none(workflow.get("run_id")) if isinstance(workflow, Mapping) else None,
        "history_event_count": len(history_events) if isinstance(history_events, list) else 0,
    }

    return _finalize(findings, bundle_summary, integrity_report)


def _check_envelope(bundle: Mapping[str, Any], findings: list[dict[str, Any]]) -> None:
    if "schema" not in bundle:
        findings.append(_finding("bundle.schema_missing", SEVERITY_ERROR, "Bundle is missing the [schema] field."))
    elif bundle["schema"] != BUNDLE_SCHEMA:
        findings.append(
            _finding(
                "bundle.schema_unexpected",
                SEVERITY_ERROR,
                f"Bundle schema [{bundle['schema']!s}] does not match expected [{BUNDLE_SCHEMA}].",
                {"observed": bundle["schema"]},
            )
        )

    if "schema_version" not in bundle:
        findings.append(
            _finding(
                "bundle.schema_version_missing",
                SEVERITY_ERROR,
                "Bundle is missing the [schema_version] field.",
            )
        )
    elif bundle["schema_version"] != BUNDLE_SCHEMA_VERSION:
        findings.append(
            _finding(
                "bundle.schema_version_unsupported",
                SEVERITY_ERROR,
                f"Bundle schema_version [{bundle['schema_version']!s}] is unsupported; expected {BUNDLE_SCHEMA_VERSION}.",
                {"observed": bundle["schema_version"]},
            )
        )

    if not _string_or_none(bundle.get("exported_at")):
        findings.append(
            _finding(
                "bundle.exported_at_missing",
                SEVERITY_WARNING,
                "Bundle is missing a non-empty [exported_at] timestamp.",
            )
        )

    for section in ("workflow", "history_events", "commands", "payloads"):
        if section not in bundle:
            findings.append(
                _finding(
                    "bundle.section_missing",
                    SEVERITY_ERROR,
                    f"Bundle is missing required section [{section}].",
                    {"section": section},
                )
            )
            continue
        value = bundle[section]
        expected_type = list if section in ("history_events", "commands") else dict
        if not isinstance(value, expected_type):
            findings.append(
                _finding(
                    "bundle.section_invalid",
                    SEVERITY_ERROR,
                    f"Bundle section [{section}] must be a {expected_type.__name__}.",
                    {"section": section},
                )
            )


def _check_workflow(bundle: Mapping[str, Any], findings: list[dict[str, Any]]) -> None:
    workflow = bundle.get("workflow")
    if not isinstance(workflow, Mapping):
        return

    if not _string_or_none(workflow.get("run_id")):
        findings.append(
            _finding(
                "workflow.run_id_missing",
                SEVERITY_ERROR,
                "Bundle workflow.run_id must be a non-empty string.",
            )
        )

    if not _string_or_none(workflow.get("instance_id")):
        findings.append(
            _finding(
                "workflow.instance_id_missing",
                SEVERITY_ERROR,
                "Bundle workflow.instance_id must be a non-empty string.",
            )
        )

    if not _string_or_none(workflow.get("workflow_type")):
        findings.append(
            _finding(
                "workflow.workflow_type_missing",
                SEVERITY_ERROR,
                "Bundle workflow.workflow_type must be a non-empty string.",
            )
        )

    declared = _int_or_none(workflow.get("last_history_sequence"))
    events = bundle.get("history_events")
    if declared is not None and isinstance(events, list) and events:
        highest = 0
        for event in events:
            if isinstance(event, Mapping):
                seq = _int_or_none(event.get("sequence"))
                if seq is not None and seq > highest:
                    highest = seq
        if declared < highest:
            findings.append(
                _finding(
                    "workflow.last_history_sequence_stale",
                    SEVERITY_WARNING,
                    (
                        f"workflow.last_history_sequence={declared} is below the highest "
                        f"stored history event sequence {highest}."
                    ),
                    {"declared": declared, "observed": highest},
                )
            )


def _check_history_events(bundle: Mapping[str, Any], findings: list[dict[str, Any]]) -> None:
    events = bundle.get("history_events")
    if not isinstance(events, list):
        return

    previous: int | None = None
    seen_ids: set[str] = set()

    for index, event in enumerate(events):
        if not isinstance(event, Mapping):
            findings.append(
                _finding(
                    "history_events.entry_invalid",
                    SEVERITY_ERROR,
                    f"history_events[{index}] is not an object.",
                    {"index": index},
                )
            )
            continue

        sequence = _int_or_none(event.get("sequence"))
        if sequence is None:
            findings.append(
                _finding(
                    "history_events.sequence_missing",
                    SEVERITY_ERROR,
                    f"history_events[{index}].sequence must be an integer.",
                    {"index": index},
                )
            )
        elif previous is not None and sequence <= previous:
            findings.append(
                _finding(
                    "history_events.sequence_not_monotonic",
                    SEVERITY_ERROR,
                    (
                        f"history_events[{index}].sequence={sequence} does not strictly "
                        f"follow previous sequence {previous}."
                    ),
                    {"index": index, "sequence": sequence, "previous_sequence": previous},
                )
            )

        if sequence is not None:
            previous = sequence

        if not _string_or_none(event.get("type")):
            findings.append(
                _finding(
                    "history_events.type_missing",
                    SEVERITY_ERROR,
                    f"history_events[{index}].type must be a non-empty string.",
                    {"index": index},
                )
            )

        event_id = _string_or_none(event.get("id"))
        if event_id is not None:
            if event_id in seen_ids:
                findings.append(
                    _finding(
                        "history_events.id_duplicate",
                        SEVERITY_ERROR,
                        f"history_events[{index}].id=[{event_id}] duplicates an earlier event.",
                        {"index": index, "event_id": event_id},
                    )
                )
            seen_ids.add(event_id)


def _check_commands(bundle: Mapping[str, Any], findings: list[dict[str, Any]]) -> None:
    commands = bundle.get("commands")
    if not isinstance(commands, list):
        return

    events = bundle.get("history_events") if isinstance(bundle.get("history_events"), list) else []
    event_command_ids: set[str] = set()
    for event in events:
        if isinstance(event, Mapping):
            event_command_id = _string_or_none(event.get("workflow_command_id"))
            if event_command_id is not None:
                event_command_ids.add(event_command_id)

    for index, command in enumerate(commands):
        if not isinstance(command, Mapping):
            continue

        command_id = _string_or_none(command.get("id"))
        if command_id is None:
            findings.append(
                _finding(
                    "commands.id_missing",
                    SEVERITY_ERROR,
                    f"commands[{index}].id must be a non-empty string.",
                    {"index": index},
                )
            )
            continue

        applied = _string_or_none(command.get("applied_at"))
        status = _string_or_none(command.get("status"))
        if applied is not None and event_command_ids and command_id not in event_command_ids:
            findings.append(
                _finding(
                    "commands.history_event_missing",
                    SEVERITY_WARNING,
                    (
                        f"commands[{index}] (id={command_id}, status={status or 'unknown'}) was applied "
                        "but no history event references it."
                    ),
                    {"index": index, "command_id": command_id, "command_status": status},
                )
            )


def _check_payload_manifest(bundle: Mapping[str, Any], findings: list[dict[str, Any]]) -> None:
    manifest = bundle.get("payload_manifest")
    if not isinstance(manifest, Mapping):
        return

    entries = manifest.get("entries")
    if not isinstance(entries, list):
        return

    for index, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            continue

        path = _string_or_none(entry.get("path")) or f"entries[{index}]"
        if not _string_or_none(entry.get("codec")):
            findings.append(
                _finding(
                    "payload_manifest.codec_missing",
                    SEVERITY_WARNING,
                    f"payload_manifest entry [{path}] does not declare a codec.",
                    {"path": path},
                )
            )

        available = bool(entry.get("available"))
        redacted = bool(entry.get("redacted"))
        diagnostic = _string_or_none(entry.get("diagnostic"))

        if not available and not redacted and diagnostic == "payload_missing":
            findings.append(
                _finding(
                    "payload_manifest.payload_missing",
                    SEVERITY_WARNING,
                    (
                        f"payload_manifest entry [{path}] is marked unavailable but flagged "
                        "as payload_missing — bundle decode will be lossy."
                    ),
                    {"path": path},
                )
            )

        if entry.get("codec") == "avro" and available:
            if not _string_or_none(entry.get("avro_framing")):
                findings.append(
                    _finding(
                        "payload_manifest.avro_framing_missing",
                        SEVERITY_WARNING,
                        f"payload_manifest entry [{path}] is codec=avro but missing avro_framing metadata.",
                        {"path": path},
                    )
                )

            declared = _string_or_none(entry.get("writer_schema_fingerprint"))
            writer = _string_or_none(entry.get("writer_schema"))
            if declared is not None and writer is not None:
                expected = "sha256:" + hashlib.sha256(writer.encode("utf-8")).hexdigest()
                if declared != expected:
                    findings.append(
                        _finding(
                            "payload_manifest.writer_schema_fingerprint_mismatch",
                            SEVERITY_ERROR,
                            (
                                f"payload_manifest entry [{path}] writer_schema_fingerprint does "
                                "not match the embedded writer_schema."
                            ),
                            {"path": path, "declared": declared, "expected": expected},
                        )
                    )


def _check_redaction(bundle: Mapping[str, Any], findings: list[dict[str, Any]]) -> None:
    redaction = bundle.get("redaction")
    if not isinstance(redaction, Mapping):
        return

    if redaction.get("applied") is True and not redaction.get("paths"):
        findings.append(
            _finding(
                "redaction.empty_paths",
                SEVERITY_WARNING,
                "redaction.applied=true but redaction.paths is empty — operators cannot tell what was redacted.",
            )
        )


def _check_integrity(
    bundle: Mapping[str, Any],
    signing_key: str | None,
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    integrity = bundle.get("integrity")
    if not isinstance(integrity, Mapping):
        findings.append(
            _finding("integrity.missing", SEVERITY_ERROR, "Bundle is missing the [integrity] block.")
        )
        return _integrity_report_for(None, None, None, None, None)

    canonicalization = _string_or_none(integrity.get("canonicalization"))
    checksum_algorithm = _string_or_none(integrity.get("checksum_algorithm"))
    checksum = _string_or_none(integrity.get("checksum"))
    signature_algorithm = _string_or_none(integrity.get("signature_algorithm"))
    signature = _string_or_none(integrity.get("signature"))
    key_id = _string_or_none(integrity.get("key_id"))

    if canonicalization is not None and canonicalization not in SUPPORTED_CANONICALIZATIONS:
        findings.append(
            _finding(
                "integrity.canonicalization_unsupported",
                SEVERITY_ERROR,
                (
                    f"integrity.canonicalization=[{canonicalization}] is not supported by this verifier "
                    f"(supported: {sorted(SUPPORTED_CANONICALIZATIONS)})."
                ),
                {"canonicalization": canonicalization},
            )
        )
        return _integrity_report_for(canonicalization, checksum_algorithm, checksum, signature_algorithm, key_id)

    if checksum_algorithm is not None and checksum_algorithm not in SUPPORTED_CHECKSUM_ALGORITHMS:
        findings.append(
            _finding(
                "integrity.checksum_algorithm_unsupported",
                SEVERITY_ERROR,
                (
                    f"integrity.checksum_algorithm=[{checksum_algorithm}] is not supported "
                    f"(supported: {sorted(SUPPORTED_CHECKSUM_ALGORITHMS)})."
                ),
                {"checksum_algorithm": checksum_algorithm},
            )
        )

    if checksum is None:
        findings.append(
            _finding("integrity.checksum_missing", SEVERITY_ERROR, "Bundle integrity.checksum is missing.")
        )

    try:
        canonical_json = _canonicalize(bundle)
    except (TypeError, ValueError) as exc:
        findings.append(
            _finding(
                "integrity.canonicalization_failed",
                SEVERITY_ERROR,
                f"Bundle could not be canonicalized: {exc}",
            )
        )
        return _integrity_report_for(canonicalization, checksum_algorithm, checksum, signature_algorithm, key_id)

    checksum_ok: bool | None = None
    if checksum_algorithm == "sha256" and checksum is not None:
        expected = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
        checksum_ok = hmac.compare_digest(expected, checksum)
        if not checksum_ok:
            findings.append(
                _finding(
                    "integrity.checksum_mismatch",
                    SEVERITY_ERROR,
                    (
                        f"integrity.checksum=[{checksum}] does not match recomputed sha256=[{expected}]. "
                        "Bundle has been tampered with or canonicalization disagrees."
                    ),
                    {"expected": expected, "observed": checksum},
                )
            )

    signature_ok: bool | None = None
    if signature_algorithm is not None:
        if signature_algorithm not in SUPPORTED_SIGNATURE_ALGORITHMS:
            findings.append(
                _finding(
                    "integrity.signature_algorithm_unsupported",
                    SEVERITY_ERROR,
                    (
                        f"integrity.signature_algorithm=[{signature_algorithm}] is not supported "
                        f"(supported: {sorted(SUPPORTED_SIGNATURE_ALGORITHMS)})."
                    ),
                    {"signature_algorithm": signature_algorithm},
                )
            )
        elif signature is None:
            findings.append(
                _finding(
                    "integrity.signature_missing",
                    SEVERITY_ERROR,
                    "Bundle declares a signature_algorithm but does not include a signature.",
                )
            )
        elif not signing_key:
            findings.append(
                _finding(
                    "integrity.signature_key_unavailable",
                    SEVERITY_WARNING,
                    "Bundle is signed but the verifier was not given a signing key — signature was not validated.",
                    {"key_id": key_id},
                )
            )
        else:
            expected = hmac.new(
                signing_key.encode("utf-8"),
                canonical_json.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            signature_ok = hmac.compare_digest(expected, signature)
            if not signature_ok:
                findings.append(
                    _finding(
                        "integrity.signature_mismatch",
                        SEVERITY_ERROR,
                        (
                            "integrity.signature does not match HMAC-SHA256 of the canonical bundle "
                            f"under the provided key (key_id={key_id or 'unknown'})."
                        ),
                        {"key_id": key_id},
                    )
                )

    report = _integrity_report_for(canonicalization, checksum_algorithm, checksum, signature_algorithm, key_id)
    report["checksum_ok"] = checksum_ok
    report["signature_ok"] = signature_ok
    report["signing_key_provided"] = bool(signing_key)
    return report


def _canonicalize(bundle: Mapping[str, Any]) -> str:
    cloned = {k: v for k, v in bundle.items() if k != "integrity"}
    canonical = _canonicalize_value(cloned)
    return json.dumps(canonical, separators=(",", ":"), ensure_ascii=False, sort_keys=False)


def _canonicalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize_value(item) for item in value]
    return value


def _finalize(
    findings: list[dict[str, Any]],
    bundle_summary: Mapping[str, Any],
    integrity_report: Mapping[str, Any],
) -> dict[str, Any]:
    summary = {"findings": len(findings), "errors": 0, "warnings": 0, "info": 0}
    for finding in findings:
        severity = finding.get("severity", SEVERITY_INFO)
        if severity == SEVERITY_ERROR:
            summary["errors"] += 1
        elif severity == SEVERITY_WARNING:
            summary["warnings"] += 1
        else:
            summary["info"] += 1

    if summary["errors"] > 0:
        status = STATUS_FAILED
    elif summary["warnings"] > 0:
        status = STATUS_WARNING
    else:
        status = STATUS_OK

    return {
        "schema": REPORT_SCHEMA,
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "bundle": dict(bundle_summary),
        "integrity": dict(integrity_report),
        "findings": list(findings),
        "summary": summary,
    }


def _finding(
    rule: str,
    severity: str,
    message: str,
    context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    finding: dict[str, Any] = {"rule": rule, "severity": severity, "message": message}
    if context:
        finding["context"] = dict(context)
    return finding


def _integrity_report_for(
    canonicalization: str | None,
    checksum_algorithm: str | None,
    checksum: str | None,
    signature_algorithm: str | None,
    key_id: str | None,
) -> dict[str, Any]:
    return {
        "canonicalization": canonicalization,
        "checksum_algorithm": checksum_algorithm,
        "checksum": checksum,
        "signature_algorithm": signature_algorithm,
        "signature_present": signature_algorithm is not None,
        "key_id": key_id,
        "checksum_ok": None,
        "signature_ok": None,
        "signing_key_provided": False,
    }


def _empty_bundle_summary() -> dict[str, Any]:
    return {
        "schema": None,
        "schema_version": None,
        "instance_id": None,
        "run_id": None,
        "history_event_count": 0,
    }


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m durable_workflow.history_bundle_verify",
        description=(
            "Verify a durable-workflow.v2.history-export bundle and emit "
            "the durable-workflow.v2.history-bundle-verification report."
        ),
    )
    parser.add_argument("bundle", help="Path to a history-export JSON bundle.")
    parser.add_argument("--signing-key", help="HMAC signing key for signature verification.")
    parser.add_argument("--output", help="Write the report to a file instead of stdout.")
    parser.add_argument(
        "--strict-warnings",
        action="store_true",
        help="Treat warning verdicts as a failing exit.",
    )
    args = parser.parse_args(argv)

    payload = Path(args.bundle).read_text(encoding="utf-8")
    report = verify_bundle_json(payload, args.signing_key)
    text = json.dumps(report, indent=2, sort_keys=True)

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)

    if report["status"] == STATUS_FAILED:
        return 1
    if report["status"] == STATUS_WARNING and args.strict_warnings:
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return _cli(argv)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())


__all__ = [
    "REPORT_SCHEMA",
    "REPORT_SCHEMA_VERSION",
    "BUNDLE_SCHEMA",
    "BUNDLE_SCHEMA_VERSION",
    "STATUS_OK",
    "STATUS_WARNING",
    "STATUS_FAILED",
    "SEVERITY_INFO",
    "SEVERITY_WARNING",
    "SEVERITY_ERROR",
    "verify_bundle",
    "verify_bundle_json",
    "main",
]
