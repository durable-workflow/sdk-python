#!/usr/bin/env python3
"""Validate immutable replay and payload-codec regression evidence."""

from __future__ import annotations

import argparse
import base64
import binascii
import fnmatch
import hashlib
import json
import math
import re
import subprocess
import sys
import tempfile
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

POLICY_SCHEMA = "durable-workflow.regression-corpus-policy/v1"
CODEC_SCHEMA = "durable-workflow.codec-regression/v1"
REPLAY_SCHEMA = "durable-workflow.replay-regression/v1"
GOLDEN_HISTORY_SCHEMA = "durable-workflow.golden-history.v1"
SUPPORTED_FORMATS = {
    "avro-value-golden-v1",
    "codec-regression-v1",
    "golden-history-v1",
    "replay-regression-v1",
}
SUPPORTED_CATEGORIES = {"codec", "replay"}
SUPPORTED_BINDINGS = {"php", "python", "rust"}
ZERO_COMMIT = re.compile(r"^0+$")


class CorpusError(RuntimeError):
    """The regression-corpus contract is not satisfied."""


@dataclass(frozen=True)
class Evidence:
    category: str
    identity: str
    path: str
    protocol_version: str
    semantic_digest: str
    duplicate_digests: tuple[str, ...]
    supersedes: tuple[str, ...] = ()


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _object(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CorpusError(f"{context} must be an object")
    return value


def _list(value: Any, context: str, *, nonempty: bool = False) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise CorpusError(f"{context} must be an array")
    if nonempty and not value:
        raise CorpusError(f"{context} must not be empty")
    return value


def _string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise CorpusError(f"{context} must be a non-empty string")
    return value


def _nullable_string(
    value: Any,
    context: str,
    *,
    allow_empty: bool = False,
) -> str | None:
    if value is None:
        return None
    if allow_empty and isinstance(value, str):
        return value
    return _string(value, context)


def _required_member(value: Mapping[str, Any], member: str, context: str) -> Any:
    if member not in value:
        raise CorpusError(f"{context}.{member} is required")
    return value[member]


def _unique_strings(value: Any, context: str, *, allowed: set[str] | None = None) -> tuple[str, ...]:
    values = tuple(_string(item, f"{context}[]") for item in _list(value, context, nonempty=True))
    if len(values) != len(set(values)):
        raise CorpusError(f"{context} contains duplicates")
    if allowed is not None and not set(values) <= allowed:
        raise CorpusError(f"{context} contains unsupported values: {sorted(set(values) - allowed)}")
    return values


def _json(content: bytes, path: str) -> Mapping[str, Any]:
    try:
        value = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CorpusError(f"{path} is not valid UTF-8 JSON: {error}") from error
    return _object(value, path)


def _canonical_base64(
    value: str,
    context: str,
) -> str:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise CorpusError(f"{context} is not canonical base64") from error
    canonical = base64.b64encode(decoded).decode("ascii")
    if value != canonical:
        raise CorpusError(f"{context} is not canonical base64")
    return canonical


def _canonical_wire_replacement(value: str) -> str | None:
    """Return the only permitted canonical replacement for a legacy wire."""

    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return None

    canonical = base64.b64encode(decoded).decode("ascii")
    return canonical if canonical != value else None


def _avro_golden_migration(base_content: bytes, current_content: bytes) -> bool:
    """Allow one-way repairs of legacy malformed-frame wire metadata."""

    try:
        base_document = json.loads(base_content)
        current_document = json.loads(current_content)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(base_document, dict) or not isinstance(current_document, dict):
        return False
    base_frames = base_document.get("malformed_frames")
    current_frames = current_document.get("malformed_frames")
    if not isinstance(base_frames, list) or not isinstance(current_frames, list):
        return False
    if len(base_frames) != len(current_frames):
        return False

    migrated = False
    for index, (base_frame, current_frame) in enumerate(
        zip(base_frames, current_frames, strict=True)
    ):
        if not isinstance(base_frame, dict) or not isinstance(current_frame, dict):
            return False
        base_wire = base_frame.get("wire_base64")
        current_wire = current_frame.get("wire_base64")
        if base_wire != current_wire:
            if not isinstance(base_wire, str) or not isinstance(current_wire, str):
                return False
            if current_wire != _canonical_wire_replacement(base_wire):
                return False
            try:
                _canonical_base64(
                    current_wire,
                    f"current.malformed_frames[{index}].wire_base64",
                )
            except CorpusError:
                return False
            base_frame["wire_base64"] = current_wire
            migrated = True

        base_name = base_frame.get("name")
        current_name = current_frame.get("name")
        if base_name != current_name:
            if (
                base_name != "invalid_base64"
                or current_name != "decoded_non_magic_bytes"
                or current_wire != "JSUl"
                or base_frame.get("error") != "invalid_payload_framing"
                or current_frame.get("error") != "invalid_payload_framing"
            ):
                return False
            base_frame["name"] = current_name
            migrated = True

    return migrated and base_document == current_document


def _canonical_command_type(value: str) -> str:
    """Normalize runtime command class names to their wire discriminator."""

    words = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", words).lower()


def _canonical_replay_command(value: Any) -> Any:
    """Normalize the command forms accepted by the replay consumer."""

    if not isinstance(value, Mapping):
        return value

    command = dict(value)
    command_type = command.get("command_type")
    if not isinstance(command_type, str) or not command_type:
        return command

    wire_type = _canonical_command_type(command_type)
    declared_type = command.get("type")
    if declared_type is None or declared_type == wire_type:
        command.pop("command_type")
        command["type"] = wire_type
    return command


def _canonical_replay_commands(value: Any) -> Any:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return value
    return [_canonical_replay_command(command) for command in value]


def _merge_replay_assertions(left: Any, right: Any, context: str) -> Any:
    """Merge two compatible partial assertions over the same replay output."""

    if isinstance(left, Mapping) and isinstance(right, Mapping):
        merged = dict(left)
        for key, value in right.items():
            if key in merged:
                merged[key] = _merge_replay_assertions(
                    merged[key],
                    value,
                    f"{context}.{key}",
                )
            else:
                merged[key] = value
        return merged

    if (
        isinstance(left, Sequence)
        and not isinstance(left, str | bytes)
        and isinstance(right, Sequence)
        and not isinstance(right, str | bytes)
    ):
        if len(left) != len(right):
            raise CorpusError(f"replay command assertions conflict at {context}")
        return [
            _merge_replay_assertions(left_item, right_item, f"{context}[{index}]")
            for index, (left_item, right_item) in enumerate(
                zip(left, right, strict=True)
            )
        ]

    if left != right:
        raise CorpusError(f"replay command assertions conflict at {context}")
    return left


def _canonical_executed_commands(
    command_sequence: Any,
    expected: Mapping[str, Any],
) -> Any:
    """Collapse every consumer-supported command assertion onto one output."""

    executed_commands = (
        _canonical_replay_commands(command_sequence)
        if command_sequence is not None
        else None
    )
    expected_sequence = expected.get("command_sequence")
    if expected_sequence is not None:
        canonical_expected = _canonical_replay_commands(expected_sequence)
        executed_commands = (
            canonical_expected
            if executed_commands is None
            else _merge_replay_assertions(
                executed_commands,
                canonical_expected,
                "command_sequence",
            )
        )

    first_command = {
        key: value
        for key, value in expected.items()
        if key != "command_sequence"
    }
    if first_command:
        canonical_first = _canonical_replay_command(first_command)
        if executed_commands is None:
            executed_commands = [canonical_first]
        elif (
            not isinstance(executed_commands, Sequence)
            or isinstance(executed_commands, str | bytes)
            or len(executed_commands) != 1
        ):
            raise CorpusError(
                "flattened expected command requires exactly one executed command"
            )
        else:
            executed_commands = [
                _merge_replay_assertions(
                    executed_commands[0],
                    canonical_first,
                    "command_sequence[0]",
                )
            ]

    return executed_commands


def _replay_semantic(
    *,
    workflow_type: str,
    workflow_input: Any,
    history: Any,
    command_sequence: Any,
    expected: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Project every replay representation onto consumer-executed values."""

    return {
        "workflow": {"type": workflow_type, "input": workflow_input},
        "history": history,
        "executed_commands": _canonical_executed_commands(
            command_sequence,
            expected,
        ),
    }


def _canonical_codec_version(codec: str, version: str, context: str) -> str:
    if codec == "avro" and re.fullmatch(r"[1-9][0-9]*", version) is None:
        raise CorpusError(f"{context} must be a canonical positive integer")
    return version


def _semantic_codec_value(
    value: Mapping[str, Any],
    context: str,
    *,
    wire_backed: bool,
) -> Mapping[str, Any]:
    """Normalize tagged codec values independently of their fixture format."""

    kind = _string(value.get("type"), f"{context}.type")
    if kind == "null":
        return {"type": kind}
    if kind == "boolean":
        raw_boolean = value.get("value")
        if not isinstance(raw_boolean, bool):
            raise CorpusError(f"{context}.value must be a boolean")
        return {"type": kind, "value": raw_boolean}
    if kind == "long":
        raw_long = value.get("value")
        if isinstance(raw_long, bool) or not isinstance(raw_long, int | str):
            raise CorpusError(f"{context}.value must be an integer string")
        try:
            parsed_long = int(raw_long)
        except ValueError as error:
            raise CorpusError(f"{context}.value must be an integer string") from error
        if not -(2**63) <= parsed_long < 2**63:
            raise CorpusError(f"{context}.value must fit a signed 64-bit integer")
        return {"type": kind, "value": str(parsed_long)}
    if kind == "double":
        raw_double = value.get("value")
        if isinstance(raw_double, bool) or not isinstance(raw_double, int | float | str):
            raise CorpusError(f"{context}.value must be a number or numeric string")
        try:
            parsed_double = float(raw_double)
        except ValueError as error:
            raise CorpusError(f"{context}.value must be a number or numeric string") from error
        if math.isnan(parsed_double):
            canonical_double = "nan"
        elif math.isinf(parsed_double):
            canonical_double = "-infinity" if parsed_double < 0 else "infinity"
        else:
            canonical_double = parsed_double.hex()
        return {"type": kind, "value": canonical_double}
    if kind == "bytes":
        aliases = [field for field in ("base64", "value_base64") if field in value]
        if not aliases:
            raise CorpusError(f"{context} must include base64 bytes")
        canonical_bytes: set[str] = set()
        for field in aliases:
            encoded = value[field]
            if not isinstance(encoded, str):
                raise CorpusError(f"{context}.{field} must be a string")
            normalized = _canonical_base64(encoded, f"{context}.{field}")
            if not isinstance(normalized, str):
                raise CorpusError(f"{context}.{field} must contain valid base64")
            canonical_bytes.add(normalized)
        if len(canonical_bytes) != 1:
            raise CorpusError(f"{context} contains conflicting base64 byte values")
        return {"type": kind, "base64": canonical_bytes.pop()}
    if kind == "string":
        raw_string = value.get("value")
        if not isinstance(raw_string, str):
            raise CorpusError(f"{context}.value must be a string")
        return {"type": kind, "value": raw_string}
    if kind == "array":
        if wire_backed:
            return {"type": kind}
        items = _list(value.get("items"), f"{context}.items")
        return {
            "type": kind,
            "items": [
                _semantic_codec_value(
                    _object(item, f"{context}.items[{index}]"),
                    f"{context}.items[{index}]",
                    wire_backed=False,
                )
                for index, item in enumerate(items)
            ],
        }
    if kind == "map":
        if wire_backed:
            return {"type": kind}
        entries = _list(value.get("entries"), f"{context}.entries")
        canonical_entries: dict[str, Mapping[str, Any]] = {}
        for index, raw_entry in enumerate(entries):
            entry_context = f"{context}.entries[{index}]"
            entry = _object(raw_entry, entry_context)
            key = entry.get("key")
            if not isinstance(key, str):
                raise CorpusError(f"{entry_context}.key must be a string")
            if key in canonical_entries:
                raise CorpusError(f"{context}.entries contains duplicate key {key!r}")
            canonical_entries[key] = _semantic_codec_value(
                _object(entry.get("value"), f"{entry_context}.value"),
                f"{entry_context}.value",
                wire_backed=False,
            )
        return {
            "type": kind,
            "entries": [
                {"key": key, "value": canonical_entries[key]}
                for key in sorted(canonical_entries)
            ],
        }
    raise CorpusError(f"{context}.type is unsupported")


def _codec_semantic(
    *,
    value: Mapping[str, Any] | None,
    wire: str | Mapping[str, str] | Sequence[str | Mapping[str, str]] | None,
    operation: str,
    error: str | None,
) -> Mapping[str, Any]:
    semantic: dict[str, Any] = {
        "value": value,
        "failure_policy": {"operation": operation, "error": error},
    }
    if operation != "encode_reject":
        semantic["wire"] = wire
    return semantic


def _fixture_evidence(
    *,
    category: str,
    identity: str,
    path: str,
    protocol_version: str,
    semantic_value: Any,
    duplicate_values: Sequence[Any] | None = None,
    supersedes: tuple[str, ...] = (),
) -> Evidence:
    duplicate_values = [semantic_value] if duplicate_values is None else duplicate_values
    return Evidence(
        category=category,
        identity=identity,
        path=path,
        protocol_version=protocol_version,
        semantic_digest=_canonical_digest(semantic_value),
        duplicate_digests=tuple(_canonical_digest(value) for value in duplicate_values),
        supersedes=supersedes,
    )


def _codec_fixture(document: Mapping[str, Any], path: str, binding: str | None) -> list[Evidence]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("fixture_schema") != CODEC_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={CODEC_SCHEMA}")
    identity = _string(document.get("id"), f"{path}.id")
    protocol = _object(document.get("protocol"), f"{path}.protocol")
    codec = _string(protocol.get("codec"), f"{path}.protocol.codec")
    _string(protocol.get("schema"), f"{path}.protocol.schema")
    version = _string(protocol.get("version"), f"{path}.protocol.version")
    canonical_version = _canonical_codec_version(
        codec,
        version,
        f"{path}.protocol.version",
    )
    _nullable_string(
        _required_member(protocol, "fingerprint", f"{path}.protocol"),
        f"{path}.protocol.fingerprint",
    )
    bindings = _unique_strings(
        document.get("bindings"),
        f"{path}.bindings",
        allowed=SUPPORTED_BINDINGS,
    )
    if binding is not None and binding not in bindings:
        raise CorpusError(f"{path} does not name this repository's {binding} binding")

    value = _object(document.get("value"), f"{path}.value")
    canonical_value = _semantic_codec_value(
        value,
        f"{path}.value",
        wire_backed=False,
    )
    framing = _object(document.get("framing"), f"{path}.framing")
    _string(framing.get("encoding"), f"{path}.framing.encoding")
    wire = _nullable_string(
        _required_member(framing, "wire_base64", f"{path}.framing"),
        f"{path}.framing.wire_base64",
        allow_empty=True,
    )
    policy = _object(document.get("failure_policy"), f"{path}.failure_policy")
    operation = _string(policy.get("operation"), f"{path}.failure_policy.operation")
    if operation not in {"round_trip", "decode_reject", "encode_reject"}:
        raise CorpusError(f"{path}.failure_policy.operation is unsupported")
    error = _nullable_string(
        _required_member(policy, "error", f"{path}.failure_policy"),
        f"{path}.failure_policy.error",
    )
    if operation in {"round_trip", "decode_reject"} and wire is None:
        raise CorpusError(f"{path} must include wire_base64 for {operation}")
    if operation == "round_trip" and error is not None:
        raise CorpusError(f"{path} round-trip evidence cannot declare an error")
    if operation != "round_trip" and error is None:
        raise CorpusError(f"{path} rejection evidence must declare its stable error policy")
    canonical_wire = (
        _canonical_base64(wire, f"{path}.framing.wire_base64")
        if wire is not None
        else None
    )

    supersedes = tuple(
        _string(item, f"{path}.supersedes[]")
        for item in _list(document.get("supersedes", []), f"{path}.supersedes")
    )
    if len(supersedes) != len(set(supersedes)) or identity in supersedes:
        raise CorpusError(f"{path}.supersedes is invalid")
    if operation == "round_trip" and canonical_value["type"] in {"array", "map"}:
        canonical_value = {"type": canonical_value["type"]}
    semantic = _codec_semantic(
        value=(
            canonical_value
            if operation in {"round_trip", "encode_reject"}
            else None
        ),
        wire=canonical_wire,
        operation=operation,
        error=error,
    )
    return [
        _fixture_evidence(
            category="codec",
            identity=identity,
            path=path,
            protocol_version=canonical_version,
            semantic_value=semantic,
            supersedes=supersedes,
        )
    ]


def _replay_fixture(document: Mapping[str, Any], path: str, binding: str | None) -> list[Evidence]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("fixture_schema") != REPLAY_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={REPLAY_SCHEMA}")
    identity = _string(document.get("id"), f"{path}.id")
    protocol_version = _string(document.get("protocol_version"), f"{path}.protocol_version")
    bindings = _unique_strings(
        document.get("bindings"),
        f"{path}.bindings",
        allowed=SUPPORTED_BINDINGS,
    )
    if binding is not None and binding not in bindings:
        raise CorpusError(f"{path} does not name this repository's {binding} binding")
    workflow = _object(document.get("workflow"), f"{path}.workflow")
    _string(workflow.get("type"), f"{path}.workflow.type")
    history = document.get("history")
    commands = document.get("command_sequence")
    if history is None and commands is None:
        raise CorpusError(f"{path} must include history or command_sequence")
    if history is not None:
        _list(history, f"{path}.history", nonempty=True)
    if commands is not None:
        _list(commands, f"{path}.command_sequence", nonempty=True)
    expected = _object(document.get("expected"), f"{path}.expected")
    if not expected:
        raise CorpusError(f"{path}.expected must not be empty")
    supersedes = tuple(
        _string(item, f"{path}.supersedes[]")
        for item in _list(document.get("supersedes", []), f"{path}.supersedes")
    )
    if len(supersedes) != len(set(supersedes)) or identity in supersedes:
        raise CorpusError(f"{path}.supersedes is invalid")
    semantic = _replay_semantic(
        workflow_type=workflow["type"],
        workflow_input=workflow.get("input", workflow.get("arguments", [])),
        history=history if history is not None else [],
        command_sequence=commands,
        expected=expected,
    )
    return [
        _fixture_evidence(
            category="replay",
            identity=identity,
            path=path,
            protocol_version=protocol_version,
            semantic_value=semantic,
            supersedes=supersedes,
        )
    ]


def _avro_golden_fixture(document: Mapping[str, Any], path: str) -> list[Evidence]:
    _string(document.get("schema"), f"{path}.schema")
    _string(document.get("fingerprint"), f"{path}.fingerprint")
    fixture_version = "avro-value-v1"
    protocol_version = "1"
    evidence: list[Evidence] = []
    sections = {
        "case": _list(document.get("cases"), f"{path}.cases", nonempty=True),
        "malformed": _list(document.get("malformed_frames"), f"{path}.malformed_frames", nonempty=True),
        "alternate": _list(document.get("alternate_map_orders"), f"{path}.alternate_map_orders", nonempty=True),
    }
    for section, entries in sections.items():
        for index, raw_entry in enumerate(entries):
            entry = _object(raw_entry, f"{path}.{section}[{index}]")
            name = _string(entry.get("name"), f"{path}.{section}[{index}].name")
            wire = entry.get("wire_base64")
            semantic_wires: list[str | Mapping[str, str]]
            semantic_value: Mapping[str, Any] | None = None
            if section == "alternate":
                wire_values = _unique_strings(
                    wire,
                    f"{path}.{section}[{index}].wire_base64",
                )
                semantic_wires = [
                    _canonical_base64(
                        wire_value,
                        f"{path}.{section}[{index}].wire_base64[]",
                    )
                    for wire_value in wire_values
                ]
                if len({_canonical_digest(value) for value in semantic_wires}) != len(
                    semantic_wires
                ):
                    raise CorpusError(
                        f"{path}.{section}[{index}].wire_base64 contains equivalent bytes"
                    )
                semantic_value = {"type": "map"}
            elif section == "case":
                wire_value = _string(wire, f"{path}.{section}[{index}].wire_base64")
                semantic_wires = [
                    _canonical_base64(
                        wire_value,
                        f"{path}.{section}[{index}].wire_base64",
                    )
                ]
                kind = _string(entry.get("kind"), f"{path}.{section}[{index}].kind")
                canonical_value: dict[str, Any] = {"type": kind}
                if "value" in entry:
                    canonical_value["value"] = entry["value"]
                if "value_base64" in entry:
                    canonical_value["value_base64"] = entry["value_base64"]
                semantic_value = _semantic_codec_value(
                    canonical_value,
                    f"{path}.{section}[{index}]",
                    wire_backed=True,
                )
            elif not isinstance(wire, str):
                raise CorpusError(f"{path}.{section}[{index}].wire_base64 must be a string")
            else:
                semantic_wires = [
                    _canonical_base64(
                        wire,
                        f"{path}.{section}[{index}].wire_base64",
                    )
                ]

            operation = "decode_reject" if section == "malformed" else "round_trip"
            error = (
                _string(entry.get("error"), f"{path}.{section}[{index}].error")
                if section == "malformed"
                else None
            )
            duplicate_values = [
                _codec_semantic(
                    value=semantic_value,
                    wire=semantic_wire,
                    operation=operation,
                    error=error,
                )
                for semantic_wire in semantic_wires
            ]
            semantic = (
                duplicate_values[0]
                if len(duplicate_values) == 1
                else {"equivalent_wire_encodings": duplicate_values}
            )
            evidence.append(
                _fixture_evidence(
                    category="codec",
                    identity=f"{fixture_version}:{section}:{name}",
                    path=path,
                    protocol_version=protocol_version,
                    semantic_value=semantic,
                    duplicate_values=duplicate_values,
                )
            )
    return evidence


def _golden_history_fixture(
    document: Mapping[str, Any],
    path: str,
    *,
    require_single_case: bool,
) -> list[Evidence]:
    if document.get("fixture_schema") != GOLDEN_HISTORY_SCHEMA:
        raise CorpusError(f"{path} must declare fixture_schema={GOLDEN_HISTORY_SCHEMA}")
    source = _object(document.get("source"), f"{path}.source")
    runtime = _string(source.get("runtime"), f"{path}.source.runtime")
    version = _string(source.get("version"), f"{path}.source.version")
    protocol_version = _string(
        source.get("worker_protocol_version"),
        f"{path}.source.worker_protocol_version",
    )
    cases = _list(document.get("cases"), f"{path}.cases", nonempty=True)
    if require_single_case and len(cases) != 1:
        raise CorpusError(f"new golden-history fixture {path} must contain exactly one minimal case")
    evidence: list[Evidence] = []
    for index, raw_case in enumerate(cases):
        case = _object(raw_case, f"{path}.cases[{index}]")
        name = _string(case.get("name"), f"{path}.cases[{index}].name")
        history = _list(case.get("history"), f"{path}.cases[{index}].history", nonempty=True)
        expected = case.get("expected", case.get("expected_state"))
        _object(expected, f"{path}.cases[{index}].expected")
        workflow_type = case.get("workflow_type", case.get("scenario"))
        _string(workflow_type, f"{path}.cases[{index}].workflow identity")
        semantic = _replay_semantic(
            workflow_type=workflow_type,
            workflow_input=case.get("start_input", []),
            history=history,
            command_sequence=case.get("command_sequence"),
            expected=expected,
        )
        evidence.append(
            _fixture_evidence(
                category="replay",
                identity=f"{runtime}@{version}:{name}",
                path=path,
                protocol_version=protocol_version,
                semantic_value=semantic,
            )
        )
    return evidence


def _run(command: Sequence[str], root: Path, *, check: bool = True) -> str:
    result = subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise CorpusError(f"{' '.join(command)} failed: {detail}")
    return result.stdout


def _run_codec_fixture(
    *,
    root: Path,
    python_executable: str,
    codec_runner: Path,
    source_root: Path,
    fixture: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            python_executable,
            str(codec_runner),
            "--source-root",
            str(source_root),
            "--fixture",
            str(fixture),
        ],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )


def _process_detail(result: subprocess.CompletedProcess[str]) -> str:
    return result.stderr.strip() or result.stdout.strip() or f"exit status {result.returncode}"


def _verify_new_codec_evidence(
    *,
    root: Path,
    base_files: Mapping[str, bytes],
    fixture_paths: Sequence[str],
    require_defective_base: bool,
    python_executable: str,
    codec_runner: Path,
) -> int:
    if not fixture_paths:
        if not require_defective_base:
            return 0
        raise CorpusError(
            "codec implementation changed but no newly added codec fixture can prove the defective revision"
        )
    if not codec_runner.is_file():
        raise CorpusError(f"official Python codec fixture runner is missing: {codec_runner}")

    for path in fixture_paths:
        fixture = root / path
        candidate = _run_codec_fixture(
            root=root,
            python_executable=python_executable,
            codec_runner=codec_runner,
            source_root=root,
            fixture=fixture,
        )
        if candidate.returncode != 0:
            raise CorpusError(
                f"new codec fixture {path} does not pass on the candidate "
                "through the official Python binding: "
                f"{_process_detail(candidate)}"
            )

    if not require_defective_base:
        return len(fixture_paths)

    with tempfile.TemporaryDirectory(prefix="sdk-python-codec-base-") as temporary:
        base_root = Path(temporary)
        source_files = {
            path: content for path, content in base_files.items() if Path(path).parts and Path(path).parts[0] == "src"
        }
        if not source_files:
            raise CorpusError("the base revision has no Python SDK source tree")
        for path, content in source_files.items():
            destination = base_root / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(content)

        for path in fixture_paths:
            fixture = root / path
            defective = _run_codec_fixture(
                root=root,
                python_executable=python_executable,
                codec_runner=codec_runner,
                source_root=base_root,
                fixture=fixture,
            )
            if defective.returncode == 0:
                raise CorpusError(
                    f"new codec fixture {path} also passes on the defective base; "
                    "it does not reproduce the guarded codec change"
                )

    return len(fixture_paths)


def _policy(document: Mapping[str, Any], path: str) -> Mapping[str, Any]:
    _string(document.get("$schema"), f"{path}.$schema")
    if document.get("schema") != POLICY_SCHEMA:
        raise CorpusError(f"{path} must declare schema={POLICY_SCHEMA}")
    _string(document.get("repository"), f"{path}.repository")
    binding = document.get("binding")
    if binding is not None and binding not in SUPPORTED_BINDINGS:
        raise CorpusError(f"{path}.binding is unsupported")
    categories = _object(document.get("categories"), f"{path}.categories")
    if not categories or not set(categories) <= SUPPORTED_CATEGORIES:
        raise CorpusError(f"{path}.categories must contain only replay and/or codec")
    for name, raw_category in categories.items():
        category = _object(raw_category, f"{path}.categories.{name}")
        fixtures = _list(category.get("fixtures"), f"{path}.categories.{name}.fixtures", nonempty=True)
        for index, raw_fixture in enumerate(fixtures):
            fixture = _object(raw_fixture, f"{path}.categories.{name}.fixtures[{index}]")
            _string(fixture.get("glob"), f"{path}.categories.{name}.fixtures[{index}].glob")
            fixture_format = _string(
                fixture.get("format"),
                f"{path}.categories.{name}.fixtures[{index}].format",
            )
            if fixture_format not in SUPPORTED_FORMATS:
                raise CorpusError(f"{path}.categories.{name}.fixtures[{index}].format is unsupported")
            if not fixture_format.startswith(name) and not (
                name == "codec" and fixture_format == "avro-value-golden-v1"
            ) and not (name == "replay" and fixture_format == "golden-history-v1"):
                raise CorpusError(f"{path}.categories.{name} contains a fixture for another category")
        guards = _list(category.get("guards"), f"{path}.categories.{name}.guards", nonempty=True)
        for index, raw_guard in enumerate(guards):
            guard = _object(raw_guard, f"{path}.categories.{name}.guards[{index}]")
            _string(guard.get("glob"), f"{path}.categories.{name}.guards[{index}].glob")
            patterns = guard.get("content_patterns")
            if patterns is not None:
                for pattern in _unique_strings(
                    patterns,
                    f"{path}.categories.{name}.guards[{index}].content_patterns",
                ):
                    try:
                        re.compile(pattern)
                    except re.error as error:
                        raise CorpusError(f"invalid guard regex {pattern!r}: {error}") from error
    return document


def _validate_policy_evolution(
    base_policy: Mapping[str, Any],
    current_policy: Mapping[str, Any],
) -> None:
    for field in ("repository", "binding"):
        if current_policy.get(field) != base_policy.get(field):
            raise CorpusError(f"policy {field} cannot change")

    base_categories = _object(base_policy["categories"], "base categories")
    current_categories = _object(current_policy["categories"], "current categories")
    for category_name, raw_base_category in base_categories.items():
        if category_name not in current_categories:
            raise CorpusError(f"{category_name} policy category was removed")
        base_category = _object(raw_base_category, f"base categories.{category_name}")
        current_category = _object(
            current_categories[category_name],
            f"current categories.{category_name}",
        )

        current_fixtures = {
            (
                _string(_object(raw_fixture, "fixture")["glob"], "fixture.glob"),
                _string(_object(raw_fixture, "fixture")["format"], "fixture.format"),
            )
            for raw_fixture in _list(
                current_category["fixtures"],
                f"current categories.{category_name}.fixtures",
            )
        }
        for raw_fixture in _list(
            base_category["fixtures"],
            f"base categories.{category_name}.fixtures",
        ):
            fixture = _object(raw_fixture, "base fixture")
            selector = (
                _string(fixture["glob"], "base fixture.glob"),
                _string(fixture["format"], "base fixture.format"),
            )
            if selector not in current_fixtures:
                raise CorpusError(
                    f"{category_name} fixture selector was removed or narrowed: "
                    f"glob={selector[0]!r}, format={selector[1]!r}; "
                    "preserve existing selectors and add a new selector for expansions"
                )

        current_guards_by_glob: dict[str, list[Mapping[str, Any]]] = {}
        for raw_guard in _list(
            current_category["guards"],
            f"current categories.{category_name}.guards",
        ):
            guard = _object(raw_guard, "current guard")
            glob = _string(guard["glob"], "current guard.glob")
            current_guards_by_glob.setdefault(glob, []).append(guard)
        for raw_guard in _list(
            base_category["guards"],
            f"base categories.{category_name}.guards",
        ):
            base_guard = _object(raw_guard, "base guard")
            glob = _string(base_guard["glob"], "base guard.glob")
            candidate_guards = current_guards_by_glob.get(glob, [])
            base_patterns = base_guard.get("content_patterns")
            if base_patterns is None:
                covered = any(guard.get("content_patterns") is None for guard in candidate_guards)
            else:
                required_patterns = set(_unique_strings(base_patterns, "base guard.content_patterns"))
                covered = any(guard.get("content_patterns") is None for guard in candidate_guards)
                if not covered:
                    candidate_patterns = {
                        pattern
                        for guard in candidate_guards
                        for pattern in _unique_strings(
                            guard.get("content_patterns"),
                            "current guard.content_patterns",
                        )
                    }
                    covered = required_patterns <= candidate_patterns
            if not covered:
                raise CorpusError(
                    f"{category_name} guard selector was removed or narrowed: glob={glob!r}; "
                    "preserve its glob and content patterns when adding broader guards"
                )


def _tracked_worktree_files(root: Path) -> dict[str, bytes]:
    paths = _run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        root,
    ).split("\0")
    return {
        path: (root / path).read_bytes()
        for path in paths
        if path and (root / path).is_file()
    }


def _ref_files(root: Path, ref: str) -> dict[str, bytes]:
    paths = _run(["git", "ls-tree", "-r", "--name-only", "-z", ref], root).split("\0")
    return {
        path: _run(["git", "show", f"{ref}:{path}"], root).encode()
        for path in paths
        if path
    }


def _matches(path: str, pattern: str) -> bool:
    return fnmatch.fnmatchcase(path, pattern)


def _inventory(
    policy: Mapping[str, Any],
    files: Mapping[str, bytes],
    *,
    new_paths: set[str] | None = None,
) -> list[Evidence]:
    binding = policy.get("binding")
    evidence: list[Evidence] = []
    selected_paths: set[str] = set()
    for category_name, raw_category in _object(policy["categories"], "categories").items():
        category = _object(raw_category, f"categories.{category_name}")
        for raw_fixture in _list(category["fixtures"], f"categories.{category_name}.fixtures"):
            fixture = _object(raw_fixture, f"categories.{category_name}.fixtures[]")
            pattern = _string(fixture["glob"], "fixture.glob")
            fixture_format = _string(fixture["format"], "fixture.format")
            for path in sorted(candidate for candidate in files if _matches(candidate, pattern)):
                if path in selected_paths:
                    raise CorpusError(f"fixture path {path} is selected more than once")
                selected_paths.add(path)
                document = _json(files[path], path)
                if fixture_format == "codec-regression-v1":
                    parsed = _codec_fixture(document, path, binding if isinstance(binding, str) else None)
                elif fixture_format == "replay-regression-v1":
                    parsed = _replay_fixture(document, path, binding if isinstance(binding, str) else None)
                elif fixture_format == "avro-value-golden-v1":
                    parsed = _avro_golden_fixture(document, path)
                else:
                    parsed = _golden_history_fixture(
                        document,
                        path,
                        require_single_case=new_paths is not None and path in new_paths,
                    )
                if any(item.category != category_name for item in parsed):
                    raise CorpusError(f"{path} produced evidence for the wrong category")
                evidence.extend(parsed)

    identities = Counter(item.identity for item in evidence)
    repeated_identities = sorted(identity for identity, count in identities.items() if count > 1)
    if repeated_identities:
        raise CorpusError(f"duplicate fixture identities: {repeated_identities}")
    semantic_owners: dict[tuple[str, str], list[Evidence]] = {}
    for item in evidence:
        for digest in set(item.duplicate_digests):
            semantic_owners.setdefault((item.category, digest), []).append(item)
    duplicate_semantics = sorted(
        key for key, owners in semantic_owners.items() if len(owners) > 1
    )
    if duplicate_semantics:
        paths = {
            key: sorted(item.path for item in semantic_owners[key])
            for key in duplicate_semantics
        }
        raise CorpusError(f"duplicate semantic fixtures: {paths}")
    return evidence


def _fixture_paths(policy: Mapping[str, Any], files: Mapping[str, bytes]) -> set[str]:
    return {
        path
        for raw_category in _object(policy["categories"], "categories").values()
        for raw_fixture in _list(
            _object(raw_category, "category")["fixtures"],
            "category.fixtures",
        )
        for path in files
        if _matches(path, _string(_object(raw_fixture, "fixture")["glob"], "fixture.glob"))
    }


def _changed_paths(root: Path, base_ref: str) -> tuple[set[str], set[str]]:
    output = _run(["git", "diff", "--name-status", "--find-renames", base_ref, "--"], root)
    changed: set[str] = set()
    added: set[str] = set()
    for line in output.splitlines():
        parts = line.split("\t")
        status = parts[0]
        paths = parts[1:]
        if not paths:
            continue
        changed.update(paths)
        if status.startswith("A"):
            added.add(paths[-1])
    untracked = {
        path
        for path in _run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            root,
        ).splitlines()
        if path
    }
    return changed | untracked, added | untracked


def _guard_matches(
    root: Path,
    base_ref: str,
    changed: set[str],
    raw_guard: Any,
) -> bool:
    guard = _object(raw_guard, "guard")
    matching = sorted(path for path in changed if _matches(path, _string(guard["glob"], "guard.glob")))
    if not matching:
        return False
    patterns = guard.get("content_patterns")
    if patterns is None:
        return True
    diff = _run(
        [
            "git",
            "diff",
            "--function-context",
            "--no-ext-diff",
            "--no-color",
            base_ref,
            "--",
            *matching,
        ],
        root,
    )
    untracked = set(_run(["git", "ls-files", "--others", "--exclude-standard"], root).splitlines())
    context_lines: list[str] = []
    inside_hunk = False
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            inside_hunk = False
        elif line.startswith("@@"):
            inside_hunk = True
        elif inside_hunk and line.startswith((" ", "+", "-")):
            context_lines.append(line[1:])
    for path in matching:
        if path in untracked and (root / path).is_file():
            context_lines.append((root / path).read_text(encoding="utf-8", errors="replace"))
    changed_context = "\n".join(context_lines)
    return any(re.search(pattern, changed_context) for pattern in patterns)


def validate(
    root: Path,
    policy_path: Path,
    base_ref: str | None,
    *,
    python_executable: str = sys.executable,
    codec_runner_path: Path = Path("scripts/ci/run-codec-regression-fixture.py"),
) -> dict[str, Any]:
    policy_file = policy_path if policy_path.is_absolute() else root / policy_path
    codec_runner = codec_runner_path if codec_runner_path.is_absolute() else root / codec_runner_path
    policy = _policy(_json(policy_file.read_bytes(), str(policy_path)), str(policy_path))
    current_files = _tracked_worktree_files(root)
    changed: set[str] = set()
    added_paths: set[str] = set()
    base_files: dict[str, bytes] = {}
    base_evidence: list[Evidence] = []
    if base_ref and not ZERO_COMMIT.fullmatch(base_ref):
        _run(["git", "rev-parse", "--verify", f"{base_ref}^{{commit}}"], root)
        changed, added_paths = _changed_paths(root, base_ref)
        base_files = _ref_files(root, base_ref)
        try:
            policy_relative_path = policy_file.resolve().relative_to(root.resolve()).as_posix()
        except ValueError as error:
            raise CorpusError("policy must be inside the repository when validating a base ref") from error
        if policy_relative_path in base_files:
            base_policy_context = f"{base_ref}:{policy_relative_path}"
            base_policy = _policy(
                _json(base_files[policy_relative_path], base_policy_context),
                base_policy_context,
            )
            _validate_policy_evolution(base_policy, policy)
            for path in _fixture_paths(base_policy, base_files):
                current_content = current_files.get(path)
                if (
                    current_content != base_files[path]
                    and current_content is not None
                    and _avro_golden_migration(base_files[path], current_content)
                ):
                    base_files[path] = current_content
                    continue
                if current_content != base_files[path]:
                    raise CorpusError(f"immutable fixture file {path} was changed, moved, or removed")
            base_evidence = _inventory(base_policy, base_files)
    current_evidence = _inventory(policy, current_files, new_paths=added_paths)

    current_by_id = {item.identity: item for item in current_evidence}
    base_by_id = {item.identity: item for item in base_evidence}
    for identity, previous in base_by_id.items():
        current = current_by_id.get(identity)
        if current is None:
            raise CorpusError(f"immutable fixture {identity} was removed")
        if current.path != previous.path or current.semantic_digest != previous.semantic_digest:
            raise CorpusError(f"immutable fixture {identity} was changed; append a superseding fixture instead")
    for item in current_evidence:
        for superseded in item.supersedes:
            previous = current_by_id.get(superseded)
            if previous is None:
                raise CorpusError(f"{item.identity} supersedes unknown fixture {superseded}")
            if previous.category != item.category or previous.protocol_version == item.protocol_version:
                raise CorpusError(
                    f"{item.identity} must supersede evidence in the same category at an older protocol version"
                )

    counts: dict[str, dict[str, int | bool]] = {}
    for category_name, raw_category in _object(policy["categories"], "categories").items():
        category = _object(raw_category, f"categories.{category_name}")
        current_count = sum(item.category == category_name for item in current_evidence)
        base_count = sum(item.category == category_name for item in base_evidence)
        added_count = sum(item.category == category_name and item.path in added_paths for item in current_evidence)
        related = False
        if base_ref and not ZERO_COMMIT.fullmatch(base_ref):
            related = any(
                _guard_matches(root, base_ref, changed, guard)
                for guard in _list(category["guards"], f"categories.{category_name}.guards")
            )
            if related and added_count == 0:
                raise CorpusError(
                    f"{category_name} implementation changed but its corpus has no evidence "
                    "on a newly added fixture path "
                    f"(base={base_count}, current={current_count})"
                )
            if related and current_count <= base_count:
                raise CorpusError(
                    f"{category_name} implementation changed but its corpus did not grow "
                    f"(base={base_count}, current={current_count})"
                )
        revision_verified = 0
        if category_name == "codec":
            codec_fixture_patterns: list[str] = []
            for raw_fixture in _list(category["fixtures"], "categories.codec.fixtures"):
                fixture = _object(raw_fixture, "categories.codec.fixtures[]")
                if _string(fixture["format"], "fixture.format") == "codec-regression-v1":
                    codec_fixture_patterns.append(_string(fixture["glob"], "fixture.glob"))
            new_codec_fixture_paths = sorted(
                {
                    item.path
                    for item in current_evidence
                    if item.category == "codec"
                    and item.path in added_paths
                    and any(_matches(item.path, pattern) for pattern in codec_fixture_patterns)
                }
            )
            revision_verified = _verify_new_codec_evidence(
                root=root,
                base_files=base_files,
                fixture_paths=new_codec_fixture_paths,
                require_defective_base=related,
                python_executable=python_executable,
                codec_runner=codec_runner,
            )
        count = {
            "base": base_count,
            "current": current_count,
            "added": added_count,
            "related_change": related,
        }
        if category_name == "codec":
            count["revision_verified"] = revision_verified
        counts[category_name] = count
    return {
        "schema": POLICY_SCHEMA,
        "repository": policy["repository"],
        "base_ref": base_ref,
        "changed_paths": len(changed),
        "counts": counts,
        "status": "pass",
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--policy", type=Path, default=Path("regression-corpus-policy.json"))
    parser.add_argument("--base-ref")
    parser.add_argument("--python-executable", default=sys.executable)
    parser.add_argument(
        "--codec-runner",
        type=Path,
        default=Path("scripts/ci/run-codec-regression-fixture.py"),
    )
    args = parser.parse_args(argv)
    try:
        result = validate(
            args.root.resolve(),
            args.policy,
            args.base_ref,
            python_executable=args.python_executable,
            codec_runner_path=args.codec_runner,
        )
    except (CorpusError, OSError) as error:
        print(f"regression corpus validation failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
