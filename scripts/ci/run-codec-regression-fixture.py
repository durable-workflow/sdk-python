#!/usr/bin/env python3
"""Execute one codec-regression fixture through the Python Avro binding."""

from __future__ import annotations

import argparse
import base64
import importlib
import json
import re
import sys
from pathlib import Path
from typing import Any

FIXTURE_SCHEMA = "durable-workflow.codec-regression/v1"
AVRO_PROTOCOL_VERSION = "1"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _tagged_value(value: dict[str, Any]) -> object:
    return {
        "null": lambda: None,
        "boolean": lambda: bool(value["value"]),
        "long": lambda: int(value["value"]),
        "double": lambda: float(value["value"]),
        "bytes": lambda: base64.b64decode(value["base64"], validate=True),
        "string": lambda: str(value["value"]),
        "array": lambda: [_tagged_value(item) for item in value["items"]],
        "map": lambda: {str(entry["key"]): _tagged_value(entry["value"]) for entry in value["entries"]},
    }[value["type"]]()


def execute_fixture(fixture: dict[str, Any], codec: Any) -> None:
    """Assert one fixture's declared policy against a loaded codec module."""

    _require(
        fixture["fixture_schema"] == FIXTURE_SCHEMA,
        f"fixture_schema must be {FIXTURE_SCHEMA}",
    )
    _require("python" in fixture["bindings"], "fixture must name the python binding")
    protocol = fixture["protocol"]
    if protocol["codec"] == "batch-envelope":
        from durable_workflow import serializer

        wire = base64.b64decode(fixture["framing"]["wire_base64"], validate=True).decode("utf-8")
        matrix = json.loads(wire)
        _require(
            matrix == _tagged_value(fixture["value"]),
            "batch envelope fixture value does not match its wire bytes",
        )
        _require(
            fixture["failure_policy"]
            == {
                "operation": "decode_reject",
                "error": "unsupported_payload_codec",
            },
            "batch envelope fixture must declare the stable codec rejection",
        )
        for item_codec in matrix["item_codecs"]:
            for blob_state in matrix["blob_states"]:
                item = {"codec": item_codec}
                if blob_state == "null":
                    item["blob"] = None
                for fallback_name in matrix["fallback_codecs"]:
                    fallback_codec = None if fallback_name == "absent" else fallback_name
                    for external_storage_enabled in matrix["external_storage"]:
                        try:
                            serializer.decode_envelopes(
                                [item],
                                codec=fallback_codec,
                                external_storage=object() if external_storage_enabled else None,
                            )
                        except ValueError as caught:
                            _require(
                                "unsupported_payload_codec" in str(caught),
                                "batch envelope rejection is not actionable",
                            )
                            _require(
                                repr(item_codec) in str(caught),
                                "batch envelope rejection did not preserve item codec precedence",
                            )
                            continue
                        raise AssertionError("unsupported batch item payload codec did not fail closed")
        return

    if protocol["codec"] == "task-root":
        from durable_workflow.worker import _validate_payload_codec

        wire = base64.b64decode(fixture["framing"]["wire_base64"], validate=True).decode("utf-8")
        tasks = json.loads(wire)
        _require(tasks == _tagged_value(fixture["value"]), "task fixture value does not match its wire bytes")
        _require(
            fixture["failure_policy"]
            == {
                "operation": "decode_reject",
                "error": "unsupported_payload_codec",
            },
            "task-root fixture must declare the stable codec rejection",
        )
        _require(isinstance(tasks, list) and tasks, "task-root fixture must contain rejected tasks")
        for task in tasks:
            _require(isinstance(task, dict), "task-root fixture entries must be task objects")
            try:
                _validate_payload_codec(task.get("payload_codec"))
            except ValueError as caught:
                _require("unsupported_payload_codec" in str(caught), "task-root rejection is not actionable")
                continue
            raise AssertionError("non-exact root task payload codec did not fail closed")
        return

    if protocol["codec"] == "json":
        from durable_workflow import serializer

        wire = base64.b64decode(fixture["framing"]["wire_base64"], validate=True).decode("utf-8")
        blob = None if fixture["value"]["type"] == "null" else wire
        try:
            serializer.decode_envelope({"codec": "json", "blob": blob})
        except ValueError as caught:
            _require(
                fixture["failure_policy"]
                == {
                    "operation": "decode_reject",
                    "error": "unsupported_payload_codec",
                },
                "JSON fixture must declare the stable decode rejection",
            )
            _require("unsupported_payload_codec" in str(caught), "JSON rejection is not actionable")
            return
        raise AssertionError("json-tagged workflow payload did not fail closed")

    if protocol["codec"] == "runtime-external-payload":
        from durable_workflow import serializer

        wire = base64.b64decode(fixture["framing"]["wire_base64"], validate=True).decode("utf-8")
        envelope = json.loads(wire)
        _require(
            envelope == _tagged_value(fixture["value"]),
            "runtime external payload fixture value does not match its wire bytes",
        )
        _require(
            fixture["failure_policy"]
            == {
                "operation": "decode_reject",
                "error": "external_payload_unsupported",
            },
            "runtime external payload fixture must declare the stable unresolved-reference rejection",
        )
        try:
            serializer.decode_envelope(envelope)
        except Exception as caught:
            _require(
                type(caught).__name__ == "ExternalPayloadUnsupported",
                "unresolved runtime reference did not raise the typed transport failure",
            )
            _require(
                getattr(caught, "reason", None) == fixture["failure_policy"]["error"],
                "unresolved runtime reference did not expose its stable failure reason",
            )
            _require(
                getattr(caught, "retryable", None) is False,
                "unresolved runtime reference rejection must not be retryable",
            )
            _require(
                "resolved by Client" in str(caught),
                "unresolved runtime reference rejection is not actionable",
            )
            return
        raise AssertionError("unresolved runtime external payload reference reached the Avro decoder")

    _require(
        protocol["codec"] == "avro",
        "fixture codec must be avro, batch-envelope, task-root, runtime-external-payload, or a JSON rejection",
    )
    _require(
        protocol["version"] == AVRO_PROTOCOL_VERSION,
        "fixture protocol.version must be the canonical version supported by "
        f"the Python Avro binding: {AVRO_PROTOCOL_VERSION}",
    )
    _require(
        protocol["fingerprint"] == codec.VALUE_SCHEMA_FINGERPRINT_HEX,
        "fixture fingerprint does not match the Python binding",
    )

    value = _tagged_value(fixture["value"])
    wire = fixture["framing"]["wire_base64"]
    operation = fixture["failure_policy"]["operation"]
    error = fixture["failure_policy"]["error"]

    if operation == "round_trip":
        _require(codec.encode(value) == wire, "encoded wire bytes do not match the fixture")
        decoded = codec.decode(wire)
        _require(decoded == value, "decoded value does not match the fixture")
        _require(codec.encode(decoded) == wire, "re-encoded wire bytes do not match the fixture")
        return

    try:
        if operation == "decode_reject":
            codec.decode(wire)
        elif operation == "encode_reject":
            codec.encode(value)
        else:
            raise AssertionError(f"unsupported failure policy {operation}")
    except (TypeError, ValueError) as caught:
        if not isinstance(error, str) or re.search(error, str(caught)) is None:
            raise AssertionError(f"{operation} raised {caught!r}, which does not match {error!r}") from caught
        return
    raise AssertionError(f"{operation} did not raise {error!r}")


def _load_codec(source_root: Path) -> Any:
    source = source_root.resolve() / "src"
    if not source.is_dir():
        raise RuntimeError(f"Python SDK source tree is missing: {source}")
    sys.path.insert(0, str(source))
    return importlib.import_module("durable_workflow._avro")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()

    try:
        fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
        if not isinstance(fixture, dict):
            raise TypeError("fixture must be a JSON object")
        execute_fixture(fixture, _load_codec(args.source_root))
    except Exception as error:
        print(f"codec regression fixture failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
