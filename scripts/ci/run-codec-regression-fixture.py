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

    _require(protocol["codec"] == "avro", "fixture codec must be avro or a JSON rejection")
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
