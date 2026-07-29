from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import _avro

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "codec_regressions"


def _fixtures() -> list[dict[str, Any]]:
    paths = sorted(FIXTURE_DIR.glob("*.json"))
    assert paths, f"expected codec regression fixtures in {FIXTURE_DIR}"
    return [json.loads(path.read_text()) for path in paths]


def _tagged_value(value: dict[str, Any]) -> object:
    return {
        "null": lambda: None,
        "boolean": lambda: bool(value["value"]),
        "long": lambda: int(value["value"]),
        "double": lambda: float(value["value"]),
        "bytes": lambda: base64.b64decode(value["base64"]),
        "string": lambda: str(value["value"]),
        "array": lambda: [_tagged_value(item) for item in value["items"]],
        "map": lambda: {
            str(entry["key"]): _tagged_value(entry["value"])
            for entry in value["entries"]
        },
    }[value["type"]]()


@pytest.mark.parametrize("fixture", _fixtures(), ids=lambda fixture: str(fixture["id"]))
def test_checked_in_codec_regression_corpus_uses_fastavro(fixture: dict[str, Any]) -> None:
    assert fixture["fixture_schema"] == "durable-workflow.codec-regression/v1"
    assert "python" in fixture["bindings"]
    assert fixture["protocol"]["fingerprint"] == _avro.VALUE_SCHEMA_FINGERPRINT_HEX

    value = _tagged_value(fixture["value"])
    wire = fixture["framing"]["wire_base64"]
    operation = fixture["failure_policy"]["operation"]
    error = fixture["failure_policy"]["error"]

    if operation == "round_trip":
        assert _avro.encode(value) == wire
        decoded = _avro.decode(wire)
        assert decoded == value
        assert _avro.encode(decoded) == wire
        return

    with pytest.raises((TypeError, ValueError), match=error):
        if operation == "decode_reject":
            _avro.decode(wire)
        elif operation == "encode_reject":
            _avro.encode(value)
        else:
            raise AssertionError(f"unsupported failure policy {operation}")
