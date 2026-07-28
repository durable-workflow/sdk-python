from __future__ import annotations

import base64
import io
import json
import math
from pathlib import Path

import pytest
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.schema import fingerprint, to_parsing_canonical_form

from durable_workflow import _avro


def test_canonical_schema_and_rabin_fingerprint() -> None:
    schema_path = (
        Path(__file__).parents[1]
        / "schema"
        / "durable_workflow.protocol.Value.v1.avsc"
    )
    assert schema_path.read_text().strip() == _avro.VALUE_SCHEMA_JSON
    canonical = to_parsing_canonical_form(json.loads(_avro.VALUE_SCHEMA_JSON))
    assert fingerprint(canonical, "CRC-64-AVRO") == _avro.VALUE_SCHEMA_FINGERPRINT_HEX


def test_every_branch_matches_cross_language_golden_bytes() -> None:
    fixture = json.loads(
        (Path(__file__).parents[1] / "schema" / "avro-value-v1-golden.json").read_text()
    )
    golden = {case["name"]: case["wire_base64"] for case in fixture["cases"]}
    cases = {
        "null": None,
        "boolean_false": False,
        "boolean_true": True,
        "long_min": -(2**63),
        "long_max": 2**63 - 1,
        "long_7": 7,
        "double_7": 7.0,
        "negative_zero": -0.0,
        "bytes_00ff": b"\x00\xff",
        "string_utf8": "héllo",
        "array": [None, True, 7, 7.0, b"\x00\xff", "text"],
        "map": {"a": 1, "b": [False]},
        "map_empty": {},
        "map_key_0": {"0": "zero"},
        "map_keys_0_1": {"0": "zero", "1": "one"},
        "nested": {"items": [{"enabled": True}, b"bytes", -2.5]},
    }
    for name, value in cases.items():
        blob = _avro.encode(value)
        assert blob == golden[name], name
        decoded = _avro.decode(blob)
        assert decoded == value, name
        assert _avro.encode(decoded) == blob, name
        assert base64.b64decode(blob)[:10].hex() == "c301e2a33dff55802237"


def test_shared_malformed_frames_are_rejected() -> None:
    fixture = json.loads(
        (Path(__file__).parents[1] / "schema" / "avro-value-v1-golden.json").read_text()
    )
    for case in fixture["malformed_frames"]:
        with pytest.raises(ValueError, match=case["error"]):
            _avro.decode(case["wire_base64"])


def test_shared_alternate_map_orders_decode_to_the_same_nested_value() -> None:
    fixture = json.loads(
        (Path(__file__).parents[1] / "schema" / "avro-value-v1-golden.json").read_text()
    )
    expected = {"outer": [{"left": 1, "right": b"x"}], "tail": "done"}

    for blob in fixture["alternate_map_orders"][0]["wire_base64"]:
        decoded = _avro.decode(blob)
        assert decoded == expected
        assert _avro.decode(_avro.encode(decoded)) == expected


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ({1: "one"}, "invalid_map_key"),
        (2**63, "integer_overflow"),
        (-(2**63) - 1, "integer_overflow"),
        (math.nan, "non_finite_float"),
        (math.inf, "non_finite_float"),
    ],
)
def test_rejects_value_policy_violations(value: object, reason: str) -> None:
    with pytest.raises((TypeError, ValueError), match=reason):
        _avro.encode(value)


def test_unknown_fingerprint_and_prerelease_wrapper_never_fall_back() -> None:
    raw = bytearray(base64.b64decode(_avro.encode(None)))
    raw[2] ^= 0xFF
    with pytest.raises(ValueError, match="unsupported_payload_schema"):
        _avro.decode(base64.b64encode(raw).decode())

    with pytest.raises(ValueError, match="invalid_payload_framing"):
        _avro.decode(base64.b64encode(b"\x00legacy-wrapper").decode())


def test_appended_named_branch_resolves_old_data_and_old_reader_rejects_new_branch() -> None:
    v1 = json.loads(_avro.VALUE_SCHEMA_JSON)
    v2 = json.loads(_avro.VALUE_SCHEMA_JSON)
    v2["fields"][0]["type"].append(
        {
            "type": "record",
            "name": "TimestampValue",
            "fields": [{"name": "timestamp", "type": "string"}],
        }
    )
    v1_schema = parse_schema(v1)
    v2_schema = parse_schema(v2)

    old_buffer = io.BytesIO()
    schemaless_writer(
        old_buffer,
        v1_schema,
        {
            "value": (
                "durable_workflow.protocol.LongValue",
                {"long": 7},
            )
        },
    )
    assert schemaless_reader(io.BytesIO(old_buffer.getvalue()), v1_schema, v2_schema) == {
        "value": {"long": 7}
    }

    new_buffer = io.BytesIO()
    schemaless_writer(
        new_buffer,
        v2_schema,
        {
            "value": (
                "durable_workflow.protocol.TimestampValue",
                {"timestamp": "2026-07-28T00:00:00Z"},
            )
        },
    )
    with pytest.raises(Exception, match="schema mismatch"):
        schemaless_reader(io.BytesIO(new_buffer.getvalue()), v2_schema, v1_schema)
