"""Fixed typed Avro Value protocol support.

The wire form is standard Avro single-object encoding:

    C3 01 || CRC-64-AVRO fingerprint || Avro datum

The immutable ``durable_workflow.protocol.Value`` schema preserves booleans,
signed 64-bit integers, finite doubles, bytes, UTF-8 strings, lists, and
string-keyed maps without workflow-specific schemas or a schema registry.
"""
from __future__ import annotations

import base64
import io
import json
import math
from functools import lru_cache
from typing import Any

from .errors import AvroNotInstalledError

VALUE_SCHEMA_JSON = (
    '{"type":"record","name":"Value","namespace":"durable_workflow.protocol",'
    '"fields":[{"name":"value","type":["null",'
    '{"type":"record","name":"BooleanValue","fields":[{"name":"boolean","type":"boolean"}]},'
    '{"type":"record","name":"LongValue","fields":[{"name":"long","type":"long"}]},'
    '{"type":"record","name":"DoubleValue","fields":[{"name":"double","type":"double"}]},'
    '{"type":"record","name":"BytesValue","fields":[{"name":"bytes","type":"bytes"}]},'
    '{"type":"record","name":"StringValue","fields":[{"name":"string","type":"string"}]},'
    '{"type":"record","name":"ArrayValue","fields":[{"name":"items",'
    '"type":{"type":"array","items":"Value"}}]},'
    '{"type":"record","name":"MapValue","fields":[{"name":"entries",'
    '"type":{"type":"map","values":"Value"}}]}]}]}'
)
VALUE_SCHEMA_FINGERPRINT_HEX = "e2a33dff55802237"
VALUE_SCHEMA_FINGERPRINT = bytes.fromhex(VALUE_SCHEMA_FINGERPRINT_HEX)
SINGLE_OBJECT_MAGIC = b"\xC3\x01"
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1


@lru_cache(maxsize=1)
def _load_avro_schema() -> Any:
    try:
        from fastavro import parse_schema
    except ImportError as exc:
        raise AvroNotInstalledError(
            "The 'fastavro' package is required for the Avro Value codec. "
            "Reinstall durable-workflow with its runtime dependencies."
        ) from exc

    return parse_schema(json.loads(VALUE_SCHEMA_JSON))


def encode(value: Any) -> str:
    """Encode one native value as a base64 Avro single-object payload."""
    return encode_many([value])[0]


def encode_many(values: list[Any]) -> list[str]:
    """Encode independent payloads while reusing the parsed production schema."""
    if not values:
        return []

    try:
        from fastavro import schemaless_writer
    except ImportError as exc:
        raise AvroNotInstalledError(
            "The 'fastavro' package is required for the Avro Value codec. "
            "Reinstall durable-workflow with its runtime dependencies."
        ) from exc

    schema = _load_avro_schema()
    encoded: list[str] = []
    for value in values:
        buffer = io.BytesIO()
        buffer.write(SINGLE_OBJECT_MAGIC)
        buffer.write(VALUE_SCHEMA_FINGERPRINT)
        schemaless_writer(buffer, schema, _to_datum(value))
        encoded.append(base64.b64encode(buffer.getvalue()).decode("ascii"))
    return encoded


def decode(blob: str) -> Any:
    """Decode one base64 Avro single-object Value payload."""
    return decode_many([blob])[0]


def decode_many(blobs: list[str]) -> list[Any]:
    """Decode independent payloads and resolve bundled writers to the current reader."""
    if not blobs:
        return []

    try:
        from fastavro import schemaless_reader
    except ImportError as exc:
        raise AvroNotInstalledError(
            "The 'fastavro' package is required for the Avro Value codec. "
            "Reinstall durable-workflow with its runtime dependencies."
        ) from exc

    current_reader = _load_avro_schema()
    decoded: list[Any] = []
    for blob in blobs:
        raw = _decode_base64(blob)
        if len(raw) < 10 or raw[:2] != SINGLE_OBJECT_MAGIC:
            raise ValueError(
                "invalid_payload_framing: expected Avro single-object magic c301."
            )

        fingerprint = raw[2:10]
        writer_schema = _schema_for_fingerprint(fingerprint)
        buffer = io.BytesIO(raw[10:])
        try:
            datum = schemaless_reader(buffer, writer_schema, current_reader)
        except EOFError as exc:
            raise ValueError("invalid_payload_framing: truncated Avro Value datum.") from exc
        except Exception as exc:
            raise ValueError(
                "invalid_payload_framing: malformed Avro Value datum."
            ) from exc
        if buffer.read(1):
            raise ValueError("invalid_payload_framing: trailing bytes after Avro Value datum.")
        decoded.append(_from_datum(datum))
    return decoded


def _to_datum(value: Any) -> dict[str, Any]:
    if value is None:
        return {"value": None}
    if isinstance(value, bool):
        return {
            "value": (
                "durable_workflow.protocol.BooleanValue",
                {"boolean": value},
            )
        }
    if isinstance(value, int):
        if value < _INT64_MIN or value > _INT64_MAX:
            raise ValueError(
                "integer_overflow: Avro Value long must be within signed 64-bit range."
            )
        return {
            "value": (
                "durable_workflow.protocol.LongValue",
                {"long": value},
            )
        }
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non_finite_float: Avro Value doubles must be finite.")
        return {
            "value": (
                "durable_workflow.protocol.DoubleValue",
                {"double": value},
            )
        }
    if isinstance(value, bytes):
        return {
            "value": (
                "durable_workflow.protocol.BytesValue",
                {"bytes": value},
            )
        }
    if isinstance(value, str):
        return {
            "value": (
                "durable_workflow.protocol.StringValue",
                {"string": value},
            )
        }
    if isinstance(value, list):
        return {
            "value": (
                "durable_workflow.protocol.ArrayValue",
                {"items": [_to_datum(item) for item in value]},
            )
        }
    if isinstance(value, dict):
        entries: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(
                    "invalid_map_key: Avro Value maps require string keys; "
                    "keys are never stringified."
                )
            entries[key] = _to_datum(item)
        return {
            "value": (
                "durable_workflow.protocol.MapValue",
                {"entries": entries},
            )
        }

    raise TypeError(
        f"unsupported_value_type: adapt {type(value).__name__} to a canonical "
        "Avro Value kind before encoding."
    )


def _from_datum(datum: Any) -> Any:
    if not isinstance(datum, dict) or "value" not in datum:
        raise ValueError(
            "invalid_payload_framing: datum is not a "
            "durable_workflow.protocol.Value record."
        )
    branch = datum["value"]
    if branch is None:
        return None
    if not isinstance(branch, dict):
        raise ValueError("invalid_payload_framing: invalid Value union branch.")
    if "boolean" in branch and type(branch["boolean"]) is bool:
        return branch["boolean"]
    if (
        "long" in branch
        and type(branch["long"]) is int
        and _INT64_MIN <= branch["long"] <= _INT64_MAX
    ):
        return branch["long"]
    if (
        "double" in branch
        and type(branch["double"]) is float
        and math.isfinite(branch["double"])
    ):
        return branch["double"]
    if "bytes" in branch and isinstance(branch["bytes"], bytes):
        return branch["bytes"]
    if "string" in branch and isinstance(branch["string"], str):
        return branch["string"]
    if "items" in branch and isinstance(branch["items"], list):
        return [_from_datum(item) for item in branch["items"]]
    if "entries" in branch and isinstance(branch["entries"], dict):
        return {key: _from_datum(item) for key, item in branch["entries"].items()}
    raise ValueError("invalid_payload_framing: unknown named Value branch.")


def _decode_base64(blob: str) -> bytes:
    try:
        raw = base64.b64decode(blob, validate=True)
    except (ValueError, TypeError) as exc:
        _diagnose_ingress(blob, exc)
    if not raw:
        raise ValueError("invalid_payload_framing: Avro payload is empty.")
    return raw


def _schema_for_fingerprint(fingerprint: bytes) -> Any:
    if fingerprint != VALUE_SCHEMA_FINGERPRINT:
        raise ValueError(
            "unsupported_payload_schema: unknown CRC-64-AVRO fingerprint "
            f"{fingerprint.hex()}."
        )
    return _load_avro_schema()


def _diagnose_ingress(blob: str, cause: Exception) -> None:
    stripped = blob.lstrip() if isinstance(blob, str) else ""
    looks_like_json = stripped[:1] in {"{", "[", '"', "-", "t", "f", "n"} or (
        stripped[:1].isdigit() if stripped else False
    )
    if looks_like_json:
        raise ValueError(
            "invalid_payload_framing: payload bytes look like JSON, not base64-encoded Avro. Use the "
            'explicit "json" codec or encode with the Avro Value codec.'
        ) from cause
    raise ValueError(
        "invalid_payload_framing: failed to base64-decode Avro payload bytes. Avro payloads must be "
        "strict base64 containing a c301 single-object frame."
    ) from cause
