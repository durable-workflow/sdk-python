"""Avro codec support for the Durable Workflow Python SDK.

The Durable Workflow server uses an Avro generic-wrapper format on the
wire when the ``payload_codec`` tag is ``"avro"``.  The wire layout is:

    base64( 0x00 || avro_binary( record{ json: string, version: int } ) )

The ``json`` field carries ``json.dumps(value)``; ``version`` is currently
``1``. That means the generic wrapper preserves only JSON-native shapes:
``None``, booleans, numbers, strings, lists, and mappings with string keys.
Class identity is not carried on the wire. ``OrderedDict`` decodes as a plain
``dict``; ``IntEnum`` decodes as ``int``; and ``StrEnum`` decodes as ``str``.
Objects that the standard library JSON encoder does not know how to encode,
including dataclasses, attrs classes, pydantic models, pendulum values,
``datetime`` / ``date`` / ``time``, ``uuid.UUID``, ``decimal.Decimal``, and
plain ``Enum`` values, raise ``TypeError`` during encode. Convert those values
to explicit JSON-native dictionaries or scalars before passing them to the
SDK, then rebuild domain objects in workflow or activity code.

A ``0x01`` prefix is reserved for typed-schema payloads — those are not yet
encodeable/decodeable from this SDK because typed schemas require a schema
registry that is out of scope for the first Avro release.

The ``avro`` third-party package is a core runtime dependency. If it is
missing from a broken or partial installation, calling :func:`encode` or
:func:`decode` raises :class:`AvroNotInstalledError` with a reinstall hint.
"""
from __future__ import annotations

import base64
import io
import json
from functools import lru_cache
from typing import Any

from .errors import AvroNotInstalledError

WRAPPER_SCHEMA_JSON = (
    '{"type":"record","name":"Payload","namespace":"durable_workflow",'
    '"fields":[{"name":"json","type":"string"},'
    '{"name":"version","type":"int","default":1}]}'
)
WRAPPER_VERSION = 1
_PREFIX_GENERIC_WRAPPER = b"\x00"
_PREFIX_TYPED_SCHEMA = b"\x01"


@lru_cache(maxsize=1)
def _load_avro_schema() -> Any:
    try:
        import avro.schema
    except ImportError as exc:
        raise AvroNotInstalledError(
            "The 'avro' package is required to encode/decode payloads with the 'avro' "
            "codec. Reinstall durable-workflow with its runtime dependencies."
        ) from exc

    return avro.schema.parse(WRAPPER_SCHEMA_JSON)


def encode(value: Any) -> str:
    """Encode a Python value as an Avro generic-wrapper payload blob.

    Returns a base64 string the server accepts under ``payload_codec="avro"``.
    The generic wrapper accepts the same value shapes as ``json.dumps``; adapt
    domain objects to JSON-native data before encoding.
    """
    return encode_many([value])[0]


def encode_many(values: list[Any]) -> list[str]:
    """Encode several Avro generic-wrapper payloads through one codec visit.

    The Avro runtime does not provide a useful vectorized API for independent
    payload blobs, but batching here still avoids repeated import/schema/writer
    setup at high fan-out command boundaries.
    """
    if not values:
        return []

    try:
        import avro.io
    except ImportError as exc:
        raise AvroNotInstalledError(
            "The 'avro' package is required to encode payloads with the 'avro' "
            "codec. Reinstall durable-workflow with its runtime dependencies."
        ) from exc

    schema = _load_avro_schema()
    writer = avro.io.DatumWriter(schema)
    return [_encode_with_writer(value, writer, avro.io.BinaryEncoder) for value in values]


def decode(blob: str) -> Any:
    """Decode an Avro ``payload_codec="avro"`` blob into a Python value.

    Accepts the server's generic-wrapper format (prefix ``0x00``).  Typed
    schemas (prefix ``0x01``) raise :class:`ValueError` because the SDK
    has no schema registry.
    """
    return decode_many([blob])[0]


def decode_many(blobs: list[str]) -> list[Any]:
    """Decode several Avro payload blobs through one codec visit."""
    if not blobs:
        return []

    try:
        import avro.io
    except ImportError as exc:
        raise AvroNotInstalledError(
            "The 'avro' package is required to decode payloads with the 'avro' "
            "codec. Reinstall durable-workflow with its runtime dependencies."
        ) from exc

    schema = _load_avro_schema()
    reader = avro.io.DatumReader(schema)
    return [_decode_with_reader(blob, reader, avro.io.BinaryDecoder) for blob in blobs]


def _encode_with_writer(value: Any, writer: Any, encoder_cls: Any) -> str:
    buf = io.BytesIO()
    encoder = encoder_cls(buf)
    writer.write(
        {
            "json": json.dumps(value, separators=(",", ":"), ensure_ascii=False),
            "version": WRAPPER_VERSION,
        },
        encoder,
    )
    return base64.b64encode(_PREFIX_GENERIC_WRAPPER + buf.getvalue()).decode("ascii")


def _decode_with_reader(blob: str, reader: Any, decoder_cls: Any) -> Any:
    try:
        raw = base64.b64decode(blob, validate=True)
    except (ValueError, TypeError) as exc:
        _diagnose_ingress(blob, exc)

    if not raw:
        raise ValueError("Avro payload is empty after base64 decode.")

    prefix = raw[:1]
    if prefix == _PREFIX_TYPED_SCHEMA:
        raise ValueError(
            "Typed Avro payload (prefix 0x01) received without a schema context. "
            "This SDK currently supports only the generic wrapper (prefix 0x00); "
            "typed schemas are not yet implemented."
        )
    if prefix != _PREFIX_GENERIC_WRAPPER:
        raise ValueError(
            f"Unknown Avro payload prefix: 0x{prefix.hex()} "
            f"(expected 0x00 generic wrapper or 0x01 typed schema). "
            f"These bytes were not produced by a Durable Workflow Avro serializer."
        )

    decoder = decoder_cls(io.BytesIO(raw[1:]))
    try:
        record = reader.read(decoder)
    except Exception as exc:
        raise ValueError(f"Avro generic-wrapper decode failed: {exc}") from exc

    if not isinstance(record, dict) or "json" not in record:
        raise ValueError(
            "Avro generic-wrapper payload did not decode to a {json, version} record."
        )

    try:
        return json.loads(record["json"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Avro generic-wrapper 'json' field is not valid JSON: {exc}") from exc


def _diagnose_ingress(blob: str, cause: Exception) -> None:
    """Re-raise an ingress base64 failure with a typed remediation hint."""
    stripped = blob.lstrip() if isinstance(blob, str) else ""
    looks_like_json = stripped[:1] in {"{", "[", '"', "-", "t", "f", "n"} or (
        stripped[:1].isdigit() if stripped else False
    )
    if looks_like_json:
        raise ValueError(
            "Payload bytes look like JSON, not base64-encoded Avro. The producer "
            "appears to have JSON-encoded the payload but tagged it with codec "
            '"avro". Either change the codec tag to "json", or re-encode the '
            'payload with the Avro serializer before tagging it "avro".'
        ) from cause

    raise ValueError(
        "Failed to base64-decode Avro payload bytes. Avro payloads on the wire "
        "must be base64-encoded bytes whose first byte is 0x00 (generic wrapper) "
        "or 0x01 (typed schema). Re-encode the payload, or change the codec tag "
        "if the producer used a different codec."
    ) from cause
