"""Payload serialization for the Durable Workflow worker protocol.

The server exposes a language-neutral payload envelope (see issue #164 and
``docs/configuration/worker-protocol.md`` in the docs repo).  Every payload on
the wire carries a ``payload_codec`` tag alongside its opaque blob.

Supported codecs:

- ``"json"`` — the blob is a UTF-8 JSON document. Supported for decoding
  existing data only, not used for new workflows.
- ``"avro"`` — the blob is a base64-encoded Avro generic-wrapper payload
  (see :mod:`durable_workflow._avro`). Default for all new v2 workflows.
"""
from __future__ import annotations

import json
from typing import Any

from . import _avro

JSON_CODEC = "json"
AVRO_CODEC = "avro"
SUPPORTED_CODECS = (JSON_CODEC, AVRO_CODEC)


def encode(value: Any, codec: str = AVRO_CODEC) -> str:
    """Encode a Python value as a payload blob for *codec*.

    Raises ``ValueError`` for unknown codecs and
    :class:`~durable_workflow.errors.AvroNotInstalledError` when the Avro
    extra is requested but not installed.
    """
    if codec == JSON_CODEC:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    if codec == AVRO_CODEC:
        return _avro.encode(value)
    raise ValueError(
        f"Unsupported payload codec {codec!r}; this SDK supports {SUPPORTED_CODECS!r}."
    )


def envelope(value: Any, codec: str = AVRO_CODEC) -> dict[str, str]:
    """Wrap a value in a ``{codec, blob}`` payload envelope."""
    return {"codec": codec, "blob": encode(value, codec=codec)}


def decode_envelope(value: Any, codec: str | None = None) -> Any:
    """Decode a value that may be a ``{codec, blob}`` envelope or a raw blob.

    When *value* is an envelope, its inner ``codec`` takes precedence over
    the *codec* argument.  When *value* is a raw blob, *codec* selects the
    decoder (defaulting to JSON).
    """
    if isinstance(value, dict) and "codec" in value and "blob" in value:
        return decode(value["blob"], codec=value["codec"])
    return decode(value, codec=codec)


def decode(blob: str | None, codec: str | None = None) -> Any:
    """Decode a payload blob into a Python value.

    Raises ``ValueError`` for unknown codecs or malformed blobs, and
    :class:`~durable_workflow.errors.AvroNotInstalledError` when the Avro
    extra is requested but not installed.
    """
    if blob is None or blob == "":
        return None

    if codec is None or codec == JSON_CODEC:
        try:
            return json.loads(blob)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"Payload is not valid JSON: {exc}") from exc

    if codec == AVRO_CODEC:
        return _avro.decode(blob)

    raise ValueError(
        f"Cannot decode payload with codec {codec!r}: this SDK supports "
        f"{SUPPORTED_CODECS!r}. Ensure the workflow was started with a "
        f"compatible codec or an explicit {{'codec': '<codec>', 'blob': '...'}} envelope."
    )
