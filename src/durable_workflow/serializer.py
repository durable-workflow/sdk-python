"""Payload serialization for the Durable Workflow worker protocol.

The server exposes a language-neutral payload envelope (see issue #164 and
``docs/configuration/worker-protocol.md`` in the docs repo).  Every payload on
the wire carries a ``payload_codec`` tag alongside its opaque blob.  Python
workers use the ``json`` codec exclusively: the blob is a raw UTF-8 JSON
document.
"""
from __future__ import annotations

import json
from typing import Any

JSON_CODEC = "json"


def encode(value: Any) -> str:
    """Encode a Python value as a JSON-codec payload blob."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def envelope(value: Any) -> dict[str, str]:
    """Wrap a value in a ``{codec, blob}`` payload envelope."""
    return {"codec": JSON_CODEC, "blob": encode(value)}


def decode(blob: str | None, codec: str | None = None) -> Any:
    """Decode a payload blob into a Python value.

    Raises ``ValueError`` when *codec* names a non-JSON codec or when *blob*
    is not valid JSON.
    """
    if blob is None or blob == "":
        return None

    if codec is not None and codec != JSON_CODEC:
        raise ValueError(
            f"Cannot decode payload with codec {codec!r}: this SDK only "
            f"supports the {JSON_CODEC!r} codec. Ensure the workflow was "
            f"started with a JSON input or an explicit "
            f'{{"codec": "json", "blob": "..."}} envelope.'
        )

    try:
        return json.loads(blob)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError(f"Payload is not valid JSON: {exc}") from exc
