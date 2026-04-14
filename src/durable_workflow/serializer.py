"""Payload serialization for the Durable Workflow worker protocol.

The server now exposes a language-neutral payload envelope (see issue #164 and
``docs/configuration/worker-protocol.md`` in the docs repo). Every payload on
the wire carries a ``payload_codec`` tag alongside its opaque blob. Python
workers use the ``json`` codec: the blob is a raw UTF-8 JSON document and no
PHP SerializableClosure / HMAC wrapping is involved.

For back-compat with runs started against older servers, ``decode`` still
tolerates the legacy ``json:``-prefixed form and the PHP-serialized
SerializableClosure that embeds a ``json:`` blob.
"""
from __future__ import annotations

import json
import re
from typing import Any

# The canonical codec name this SDK produces and consumes natively.
JSON_CODEC = "json"

# Legacy: older workers (pre-#164) used a ``json:`` prefix when embedding
# their payloads inside a PHP SerializableClosure wrapper. We keep the ability
# to read those so in-flight workflows started before the upgrade don't break.
_LEGACY_JSON_PREFIX = "json:"
_PHP_LEGACY_JSON_RE = re.compile(r's:\d+:"(json:.*?)";', re.DOTALL)


def encode(value: Any) -> str:
    """Encode a Python value as a JSON-codec payload blob."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def decode(blob: str | None, codec: str | None = None) -> Any:
    """Decode a payload blob into a Python value.

    Args:
        blob:  the opaque bytes as received from the server.
        codec: the declared ``payload_codec`` from the response, if known.
               When ``codec == "json"`` the blob is parsed directly.
               When ``codec`` is a legacy PHP codec (``workflow-serializer-y``
               or similar), no decode is attempted — the caller must surface
               a clear error.
    """
    if blob is None or blob == "":
        return None

    if codec in (None, JSON_CODEC):
        # Prefer strict JSON parsing when the codec is declared or unknown.
        try:
            return json.loads(blob)
        except (json.JSONDecodeError, TypeError):
            pass

    # Legacy shapes — kept so pre-#164 runs still decode.
    if blob.startswith(_LEGACY_JSON_PREFIX):
        return json.loads(blob[len(_LEGACY_JSON_PREFIX):])

    if blob.startswith("O:") and "json:" in blob:
        m = _PHP_LEGACY_JSON_RE.search(blob)
        if m:
            inner = m.group(1)
            return json.loads(inner[len(_LEGACY_JSON_PREFIX):])

    # PHP-serialized payload with no embedded JSON — this worker cannot decode it.
    if codec and codec != JSON_CODEC:
        raise ValueError(
            f"Cannot decode payload with codec {codec!r}: this SDK only supports "
            f"the {JSON_CODEC!r} codec. Ensure the workflow was started with "
            f"input as a plain JSON array or with an explicit "
            f'{{"codec": "json", "blob": "..."}} envelope.'
        )

    # Last-resort: return the raw string so the caller can flag it.
    return blob
