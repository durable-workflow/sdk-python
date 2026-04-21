"""Payload serialization for the Durable Workflow worker protocol.

The server exposes a language-neutral payload envelope (see issue #164 and
``docs/configuration/worker-protocol.md`` in the docs repo).  Every payload on
the wire carries a ``payload_codec`` tag alongside its opaque blob.

Supported codecs:

- ``"json"`` — the blob is a UTF-8 JSON document. Supported for decoding
  existing data only, not used for new workflows.
- ``"avro"`` — the blob is a base64-encoded Avro generic-wrapper payload
  (see :mod:`durable_workflow._avro`). Default for all new v2 workflows.
  The wrapper carries JSON-native values only. Class-carrying values such as
  dataclasses, attrs classes, pydantic models, pendulum values, datetimes,
  UUIDs, ``Decimal``, and plain ``Enum`` values need an explicit adapter to a
  dictionary or scalar before encode; JSON-subclass values such as ``IntEnum``
  and ``StrEnum`` round-trip as their JSON scalar, not as the original class.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from . import _avro

JSON_CODEC = "json"
AVRO_CODEC = "avro"
SUPPORTED_CODECS = (JSON_CODEC, AVRO_CODEC)
DEFAULT_PAYLOAD_SIZE_BYTES = 2_097_152
DEFAULT_WARNING_THRESHOLD_PERCENT = 80

log = logging.getLogger("durable_workflow.serializer")


@dataclass(frozen=True)
class PayloadSizeWarningConfig:
    """Configuration for pre-flight payload-size warnings."""

    limit_bytes: int = DEFAULT_PAYLOAD_SIZE_BYTES
    threshold_percent: int = DEFAULT_WARNING_THRESHOLD_PERCENT

    @property
    def threshold_bytes(self) -> int:
        return max(1, (self.limit_bytes * self.threshold_percent) // 100)

    def __post_init__(self) -> None:
        if self.limit_bytes < 1:
            raise ValueError("payload size warning limit must be at least 1 byte")
        if self.threshold_percent < 1 or self.threshold_percent > 100:
            raise ValueError("payload size warning threshold percent must be between 1 and 100")


@dataclass(frozen=True)
class PayloadSizeWarningContext:
    """Structured context attached to payload-size warning logs."""

    kind: str
    workflow_id: str | None = None
    workflow_type: str | None = None
    run_id: str | None = None
    activity_name: str | None = None
    signal_name: str | None = None
    update_name: str | None = None
    query_name: str | None = None
    schedule_id: str | None = None
    task_queue: str | None = None
    namespace: str | None = None

    def to_log_context(self) -> dict[str, str]:
        values = {
            "kind": self.kind,
            "workflow_id": self.workflow_id,
            "workflow_type": self.workflow_type,
            "run_id": self.run_id,
            "activity_name": self.activity_name,
            "signal_name": self.signal_name,
            "update_name": self.update_name,
            "query_name": self.query_name,
            "schedule_id": self.schedule_id,
            "task_queue": self.task_queue,
            "namespace": self.namespace,
        }
        return {key: value for key, value in values.items() if value is not None}


DEFAULT_PAYLOAD_SIZE_WARNING = PayloadSizeWarningConfig()


def encode(
    value: Any,
    codec: str = AVRO_CODEC,
    *,
    size_warning: PayloadSizeWarningConfig | None = DEFAULT_PAYLOAD_SIZE_WARNING,
    warning_context: PayloadSizeWarningContext | Mapping[str, Any] | None = None,
) -> str:
    """Encode a Python value as a payload blob for *codec*.

    Raises ``ValueError`` for unknown codecs and
    :class:`~durable_workflow.errors.AvroNotInstalledError` when the Avro
    runtime dependency is missing from a broken or partial installation.
    """
    if codec == JSON_CODEC:
        blob = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    elif codec == AVRO_CODEC:
        blob = _avro.encode(value)
    else:
        raise ValueError(
            f"Unsupported payload codec {codec!r}; this SDK supports {SUPPORTED_CODECS!r}."
        )
    warn_if_payload_near_limit(
        blob,
        codec=codec,
        size_warning=size_warning,
        warning_context=warning_context,
    )
    return blob


def envelope(
    value: Any,
    codec: str = AVRO_CODEC,
    *,
    size_warning: PayloadSizeWarningConfig | None = DEFAULT_PAYLOAD_SIZE_WARNING,
    warning_context: PayloadSizeWarningContext | Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Wrap a value in a ``{codec, blob}`` payload envelope."""
    return {
        "codec": codec,
        "blob": encode(
            value,
            codec=codec,
            size_warning=size_warning,
            warning_context=warning_context,
        ),
    }


def warn_if_json_payload_near_limit(
    value: Any,
    *,
    size_warning: PayloadSizeWarningConfig | None = DEFAULT_PAYLOAD_SIZE_WARNING,
    warning_context: PayloadSizeWarningContext | Mapping[str, Any] | None = None,
) -> None:
    """Warn when a raw JSON request payload approaches the configured limit."""
    blob = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    warn_if_payload_near_limit(
        blob,
        codec=JSON_CODEC,
        size_warning=size_warning,
        warning_context=warning_context,
    )


def warn_if_payload_near_limit(
    blob: str,
    *,
    codec: str,
    size_warning: PayloadSizeWarningConfig | None = DEFAULT_PAYLOAD_SIZE_WARNING,
    warning_context: PayloadSizeWarningContext | Mapping[str, Any] | None = None,
) -> None:
    """Emit a structured warning when an encoded payload is close to the server limit."""
    if size_warning is None:
        return

    payload_size = len(blob.encode("utf-8"))
    threshold = size_warning.threshold_bytes
    if payload_size < threshold:
        return

    context = _normalize_warning_context(warning_context)
    log.warning(
        "encoded payload size %d bytes is at or above warning threshold %d bytes "
        "(limit %d bytes)",
        payload_size,
        threshold,
        size_warning.limit_bytes,
        extra={
            "durable_workflow_payload": {
                **context,
                "codec": codec,
                "payload_size": payload_size,
                "threshold_bytes": threshold,
                "limit_bytes": size_warning.limit_bytes,
            },
        },
    )


def _normalize_warning_context(
    context: PayloadSizeWarningContext | Mapping[str, Any] | None,
) -> dict[str, str]:
    if context is None:
        return {"kind": "payload"}
    if isinstance(context, PayloadSizeWarningContext):
        return context.to_log_context()
    normalized: dict[str, str] = {}
    for key, value in context.items():
        if value is not None:
            normalized[str(key)] = str(value)
    if "kind" not in normalized:
        normalized["kind"] = "payload"
    return normalized


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
    runtime dependency is missing from a broken or partial installation.
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
