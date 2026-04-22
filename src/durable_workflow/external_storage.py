"""External payload storage contracts for large Durable Workflow payloads."""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import unquote, urlparse

EXTERNAL_PAYLOAD_REFERENCE_SCHEMA = "durable-workflow.v2.external-payload-reference.v1"


class ExternalPayloadIntegrityError(ValueError):
    """Raised when fetched external payload bytes do not match their reference."""


class ExternalStorageDriver(Protocol):
    """Protocol implemented by pluggable external payload storage drivers."""

    def put(self, data: bytes, *, sha256: str, codec: str) -> str:
        """Persist *data* and return a stable URI for later fetches."""

    def get(self, uri: str) -> bytes:
        """Fetch previously persisted payload bytes."""

    def delete(self, uri: str) -> None:
        """Delete previously persisted payload bytes when retention removes a run."""


@dataclass(frozen=True)
class ExternalPayloadReference:
    """Stable wire envelope for a payload stored outside workflow history."""

    uri: str
    sha256: str
    size_bytes: int
    codec: str
    schema: str = EXTERNAL_PAYLOAD_REFERENCE_SCHEMA

    def to_dict(self) -> dict[str, str | int]:
        return {
            "schema": self.schema,
            "uri": self.uri,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "codec": self.codec,
        }

    @classmethod
    def from_dict(cls, data: object) -> ExternalPayloadReference:
        if not isinstance(data, dict):
            raise ValueError("external payload reference must be an object")

        schema = data.get("schema")
        uri = data.get("uri")
        sha256 = data.get("sha256")
        size_bytes = data.get("size_bytes")
        codec = data.get("codec")

        if schema != EXTERNAL_PAYLOAD_REFERENCE_SCHEMA:
            raise ValueError("unsupported external payload reference schema")
        if not isinstance(uri, str) or not uri:
            raise ValueError("external payload reference uri must be a non-empty string")
        if not isinstance(sha256, str) or len(sha256) != 64:
            raise ValueError("external payload reference sha256 must be a hex digest")
        try:
            int(sha256, 16)
        except ValueError as exc:
            raise ValueError("external payload reference sha256 must be a hex digest") from exc
        if not isinstance(size_bytes, int) or size_bytes < 0:
            raise ValueError("external payload reference size_bytes must be a non-negative integer")
        if not isinstance(codec, str) or not codec:
            raise ValueError("external payload reference codec must be a non-empty string")

        return cls(uri=uri, sha256=sha256, size_bytes=size_bytes, codec=codec, schema=schema)


class ExternalPayloadCache:
    """Bounded cache for verified external payload bytes during replay.

    Cache entries are keyed by the complete reference identity. Bytes are
    inserted only by :func:`fetch_external_payload` after size and sha256
    verification has succeeded, so cache hits preserve the same integrity
    contract as a fresh driver fetch.
    """

    def __init__(self, *, max_entries: int = 128, max_bytes: int = 16 * 1024 * 1024) -> None:
        if max_entries < 1:
            raise ValueError("external payload cache max_entries must be at least 1")
        if max_bytes < 1:
            raise ValueError("external payload cache max_bytes must be at least 1")
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.current_bytes = 0
        self._entries: OrderedDict[tuple[str, str, int, str], bytes] = OrderedDict()

    def get(self, reference: ExternalPayloadReference) -> bytes | None:
        key = self._key(reference)
        data = self._entries.get(key)
        if data is None:
            return None
        self._entries.move_to_end(key)
        return data

    def put(self, reference: ExternalPayloadReference, data: bytes) -> None:
        if len(data) > self.max_bytes:
            return

        key = self._key(reference)
        existing = self._entries.pop(key, None)
        if existing is not None:
            self.current_bytes -= len(existing)

        self._entries[key] = data
        self.current_bytes += len(data)
        self._evict()

    def clear(self) -> None:
        self._entries.clear()
        self.current_bytes = 0

    def __len__(self) -> int:
        return len(self._entries)

    @staticmethod
    def _key(reference: ExternalPayloadReference) -> tuple[str, str, int, str]:
        return (reference.uri, reference.sha256, reference.size_bytes, reference.codec)

    def _evict(self) -> None:
        while len(self._entries) > self.max_entries or self.current_bytes > self.max_bytes:
            _, data = self._entries.popitem(last=False)
            self.current_bytes -= len(data)


class LocalFilesystemExternalStorage:
    """Dependency-free external storage driver for development and tests."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def put(self, data: bytes, *, sha256: str, codec: str) -> str:
        _validate_sha256(sha256)
        codec_segment = _safe_codec_segment(codec)
        path = self.root / codec_segment / sha256[:2] / sha256
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(data)
        return path.as_uri()

    def get(self, uri: str) -> bytes:
        path = self._path_from_uri(uri)
        return path.read_bytes()

    def delete(self, uri: str) -> None:
        path = self._path_from_uri(uri)
        try:
            path.unlink()
        except FileNotFoundError:
            return

    def _path_from_uri(self, uri: str) -> Path:
        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
            raise ValueError("local external storage can only read file:// URIs")

        path = Path(unquote(parsed.path)).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("external payload URI is outside the local storage root") from exc
        return path


def store_external_payload(
    driver: ExternalStorageDriver,
    data: bytes,
    *,
    codec: str,
) -> ExternalPayloadReference:
    """Store encoded payload bytes and return their reference envelope."""
    sha256 = hashlib.sha256(data).hexdigest()
    uri = driver.put(data, sha256=sha256, codec=codec)
    return ExternalPayloadReference(
        uri=uri,
        sha256=sha256,
        size_bytes=len(data),
        codec=codec,
    )


def fetch_external_payload(
    driver: ExternalStorageDriver,
    reference: ExternalPayloadReference,
    *,
    cache: ExternalPayloadCache | None = None,
) -> bytes:
    """Fetch payload bytes and verify size/hash before replay or decode."""
    if cache is not None:
        cached = cache.get(reference)
        if cached is not None:
            return cached

    data = driver.get(reference.uri)
    if len(data) != reference.size_bytes:
        raise ExternalPayloadIntegrityError("external payload size does not match its reference")

    actual_sha256 = hashlib.sha256(data).hexdigest()
    if actual_sha256 != reference.sha256:
        raise ExternalPayloadIntegrityError("external payload hash does not match its reference")
    if cache is not None:
        cache.put(reference, data)
    return data


def _validate_sha256(sha256: str) -> None:
    if len(sha256) != 64:
        raise ValueError("sha256 must be a hex digest")
    try:
        int(sha256, 16)
    except ValueError as exc:
        raise ValueError("sha256 must be a hex digest") from exc


def _safe_codec_segment(codec: str) -> str:
    if not codec:
        raise ValueError("codec must be a non-empty string")
    if not all(char.isalnum() or char in {"-", "_", "."} for char in codec):
        raise ValueError("codec contains characters that are unsafe for local storage paths")
    return codec
