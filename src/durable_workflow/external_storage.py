"""External payload storage contracts for large Durable Workflow payloads."""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, unquote, urlparse

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
class ExternalPayloadStoragePolicy:
    """Normalized external payload storage policy from server or Cloud APIs."""

    enabled: bool
    driver: str | None = None
    threshold_bytes: int | None = None
    config: Mapping[str, Any] = field(default_factory=dict)
    reference: str | None = None
    prefix: str = ""
    mode: str | None = None
    status: str | None = None
    integrity_required: bool = True

    @classmethod
    def from_dict(cls, data: object) -> ExternalPayloadStoragePolicy:
        """Parse a server namespace or Cloud organization storage policy."""
        policy = _extract_policy(data)
        driver = _optional_string(policy.get("driver"), "external payload storage driver")
        enabled = bool(policy.get("enabled", driver is not None))
        threshold_bytes = _optional_positive_int(policy.get("threshold_bytes"), "threshold_bytes")
        config = _optional_mapping(policy.get("config"), "config")
        prefix = _optional_string(policy.get("prefix"), "prefix") or _optional_string(
            config.get("prefix"),
            "config.prefix",
        )
        integrity_required = bool(policy.get("integrity_required", True))

        return cls(
            enabled=enabled,
            driver=driver,
            threshold_bytes=threshold_bytes,
            config=config,
            reference=_optional_string(policy.get("reference"), "reference"),
            prefix=prefix or "",
            mode=_optional_string(policy.get("mode"), "mode"),
            status=_optional_string(policy.get("status"), "status"),
            integrity_required=integrity_required,
        )


@dataclass(frozen=True)
class ExternalPayloadReference:
    """Stable wire envelope for a payload stored outside workflow history."""

    uri: str
    sha256: str
    size_bytes: int
    codec: str
    expires_at: str | None = None
    schema: str = EXTERNAL_PAYLOAD_REFERENCE_SCHEMA

    def to_dict(self) -> dict[str, str | int]:
        data: dict[str, str | int] = {
            "schema": self.schema,
            "uri": self.uri,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "codec": self.codec,
        }
        if self.expires_at is not None:
            data["expires_at"] = self.expires_at
        return data

    @classmethod
    def from_dict(cls, data: object) -> ExternalPayloadReference:
        if not isinstance(data, dict):
            raise ValueError("external payload reference must be an object")

        schema = data.get("schema")
        uri = data.get("uri")
        sha256 = data.get("sha256")
        size_bytes = data.get("size_bytes")
        codec = data.get("codec")
        expires_at = data.get("expires_at")

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
        if expires_at is not None:
            if not isinstance(expires_at, str) or not expires_at:
                raise ValueError("external payload reference expires_at must be a non-empty RFC3339 string")
            _validate_rfc3339(expires_at)

        return cls(
            uri=uri,
            sha256=sha256,
            size_bytes=size_bytes,
            codec=codec,
            expires_at=expires_at,
            schema=schema,
        )


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

    def discard(self, reference: ExternalPayloadReference) -> None:
        """Remove verified bytes for *reference* after external retention cleanup."""
        data = self._entries.pop(self._key(reference), None)
        if data is not None:
            self.current_bytes -= len(data)

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


class S3ExternalStorage:
    """External storage driver backed by a boto3-compatible S3 client.

    The SDK does not depend on boto3. Applications that need S3 pass an
    already-configured client exposing ``put_object``, ``get_object``, and
    ``delete_object``.
    """

    def __init__(self, client: Any, *, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("s3 external storage bucket must be non-empty")
        self.client = client
        self.bucket = bucket
        self.prefix = _normalize_object_prefix(prefix)

    def put(self, data: bytes, *, sha256: str, codec: str) -> str:
        key = _object_key(self.prefix, sha256=sha256, codec=codec)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType="application/octet-stream",
            Metadata={"sha256": sha256, "codec": codec},
        )
        return _object_uri("s3", self.bucket, key)

    def get(self, uri: str) -> bytes:
        bucket, key = _parse_object_uri(uri, scheme="s3", expected_bucket=self.bucket, expected_prefix=self.prefix)
        response = self.client.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        data = body.read() if hasattr(body, "read") else body
        if not isinstance(data, bytes):
            raise ValueError("s3 external storage client returned non-bytes payload")
        return data

    def delete(self, uri: str) -> None:
        bucket, key = _parse_object_uri(uri, scheme="s3", expected_bucket=self.bucket, expected_prefix=self.prefix)
        self.client.delete_object(Bucket=bucket, Key=key)


class GCSExternalStorage:
    """External storage driver backed by a google-cloud-storage client.

    The SDK does not depend on google-cloud-storage. Applications pass a
    configured client exposing ``bucket(name).blob(key)``.
    """

    def __init__(self, client: Any, *, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("gcs external storage bucket must be non-empty")
        self.client = client
        self.bucket = bucket
        self.prefix = _normalize_object_prefix(prefix)

    def put(self, data: bytes, *, sha256: str, codec: str) -> str:
        key = _object_key(self.prefix, sha256=sha256, codec=codec)
        blob = self.client.bucket(self.bucket).blob(key)
        blob.metadata = {"sha256": sha256, "codec": codec}
        blob.upload_from_string(data, content_type="application/octet-stream")
        return _object_uri("gs", self.bucket, key)

    def get(self, uri: str) -> bytes:
        bucket, key = _parse_object_uri(uri, scheme="gs", expected_bucket=self.bucket, expected_prefix=self.prefix)
        data = self.client.bucket(bucket).blob(key).download_as_bytes()
        if not isinstance(data, bytes):
            raise ValueError("gcs external storage client returned non-bytes payload")
        return data

    def delete(self, uri: str) -> None:
        bucket, key = _parse_object_uri(uri, scheme="gs", expected_bucket=self.bucket, expected_prefix=self.prefix)
        self.client.bucket(bucket).blob(key).delete()


class AzureBlobExternalStorage:
    """External storage driver backed by an azure-storage-blob container client.

    The SDK does not depend on azure-storage-blob. Applications pass a
    configured container client exposing ``upload_blob``, ``download_blob``,
    and ``delete_blob``.
    """

    def __init__(self, container_client: Any, *, container: str, prefix: str = "") -> None:
        if not container:
            raise ValueError("azure external storage container must be non-empty")
        self.container_client = container_client
        self.container = container
        self.prefix = _normalize_object_prefix(prefix)

    def put(self, data: bytes, *, sha256: str, codec: str) -> str:
        key = _object_key(self.prefix, sha256=sha256, codec=codec)
        self.container_client.upload_blob(
            name=key,
            data=data,
            overwrite=True,
            metadata={"sha256": sha256, "codec": codec},
        )
        return _object_uri("azure-blob", self.container, key)

    def get(self, uri: str) -> bytes:
        container, key = _parse_object_uri(
            uri,
            scheme="azure-blob",
            expected_bucket=self.container,
            expected_prefix=self.prefix,
        )
        data = self.container_client.download_blob(key).readall()
        if not isinstance(data, bytes):
            raise ValueError("azure external storage client returned non-bytes payload")
        if container != self.container:
            raise ValueError("azure external storage URI uses a different container")
        return data

    def delete(self, uri: str) -> None:
        container, key = _parse_object_uri(
            uri,
            scheme="azure-blob",
            expected_bucket=self.container,
            expected_prefix=self.prefix,
        )
        if container != self.container:
            raise ValueError("azure external storage URI uses a different container")
        self.container_client.delete_blob(key)


def external_storage_driver_from_policy(
    policy: ExternalPayloadStoragePolicy | Mapping[str, Any],
    *,
    s3_client: Any | None = None,
    gcs_client: Any | None = None,
    azure_container_client: Any | None = None,
    local_root: str | Path | None = None,
) -> ExternalStorageDriver:
    """Build an SDK storage driver from a server or Cloud policy payload.

    Provider SDK clients remain application-owned. Pass the already-configured
    S3/GCS/Azure client that matches the policy returned by the control plane.
    """
    normalized = (
        policy
        if isinstance(policy, ExternalPayloadStoragePolicy)
        else ExternalPayloadStoragePolicy.from_dict(policy)
    )
    if not normalized.enabled:
        raise ValueError("external payload storage policy is disabled")
    if normalized.driver is None:
        raise ValueError("external payload storage policy driver is required")

    driver = normalized.driver.lower()
    if driver == "local":
        root = local_root or _policy_string(normalized, "uri") or normalized.reference
        if root is None:
            raise ValueError("local external payload storage policy requires config.uri or local_root")
        return LocalFilesystemExternalStorage(_local_path(root))

    if driver == "s3":
        if s3_client is None:
            raise ValueError("s3 external payload storage policy requires s3_client")
        bucket = _policy_string(normalized, "bucket") or _s3_bucket_from_reference(normalized.reference)
        if bucket is None:
            raise ValueError("s3 external payload storage policy requires config.bucket or an S3 bucket reference")
        return S3ExternalStorage(s3_client, bucket=bucket, prefix=_policy_prefix(normalized))

    if driver == "gcs":
        if gcs_client is None:
            raise ValueError("gcs external payload storage policy requires gcs_client")
        bucket = _policy_string(normalized, "bucket") or _gcs_bucket_from_reference(normalized.reference)
        if bucket is None:
            raise ValueError("gcs external payload storage policy requires config.bucket or a GCS bucket reference")
        return GCSExternalStorage(gcs_client, bucket=bucket, prefix=_policy_prefix(normalized))

    if driver in {"azure", "azure-blob"}:
        if azure_container_client is None:
            raise ValueError("azure external payload storage policy requires azure_container_client")
        container = (
            _policy_string(normalized, "container")
            or _policy_string(normalized, "bucket")
            or _azure_container_from_reference(normalized.reference)
        )
        if container is None:
            raise ValueError(
                "azure external payload storage policy requires config.container, "
                "config.bucket, or a container reference"
            )
        return AzureBlobExternalStorage(azure_container_client, container=container, prefix=_policy_prefix(normalized))

    raise ValueError(f"unsupported external payload storage driver {normalized.driver!r}")


def store_external_payload(
    driver: ExternalStorageDriver,
    data: bytes,
    *,
    codec: str,
    expires_at: str | None = None,
) -> ExternalPayloadReference:
    """Store encoded payload bytes and return their reference envelope."""
    if expires_at is not None:
        _validate_rfc3339(expires_at)
    sha256 = hashlib.sha256(data).hexdigest()
    uri = driver.put(data, sha256=sha256, codec=codec)
    return ExternalPayloadReference(
        uri=uri,
        sha256=sha256,
        size_bytes=len(data),
        codec=codec,
        expires_at=expires_at,
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


def delete_external_payload(
    driver: ExternalStorageDriver,
    reference: ExternalPayloadReference,
    *,
    cache: ExternalPayloadCache | None = None,
) -> None:
    """Delete referenced payload bytes and evict any verified replay cache entry."""
    driver.delete(reference.uri)
    if cache is not None:
        cache.discard(reference)


def _validate_sha256(sha256: str) -> None:
    if len(sha256) != 64:
        raise ValueError("sha256 must be a hex digest")
    try:
        int(sha256, 16)
    except ValueError as exc:
        raise ValueError("sha256 must be a hex digest") from exc


def _extract_policy(data: object) -> Mapping[str, Any]:
    if not isinstance(data, Mapping):
        raise ValueError("external payload storage policy must be an object")
    nested = data.get("external_payload_storage")
    if nested is not None:
        if not isinstance(nested, Mapping):
            raise ValueError("external_payload_storage must be an object")
        return nested
    return data


def _optional_mapping(data: object, field_name: str) -> Mapping[str, Any]:
    if data is None:
        return {}
    if not isinstance(data, Mapping):
        raise ValueError(f"external payload storage policy {field_name} must be an object")
    return data


def _optional_string(data: object, field_name: str) -> str | None:
    if data is None:
        return None
    if not isinstance(data, str):
        raise ValueError(f"external payload storage policy {field_name} must be a string")
    return data


def _optional_positive_int(data: object, field_name: str) -> int | None:
    if data is None:
        return None
    if not isinstance(data, int) or data < 1:
        raise ValueError(f"external payload storage policy {field_name} must be a positive integer")
    return data


def _policy_string(policy: ExternalPayloadStoragePolicy, key: str) -> str | None:
    return _optional_string(policy.config.get(key), f"config.{key}")


def _policy_prefix(policy: ExternalPayloadStoragePolicy) -> str:
    return _policy_string(policy, "prefix") or policy.prefix


def _local_path(root: str | Path) -> str | Path:
    if isinstance(root, Path):
        return root
    parsed = urlparse(root)
    if parsed.scheme == "file":
        if parsed.netloc not in {"", "localhost"}:
            raise ValueError("local external payload storage file URI must be local")
        return unquote(parsed.path)
    if parsed.scheme:
        raise ValueError("local external payload storage policy must use a file:// URI or filesystem path")
    return root


def _s3_bucket_from_reference(reference: str | None) -> str | None:
    if reference is None:
        return None
    if reference.startswith("arn:aws:s3:::"):
        bucket = reference.removeprefix("arn:aws:s3:::").split("/", 1)[0]
        return bucket or None
    parsed = urlparse(reference)
    if parsed.scheme == "s3" and parsed.netloc:
        return parsed.netloc
    return reference or None


def _gcs_bucket_from_reference(reference: str | None) -> str | None:
    if reference is None:
        return None
    marker = "/buckets/"
    if marker in reference:
        bucket = reference.rsplit(marker, 1)[1].split("/", 1)[0]
        return bucket or None
    parsed = urlparse(reference)
    if parsed.scheme == "gs" and parsed.netloc:
        return parsed.netloc
    return reference or None


def _azure_container_from_reference(reference: str | None) -> str | None:
    if reference is None:
        return None
    parsed = urlparse(reference)
    if parsed.scheme in {"azure", "azure-blob"} and parsed.netloc:
        return parsed.netloc
    return reference or None


def _validate_rfc3339(value: str) -> None:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("external payload reference expires_at must be an RFC3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("external payload reference expires_at must include a timezone")


def _safe_codec_segment(codec: str) -> str:
    if not codec:
        raise ValueError("codec must be a non-empty string")
    if not all(char.isalnum() or char in {"-", "_", "."} for char in codec):
        raise ValueError("codec contains characters that are unsafe for local storage paths")
    return codec


def _normalize_object_prefix(prefix: str) -> str:
    cleaned = prefix.strip("/")
    if not cleaned:
        return ""
    parts = cleaned.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("external storage prefix contains an unsafe path segment")
    return "/".join(quote(part, safe="-_.~") for part in parts)


def _object_key(prefix: str, *, sha256: str, codec: str) -> str:
    _validate_sha256(sha256)
    codec_segment = quote(_safe_codec_segment(codec), safe="-_.~")
    key = f"{codec_segment}/{sha256[:2]}/{sha256}"
    return f"{prefix}/{key}" if prefix else key


def _object_uri(scheme: str, bucket: str, key: str) -> str:
    return f"{scheme}://{bucket}/{key}"


def _parse_object_uri(
    uri: str,
    *,
    scheme: str,
    expected_bucket: str,
    expected_prefix: str,
) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != scheme or parsed.netloc != expected_bucket:
        raise ValueError(f"{scheme} external storage URI uses a different bucket or container")

    key = parsed.path.lstrip("/")
    if not key:
        raise ValueError(f"{scheme} external storage URI must include an object key")
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"{scheme} external storage URI contains an unsafe object key")
    if expected_prefix and not (key == expected_prefix or key.startswith(f"{expected_prefix}/")):
        raise ValueError(f"{scheme} external storage URI is outside the configured prefix")
    return parsed.netloc, key
