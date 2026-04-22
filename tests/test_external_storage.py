import hashlib
from pathlib import Path

import pytest

from durable_workflow import serializer
from durable_workflow.external_storage import (
    EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
    ExternalPayloadIntegrityError,
    ExternalPayloadReference,
    LocalFilesystemExternalStorage,
    fetch_external_payload,
    store_external_payload,
)


def test_external_storage_envelope_offloads_large_payload(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)

    env = serializer.external_storage_envelope(
        {"message": "x" * 64},
        external_storage=storage,
        threshold_bytes=10,
        codec="json",
    )

    assert env["codec"] == "json"
    assert "blob" not in env
    reference = env["external_storage"]
    assert reference["schema"] == EXTERNAL_PAYLOAD_REFERENCE_SCHEMA
    assert reference["codec"] == "json"
    assert reference["size_bytes"] > 10
    assert serializer.decode_envelope(env, external_storage=storage) == {"message": "x" * 64}


def test_external_storage_envelope_keeps_small_payload_inline(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)

    env = serializer.external_storage_envelope(
        {"ok": True},
        external_storage=storage,
        threshold_bytes=100,
        codec="json",
    )

    assert env == {"codec": "json", "blob": '{"ok":true}'}
    assert serializer.decode_envelope(env, external_storage=storage) == {"ok": True}


def test_decode_envelope_requires_driver_for_external_reference(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    env = serializer.external_storage_envelope(
        "large",
        external_storage=storage,
        threshold_bytes=1,
        codec="json",
    )

    with pytest.raises(ValueError, match="external storage driver"):
        serializer.decode_envelope(env)


def test_decode_envelope_rejects_codec_mismatch(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    env = serializer.external_storage_envelope(
        "large",
        external_storage=storage,
        threshold_bytes=1,
        codec="json",
    )
    env["codec"] = "avro"

    with pytest.raises(ValueError, match="codec"):
        serializer.decode_envelope(env, external_storage=storage)


def test_decode_envelopes_resolves_external_references_in_order(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    large = serializer.external_storage_envelope(
        "large",
        external_storage=storage,
        threshold_bytes=1,
        codec="json",
    )

    assert serializer.decode_envelopes(
        [
            {"codec": "json", "blob": '"inline"'},
            large,
            None,
        ],
        external_storage=storage,
    ) == ["inline", "large", None]


def test_fetch_external_payload_rejects_mutated_bytes(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    reference = store_external_payload(storage, b'{"safe":true}', codec="json")
    path = Path(reference.uri.removeprefix("file://"))
    path.write_bytes(b'{"safe":false}')

    with pytest.raises(ExternalPayloadIntegrityError, match="size|hash"):
        fetch_external_payload(storage, reference)


def test_local_storage_rejects_file_uri_outside_root(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path / "root")
    outside = tmp_path / "outside"
    outside.write_bytes(b"nope")

    with pytest.raises(ValueError, match="outside"):
        storage.get(outside.resolve().as_uri())


def test_reference_from_dict_validates_schema_and_hash() -> None:
    good_hash = hashlib.sha256(b"payload").hexdigest()

    reference = ExternalPayloadReference.from_dict(
        {
            "schema": EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
            "uri": "s3://bucket/key",
            "sha256": good_hash,
            "size_bytes": 7,
            "codec": "avro",
        }
    )

    assert reference.sha256 == good_hash

    with pytest.raises(ValueError, match="schema"):
        ExternalPayloadReference.from_dict(
            {
                "schema": "v0",
                "uri": "s3://bucket/key",
                "sha256": good_hash,
                "size_bytes": 7,
                "codec": "avro",
            }
        )
