import hashlib
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from durable_workflow import Replayer, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.external_storage import (
    EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
    AzureBlobExternalStorage,
    ExternalPayloadCache,
    ExternalPayloadIntegrityError,
    ExternalPayloadReference,
    ExternalPayloadStoragePolicy,
    GCSExternalStorage,
    LocalFilesystemExternalStorage,
    S3ExternalStorage,
    delete_external_payload,
    external_storage_driver_from_policy,
    fetch_external_payload,
    store_external_payload,
)
from durable_workflow.worker import Worker
from durable_workflow.workflow import (
    CompleteUpdate,
    CompleteWorkflow,
    ContinueAsNew,
    RecordSideEffect,
    ScheduleActivity,
    StartChildWorkflow,
    WorkflowContext,
    commands_to_server_commands,
)


@workflow.defn(name="external-storage-replay")
class ExternalStorageReplayWorkflow:
    def run(self, ctx: WorkflowContext, seed: str):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("large-activity", [seed])
        return result


@workflow.defn(name="external-storage-query")
class ExternalStorageQueryWorkflow:
    def __init__(self) -> None:
        self.message = "unset"

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: False)

    @workflow.signal("set_message")
    def set_message(self, message: str) -> None:
        self.message = message

    @workflow.query("message")
    def get_message(self, suffix: str = "") -> str:
        return f"{self.message}{suffix}"


@workflow.defn(name="external-storage-complete")
class ExternalStorageCompleteWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return {"message": "x" * 64}


def external_storage_activity_result() -> dict[str, str]:
    return {"message": "x" * 64}


def _mock_worker_client(storage: LocalFilesystemExternalStorage, threshold: int = 1) -> AsyncMock:
    client = AsyncMock()
    client.metrics = None
    client.external_storage = storage
    client.external_storage_threshold_bytes = threshold
    client.external_storage_cache = ExternalPayloadCache()
    client.payload_size_warning_config = None
    client.complete_workflow_task = AsyncMock(return_value={"outcome": "completed"})
    client.complete_query_task = AsyncMock(return_value={"outcome": "completed"})
    client.fail_workflow_task = AsyncMock(return_value={"outcome": "failed"})
    client.fail_query_task = AsyncMock(return_value={"outcome": "failed"})
    return client


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


@pytest.mark.asyncio
async def test_client_payload_envelope_uses_configured_external_storage(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    client = Client(
        "http://durable-workflow.test",
        external_storage=storage,
        external_storage_threshold_bytes=10,
    )

    try:
        env = client._payload_envelope(
            {"message": "x" * 64},
            kind="workflow_input",
            codec="json",
        )
    finally:
        await client.aclose()

    assert env["codec"] == "json"
    assert "blob" not in env
    assert env["external_storage"]["schema"] == EXTERNAL_PAYLOAD_REFERENCE_SCHEMA
    assert serializer.decode_envelope(env, external_storage=storage) == {"message": "x" * 64}


@pytest.mark.asyncio
async def test_client_decodes_external_activity_result(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    result = serializer.external_storage_envelope(
        {"message": "x" * 64},
        external_storage=storage,
        threshold_bytes=1,
        codec="json",
    )
    client = Client("http://durable-workflow.test", external_storage=storage)

    async def fake_request(*args: object, **kwargs: object) -> dict[str, object]:
        return {
            "activity_id": "activity-1",
            "workflow_type": "dw.standalone_activity",
            "payload_codec": "json",
            "result": result,
        }

    client._request = fake_request  # type: ignore[method-assign]

    try:
        activity = await client.describe_activity("activity-1")
    finally:
        await client.aclose()

    assert activity.result == {"message": "x" * 64}


def test_replayer_fetches_external_start_and_activity_payloads(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    start = serializer.external_storage_envelope(
        ["seed"],
        external_storage=storage,
        threshold_bytes=1,
        codec="json",
    )
    activity_result = serializer.external_storage_envelope(
        {"message": "x" * 64},
        external_storage=storage,
        threshold_bytes=1,
        codec="json",
    )

    first = Replayer(workflows=[ExternalStorageReplayWorkflow]).replay(
        {
            "events": [
                {
                    "event_type": "WorkflowStarted",
                    "payload": {
                        "workflow_type": "external-storage-replay",
                        "arguments": start,
                    },
                },
            ],
        },
        external_storage=storage,
    )

    assert isinstance(first.commands[0], ScheduleActivity)
    assert first.commands[0].arguments == ["seed"]

    completed = Replayer(workflows=[ExternalStorageReplayWorkflow]).replay(
        {
            "events": [
                {
                    "event_type": "WorkflowStarted",
                    "payload": {
                        "workflow_type": "external-storage-replay",
                        "arguments": start,
                    },
                },
                {
                    "event_type": "ActivityCompleted",
                    "payload": {
                        "result": activity_result,
                    },
                },
            ],
        },
        external_storage=storage,
    )

    assert isinstance(completed.commands[0], CompleteWorkflow)
    assert completed.commands[0].result == {"message": "x" * 64}


@pytest.mark.asyncio
async def test_worker_query_task_replay_fetches_external_history_payload(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    client = _mock_worker_client(storage)
    worker = Worker(client, task_queue="q1", workflows=[ExternalStorageQueryWorkflow], activities=[])
    message = "x" * 64
    signal_args = serializer.external_storage_envelope(
        [message],
        external_storage=storage,
        threshold_bytes=1,
        codec="json",
    )
    query_args = serializer.external_storage_envelope(
        ["!"],
        external_storage=storage,
        threshold_bytes=1,
        codec="json",
    )

    outcome = await worker._run_query_task(
        {
            "query_task_id": "qt-external-history",
            "query_task_attempt": 1,
            "workflow_type": "external-storage-query",
            "query_name": "message",
            "history_events": [
                {
                    "event_type": "SignalReceived",
                    "payload": {
                        "signal_name": "set_message",
                        "value": signal_args,
                    },
                },
            ],
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": query_args,
            "payload_codec": "json",
        }
    )

    assert outcome == "completed"
    client.complete_query_task.assert_awaited_once()
    assert client.complete_query_task.call_args.kwargs["result"] == f"{message}!"
    client.fail_query_task.assert_not_called()


@pytest.mark.asyncio
async def test_worker_workflow_commands_use_client_external_storage_threshold(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    client = _mock_worker_client(storage)
    worker = Worker(client, task_queue="q1", workflows=[ExternalStorageCompleteWorkflow], activities=[])

    commands = await worker._run_workflow_task(
        {
            "task_id": "wf-external-result",
            "workflow_type": "external-storage-complete",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }
    )

    assert commands is not None
    result = commands[0]["result"]
    assert "external_storage" in result
    assert "blob" not in result
    assert serializer.decode_envelope(result, external_storage=storage) == {"message": "x" * 64}


@pytest.mark.asyncio
async def test_worker_activity_result_uses_worker_external_storage_threshold(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    client = Client("http://durable-workflow.test")
    requests: list[dict[str, object]] = []

    async def fake_request(*args: object, **kwargs: object) -> dict[str, str]:
        requests.append(kwargs["json"])
        return {"outcome": "completed"}

    client._request = fake_request  # type: ignore[method-assign]
    try:
        worker = Worker(
            client,
            task_queue="q1",
            workflows=[],
            activities=[external_storage_activity_result],
            external_storage=storage,
            external_storage_threshold_bytes=1,
        )
        outcome = await worker._run_activity_task(
            {
                "task_id": "activity-external-result",
                "activity_attempt_id": "attempt-1",
                "activity_type": "external_storage_activity_result",
                "arguments": serializer.envelope([], codec="json"),
                "payload_codec": "json",
            }
        )
    finally:
        await client.aclose()

    assert outcome == "completed"
    result = requests[0]["result"]
    assert isinstance(result, dict)
    assert "external_storage" in result
    assert "blob" not in result
    assert serializer.decode_envelope(result, external_storage=storage) == {"message": "x" * 64}


@pytest.mark.asyncio
async def test_worker_query_result_uses_worker_external_storage_threshold(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    client = Client("http://durable-workflow.test")
    requests: list[dict[str, object]] = []
    message = "x" * 64

    async def fake_request(*args: object, **kwargs: object) -> dict[str, str]:
        requests.append(kwargs["json"])
        return {"outcome": "completed"}

    client._request = fake_request  # type: ignore[method-assign]
    try:
        worker = Worker(
            client,
            task_queue="q1",
            workflows=[ExternalStorageQueryWorkflow],
            activities=[],
            external_storage=storage,
            external_storage_threshold_bytes=1,
        )
        outcome = await worker._run_query_task(
            {
                "query_task_id": "query-external-result",
                "query_task_attempt": 1,
                "workflow_type": "external-storage-query",
                "query_name": "message",
                "history_events": [
                    {
                        "event_type": "SignalReceived",
                        "payload": {
                            "signal_name": "set_message",
                            "value": serializer.envelope([message], codec="json"),
                        },
                    },
                ],
                "workflow_arguments": serializer.envelope([], codec="json"),
                "query_arguments": serializer.envelope(["!"], codec="json"),
                "payload_codec": "json",
            }
        )
    finally:
        await client.aclose()

    assert outcome == "completed"
    assert requests[0]["result"] == f"{message}!"
    result_envelope = requests[0]["result_envelope"]
    assert isinstance(result_envelope, dict)
    assert "external_storage" in result_envelope
    assert "blob" not in result_envelope
    assert serializer.decode_envelope(result_envelope, external_storage=storage) == f"{message}!"


def test_workflow_command_serialization_offloads_configured_payloads(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    large = "x" * 64

    commands = commands_to_server_commands(
        [
            ScheduleActivity("large-activity", [large]),
            CompleteWorkflow({"workflow": large}),
            CompleteUpdate("update-1", {"update": large}),
            StartChildWorkflow("child", [large]),
            ContinueAsNew(arguments=[large]),
            RecordSideEffect({"side_effect": large}),
        ],
        "q1",
        payload_codec="json",
        size_warning=None,
        external_storage=storage,
        external_storage_threshold_bytes=1,
    )

    assert serializer.decode_envelope(commands[0]["arguments"], external_storage=storage) == [large]
    assert serializer.decode_envelope(commands[1]["result"], external_storage=storage) == {"workflow": large}
    assert serializer.decode_envelope(commands[2]["result"], external_storage=storage) == {"update": large}
    assert serializer.decode_envelope(commands[3]["arguments"], external_storage=storage) == [large]
    assert serializer.decode_envelope(commands[4]["arguments"], external_storage=storage) == [large]
    assert serializer.decode_envelope(commands[5]["result"], external_storage=storage) == {"side_effect": large}
    assert all("external_storage" in command[key] for command, key in [
        (commands[0], "arguments"),
        (commands[1], "result"),
        (commands[2], "result"),
        (commands[3], "arguments"),
        (commands[4], "arguments"),
        (commands[5], "result"),
    ])

    direct_update = CompleteUpdate("update-2", {"direct": large}).to_server_command(
        "q1",
        payload_codec="json",
        size_warning=None,
        external_storage=storage,
        external_storage_threshold_bytes=1,
    )
    assert serializer.decode_envelope(direct_update["result"], external_storage=storage) == {"direct": large}


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


def test_fetch_external_payload_cache_reuses_verified_bytes(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    cache = ExternalPayloadCache(max_entries=2, max_bytes=1024)
    reference = store_external_payload(storage, b'{"stable":true}', codec="json")
    path = Path(reference.uri.removeprefix("file://"))

    assert fetch_external_payload(storage, reference, cache=cache) == b'{"stable":true}'
    path.write_bytes(b'{"stable":false}')

    assert fetch_external_payload(storage, reference, cache=cache) == b'{"stable":true}'
    assert len(cache) == 1


def test_delete_external_payload_removes_blob_and_cache_entry(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    cache = ExternalPayloadCache(max_entries=2, max_bytes=1024)
    reference = store_external_payload(storage, b'{"retained":false}', codec="json")
    path = Path(reference.uri.removeprefix("file://"))

    assert fetch_external_payload(storage, reference, cache=cache) == b'{"retained":false}'
    assert cache.get(reference) == b'{"retained":false}'

    delete_external_payload(storage, reference, cache=cache)

    assert not path.exists()
    assert cache.get(reference) is None


def test_delete_external_payload_is_idempotent_for_local_retention_cleanup(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    reference = store_external_payload(storage, b"old-history", codec="json")

    delete_external_payload(storage, reference)
    delete_external_payload(storage, reference)


def test_fetch_external_payload_does_not_cache_failed_integrity_check(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    cache = ExternalPayloadCache(max_entries=2, max_bytes=1024)
    reference = store_external_payload(storage, b'{"safe":true}', codec="json")
    path = Path(reference.uri.removeprefix("file://"))
    path.write_bytes(b'{"safe":false}')

    with pytest.raises(ExternalPayloadIntegrityError, match="size|hash"):
        fetch_external_payload(storage, reference, cache=cache)

    assert len(cache) == 0


def test_decode_envelopes_can_share_external_payload_cache(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    cache = ExternalPayloadCache(max_entries=2, max_bytes=1024)
    env = serializer.external_storage_envelope(
        {"message": "x" * 64},
        external_storage=storage,
        threshold_bytes=10,
        codec="json",
    )
    path = Path(env["external_storage"]["uri"].removeprefix("file://"))

    assert serializer.decode_envelope(env, external_storage=storage, external_storage_cache=cache) == {
        "message": "x" * 64
    }
    path.write_bytes(b'{"message":"mutated"}')

    assert serializer.decode_envelopes(
        [env],
        external_storage=storage,
        external_storage_cache=cache,
    ) == [{"message": "x" * 64}]


def test_external_payload_cache_is_bounded_by_entries_and_bytes(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    cache = ExternalPayloadCache(max_entries=1, max_bytes=20)
    first = store_external_payload(storage, b"first", codec="json")
    second = store_external_payload(storage, b"second", codec="json")

    fetch_external_payload(storage, first, cache=cache)
    fetch_external_payload(storage, second, cache=cache)

    assert cache.get(first) is None
    assert cache.get(second) == b"second"

    too_large = store_external_payload(storage, b"x" * 21, codec="json")
    fetch_external_payload(storage, too_large, cache=cache)

    assert cache.get(too_large) is None
    assert cache.current_bytes <= cache.max_bytes


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
            "expires_at": "2026-04-23T12:00:00Z",
        }
    )

    assert reference.sha256 == good_hash
    assert reference.expires_at == "2026-04-23T12:00:00Z"
    assert reference.to_dict()["expires_at"] == "2026-04-23T12:00:00Z"

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


def test_reference_from_dict_validates_optional_expiry() -> None:
    good_hash = hashlib.sha256(b"payload").hexdigest()

    with pytest.raises(ValueError, match="expires_at"):
        ExternalPayloadReference.from_dict(
            {
                "schema": EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
                "uri": "s3://bucket/key",
                "sha256": good_hash,
                "size_bytes": 7,
                "codec": "avro",
                "expires_at": "2026-04-23T12:00:00",
            }
        )


def test_store_external_payload_round_trips_optional_expiry(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)

    reference = store_external_payload(
        storage,
        b'{"large":true}',
        codec="json",
        expires_at="2026-04-23T12:00:00+00:00",
    )

    assert reference.expires_at == "2026-04-23T12:00:00+00:00"
    assert ExternalPayloadReference.from_dict(reference.to_dict()).expires_at == "2026-04-23T12:00:00+00:00"
    assert fetch_external_payload(storage, reference) == b'{"large":true}'


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
        Metadata: dict[str, str],
    ) -> None:
        assert ContentType == "application/octet-stream"
        assert Metadata["sha256"] == hashlib.sha256(Body).hexdigest()
        self.objects[(Bucket, Key)] = Body

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        return {"Body": FakeReadable(self.objects[(Bucket, Key)])}

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.objects.pop((Bucket, Key), None)


class FakeReadable:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self) -> bytes:
        return self.data


def test_s3_external_storage_round_trips_and_deletes_payload() -> None:
    client = FakeS3Client()
    storage = S3ExternalStorage(client, bucket="payloads", prefix="tenant-a/history")

    reference = store_external_payload(storage, b'{"large":true}', codec="json")

    assert reference.uri.startswith("s3://payloads/tenant-a/history/json/")
    assert fetch_external_payload(storage, reference) == b'{"large":true}'
    storage.delete(reference.uri)

    assert client.objects == {}


def test_s3_external_storage_rejects_foreign_bucket_or_prefix() -> None:
    storage = S3ExternalStorage(FakeS3Client(), bucket="payloads", prefix="tenant-a")

    with pytest.raises(ValueError, match="bucket"):
        storage.get("s3://other/tenant-a/json/aa/hash")

    with pytest.raises(ValueError, match="prefix"):
        storage.get("s3://payloads/tenant-b/json/aa/hash")


class FakeGCSClient:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def bucket(self, name: str) -> "FakeGCSBucket":
        return FakeGCSBucket(self.objects, name)


class FakeGCSBucket:
    def __init__(self, objects: dict[tuple[str, str], bytes], name: str) -> None:
        self.objects = objects
        self.name = name

    def blob(self, key: str) -> "FakeGCSBlob":
        return FakeGCSBlob(self.objects, self.name, key)


class FakeGCSBlob:
    def __init__(self, objects: dict[tuple[str, str], bytes], bucket: str, key: str) -> None:
        self.objects = objects
        self.bucket = bucket
        self.key = key
        self.metadata: dict[str, str] = {}

    def upload_from_string(self, data: bytes, *, content_type: str) -> None:
        assert content_type == "application/octet-stream"
        self.objects[(self.bucket, self.key)] = data

    def download_as_bytes(self) -> bytes:
        return self.objects[(self.bucket, self.key)]

    def delete(self) -> None:
        self.objects.pop((self.bucket, self.key), None)


def test_gcs_external_storage_round_trips_and_deletes_payload() -> None:
    client = FakeGCSClient()
    storage = GCSExternalStorage(client, bucket="payloads", prefix="tenant-a")

    reference = store_external_payload(storage, b'{"cloud":"gcs"}', codec="json")

    assert reference.uri.startswith("gs://payloads/tenant-a/json/")
    assert fetch_external_payload(storage, reference) == b'{"cloud":"gcs"}'
    storage.delete(reference.uri)

    assert client.objects == {}


class FakeAzureContainerClient:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def upload_blob(
        self,
        *,
        name: str,
        data: bytes,
        overwrite: bool,
        metadata: dict[str, str],
    ) -> None:
        assert overwrite is True
        assert metadata["sha256"] == hashlib.sha256(data).hexdigest()
        self.objects[name] = data

    def download_blob(self, name: str) -> "FakeAzureDownloader":
        return FakeAzureDownloader(self.objects[name])

    def delete_blob(self, name: str) -> None:
        self.objects.pop(name, None)


class FakeAzureDownloader:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def readall(self) -> bytes:
        return self.data


def test_azure_external_storage_round_trips_and_deletes_payload() -> None:
    client = FakeAzureContainerClient()
    storage = AzureBlobExternalStorage(client, container="payloads", prefix="tenant-a")

    reference = store_external_payload(storage, b'{"cloud":"azure"}', codec="json")

    assert reference.uri.startswith("azure-blob://payloads/tenant-a/json/")
    assert fetch_external_payload(storage, reference) == b'{"cloud":"azure"}'
    storage.delete(reference.uri)

    assert client.objects == {}


def test_object_storage_rejects_unsafe_prefix() -> None:
    with pytest.raises(ValueError, match="unsafe"):
        S3ExternalStorage(FakeS3Client(), bucket="payloads", prefix="../tenant-a")


def test_external_storage_policy_builds_s3_driver_from_server_namespace_policy() -> None:
    client = FakeS3Client()
    policy = ExternalPayloadStoragePolicy.from_dict(
        {
            "external_payload_storage": {
                "driver": "s3",
                "enabled": True,
                "threshold_bytes": 2_097_152,
                "config": {
                    "bucket": "dw-payloads",
                    "prefix": "billing/",
                },
            }
        }
    )

    storage = external_storage_driver_from_policy(policy, s3_client=client)
    assert isinstance(storage, S3ExternalStorage)
    assert policy.threshold_bytes == 2_097_152

    reference = store_external_payload(storage, b'{"from":"server-policy"}', codec="json")

    assert reference.uri.startswith("s3://dw-payloads/billing/json/")
    assert fetch_external_payload(storage, reference) == b'{"from":"server-policy"}'


def test_external_storage_policy_builds_gcs_driver_from_cloud_reference() -> None:
    client = FakeGCSClient()
    policy = ExternalPayloadStoragePolicy.from_dict(
        {
            "enabled": True,
            "mode": "byob",
            "driver": "gcs",
            "reference": "projects/acme/buckets/workflow-payloads",
            "prefix": "prod",
            "threshold_bytes": 1_500_000,
            "status": "pending_validation",
        }
    )

    storage = external_storage_driver_from_policy(policy, gcs_client=client)

    reference = store_external_payload(storage, b'{"from":"cloud-policy"}', codec="json")

    assert reference.uri.startswith("gs://workflow-payloads/prod/json/")
    assert fetch_external_payload(storage, reference) == b'{"from":"cloud-policy"}'


def test_external_storage_policy_builds_local_driver_from_file_uri(tmp_path: Path) -> None:
    root = tmp_path / "payloads"
    storage = external_storage_driver_from_policy(
        {
            "driver": "local",
            "enabled": True,
            "config": {"uri": root.as_uri()},
        }
    )

    reference = store_external_payload(storage, b"local-policy", codec="json")

    assert fetch_external_payload(storage, reference) == b"local-policy"


def test_external_storage_policy_builds_azure_driver_from_container_config() -> None:
    client = FakeAzureContainerClient()
    storage = external_storage_driver_from_policy(
        {
            "driver": "azure",
            "enabled": True,
            "config": {"container": "payloads", "prefix": "tenant-a"},
        },
        azure_container_client=client,
    )

    reference = store_external_payload(storage, b'{"from":"azure-policy"}', codec="json")

    assert reference.uri.startswith("azure-blob://payloads/tenant-a/json/")
    assert fetch_external_payload(storage, reference) == b'{"from":"azure-policy"}'


def test_external_storage_policy_rejects_disabled_or_missing_provider_client() -> None:
    with pytest.raises(ValueError, match="disabled"):
        external_storage_driver_from_policy({"driver": "s3", "enabled": False, "config": {"bucket": "payloads"}})

    with pytest.raises(ValueError, match="s3_client"):
        external_storage_driver_from_policy({"driver": "s3", "enabled": True, "config": {"bucket": "payloads"}})


def test_external_storage_policy_validates_threshold_and_config_shape() -> None:
    with pytest.raises(ValueError, match="threshold_bytes"):
        ExternalPayloadStoragePolicy.from_dict({"driver": "local", "threshold_bytes": 0})

    with pytest.raises(ValueError, match="threshold_bytes"):
        ExternalPayloadStoragePolicy.from_dict({"driver": "local", "threshold_bytes": True})

    with pytest.raises(ValueError, match="config"):
        ExternalPayloadStoragePolicy.from_dict({"driver": "local", "config": "file:///tmp/payloads"})


def test_external_storage_policy_validates_boolean_fields() -> None:
    with pytest.raises(ValueError, match="enabled"):
        ExternalPayloadStoragePolicy.from_dict({"driver": "local", "enabled": "false"})

    with pytest.raises(ValueError, match="integrity_required"):
        ExternalPayloadStoragePolicy.from_dict({"driver": "local", "integrity_required": 1})

    policy = ExternalPayloadStoragePolicy.from_dict(
        {"driver": "local", "enabled": False, "integrity_required": False}
    )

    assert policy.enabled is False
    assert policy.integrity_required is False
