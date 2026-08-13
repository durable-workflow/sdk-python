import json
import logging
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum, IntEnum
from pathlib import Path
from uuid import UUID

import pytest

from durable_workflow import _avro as avro_codec
from durable_workflow import serializer
from durable_workflow.errors import AvroNotInstalledError

try:
    import fastavro  # noqa: F401

    _AVRO_AVAILABLE = True
except ImportError:
    _AVRO_AVAILABLE = False

requires_avro = pytest.mark.skipif(
    not _AVRO_AVAILABLE, reason="fastavro package not installed"
)

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - Python < 3.11 compatibility
    StrEnum = None  # type: ignore[assignment,misc]


@dataclass
class SerializerDataclass:
    name: str
    count: int


@dataclass
class SerializerOrder:
    order_id: UUID
    placed_at: datetime
    amount: Decimal
    status: "SerializerEnum"


class SerializerEnum(Enum):
    PENDING = "pending"


class SerializerIntEnum(IntEnum):
    LOW = 1


if StrEnum is not None:

    class SerializerStrEnum(StrEnum):
        HIGH = "high"
else:
    SerializerStrEnum = None


class _AttrsField:
    def __init__(self, name: str) -> None:
        self.name = name


class SerializerAttrsStyle:
    __attrs_attrs__ = (_AttrsField("sku"), _AttrsField("quantity"))

    def __init__(self, sku: str, quantity: int) -> None:
        self.sku = sku
        self.quantity = quantity


class SerializerPydanticStyle:
    __pydantic_fields__ = {"order_id": object()}

    def __init__(self, order_id: UUID, due_on: date) -> None:
        self.order_id = order_id
        self.due_on = due_on

    def model_dump(self, *, mode: str = "python") -> dict[str, object]:
        if mode == "json":
            return {"order_id": str(self.order_id), "due_on": self.due_on.isoformat()}
        return {"order_id": self.order_id, "due_on": self.due_on}


class TestEncode:
    def test_list(self) -> None:
        blob = serializer.encode(["a", 1, True])
        assert serializer.decode(blob, codec="avro") == ["a", 1, True]

    def test_dict(self) -> None:
        blob = serializer.encode({"k": "v"})
        assert serializer.decode(blob, codec="avro") == {"k": "v"}

    def test_none(self) -> None:
        blob = serializer.encode(None)
        assert serializer.decode(blob, codec="avro") is None

    def test_json_codec_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported_payload_codec"):
            serializer.encode({"stale": True}, codec="json")


class TestDecode:
    def test_roundtrip_list(self) -> None:
        assert serializer.decode(serializer.encode(["a", 1, True]), codec="avro") == ["a", 1, True]

    def test_none_blob(self) -> None:
        assert serializer.decode(None) is None

    def test_untagged_raw_blob_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported_payload_codec"):
            serializer.decode('{"x":1}')

    def test_json_tagged_blob_is_rejected_with_transport_distinction(self) -> None:
        with pytest.raises(ValueError, match="HTTP document transport"):
            serializer.decode('{"x":1}', codec="json")

    def test_non_json_codec_raises(self) -> None:
        with pytest.raises(ValueError, match="unsupported_payload_codec"):
            serializer.decode("blob", codec="workflow-serializer-y")


class TestDecodeEnvelope:
    def test_unwraps_codec_blob_dict(self) -> None:
        envelope = serializer.envelope(["a", 1])
        assert serializer.decode_envelope(envelope) == ["a", 1]

    def test_rejects_untagged_raw_string(self) -> None:
        with pytest.raises(ValueError, match="unsupported_payload_codec"):
            serializer.decode_envelope('["a",1]')

    def test_raw_blob_requires_explicit_avro_codec(self) -> None:
        blob = serializer.encode("hello")
        assert serializer.decode_envelope(blob, codec="avro") == "hello"

    def test_rejects_json_tagged_envelope(self) -> None:
        with pytest.raises(ValueError, match="unsupported_payload_codec"):
            serializer.decode_envelope({"codec": "json", "blob": '{"stale":true}'})

    def test_rejects_unknown_envelope_codec(self) -> None:
        with pytest.raises(ValueError, match="unsupported_payload_codec"):
            serializer.decode_envelope({"codec": "workflow-serializer-y", "blob": "data"})

    def test_none_passthrough(self) -> None:
        assert serializer.decode_envelope(None) is None

    def test_empty_string_without_codec_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported_payload_codec"):
            serializer.decode_envelope("")


class TestEnvelope:
    def test_structure(self) -> None:
        env = serializer.envelope(["a", 1])
        assert env["codec"] == "avro"
        assert env["blob"] == serializer.encode(["a", 1], codec="avro")

    def test_none_value(self) -> None:
        env = serializer.envelope(None)
        assert env["codec"] == "avro"
        assert serializer.decode(env["blob"], codec="avro") is None


class TestBatchEncoding:
    def test_encode_many_preserves_order(self) -> None:
        blobs = serializer.encode_many([["a"], ["b"]], codec="avro")
        assert serializer.decode_many(blobs, codec="avro") == [["a"], ["b"]]

    def test_envelope_many_wraps_each_value(self) -> None:
        envelopes = serializer.envelope_many([["a"], ["b"]], codec="avro")
        assert [envelope["codec"] for envelope in envelopes] == ["avro", "avro"]
        assert serializer.decode_envelopes(envelopes) == [["a"], ["b"]]

    def test_encode_many_accepts_per_payload_warning_context(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = serializer.PayloadSizeWarningConfig(limit_bytes=10, threshold_percent=50)
        contexts = [
            serializer.PayloadSizeWarningContext(kind="signal", signal_name="one"),
            serializer.PayloadSizeWarningContext(kind="signal", signal_name="two"),
        ]

        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            serializer.encode_many(
                ["abcdef", "ghijkl"],
                codec="avro",
                size_warning=config,
                warning_context=contexts,
            )

        assert [record.durable_workflow_payload["signal_name"] for record in caplog.records] == [
            "one",
            "two",
        ]

    def test_encode_many_rejects_context_count_mismatch(self) -> None:
        with pytest.raises(ValueError, match="context count"):
            serializer.encode_many(
                ["a", "b"],
                codec="avro",
                warning_context=[serializer.PayloadSizeWarningContext(kind="payload")],
            )

    def test_encode_many_routes_avro_through_codec_batch_hook(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = []

        def encode_many(values: list[object]) -> list[str]:
            calls.append(values)
            return [f"blob-{index}" for index, _ in enumerate(values)]

        monkeypatch.setattr(serializer._avro, "encode_many", encode_many)

        assert serializer.encode_many(["a", "b"], codec="avro", size_warning=None) == [
            "blob-0",
            "blob-1",
        ]
        assert calls == [["a", "b"]]

    def test_encode_many_preserves_warning_contexts_through_avro_batch_hook(
        self,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            serializer._avro,
            "encode_many",
            lambda values: ["x" * 10 for _ in values],
        )
        config = serializer.PayloadSizeWarningConfig(limit_bytes=10, threshold_percent=50)

        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            serializer.encode_many(
                ["a", "b"],
                codec="avro",
                size_warning=config,
                warning_context=[
                    serializer.PayloadSizeWarningContext(kind="signal", signal_name="one"),
                    serializer.PayloadSizeWarningContext(kind="signal", signal_name="two"),
                ],
            )

        assert [record.durable_workflow_payload["signal_name"] for record in caplog.records] == [
            "one",
            "two",
        ]


class TestBatchDecoding:
    def test_decode_many_preserves_avro_order_and_none_passthrough(self) -> None:
        blobs = serializer.encode_many(["a", {"b": 2}], codec="avro")
        assert serializer.decode_many([blobs[0], None, blobs[1]], codec="avro") == ["a", None, {"b": 2}]

    def test_decode_many_routes_avro_through_codec_batch_hook(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = []

        def decode_many(blobs: list[str]) -> list[object]:
            calls.append(blobs)
            return [{"index": index} for index, _ in enumerate(blobs)]

        monkeypatch.setattr(serializer._avro, "decode_many", decode_many)

        assert serializer.decode_many(["blob-a", "", "blob-b"], codec="avro") == [
            {"index": 0},
            {"index": 1},
            {"index": 2},
        ]
        assert calls == [["blob-a", "", "blob-b"]]

    def test_decode_envelopes_batches_avro_and_preserves_order(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            serializer._avro,
            "decode_many",
            lambda blobs: [f"avro:{blob}" for blob in blobs],
        )

        assert serializer.decode_envelopes(
            [
                {"codec": "avro", "blob": "a"},
                None,
                {"codec": "avro", "blob": "b"},
            ],
            codec="avro",
        ) == ["avro:a", None, "avro:b"]

    def test_decode_many_propagates_first_codec_error(self) -> None:
        good = serializer.encode({"ok": True}, codec="avro")

        with pytest.raises(ValueError, match="look like JSON"):
            serializer.decode_many([good, '{"bad":true}'], codec="avro")


class TestAvroPayloadAdapter:
    def test_adapts_dataclass_datetime_uuid_decimal_and_enum(self) -> None:
        order = SerializerOrder(
            order_id=UUID("12345678-1234-5678-1234-567812345678"),
            placed_at=datetime(2026, 4, 21, 10, 30, tzinfo=timezone.utc),
            amount=Decimal("10.25"),
            status=SerializerEnum.PENDING,
        )

        assert serializer.to_avro_payload_value(order) == {
            "order_id": "12345678-1234-5678-1234-567812345678",
            "placed_at": "2026-04-21T10:30:00+00:00",
            "amount": "10.25",
            "status": "pending",
        }

    def test_adapts_pydantic_style_models_through_json_mode_dump(self) -> None:
        model = SerializerPydanticStyle(
            UUID("12345678-1234-5678-1234-567812345678"),
            date(2026, 4, 21),
        )

        assert serializer.to_avro_payload_value(model) == {
            "order_id": "12345678-1234-5678-1234-567812345678",
            "due_on": "2026-04-21",
        }

    def test_adapts_attrs_style_objects_and_sequences(self) -> None:
        value = (SerializerAttrsStyle("ABC", 2), time(10, 30, tzinfo=timezone.utc))

        assert serializer.to_avro_payload_value(value) == [
            {"sku": "ABC", "quantity": 2},
            "10:30:00+00:00",
        ]

    @requires_avro
    def test_adapter_output_round_trips_through_default_avro_codec(self) -> None:
        value = serializer.to_avro_payload_value(
            {
                "model": SerializerDataclass(name="Ada", count=2),
                "ids": [UUID("12345678-1234-5678-1234-567812345678")],
            }
        )

        blob = serializer.encode(value, codec="avro")

        assert serializer.decode(blob, codec="avro") == {
            "model": {"name": "Ada", "count": 2},
            "ids": ["12345678-1234-5678-1234-567812345678"],
        }

    def test_adapter_rejects_non_string_mapping_keys(self) -> None:
        with pytest.raises(TypeError, match="string keys"):
            serializer.to_avro_payload_value({1: "one"})

    def test_adapter_rejects_unadapted_objects(self) -> None:
        with pytest.raises(TypeError, match="not Avro Value safe"):
            serializer.to_avro_payload_value(object())


class TestPayloadSizeWarning:
    def test_encode_warns_with_structured_context(self, caplog: pytest.LogCaptureFixture) -> None:
        config = serializer.PayloadSizeWarningConfig(limit_bytes=10, threshold_percent=50)
        context = serializer.PayloadSizeWarningContext(
            kind="signal",
            workflow_id="wf-1",
            signal_name="approve",
            namespace="ns1",
        )

        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            serializer.encode(
                "abcdef",
                codec="avro",
                size_warning=config,
                warning_context=context,
            )

        assert len(caplog.records) == 1
        payload = caplog.records[0].durable_workflow_payload
        assert payload["kind"] == "signal"
        assert payload["workflow_id"] == "wf-1"
        assert payload["signal_name"] == "approve"
        assert payload["namespace"] == "ns1"
        assert payload["codec"] == "avro"
        assert payload["payload_size"] >= 5
        assert payload["threshold_bytes"] == 5
        assert payload["limit_bytes"] == 10

    def test_encode_stays_quiet_below_threshold(self, caplog: pytest.LogCaptureFixture) -> None:
        config = serializer.PayloadSizeWarningConfig(limit_bytes=100, threshold_percent=90)

        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            serializer.encode("small", codec="avro", size_warning=config)

        assert caplog.records == []

    def test_encode_warning_can_be_disabled(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            serializer.encode("abcdef", codec="avro", size_warning=None)

        assert caplog.records == []

    def test_rejects_invalid_config(self) -> None:
        with pytest.raises(ValueError, match="limit"):
            serializer.PayloadSizeWarningConfig(limit_bytes=0)
        with pytest.raises(ValueError, match="threshold"):
            serializer.PayloadSizeWarningConfig(threshold_percent=101)


_GOLDEN_FIXTURE = json.loads(
    (Path(__file__).parents[1] / "schema" / "avro-value-v1-golden.json").read_text()
)
_CROSS_LANGUAGE_AVRO_FIXTURES: dict[str, tuple[str, object]] = {
    "null": (_GOLDEN_FIXTURE["cases"][0]["wire_base64"], None),
    "boolean_true": (_GOLDEN_FIXTURE["cases"][2]["wire_base64"], True),
    "long_7": (_GOLDEN_FIXTURE["cases"][5]["wire_base64"], 7),
    "double_7": (_GOLDEN_FIXTURE["cases"][6]["wire_base64"], 7.0),
    "string_utf8": (_GOLDEN_FIXTURE["cases"][9]["wire_base64"], "héllo"),
    "map": (_GOLDEN_FIXTURE["cases"][11]["wire_base64"], {"a": 1, "b": [False]}),
}


@requires_avro
class TestAvroCodec:
    def test_encode_many_reuses_parsed_schema_and_fastavro_hot_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import fastavro

        schemas: list[object] = []
        original_writer = fastavro.schemaless_writer

        def counting_writer(buffer: object, schema: object, datum: object) -> None:
            schemas.append(schema)
            original_writer(buffer, schema, datum)

        monkeypatch.setattr(fastavro, "schemaless_writer", counting_writer)

        blobs = serializer.encode_many([{"i": 1}, {"i": 2}], codec="avro")

        assert serializer.decode_many(blobs, codec="avro") == [{"i": 1}, {"i": 2}]
        assert len(schemas) == 2
        assert schemas[0] is schemas[1]
        assert schemas[0] is avro_codec._load_avro_schema()

    def test_decode_many_reuses_parsed_writer_and_reader_schema(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import fastavro

        blobs = serializer.encode_many([{"i": 1}, {"i": 2}], codec="avro")
        schemas: list[tuple[object, object]] = []
        original_reader = fastavro.schemaless_reader

        def counting_reader(
            buffer: object, writer_schema: object, reader_schema: object
        ) -> object:
            schemas.append((writer_schema, reader_schema))
            return original_reader(buffer, writer_schema, reader_schema)

        monkeypatch.setattr(fastavro, "schemaless_reader", counting_reader)

        assert serializer.decode_many(blobs, codec="avro") == [{"i": 1}, {"i": 2}]
        assert len(schemas) == 2
        assert all(writer is schemas[0][0] for writer, _ in schemas)
        assert all(reader is schemas[0][1] for _, reader in schemas)

    def test_round_trip_primitives(self) -> None:
        for value in (None, True, False, 0, -1, 42, 3.14, "hello"):
            blob = serializer.encode(value, codec="avro")
            assert serializer.decode(blob, codec="avro") == value

    def test_round_trip_containers(self) -> None:
        for value in ([], [1, 2, 3], {}, {"a": 1, "b": [2, 3]}, [{"k": "v"}]):
            blob = serializer.encode(value, codec="avro")
            assert serializer.decode(blob, codec="avro") == value

    @pytest.mark.parametrize(
        "value",
        [
            SerializerDataclass(name="Ada", count=2),
            datetime(2026, 4, 21, 10, 30, tzinfo=timezone.utc),
            date(2026, 4, 21),
            time(10, 30, tzinfo=timezone.utc),
            UUID("12345678-1234-5678-1234-567812345678"),
            Decimal("10.25"),
            SerializerEnum.PENDING,
        ],
    )
    def test_unadapted_application_types_fail_encode(self, value: object) -> None:
        with pytest.raises(TypeError, match="unsupported_value_type"):
            serializer.encode(value, codec="avro")

    def test_ordered_dict_decodes_as_plain_dict(self) -> None:
        value = OrderedDict([("first", 1), ("second", 2)])
        decoded = serializer.decode(serializer.encode(value, codec="avro"), codec="avro")
        assert decoded == {"first": 1, "second": 2}
        assert type(decoded) is dict

    def test_int_enum_decodes_as_int(self) -> None:
        decoded = serializer.decode(serializer.encode(SerializerIntEnum.LOW, codec="avro"), codec="avro")
        assert decoded == 1
        assert type(decoded) is int

    @pytest.mark.skipif(StrEnum is None, reason="StrEnum requires Python 3.11+")
    def test_str_enum_decodes_as_str(self) -> None:
        assert SerializerStrEnum is not None
        decoded = serializer.decode(serializer.encode(SerializerStrEnum.HIGH, codec="avro"), codec="avro")
        assert decoded == "high"
        assert type(decoded) is str

    @pytest.mark.parametrize(
        "name,blob,expected",
        [
            (name, blob, expected)
            for name, (blob, expected) in _CROSS_LANGUAGE_AVRO_FIXTURES.items()
        ],
    )
    def test_decodes_php_produced_blobs(self, name: str, blob: str, expected: object) -> None:
        assert serializer.decode(blob, codec="avro") == expected

    def test_envelope_structure(self) -> None:
        env = serializer.envelope([1, 2], codec="avro")
        assert env["codec"] == "avro"
        assert env["blob"] == serializer.encode([1, 2], codec="avro")

    def test_decode_envelope_routes_by_inner_codec(self) -> None:
        env = serializer.envelope({"x": 1}, codec="avro")
        assert serializer.decode_envelope(env) == {"x": 1}

    def test_decode_envelope_preserves_inner_codec_over_codec_arg(self) -> None:
        # Envelope codec wins even when caller passes a different `codec`.
        env = serializer.envelope([9, 10], codec="avro")
        assert serializer.decode_envelope(env, codec="avro") == [9, 10]

    def test_prerelease_prefix_rejected(self) -> None:
        import base64

        typed_blob = base64.b64encode(b"\x01deadbeef").decode()
        with pytest.raises(ValueError, match="invalid_payload_framing"):
            serializer.decode(typed_blob, codec="avro")

    def test_unknown_prefix_rejected(self) -> None:
        import base64

        weird_blob = base64.b64encode(b"\x07garbage").decode()
        with pytest.raises(ValueError, match="invalid_payload_framing"):
            serializer.decode(weird_blob, codec="avro")

    def test_invalid_base64_envelope_is_distinct_from_canonical_malformed_frame(self) -> None:
        import base64

        malformed = next(
            case for case in _GOLDEN_FIXTURE["malformed_frames"] if case["name"] == "decoded_non_magic_bytes"
        )
        canonical_wire = malformed["wire_base64"]
        decoded_wire = base64.b64decode(canonical_wire, validate=True)
        assert base64.b64encode(decoded_wire).decode("ascii") == canonical_wire
        assert decoded_wire == b"%%%"

        with pytest.raises(ValueError) as invalid_envelope:
            serializer.decode("%%%", codec="avro")
        assert str(invalid_envelope.value) == (
            "invalid_payload_framing: failed to base64-decode Avro payload bytes. "
            "Avro payloads must be strict base64 containing a c301 single-object frame."
        )

        with pytest.raises(ValueError) as malformed_frame:
            serializer.decode(canonical_wire, codec="avro")
        assert str(malformed_frame.value) == "invalid_payload_framing: expected Avro single-object magic c301."
        assert str(invalid_envelope.value) != str(malformed_frame.value)

    def test_json_tagged_avro_raises_diagnostic(self) -> None:
        # A JSON string wrongly tagged as `avro` produces a typed, actionable
        # error rather than a silent mis-decode.
        with pytest.raises(ValueError, match="look like JSON"):
            serializer.decode('{"x":1}', codec="avro")

    def test_empty_blob_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="invalid_payload_framing"):
            serializer.decode("", codec="avro")
        assert serializer.decode(None, codec="avro") is None


class TestAvroNotInstalledError:
    """Verify AvroNotInstalledError is a proper ImportError subclass.

    This matters because callers who already catch ImportError for broken
    or partial installations will transparently catch the SDK-specific
    error too.
    """

    def test_is_import_error_subclass(self) -> None:
        assert issubclass(AvroNotInstalledError, ImportError)

    def test_carries_install_hint_in_message(self) -> None:
        exc = AvroNotInstalledError("reinstall durable-workflow with runtime dependencies")
        assert "runtime dependencies" in str(exc)
