import logging
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum, IntEnum
from uuid import UUID

import pytest

from durable_workflow import serializer
from durable_workflow.errors import AvroNotInstalledError

try:
    import avro  # type: ignore[import-not-found]  # noqa: F401

    _AVRO_AVAILABLE = True
except ImportError:
    _AVRO_AVAILABLE = False

requires_avro = pytest.mark.skipif(
    not _AVRO_AVAILABLE, reason="avro package not installed"
)

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - Python < 3.11 compatibility
    StrEnum = None  # type: ignore[assignment,misc]


@dataclass
class SerializerDataclass:
    name: str
    count: int


class SerializerEnum(Enum):
    PENDING = "pending"


class SerializerIntEnum(IntEnum):
    LOW = 1


if StrEnum is not None:

    class SerializerStrEnum(StrEnum):
        HIGH = "high"
else:
    SerializerStrEnum = None


class TestEncode:
    def test_list(self) -> None:
        assert serializer.encode(["a", 1, True], codec="json") == '["a",1,true]'

    def test_dict(self) -> None:
        assert serializer.encode({"k": "v"}, codec="json") == '{"k":"v"}'

    def test_none(self) -> None:
        assert serializer.encode(None, codec="json") == "null"


class TestDecode:
    def test_roundtrip_list(self) -> None:
        assert serializer.decode(serializer.encode(["a", 1, True], codec="json")) == ["a", 1, True]

    def test_none_blob(self) -> None:
        assert serializer.decode(None) is None

    def test_empty_string(self) -> None:
        assert serializer.decode("") is None

    def test_valid_json(self) -> None:
        assert serializer.decode('{"x":1}') == {"x": 1}

    def test_json_codec_explicit(self) -> None:
        assert serializer.decode('"hello"', codec="json") == "hello"

    def test_non_json_codec_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot decode payload with codec"):
            serializer.decode("blob", codec="workflow-serializer-y")

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(ValueError, match="not valid JSON"):
            serializer.decode("not-json")

    def test_invalid_json_with_json_codec_raises(self) -> None:
        with pytest.raises(ValueError, match="not valid JSON"):
            serializer.decode("not-json", codec="json")


class TestDecodeEnvelope:
    def test_unwraps_codec_blob_dict(self) -> None:
        assert serializer.decode_envelope({"codec": "json", "blob": '["a",1]'}) == ["a", 1]

    def test_falls_back_to_raw_string(self) -> None:
        assert serializer.decode_envelope('["a",1]') == ["a", 1]

    def test_falls_back_with_codec(self) -> None:
        assert serializer.decode_envelope('"hello"', codec="json") == "hello"

    def test_rejects_unknown_envelope_codec(self) -> None:
        with pytest.raises(ValueError, match="Cannot decode payload with codec"):
            serializer.decode_envelope({"codec": "workflow-serializer-y", "blob": "data"})

    def test_none_passthrough(self) -> None:
        assert serializer.decode_envelope(None) is None

    def test_empty_string_passthrough(self) -> None:
        assert serializer.decode_envelope("") is None


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
        blobs = serializer.encode_many([["a"], ["b"]], codec="json")
        assert blobs == ['["a"]', '["b"]']

    def test_envelope_many_wraps_each_value(self) -> None:
        envelopes = serializer.envelope_many([["a"], ["b"]], codec="json")
        assert envelopes == [
            {"codec": "json", "blob": '["a"]'},
            {"codec": "json", "blob": '["b"]'},
        ]

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
                codec="json",
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
                codec="json",
                warning_context=[serializer.PayloadSizeWarningContext(kind="payload")],
            )


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
                codec="json",
                size_warning=config,
                warning_context=context,
            )

        assert len(caplog.records) == 1
        payload = caplog.records[0].durable_workflow_payload
        assert payload["kind"] == "signal"
        assert payload["workflow_id"] == "wf-1"
        assert payload["signal_name"] == "approve"
        assert payload["namespace"] == "ns1"
        assert payload["codec"] == "json"
        assert payload["payload_size"] >= 5
        assert payload["threshold_bytes"] == 5
        assert payload["limit_bytes"] == 10

    def test_encode_stays_quiet_below_threshold(self, caplog: pytest.LogCaptureFixture) -> None:
        config = serializer.PayloadSizeWarningConfig(limit_bytes=100, threshold_percent=90)

        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            serializer.encode("small", codec="json", size_warning=config)

        assert caplog.records == []

    def test_encode_warning_can_be_disabled(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            serializer.encode("abcdef", codec="json", size_warning=None)

        assert caplog.records == []

    def test_rejects_invalid_config(self) -> None:
        with pytest.raises(ValueError, match="limit"):
            serializer.PayloadSizeWarningConfig(limit_bytes=0)
        with pytest.raises(ValueError, match="threshold"):
            serializer.PayloadSizeWarningConfig(threshold_percent=101)


# Fixture blobs were generated by the PHP workflow package's
# Workflow\Serializers\Avro::serialize() so these tests double as a
# cross-language wire-format contract against the PHP producer.
_PHP_AVRO_FIXTURES: dict[str, tuple[str, object]] = {
    "null": ("AAhudWxsAg==", None),
    "bool_true": ("AAh0cnVlAg==", True),
    "int_42": ("AAQ0MgI=", 42),
    "float": ("AAgzLjE0Ag==", 3.14),
    "string": ("AA4iaGVsbG8iAg==", "hello"),
    "list": ("AA5bMSwyLDNdAg==", [1, 2, 3]),
    "map": ("AC57ImEiOjEsImIiOnsiYyI6WzQsNV19fQI=", {"a": 1, "b": {"c": [4, 5]}}),
    "nested": (
        "AIQBeyJvcmRlcl9pZCI6Im9yZC03IiwiYW1vdW50Ijo5OS41LCJpdGVtcyI6W3sic2t1IjoiQUJDIiwicXR5IjoyfV19Ag==",
        {"order_id": "ord-7", "amount": 99.5, "items": [{"sku": "ABC", "qty": 2}]},
    ),
}


@requires_avro
class TestAvroCodec:
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
    def test_json_unsupported_user_types_fail_encode(self, value: object) -> None:
        with pytest.raises(TypeError, match="not JSON serializable"):
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
        [(name, blob, expected) for name, (blob, expected) in _PHP_AVRO_FIXTURES.items()],
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

    def test_decode_envelope_preserves_json_over_codec_arg(self) -> None:
        # Envelope codec wins even when caller passes a different `codec`.
        env = serializer.envelope([9, 10], codec="avro")
        assert serializer.decode_envelope(env, codec="json") == [9, 10]

    def test_typed_schema_prefix_rejected(self) -> None:
        # 0x01-prefixed payloads would need a writer schema the SDK doesn't
        # currently carry; the error must be loud and actionable, not a
        # silent mis-decode.
        import base64

        typed_blob = base64.b64encode(b"\x01deadbeef").decode()
        with pytest.raises(ValueError, match="Typed Avro payload"):
            serializer.decode(typed_blob, codec="avro")

    def test_unknown_prefix_rejected(self) -> None:
        import base64

        weird_blob = base64.b64encode(b"\x07garbage").decode()
        with pytest.raises(ValueError, match="Unknown Avro payload prefix"):
            serializer.decode(weird_blob, codec="avro")

    def test_json_tagged_avro_raises_diagnostic(self) -> None:
        # A JSON string wrongly tagged as `avro` produces a typed, actionable
        # error rather than a silent mis-decode.
        with pytest.raises(ValueError, match="look like JSON"):
            serializer.decode('{"x":1}', codec="avro")

    def test_empty_blob_is_none(self) -> None:
        assert serializer.decode("", codec="avro") is None
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
