import pytest

from durable_workflow import serializer


class TestEncode:
    def test_list(self) -> None:
        assert serializer.encode(["a", 1, True]) == '["a",1,true]'

    def test_dict(self) -> None:
        assert serializer.encode({"k": "v"}) == '{"k":"v"}'

    def test_none(self) -> None:
        assert serializer.encode(None) == "null"


class TestDecode:
    def test_roundtrip_list(self) -> None:
        assert serializer.decode(serializer.encode(["a", 1, True])) == ["a", 1, True]

    def test_none_blob(self) -> None:
        assert serializer.decode(None) is None

    def test_empty_string(self) -> None:
        assert serializer.decode("") is None

    def test_valid_json(self) -> None:
        assert serializer.decode('{"x":1}') == {"x": 1}

    def test_json_codec_explicit(self) -> None:
        assert serializer.decode('"hello"', codec="json") == "hello"

    def test_non_json_codec_raises(self) -> None:
        with pytest.raises(ValueError, match="only supports the 'json' codec"):
            serializer.decode("blob", codec="workflow-serializer-y")

    def test_invalid_json_raises(self) -> None:
        with pytest.raises(ValueError, match="not valid JSON"):
            serializer.decode("not-json")

    def test_invalid_json_with_json_codec_raises(self) -> None:
        with pytest.raises(ValueError, match="not valid JSON"):
            serializer.decode("not-json", codec="json")


class TestEnvelope:
    def test_structure(self) -> None:
        env = serializer.envelope(["a", 1])
        assert env["codec"] == "json"
        assert env["blob"] == '["a",1]'

    def test_none_value(self) -> None:
        env = serializer.envelope(None)
        assert env == {"codec": "json", "blob": "null"}
