from durable_workflow import serializer


def test_roundtrip_list():
    assert serializer.decode(serializer.encode(["a", 1, True])) == ["a", 1, True]


def test_decode_none():
    assert serializer.decode(None) is None


def test_decode_raw_json():
    assert serializer.decode('{"x":1}') == {"x": 1}


def test_decode_unknown_returns_raw():
    assert serializer.decode("not-json") == "not-json"
