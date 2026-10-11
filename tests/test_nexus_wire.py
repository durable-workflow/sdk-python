import json
from pathlib import Path

import pytest

from durable_workflow import serializer
from durable_workflow.nexus import nexus_request_payload, stable_nexus_idempotency_key


def test_nexus_request_matches_the_regression_corpus_wire():
    fixture = json.loads((Path(__file__).parent / "fixtures" / "codec_regressions"
                          / "nexus-world-arguments-avro.json").read_text())
    request = nexus_request_payload(arguments=["world"])
    assert request["arguments"] == fixture["framing"]["wire_base64"]
    assert serializer.decode(request["arguments"], codec=request["payload_codec"]) == ["world"]


@pytest.mark.parametrize("arguments", [[], ["world"], [None, True, 42, {"nested": [b"\x00\xff", 1.5]}]])
def test_nexus_arguments_use_the_portable_avro_payload(arguments):
    request = nexus_request_payload(arguments=arguments)
    assert request["payload_codec"] == "avro"
    assert isinstance(request["arguments"], str)
    assert serializer.decode(request["arguments"], codec="avro") == arguments


def test_nexus_refuses_json_as_a_workflow_payload_codec():
    with pytest.raises(ValueError, match="unsupported_payload_codec"):
        nexus_request_payload(arguments=["world"], payload_codec="json")


def test_encoding_does_not_change_the_existing_workflow_call_identity():
    arguments = ["world", {"value": 42}]
    identity = dict(workflow_id="caller", run_id="original-run", ordinal=0,
                    endpoint_name="endpoint", service_name="service", operation_name="greet")
    key = stable_nexus_idempotency_key(**identity, arguments=arguments)
    request = nexus_request_payload(arguments=arguments, idempotency_key=key)
    decoded = serializer.decode(request["arguments"], codec=request["payload_codec"])
    assert request["idempotency_key"] == stable_nexus_idempotency_key(**identity, arguments=decoded)
