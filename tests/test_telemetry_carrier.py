from __future__ import annotations

import copy
import logging
from typing import Any

import pytest
from opentelemetry import baggage, context, propagate, trace
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from durable_workflow.telemetry_carrier import (
    CAPABILITY,
    SCHEMA,
    capture,
    dispatch,
    identity,
    links,
    normalize,
    supported,
)

PARENT = "00-12345678901234567890123456789012-1234567890123456-01"


def envelope(**headers: str) -> dict[str, Any]:
    return {"schema": SCHEMA, "version": 1, "carrier": {"traceparent": PARENT, **headers}}


def test_exact_negotiation_is_independent_of_version_and_rejects_coerced_types() -> None:
    info = {"worker_protocol": {"server_capabilities": {"observability_context": dict(CAPABILITY)}}}
    assert supported(info)
    for name in CAPABILITY:
        for wrong in (None, False, "1", 1.0, [], {}, "unknown"):
            candidate = copy.deepcopy(info)
            candidate["worker_protocol"]["server_capabilities"]["observability_context"][name] = wrong
            assert not supported(candidate), (name, wrong)
    assert not supported({"version": "999.0.0"})
    assert not supported({"worker_protocol": {"server_capabilities": {"observability_context": True}}})


@pytest.mark.parametrize("value", [None, [], True, "carrier", {}, {"schema": SCHEMA, "version": True, "carrier": {}}])
def test_absence_and_malformed_values_have_only_fixed_diagnostics(value: Any) -> None:
    assert normalize(value) == (None, None if value is None else "discarded")


@pytest.mark.parametrize("parent", [
    "00-00000000000000000000000000000000-1234567890123456-01",
    "00-12345678901234567890123456789012-0000000000000000-01",
    "ff-12345678901234567890123456789012-1234567890123456-01",
    "00-ABCDEF78901234567890123456789012-1234567890123456-01", "private-header", "x" * 257,
])
def test_official_w3c_validation_rejects_bad_ids_without_ambient_fallback(parent: str) -> None:
    value = envelope()
    value["carrier"]["traceparent"] = parent
    with TracerProvider().get_tracer("application").start_as_current_span("ambient"):
        assert normalize(value) == (None, "discarded")


def test_baggage_is_empty_by_default_allowlisted_bounded_and_nonmutating() -> None:
    value = envelope(baggage="tenant=public,credential=private")
    original = copy.deepcopy(value)
    normalized, diagnostic = normalize(value)
    assert diagnostic == "filtered" and normalized == envelope()
    normalized, diagnostic = normalize(value, ["tenant"])
    assert diagnostic == "filtered" and normalized == envelope(baggage="tenant=public")
    assert value == original
    for key, size in (("traceparent", 257), ("tracestate", 513), ("baggage", 2049)):
        oversized = envelope()
        oversized["carrier"][key] = "x" * size
        assert normalize(oversized) == (None, "discarded")
    members = [f"k{number}" for number in range(33)]
    too_many = envelope(baggage=",".join(f"{key}=v" for key in members))
    assert normalize(too_many, members) == (None, "discarded")
    bounded = envelope(baggage=",".join(f"{key}=v" for key in members[:32]))
    assert normalize(bounded, members)[0] is not None


def test_raw_propagator_warnings_are_private_only_during_normalization(caplog: Any) -> None:
    value = envelope(tracestate="private-token", baggage="private-customer-data")
    normalized, diagnostic = normalize(value, ["tenant"])
    assert normalized == envelope() and diagnostic == "filtered"
    assert "private" not in caplog.text
    logging.getLogger("opentelemetry.trace.span").warning("outside validation")
    assert "outside validation" in caplog.text


def test_dispatch_requires_exact_task_and_never_imports_baggage() -> None:
    value = {"schema": "durable-workflow.queue-trace", "version": 1, "task_id": "task", "context": envelope()}
    assert dispatch(value, "task") == (envelope(), None)
    assert dispatch(value, "other") == (None, "discarded")
    value["context"] = envelope(baggage="tenant=public")
    assert dispatch(value, "task") == (None, "discarded")
    value["context"] = {"carrier": {"traceparent": PARENT}, "unknown": {"nested": []}}
    assert dispatch(value, "task") == (None, "discarded")
    for invalid in ("", "x" * 513, "task\n", "task\u0085", "\ud800"):
        assert not identity(invalid)
    assert identity("t" * 512) and identity("任务")


def test_capture_reads_actual_caller_before_transport_and_preserves_sampling() -> None:
    previous = propagate.get_global_textmap()
    propagate.set_global_textmap(CompositePropagator([TraceContextTextMapPropagator()]))
    remote = TraceContextTextMapPropagator().extract({"traceparent": PARENT[:-2] + "00"}, context=context.Context())
    token = context.attach(baggage.set_baggage("secret", "hidden", context=remote))
    try:
        captured, diagnostic = capture()
        assert diagnostic is None
        assert captured is not None and captured["carrier"]["traceparent"] == PARENT[:-2] + "00"
        assert "baggage" not in captured["carrier"]
        assert trace.get_current_span().get_span_context().trace_id == int(PARENT[3:35], 16)
    finally:
        context.detach(token)
        propagate.set_global_textmap(previous)


def test_secondary_links_bind_real_event_provenance_and_allow_missing_child_producer() -> None:
    entries = [
        {"kind": "command", "command_id": "command", "context": envelope()},
        {"kind": "task_outcome", "task_id": "producer", "event_id": "event", "sequence": 1,
         "event_type": "ActivityCompleted", "activity_execution_id": "activity", "context": envelope()},
        {"kind": "child_outcome", "workflow_link_id": "link", "child_run_id": "child", "event_id": "terminal",
         "sequence": 2**63 - 1, "event_type": "WorkflowCancelled", "context": envelope()},
    ]
    value = {"schema": "durable-workflow.trace-links", "version": 1, "task_id": "task", "run_id": "run",
             "namespace": "tenant", "links": entries}
    def read(candidate: Any) -> tuple[list[dict[str, Any]], str | None]:
        return links(candidate, task_id="task", run_id="run", namespace="tenant")

    assert read(value) == (entries, None)
    value["links"][2]["child_task_id"] = None
    assert read(value) == (entries, None)
    assert links(value, task_id="other", run_id="run", namespace="tenant") == ([], "discarded")
    for sequence in (True, 0, -1, 2**63, 1.0, "1"):
        invalid = copy.deepcopy(value)
        invalid["links"][1]["sequence"] = sequence
        assert read(invalid) == ([], "discarded")
    for key in ("event_id", "event_type", "sequence"):
        invalid = copy.deepcopy(value)
        del invalid["links"][2][key]
        assert read(invalid) == ([], "discarded")
    invalid = copy.deepcopy(value)
    invalid["links"][1]["activity_id"] = invalid["links"][1].pop("activity_execution_id")
    assert read(invalid) == ([], "discarded")
    invalid = copy.deepcopy(value)
    invalid["links"][0]["context"] = envelope(baggage="tenant=private")
    assert read(invalid) == ([], "discarded")
    invalid = copy.deepcopy(value)
    invalid["links"] = [entries[0]] * 33
    assert read(invalid) == ([], "discarded")
    invalid["links"] = [{**entries[0], "command_id": "x" * 512}] * 10
    assert read(invalid) == ([], "discarded")
