"""Bounded optional carriers; these values never confer execution authority."""

from __future__ import annotations

import importlib
import json
import logging
import unicodedata
from collections.abc import Iterable
from contextvars import ContextVar
from typing import Any

SCHEMA = "durable-workflow.trace-context"
LIMITS = {"traceparent": 256, "tracestate": 512, "baggage": 2048}
CAPABILITY = {
    "supported": True, "schema": SCHEMA, "version": 1,
    "dispatch_schema": "durable-workflow.queue-trace", "links_schema": "durable-workflow.trace-links",
    "max_context_bytes": 4096, "max_links": 32, "max_links_bytes": 4096,
}
_validating: ContextVar[bool] = ContextVar("durable_validating_carrier", default=False)
_filters_installed = False


class _PrivatePropagatorDiagnostics(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Official propagators may include rejected tracestate/baggage in warning
        # text. Suppress only those library records in our normalization scope.
        return not _validating.get()


def _private_diagnostics() -> None:
    global _filters_installed
    if not _filters_installed:
        for name in ("opentelemetry.trace.span", "opentelemetry.baggage", "opentelemetry.baggage.propagation"):
            logging.getLogger(name).addFilter(_PrivatePropagatorDiagnostics())
        _filters_installed = True


def supported(cluster_info: Any) -> bool:
    """Accept only the exact implemented v1 capability, independent of version strings."""
    if not isinstance(cluster_info, dict):
        return False
    protocol = cluster_info.get("worker_protocol")
    capabilities = protocol.get("server_capabilities") if isinstance(protocol, dict) else None
    value = capabilities.get("observability_context") if isinstance(capabilities, dict) else None
    return isinstance(value, dict) and all(type(value.get(key)) is type(expected) and value[key] == expected
                                          for key, expected in CAPABILITY.items())


def identity(value: Any) -> bool:
    try:
        return (isinstance(value, str) and bool(value) and len(value) <= 512 and len(value.encode("utf-8")) <= 512
                and not any(unicodedata.category(character) == "Cc" for character in value))
    except UnicodeError:
        return False


def _size(value: Any) -> int:
    # Match Core's conservative JSON encoding budget, including escaped slashes.
    return len(json.dumps(value, separators=(",", ":"), ensure_ascii=True).replace("/", "\\/").encode("utf-8"))


def normalize(value: Any, allowlist: Iterable[str] = ()) -> tuple[dict[str, Any] | None, str | None]:
    """Normalize with official W3C propagators; absence and rejection are distinct."""
    diagnostic = None if value is None else "discarded"
    if (not isinstance(value, dict) or len(value) != 3 or set(value) != {"schema", "version", "carrier"}
            or value.get("schema") != SCHEMA or type(value.get("version")) is not int or value["version"] != 1):
        return None, diagnostic
    carrier = value.get("carrier")
    if (not isinstance(carrier, dict) or len(carrier) > 3
            or "traceparent" not in carrier or not set(carrier) <= LIMITS.keys()):
        return None, diagnostic
    try:
        if any(not isinstance(header, str) or len(header) > LIMITS[key] or len(header.encode("utf-8")) > LIMITS[key]
               for key, header in carrier.items()) or _size(value) > 4096:
            return None, diagnostic
        trace = importlib.import_module("opentelemetry.trace")
        context = importlib.import_module("opentelemetry.context")
        propagation = importlib.import_module("opentelemetry.trace.propagation.tracecontext")
        _private_diagnostics()
        propagator = propagation.TraceContextTextMapPropagator()
        token = _validating.set(True)
        try:
            return _normalize_official(carrier, allowlist, trace, context, propagator)
        finally:
            _validating.reset(token)
    except Exception:
        return None, diagnostic


def _normalize_official(
    carrier: dict[str, str], allowlist: Iterable[str],
    trace: Any, context: Any, propagator: Any,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        extracted = propagator.extract(carrier, context=context.Context())
        if not trace.get_current_span(extracted).get_span_context().is_valid:
            return None, "discarded"
        normalized: dict[str, str] = {}
        propagator.inject(normalized, context=extracted)
        allowed = frozenset(allowlist)
        if allowed and "baggage" in carrier:
            baggage = importlib.import_module("opentelemetry.baggage")
            baggage_propagation = importlib.import_module("opentelemetry.baggage.propagation")
            baggage_propagator = baggage_propagation.W3CBaggagePropagator()
            baggage_context = baggage_propagator.extract(carrier, context=context.Context())
            retained = {key: entry for key, entry in baggage.get_all(baggage_context).items() if key in allowed}
            if len(retained) > 32:
                return None, "discarded"
            output = context.Context()
            for key, entry in retained.items():
                output = baggage.set_baggage(key, entry, context=output)
            baggage_propagator.inject(normalized, context=output)
        result = {"schema": SCHEMA, "version": 1, "carrier": normalized}
        if len(normalized.get("baggage", "").encode("utf-8")) > 2048 or _size(result) > 4096:
            return None, "discarded"
        return result, None if normalized == carrier else "filtered"
    except Exception:
        return None, "discarded"


def capture(allowlist: Iterable[str] = ()) -> tuple[dict[str, Any] | None, str | None]:
    """Capture the caller before starting an HTTP transport span."""
    try:
        propagation = importlib.import_module("opentelemetry.propagate")
        carrier: dict[str, str] = {}
        propagation.inject(carrier)
        carrier = {key: value for key, value in carrier.items() if key in LIMITS}
        if not carrier:
            return None, None
        return normalize({"schema": SCHEMA, "version": 1, "carrier": carrier}, allowlist)
    except Exception:
        return None, "discarded"


def dispatch(value: Any, task_id: str) -> tuple[dict[str, Any] | None, str | None]:
    """Read a task-bound producer link only after the caller admits the claim."""
    diagnostic = None if value is None else "discarded"
    if (not identity(task_id) or not isinstance(value, dict)
            or len(value) != 4 or set(value) != {"schema", "version", "task_id", "context"}
            or value.get("schema") != "durable-workflow.queue-trace"
            or type(value.get("version")) is not int or value["version"] != 1 or value.get("task_id") != task_id):
        return None, diagnostic
    carrier = value.get("context")
    headers = carrier.get("carrier") if isinstance(carrier, dict) else None
    if not isinstance(headers, dict) or len(headers) > 2 or not set(headers) <= {"traceparent", "tracestate"}:
        return None, diagnostic
    normalized, context_diagnostic = normalize(carrier)
    if normalized is None:
        return None, diagnostic
    try:
        if _size(value) > 4096:
            return None, diagnostic
        return normalized, context_diagnostic
    except Exception:
        return None, diagnostic


def links(
    value: Any, *, task_id: str, run_id: str, namespace: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """Read secondary links bound to the already authorized original claim."""
    diagnostic = None if value is None else "discarded"
    if (not all(identity(item) for item in (task_id, run_id, namespace)) or not isinstance(value, dict)
            or len(value) != 6 or set(value) != {"schema", "version", "task_id", "run_id", "namespace", "links"}
            or value.get("schema") != "durable-workflow.trace-links" or type(value.get("version")) is not int
            or value["version"] != 1 or value.get("task_id") != task_id or value.get("run_id") != run_id
            or value.get("namespace") != namespace or not isinstance(value.get("links"), list)
            or len(value["links"]) > 32):
        return [], diagnostic
    result: list[dict[str, Any]] = []
    filtered = False
    for entry in value["links"]:
        normalized = _link_entry(entry)
        if normalized is None:
            return [], diagnostic
        retained, context_diagnostic = normalized
        filtered = filtered or context_diagnostic is not None
        result.append(retained)
    try:
        if _size(value) > 4096:
            return [], diagnostic
        return result, "filtered" if filtered else None
    except Exception:
        return [], diagnostic


def _link_entry(value: Any) -> tuple[dict[str, Any], str | None] | None:
    if not isinstance(value, dict) or len(value) > 10:
        return None
    kind = value.get("kind")
    if kind == "command":
        required, optional = {"command_id"}, set()
    elif kind == "task_outcome":
        required = {"task_id", "event_id", "sequence", "event_type"}
        optional = {"activity_execution_id", "activity_attempt_id", "timer_id"}
    elif kind == "child_outcome":
        required = {"workflow_link_id", "child_run_id", "event_id", "sequence", "event_type"}
        optional = {"child_task_id"}
    else:
        return None
    if not required <= value.keys() or not value.keys() <= required | optional | {"kind", "context"}:
        return None
    for key in required | (optional & value.keys()):
        if key == "sequence":
            if type(value[key]) is not int or not 0 < value[key] <= 2**63 - 1:
                return None
        elif key == "child_task_id" and value[key] is None:
            continue
        elif not identity(value[key]):
            return None
    context = value.get("context")
    headers = context.get("carrier") if isinstance(context, dict) else None
    if not isinstance(headers, dict) or len(headers) > 2 or not headers.keys() <= {"traceparent", "tracestate"}:
        return None
    normalized, diagnostic = normalize(context)
    return None if normalized is None else ({**value, "context": normalized}, diagnostic)
