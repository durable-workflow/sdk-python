"""Bound diagnostic text using the same JSON encoder as the HTTP transport.

Only failure diagnostics may be reduced. Claim identity, classification and
retry policy remain exact, even when that makes the complete report unfit.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from .errors import FailureReportTooLarge

MESSAGE_BYTES = 62 * 1024
STACK_BYTES = 128 * 1024
DETAIL_BYTES = 64 * 1024
REQUEST_BYTES = 1024 * 1024
UNENCODABLE_DETAILS = object()
_OPTIONAL = ("details", "diagnostics", "runtime_diagnostics", "validation_errors")
_EXCEPTION_IDENTITY = frozenset({
    "type", "class", "code", "reason", "non_retryable", "retryable", "retry_after_seconds",
    "next_retry_delay", "activity_type", "activity_attempt_id", "activity_execution_id",
    "failed_step_sequence", "failed_activity_execution_id",
})


def encoded_json(value: Any) -> bytes:
    """Use HTTPX's actual JSON settings, including escaping and NaN refusal."""
    return httpx.Request("POST", "http://failure.invalid", json=value).content


def _size(value: Any) -> int | None:
    try:
        return len(encoded_json(value))
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return None


def _utf8(value: str) -> bytes:
    return value.encode("utf-8", errors="replace")


def _prefix(value: str, size: int) -> str:
    return _utf8(value)[:max(0, size)].decode("utf-8", errors="ignore")


def _marker(omitted: dict[str, int | None]) -> str:
    return "\n[truncated " + encoded_json({"omitted_fields": omitted}).decode("utf-8") + "]"


class _ObservedLocalFailure(str):
    """A local message already delivered to workflow code.

    JSON serializes this as ordinary text. Keep the in-memory distinction until
    encoding so final batch fitting cannot change first-run/replay behavior.
    """


@dataclass
class _Report:
    original: dict[str, Any]
    terminal: bool = False
    structured: bool = False

    def _copy(self) -> dict[str, Any]:
        result = dict(self.original)
        if self.terminal and isinstance(result.get("exception"), dict):
            result["exception"] = dict(result["exception"])
        return result

    def prepare(self) -> dict[str, Any]:
        result = self._copy()
        omitted: dict[str, int | None] = {}
        self._bound_metadata(result, omitted, DETAIL_BYTES)
        if omitted:
            self._attach_diagnostics(result, omitted)
        return result

    def render(self, budget: int) -> dict[str, Any]:
        result = self._copy()
        if isinstance(result.get("message"), _ObservedLocalFailure):
            return result
        previous: dict[str, int | None] = {}
        if self.structured:
            self._previous_sizes(previous)
        omitted = dict(previous)
        message = result.get("message", "")
        stack = result.get("stack_trace")
        message_limit = min(MESSAGE_BYTES, budget)
        stack_limit = min(STACK_BYTES, budget)
        if len(_utf8(message)) > message_limit or _prefix(message, len(_utf8(message))) != message:
            omitted.setdefault("message", len(_utf8(message)))
        if isinstance(stack, str) and len(_utf8(stack)) > stack_limit:
            omitted.setdefault("stack_trace", len(_utf8(stack)))
            if stack_limit >= len("\n[truncated]"):
                result["stack_trace"] = _prefix(stack, stack_limit - len("\n[truncated]")) + "\n[truncated]"
            else:
                result.pop("stack_trace", None)
        self._bound_metadata(result, omitted, min(DETAIL_BYTES, budget))
        if not omitted:
            return result
        if self._unchanged(previous, omitted, budget):
            return result
        # Inserting the marker can itself shorten an otherwise small message,
        # and inserting diagnostics can require omitting another optional field.
        # Recompute until the finite set of omitted fields is stable.
        while True:
            before = dict(omitted)
            self._attach_diagnostics(result, omitted)
            marker = _marker(omitted)
            if len(_utf8(message)) + len(_utf8(marker)) > message_limit:
                omitted.setdefault("message", len(_utf8(message)))
            if omitted == before:
                break
        retained = max(0, message_limit - len(_utf8(marker)))
        result["message"] = _prefix(message, retained) + marker
        return result

    def _unchanged(
        self, previous: dict[str, int | None], omitted: dict[str, int | None], budget: int,
    ) -> bool:
        message = self.original.get("message", "")
        stack = self.original.get("stack_trace")
        return bool(
            previous and previous == omitted and message.endswith(_marker(previous))
            and len(_utf8(message)) <= min(MESSAGE_BYTES, budget)
            and (not isinstance(stack, str) or len(_utf8(stack)) <= min(STACK_BYTES, budget))
        )

    def _bound_metadata(self, result: dict[str, Any], omitted: dict[str, int | None], budget: int) -> None:
        fields = ("exception",) if self.terminal else _OPTIONAL
        values = {key: result[key] for key in fields if key in result}
        size = _size(values)
        if size is not None and size <= budget:
            return
        for key, value in values.items():
            omitted.setdefault(key, _size(value))
            if key == "exception" and isinstance(value, dict):
                result[key] = {k: v for k, v in value.items() if k in _EXCEPTION_IDENTITY}
            else:
                result.pop(key, None)

    def _previous_sizes(self, omitted: dict[str, int | None]) -> None:
        original = self.original.get("exception", {}) if self.terminal else self.original
        diagnostics = original.get("diagnostics") if isinstance(original, dict) else None
        previous = diagnostics.get("failure_report") if isinstance(diagnostics, dict) else None
        sizes = previous.get("omitted_fields") if isinstance(previous, dict) else None
        if not isinstance(sizes, dict):
            return
        for key, size in sizes.items():
            if (key in {*_OPTIONAL, "exception", "message", "stack_trace"}
                    and (size is None or type(size) is int and size >= 0)):
                omitted[key] = size

    def _attach_diagnostics(self, result: dict[str, Any], omitted: dict[str, int | None]) -> None:
        if not self.structured:
            return
        container = result.setdefault("exception", {}) if self.terminal else result
        self._previous_sizes(omitted)
        original = self.original.get("exception", {}) if self.terminal else self.original
        existing = original.get("diagnostics") if isinstance(original, dict) else None
        diagnostics = dict(existing) if isinstance(existing, dict) and "diagnostics" not in omitted else {}
        diagnostics["failure_report"] = {"omitted_fields": omitted}
        container["diagnostics"] = diagnostics
        optional = {key: container[key] for key in _OPTIONAL if key in container}
        size = _size(container if self.terminal else optional)
        if size is not None and size <= DETAIL_BYTES:
            return
        self._omit_optional_metadata(container, omitted, existing)

    def _omit_optional_metadata(
        self, container: dict[str, Any], omitted: dict[str, int | None], existing: Any,
    ) -> None:
        # Count inserted diagnostics too. Omit optional fields whole so an
        # Avro envelope is either valid and intact or explicitly absent.
        if existing is not None:
            omitted.setdefault("diagnostics", _size(existing))
        for key in list(container):
            removable = key not in _EXCEPTION_IDENTITY if self.terminal else key in _OPTIONAL
            if removable and key != "diagnostics":
                omitted.setdefault(key if key in _OPTIONAL else "exception", _size(container.pop(key)))
        container["diagnostics"] = {"failure_report": {"omitted_fields": omitted}}


def _command_reports(command: dict[str, Any]) -> list[tuple[tuple[str | int, ...], _Report]]:
    kind = command.get("type")
    terminal = kind == "fail_workflow"
    result: list[tuple[tuple[str | int, ...], _Report]] = []
    if kind in {"fail_workflow", "fail_update"} or kind == "record_local_activity" and "message" in command:
        result.append(((), _Report(command, terminal=terminal, structured=terminal)))
    if kind == "record_local_activity":
        for index, attempt in enumerate(command.get("attempts", [])):
            if isinstance(attempt, dict) and "message" in attempt:
                result.append((("attempts", index), _Report(attempt)))
    return result


def _reports(path: str, body: dict[str, Any]) -> list[tuple[tuple[str | int, ...], _Report]]:
    failure = body.get("failure")
    if isinstance(failure, dict) and (path.endswith("/fail") or path.endswith("/reject")):
        return [(("failure",), _Report(failure, structured="/activity-tasks/" in path))]
    if "/local-activities/" in path and path.endswith("/outcome"):
        report = body.get("report")
        if isinstance(report, dict) and "message" in report:
            return [(("report",), _Report(report))]
    commands = body.get("commands")
    if not isinstance(commands, list) or not path.endswith("/complete"):
        return []
    return [
        (("commands", index, *location), report)
        for index, command in enumerate(commands) if isinstance(command, dict)
        for location, report in _command_reports(command)
    ]


def _replace_report(body: Any, location: tuple[str | int, ...], report: dict[str, Any]) -> Any:
    if not location:
        return report
    result: Any = dict(body) if isinstance(body, dict) else list(body)
    key = location[0]
    result[key] = _replace_report(body[key], location[1:], report)
    return result


def prepare_failure_request(path: str, body: Any) -> Any:
    """Omit oversized diagnostic envelopes before optional payload uploads.

    Keep original message/stack text until the final fit so its original sizes
    survive normal externalization of unrelated business payloads in a batch.
    """
    if not path.startswith("/worker/") or not isinstance(body, dict):
        return body
    result = body
    for location, report in _reports(path, body):
        if report.structured:
            result = _replace_report(result, location, report.prepare())
    return result


def bound_failure_request(path: str, body: Any, limit: int = REQUEST_BYTES) -> Any:
    if not path.startswith("/worker/") or not isinstance(body, dict):
        return body
    reports = _reports(path, body)
    if not reports:
        return body
    limit = min(limit, REQUEST_BYTES)
    budget = STACK_BYTES
    while True:
        result = body
        for location, report in reports:
            rendered = report.render(budget)
            metadata = rendered.get("exception", {}) if report.terminal else {
                key: rendered[key] for key in _OPTIONAL if key in rendered
            }
            metadata_size = _size(metadata)
            if metadata_size is None or metadata_size > DETAIL_BYTES:
                raise FailureReportTooLarge(limit)
            result = _replace_report(result, location, rendered)
        size = _size(result)
        if size is not None and size <= limit:
            return result
        if budget == 0:
            raise FailureReportTooLarge(limit)
        budget //= 2


def normalize_local_failure(
    command: dict[str, Any], *, lease_owner: str, workflow_task_attempt: int, limit: int = REQUEST_BYTES,
) -> dict[str, Any]:
    """Fit local diagnostics before the workflow sees its durable failure.

    Business arguments can still be externalized by Client. Include all claim,
    retry and attempt data here; the final complete request is measured again.
    An already observed message is then immutable, including on a lower limit
    discovered later or when another command makes the batch unfit.
    """
    frame = {key: value for key, value in command.items() if key != "arguments"}
    body = {"lease_owner": lease_owner, "workflow_task_attempt": workflow_task_attempt, "commands": [frame]}
    normalized = bound_failure_request("/worker/workflow-tasks/local/complete", body, limit)["commands"][0]
    normalized = {**command, **normalized}
    if isinstance(normalized.get("message"), str):
        normalized["message"] = _ObservedLocalFailure(normalized["message"])
    for attempt in normalized.get("attempts", []):
        if isinstance(attempt, dict) and isinstance(attempt.get("message"), str):
            attempt["message"] = _ObservedLocalFailure(attempt["message"])
    return normalized
