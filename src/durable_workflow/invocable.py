from __future__ import annotations

import asyncio
import inspect
import time
import traceback
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timezone
from typing import Any

from . import serializer
from .errors import ActivityCancelled, NonRetryableError
from .external_storage import ExternalPayloadCache, ExternalStorageDriver
from .external_task_input import ExternalTaskInput, parse_external_task_input
from .external_task_result import EXTERNAL_TASK_RESULT_SCHEMA, EXTERNAL_TASK_RESULT_VERSION

InvocableActivityCallable = Callable[..., Any | Awaitable[Any]]


class InvocableActivityHandler:
    """Reference adapter for activity-grade invocable carriers.

    The handler consumes the stable external-task input envelope and returns
    the stable external-task result envelope. It deliberately rejects workflow
    tasks so lightweight HTTP/serverless carriers cannot become hidden workflow
    runtimes.
    """

    def __init__(
        self,
        handlers: Mapping[str, InvocableActivityCallable],
        *,
        carrier: str = "python-invocable",
        result_codec: str = serializer.AVRO_CODEC,
        external_storage: ExternalStorageDriver | None = None,
        external_storage_cache: ExternalPayloadCache | None = None,
    ) -> None:
        if result_codec not in serializer.SUPPORTED_CODECS:
            raise ValueError(f"unsupported invocable result codec {result_codec!r}")
        self.handlers = dict(handlers)
        self.carrier = carrier
        self.result_codec = result_codec
        self.external_storage = external_storage
        self.external_storage_cache = external_storage_cache

    async def handle(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        started = time.monotonic()
        task_input = parse_external_task_input(envelope)

        if not task_input.is_activity_task:
            return self._failure(
                task_input,
                started,
                kind="application",
                classification="application_error",
                message="invocable activity handlers only accept activity_task inputs",
                failure_type="UnsupportedExternalTaskKind",
                retryable=False,
            )

        deadline_failure = self._expired_deadline_failure(task_input, started)
        if deadline_failure is not None:
            return deadline_failure

        handler_name = task_input.task.handler
        handler = self.handlers.get(handler_name or "")
        if handler is None:
            return self._failure(
                task_input,
                started,
                kind="application",
                classification="application_error",
                message=f"no invocable activity handler registered for {handler_name!r}",
                failure_type="UnknownActivityHandler",
                retryable=False,
            )

        try:
            args = self._decode_arguments(task_input)
            timeout_seconds = self._remaining_timeout_seconds(task_input)
            result = handler(*args)
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(result, timeout=timeout_seconds)
        except ActivityCancelled:
            return self._failure(
                task_input,
                started,
                kind="cancellation",
                classification="cancelled",
                message="Activity was cancelled before the handler completed.",
                failure_type="ActivityCancelled",
                retryable=False,
                cancelled=True,
            )
        except NonRetryableError as exc:
            return self._failure(
                task_input,
                started,
                kind="application",
                classification="application_error",
                message=str(exc),
                failure_type=type(exc).__name__,
                stack_trace=traceback.format_exc(),
                retryable=False,
            )
        except (asyncio.TimeoutError, TimeoutError) as exc:
            return self._failure(
                task_input,
                started,
                kind="timeout",
                classification="deadline_exceeded",
                message=str(exc) or "Invocable activity handler exceeded its deadline.",
                failure_type=type(exc).__name__,
                stack_trace=traceback.format_exc(),
                timeout_type="deadline_exceeded",
                retryable=True,
            )
        except (TypeError, ValueError) as exc:
            return self._failure(
                task_input,
                started,
                kind="decode_failure",
                classification="decode_failure",
                message=f"Carrier could not decode or encode the activity payload: {exc}",
                failure_type=type(exc).__name__,
                stack_trace=traceback.format_exc(),
                retryable=False,
                details={"codec": self._input_codec(task_input)},
            )
        except Exception as exc:
            return self._failure(
                task_input,
                started,
                kind="application",
                classification="application_error",
                message=str(exc),
                failure_type=type(exc).__name__,
                stack_trace=traceback.format_exc(),
                retryable=True,
            )

        deadline_failure = self._expired_deadline_failure(task_input, started, completed=True)
        if deadline_failure is not None:
            return deadline_failure

        return self._success(task_input, started, result)

    def _decode_arguments(self, task_input: ExternalTaskInput) -> list[Any]:
        raw_args = task_input.payloads["arguments"]
        if raw_args is None:
            return []

        decoded = serializer.decode_envelope(
            raw_args,
            codec=self._input_codec(task_input),
            external_storage=self.external_storage,
            external_storage_cache=self.external_storage_cache,
        )
        if decoded is None:
            return []
        if isinstance(decoded, list):
            return decoded
        return [decoded]

    @staticmethod
    def _input_codec(task_input: ExternalTaskInput) -> str:
        raw_args = task_input.payloads.get("arguments")
        if isinstance(raw_args, Mapping):
            codec = raw_args.get("codec")
            if isinstance(codec, str):
                return codec
        return serializer.JSON_CODEC

    def _expired_deadline_failure(
        self,
        task_input: ExternalTaskInput,
        started: float,
        *,
        completed: bool = False,
    ) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        for name, expires_at in self._deadline_candidates(task_input).items():
            try:
                expires_at_utc = self._parse_deadline(expires_at)
            except ValueError as exc:
                return self._failure(
                    task_input,
                    started,
                    kind="decode_failure",
                    classification="decode_failure",
                    message=f"Carrier could not parse activity deadline {name}: {exc}",
                    failure_type=type(exc).__name__,
                    stack_trace=traceback.format_exc(),
                    retryable=False,
                    details={"deadline": name, "expires_at": expires_at},
                )
            if expires_at_utc <= now:
                suffix = "completed after" if completed else "received after"
                return self._failure(
                    task_input,
                    started,
                    kind="timeout",
                    classification="deadline_exceeded",
                    message=f"Invocable activity task {suffix} {name}.",
                    failure_type="ExternalTaskDeadlineExceeded",
                    timeout_type="deadline_exceeded",
                    retryable=True,
                    details={"deadline": name, "expires_at": expires_at},
                )
        return None

    def _remaining_timeout_seconds(self, task_input: ExternalTaskInput) -> float | None:
        now = datetime.now(timezone.utc)
        remaining = [
            max(0.0, (self._parse_deadline(expires_at) - now).total_seconds())
            for expires_at in self._deadline_candidates(task_input).values()
        ]
        if not remaining:
            return None
        return min(remaining)

    @staticmethod
    def _deadline_candidates(task_input: ExternalTaskInput) -> dict[str, str]:
        candidates = {"lease.expires_at": task_input.lease.expires_at}
        for key, value in (task_input.deadlines or {}).items():
            if isinstance(value, str):
                candidates[f"deadlines.{key}"] = value
        return candidates

    @staticmethod
    def _parse_deadline(value: str) -> datetime:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _success(self, task_input: ExternalTaskInput, started: float, result: Any) -> dict[str, Any]:
        return {
            "schema": EXTERNAL_TASK_RESULT_SCHEMA,
            "version": EXTERNAL_TASK_RESULT_VERSION,
            "outcome": {
                "status": "succeeded",
                "recorded": True,
            },
            "task": self._task_identity(task_input),
            "result": {
                "payload": serializer.envelope(result, codec=self.result_codec),
                "metadata": {
                    "content_type": "application/vnd.durable-workflow.result+json",
                },
            },
            "metadata": self._metadata(task_input, started),
        }

    def _failure(
        self,
        task_input: ExternalTaskInput,
        started: float,
        *,
        kind: str,
        classification: str,
        message: str,
        failure_type: str,
        retryable: bool,
        stack_trace: str | None = None,
        timeout_type: str | None = None,
        cancelled: bool = False,
        details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "schema": EXTERNAL_TASK_RESULT_SCHEMA,
            "version": EXTERNAL_TASK_RESULT_VERSION,
            "outcome": {
                "status": "failed",
                "retryable": retryable,
                "recorded": True,
            },
            "task": self._task_identity(task_input),
            "failure": {
                "kind": kind,
                "classification": classification,
                "message": message,
                "type": failure_type,
                "stack_trace": stack_trace,
                "timeout_type": timeout_type,
                "cancelled": cancelled,
                "details": details,
            },
            "metadata": self._metadata(task_input, started),
        }

    @staticmethod
    def _task_identity(task_input: ExternalTaskInput) -> dict[str, Any]:
        return {
            "id": task_input.task.id,
            "kind": task_input.task.kind,
            "attempt": task_input.task.attempt,
            "idempotency_key": task_input.task.idempotency_key,
        }

    def _metadata(self, task_input: ExternalTaskInput, started: float) -> dict[str, Any]:
        return {
            "handler": task_input.task.handler,
            "carrier": self.carrier,
            "duration_ms": max(0, int((time.monotonic() - started) * 1000)),
        }


async def handle_invocable_activity(
    envelope: Mapping[str, Any],
    handlers: Mapping[str, InvocableActivityCallable],
    **options: Any,
) -> dict[str, Any]:
    """Handle one invocable activity task with a temporary adapter instance."""

    return await InvocableActivityHandler(handlers, **options).handle(envelope)
