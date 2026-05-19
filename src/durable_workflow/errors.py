"""Typed exceptions raised by the Durable Workflow client and worker.

Every *error* exception inherits from :class:`DurableWorkflowError`, so
callers that only want to distinguish SDK errors from unrelated failures can
catch that base. More specific subclasses let callers react to particular
outcomes — workflow-not-found, update-rejected, schedule-already-exists —
without parsing server response bodies.

Cancellation is intentionally *not* in that hierarchy. :class:`WorkflowCancelled`
and :class:`ActivityCancelled` inherit from :class:`BaseException` directly,
so a generic ``except Exception:`` block cannot accidentally swallow a
cancellation signal. Callers that want to handle cancellation must name the
class explicitly (``except (ActivityCancelled, ...):``). This mirrors the way
:class:`asyncio.CancelledError` and :class:`KeyboardInterrupt` behave in the
standard library and avoids the historical mistake called out in
https://github.com/temporalio/sdk-python/issues/1292.
"""

from __future__ import annotations

from typing import Any


class DurableWorkflowError(Exception):
    """Base class for every exception raised by the SDK."""


class ServerError(DurableWorkflowError):
    """A server response was an error that does not map to a typed subclass.

    The HTTP status is on :attr:`status`, and the parsed JSON body is on
    :attr:`body` when the server returned one.
    """

    def __init__(self, status: int, body: object) -> None:
        super().__init__(f"server returned {status}: {body!r}")
        self.status = status
        self.body = body

    def reason(self) -> str | None:
        """Return the machine-readable ``reason`` field from the response body, if any."""
        if isinstance(self.body, dict):
            return self.body.get("reason")
        return None


class WorkflowFailed(DurableWorkflowError):
    """A workflow finished in the ``failed`` state.

    :attr:`exception_class` carries the fully qualified name of the exception
    class the workflow raised, when the server recorded one.
    """

    def __init__(self, message: str, exception_class: str | None = None) -> None:
        super().__init__(message)
        self.exception_class = exception_class


class WorkflowNotFound(DurableWorkflowError):
    """The addressed workflow instance does not exist on the server."""

    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"workflow not found: {workflow_id}")
        self.workflow_id = workflow_id


class WorkflowAlreadyStarted(DurableWorkflowError):
    """A start request collided with an existing instance id.

    Raised when duplicate-start policy is ``reject`` (the default) and the
    caller-supplied ``workflow_id`` is already in use.
    """

    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"workflow already started: {workflow_id}")
        self.workflow_id = workflow_id


class NamespaceNotFound(DurableWorkflowError):
    """The namespace configured on the :class:`~durable_workflow.Client` is unknown to the server."""

    def __init__(self, namespace: str) -> None:
        super().__init__(f"namespace not found: {namespace}")
        self.namespace = namespace


class InvalidArgument(DurableWorkflowError):
    """The server rejected the request as malformed (HTTP 422).

    :attr:`errors` holds the structured validation errors from the response
    body when the server returned them.
    """

    def __init__(self, message: str, errors: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.errors = errors


class Unauthorized(DurableWorkflowError):
    """The request was rejected for missing or invalid authentication (HTTP 401)."""

    def __init__(self, message: str = "unauthorized") -> None:
        super().__init__(message)


class ScheduleNotFound(DurableWorkflowError):
    """The addressed schedule does not exist on the server."""

    def __init__(self, schedule_id: str) -> None:
        super().__init__(f"schedule not found: {schedule_id}")
        self.schedule_id = schedule_id


class ScheduleAlreadyExists(DurableWorkflowError):
    """A create-schedule request collided with an existing schedule id."""

    def __init__(self, schedule_id: str) -> None:
        super().__init__(f"schedule already exists: {schedule_id}")
        self.schedule_id = schedule_id


class QueryFailed(DurableWorkflowError):
    """A workflow query was rejected or the workflow raised while handling it."""

    def __init__(
        self,
        message: str,
        *,
        reason: str | None = None,
        status: int | None = None,
        body: object | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.status = status
        self.body = body

    @property
    def validation_errors(self) -> dict[str, Any] | None:
        """Return structured query argument validation errors, if the server provided them."""
        if isinstance(self.body, dict):
            errors = self.body.get("validation_errors") or self.body.get("errors")
            if isinstance(errors, dict):
                return errors
        return None


class SignalFailed(DurableWorkflowError):
    """A workflow signal was rejected before it could be delivered."""

    def __init__(
        self,
        message: str,
        *,
        reason: str | None = None,
        status: int | None = None,
        body: object | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.status = status
        self.body = body

    @property
    def validation_errors(self) -> dict[str, Any] | None:
        """Return structured signal argument validation errors, if the server provided them."""
        if isinstance(self.body, dict):
            errors = self.body.get("validation_errors") or self.body.get("errors")
            if isinstance(errors, dict):
                return errors
        return None


class WorkflowPayloadDecodeError(DurableWorkflowError):
    """A committed workflow history payload could not be decoded during replay."""

    def __init__(
        self,
        message: str,
        *,
        workflow_id: str | None = None,
        run_id: str | None = None,
        event_id: str | None = None,
        receiver_kind: str | None = None,
        receiver_name: str | None = None,
        codec: str | None = None,
        payload_head: str | None = None,
        exception_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.event_id = event_id
        self.receiver_kind = receiver_kind
        self.receiver_name = receiver_name
        self.codec = codec
        self.payload_head = payload_head
        self.exception_type = exception_type


class UpdateRejected(DurableWorkflowError):
    """A workflow update was rejected by the workflow's validator."""


class ChildWorkflowFailed(DurableWorkflowError):
    """A child workflow finished in the ``failed`` state.

    Raised inside the parent workflow when it awaits the child's result.
    :attr:`exception_class` mirrors the child's recorded exception class.
    """

    def __init__(self, message: str, exception_class: str | None = None) -> None:
        super().__init__(message)
        self.exception_class = exception_class


class ActivityFailed(DurableWorkflowError):
    """An activity finished in the ``failed`` state.

    Raised inside workflow code when it awaits an activity that exhausted its
    retry policy or reported a non-retryable failure. The attributes mirror
    the stable failure fields recorded in workflow history so saga workflows
    can branch or compensate without parsing raw history events.
    """

    def __init__(
        self,
        message: str,
        *,
        activity_type: str | None = None,
        activity_execution_id: str | None = None,
        activity_attempt_id: str | None = None,
        failure_id: str | None = None,
        failure_category: str | None = None,
        exception_type: str | None = None,
        exception_class: str | None = None,
        non_retryable: bool = False,
        code: Any | None = None,
        exception_payload: dict[str, Any] | None = None,
        activity: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.activity_type = activity_type
        self.activity_execution_id = activity_execution_id
        self.activity_attempt_id = activity_attempt_id
        self.failure_id = failure_id
        self.failure_category = failure_category
        self.exception_type = exception_type
        self.exception_class = exception_class
        self.non_retryable = non_retryable
        self.code = code
        self.exception_payload = exception_payload
        self.activity = activity


class WorkflowTerminated(DurableWorkflowError):
    """A workflow was terminated by operator action.

    Termination is non-gracious and skips normal cleanup, unlike cancellation.
    """

    def __init__(self, message: str = "workflow was terminated") -> None:
        super().__init__(message)


class WorkflowCancelled(BaseException):
    """A workflow was cancelled and finished in the ``cancelled`` state.

    Inherits from :class:`BaseException` — not :class:`Exception` — so that a
    generic ``except Exception:`` block cannot accidentally swallow the
    cancellation outcome. Callers that want to treat a cancelled workflow
    differently from a failed one (e.g. to skip alerting) must catch this
    class by name.
    """

    def __init__(self, message: str = "workflow was cancelled") -> None:
        super().__init__(message)


class ActivityCancelled(BaseException):
    """An in-flight activity was cancelled.

    Raised inside :meth:`durable_workflow.ActivityContext.heartbeat` when the
    server reports that the owning workflow has asked for cancellation, so the
    activity can exit cleanly on its next heartbeat.

    Inherits from :class:`BaseException` — not :class:`Exception` — so that a
    user ``except Exception:`` block inside the activity function cannot
    accidentally swallow the cancellation signal. Activities that need to run
    cleanup on cancellation should catch this class by name and re-raise:

    .. code-block:: python

        try:
            await activity.context().heartbeat()
        except ActivityCancelled:
            cleanup()
            raise
    """

    def __init__(self, message: str = "activity was cancelled") -> None:
        super().__init__(message)


class NonRetryableError(DurableWorkflowError):
    """Marker an activity can raise to fail its workflow without further retries.

    The server stops retrying the activity and surfaces the failure to the
    workflow as a terminal activity error, regardless of the configured retry
    policy.
    """

    def __init__(self, message: str, *, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.__cause__ = cause


class AvroNotInstalledError(DurableWorkflowError, ImportError):
    """Raised when the core ``avro`` runtime dependency is unavailable."""


def _raise_for_status(status: int, body: object, *, context: str = "") -> None:
    if status < 400:
        return

    reason = body.get("reason") if isinstance(body, dict) else None
    message = body.get("message", "") if isinstance(body, dict) else str(body)
    operation = _control_plane_operation(body)

    if status == 401:
        raise Unauthorized(message or "unauthorized")

    def query_failed(default: str) -> QueryFailed:
        return QueryFailed(
            message or default,
            reason=reason if isinstance(reason, str) else None,
            status=status,
            body=body,
        )

    def signal_failed(default: str) -> SignalFailed:
        return SignalFailed(
            message or default,
            reason=reason if isinstance(reason, str) else None,
            status=status,
            body=body,
        )

    if status == 404:
        if reason == "unknown_signal":
            raise signal_failed("signal not found")
        if reason in ("query_not_found", "rejected_unknown_query"):
            raise query_failed("query not found")
        if reason == "schedule_not_found":
            raise ScheduleNotFound(context)
        if reason in ("instance_not_found", "workflow_not_found") or "workflow" in context.lower():
            raise WorkflowNotFound(context)
        if reason == "namespace_not_found":
            raise NamespaceNotFound(message)
        raise ServerError(status, body)

    if status == 409:
        if reason == "schedule_already_exists":
            raise ScheduleAlreadyExists(context)
        if reason == "duplicate_not_allowed":
            raise WorkflowAlreadyStarted(context)
        if reason == "run_not_active":
            if operation == "signal":
                raise signal_failed("signal rejected")
            if operation == "query":
                raise query_failed("query rejected")
        if reason in (
            "query_rejected",
            "query_worker_unavailable",
            "query_worker_incompatible",
            "query_workflow_state_unavailable",
        ):
            raise query_failed("query rejected")
        if reason == "update_rejected":
            raise UpdateRejected(message or "update rejected")
        raise ServerError(status, body)

    if status == 504:
        if reason in (
            "query_worker_timeout",
            "query_worker_execution_timeout",
            "query_task_not_claimed",
        ):
            raise query_failed("query worker timed out")
        raise ServerError(status, body)

    if status == 422:
        if reason == "invalid_signal_arguments":
            raise signal_failed("signal argument validation failed")
        if reason == "invalid_query_arguments":
            raise query_failed("query argument validation failed")
        errors = None
        if isinstance(body, dict):
            errors = body.get("errors") or body.get("validation_errors")
        raise InvalidArgument(message, errors)

    raise ServerError(status, body)


def _control_plane_operation(body: object) -> str | None:
    if not isinstance(body, dict):
        return None

    control_plane = body.get("control_plane")
    if isinstance(control_plane, dict):
        operation = control_plane.get("operation")
        if isinstance(operation, str) and operation:
            return operation

    operation = body.get("control_plane_operation")
    if isinstance(operation, str) and operation:
        return operation

    return None
