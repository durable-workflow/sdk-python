from __future__ import annotations

from typing import Any


class DurableWorkflowError(Exception):
    pass


class ServerError(DurableWorkflowError):
    def __init__(self, status: int, body: object) -> None:
        super().__init__(f"server returned {status}: {body!r}")
        self.status = status
        self.body = body

    def reason(self) -> str | None:
        if isinstance(self.body, dict):
            return self.body.get("reason")
        return None


class WorkflowFailed(DurableWorkflowError):
    def __init__(self, message: str, exception_class: str | None = None) -> None:
        super().__init__(message)
        self.exception_class = exception_class


class WorkflowNotFound(DurableWorkflowError):
    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"workflow not found: {workflow_id}")
        self.workflow_id = workflow_id


class WorkflowAlreadyStarted(DurableWorkflowError):
    def __init__(self, workflow_id: str) -> None:
        super().__init__(f"workflow already started: {workflow_id}")
        self.workflow_id = workflow_id


class NamespaceNotFound(DurableWorkflowError):
    def __init__(self, namespace: str) -> None:
        super().__init__(f"namespace not found: {namespace}")
        self.namespace = namespace


class InvalidArgument(DurableWorkflowError):
    def __init__(self, message: str, errors: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.errors = errors


class Unauthorized(DurableWorkflowError):
    def __init__(self, message: str = "unauthorized") -> None:
        super().__init__(message)


class ScheduleNotFound(DurableWorkflowError):
    def __init__(self, schedule_id: str) -> None:
        super().__init__(f"schedule not found: {schedule_id}")
        self.schedule_id = schedule_id


class ScheduleAlreadyExists(DurableWorkflowError):
    def __init__(self, schedule_id: str) -> None:
        super().__init__(f"schedule already exists: {schedule_id}")
        self.schedule_id = schedule_id


class QueryFailed(DurableWorkflowError):
    pass


class UpdateRejected(DurableWorkflowError):
    pass


class ChildWorkflowFailed(DurableWorkflowError):
    def __init__(self, message: str, exception_class: str | None = None) -> None:
        super().__init__(message)
        self.exception_class = exception_class


class WorkflowTerminated(DurableWorkflowError):
    def __init__(self, message: str = "workflow was terminated") -> None:
        super().__init__(message)


class WorkflowCancelled(DurableWorkflowError):
    def __init__(self, message: str = "workflow was cancelled") -> None:
        super().__init__(message)


class ActivityCancelled(DurableWorkflowError):
    def __init__(self, message: str = "activity was cancelled") -> None:
        super().__init__(message)


class NonRetryableError(DurableWorkflowError):
    def __init__(self, message: str, *, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.__cause__ = cause


def _raise_for_status(status: int, body: object, *, context: str = "") -> None:
    if status < 400:
        return

    reason = body.get("reason") if isinstance(body, dict) else None
    message = body.get("message", "") if isinstance(body, dict) else str(body)

    if status == 401:
        raise Unauthorized(message or "unauthorized")

    if status == 404:
        if reason == "query_not_found":
            raise QueryFailed(message or "query not found")
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
        if reason == "query_rejected":
            raise QueryFailed(message or "query rejected")
        if reason == "update_rejected":
            raise UpdateRejected(message or "update rejected")
        raise ServerError(status, body)

    if status == 422:
        errors = None
        if isinstance(body, dict):
            errors = body.get("errors") or body.get("validation_errors")
        raise InvalidArgument(message, errors)

    raise ServerError(status, body)
