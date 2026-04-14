class DurableWorkflowError(Exception):
    pass


class ServerError(DurableWorkflowError):
    def __init__(self, status: int, body: object):
        super().__init__(f"server returned {status}: {body!r}")
        self.status = status
        self.body = body


class WorkflowFailed(DurableWorkflowError):
    def __init__(self, message: str, exception_class: str | None = None):
        super().__init__(message)
        self.exception_class = exception_class
