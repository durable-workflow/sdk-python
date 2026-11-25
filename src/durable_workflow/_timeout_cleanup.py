"""Worker side of deadline-triggered timeout cleanup (server#1201, workflow#863).

A worker that delivers cooperative cancellation advertises ``timeout_cleanup``
version 1, so the Server admits starts that reserve cleanup time on its queue.
When the engine's timeout request reaches workflow code as
:class:`~durable_workflow.errors.WorkflowTimeoutCleanupRequested` and the
workflow lets that exception escape, uncaught or re-raised, the
``fail_workflow`` command carries ``failure.interruption: "timeout_cleanup"``.
The Server then records the cleanup as completed, and the run still closes as
timed out. Any other failure, including one that wraps the interruption, is an
ordinary failure, which the Server records as failed cleanup. The marker never
starts cleanup; the Server accepts it only while the run's own engine timeout
request is active.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .cancellation import CancellationContext
from .errors import WorkflowTimeoutCleanupRequested

CAPABILITY = "timeout_cleanup"
INTERRUPTION = "timeout_cleanup"
MINIMUM_PROTOCOL_VERSION = "1.20"
VERSION = 1


def manifest_entry() -> dict[str, Any]:
    """The registration ``capability_manifest`` entry native Server admits."""
    return {"supported": True, "minimum_protocol_version": MINIMUM_PROTOCOL_VERSION, "version": VERSION}


def failure_interruption(
    failure: BaseException, *, delivered: Sequence[BaseException], run_id: str,
) -> str | None:
    """The portable marker for an escaped timeout interruption, else ``None``.

    Only the exception object the runtime raised for this run's root request
    with ``cause: "timeout"`` qualifies. An exception the application
    constructs, a different exception raised from or wrapping the delivered
    one, an ordinary or explicit ``cause: "cancellation"`` request and a
    descendant or scoped request are not marked. The type alone never decides.
    """
    if not isinstance(failure, WorkflowTimeoutCleanupRequested):
        return None
    if not any(failure is candidate for candidate in delivered):
        return None
    context = failure.context
    if (
        not isinstance(context, CancellationContext)
        or not context.is_timeout
        or context.scope_origin is not None
        or failure.request_id is None
        or failure.request_id != context.request_id
        or context.request_id != context.root_request_id
        or context.parent_request_id is not None
        or not run_id
        or context.root_workflow_run_id != run_id
    ):
        return None
    return INTERRUPTION
