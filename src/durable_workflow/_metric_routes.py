"""Bound request labels to reviewed SDK operations, never arbitrary URL values."""

from __future__ import annotations

_ROUTES = (
    "/", "/health", "/cluster/info", "/activities", "/activities/{activity_id}",
    "/namespaces", "/namespaces/{name}", "/namespaces/{name}/external-storage",
    "/search-attributes", "/search-attributes/{name}", "/storage/test",
    "/task-queues", "/task-queues/{task_queue}", "/task-queues/{task_queue}/build-ids",
    *(f"/task-queues/{{task_queue}}/build-ids/{action}" for action in ("drain", "promote", "resume")),
    *(f"/system/{operation}{suffix}" for operation in ("activity-timeouts", "repair", "retention")
      for suffix in ("", "/pass")),
    "/workers", "/workers/{worker_id}",
    "/worker/register", "/worker/heartbeat", "/worker/registrations/{worker_id}",
    "/worker/sessions", "/worker/sessions/{session_id}", "/worker/sessions/{session_id}/heartbeat",
    *(f"/worker/{kind}-tasks/poll" for kind in ("workflow", "activity", "query", "update-validation")),
    *(f"/worker/{kind}-tasks/{{task_id}}/{action}" for kind, actions in (
        ("workflow", ("complete", "fail", "heartbeat", "history", "deliver-cancellation")),
        ("activity", ("complete", "fail", "heartbeat", "status", "renew-lease", "acknowledge-cancellation")),
        ("query", ("complete", "fail", "status")),
        ("update-validation", ("approve", "reject")),
    ) for action in actions),
    *(f"/worker/workflow-tasks/{{task_id}}/cancellation-scopes/{action}"
      for action in ("open", "prepare", "deliver")),
    *(f"/worker/workflow-tasks/{{task_id}}/local-activities/{action}"
      for action in ("checkpoint", "checkpoint-group", "prepare", "recover")),
    *(f"/worker/workflow-tasks/{{task_id}}/local-activities/{{activity_attempt_id}}/{action}"
      for action in ("control", "heartbeat", "outcome", "acknowledge-cancellation")),
    "/workflows", "/workflows/{workflow_id}",
    *(f"/workflows/{{workflow_id}}/{action}"
      for action in ("archive", "cancel", "repair", "terminate", "request-cancellation", "runs")),
    *(f"/workflows/{{workflow_id}}/{action}/{{name}}" for action in ("signal", "query", "update")),
    "/workflows/{workflow_id}/message-streams/{name}/messages",
    "/workflows/{workflow_id}/runs/{run_id}",
    *(f"/workflows/{{workflow_id}}/runs/{{run_id}}/{action}"
      for action in ("history", "history/export", "redrive", "request-cancellation", "streams")),
    "/workflows/{workflow_id}/runs/{run_id}/streams/{name}",
    *(f"/workflows/{{workflow_id}}/runs/{{run_id}}/streams/{{name}}/{action}" for action in ("items", "close")),
    "/schedules", "/schedules/{schedule_id}",
    *(f"/schedules/{{schedule_id}}/{action}" for action in ("backfill", "pause", "resume", "trigger", "history")),
    "/bridge-adapters/webhook/{adapter}",
    "/service-endpoints/{endpoint_name}/services/{service_name}/operations/{operation_name}/execute",
)
_TEMPLATES = tuple((route, tuple(route.strip("/").split("/"))) for route in _ROUTES)


def route_for_metrics(path: str) -> str:
    """Return a supported operation template or the single bounded fallback."""
    clean_path = path.split("?", 1)[0].split("#", 1)[0]
    parts = tuple(clean_path.strip("/").split("/"))
    for route, template in _TEMPLATES:
        if len(parts) == len(template) and all(
            expected == actual or expected.startswith("{") and bool(actual)
            for actual, expected in zip(parts, template, strict=True)
        ):
            return route
    return "unknown"
