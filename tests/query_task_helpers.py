"""Ordinary ephemeral lease metadata for query transport and replay tests."""

from datetime import datetime, timedelta, timezone
from typing import Any

from durable_workflow import Worker


def query_task_delivery(task: dict[str, Any], *, worker_id: str, task_queue: str) -> dict[str, Any]:
    return {"query_task_attempt": 1, "lease_owner": worker_id, "task_queue": task_queue,
            "lease_expires_at": (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat(), **task}


def leased_query_task(worker: Worker, task: dict[str, Any]) -> dict[str, Any]:
    return query_task_delivery(task, worker_id=worker.worker_id, task_queue=worker.task_queue)
