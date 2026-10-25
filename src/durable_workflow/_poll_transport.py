"""Negotiated long-poll responses and early transport failure diagnostics."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from typing import Any

import httpx

MEDIA_TYPE = "application/vnd.durable-workflow.poll.v1+json"
ACCEPT = f"{MEDIA_TYPE}, application/json;q=0.9"
_SCHEMA = "durable-workflow.poll-transport.v1"
POLL_PATHS = frozenset(
    f"/worker/{kind}-tasks/poll" for kind in ("workflow", "activity", "query", "update-validation")
)
_HEADERS = frozenset(("retry-after", "x-durable-workflow-protocol-version"))
log = logging.getLogger(__name__)


def unwrap_response(response: httpx.Response) -> httpx.Response:
    if response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != MEDIA_TYPE:
        return response

    try:
        envelope = response.json()
        if not isinstance(envelope, dict) or envelope.get("schema") != _SCHEMA:
            raise ValueError("unrecognized poll transport")
        status, headers, body = envelope.get("status"), envelope.get("headers"), envelope.get("body")
        if (type(status) is not int or not 200 <= status < 600
                or response.status_code not in (200, status)
                or not isinstance(body, dict) or not isinstance(headers, dict)
                or any(key not in _HEADERS or not isinstance(value, str)
                       or "\r" in value or "\n" in value for key, value in headers.items())):
            raise ValueError("invalid poll transport result")
        # Apply the original status and retry headers before the normal client
        # error handling. Outer HTTP 200 may have been committed by progress.
        return httpx.Response(status, headers=headers, json=body, request=response.request)
    except (ValueError, TypeError, UnicodeError) as error:
        raise httpx.RemoteProtocolError("Invalid worker poll transport response.", request=response.request) from error


def warn_early_cutoff(path: str, body: Any, started: float, error: Exception) -> None:
    wait = body.get("timeout_seconds") if isinstance(body, Mapping) else None
    observed = time.monotonic() - started
    if isinstance(wait, bool) or not isinstance(wait, int | float) or wait <= 0 or observed >= wait:
        return
    if isinstance(error, httpx.HTTPStatusError) and error.response.status_code not in (502, 504):
        return
    log.warning(
        "Worker long poll %s ended after %.3fs before its configured wait of %.3fs (%s). "
        "Check proxy and HTTP timeout budgets.",
        path, observed, wait, type(error).__name__,
    )
