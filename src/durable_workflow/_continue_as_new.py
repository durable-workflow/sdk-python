"""Worker protocol contract for targeted continue-as-new."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Literal, TypeAlias, TypeGuard

ContinueAsNewTargetBuild: TypeAlias = Literal["inherited", "promoted"]

CAPABILITY = "continue_as_new_target_build"
WORKER_PROTOCOL = "1.21"
PROMOTED_BUILD_UNAVAILABLE = "promoted_build_unavailable"


def valid_target(value: object) -> TypeGuard[ContinueAsNewTargetBuild]:
    return isinstance(value, str) and value in ("inherited", "promoted")


def registration_supports_target(ack: object, worker_id: str, namespace: str | None, task_queue: str) -> bool:
    if (
        not isinstance(ack, Mapping)
        or namespace is None
        or ack.get("registered") is not True
        or ack.get("worker_id") != worker_id
        or ack.get("namespace") != namespace
        or ack.get("task_queue") != task_queue
    ):
        return False
    capabilities = ack.get("server_capabilities")
    if not isinstance(capabilities, Mapping) or capabilities.get(CAPABILITY) is not True:
        return False
    protocol = ack.get("protocol_version")
    match = re.fullmatch(r"1\.([0-9]{1,4})", protocol) if isinstance(protocol, str) else None
    return match is not None and int(match[1]) >= 21
