"""Bind the retry case to the independently reviewed SDK release bytes."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from scripts.ci.activity_acceptance_entry import captured_file

VERSION = "2.18.0"
WHEEL = f"durable_workflow-{VERSION}-py3-none-any.whl"
SDIST = f"durable_workflow-{VERSION}.tar.gz"


def lock(dependencies: bytes, approval: dict) -> tuple[bytes, str]:
    if approval["version"] != VERSION or set(approval["files"]) != {WHEEL, SDIST}:
        raise ValueError("retry case requires its exact reviewed release")
    digest = approval["files"][WHEEL]
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("retry case requires the approved wheel digest")
    if re.search(rb"(?im)^[ \t]*durable[-_]workflow\b", dependencies):
        raise ValueError("retry dependency lock already contains an SDK selection")
    result = dependencies.rstrip() + f"\ndurable-workflow=={VERSION} --hash=sha256:{digest}\n".encode()
    return result, digest


def candidate_wheel(root: Path, digest: str) -> bytes:
    # The protected download/extract steps validate the same-repository build
    # and complete handoff archive. This read binds the consumed bytes again.
    data = captured_file(root / "dist" / WHEEL)
    if hashlib.sha256(data).hexdigest() != digest:
        raise ValueError("candidate wheel differs from the approved release")
    return data
