#!/usr/bin/env python3
"""Classify pull-request changes for focused or complete qualification."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath

FOCUSED_DOCUMENTATION = "focused-documentation"
COMPLETE = "complete"

DOCS_ONLY_PREFIXES = (
    "docs/",
    "overrides/",
)
DOCS_ONLY_FILES = frozenset(
    {
        "mkdocs.yml",
        "scripts/ci/classify_docs_visual_changes.py",
        "scripts/ci/test-classify-docs-visual-changes.py",
        "scripts/check_api_reference_install.py",
        "scripts/check-docs-analytics.py",
        "scripts/check-docs-layout.py",
        "scripts/mkdocs_hooks.py",
        "scripts/qualify-docs-promotion.py",
    }
)

_OBJECT_ID = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")


@dataclass(frozen=True)
class Qualification:
    classification: str
    reason: str
    changed_files: tuple[str, ...]


class ChangedPathIdentityError(RuntimeError):
    """Raised when the exact pull-request path set cannot be established."""


def _is_canonical_repo_path(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path or any(character in path for character in "\0\r\n"):
        return False
    parts = PurePosixPath(path).parts
    return bool(parts) and all(part not in {"", ".", ".."} for part in parts) and str(PurePosixPath(path)) == path


def _is_documentation_path(path: str) -> bool:
    return path in DOCS_ONLY_FILES or path.startswith(DOCS_ONLY_PREFIXES)


def classify_changed_files(changed_files: Sequence[str]) -> Qualification:
    paths = tuple(sorted(set(changed_files)))
    if not paths:
        return Qualification(COMPLETE, "changed-path-identity-unavailable", paths)
    if any(not _is_canonical_repo_path(path) for path in paths):
        return Qualification(COMPLETE, "changed-path-identity-unavailable", paths)
    if all(_is_documentation_path(path) for path in paths):
        return Qualification(FOCUSED_DOCUMENTATION, "all-changed-paths-are-documentation", paths)
    return Qualification(COMPLETE, "runtime-sensitive-or-unclassified-path", paths)


def changed_files_between(root: Path, base_ref: str, head_ref: str) -> tuple[str, ...]:
    if not _OBJECT_ID.fullmatch(base_ref) or not _OBJECT_ID.fullmatch(head_ref):
        raise ChangedPathIdentityError("base and head revisions must be immutable object IDs")

    try:
        for revision in (base_ref, head_ref):
            subprocess.run(
                ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
                cwd=root,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        result = subprocess.run(
            ["git", "diff", "--name-only", "-z", "--no-renames", f"{base_ref}...{head_ref}", "--"],
            cwd=root,
            check=True,
            capture_output=True,
        )
        return tuple(path.decode("utf-8") for path in result.stdout.split(b"\0") if path)
    except (OSError, subprocess.CalledProcessError, UnicodeDecodeError) as error:
        raise ChangedPathIdentityError("unable to resolve the pull-request path set") from error


def write_github_output(path: Path, qualification: Qualification) -> None:
    with path.open("a", encoding="utf-8") as output:
        print(f"classification={qualification.classification}", file=output)
        print(f"reason={qualification.reason}", file=output)
        print(f"changed_count={len(qualification.changed_files)}", file=output)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--event-name")
    parser.add_argument("--base-ref")
    parser.add_argument("--head-ref")
    parser.add_argument("--changed-file", action="append", dest="changed_files")
    parser.add_argument("--github-output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.changed_files is not None:
        qualification = classify_changed_files(args.changed_files)
    elif args.event_name and args.event_name != "pull_request":
        qualification = Qualification(COMPLETE, "non-pull-request-event", ())
    elif args.base_ref and args.head_ref:
        try:
            changed_files = changed_files_between(args.root.resolve(), args.base_ref, args.head_ref)
        except ChangedPathIdentityError:
            qualification = Qualification(COMPLETE, "changed-path-identity-unavailable", ())
        else:
            qualification = classify_changed_files(changed_files)
    else:
        qualification = Qualification(COMPLETE, "changed-path-identity-unavailable", ())

    if args.github_output is not None:
        write_github_output(args.github_output, qualification)
    print(json.dumps(asdict(qualification), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
