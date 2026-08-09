#!/usr/bin/env python3
"""Validate the immutable source tuple used by a release docs deployment."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

try:
    import tomllib  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - exercised by the Python 3.10 CI cell
    import tomli as tomllib  # type: ignore[import-not-found]


OBJECT_ID = re.compile(r"[0-9a-f]{40}")
RELEASE_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?")


def git(repo_root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def validate_release_source(
    repo_root: Path,
    *,
    source_sha: str,
    parent_sha: str,
    release_version: str,
) -> None:
    """Fail unless the checkout, tag, parent, and package manifest identify one release."""
    if OBJECT_ID.fullmatch(source_sha) is None:
        raise ValueError("Release docs source SHA must be an exact lowercase Git object ID")
    if OBJECT_ID.fullmatch(parent_sha) is None:
        raise ValueError("Release docs parent SHA must be an exact lowercase Git object ID")
    if source_sha == parent_sha:
        raise ValueError("Release docs source and parent SHAs must differ")
    if RELEASE_VERSION.fullmatch(release_version) is None:
        raise ValueError("Release docs version must be an exact SDK release version")

    observed_source = git(repo_root, "rev-parse", "HEAD")
    if observed_source != source_sha:
        raise ValueError(f"Release docs checkout is {observed_source}, expected {source_sha}")

    observed_parent = git(repo_root, "rev-parse", "HEAD^1")
    if observed_parent != parent_sha:
        raise ValueError(f"Release docs parent is {observed_parent}, expected {parent_sha}")

    with (repo_root / "pyproject.toml").open("rb") as file:
        manifest = tomllib.load(file)
    package_version = manifest["project"]["version"]
    product_train = manifest["tool"]["durable-workflow"]["product-train"]
    if package_version != release_version or product_train != release_version:
        raise ValueError("Release docs version does not match project.version and tool.durable-workflow.product-train")

    try:
        tagged_source = git(repo_root, "rev-parse", "--verify", f"refs/tags/{release_version}^{{commit}}")
    except subprocess.CalledProcessError as error:
        raise ValueError(f"Release docs version has no immutable source tag: {release_version}") from error
    if tagged_source != source_sha:
        raise ValueError(f"Release docs tag points to {tagged_source}, expected {source_sha}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--parent-sha", required=True)
    parser.add_argument("--release-version", required=True)
    args = parser.parse_args()

    try:
        validate_release_source(
            args.repo_root.resolve(),
            source_sha=args.source_sha,
            parent_sha=args.parent_sha,
            release_version=args.release_version,
        )
    except (KeyError, OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"release docs source validation failed: {error}", file=sys.stderr)
        return 1

    print(f"Validated release docs source {args.release_version}@{args.source_sha} (parent {args.parent_sha})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
