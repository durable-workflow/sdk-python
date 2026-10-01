#!/usr/bin/env python3
"""Checkout a public integration source from its GitHub authority."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path

PUBLIC_REPOSITORIES = {
    "cli": "https://github.com/durable-workflow/cli.git",
    "server": "https://github.com/durable-workflow/server.git",
}


def checkout(repository: str, destination: Path, commit: str = "") -> None:
    """Clone a supported public repository without runner-host credentials."""
    if commit and re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("integration source commit must be a full lowercase Git SHA")
    environment = os.environ.copy()
    environment["GIT_TERMINAL_PROMPT"] = "0"

    subprocess.run(
        [
            "git",
            "-c",
            "credential.helper=",
            "clone",
            "--depth=1",
            "--no-tags",
            PUBLIC_REPOSITORIES[repository],
            str(destination),
        ],
        check=True,
        env=environment,
    )
    if commit:
        git = ["git", "-c", "credential.helper=", "-C", str(destination)]
        subprocess.run([*git, "fetch", "--depth=1", "origin", commit], check=True, env=environment)
        subprocess.run([*git, "checkout", "--detach", commit], check=True, env=environment)
        resolved = subprocess.run(
            [*git, "rev-parse", "HEAD"], check=True, env=environment, capture_output=True, text=True,
        ).stdout.strip()
        if resolved != commit:
            raise RuntimeError("integration checkout did not resolve the requested commit")
        print(f"Integration {repository} source commit: {resolved}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", choices=sorted(PUBLIC_REPOSITORIES))
    parser.add_argument("destination", type=Path)
    parser.add_argument(
        "--commit", default="", help="Exact public candidate SHA. Defaults to the repository default branch.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    checkout(args.repository, args.destination, args.commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
