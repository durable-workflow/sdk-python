#!/usr/bin/env python3
"""Checkout a public integration source from its GitHub authority."""

from __future__ import annotations

import argparse
import os
import subprocess
from collections.abc import Sequence
from pathlib import Path

PUBLIC_REPOSITORIES = {
    "cli": "https://github.com/durable-workflow/cli.git",
    "server": "https://github.com/durable-workflow/server.git",
}


def checkout(repository: str, destination: Path) -> None:
    """Clone a supported public repository without runner-host credentials."""
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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", choices=sorted(PUBLIC_REPOSITORIES))
    parser.add_argument("destination", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    checkout(args.repository, args.destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
