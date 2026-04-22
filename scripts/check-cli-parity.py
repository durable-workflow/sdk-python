#!/usr/bin/env python3
"""Fail if any control-plane parity fixture drifts between this repo and the CLI.

The CLI and SDK-Python both carry a copy of each shared parity fixture under
tests/fixtures/control-plane/. Each copy projects the language-specific call
(the `cli` and `sdk_python` sections) alongside the shared wire contract
(`schema`, `version`, `operation`, `request`, `semantic_body`, `response_body`).

This script enforces that whenever a fixture file exists in both repos, every
byte of that file matches. Byte-level parity catches both shared-contract
drift (a request body that differs between CLI and SDK tests) and
language-projection drift (a CLI argv spelling in the SDK copy that no longer
matches the CLI copy's argv spelling).

Usage:
    python scripts/check-cli-parity.py --cli path/to/cli-checkout

Exits 0 if every common fixture is byte-identical. Exits 1 with a unified diff
for each divergent fixture otherwise.
"""

from __future__ import annotations

import argparse
import difflib
import sys
from pathlib import Path

FIXTURES_SUBPATH = Path("tests/fixtures/control-plane")


def list_fixtures(root: Path) -> dict[str, Path]:
    directory = root / FIXTURES_SUBPATH
    if not directory.is_dir():
        raise SystemExit(f"Fixtures directory not found: {directory}")
    return {p.name: p for p in sorted(directory.glob("*-parity.json"))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cli",
        required=True,
        type=Path,
        help="Path to a checkout of the durable-workflow/cli repository.",
    )
    parser.add_argument(
        "--self",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Path to this (sdk-python) repository. Defaults to the repo that contains this script.",
    )
    args = parser.parse_args()

    sdk_fixtures = list_fixtures(args.self)
    cli_fixtures = list_fixtures(args.cli)

    common = sorted(set(sdk_fixtures) & set(cli_fixtures))
    if not common:
        print(
            "No shared parity fixtures found. Check that the CLI checkout path is correct.",
            file=sys.stderr,
        )
        return 1

    divergent: list[tuple[str, str]] = []
    for name in common:
        sdk_bytes = sdk_fixtures[name].read_bytes()
        cli_bytes = cli_fixtures[name].read_bytes()
        if sdk_bytes == cli_bytes:
            continue
        sdk_text = sdk_bytes.decode("utf-8", errors="replace").splitlines(keepends=True)
        cli_text = cli_bytes.decode("utf-8", errors="replace").splitlines(keepends=True)
        diff = "".join(
            difflib.unified_diff(
                cli_text,
                sdk_text,
                fromfile=f"cli/{FIXTURES_SUBPATH / name}",
                tofile=f"sdk-python/{FIXTURES_SUBPATH / name}",
            )
        )
        divergent.append((name, diff))

    print(
        f"Compared {len(common)} shared parity fixture(s) between CLI and SDK-Python."
    )
    if not divergent:
        print("All shared fixtures are byte-identical.")
        return 0

    print(f"\n{len(divergent)} fixture(s) drifted:\n", file=sys.stderr)
    for name, diff in divergent:
        print(f"--- {name} ---", file=sys.stderr)
        print(diff, file=sys.stderr)
    print(
        "Reconcile the fixtures so the CLI and Python SDK agree on shared control-plane contracts.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
