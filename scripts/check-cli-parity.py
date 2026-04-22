#!/usr/bin/env python3
"""Fail if any control-plane parity fixture drifts between this repo and the CLI.

The CLI and SDK-Python both carry a copy of each shared parity fixture under
tests/fixtures/control-plane/. Each copy projects the language-specific call
(the `cli` and `sdk_python` sections) alongside the shared wire contract
(`schema`, `version`, `operation`, `request`, `semantic_body`, `response_body`).

This script enforces that both repos carry the same fixture filenames and that
every fixture is byte-identical. Byte-level parity catches both
shared-contract drift (a request body that differs between CLI and SDK tests)
and language-projection drift (a CLI argv spelling in the SDK copy that no
longer matches the CLI copy's argv spelling).

Usage:
    python scripts/check-cli-parity.py --cli path/to/cli-checkout

Exits 0 if both repos carry the same byte-identical fixtures. Exits 1 with a
missing-file report or unified diff for each divergent fixture otherwise.
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

    sdk_names = set(sdk_fixtures)
    cli_names = set(cli_fixtures)
    common = sorted(sdk_names & cli_names)
    if not common:
        print(
            "No shared parity fixtures found. Check that the CLI checkout path is correct.",
            file=sys.stderr,
        )
        return 1

    cli_only = sorted(cli_names - sdk_names)
    sdk_only = sorted(sdk_names - cli_names)
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
    if not cli_only and not sdk_only and not divergent:
        print("All shared fixtures are byte-identical.")
        return 0

    if cli_only or sdk_only:
        print("\nParity fixture filename drift detected:\n", file=sys.stderr)
        if cli_only:
            print("Present in CLI only:", file=sys.stderr)
            for name in cli_only:
                print(f"  - {FIXTURES_SUBPATH / name}", file=sys.stderr)
            print(file=sys.stderr)
        if sdk_only:
            print("Present in SDK-Python only:", file=sys.stderr)
            for name in sdk_only:
                print(f"  - {FIXTURES_SUBPATH / name}", file=sys.stderr)
            print(file=sys.stderr)

    if not divergent:
        print(
            "Reconcile the fixture set so both repos advertise the same shared control-plane operations.",
            file=sys.stderr,
        )
        return 1

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
