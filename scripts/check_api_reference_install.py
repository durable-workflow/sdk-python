#!/usr/bin/env python3
"""Validate and optionally exercise the API reference's first install command."""

from __future__ import annotations

import argparse
import html.parser
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

try:
    from scripts.api_reference_release import ReleaseIdentity, load_release_identity, render_release_identity
except ModuleNotFoundError as error:  # pragma: no cover - used by the documented command-line entry point
    if error.name != "scripts":
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.api_reference_release import ReleaseIdentity, load_release_identity, render_release_identity


class InstallCodeParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.commands: list[str] = []
        self._in_install_section = False
        self._pre_depth = 0
        self._capturing = False
        self._parts: list[str] = []
        self._in_versioning_section = False
        self.versioning_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "h2":
            self._in_install_section = attributes.get("id") == "install"
            self._in_versioning_section = attributes.get("id") == "versioning"
        elif tag == "pre" and self._in_install_section:
            self._pre_depth += 1
        elif tag == "code" and self._pre_depth:
            self._capturing = True
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._capturing:
            self._parts.append(data)
        if self._in_versioning_section:
            self.versioning_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "code" and self._capturing:
            self.commands.append("".join(self._parts).strip())
            self._capturing = False
            self._parts = []
        elif tag == "pre" and self._pre_depth:
            self._pre_depth -= 1


def markdown_install_command(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    section = re.search(r"(?ms)^## Install\s*$\n(?P<body>.*?)(?=^##\s|\Z)", text)
    if section is None:
        raise ValueError(f"{path} has no Install section")
    block = re.search(r"(?ms)^```(?:bash|shell)\s*$\n(?P<code>.*?)^```\s*$", section.group("body"))
    if block is None:
        raise ValueError(f"{path} Install section has no shell command")
    commands = [line.strip() for line in block.group("code").splitlines() if line.strip()]
    if not commands:
        raise ValueError(f"{path} Install section has an empty shell block")
    return commands[0]


def validate_source_templates(repo_root: Path, identity: ReleaseIdentity) -> None:
    reference = (repo_root / "docs" / "index.md").read_text(encoding="utf-8")
    render_release_identity(reference, identity)
    if re.search(r"\b[0-9]+\.[0-9]+\.[0-9]+-(?:alpha|beta|rc)\.[0-9]+\b", reference, re.IGNORECASE):
        raise ValueError("docs/index.md must derive exact prerelease identities from pyproject.toml")

    readme = repo_root / "README.md"
    expected = ["pip", "install", "--pre", "durable-workflow~=2.0.0rc0"]
    command = markdown_install_command(readme)
    if shlex.split(command) != expected:
        raise ValueError(f"README first install command must select the 2.0 prerelease line, got {command!r}")


def validate_command(command: str, identity: ReleaseIdentity) -> str:
    arguments = shlex.split(command)
    expected = ["pip", "install", identity.requirement]
    if arguments != expected:
        raise ValueError(f"First install command must be {identity.install_command!r}, got {command!r}")
    return arguments[2]


def rendered_install_command(site: Path) -> str:
    index = site / "index.html"
    parser = InstallCodeParser()
    parser.feed(index.read_text(encoding="utf-8"))
    if not parser.commands:
        raise ValueError(f"{index} has no rendered bash command")
    return parser.commands[0]


def validate_rendered_site(site: Path, identity: ReleaseIdentity) -> str:
    index = site / "index.html"
    parser = InstallCodeParser()
    parser.feed(index.read_text(encoding="utf-8"))
    if not parser.commands:
        raise ValueError(f"{index} has no rendered bash command")
    command = parser.commands[0]
    qualification = " ".join("".join(parser.versioning_parts).split())
    expected_server = f"durableworkflow/server:{identity.server_version}"
    if identity.version not in qualification or expected_server not in qualification:
        raise ValueError(
            "Rendered Versioning section does not contain the manifest-derived SDK and Server release tuple"
        )
    return validate_command(command, identity)


PUBLIC_PYPI_INDEX = "https://pypi.org/simple"


def install_public_requirement(
    python: Path,
    requirement: str,
    *,
    cwd: Path,
    attempts: int,
    retry_sleep: float,
) -> None:
    """Install an exact requirement from public PyPI, retrying propagation delays."""
    if attempts < 1:
        raise ValueError("Public PyPI install attempts must be at least 1")
    if retry_sleep < 0:
        raise ValueError("Public PyPI install retry sleep must not be negative")

    command = [
        str(python),
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "--no-cache-dir",
        "--index-url",
        PUBLIC_PYPI_INDEX,
        requirement,
    ]
    env = {**os.environ, "PIP_CONFIG_FILE": os.devnull, "PIP_INDEX_URL": PUBLIC_PYPI_INDEX, "PYTHONPATH": ""}
    for variable in ("PIP_EXTRA_INDEX_URL", "PIP_FIND_LINKS", "PIP_NO_INDEX"):
        env.pop(variable, None)

    result: subprocess.CompletedProcess[str] | None = None
    for attempt in range(1, attempts + 1):
        result = subprocess.run(command, cwd=cwd, env=env, check=False, text=True)
        if result.returncode == 0:
            return
        if attempt < attempts:
            print(
                f"Public PyPI does not yet serve {requirement}; retrying in {retry_sleep:g}s ({attempt}/{attempts})",
                file=sys.stderr,
            )
            time.sleep(retry_sleep)

    assert result is not None
    raise subprocess.CalledProcessError(result.returncode, command)


def run_clean_install(
    requirement: str,
    identity: ReleaseIdentity,
    *,
    install_attempts: int = 6,
    install_retry_sleep: float = 20,
) -> None:
    with tempfile.TemporaryDirectory(prefix="dw-api-reference-install-") as temporary:
        clean_root = Path(temporary)
        venv = clean_root / ".venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        install_public_requirement(
            python,
            requirement,
            cwd=clean_root,
            attempts=install_attempts,
            retry_sleep=install_retry_sleep,
        )
        check = """
import importlib.metadata
import os

import durable_workflow

expected = os.environ["EXPECTED_DURABLE_WORKFLOW_VERSION"]
installed = importlib.metadata.version("durable-workflow")
if installed != expected:
    raise SystemExit(f"installed durable-workflow {installed}, expected {expected}")
if durable_workflow.__version__ != expected:
    raise SystemExit(f"imported durable-workflow {durable_workflow.__version__}, expected {expected}")
print(f"clean API-reference install imported durable-workflow {installed}")
"""
        env = {
            **os.environ,
            "EXPECTED_DURABLE_WORKFLOW_VERSION": identity.registry_version,
            "PYTHONPATH": "",
        }
        subprocess.run([str(python), "-c", check], cwd=clean_root, env=env, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--site", type=Path, default=Path("site"))
    parser.add_argument(
        "--install",
        action="store_true",
        help="Install the rendered requirement from the package registry in a clean virtual environment.",
    )
    parser.add_argument(
        "--install-attempts",
        type=int,
        default=6,
        help="Number of public PyPI attempts allowed while a new release propagates.",
    )
    parser.add_argument(
        "--install-retry-sleep",
        type=float,
        default=20,
        help="Seconds between public PyPI install attempts.",
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    site = args.site if args.site.is_absolute() else repo_root / args.site
    identity = load_release_identity(repo_root)
    validate_source_templates(repo_root, identity)
    requirement = validate_rendered_site(site, identity)
    if args.install:
        run_clean_install(
            requirement,
            identity,
            install_attempts=args.install_attempts,
            install_retry_sleep=args.install_retry_sleep,
        )
    print(
        f"API-reference install command selects {identity.package} {identity.version} "
        f"with Server {identity.server_version}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
