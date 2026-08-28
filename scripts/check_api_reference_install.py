#!/usr/bin/env python3
"""Validate and optionally exercise the API reference's first install command."""

from __future__ import annotations

import argparse
import html.parser
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from scripts.api_reference_release import (
        QUICKSTART_CONTRACT_SCHEMA,
        QUICKSTART_CONTRACT_URL,
        SUPPORTED_PRERELEASE_INSTALL_COMMAND,
        SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND,
        QualifiedOnboarding,
        ReleaseIdentity,
        load_qualified_onboarding,
        load_release_identity,
        render_onboarding_resolvers,
        validate_release_evidence,
        write_release_evidence,
    )
except ModuleNotFoundError as error:  # pragma: no cover - used by the documented command-line entry point
    if error.name != "scripts":
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.api_reference_release import (
        QUICKSTART_CONTRACT_SCHEMA,
        QUICKSTART_CONTRACT_URL,
        SUPPORTED_PRERELEASE_INSTALL_COMMAND,
        SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND,
        QualifiedOnboarding,
        ReleaseIdentity,
        load_qualified_onboarding,
        load_release_identity,
        render_onboarding_resolvers,
        validate_release_evidence,
        write_release_evidence,
    )


@dataclass(frozen=True)
class OnboardingCommands:
    install: str
    server_image: str


class InstallCodeParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.commands: list[str] = []
        self._in_install_section = False
        self._local_section_depth = 0
        self._pre_depth = 0
        self._capturing = False
        self._capture_target: str | None = None
        self._parts: list[str] = []
        self._in_versioning_section = False
        self.local_commands: list[str] = []
        self.release_authority_urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "section":
            if self._local_section_depth:
                self._local_section_depth += 1
            elif attributes.get("data-docs-journey") == "local-self-hosted":
                self._local_section_depth = 1
                self._in_install_section = False
        elif tag == "h2":
            self._in_install_section = attributes.get("id") == "install"
            self._in_versioning_section = attributes.get("id") == "versioning"
        elif tag == "pre" and (self._in_install_section or self._local_section_depth):
            self._pre_depth += 1
        elif tag == "code" and self._pre_depth:
            self._capturing = True
            self._capture_target = "install" if self._in_install_section else "local"
            self._parts = []
        if self._in_versioning_section and attributes.get("data-release-authority-url"):
            self.release_authority_urls.append(attributes["data-release-authority-url"] or "")

    def handle_data(self, data: str) -> None:
        if self._capturing:
            self._parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "code" and self._capturing:
            command = "".join(self._parts).strip()
            if self._capture_target == "install":
                self.commands.append(command)
            elif self._capture_target == "local":
                self.local_commands.append(command)
            self._capturing = False
            self._capture_target = None
            self._parts = []
        elif tag == "pre" and self._pre_depth:
            self._pre_depth -= 1
        elif tag == "section" and self._local_section_depth:
            self._local_section_depth -= 1


def markdown_install_command_from_text(text: str, source: str) -> str:
    section = re.search(r"(?ms)^## Install\s*$\n(?P<body>.*?)(?=^##\s|\Z)", text)
    if section is None:
        raise ValueError(f"{source} has no Install section")
    block = re.search(r"(?ms)^```(?:bash|shell)\s*$\n(?P<code>.*?)^```\s*$", section.group("body"))
    if block is None:
        raise ValueError(f"{source} Install section has no shell command")
    commands = [line.strip() for line in block.group("code").splitlines() if line.strip()]
    if not commands:
        raise ValueError(f"{source} Install section has an empty shell block")
    return commands[0]


def markdown_install_command(path: Path) -> str:
    return markdown_install_command_from_text(path.read_text(encoding="utf-8"), str(path))


def markdown_server_image_command_from_text(text: str, source: str) -> str:
    section = re.search(r"(?ms)^### 1\. Start Server\s*$\n(?P<body>.*?)(?=^###\s|\Z)", text)
    if section is None:
        raise ValueError(f"{source} has no first-run Server section")
    blocks = re.finditer(
        r"(?ms)^```(?:bash|shell)\s*$\n(?P<code>.*?)^```\s*$",
        section.group("body"),
    )
    for block in blocks:
        command = block.group("code").strip()
        if "DW_SERVER_IMAGE=" in command:
            return command
    raise ValueError(f"{source} first-run Server section has no image resolver")


def validate_source_templates(repo_root: Path) -> None:
    reference = (repo_root / "docs" / "index.md").read_text(encoding="utf-8")
    rendered_reference = render_onboarding_resolvers(reference)

    exact_prerelease = re.compile(
        r"\bv?[0-9]+\.[0-9]+\.[0-9]+-(?:alpha|beta|rc)\.[0-9]+\b|"
        r"\b[0-9]+\.[0-9]+\.[0-9]+(?:a|b|rc)[0-9]+\b",
        re.IGNORECASE,
    )
    for relative_path in ("README.md", "docs/index.md"):
        path = repo_root / relative_path
        if exact_prerelease.search(path.read_text(encoding="utf-8")):
            raise ValueError(f"{relative_path} must not hand-maintain an exact prerelease identity")
    validate_command(markdown_install_command(repo_root / "README.md"))
    validate_command(
        markdown_install_command_from_text(rendered_reference, "docs/index.md"),
    )
    validate_server_image_command(markdown_server_image_command_from_text(rendered_reference, "docs/index.md"))


def validate_command(command: str) -> str:
    arguments = shlex.split(command)
    expected = shlex.split(SUPPORTED_PRERELEASE_INSTALL_COMMAND)
    if arguments != expected:
        raise ValueError(
            f"First install command must use the supported prerelease resolver "
            f"{SUPPORTED_PRERELEASE_INSTALL_COMMAND!r}, got {command!r}"
        )
    return SUPPORTED_PRERELEASE_INSTALL_COMMAND


def validate_server_image_command(command: str) -> str:
    if "{{" in command or "}}" in command:
        raise ValueError("First-run Server command contains an unresolved documentation template placeholder")
    if command.strip() != SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND:
        raise ValueError("First-run Server command must use the public quickstart contract resolver")
    return SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND


def select_server_image_command(commands: Sequence[str], source: str) -> str:
    selected = [command.strip() for command in commands if "DW_SERVER_IMAGE=" in command]
    if len(selected) != 1:
        raise ValueError(f"{source} must contain exactly one Server image resolver")
    return selected[0]


def rendered_install_command(site: Path) -> str:
    index = site / "index.html"
    parser = InstallCodeParser()
    parser.feed(index.read_text(encoding="utf-8"))
    if not parser.commands:
        raise ValueError(f"{index} has no rendered bash command")
    return parser.commands[0]


def rendered_server_image_command(site: Path) -> str:
    index = site / "index.html"
    parser = InstallCodeParser()
    parser.feed(index.read_text(encoding="utf-8"))
    return select_server_image_command(parser.local_commands, str(index))


def validate_rendered_site(site: Path) -> OnboardingCommands:
    index = site / "index.html"
    parser = InstallCodeParser()
    parser.feed(index.read_text(encoding="utf-8"))
    if not parser.commands:
        raise ValueError(f"{index} has no rendered bash command")
    if parser.release_authority_urls != [QUICKSTART_CONTRACT_URL]:
        raise ValueError("Rendered Versioning authority does not identify the public quickstart contract")
    return OnboardingCommands(
        install=validate_command(parser.commands[0]),
        server_image=validate_server_image_command(select_server_image_command(parser.local_commands, str(index))),
    )


PUBLIC_PYPI_INDEX = "https://pypi.org/simple"


class PublicRequirementUnavailable(subprocess.CalledProcessError):
    """A required package version could not be installed from the public channel."""


def install_public_requirement(
    python: Path,
    requirement: str,
    *,
    cwd: Path,
    attempts: int,
    retry_sleep: float,
) -> None:
    """Install the supported prerelease requirement from public PyPI."""
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
    raise PublicRequirementUnavailable(result.returncode, command)


def _run_clean_requirement(
    requirement: str,
    *,
    expected_version: str | None,
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

expected = os.environ.get("EXPECTED_DURABLE_WORKFLOW_VERSION")
installed = importlib.metadata.version("durable-workflow")
if expected and installed != expected:
    raise SystemExit(f"installed durable-workflow {installed}, expected {expected}")
if durable_workflow.__version__ != installed:
    raise SystemExit(f"imported durable-workflow {durable_workflow.__version__}, installed {installed}")
print(f"clean API-reference install imported durable-workflow {installed}")
"""
        env = {
            **os.environ,
            "EXPECTED_DURABLE_WORKFLOW_VERSION": expected_version or "",
            "PYTHONPATH": "",
        }
        subprocess.run([str(python), "-c", check], cwd=clean_root, env=env, check=True)


def load_public_qualified_onboarding() -> QualifiedOnboarding:
    request = urllib.request.Request(QUICKSTART_CONTRACT_URL, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - fixed public HTTPS URL
        return load_qualified_onboarding(json.load(response))


def _run_clean_onboarding(commands: OnboardingCommands, qualified: QualifiedOnboarding) -> None:
    """Exercise both documented resolvers against one contract snapshot."""
    with tempfile.TemporaryDirectory(prefix="dw-api-reference-resolver-") as temporary:
        clean_root = Path(temporary)
        venv = clean_root / ".venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        bin_dir = venv / ("Scripts" if os.name == "nt" else "bin")
        python = bin_dir / ("python.exe" if os.name == "nt" else "python")
        pip = bin_dir / ("pip.exe" if os.name == "nt" else "pip")
        contract = clean_root / "quickstart-execution-contract.json"
        contract.write_text(
            json.dumps(
                {
                    "schema": QUICKSTART_CONTRACT_SCHEMA,
                    "artifacts": {
                        "sdk-python": {"version": qualified.sdk_version},
                        "server": {
                            "version": qualified.server_version,
                            "image": qualified.server_reference.rsplit(":", 1)[0],
                            "reference": qualified.server_reference,
                        },
                    },
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        env = {
            **os.environ,
            "DURABLE_WORKFLOW_QUICKSTART_CONTRACT_URL": contract.as_uri(),
            "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}",
            "PIP_BIN": str(pip),
            "PIP_CONFIG_FILE": os.devnull,
            "PIP_INDEX_URL": PUBLIC_PYPI_INDEX,
            "PYTHONPATH": "",
        }
        for variable in ("PIP_EXTRA_INDEX_URL", "PIP_FIND_LINKS", "PIP_NO_INDEX"):
            env.pop(variable, None)

        subprocess.run(["sh", "-c", commands.install], cwd=clean_root, env=env, check=True)
        check = """
import importlib.metadata
import os

import durable_workflow

expected = os.environ["EXPECTED_DURABLE_WORKFLOW_VERSION"]
installed = importlib.metadata.version("durable-workflow")
if installed != expected:
    raise SystemExit(f"qualified resolver installed durable-workflow {installed}, expected {expected}")
if durable_workflow.__version__ != installed:
    raise SystemExit(f"imported durable-workflow {durable_workflow.__version__}, installed {installed}")
print(f"clean API-reference resolver imported durable-workflow {installed}")
"""
        subprocess.run(
            [str(python), "-c", check],
            cwd=clean_root,
            env={**env, "EXPECTED_DURABLE_WORKFLOW_VERSION": qualified.sdk_registry_version},
            check=True,
        )
        server = subprocess.run(
            ["sh", "-c", f"set -eu\n{commands.server_image}\nprintf '%s\\n' \"$DW_SERVER_IMAGE\""],
            cwd=clean_root,
            env=env,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        )
        if server.stdout.strip() != qualified.server_reference:
            raise ValueError(
                f"Server resolver selected {server.stdout.strip()!r}, expected {qualified.server_reference!r}"
            )


def run_clean_install(
    commands: OnboardingCommands,
    identity: ReleaseIdentity,
    *,
    install_attempts: int = 6,
    install_retry_sleep: float = 20,
) -> None:
    """Require the exact release before exercising the documented resolver pair."""
    validate_command(commands.install)
    validate_server_image_command(commands.server_image)
    _run_clean_requirement(
        identity.exact_requirement,
        expected_version=identity.registry_version,
        install_attempts=install_attempts,
        install_retry_sleep=install_retry_sleep,
    )
    _run_clean_onboarding(commands, load_public_qualified_onboarding())


def verify_public_deployment(
    url: str,
    identity: ReleaseIdentity,
    source_revision: str,
    *,
    attempts: int,
    retry_sleep: float,
) -> dict[str, Any]:
    """Wait for the live release record to identify the deployed release source."""
    if attempts < 1:
        raise ValueError("Public deployment attempts must be at least 1")
    if retry_sleep < 0:
        raise ValueError("Public deployment retry sleep must not be negative")
    if not url.startswith("https://"):
        raise ValueError("Public deployment evidence URL must use HTTPS")

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - fixed public HTTPS URL
                evidence = json.load(response)
            return validate_release_evidence(evidence, identity, source_revision)
        except (OSError, ValueError) as error:
            last_error = error
            if attempt < attempts:
                print(
                    f"Public API reference does not yet identify {source_revision}; "
                    f"retrying in {retry_sleep:g}s ({attempt}/{attempts})",
                    file=sys.stderr,
                )
                time.sleep(retry_sleep)

    assert last_error is not None
    raise last_error


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--site", type=Path, default=Path("site"))
    parser.add_argument(
        "--install",
        action="store_true",
        help="Exercise the rendered supported-prerelease resolver in a clean virtual environment.",
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
    parser.add_argument(
        "--unavailable-exit-code",
        type=int,
        help="Return this distinct code only when public PyPI cannot install the exact API-reference release.",
    )
    parser.add_argument(
        "--source-revision",
        help="Exact source revision to record in the built site and require from a public deployment.",
    )
    parser.add_argument(
        "--verify-deployed-url",
        help="Public release-evidence URL to verify after deployment.",
    )
    parser.add_argument(
        "--deployment-attempts",
        type=int,
        default=12,
        help="Number of attempts allowed while the deployed release evidence propagates.",
    )
    parser.add_argument(
        "--deployment-retry-sleep",
        type=float,
        default=10,
        help="Seconds between public deployment evidence attempts.",
    )
    args = parser.parse_args()
    if args.unavailable_exit_code is not None and not 1 <= args.unavailable_exit_code <= 125:
        parser.error("--unavailable-exit-code must be between 1 and 125")

    repo_root = args.repo_root.resolve()
    site = args.site if args.site.is_absolute() else repo_root / args.site
    identity = load_release_identity(repo_root)
    validate_source_templates(repo_root)
    commands = validate_rendered_site(site)
    if args.source_revision:
        write_release_evidence(site, identity, args.source_revision)
    if args.install:
        try:
            run_clean_install(
                commands,
                identity,
                install_attempts=args.install_attempts,
                install_retry_sleep=args.install_retry_sleep,
            )
        except PublicRequirementUnavailable:
            if args.unavailable_exit_code is None:
                raise
            return int(args.unavailable_exit_code)
    if args.verify_deployed_url:
        if not args.source_revision:
            parser.error("--verify-deployed-url requires --source-revision")
        verify_public_deployment(
            args.verify_deployed_url,
            identity,
            args.source_revision,
            attempts=args.deployment_attempts,
            retry_sleep=args.deployment_retry_sleep,
        )
    print(
        "API-reference SDK and Server commands use the public qualified resolver pair; "
        f"release evidence records {identity.package} {identity.version} "
        f"with its package-supported Server baseline {identity.server_version}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
