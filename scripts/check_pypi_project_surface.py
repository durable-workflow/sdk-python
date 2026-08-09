#!/usr/bin/env python3
"""Qualify authoritative PyPI project-root metadata and package resolution.

This audit deliberately uses the PyPI JSON API and pip. Rendered project-page
HTML is a separate, non-authoritative presentation surface and is not fetched
here because an external client challenge must not be mistaken for metadata.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

try:
    from scripts.check_release_metadata import ReleaseMetadataError, SourceMetadata, load_source_metadata
except ModuleNotFoundError as error:  # pragma: no cover - direct command-line execution
    if error.name != "scripts":
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.check_release_metadata import ReleaseMetadataError, SourceMetadata, load_source_metadata

PUBLIC_PYPI_INDEX = "https://pypi.org/simple"
LEGACY_STABLE_VERSION = re.compile(r"0(?:\.[0-9]+)+")


class ProjectSurfaceError(RuntimeError):
    """The canonical PyPI project surface differs from release authority."""


def _normalized_name(value: object) -> str:
    return re.sub(r"[-_.]+", "-", str(value or "").strip().lower())


def _release_files(releases: dict[str, Any], version: str) -> list[dict[str, Any]]:
    files = releases.get(version)
    if not isinstance(files, list) or not files or not all(isinstance(item, dict) for item in files):
        raise ProjectSurfaceError(f"PyPI release history does not retain files for {version}")
    return files


def _legacy_version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def verify_project_json(payload: object, source: SourceMetadata) -> str:
    """Verify root JSON and return the newest retained legacy stable version."""

    if not isinstance(payload, dict):
        raise ProjectSurfaceError("PyPI project-root JSON must be an object")
    info = payload.get("info")
    releases = payload.get("releases")
    urls = payload.get("urls")
    if not isinstance(info, dict) or not isinstance(releases, dict) or not isinstance(urls, list):
        raise ProjectSurfaceError("PyPI project-root JSON lacks info, releases, or selected-release files")

    expected: dict[str, Any] = {
        "name": source.name,
        "version": source.registry_version,
        "summary": source.summary,
        "classifiers": list(source.classifiers),
        "description": source.readme,
        "description_content_type": "text/markdown",
    }
    for field, value in expected.items():
        if info.get(field) != value:
            raise ProjectSurfaceError(
                f"authoritative PyPI project-root field {field} differs from current prerelease metadata"
            )

    current_files = _release_files(releases, source.registry_version)
    if not all(isinstance(item, dict) for item in urls) or not urls:
        raise ProjectSurfaceError("PyPI project root does not expose current prerelease files")
    current_urls = {item.get("url") for item in current_files if isinstance(item.get("url"), str)}
    selected_urls = {item.get("url") for item in urls if isinstance(item.get("url"), str)}
    if not current_urls or selected_urls != current_urls:
        raise ProjectSurfaceError("PyPI project root selected-release files differ from the current prerelease")
    if any(item.get("yanked") is not False for item in current_files) or any(
        item.get("yanked") is not False for item in urls
    ):
        raise ProjectSurfaceError(
            f"current prerelease {source.registry_version} must remain installable and non-yanked"
        )

    legacy_versions = sorted(
        (version for version in releases if isinstance(version, str) and LEGACY_STABLE_VERSION.fullmatch(version)),
        key=_legacy_version_key,
    )
    if not legacy_versions:
        raise ProjectSurfaceError("PyPI project-root JSON no longer retains historical stable 0.x releases")
    retirement_required: list[str] = []
    retirement_reason_required: list[str] = []
    for version in legacy_versions:
        files = _release_files(releases, version)
        if any(file.get("yanked") is not True for file in files):
            retirement_required.append(version)
        elif any(not isinstance(file.get("yanked_reason"), str) or not file["yanked_reason"].strip() for file in files):
            retirement_reason_required.append(version)

    failures: list[str] = []
    if retirement_required:
        failures.append(
            "yank these historical stable releases in PyPI release management with a retirement reason: "
            + ", ".join(retirement_required)
        )
    if retirement_reason_required:
        failures.append("add a PyPI yank reason for: " + ", ".join(retirement_reason_required))
    if failures:
        raise ProjectSurfaceError("; ".join(failures))

    return legacy_versions[-1]


def supported_prerelease_requirement(source: SourceMetadata) -> str:
    section = re.search(r"(?ms)^## Install\s*$\n(?P<body>.*?)(?=^##\s|\Z)", source.readme)
    if section is None:
        raise ProjectSurfaceError("README has no Install section")
    block = re.search(r"(?ms)^```(?:bash|shell)\s*$\n(?P<code>.*?)^```\s*$", section.group("body"))
    if block is None:
        raise ProjectSurfaceError("README Install section has no shell command")
    commands = [line.strip() for line in block.group("code").splitlines() if line.strip()]
    if not commands:
        raise ProjectSurfaceError("README Install section has an empty shell block")
    arguments = shlex.split(commands[0])
    if len(arguments) != 4 or arguments[:3] != ["pip", "install", "--pre"]:
        raise ProjectSurfaceError("README first install command must explicitly select a prerelease requirement")
    requirement = arguments[3]
    if requirement != f"{source.name}~=2.0.0rc0":
        raise ProjectSurfaceError("README first install command must select the supported 2.0 prerelease line")
    return requirement


def verify_pip_report(report: object, source: SourceMetadata, expected_version: str) -> None:
    if not isinstance(report, dict) or not isinstance(report.get("install"), list):
        raise ProjectSurfaceError("pip resolution report is invalid")
    selected = next(
        (
            item
            for item in report["install"]
            if isinstance(item, dict)
            and isinstance(item.get("metadata"), dict)
            and _normalized_name(item["metadata"].get("name")) == _normalized_name(source.name)
        ),
        None,
    )
    if selected is None:
        raise ProjectSurfaceError(f"pip did not select {source.name}")
    version = selected["metadata"].get("version")
    if version != expected_version:
        raise ProjectSurfaceError(f"pip selected {version or '<missing>'}; expected {expected_version}")


def _pip_report(requirement: str, *, prerelease: bool) -> object:
    with tempfile.TemporaryDirectory(prefix="dw-pypi-project-surface-") as temporary:
        report_path = Path(temporary) / "pip-report.json"
        command = [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--dry-run",
            "--ignore-installed",
            "--no-deps",
            "--index-url",
            PUBLIC_PYPI_INDEX,
        ]
        if prerelease:
            command.append("--pre")
        command.extend(("--report", str(report_path), requirement))
        environment = {
            **os.environ,
            "PIP_CONFIG_FILE": os.devnull,
            "PIP_INDEX_URL": PUBLIC_PYPI_INDEX,
            "PYTHONPATH": "",
        }
        for variable in ("PIP_EXTRA_INDEX_URL", "PIP_FIND_LINKS", "PIP_NO_INDEX"):
            environment.pop(variable, None)
        result = subprocess.run(command, check=False, capture_output=True, text=True, env=environment)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            raise ProjectSurfaceError(f"pip could not resolve {requirement}: {detail}")
        try:
            return json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProjectSurfaceError("pip did not produce a valid JSON resolution report") from error


def _request_json(url: str) -> object:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "durable-workflow-pypi-project-surface-audit"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise ProjectSurfaceError(f"{url} returned HTTP {response.status}")
        try:
            return json.loads(response.read().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProjectSurfaceError("authoritative PyPI project-root JSON is invalid") from error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-ref", required=True, help="Exact current prerelease source commit or tag")
    parser.add_argument("--attempts", type=int, default=1, help="PyPI root-metadata propagation attempts")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="Delay between metadata attempts")
    args = parser.parse_args(argv)
    if args.attempts < 1 or args.interval_seconds < 0:
        raise ProjectSurfaceError("attempts must be positive and interval-seconds must be non-negative")

    try:
        source = load_source_metadata(args.source_ref)
    except ReleaseMetadataError as error:
        raise ProjectSurfaceError(str(error)) from error
    project_json_url = f"https://pypi.org/pypi/{source.name}/json"
    last_error: BaseException | None = None
    legacy_version: str | None = None
    for attempt in range(1, args.attempts + 1):
        try:
            legacy_version = verify_project_json(_request_json(project_json_url), source)
            break
        except (ProjectSurfaceError, urllib.error.URLError) as error:
            last_error = error
            if attempt < args.attempts:
                time.sleep(args.interval_seconds)
    if legacy_version is None:
        assert last_error is not None
        raise ProjectSurfaceError(
            f"PyPI project-root metadata did not converge after {args.attempts} attempt(s): {last_error}"
        )

    verify_pip_report(_pip_report(source.name, prerelease=False), source, source.registry_version)
    requirement = supported_prerelease_requirement(source)
    verify_pip_report(_pip_report(requirement, prerelease=True), source, source.registry_version)
    legacy_requirement = f"{source.name}=={legacy_version}"
    verify_pip_report(_pip_report(legacy_requirement, prerelease=False), source, legacy_version)
    print(
        f"PyPI project root, default install, and supported prerelease install select "
        f"{source.name} {source.registry_version}; "
        f"exact historical install {legacy_requirement} remains resolvable"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
