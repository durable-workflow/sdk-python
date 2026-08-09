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
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ProjectSurfaceEvidence:
    """Public registry evidence produced by a successful project-surface audit."""

    project_json_url: str
    exact_version_json_url: str
    selected_version: str
    documented_requirement: str
    documented_install_version: str
    yanked_versions: tuple[str, ...]
    historical_exact_probe: str


def _normalized_name(value: object) -> str:
    return re.sub(r"[-_.]+", "-", str(value or "").strip().lower())


def _release_files(releases: dict[str, Any], version: str) -> list[dict[str, Any]]:
    files = releases.get(version)
    if not isinstance(files, list) or not files or not all(isinstance(item, dict) for item in files):
        raise ProjectSurfaceError(f"PyPI release history does not retain files for {version}")
    return files


def _legacy_version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def _verify_info(info: object, source: SourceMetadata, surface: str) -> None:
    if not isinstance(info, dict):
        raise ProjectSurfaceError(f"{surface} lacks info metadata")

    expected: dict[str, object] = {
        "name": source.name,
        "version": source.registry_version,
        "summary": source.summary,
        "classifiers": list(source.classifiers),
        "description": source.readme,
        "description_content_type": "text/markdown",
    }
    for field, value in expected.items():
        if info.get(field) != value:
            raise ProjectSurfaceError(f"{surface} field {field} differs from current prerelease metadata")


def _non_yanked_release_urls(files: object, version: str, surface: str) -> frozenset[str]:
    if not isinstance(files, list) or not files or not all(isinstance(item, dict) for item in files):
        raise ProjectSurfaceError(f"{surface} does not expose files for {version}")
    urls = frozenset(item.get("url") for item in files if isinstance(item.get("url"), str))
    if not urls:
        raise ProjectSurfaceError(f"{surface} does not expose file URLs for {version}")
    if any(item.get("yanked") is not False for item in files):
        raise ProjectSurfaceError(f"current prerelease {version} must remain installable and non-yanked")
    return urls


def verify_exact_version_json(payload: object, source: SourceMetadata) -> frozenset[str]:
    """Verify the immutable exact-version JSON surface and return its file URLs."""

    if not isinstance(payload, dict):
        raise ProjectSurfaceError("PyPI exact-version JSON must be an object")
    _verify_info(payload.get("info"), source, "authoritative PyPI exact-version JSON")
    return _non_yanked_release_urls(
        payload.get("urls"),
        source.registry_version,
        "PyPI exact-version JSON",
    )


def verify_project_json(
    payload: object,
    source: SourceMetadata,
    exact_version_urls: frozenset[str] | None = None,
) -> tuple[str, ...]:
    """Verify project-root JSON and return retained legacy stable versions."""

    if not isinstance(payload, dict):
        raise ProjectSurfaceError("PyPI project-root JSON must be an object")
    info = payload.get("info")
    releases = payload.get("releases")
    urls = payload.get("urls")
    if not isinstance(releases, dict) or not isinstance(urls, list):
        raise ProjectSurfaceError("PyPI project-root JSON lacks releases or selected-release files")
    _verify_info(info, source, "authoritative PyPI project-root JSON")

    current_files = _release_files(releases, source.registry_version)
    current_urls = _non_yanked_release_urls(
        current_files,
        source.registry_version,
        "PyPI project-root release history",
    )
    selected_urls = _non_yanked_release_urls(
        urls,
        source.registry_version,
        "PyPI project-root selected release",
    )
    if selected_urls != current_urls:
        raise ProjectSurfaceError("PyPI project root selected-release files differ from the current prerelease")
    if exact_version_urls is not None and selected_urls != exact_version_urls:
        raise ProjectSurfaceError("PyPI project-root files differ from the exact-version JSON surface")

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

    return tuple(legacy_versions)


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
    if len(arguments) != 3 or arguments[:2] != ["pip", "install"]:
        raise ProjectSurfaceError("README first install command must select one package requirement")
    requirement = arguments[2]
    if requirement != f"{source.name}~=2.0.0rc0":
        raise ProjectSurfaceError("README first install command must select the supported 2.0 prerelease line")
    return requirement


def verify_pip_report(report: object, source: SourceMetadata, expected_version: str) -> str:
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
    return version


def write_evidence(path: Path, source: SourceMetadata, evidence: ProjectSurfaceEvidence) -> None:
    payload = {
        "schema": "durable-workflow.python-pypi-project-surface.v1",
        "source_commit": source.commit,
        "package": source.name,
        "public_urls": {
            "project_json": evidence.project_json_url,
            "exact_version_json": evidence.exact_version_json_url,
        },
        "selected_version": evidence.selected_version,
        "documented_requirement": evidence.documented_requirement,
        "documented_install_version": evidence.documented_install_version,
        "yanked_versions": list(evidence.yanked_versions),
        "historical_exact_probe": evidence.historical_exact_probe,
    }
    path.write_text(f"{json.dumps(payload, indent=2, sort_keys=True)}\n", encoding="utf-8")


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
    parser.add_argument(
        "--source-only",
        action="store_true",
        help="Validate source release metadata and the documented prerelease requirement without querying PyPI",
    )
    parser.add_argument("--attempts", type=int, default=1, help="PyPI root-metadata propagation attempts")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="Delay between metadata attempts")
    parser.add_argument("--evidence", type=Path, help="Write successful public audit evidence as JSON")
    args = parser.parse_args(argv)
    if args.attempts < 1 or args.interval_seconds < 0:
        raise ProjectSurfaceError("attempts must be positive and interval-seconds must be non-negative")

    try:
        source = load_source_metadata(args.source_ref)
    except ReleaseMetadataError as error:
        raise ProjectSurfaceError(str(error)) from error
    requirement = supported_prerelease_requirement(source)
    if args.source_only:
        print(
            f"source metadata declares {source.name} {source.registry_version}; "
            f"documented prerelease install selects {requirement}"
        )
        return 0

    project_json_url = f"https://pypi.org/pypi/{source.name}/json"
    exact_version_json_url = f"https://pypi.org/pypi/{source.name}/{source.registry_version}/json"
    last_error: BaseException | None = None
    legacy_versions: tuple[str, ...] | None = None
    for attempt in range(1, args.attempts + 1):
        try:
            project_payload = _request_json(project_json_url)
            exact_version_payload = _request_json(exact_version_json_url)
            exact_version_urls = verify_exact_version_json(exact_version_payload, source)
            legacy_versions = verify_project_json(project_payload, source, exact_version_urls)
            break
        except (ProjectSurfaceError, urllib.error.URLError) as error:
            last_error = error
            if attempt < args.attempts:
                time.sleep(args.interval_seconds)
    if legacy_versions is None:
        assert last_error is not None
        raise ProjectSurfaceError(
            f"PyPI project and exact-version metadata did not converge after {args.attempts} attempt(s): {last_error}"
        )

    default_install_version = verify_pip_report(
        _pip_report(source.name, prerelease=False),
        source,
        source.registry_version,
    )
    documented_install_version = verify_pip_report(
        _pip_report(requirement, prerelease=False),
        source,
        source.registry_version,
    )
    legacy_version = legacy_versions[-1]
    legacy_requirement = f"{source.name}=={legacy_version}"
    verify_pip_report(_pip_report(legacy_requirement, prerelease=False), source, legacy_version)
    evidence = ProjectSurfaceEvidence(
        project_json_url=project_json_url,
        exact_version_json_url=exact_version_json_url,
        selected_version=default_install_version,
        documented_requirement=requirement,
        documented_install_version=documented_install_version,
        yanked_versions=legacy_versions,
        historical_exact_probe=legacy_version,
    )
    if args.evidence is not None:
        write_evidence(args.evidence, source, evidence)
    print(
        f"PyPI project root, exact-version JSON, default install, and supported prerelease install select "
        f"{source.name} {source.registry_version}; yanked historical releases: {', '.join(legacy_versions)}; "
        f"exact historical install {legacy_requirement} remains resolvable; "
        f"public metadata: {project_json_url}, {exact_version_json_url}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
