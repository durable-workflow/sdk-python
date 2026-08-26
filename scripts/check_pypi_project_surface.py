#!/usr/bin/env python3
"""Qualify authoritative PyPI release metadata and package resolution.

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
    from scripts.api_reference_release import SUPPORTED_PRERELEASE_INSTALL_COMMAND
    from scripts.check_release_metadata import ReleaseMetadataError, SourceMetadata, load_source_metadata
except ModuleNotFoundError as error:  # pragma: no cover - direct command-line execution
    if error.name != "scripts":
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.api_reference_release import SUPPORTED_PRERELEASE_INSTALL_COMMAND
    from scripts.check_release_metadata import ReleaseMetadataError, SourceMetadata, load_source_metadata

PUBLIC_PYPI_INDEX = "https://pypi.org/simple"
LEGACY_STABLE_VERSION = re.compile(r"0(?:\.[0-9]+)+")


class ProjectSurfaceError(RuntimeError):
    """The canonical PyPI project surface differs from release authority."""


class _SimpleApiVisibilityLag(ProjectSurfaceError):
    """pip's Simple API view does not expose a requested release yet."""


@dataclass(frozen=True)
class ProjectSurfaceEvidence:
    """Public registry evidence produced by a successful project-surface audit."""

    exact_version_json_url: str
    release_channel: str
    exact_install_version: str
    documented_install_command: str | None
    project_json_url: str | None
    default_install_version: str | None
    historical_versions: tuple[str, ...]


def _normalized_name(value: object) -> str:
    return re.sub(r"[-_.]+", "-", str(value or "").strip().lower())


def _release_files(releases: dict[str, Any], version: str) -> list[dict[str, Any]]:
    files = releases.get(version)
    if not isinstance(files, list) or not files or not all(isinstance(item, dict) for item in files):
        raise ProjectSurfaceError(f"PyPI release history does not retain files for {version}")
    return files


def _legacy_version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def release_channel(source: SourceMetadata) -> str:
    """Classify the only release identities this audit is allowed to qualify."""

    if re.fullmatch(r"2\.0\.0rc[1-9][0-9]*", source.registry_version):
        return "prerelease"
    if source.registry_version == "2.0.0":
        return "stable"
    raise ProjectSurfaceError(f"unsupported PyPI release identity: {source.registry_version}")


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
            raise ProjectSurfaceError(f"{surface} field {field} differs from current release metadata")


def _non_yanked_release_urls(files: object, version: str, surface: str) -> frozenset[str]:
    if not isinstance(files, list) or not files or not all(isinstance(item, dict) for item in files):
        raise ProjectSurfaceError(f"{surface} does not expose files for {version}")
    urls = frozenset(item.get("url") for item in files if isinstance(item.get("url"), str))
    if not urls:
        raise ProjectSurfaceError(f"{surface} does not expose file URLs for {version}")
    if any(item.get("yanked") is not False for item in files):
        raise ProjectSurfaceError(f"current release {version} must remain installable and non-yanked")
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


def verify_stable_project_json(
    payload: object,
    source: SourceMetadata,
    exact_version_urls: frozenset[str] | None = None,
) -> tuple[str, ...]:
    """Verify the strict project-root contract for an authorized stable release."""

    if release_channel(source) != "stable":
        raise ProjectSurfaceError("PyPI project-root selection is release-blocking only for stable 2.0")

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
        raise ProjectSurfaceError("PyPI project root selected-release files differ from the current stable release")
    if exact_version_urls is not None and selected_urls != exact_version_urls:
        raise ProjectSurfaceError("PyPI project-root files differ from the exact-version JSON surface")

    legacy_versions = sorted(
        (version for version in releases if isinstance(version, str) and LEGACY_STABLE_VERSION.fullmatch(version)),
        key=_legacy_version_key,
    )
    if not legacy_versions:
        raise ProjectSurfaceError("PyPI project-root JSON no longer retains historical stable 0.x releases")
    for version in legacy_versions:
        files = _release_files(releases, version)
        if any(file.get("yanked") is not False for file in files):
            raise ProjectSurfaceError(f"historical release {version} must remain retained and non-yanked")

    return tuple(legacy_versions)


def supported_prerelease_install_command(source: SourceMetadata) -> str:
    section = re.search(r"(?ms)^## Install\s*$\n(?P<body>.*?)(?=^##\s|\Z)", source.readme)
    if section is None:
        raise ProjectSurfaceError("README has no Install section")
    block = re.search(r"(?ms)^```(?:bash|shell)\s*$\n(?P<code>.*?)^```\s*$", section.group("body"))
    if block is None:
        raise ProjectSurfaceError("README Install section has no shell command")
    commands = [line.strip() for line in block.group("code").splitlines() if line.strip()]
    if not commands:
        raise ProjectSurfaceError("README Install section has an empty shell block")
    command = commands[0]
    if shlex.split(command) != shlex.split(SUPPORTED_PRERELEASE_INSTALL_COMMAND):
        raise ProjectSurfaceError("README first install command must use the supported prerelease resolver")
    return SUPPORTED_PRERELEASE_INSTALL_COMMAND


def _selected_pip_version(report: object, source: SourceMetadata) -> str:
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
    if not isinstance(version, str) or not version:
        raise ProjectSurfaceError("pip resolution report is invalid")
    return version


def verify_pip_report(report: object, source: SourceMetadata, expected_version: str) -> str:
    version = _selected_pip_version(report, source)
    if version != expected_version:
        raise ProjectSurfaceError(f"pip selected {version}; expected {expected_version}")
    return version


def _resolve_pip_with_convergence(
    requirement: str,
    source: SourceMetadata,
    *,
    attempts: int,
    interval_seconds: float,
) -> str:
    last_lag: ProjectSurfaceError | None = None
    for attempt in range(1, attempts + 1):
        try:
            report = _pip_report(requirement, prerelease=False)
        except _SimpleApiVisibilityLag as error:
            last_lag = error
        else:
            return verify_pip_report(report, source, source.registry_version)
        if attempt < attempts:
            time.sleep(interval_seconds)

    assert last_lag is not None
    raise ProjectSurfaceError(
        f"PyPI Simple API did not converge for {requirement} after {attempts} attempt(s): {last_lag}"
    )


def write_evidence(path: Path, source: SourceMetadata, evidence: ProjectSurfaceEvidence) -> None:
    payload = {
        "schema": "durable-workflow.python-pypi-project-surface.v3",
        "source_commit": source.commit,
        "package": source.name,
        "public_urls": {
            "project_json": evidence.project_json_url,
            "exact_version_json": evidence.exact_version_json_url,
        },
        "release_channel": evidence.release_channel,
        "exact_install_version": evidence.exact_install_version,
        "default_install_version": evidence.default_install_version,
        "documented_install_command": evidence.documented_install_command,
        "historical_versions": list(evidence.historical_versions),
    }
    path.write_text(f"{json.dumps(payload, indent=2, sort_keys=True)}\n", encoding="utf-8")


def _exact_requirement_missing_from_simple_api(requirement: str, detail: str) -> bool:
    exact = re.fullmatch(r"[^<>=!~\s]+==(?P<version>[^\s]+)", requirement)
    available = re.search(r"\(from versions: (?P<versions>[^)]*)\)", detail)
    if exact is None or available is None or "No matching distribution found for" not in detail:
        return False
    lowered = detail.lower()
    if any(
        marker in lowered
        for marker in (
            "has inconsistent name",
            "hashes are required",
            "requires a different python",
            "requires-python",
        )
    ):
        return False
    versions = {version.strip() for version in available.group("versions").split(",")}
    return exact.group("version") not in versions


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
            "--no-cache-dir",
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
            if _exact_requirement_missing_from_simple_api(requirement, detail):
                raise _SimpleApiVisibilityLag(f"pip could not resolve {requirement}: {detail}")
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
    parser.add_argument("--source-ref", required=True, help="Exact current release source commit or tag")
    parser.add_argument(
        "--source-only",
        action="store_true",
        help="Validate source release metadata and its channel policy without querying PyPI",
    )
    parser.add_argument("--attempts", type=int, default=1, help="PyPI metadata propagation attempts")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="Delay between metadata attempts")
    parser.add_argument("--evidence", type=Path, help="Write successful public audit evidence as JSON")
    args = parser.parse_args(argv)
    if args.attempts < 1 or args.interval_seconds < 0:
        raise ProjectSurfaceError("attempts must be positive and interval-seconds must be non-negative")

    try:
        source = load_source_metadata(args.source_ref)
    except ReleaseMetadataError as error:
        raise ProjectSurfaceError(str(error)) from error
    channel = release_channel(source)
    install_command = supported_prerelease_install_command(source) if channel == "prerelease" else None
    if args.source_only:
        detail = f"; documented prerelease install uses {install_command}" if install_command is not None else ""
        print(f"source metadata declares {source.name} {source.registry_version} ({channel}){detail}")
        return 0

    exact_version_json_url = f"https://pypi.org/pypi/{source.name}/{source.registry_version}/json"
    last_error: urllib.error.URLError | None = None
    exact_version_urls: frozenset[str] | None = None
    for attempt in range(1, args.attempts + 1):
        try:
            exact_version_payload = _request_json(exact_version_json_url)
            exact_version_urls = verify_exact_version_json(exact_version_payload, source)
            break
        except urllib.error.URLError as error:
            last_error = error
            if attempt < args.attempts:
                time.sleep(args.interval_seconds)
    if exact_version_urls is None:
        assert last_error is not None
        raise ProjectSurfaceError(
            f"PyPI exact-version metadata did not become available after {args.attempts} attempt(s): {last_error}"
        )

    exact_requirement = f"{source.name}=={source.registry_version}"
    exact_install_version = _resolve_pip_with_convergence(
        exact_requirement,
        source,
        attempts=args.attempts,
        interval_seconds=args.interval_seconds,
    )

    project_json_url: str | None = None
    default_install_version: str | None = None
    historical_versions: tuple[str, ...] = ()
    if channel == "stable":
        project_json_url = f"https://pypi.org/pypi/{source.name}/json"
        project_last_error: BaseException | None = None
        for attempt in range(1, args.attempts + 1):
            try:
                historical_versions = verify_stable_project_json(
                    _request_json(project_json_url),
                    source,
                    exact_version_urls,
                )
                break
            except (ProjectSurfaceError, urllib.error.URLError) as error:
                project_last_error = error
                if attempt < args.attempts:
                    time.sleep(args.interval_seconds)
        if not historical_versions:
            assert project_last_error is not None
            raise ProjectSurfaceError(
                f"PyPI stable project-root metadata did not converge after {args.attempts} attempt(s): "
                f"{project_last_error}"
            )
        default_install_version = verify_pip_report(
            _pip_report(source.name, prerelease=False),
            source,
            source.registry_version,
        )

    evidence = ProjectSurfaceEvidence(
        exact_version_json_url=exact_version_json_url,
        release_channel=channel,
        exact_install_version=exact_install_version,
        documented_install_command=install_command,
        project_json_url=project_json_url,
        default_install_version=default_install_version,
        historical_versions=historical_versions,
    )
    if args.evidence is not None:
        write_evidence(args.evidence, source, evidence)
    if channel == "prerelease":
        print(
            f"PyPI exact-version JSON and exact install select {source.name} {source.registry_version}; "
            f"published metadata documents the supported prerelease resolver; "
            f"project-root/default selection is deferred until stable; "
            f"public metadata: {exact_version_json_url}"
        )
    else:
        print(
            f"PyPI exact-version and project-root JSON, exact install, and default install select "
            f"{source.name} {source.registry_version}; retained historical releases: "
            f"{', '.join(historical_versions)}; public metadata: {project_json_url}, {exact_version_json_url}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
