#!/usr/bin/env python3
"""Verify built and published package metadata against an exact source commit."""

from __future__ import annotations

import argparse
import html.parser
import importlib
import json
import re
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from email import policy
from email.message import Message
from email.parser import BytesParser
from pathlib import Path
from typing import Any


def _load_toml_parser() -> Any:
    """Load the stdlib TOML parser or the declared Python 3.10 fallback."""

    try:
        return importlib.import_module("tomllib")
    except ModuleNotFoundError as error:
        if error.name != "tomllib":
            raise
        return importlib.import_module("tomli")


tomllib = _load_toml_parser()

COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
BETA_CLASSIFIER = "Development Status :: 4 - Beta"


class ReleaseMetadataError(RuntimeError):
    """Release metadata does not match its exact source."""


@dataclass(frozen=True)
class SourceMetadata:
    commit: str
    name: str
    version: str
    registry_version: str
    summary: str
    classifiers: tuple[str, ...]
    readme: str


@dataclass(frozen=True)
class ProjectPageAudit:
    """Advisory evidence from PyPI's rendered presentation surface."""

    outcome: str
    detail: str


def _git(*args: str) -> bytes:
    try:
        return subprocess.run(
            ["git", *args],
            check=True,
            capture_output=True,
        ).stdout
    except subprocess.CalledProcessError as error:
        detail = error.stderr.decode(errors="replace").strip()
        raise ReleaseMetadataError(f"git {' '.join(args)} failed: {detail}") from error


def load_source_metadata(source_ref: str) -> SourceMetadata:
    commit = _git("rev-parse", "--verify", f"{source_ref}^{{commit}}").decode().strip()
    if COMMIT_PATTERN.fullmatch(commit) is None:
        raise ReleaseMetadataError("source ref did not resolve to an exact commit")

    pyproject_raw = _git("show", f"{commit}:pyproject.toml")
    readme_raw = _git("show", f"{commit}:README.md")
    try:
        pyproject = tomllib.loads(pyproject_raw.decode("utf-8"))
        readme = readme_raw.decode("utf-8")
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ReleaseMetadataError("source release metadata is not valid UTF-8/TOML") from error

    project = pyproject.get("project")
    tool = pyproject.get("tool", {}).get("durable-workflow")
    if not isinstance(project, dict) or not isinstance(tool, dict):
        raise ReleaseMetadataError("source lacks project or tool.durable-workflow metadata")

    version = project.get("version")
    registry_version = tool.get("registry-version")
    product_train = tool.get("product-train")
    classifiers = project.get("classifiers")
    if not isinstance(version, str) or not re.fullmatch(r"2\.0\.0(?:-rc\.[1-9][0-9]*)?", version):
        raise ReleaseMetadataError("source version is not an exact supported Durable Workflow 2.0 release")
    if product_train != version:
        raise ReleaseMetadataError("product-train does not match project.version")
    if registry_version != version.replace("-rc.", "rc"):
        raise ReleaseMetadataError("registry-version is not the PEP 440 form of project.version")
    if project.get("readme") != "README.md":
        raise ReleaseMetadataError("project.readme must select README.md")
    if not isinstance(classifiers, list) or not all(isinstance(item, str) for item in classifiers):
        raise ReleaseMetadataError("project.classifiers must be a string list")
    if BETA_CLASSIFIER in classifiers:
        raise ReleaseMetadataError("2.0 release metadata must not carry the Beta classifier")

    name = project.get("name")
    summary = project.get("description")
    if not isinstance(name, str) or not name or not isinstance(summary, str) or not summary:
        raise ReleaseMetadataError("project name and description must be non-empty strings")

    return SourceMetadata(
        commit=commit,
        name=name,
        version=version,
        registry_version=registry_version,
        summary=summary,
        classifiers=tuple(classifiers),
        readme=readme,
    )


def _parse_metadata(raw: bytes, label: str) -> tuple[Message, str]:
    normalized = raw.replace(b"\r\n", b"\n")
    headers, separator, description = normalized.partition(b"\n\n")
    if not separator:
        raise ReleaseMetadataError(f"{label} lacks a package-description separator")
    message = BytesParser(policy=policy.default).parsebytes(headers + separator)
    if not message.get("Name") or not message.get("Version"):
        raise ReleaseMetadataError(f"{label} lacks Python core metadata")
    try:
        decoded_description = description.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ReleaseMetadataError(f"{label} has a non-UTF-8 package description") from error
    return message, decoded_description


def _verify_core_metadata(
    message: Message,
    description: str,
    source: SourceMetadata,
    label: str,
) -> None:
    expected = {
        "Name": source.name,
        "Version": source.registry_version,
        "Summary": source.summary,
        "Description-Content-Type": "text/markdown",
    }
    for field, value in expected.items():
        if message.get(field) != value:
            raise ReleaseMetadataError(
                f"{label} {field} differs from source: expected {value!r}, got {message.get(field)!r}"
            )
    if tuple(message.get_all("Classifier", [])) != source.classifiers:
        raise ReleaseMetadataError(f"{label} classifiers differ from the source commit")
    if description != source.readme:
        raise ReleaseMetadataError(f"{label} long description differs from README.md at {source.commit}")


def verify_built_distributions(dist: Path, source: SourceMetadata) -> None:
    wheels = sorted(dist.glob("*.whl"))
    sdists = sorted(dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise ReleaseMetadataError("dist must contain exactly one wheel and one .tar.gz source distribution")

    with zipfile.ZipFile(wheels[0]) as archive:
        metadata_names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise ReleaseMetadataError("wheel must contain exactly one .dist-info/METADATA")
        wheel_metadata, wheel_description = _parse_metadata(
            archive.read(metadata_names[0]),
            "wheel METADATA",
        )
    _verify_core_metadata(
        wheel_metadata,
        wheel_description,
        source,
        "wheel METADATA",
    )

    with tarfile.open(sdists[0], mode="r:gz") as archive:
        pkg_info_members = [
            member
            for member in archive.getmembers()
            if member.name.count("/") == 1 and member.name.endswith("/PKG-INFO")
        ]
        readme_members = [
            member
            for member in archive.getmembers()
            if member.name.count("/") == 1 and member.name.endswith("/README.md")
        ]
        if len(pkg_info_members) != 1 or len(readme_members) != 1:
            raise ReleaseMetadataError("sdist must contain exactly one PKG-INFO and README.md")
        pkg_info_file = archive.extractfile(pkg_info_members[0])
        readme_file = archive.extractfile(readme_members[0])
        if pkg_info_file is None or readme_file is None:
            raise ReleaseMetadataError("sdist release metadata is not readable")
        sdist_metadata, sdist_description = _parse_metadata(
            pkg_info_file.read(),
            "sdist PKG-INFO",
        )
        sdist_readme = readme_file.read().decode("utf-8")
    _verify_core_metadata(
        sdist_metadata,
        sdist_description,
        source,
        "sdist PKG-INFO",
    )
    if sdist_readme != source.readme:
        raise ReleaseMetadataError(f"sdist README.md differs from README.md at {source.commit}")


class _VisibleTextParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag in {"script", "style"}:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"} and self._ignored_depth:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._ignored_depth == 0:
            self.parts.append(data)


def _plain_readme_marker(readme: str) -> str:
    paragraphs: list[str] = []
    current: list[str] = []
    in_code = False
    for line in readme.splitlines():
        if line.startswith("```"):
            in_code = not in_code
            continue
        if in_code or line.startswith("#") or line.startswith(("- ", "* ", "> ")):
            if current:
                paragraphs.append(" ".join(current))
                current = []
            continue
        if not line.strip():
            if current:
                paragraphs.append(" ".join(current))
                current = []
            continue
        current.append(line.strip())
    if current:
        paragraphs.append(" ".join(current))

    for paragraph in paragraphs:
        if len(paragraph) >= 60 and not any(marker in paragraph for marker in ("`", "[", "]", "*")):
            return " ".join(paragraph.split())
    raise ReleaseMetadataError("README.md lacks a plain-text project-page audit marker")


def _request_bytes(url: str, accept: str) -> bytes:
    request = urllib.request.Request(
        url,
        headers={"Accept": accept, "User-Agent": "durable-workflow-sdk-python-release-audit"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status != 200:
            raise ReleaseMetadataError(f"{url} returned HTTP {response.status}")
        content: bytes = response.read()
        return content


def _audit_pypi_project_page(source: SourceMetadata) -> ProjectPageAudit:
    page_url = f"https://pypi.org/project/{source.name}/{source.registry_version}/"
    try:
        page = _request_bytes(page_url, "text/html").decode("utf-8")
        marker = _plain_readme_marker(source.readme)
    except (ReleaseMetadataError, UnicodeDecodeError, urllib.error.URLError) as error:
        return ProjectPageAudit("unavailable", f"rendered project page could not be audited: {error}")

    parser = _VisibleTextParser()
    parser.feed(page)
    visible_page = " ".join(" ".join(parser.parts).split())
    normalized_page = visible_page.casefold()
    challenge_markers = (
        "client challenge",
        "checking your browser",
        "javascript is disabled",
        "verify that you're not a robot",
        "verify that you’re not a robot",
    )
    observed_challenge_markers = [marker for marker in challenge_markers if marker in normalized_page]
    if "client challenge" in observed_challenge_markers or len(observed_challenge_markers) >= 2:
        return ProjectPageAudit(
            "challenged",
            "rendered project page returned a client challenge; exact PyPI JSON remains authoritative",
        )
    if source.registry_version not in visible_page or marker not in visible_page:
        return ProjectPageAudit(
            "mismatch",
            "rendered project page does not show the expected version and README marker",
        )
    return ProjectPageAudit("match", "rendered project page shows the expected version and README marker")


def verify_pypi(source: SourceMetadata, requested_version: str) -> ProjectPageAudit:
    if requested_version not in {source.version, source.registry_version}:
        raise ReleaseMetadataError("requested PyPI version differs from the source commit")

    json_url = f"https://pypi.org/pypi/{source.name}/{source.registry_version}/json"
    try:
        payload = json.loads(_request_bytes(json_url, "application/json"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ReleaseMetadataError("exact PyPI JSON is invalid") from error
    info = payload.get("info") if isinstance(payload, dict) else None
    if not isinstance(info, dict):
        raise ReleaseMetadataError("exact PyPI JSON lacks info metadata")

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
            raise ReleaseMetadataError(f"exact PyPI JSON field {field} differs from the source commit")

    return _audit_pypi_project_page(source)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-ref", required=True, help="Exact source commit or ref to verify")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dist", type=Path, help="Directory containing one built wheel and sdist")
    mode.add_argument("--pypi-version", help="Exact published version to audit on PyPI")
    parser.add_argument("--attempts", type=int, default=1, help="PyPI propagation attempts")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="Delay between PyPI attempts")
    args = parser.parse_args()

    source = load_source_metadata(args.source_ref)
    if args.dist is not None:
        verify_built_distributions(args.dist, source)
        print(f"built distributions match {source.name} {source.registry_version} at {source.commit}")
        return 0

    if args.attempts < 1 or args.interval_seconds < 0:
        raise ReleaseMetadataError("attempts must be positive and interval-seconds must be non-negative")

    last_error: urllib.error.URLError | None = None
    for attempt in range(1, args.attempts + 1):
        try:
            page_audit = verify_pypi(source, args.pypi_version)
            print(f"exact PyPI JSON matches {source.name} {source.registry_version} at {source.commit}")
            if page_audit.outcome == "match":
                print(page_audit.detail)
            else:
                print(f"::warning title=PyPI rendered-page audit::{page_audit.detail}", file=sys.stderr)
            return 0
        except urllib.error.URLError as error:
            last_error = error
            if attempt < args.attempts:
                time.sleep(args.interval_seconds)
    assert last_error is not None
    raise ReleaseMetadataError(f"PyPI metadata did not converge after {args.attempts} attempt(s): {last_error}")


if __name__ == "__main__":
    raise SystemExit(main())
