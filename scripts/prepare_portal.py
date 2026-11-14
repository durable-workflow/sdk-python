#!/usr/bin/env python3
"""Bind current presentation to an explicitly selected published SDK source."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import tomllib
import yaml

PRESENTATION_DIRECTORIES = ("assets", "stylesheets", "javascripts")
CODE_BLOCKS = re.compile(r"```([^\n]*)\n(.*?)```", re.DOTALL)


def git(root: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *arguments], text=True).strip()


def source_files(root: Path, directory: str) -> list[Path]:
    names = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z", directory]).decode().split("\0")
    files = []
    for name in filter(None, names):
        source = root / name
        if source.is_symlink() or not source.resolve().is_relative_to(root.resolve()) or not source.is_file():
            raise ValueError("Selected SDK source contains an unsupported file")
        files.append(source)
    return sorted(files)


def sdk_inventory(sdk: Path, *, verify_installed: bool) -> list[dict[str, Any]]:
    distribution = importlib.metadata.distribution("durable-workflow") if verify_installed else None
    rows = []
    for source in source_files(sdk, "src/durable_workflow"):
        data = source.read_bytes()
        relative = source.relative_to(sdk / "src").as_posix()
        if distribution is not None and distribution.locate_file(relative).read_bytes() != data:
            raise ValueError("Installed published SDK differs from selected API source")
        rows.append({"path": relative, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    if not rows:
        raise ValueError("Selected SDK has no API source")
    return rows


def published_identity(root: Path, sdk: Path, tag: str, environment: dict[str, str]) -> tuple[str, dict[str, Any]]:
    if not re.fullmatch(r"\d+\.\d+\.\d+", tag):
        raise ValueError("Stable SDK requires an exact stable tag")
    revision = git(sdk, "rev-parse", "HEAD")
    if revision != git(root, "rev-parse", f"refs/tags/{tag}^{{commit}}"):
        raise ValueError("API checkout differs from the published tag")
    if git(sdk, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("Selected SDK source is modified")
    project = tomllib.loads((sdk / "pyproject.toml").read_text())["project"]
    if project["version"] != tag or importlib.metadata.version("durable-workflow") != tag:
        raise ValueError("Published SDK source/package version differs from the selected tag")
    if not re.fullmatch(r"[0-9a-f]{64}", environment.get("DOCS_WHEEL_SHA256", "")):
        raise ValueError("Published wheel digest is required")
    return revision, project


def published_landing(root: Path, sdk: Path, project: dict[str, Any], tag: str) -> str:
    landing = (root / "docs/index.md").read_text()
    if CODE_BLOCKS.findall(landing) != CODE_BLOCKS.findall((sdk / "docs/index.md").read_text()):
        raise ValueError("Landing examples differ from the selected published SDK documentation")
    floors = re.findall(r"<span>Python (\d+\.\d+)\+</span>", landing)
    if len(floors) != 1 or project.get("requires-python") != f">={floors[0]}":
        raise ValueError("Landing Python support floor differs from the published SDK")
    installed_python = importlib.metadata.distribution("durable-workflow").metadata.get("Requires-Python")
    if installed_python != project["requires-python"]:
        raise ValueError("Installed Python support metadata differs from the selected SDK source")
    landing, matches = re.subn(
        r"Python SDK · \d+\.\d+\.\d+ published", f"Python SDK · {tag} published", landing,
    )
    if matches != 1:
        raise ValueError("Published package label is missing or ambiguous")
    return landing


def published_reference(root: Path, sdk: Path, selected: str = "") -> tuple[str, bytes]:
    published_revision = git(sdk, "rev-parse", "HEAD")
    current = git(root, "rev-parse", "HEAD")
    revision = current
    if selected:
        if not re.fullmatch(r"[0-9a-f]{40}", selected):
            raise ValueError("Reference guide requires an exact merged commit")
        try:
            git(root, "merge-base", "--is-ancestor", selected, current)
        except subprocess.CalledProcessError as error:
            raise ValueError("Reference guide commit is outside accepted documentation history") from error
        revision = selected
    changed_api = git(root, "diff", "--name-only", published_revision, revision, "--", "src/durable_workflow")
    if changed_api:
        if selected:
            raise ValueError("Selected reference guide contains unpublished SDK source")
        revision = published_revision
    reference = subprocess.check_output(["git", "-C", str(root), "show", f"{revision}:docs/sdk-reference.md"])
    published = (sdk / "docs/sdk-reference.md").read_bytes()
    if CODE_BLOCKS.findall(reference.decode()) != CODE_BLOCKS.findall(published.decode()):
        raise ValueError("Reference examples differ from the selected published SDK documentation")
    return revision, reference


def stage_published_docs(
    root: Path, sdk: Path, config: dict[str, Any], project: dict[str, Any], tag: str,
    reference_commit: str = "",
) -> dict[str, str]:
    stage = Path(tempfile.mkdtemp(prefix=".docs-stage-", dir=root))
    # Start from the published tag. The landing and presentation can advance
    # independently. Reference prose can advance only for identical SDK source
    # and unchanged examples; API pages and all other guides remain tag-bound.
    for source in source_files(sdk, "docs"):
        target = stage / source.relative_to(sdk / "docs")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    for directory in PRESENTATION_DIRECTORIES:
        shutil.copytree(root / "docs" / directory, stage / directory, dirs_exist_ok=True)
    landing = published_landing(root, sdk, project, tag)
    (stage / "index.md").write_text(landing)
    reference_revision, reference = published_reference(root, sdk, reference_commit)
    (stage / "sdk-reference.md").write_bytes(reference)
    config["docs_dir"] = str(stage)
    config["nav"] = yaml.safe_load((sdk / "mkdocs.yml").read_text())["nav"]
    documentation = next(plugin["mkdocstrings"] for plugin in config["plugins"]
                         if isinstance(plugin, dict) and "mkdocstrings" in plugin)
    documentation["handlers"]["python"]["paths"] = [str(sdk / "src")]
    config["extra"]["published_sdk_version"] = tag
    return {
        "path": "sdk-reference.md", "source_revision": reference_revision,
        "sha256": hashlib.sha256(reference).hexdigest(),
    }


def prepare(root: Path, environment: dict[str, str]) -> dict[str, Any]:
    for name in (".docs-build.yml", ".docs-api-inventory.json", ".docs-provenance.json"):
        (root / name).unlink(missing_ok=True)
    config = yaml.safe_load((root / "mkdocs.yml").read_text())
    docs_revision = git(root, "rev-parse", "HEAD")
    if environment.get("GITHUB_SHA", docs_revision) != docs_revision:
        raise ValueError("Documentation checkout differs from the selected workflow revision")
    sdk = root
    channel = "source-preview"
    reference_guide = None
    tag = environment.get("STABLE_TAG", "")
    reference_commit = environment.get("DOCS_REFERENCE_GUIDE_COMMIT", "")
    if reference_commit and not tag:
        raise ValueError("Reference guide selection requires a published SDK tag")
    if tag:
        sdk = root / ".docs-sdk"
        revision, project = published_identity(root, sdk, tag, environment)
        version = project["version"]
        reference_guide = stage_published_docs(root, sdk, config, project, tag, reference_commit)
        channel = "stable"
    else:
        revision = docs_revision
        version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    inventory = sdk_inventory(sdk, verify_installed=bool(tag))
    config["extra"]["docs_channel"] = channel
    (root / ".docs-build.yml").write_text(yaml.safe_dump(config, sort_keys=False))
    inventory_bytes = (json.dumps(inventory, sort_keys=True, separators=(",", ":")) + "\n").encode()
    (root / ".docs-api-inventory.json").write_bytes(inventory_bytes)
    audit = {
        "schema": "durable-workflow.sdk-python.docs-deployment/v2",
        "channel": channel,
        "docs_source_revision": docs_revision,
        "sdk_source_revision": revision,
        "sdk_version": version,
        "sdk_source_inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
        "published_wheel_sha256": environment.get("DOCS_WHEEL_SHA256") if tag else None,
        "reference_guide": reference_guide,
    }
    (root / ".docs-provenance.json").write_text(json.dumps(audit, sort_keys=True) + "\n")
    return audit


if __name__ == "__main__":
    prepare(Path.cwd(), dict(os.environ))
