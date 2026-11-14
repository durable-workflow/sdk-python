"""Exercise source/package/content separation with local inert Git fixtures."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

if sys.version_info < (3, 11):
    pytest.skip("The portal build uses Python 3.12 tomllib", allow_module_level=True)

SCRIPT = Path(__file__).parents[1] / "scripts/prepare_portal.py"
SPEC = importlib.util.spec_from_file_location("prepare_portal", SCRIPT)
assert SPEC is not None
assert SPEC.loader is not None
portal = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(portal)


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-c", "core.hooksPath=/dev/null", "-c", "user.name=Portal fixture",
         "-c", "user.email=portal@example.test", "-C", str(root), *args], text=True,
        stderr=subprocess.DEVNULL,
    ).strip()


@pytest.fixture
def sources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, dict[str, str], Path]:
    root = tmp_path / "repo"
    root.mkdir()
    files = {
        "pyproject.toml": '[project]\nversion = "2.14.1"\nrequires-python = ">=3.10"\n',
        ".gitignore": ".docs-sdk/\n.docs-stage-*/\n.docs-*.json\n.docs-build.yml\n",
        "src/durable_workflow/__init__.py": 'PUBLIC = "published"\n',
        "docs/index.md": '# Published\n```python\nprint("complete example")\n```\n',
        "docs/reference/metrics.md": "Published metric contract\n",
        "docs/sdk-reference.md": '# Reference\nPublished explanation\n```python\nprint("published API")\n```\n',
        "docs/assets/logo.svg": "published-logo",
        "docs/stylesheets/layout.css": "published-style",
        "docs/javascripts/identity.js": "published-script",
    }
    config: dict[str, Any] = {
        "docs_dir": "docs", "plugins": ["search", {"mkdocstrings": {"handlers": {"python": {"paths": ["src"]}}}}],
        "extra": {"published_sdk_version": "2.14.1"}, "nav": [{"Published metrics": "reference/metrics.md"}],
    }
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    (root / "mkdocs.yml").write_text(yaml.safe_dump(config))
    git(root, "init")
    git(root, "add", ".")
    git(root, "commit", "-m", "Published fixture")
    git(root, "tag", "2.14.1")
    git(root, "worktree", "add", "--detach", ".docs-sdk", "2.14.1")
    (root / "pyproject.toml").write_text('[project]\nversion = "2.15.0"\n')
    (root / "src/durable_workflow/__init__.py").write_text('PUBLIC = "unreleased"\n')
    (root / "docs/reference/metrics.md").write_text("Unreleased metric contract\n")
    (root / "docs/sdk-reference.md").write_text(
        '# Reference\nCorrected explanation\n```python\nprint("published API")\n```\n',
    )
    (root / "docs/index.md").write_text(
        '# Redesigned\nPython SDK · 2.14.1 published\n<span>Python 3.10+</span>\n'
        '```python\nprint("complete example")\n```\n',
    )
    (root / "docs/stylesheets/layout.css").write_text("redesigned-style")
    config["nav"] = [{"Unreleased page": "future.md"}]
    (root / "mkdocs.yml").write_text(yaml.safe_dump(config))
    git(root, "add", ".")
    git(root, "commit", "-m", "Current docs source")
    installed = tmp_path / "installed/durable_workflow"
    installed.mkdir(parents=True)
    (installed / "__init__.py").write_text('PUBLIC = "published"\n')
    monkeypatch.setattr(portal.importlib.metadata, "version", lambda _name: "2.14.1")
    monkeypatch.setattr(portal.importlib.metadata, "distribution", lambda _name: SimpleNamespace(
        locate_file=lambda path: installed.parent / path, metadata={"Requires-Python": ">=3.10"},
    ))
    env = {"STABLE_TAG": "2.14.1", "DOCS_WHEEL_SHA256": "1" * 64, "GITHUB_SHA": git(root, "rev-parse", "HEAD")}
    return root, env, installed


def test_stable_build_combines_current_presentation_and_published_content(sources: tuple) -> None:
    root, env, _installed = sources
    audit = portal.prepare(root, env)
    config = yaml.safe_load((root / ".docs-build.yml").read_text())
    stage = Path(config["docs_dir"])
    assert (stage / "reference/metrics.md").read_text() == "Published metric contract\n"
    assert (stage / "sdk-reference.md").read_text() == (root / ".docs-sdk/docs/sdk-reference.md").read_text()
    assert (stage / "stylesheets/layout.css").read_text() == "redesigned-style"
    assert (stage / "index.md").read_text().startswith("# Redesigned")
    assert config["nav"] == [{"Published metrics": "reference/metrics.md"}]
    assert config["plugins"][1]["mkdocstrings"]["handlers"]["python"]["paths"] == [str(root / ".docs-sdk/src")]
    assert audit["docs_source_revision"] != audit["sdk_source_revision"]
    assert audit["sdk_version"] == "2.14.1"
    assert audit["channel"] == "stable"
    inventory = json.loads((root / ".docs-api-inventory.json").read_text())
    assert [row["path"] for row in inventory] == ["durable_workflow/__init__.py"]
    assert str(root) not in (root / ".docs-provenance.json").read_text()


def match_published_api(root: Path, env: dict[str, str]) -> None:
    source = root / "src/durable_workflow/__init__.py"
    source.write_bytes((root / ".docs-sdk/src/durable_workflow/__init__.py").read_bytes())
    git(root, "add", ".")
    git(root, "commit", "-m", "Same published API with corrected reference prose")
    env["GITHUB_SHA"] = git(root, "rev-parse", "HEAD")


def test_same_published_api_can_deploy_corrected_reference_prose(sources: tuple) -> None:
    root, env, _installed = sources
    match_published_api(root, env)
    audit = portal.prepare(root, env)
    config = yaml.safe_load((root / ".docs-build.yml").read_text())
    stage = Path(config["docs_dir"])
    assert (stage / "sdk-reference.md").read_bytes() == (root / "docs/sdk-reference.md").read_bytes()
    assert (stage / "reference/metrics.md").read_text() == "Published metric contract\n"
    assert audit["reference_guide"]["source_revision"] == env["GITHUB_SHA"]
    assert config["plugins"][1]["mkdocstrings"]["handlers"]["python"]["paths"] == [str(root / ".docs-sdk/src")]


@pytest.mark.parametrize("change", ["changed-source", "added-source"])
def test_unreleased_api_keeps_reference_at_published_tag(sources: tuple, change: str) -> None:
    root, env, _installed = sources
    match_published_api(root, env)
    source = root / "src/durable_workflow" / ("__init__.py" if change == "changed-source" else "future.py")
    source.write_text('PUBLIC = "unreleased"\n')
    git(root, "add", ".")
    git(root, "commit", "-m", "Unreleased API")
    env["GITHUB_SHA"] = git(root, "rev-parse", "HEAD")
    audit = portal.prepare(root, env)
    config = yaml.safe_load((root / ".docs-build.yml").read_text())
    assert (Path(config["docs_dir"]) / "sdk-reference.md").read_bytes() == (
        root / ".docs-sdk/docs/sdk-reference.md"
    ).read_bytes()
    assert audit["reference_guide"]["source_revision"] == audit["sdk_source_revision"]


def test_changed_reference_example_refuses_same_api_overlay(sources: tuple) -> None:
    root, env, _installed = sources
    match_published_api(root, env)
    portal.prepare(root, env)
    (root / "docs/sdk-reference.md").write_text('```python\nprint("future API")\n```\n')
    git(root, "add", ".")
    git(root, "commit", "-m", "Changed reference example")
    env["GITHUB_SHA"] = git(root, "rev-parse", "HEAD")
    with pytest.raises(ValueError, match="Reference examples differ"):
        portal.prepare(root, env)
    assert not (root / ".docs-provenance.json").exists()


def test_uncommitted_reference_edit_cannot_change_recorded_guide(sources: tuple) -> None:
    root, env, _installed = sources
    match_published_api(root, env)
    committed = (root / "docs/sdk-reference.md").read_bytes()
    (root / "docs/sdk-reference.md").write_text("Uncommitted explanation")
    audit = portal.prepare(root, env)
    config = yaml.safe_load((root / ".docs-build.yml").read_text())
    assert (Path(config["docs_dir"]) / "sdk-reference.md").read_bytes() == committed
    assert audit["reference_guide"]["source_revision"] == env["GITHUB_SHA"]


@pytest.mark.parametrize("fault", [
    "sdk-source", "installed-code", "installed-version", "example", "wheel", "docs-revision", "support-floor",
])
def test_changed_identity_or_examples_refuse_and_remove_stale_audit(
    sources: tuple, monkeypatch: pytest.MonkeyPatch, fault: str,
) -> None:
    root, env, installed = sources
    portal.prepare(root, env)
    if fault == "sdk-source":
        (root / ".docs-sdk/src/durable_workflow/__init__.py").write_text("tampered")
    elif fault == "installed-code":
        (installed / "__init__.py").write_text("different wheel bytes")
    elif fault == "installed-version":
        monkeypatch.setattr(portal.importlib.metadata, "version", lambda _name: "2.15.0")
    elif fault == "example":
        (root / "docs/index.md").write_text('```python\nprint("new API")\n```\n')
    elif fault == "wheel":
        env["DOCS_WHEEL_SHA256"] = ""
    elif fault == "support-floor":
        page = root / "docs/index.md"
        page.write_text(page.read_text().replace("Python 3.10+", "Python 3.11+"))
    else:
        env["GITHUB_SHA"] = "0" * 40
    with pytest.raises(ValueError):
        portal.prepare(root, env)
    assert not (root / ".docs-provenance.json").exists()


def test_source_build_stays_preview_with_current_api_and_no_package_claim(sources: tuple) -> None:
    root, env, _installed = sources
    env["STABLE_TAG"] = ""
    audit = portal.prepare(root, env)
    assert audit["channel"] == "source-preview"
    assert audit["sdk_version"] == "2.15.0"
    assert audit["docs_source_revision"] == audit["sdk_source_revision"]
    assert audit["published_wheel_sha256"] is None
