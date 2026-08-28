from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType

import pytest
from scripts import check_release_metadata
from scripts.check_release_metadata import ReleaseMetadataError, SourceMetadata

from durable_workflow.client import PROTOCOL_VERSION

REPO_ROOT = Path(__file__).resolve().parents[1]


def source_metadata() -> SourceMetadata:
    return SourceMetadata(
        commit="a" * 40,
        name="durable-workflow",
        version="2.0.0-rc.37",
        registry_version="2.0.0rc37",
        summary="Release candidate Python SDK for the Durable Workflow 2.0 train",
        classifiers=("Programming Language :: Python :: 3",),
        readme=(
            "# Durable Workflow\n\n"
            "Build replay-safe Python workflows with explicit activities and durable execution semantics.\n"
        ),
    )


def test_worker_release_identity_matches_supported_server_and_protocol() -> None:
    manifest = check_release_metadata._load_toml_parser().loads(
        (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    project = manifest["project"]
    release = manifest["tool"]["durable-workflow"]

    assert project["version"] == "2.0.0-rc.37"
    assert release["product-train"] == project["version"]
    assert release["registry-version"] == "2.0.0rc37"
    assert release["supported-server-versions"] == "2.0.0-rc.51"
    assert release["worker-protocol-version"] == PROTOCOL_VERSION == "1.16"


def pypi_json(source: SourceMetadata, **overrides: object) -> bytes:
    info: dict[str, object] = {
        "name": source.name,
        "version": source.registry_version,
        "summary": source.summary,
        "classifiers": list(source.classifiers),
        "description": source.readme,
        "description_content_type": "text/markdown",
    }
    info.update(overrides)
    return json.dumps({"info": info}).encode()


def test_release_metadata_loader_uses_tomli_without_stdlib_tomllib(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fallback = ModuleType("tomli")
    imports: list[str] = []

    def import_module(name: str) -> ModuleType:
        imports.append(name)
        if name == "tomllib":
            raise ModuleNotFoundError("No module named 'tomllib'", name="tomllib")
        assert name == "tomli"
        return fallback

    monkeypatch.setattr(check_release_metadata.importlib, "import_module", import_module)

    assert check_release_metadata._load_toml_parser() is fallback
    assert imports == ["tomllib", "tomli"]


def test_release_metadata_loader_accepts_the_authorized_stable_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commit = "b" * 40
    pyproject = b"""
[project]
name = "durable-workflow"
version = "2.0.0"
description = "Python SDK for Durable Workflow 2.0"
readme = "README.md"
classifiers = ["Programming Language :: Python :: 3"]

[tool.durable-workflow]
product-train = "2.0.0"
registry-version = "2.0.0"
"""
    readme = b"# Durable Workflow\n"

    def git(*arguments: str) -> bytes:
        if arguments[0] == "rev-parse":
            return f"{commit}\n".encode()
        assert arguments[0] == "show"
        return pyproject if arguments[1].endswith(":pyproject.toml") else readme

    monkeypatch.setattr(check_release_metadata, "_git", git)

    source = check_release_metadata.load_source_metadata("2.0.0")
    assert source.version == "2.0.0"
    assert source.registry_version == "2.0.0"


def test_normal_project_page_is_retained_as_rendered_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    source = source_metadata()
    marker = "Build replay-safe Python workflows with explicit activities and durable execution semantics."

    def request(url: str, accept: str) -> bytes:
        if accept == "application/json":
            return pypi_json(source)
        assert url.endswith(f"/{source.registry_version}/")
        return f"<html><body><h1>{source.registry_version}</h1><p>{marker}</p></body></html>".encode()

    monkeypatch.setattr(check_release_metadata, "_request_bytes", request)

    audit = check_release_metadata.verify_pypi(source, source.version)

    assert audit.outcome == "match"


def test_200_client_challenge_is_advisory_after_exact_json_matches(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = source_metadata()
    challenge = b"""<!doctype html>
    <html><head><title>Client Challenge</title></head>
    <body><p>JavaScript is disabled</p><p>Checking your browser...</p></body></html>
    """

    def request(url: str, accept: str) -> bytes:
        del url
        return pypi_json(source) if accept == "application/json" else challenge

    monkeypatch.setattr(check_release_metadata, "_request_bytes", request)
    monkeypatch.setattr(check_release_metadata, "load_source_metadata", lambda _source_ref: source)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_release_metadata.py",
            "--source-ref",
            source.commit,
            "--pypi-version",
            source.version,
        ],
    )

    assert check_release_metadata.main() == 0

    output = capsys.readouterr()
    assert "exact PyPI JSON matches" in output.out
    assert "::warning title=PyPI rendered-page audit::" in output.err
    assert "exact PyPI JSON remains authoritative" in output.err


def test_exact_json_metadata_mismatch_remains_release_blocking(monkeypatch: pytest.MonkeyPatch) -> None:
    source = source_metadata()

    def request(url: str, accept: str) -> bytes:
        del url
        assert accept == "application/json"
        return pypi_json(source, version="2.0.0rc21")

    monkeypatch.setattr(check_release_metadata, "_request_bytes", request)

    with pytest.raises(ReleaseMetadataError, match="exact PyPI JSON field version"):
        check_release_metadata.verify_pypi(source, source.version)


def test_exact_json_metadata_mismatch_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    attempts: list[str] = []
    sleeps: list[float] = []

    def verify(_source: SourceMetadata, _version: str) -> check_release_metadata.ProjectPageAudit:
        attempts.append("verify")
        raise ReleaseMetadataError("exact PyPI JSON field summary differs from the source commit")

    monkeypatch.setattr(check_release_metadata, "load_source_metadata", lambda _: source)
    monkeypatch.setattr(check_release_metadata, "verify_pypi", verify)
    monkeypatch.setattr(check_release_metadata.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_release_metadata.py",
            "--source-ref",
            source.commit,
            "--pypi-version",
            source.version,
            "--attempts",
            "30",
            "--interval-seconds",
            "10",
        ],
    )

    with pytest.raises(ReleaseMetadataError, match="exact PyPI JSON field summary differs"):
        check_release_metadata.main()

    assert attempts == ["verify"]
    assert sleeps == []
