from __future__ import annotations

import json
import sys

import pytest
from scripts import check_release_metadata
from scripts.check_release_metadata import ReleaseMetadataError, SourceMetadata


def source_metadata() -> SourceMetadata:
    return SourceMetadata(
        commit="a" * 40,
        name="durable-workflow",
        version="2.0.0-rc.22",
        registry_version="2.0.0rc22",
        summary="Release candidate Python SDK for the Durable Workflow 2.0 train",
        classifiers=("Programming Language :: Python :: 3",),
        readme=(
            "# Durable Workflow\n\n"
            "Build replay-safe Python workflows with explicit activities and durable execution semantics.\n"
        ),
    )


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
