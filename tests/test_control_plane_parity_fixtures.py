from __future__ import annotations

import json
from pathlib import Path

FIXTURE_SCHEMA = "durable-workflow.polyglot.control-plane-request-fixture"
FIXTURE_VERSION = 1
FIXTURES = Path(__file__).parent / "fixtures" / "control-plane"


def test_control_plane_parity_fixtures_are_versioned_contracts() -> None:
    paths = sorted(FIXTURES.glob("*-parity.json"))
    assert paths, f"expected control-plane parity fixtures in {FIXTURES}"

    for path in paths:
        fixture = json.loads(path.read_text())
        assert fixture.get("schema") == FIXTURE_SCHEMA, (
            f"{path.name} must declare the shared control-plane parity fixture schema"
        )
        assert fixture.get("version") == FIXTURE_VERSION, (
            f"{path.name} must declare fixture contract version {FIXTURE_VERSION}"
        )
        assert fixture.get("operation"), f"{path.name} must name the operation"

        request = fixture.get("request")
        assert isinstance(request, dict), f"{path.name} must declare the request shape"
        assert request.get("method"), f"{path.name} must declare request.method"
        assert str(request.get("path", "")).startswith("/"), f"{path.name} must declare request.path"

        assert isinstance(fixture.get("semantic_body"), dict), f"{path.name} must declare semantic_body"
        assert isinstance(fixture.get("cli"), dict), f"{path.name} must declare the CLI projection"
        assert isinstance(fixture.get("sdk_python"), dict), (
            f"{path.name} must declare the Python SDK projection"
        )
