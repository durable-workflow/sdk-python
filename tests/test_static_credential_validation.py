from __future__ import annotations

import itertools
import json
from pathlib import Path

import pytest

from durable_workflow import Client

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures/auth_configuration/static-credentials-v1.json").read_text()
)
MAPPING = FIXTURE["language_mappings"]["python"]
OPTIONS = MAPPING["logical_setting_names"]


@pytest.mark.parametrize("option", OPTIONS.values())
@pytest.mark.parametrize("case", FIXTURE["common_scalar_cases"], ids=lambda case: case["id"])
async def test_common_static_credentials(case: dict, option: str) -> None:
    value = "".join(map(chr, case["input_code_points"]))
    if case["expected"] == "reject":
        with pytest.raises(ValueError) as error:
            Client("http://localhost:8080", **{option: value})
        assert str(error.value) == f"{option} must not have edge whitespace or control characters"
    else:
        async with Client("http://localhost:8080", **{option: value}) as client:
            assert getattr(client, option).encode("utf-8").hex() == case["expected_utf8_hex"]


@pytest.mark.parametrize("case", FIXTURE["pre_selection_cases"], ids=lambda case: case["id"])
def test_invalid_unused_credentials_are_refused(case: dict) -> None:
    options = {
        OPTIONS[name]: "".join(map(chr, points)) for name, points in case["options_code_points"].items()
    }
    with pytest.raises(ValueError) as error:
        Client("http://localhost:8080", **options)
    option = OPTIONS[case["offending_option"]]
    assert str(error.value) == f"{option} must not have edge whitespace or control characters"


@pytest.mark.parametrize("values", list(itertools.product((None, "", "fixture-token"), repeat=3)))
async def test_absence_empty_values_and_role_precedence(values: tuple[str | None, ...]) -> None:
    logical = dict(zip(OPTIONS, values, strict=True))
    options = {OPTIONS[name]: value for name, value in logical.items()}
    async with Client("http://localhost:8080", **options) as client:
        for name, value in options.items():
            assert getattr(client, name) == value
        for plane in ("control", "worker"):
            expected = next((logical[name] for name in MAPPING[f"{plane}_plane_order"] if logical[name]), None)
            opposite = "worker" if plane == "control" else "control"
            if expected is None and logical[opposite]:
                with pytest.raises(ValueError, match=f"{plane}-plane requests require"):
                    client._auth_token(worker=plane == "worker")
            else:
                assert client._auth_token(worker=plane == "worker") == expected
        discovery = next((logical[name] for name in MAPPING["discovery_order"] if logical[name]), None)
        assert client._discovery_headers().get("Authorization") == (f"Bearer {discovery}" if discovery else None)
