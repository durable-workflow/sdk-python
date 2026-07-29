from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from durable_workflow import _avro

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "codec_regressions"


def _load_runner() -> ModuleType:
    path = Path(__file__).parents[1] / "scripts" / "ci" / "run-codec-regression-fixture.py"
    spec = importlib.util.spec_from_file_location("run_codec_regression_fixture", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RUNNER = _load_runner()


def _fixtures() -> list[dict[str, Any]]:
    paths = sorted(FIXTURE_DIR.glob("*.json"))
    assert paths, f"expected codec regression fixtures in {FIXTURE_DIR}"
    return [json.loads(path.read_text()) for path in paths]


@pytest.mark.parametrize("fixture", _fixtures(), ids=lambda fixture: str(fixture["id"]))
def test_checked_in_codec_regression_corpus_uses_fastavro(fixture: dict[str, Any]) -> None:
    RUNNER.execute_fixture(fixture, _avro)
