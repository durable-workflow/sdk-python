#!/usr/bin/env python3
"""Adversarial checks for regression-corpus implementation guards."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / "regression-corpus-policy.json"


def _load_validator() -> ModuleType:
    path = Path(__file__).with_name("validate-regression-corpus.py")
    spec = importlib.util.spec_from_file_location("validate_regression_corpus", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


VALIDATOR = _load_validator()


def _codec_worker_guard() -> dict[str, Any]:
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    guards = policy["categories"]["codec"]["guards"]
    matches = [guard for guard in guards if guard["glob"] == "src/durable_workflow/worker.py"]
    if len(matches) != 1:
        raise AssertionError("codec policy must declare exactly one worker.py guard")
    return matches[0]


def _codec_fixture(identity: str) -> dict[str, Any]:
    return {
        "$schema": "https://durable-workflow.github.io/schemas/codec-regression-v1.json",
        "fixture_schema": "durable-workflow.codec-regression/v1",
        "id": identity,
        "protocol": {
            "codec": "avro",
            "schema": "Value",
            "version": "1",
            "fingerprint": None,
        },
        "bindings": ["python"],
        "value": {"type": "long", "value": 0},
        "framing": {
            "encoding": "base64",
            "wire_base64": "AA==" if identity == "base" else "Ag==",
        },
        "failure_policy": {"operation": "round_trip", "error": None},
    }


class WorkerCodecGuardTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="regression-corpus-worker-guard-")
        self.root = Path(self.temporary.name)
        source = self.root / "src/durable_workflow/worker.py"
        source.parent.mkdir(parents=True)
        source.write_text(
            """\
def _validate_payload_codec(codec):
    if codec == "unsupported":
        raise ValueError("unsupported")
    return codec


def health_check(enabled):
    return enabled
""",
            encoding="utf-8",
        )
        self._git("init", "--quiet")
        self._git("add", "src/durable_workflow/worker.py")
        self._git(
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=baseline",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _git(self, *arguments: str) -> None:
        subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
        )

    def _guard_matches(self) -> bool:
        return VALIDATOR._guard_matches(
            self.root,
            "HEAD",
            {"src/durable_workflow/worker.py"},
            _codec_worker_guard(),
        )

    def test_neutral_edit_inside_payload_codec_validator_is_related(self) -> None:
        source = self.root / "src/durable_workflow/worker.py"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                'raise ValueError("unsupported")',
                'raise ValueError("invalid")',
            ),
            encoding="utf-8",
        )

        self.assertTrue(self._guard_matches())

    def test_edit_inside_unrelated_worker_function_is_not_related(self) -> None:
        source = self.root / "src/durable_workflow/worker.py"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "def health_check(enabled):\n    return enabled",
                "def health_check(enabled):\n    return not enabled",
            ),
            encoding="utf-8",
        )

        self.assertFalse(self._guard_matches())


class PolicyEvolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="regression-corpus-policy-")
        self.root = Path(self.temporary.name)
        self.policy = {
            "$schema": "https://durable-workflow.github.io/schemas/regression-corpus-policy-v1.json",
            "schema": "durable-workflow.regression-corpus-policy/v1",
            "repository": "sdk-python",
            "binding": "python",
            "categories": {
                "codec": {
                    "fixtures": [
                        {
                            "glob": "tests/fixtures/codec_regressions/*.json",
                            "format": "codec-regression-v1",
                        }
                    ],
                    "guards": [
                        {
                            "glob": "src/durable_workflow/serializer.py",
                            "content_patterns": [r"def encode\("],
                        }
                    ],
                }
            },
        }
        self._write_json("regression-corpus-policy.json", self.policy)
        self._write_json(
            "tests/fixtures/codec_regressions/base.json",
            _codec_fixture("base"),
        )
        self._write_json(
            "tests/fixtures/codec_archive/preexisting.json",
            _codec_fixture("preexisting"),
        )
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return value\n",
        )
        self._git("init", "--quiet")
        self._git("add", ".")
        self._git(
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=baseline",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _git(self, *arguments: str) -> None:
        subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
        )

    def _write_json(self, relative_path: str, value: Any) -> None:
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{json.dumps(value, indent=2)}\n", encoding="utf-8")

    def _write_text(self, relative_path: str, value: str) -> None:
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding="utf-8")

    def _validate(self) -> dict[str, Any]:
        return VALIDATOR.validate(
            self.root,
            Path("regression-corpus-policy.json"),
            "HEAD",
        )

    def test_fixture_selector_cannot_narrow_and_hide_deleted_evidence(self) -> None:
        self.policy["categories"]["codec"]["fixtures"][0]["glob"] = "tests/fixtures/codec_regressions/kept-*.json"
        self._write_json("regression-corpus-policy.json", self.policy)
        (self.root / "tests/fixtures/codec_regressions/base.json").unlink()

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "fixture selector was removed or narrowed",
        ):
            self._validate()

    def test_guard_selector_cannot_narrow_and_hide_implementation_change(self) -> None:
        self.policy["categories"]["codec"]["guards"][0]["glob"] = "src/durable_workflow/serializer_safe.py"
        self._write_json("regression-corpus-policy.json", self.policy)
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "guard selector was removed or narrowed",
        ):
            self._validate()

    def test_guard_content_patterns_cannot_narrow_implementation_coverage(self) -> None:
        self.policy["categories"]["codec"]["guards"][0]["content_patterns"] = [r"def decode\("]
        self._write_json("regression-corpus-policy.json", self.policy)
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "guard selector was removed or narrowed",
        ):
            self._validate()

    def test_selector_expansion_does_not_turn_existing_file_into_growth(self) -> None:
        self.policy["categories"]["codec"]["fixtures"].append(
            {
                "glob": "tests/fixtures/codec_archive/*.json",
                "format": "codec-regression-v1",
            }
        )
        self._write_json("regression-corpus-policy.json", self.policy)
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "newly added fixture",
        ):
            self._validate()

    def test_guarded_change_accepts_evidence_on_new_fixture_path(self) -> None:
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        self._write_json(
            "tests/fixtures/codec_regressions/new.json",
            _codec_fixture("new"),
        )

        result = self._validate()

        self.assertEqual(result["counts"]["codec"]["added"], 1)
        self.assertTrue(result["counts"]["codec"]["related_change"])


if __name__ == "__main__":
    unittest.main()
