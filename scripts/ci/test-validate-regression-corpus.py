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
CODEC_RUNNER = Path(__file__).with_name("run-codec-regression-fixture.py")


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


def _avro_golden_fixture() -> dict[str, Any]:
    return {
        "schema": "durable_workflow.protocol.Value",
        "fingerprint": "e2a33dff55802237",
        "cases": [
            {
                "name": "long_7",
                "kind": "long",
                "value": "7",
                "wire_base64": "wwHioz3/VYAiNwQO",
            }
        ],
        "malformed_frames": [
            {
                "name": "short_frame",
                "error": "invalid_payload_framing",
                "wire_base64": "wwE=",
            }
        ],
        "alternate_map_orders": [
            {
                "name": "map_order",
                "wire_base64": ["Ag==", "Aw=="],
            }
        ],
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


class OfficialCodecRunnerTest(unittest.TestCase):
    def test_source_root_selects_candidate_or_base_binding(self) -> None:
        with tempfile.TemporaryDirectory(prefix="regression-corpus-codec-runner-") as temporary:
            root = Path(temporary)
            fixture = root / "fixture.json"
            fixture.write_text(
                f"{json.dumps(_codec_fixture('base'), indent=2)}\n",
                encoding="utf-8",
            )
            results: dict[str, subprocess.CompletedProcess[str]] = {}
            for revision, encoded in (("candidate", "AA=="), ("base", "Ag==")):
                source = root / revision / "src/durable_workflow"
                source.mkdir(parents=True)
                (source / "__init__.py").write_text("", encoding="utf-8")
                (source / "_avro.py").write_text(
                    "VALUE_SCHEMA_FINGERPRINT_HEX = None\n"
                    f"def encode(value):\n    return {encoded!r}\n"
                    "def decode(wire):\n    return 0\n",
                    encoding="utf-8",
                )
                results[revision] = subprocess.run(
                    [
                        sys.executable,
                        str(CODEC_RUNNER),
                        "--source-root",
                        str(root / revision),
                        "--fixture",
                        str(fixture),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )

            self.assertEqual(results["candidate"].returncode, 0, results["candidate"].stderr)
            self.assertNotEqual(results["base"].returncode, 0, results["base"].stderr)


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
        base_fixture = _codec_fixture("base")
        base_fixture["bindings"] = ["php", "python"]
        self._write_json(
            "tests/fixtures/codec_regressions/base.json",
            base_fixture,
        )
        self._write_json(
            "tests/fixtures/codec_archive/preexisting.json",
            _codec_fixture("preexisting"),
        )
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return value\n",
        )
        self.codec_runner = self.root / "codec-runner.py"
        self._write_text(
            "codec-runner.py",
            """\
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--source-root", type=Path, required=True)
parser.add_argument("--fixture", type=Path, required=True)
args = parser.parse_args()
identity = json.loads(args.fixture.read_text())["id"]
source = (args.source_root / "src/durable_workflow/serializer.py").read_text()
if identity == "candidate-failure":
    raise SystemExit(1)
if identity == "unrelated":
    raise SystemExit(0)
raise SystemExit(0 if "return str(value)" in source else 1)
""",
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
            codec_runner_path=self.codec_runner,
        )

    def _add_avro_golden_to_base(self) -> None:
        self.policy["categories"]["codec"]["fixtures"].append(
            {
                "glob": "schema/avro-value-v1-golden.json",
                "format": "avro-value-golden-v1",
            }
        )
        self._write_json("regression-corpus-policy.json", self.policy)
        self._write_json(
            "schema/avro-value-v1-golden.json",
            _avro_golden_fixture(),
        )
        self._git("add", ".")
        self._git(
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=add-avro-golden-baseline",
        )

    def _write_cross_format_codec_fixture(
        self,
        *,
        identity: str,
        value: dict[str, Any],
        wire_base64: str,
        operation: str,
        error: str | None,
    ) -> None:
        fixture = _codec_fixture(identity)
        fixture["protocol"] = {
            "codec": "avro",
            "schema": "durable_workflow.protocol.Value",
            "version": "1",
            "fingerprint": "e2a33dff55802237",
        }
        fixture["value"] = value
        fixture["framing"] = {
            "encoding": "avro-single-object",
            "wire_base64": wire_base64,
        }
        fixture["failure_policy"] = {"operation": operation, "error": error}
        self._write_json(
            f"tests/fixtures/codec_regressions/{identity}.json",
            fixture,
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

    def test_guarded_change_accepts_counterfactual_evidence(self) -> None:
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
        self.assertEqual(result["counts"]["codec"]["revision_verified"], 1)

    def test_unrelated_fixture_beside_counterfactual_evidence_is_rejected(self) -> None:
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        counterfactual = _codec_fixture("counterfactual")
        counterfactual["value"] = {"type": "long", "value": 2}
        counterfactual["framing"]["wire_base64"] = "BA=="
        self._write_json(
            "tests/fixtures/codec_regressions/counterfactual.json",
            counterfactual,
        )
        self._write_json(
            "tests/fixtures/codec_regressions/unrelated.json",
            _codec_fixture("unrelated"),
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "also passes on the defective base",
        ):
            self._validate()

    def test_guarded_change_rejects_fixture_that_fails_on_candidate(self) -> None:
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        self._write_json(
            "tests/fixtures/codec_regressions/candidate-failure.json",
            _codec_fixture("candidate-failure"),
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "does not pass on the candidate through the official Python binding",
        ):
            self._validate()

    def test_fixture_content_remains_immutable(self) -> None:
        fixture = _codec_fixture("base")
        fixture["framing"]["wire_base64"] = "BA=="
        self._write_json(
            "tests/fixtures/codec_regressions/base.json",
            fixture,
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "immutable fixture file",
        ):
            self._validate()

    def test_newer_protocol_can_explicitly_supersede_existing_evidence(self) -> None:
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        fixture = _codec_fixture("successor")
        fixture["protocol"]["version"] = "2"
        fixture["supersedes"] = ["base"]
        self._write_json(
            "tests/fixtures/codec_regressions/successor.json",
            fixture,
        )

        result = self._validate()

        self.assertEqual(result["counts"]["codec"]["added"], 1)
        self.assertTrue(result["counts"]["codec"]["related_change"])

    def test_cross_format_round_trip_rewrap_cannot_satisfy_guarded_growth(self) -> None:
        self._add_avro_golden_to_base()
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        self._write_cross_format_codec_fixture(
            identity="rewrapped-long-seven",
            value={"type": "long", "value": "7"},
            wire_base64="wwHioz3/VYAiNwQO",
            operation="round_trip",
            error=None,
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self._validate()

    def test_cross_format_rejection_rewrap_cannot_satisfy_guarded_growth(self) -> None:
        self._add_avro_golden_to_base()
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        self._write_cross_format_codec_fixture(
            identity="rewrapped-short-frame",
            value={"type": "null"},
            wire_base64="wwE=",
            operation="decode_reject",
            error="invalid_payload_framing",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self._validate()

    def test_equivalent_base64_wire_bytes_share_cross_format_identity(self) -> None:
        self._add_avro_golden_to_base()
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        self._write_cross_format_codec_fixture(
            identity="equivalent-base64-short-frame",
            value={"type": "null"},
            wire_base64="wwF=",
            operation="decode_reject",
            error="invalid_payload_framing",
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self._validate()

    def test_metadata_only_rewrap_keeps_the_same_semantic_identity(self) -> None:
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        duplicate = _codec_fixture("base")
        duplicate["id"] = "metadata-only-rewrap"
        duplicate["bindings"] = ["rust", "python", "php"]
        self._write_json(
            "tests/fixtures/codec_regressions/metadata-only-rewrap.json",
            duplicate,
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self._validate()

    def test_genuinely_new_cross_format_evidence_satisfies_guarded_growth(self) -> None:
        self._add_avro_golden_to_base()
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        self._write_cross_format_codec_fixture(
            identity="new-long-eight",
            value={"type": "long", "value": "8"},
            wire_base64="wwHioz3/VYAiNwQQ",
            operation="round_trip",
            error=None,
        )

        result = self._validate()

        self.assertEqual(result["counts"]["codec"]["added"], 1)
        self.assertTrue(result["counts"]["codec"]["related_change"])
        self.assertEqual(result["counts"]["codec"]["revision_verified"], 1)


if __name__ == "__main__":
    unittest.main()
