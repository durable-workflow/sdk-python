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


def _encode_reject_fixture(
    identity: str,
    *,
    value: dict[str, Any] | None = None,
    wire_base64: str = "AA==",
    error: str = "non_finite_float",
) -> dict[str, Any]:
    fixture = _codec_fixture(identity)
    fixture["value"] = (
        value if value is not None else {"type": "double", "value": "nan"}
    )
    fixture["framing"]["wire_base64"] = wire_base64
    fixture["failure_policy"] = {
        "operation": "encode_reject",
        "error": error,
    }
    return fixture


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


def _replay_fixture(identity: str, workflow_type: str = "golden.single-activity") -> dict[str, Any]:
    return {
        "$schema": "https://example.invalid/evidence-schema.json",
        "fixture_schema": "durable-workflow.replay-regression/v1",
        "id": identity,
        "protocol_version": "1.0",
        "bindings": ["python"],
        "workflow": {"type": workflow_type, "input": ["Ada"]},
        "history": [
            {
                "event_type": "ActivityCompleted",
                "payload": {"result": '"hello Ada"'},
            }
        ],
        "expected": {
            "command_type": "CompleteWorkflow",
            "result": "hello Ada",
        },
    }


def _golden_history_fixture() -> dict[str, Any]:
    replay = _replay_fixture("golden-source")
    return {
        "fixture_schema": "durable-workflow.golden-history.v1",
        "source": {
            "runtime": "sdk-python",
            "version": "1.0.0",
            "worker_protocol_version": "1.0",
        },
        "cases": [
            {
                "name": "single_activity",
                "workflow_type": replay["workflow"]["type"],
                "start_input": replay["workflow"]["input"],
                "history": replay["history"],
                "expected": replay["expected"],
            }
        ],
    }


class CanonicalIdentityTest(unittest.TestCase):
    def codec_inventory(self, *fixtures: dict[str, Any]) -> list[Any]:
        policy = {
            "binding": "python",
            "categories": {
                "codec": {
                    "fixtures": [
                        {
                            "glob": "codec/*.json",
                            "format": "codec-regression-v1",
                        }
                    ],
                    "guards": [{"glob": "src/codec.py"}],
                }
            },
        }
        files = {
            f"codec/{index}.json": json.dumps(fixture).encode()
            for index, fixture in enumerate(fixtures)
        }
        return VALIDATOR._inventory(policy, files)

    def replay_inventory(self, replay: dict[str, Any]) -> list[Any]:
        policy = {
            "binding": "python",
            "categories": {
                "replay": {
                    "fixtures": [
                        {
                            "glob": "golden/*.json",
                            "format": "golden-history-v1",
                        },
                        {
                            "glob": "replay/*.json",
                            "format": "replay-regression-v1",
                        },
                    ],
                    "guards": [{"glob": "src/replay.py"}],
                }
            },
        }
        files = {
            "golden/base.json": json.dumps(_golden_history_fixture()).encode(),
            "replay/candidate.json": json.dumps(replay).encode(),
        }
        return VALIDATOR._inventory(policy, files)

    def test_golden_history_rewrap_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self.replay_inventory(_replay_fixture("rewrapped"))

    def test_golden_history_rewrap_with_nested_command_expected_is_rejected(self) -> None:
        replay = _replay_fixture("nested-command-rewrap")
        replay["expected"] = {
            "command_sequence": [
                {
                    "type": "complete_workflow",
                    "result": "hello Ada",
                }
            ]
        }

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self.replay_inventory(replay)

    def test_golden_history_rewrap_with_redundant_command_assertions_is_rejected(
        self,
    ) -> None:
        replay = _replay_fixture("redundant-command-rewrap")
        commands = [
            {
                "type": "complete_workflow",
                "result": "hello Ada",
            }
        ]
        replay["command_sequence"] = commands
        replay["expected"] = {"command_sequence": commands}

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self.replay_inventory(replay)

    def test_genuinely_new_replay_behavior_grows_inventory(self) -> None:
        evidence = self.replay_inventory(
            _replay_fixture("new-behavior", workflow_type="golden.other")
        )

        self.assertEqual(2, len(evidence))

    def test_noncanonical_base64_spelling_is_rejected(self) -> None:
        with self.assertRaisesRegex(VALIDATOR.CorpusError, "is not canonical base64"):
            VALIDATOR._canonical_base64("AB==", "wire")

    def test_encode_reject_wire_only_variant_is_duplicate_semantic_evidence(self) -> None:
        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self.codec_inventory(
                _encode_reject_fixture("base", wire_base64="AA=="),
                _encode_reject_fixture("wire-only-variant", wire_base64="Ag=="),
            )

    def test_encode_reject_value_and_error_policy_remain_distinct(self) -> None:
        evidence = self.codec_inventory(
            _encode_reject_fixture("base"),
            _encode_reject_fixture(
                "different-value",
                value={"type": "double", "value": "infinity"},
            ),
            _encode_reject_fixture(
                "different-error",
                error="Avro Value doubles must be finite",
            ),
        )

        self.assertEqual(3, len(evidence))
        self.assertEqual(3, len({item.semantic_digest for item in evidence}))

    def test_encode_reject_fixture_still_validates_wire_syntax(self) -> None:
        fixture = _encode_reject_fixture("noncanonical-wire", wire_base64="AB==")

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "is not canonical base64"):
            VALIDATOR._codec_fixture(fixture, "noncanonical-wire.json", "python")

    def test_codec_fixture_accepts_empty_canonical_wire_when_required(self) -> None:
        for operation, error in (
            ("round_trip", None),
            ("decode_reject", "invalid_payload_framing"),
        ):
            with self.subTest(operation=operation):
                fixture = _codec_fixture(f"empty-{operation}")
                fixture["framing"]["wire_base64"] = ""
                fixture["failure_policy"] = {
                    "operation": operation,
                    "error": error,
                }

                evidence = VALIDATOR._codec_fixture(fixture, "empty-wire.json", "python")

                self.assertEqual(1, len(evidence))

    def test_codec_fixture_required_wire_rejects_null(self) -> None:
        for operation, error in (
            ("round_trip", None),
            ("decode_reject", "invalid_payload_framing"),
        ):
            with self.subTest(operation=operation):
                fixture = _codec_fixture(f"null-{operation}")
                fixture["framing"]["wire_base64"] = None
                fixture["failure_policy"] = {
                    "operation": operation,
                    "error": error,
                }

                with self.assertRaisesRegex(
                    VALIDATOR.CorpusError,
                    f"must include wire_base64 for {operation}",
                ):
                    VALIDATOR._codec_fixture(fixture, "null-wire.json", "python")

    def test_codec_fixture_rejects_missing_required_nullable_members(self) -> None:
        fixtures = []

        missing_fingerprint = _codec_fixture("missing-fingerprint")
        del missing_fingerprint["protocol"]["fingerprint"]
        fixtures.append((missing_fingerprint, "protocol.fingerprint"))

        missing_wire = _encode_reject_fixture("missing-wire")
        del missing_wire["framing"]["wire_base64"]
        fixtures.append((missing_wire, "framing.wire_base64"))

        missing_error = _codec_fixture("missing-error")
        del missing_error["failure_policy"]["error"]
        fixtures.append((missing_error, "failure_policy.error"))

        for fixture, member in fixtures:
            with self.subTest(member=member), self.assertRaisesRegex(
                VALIDATOR.CorpusError,
                rf"{member} is required",
            ):
                VALIDATOR._codec_fixture(fixture, "missing-member.json", "python")

    def test_codec_fixture_accepts_explicit_null_for_nullable_members(self) -> None:
        round_trip = _codec_fixture("nullable-metadata")
        encode_reject = _encode_reject_fixture("nullable-wire")
        encode_reject["framing"]["wire_base64"] = None

        self.assertEqual(
            1,
            len(VALIDATOR._codec_fixture(round_trip, "round-trip.json", "python")),
        )
        self.assertEqual(
            1,
            len(VALIDATOR._codec_fixture(encode_reject, "encode-reject.json", "python")),
        )

    def test_codec_fixture_rejects_non_string_wire(self) -> None:
        fixture = _codec_fixture("non-string-wire")
        fixture["framing"]["wire_base64"] = []

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "must be a non-empty string"):
            VALIDATOR._codec_fixture(fixture, "non-string-wire.json", "python")

    def test_codec_fixture_rejects_noncanonical_wire(self) -> None:
        fixture = _codec_fixture("noncanonical-wire")
        fixture["framing"]["wire_base64"] = "AB=="

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "is not canonical base64"):
            VALIDATOR._codec_fixture(fixture, "noncanonical-wire.json", "python")

    def test_malformed_golden_wire_must_be_canonical_base64(self) -> None:
        fixture = _avro_golden_fixture()
        fixture["malformed_frames"][0]["wire_base64"] = "%%%"

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "is not canonical base64"):
            VALIDATOR._avro_golden_fixture(fixture, "golden.json")


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

    def test_rejects_noncanonical_and_unsupported_protocol_versions(self) -> None:
        with tempfile.TemporaryDirectory(prefix="regression-corpus-codec-version-") as temporary:
            root = Path(temporary)
            source = root / "src/durable_workflow"
            source.mkdir(parents=True)
            (source / "__init__.py").write_text("", encoding="utf-8")
            (source / "_avro.py").write_text(
                "VALUE_SCHEMA_FINGERPRINT_HEX = None\n"
                "def encode(value):\n    return 'AA=='\n"
                "def decode(wire):\n    return 0\n",
                encoding="utf-8",
            )
            fixture_path = root / "fixture.json"
            for version in ("01", "v1", "avro-value-v1", "2"):
                with self.subTest(version=version):
                    fixture = _codec_fixture("version-alias")
                    fixture["protocol"]["version"] = version
                    fixture_path.write_text(
                        f"{json.dumps(fixture, indent=2)}\n",
                        encoding="utf-8",
                    )
                    result = subprocess.run(
                        [
                            sys.executable,
                            str(CODEC_RUNNER),
                            "--source-root",
                            str(root),
                            "--fixture",
                            str(fixture_path),
                        ],
                        check=False,
                        capture_output=True,
                        text=True,
                    )

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(
                        "canonical version supported by the Python Avro binding",
                        result.stderr,
                    )


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

    def _add_avro_golden_to_base(
        self,
        malformed_wire: str | None = None,
        *,
        malformed_name: str | None = None,
    ) -> None:
        self.policy["categories"]["codec"]["fixtures"].append(
            {
                "glob": "schema/avro-value-v1-golden.json",
                "format": "avro-value-golden-v1",
            }
        )
        self._write_json("regression-corpus-policy.json", self.policy)
        golden = _avro_golden_fixture()
        if malformed_wire is not None:
            golden["malformed_frames"][0]["wire_base64"] = malformed_wire
        if malformed_name is not None:
            golden["malformed_frames"][0]["name"] = malformed_name
        self._write_json("schema/avro-value-v1-golden.json", golden)
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

    def _write_malformed_golden_wire(self, wire: str) -> None:
        golden = _avro_golden_fixture()
        golden["malformed_frames"][0]["wire_base64"] = wire
        self._write_json("schema/avro-value-v1-golden.json", golden)

    def _write_malformed_golden_name(self, name: str) -> None:
        golden = _avro_golden_fixture()
        golden["malformed_frames"][0]["wire_base64"] = "JSUl"
        golden["malformed_frames"][0]["name"] = name
        self._write_json("schema/avro-value-v1-golden.json", golden)

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

    def test_malformed_wire_migration_rejects_different_decoded_bytes(self) -> None:
        self._add_avro_golden_to_base("AR==")
        self._write_malformed_golden_wire("Ag==")

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "immutable fixture file"):
            self._validate()

    def test_malformed_wire_migration_accepts_same_decoded_bytes(self) -> None:
        self._add_avro_golden_to_base("AR==")
        self._write_malformed_golden_wire("AQ==")

        result = self._validate()

        self.assertEqual(result["counts"]["codec"]["base"], result["counts"]["codec"]["current"])

    def test_malformed_wire_migration_rejects_invalid_base64_repair(self) -> None:
        self._add_avro_golden_to_base("%%%")
        self._write_malformed_golden_wire("JSUl")

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "immutable fixture file"):
            self._validate()

    def test_malformed_name_migration_accepts_decoded_behavior_reclassification(self) -> None:
        self._add_avro_golden_to_base("JSUl", malformed_name="invalid_base64")
        self._write_malformed_golden_name("decoded_non_magic_bytes")

        result = self._validate()

        self.assertEqual(result["counts"]["codec"]["base"], result["counts"]["codec"]["current"])

    def test_malformed_name_migration_rejects_unrelated_reclassification(self) -> None:
        self._add_avro_golden_to_base("JSUl", malformed_name="invalid_base64")
        self._write_malformed_golden_name("unrelated_name")

        with self.assertRaisesRegex(VALIDATOR.CorpusError, "immutable fixture file"):
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

    def test_fixture_only_growth_runs_candidate_binding(self) -> None:
        self._write_json(
            "tests/fixtures/codec_regressions/unrelated.json",
            _codec_fixture("unrelated"),
        )

        result = self._validate()

        self.assertEqual(result["counts"]["codec"]["added"], 1)
        self.assertFalse(result["counts"]["codec"]["related_change"])
        self.assertEqual(result["counts"]["codec"]["revision_verified"], 1)

    def test_fixture_only_growth_rejects_candidate_failure(self) -> None:
        self._write_json(
            "tests/fixtures/codec_regressions/candidate-failure.json",
            _codec_fixture("candidate-failure"),
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "does not pass on the candidate through the official Python binding",
        ):
            self._validate()

    def test_guarded_growth_rejects_encode_reject_wire_only_variant(self) -> None:
        self._write_json(
            "tests/fixtures/codec_regressions/base.json",
            _encode_reject_fixture("base"),
        )
        self._git("add", ".")
        self._git(
            "-c",
            "user.name=Regression Corpus Test",
            "-c",
            "user.email=regression-corpus@example.invalid",
            "commit",
            "--quiet",
            "--message=encode-reject-baseline",
        )
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        self._write_json(
            "tests/fixtures/codec_regressions/wire-only-variant.json",
            _encode_reject_fixture(
                "wire-only-variant",
                wire_base64="Ag==",
            ),
        )

        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "duplicate semantic fixtures",
        ):
            self._validate()

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

    def test_fixture_only_leading_zero_rewrap_cannot_increase_corpus(self) -> None:
        self._add_avro_golden_to_base()
        baseline = self._validate()
        self._write_cross_format_codec_fixture(
            identity="leading-zero-long-seven",
            value={"type": "long", "value": "7"},
            wire_base64="wwHioz3/VYAiNwQO",
            operation="round_trip",
            error=None,
        )
        fixture_path = self.root / "tests/fixtures/codec_regressions/leading-zero-long-seven.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        fixture["protocol"]["version"] = "01"
        self._write_json(
            "tests/fixtures/codec_regressions/leading-zero-long-seven.json",
            fixture,
        )

        self.assertEqual(
            baseline["counts"]["codec"]["base"],
            baseline["counts"]["codec"]["current"],
        )
        with self.assertRaisesRegex(
            VALIDATOR.CorpusError,
            "protocol.version must be a canonical positive integer",
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

    def test_empty_wire_rejection_rewrap_is_duplicate_semantic_evidence(self) -> None:
        self._add_avro_golden_to_base("", malformed_name="empty_blob")
        self._write_text(
            "src/durable_workflow/serializer.py",
            "def encode(value):\n    return str(value)\n",
        )
        self._write_cross_format_codec_fixture(
            identity="rewrapped-empty-blob",
            value={"type": "null"},
            wire_base64="",
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
            "is not canonical base64",
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
        duplicate["protocol"]["codec"] = "renamed-codec"
        duplicate["protocol"]["schema"] = "renamed-schema"
        duplicate["protocol"]["version"] = "999"
        duplicate["protocol"]["fingerprint"] = "metadata-only"
        duplicate["framing"]["encoding"] = "renamed-encoding"
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
