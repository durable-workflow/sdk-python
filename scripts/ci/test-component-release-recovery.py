#!/usr/bin/env python3
"""Focused regressions for release recovery workflow source verification."""

from __future__ import annotations

import hashlib
import importlib.util
import io
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from cli_release_verifier_contract import (  # noqa: F401 - imported for unittest discovery
    CliRecoveryWorkflowSourceTest,
    CliReleaseAuthorityTest,
)

RECOVERY_SCRIPT = Path(__file__).with_name("component-release-recovery.py")
RUST_WORKFLOW_FIXTURE = Path(__file__).with_name("sdk-rust-release-plan-recovery.fixture.yml")
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
RECOVERY_WORKFLOW = REPOSITORY_ROOT / ".github/workflows/release-plan-recovery.yml"
PUBLISH_WORKFLOW = REPOSITORY_ROOT / ".github/workflows/publish.yml"

# This is the complete public sdk-rust workflow identified by the verifier's
# pinned digest, not a reduced semantic approximation of its shell commands.
CURRENT_RUST_RECOVERY_WORKFLOW = RUST_WORKFLOW_FIXTURE.read_text()

GENERIC_RECOVERY_WORKFLOW = r"""on:
  schedule:
  workflow_dispatch:
jobs:
  recover:
    steps:
      - run: |
          python recovery.py resolve --preparation-output release-preparation.json
          gh api --method POST "repos/$GITHUB_REPOSITORY/git/refs" \
            -f ref="refs/tags/$RELEASE_TAG" -f sha="$RELEASE_COMMIT"
          select-publication-run \
            --release-tag "$RELEASE_TAG" --release-commit "$RELEASE_COMMIT"
          gh run list --json databaseId,displayTitle,headBranch,headSha,status,conclusion
          gh workflow run release.yml --ref "$RELEASE_TAG" -f tag="$RELEASE_TAG"
"""


def load_recovery_module():
    spec = importlib.util.spec_from_file_location("component_release_recovery_test", RECOVERY_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def github_http_error(status: int, body: bytes = b"error", **headers: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "https://api.github.com/repos/durable-workflow/.github/releases",
        status,
        "request failed",
        headers,
        io.BytesIO(body),
    )


def load_recovery_for_retry_tests():
    loaded = globals().get("recovery")
    if loaded is not None:
        return loaded
    loader = globals().get("load_recovery_module")
    if not callable(loader):
        raise RuntimeError("release recovery module loader is unavailable")
    return loader()


class ContinuityGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_for_retry_tests()

    def test_scheduled_recovery_pauses_until_remote_resume(self) -> None:
        plan = {"plan": "workspace-unavailable-test"}
        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=["a" * 40, None],
            ),
            mock.patch.object(self.recovery, "read_record", return_value=plan),
            mock.patch.object(self.recovery, "validate_plan"),
        ):
            paused = self.recovery.scheduled_continuity_pause(mock.Mock(), plan)

        self.assertEqual(
            "beta-continuity/workspace-unavailable-test/resumed",
            paused["resumed_tag"],
        )
        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=["a" * 40, "b" * 40],
            ),
            mock.patch.object(self.recovery, "read_record", return_value=plan),
            mock.patch.object(self.recovery, "validate_plan"),
        ):
            self.assertIsNone(self.recovery.scheduled_continuity_pause(mock.Mock(), plan))


class PublicClientRetryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_for_retry_tests()

    def test_retries_service_failures_connection_resets_and_timeouts(self) -> None:
        failures = (
            ("service", github_http_error(503, **{"Retry-After": "4"}), 4),
            ("connection-reset", urllib.error.URLError(ConnectionResetError("reset")), 1),
            ("timeout", urllib.error.URLError(TimeoutError("timed out")), 1),
        )

        for label, failure, expected_delay in failures:
            with self.subTest(label=label):
                sleeps: list[float] = []
                client = self.recovery.PublicClient(
                    max_attempts=2,
                    retry_base_seconds=1,
                    sleep=sleeps.append,
                )
                with mock.patch.object(
                    self.recovery.urllib.request,
                    "urlopen",
                    side_effect=[failure, io.BytesIO(b"[]")],
                ) as open_url:
                    self.assertEqual(
                        [],
                        client.json(
                            "https://api.github.com/repos/durable-workflow/.github/releases?per_page=100"
                        ),
                    )

                self.assertEqual([expected_delay], sleeps)
                self.assertEqual(2, open_url.call_count)

    def test_authentication_is_terminal_even_with_rate_limit_guidance(self) -> None:
        sleeps: list[float] = []
        client = self.recovery.PublicClient(max_attempts=3, sleep=sleeps.append)
        error = github_http_error(
            401,
            b"Bad credentials: API rate limit exceeded",
            **{"Retry-After": "20", "X-RateLimit-Remaining": "0"},
        )

        with (
            mock.patch.object(self.recovery.urllib.request, "urlopen", side_effect=error) as open_url,
            self.assertRaisesRegex(self.recovery.RecoveryError, r"public request failed \(401\)"),
        ):
            client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100")

        self.assertEqual([], sleeps)
        self.assertEqual(1, open_url.call_count)

    def test_authorization_requires_explicit_rate_limit_guidance(self) -> None:
        client = self.recovery.PublicClient(
            max_attempts=2,
            sleep=lambda _delay: self.fail("ordinary authorization failure was retried"),
        )
        with (
            mock.patch.object(
                self.recovery.urllib.request,
                "urlopen",
                side_effect=github_http_error(403, b"Resource not accessible"),
            ) as open_url,
            self.assertRaisesRegex(self.recovery.RecoveryError, r"public request failed \(403\)"),
        ):
            client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100")
        self.assertEqual(1, open_url.call_count)

        sleeps: list[float] = []
        client = self.recovery.PublicClient(max_attempts=2, retry_base_seconds=1, sleep=sleeps.append)
        with mock.patch.object(
            self.recovery.urllib.request,
            "urlopen",
            side_effect=[
                github_http_error(
                    403,
                    b"API rate limit exceeded",
                    **{"X-RateLimit-Remaining": "0"},
                ),
                io.BytesIO(b"[]"),
            ],
        ) as open_url:
            self.assertEqual(
                [],
                client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100"),
            )
        self.assertEqual([1], sleeps)
        self.assertEqual(2, open_url.call_count)

    def test_retry_exhaustion_has_a_distinct_infrastructure_classification(self) -> None:
        client = self.recovery.PublicClient(max_attempts=2, retry_base_seconds=1, sleep=lambda _delay: None)
        with (
            mock.patch.object(
                self.recovery.urllib.request,
                "urlopen",
                side_effect=[github_http_error(503), github_http_error(502)],
            ) as open_url,
            self.assertRaisesRegex(
                self.recovery.PublicInfrastructureError,
                r"classification=github-read-transient, endpoint_class=releases-api, "
                r"attempts=2, reason=retry-exhausted, status=502",
            ),
        ):
            client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100")
        self.assertEqual(2, open_url.call_count)


class ReleasePreparationRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_module()

    def candidate(self) -> dict[str, object]:
        return {
            "plan": "missing-preparation",
            "channel": "alpha",
            "components": {"workflow": {"version": "2.0.0-alpha.1", "commit": "a" * 40}},
        }

    def test_discovery_rejects_missing_preparation_for_an_incomplete_release(self) -> None:
        candidate = self.candidate()
        tag = "release-plan/missing-preparation"
        record_commit = "b" * 40
        client = mock.Mock()
        client.json.return_value = {
            "tag_name": tag,
            "draft": False,
            "assets": [
                {
                    "name": "release-plan.json",
                    "browser_download_url": "https://example.invalid/release-plan.json",
                }
            ],
        }
        client.bytes.return_value = self.recovery.canonical_json(candidate)
        with (
            mock.patch.object(self.recovery, "validate_plan"),
            mock.patch.object(self.recovery, "resolve_tag", return_value=record_commit),
            mock.patch.object(
                self.recovery,
                "read_record",
                side_effect=[candidate, self.recovery.NotFound("missing preparation")],
            ),
            mock.patch.object(
                self.recovery,
                "verify_component",
                side_effect=self.recovery.NotFound("release is incomplete"),
            ),
            self.assertRaisesRegex(self.recovery.RecoveryError, "only completed legacy releases"),
        ):
            self.recovery.discover_plan(client, tag, "workflow")

    def test_missing_preparation_cannot_resolve_to_publish(self) -> None:
        candidate = self.candidate()
        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "resolve_tag", return_value=None),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "release preparation required before publishing workflow",
            ),
        ):
            self.recovery.resolve_component(
                mock.Mock(),
                "workflow",
                "release-plan/missing-preparation",
                "b" * 40,
                candidate,
                None,
            )

    def test_completed_legacy_release_still_resolves_to_skip(self) -> None:
        candidate = self.candidate()
        identity = candidate["components"]["workflow"]
        public_evidence = {"version": identity["version"], "commit": identity["commit"]}
        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "resolve_tag", return_value=identity["commit"]),
            mock.patch.object(self.recovery, "verify_component", return_value=public_evidence),
        ):
            state, outputs = self.recovery.resolve_component(
                mock.Mock(),
                "workflow",
                "release-plan/missing-preparation",
                "b" * 40,
                candidate,
                None,
            )

        self.assertEqual("skip", outputs["action"])
        self.assertEqual("complete", state["phase"])
        self.assertNotIn("release_preparation", state)


class RecoveryWorkflowSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_module()

    def assert_rejected(self, source: str) -> None:
        with self.assertRaises(self.recovery.RecoveryError) as caught:
            self.recovery.verify_recovery_workflow_source("sdk-rust", source)
        self.assertEqual(caught.exception.phase, "default-branch-preflight")

    def test_accepts_only_the_current_protected_rust_workflow_identity(self) -> None:
        digest = hashlib.sha256(CURRENT_RUST_RECOVERY_WORKFLOW.encode("utf-8")).hexdigest()
        self.assertEqual(digest, self.recovery.SDK_RUST_RELEASE_RECOVERY_SHA256)
        self.recovery.verify_recovery_workflow_source("sdk-rust", CURRENT_RUST_RECOVERY_WORKFLOW)
        self.recovery.verify_recovery_workflow_source(
            "sdk-rust",
            CURRENT_RUST_RECOVERY_WORKFLOW.replace("\n", "\r\n"),
        )

    def test_rejects_shell_semantic_bypasses_and_any_source_mutation(self) -> None:
        source = CURRENT_RUST_RECOVERY_WORKFLOW
        variants = {
            "one-byte mutation": source.replace("timeout-minutes: 30", "timeout-minutes: 31", 1),
            "one-line mutation": source + "\n",
            "readarray release tag mutation": source.replace(
                "          select_publication_run() {",
                "          readarray -t release_identity < <(printf '%s\\n' mutable)\n"
                '          RELEASE_TAG="${release_identity[0]}"\n\n'
                "          select_publication_run() {",
                1,
            ),
            "successful early exit": source.replace(
                "          python scripts/ci/publish-planned-tag.py \\",
                "          exit 0\n          python scripts/ci/publish-planned-tag.py \\",
                1,
            ),
            "shadowed gh command": source.replace(
                "          set -euo pipefail",
                "          set -euo pipefail\n          gh() { printf 'shadowed\\n'; }",
                1,
            ),
        }

        for label, variant in variants.items():
            with self.subTest(label):
                self.assertNotEqual(variant, source)
                self.assert_rejected(variant)

    def test_rejects_skipped_nonblocking_or_decoy_scoped_steps(self) -> None:
        source = CURRENT_RUST_RECOVERY_WORKFLOW
        tag_step = "      - name: Create or verify the exact planned source tag"
        publication_step = "      - name: Start or resume repository-owned publication"
        completion_step = "      - name: Verify crates.io source identity and the GitHub Release"
        exact_bindings = """          RELEASE_TAG: ${{ needs.discover.outputs.version }}
          RELEASE_COMMIT: ${{ needs.discover.outputs.commit }}"""
        decoy_step = f"""      - name: Unrelated release identity
        env:
{exact_bindings}
        run: echo "release identity is not consumed here"

"""
        mutable_tag_bindings = source.replace(
            exact_bindings,
            """          RELEASE_TAG: ${{ github.ref_name }}
          RELEASE_COMMIT: ${{ github.sha }}""",
            1,
        ).replace(tag_step, decoy_step + tag_step, 1)
        publication_env = """        env:
          GH_TOKEN: ${{ github.token }}
          PLAN_TAG: ${{ needs.discover.outputs.plan_tag }}
          RELEASE_TAG: ${{ needs.discover.outputs.version }}
          RELEASE_COMMIT: ${{ needs.discover.outputs.commit }}"""
        mutable_selector_bindings = source.replace(
            publication_env,
            """        env:
          GH_TOKEN: ${{ github.token }}
          PLAN_TAG: ${{ needs.discover.outputs.plan_tag }}
          RELEASE_TAG: ${{ github.ref_name }}
          RELEASE_COMMIT: ${{ github.sha }}""",
            1,
        ).replace(
            "      - name: Start or resume repository-owned publication",
            decoy_step + "      - name: Start or resume repository-owned publication",
            1,
        )
        variants = {
            "tag publication skipped": source.replace(
                tag_step,
                tag_step + "\n        if: ${{ false }}",
                1,
            ),
            "tag publication nonblocking even when false": source.replace(
                tag_step,
                tag_step + "\n        continue-on-error: false",
                1,
            ),
            "tag publication expression-enabled nonblocking": source.replace(
                tag_step,
                tag_step + "\n        continue-on-error: ${{ github.ref_name != '' }}",
                1,
            ),
            "tag publication uses a nonblocking shell": source.replace(
                tag_step,
                tag_step + "\n        shell: bash {0} || true",
                1,
            ),
            "publication selection skipped": source.replace(
                publication_step,
                publication_step + "\n        if: ${{ false }}",
                1,
            ),
            "completion verification skipped": source.replace(
                completion_step,
                completion_step + "\n        if: ${{ false }}",
                1,
            ),
            "completion verification nonblocking": source.replace(
                completion_step,
                completion_step + "\n        continue-on-error: true",
                1,
            ),
            "completion verification expression-enabled nonblocking": source.replace(
                completion_step,
                completion_step + "\n        continue-on-error: ${{ failure() }}",
                1,
            ),
            "tag bindings moved to an unrelated step": mutable_tag_bindings,
            "selector bindings moved to an unrelated step": mutable_selector_bindings,
            "checkout adds repository-token authority": source.replace(
                "          fetch-depth: 0",
                "          fetch-depth: 0\n          token: ${{ github.token }}",
                1,
            ),
            "run identity includes an unapproved field": source.replace(
                "databaseId,event,displayTitle,headBranch,headSha,status,conclusion",
                "databaseId,event,displayTitle,headBranch,headSha,status,conclusion,actor",
                1,
            ),
        }

        for label, variant in variants.items():
            with self.subTest(label):
                self.assertNotEqual(variant, source)
                self.assert_rejected(variant)

    def test_rejects_weakened_or_mismatched_rust_publication_shapes(self) -> None:
        source = CURRENT_RUST_RECOVERY_WORKFLOW
        publisher = r"""          python scripts/ci/publish-planned-tag.py \
            --tag "$RELEASE_TAG" --commit "$RELEASE_COMMIT" --plan-tag "$PLAN_TAG" \
            --evidence release-tag-publication-evidence.json"""
        deferred_publisher = source.replace(publisher, "          echo tag-publication-deferred", 1).replace(
            "      - name: Verify crates.io source identity and the GitHub Release",
            "      - name: Deferred source tag publication\n"
            "        run: |\n"
            f"{publisher}\n\n"
            "      - name: Verify crates.io source identity and the GitHub Release",
            1,
        )
        repository_token_creation = source.replace(
            "          python scripts/ci/publish-planned-tag.py",
            "          gh api --method POST \"repos/$GITHUB_REPOSITORY/git/refs\"\n"
            "          python scripts/ci/publish-planned-tag.py",
            1,
        )
        misplaced_deploy_key = source.replace(
            "          ssh-key: ${{ secrets.RELEASE_PLAN_DEPLOY_KEY }}",
            "          env:\n"
            "            UNUSED_DEPLOY_KEY: ${{ secrets.RELEASE_PLAN_DEPLOY_KEY }}",
            1,
        )
        dormant_publisher = source.replace(
            publisher,
            "          publish_planned_tag() {\n"
            + "\n".join(f"  {line}" for line in publisher.splitlines())
            + "\n          }",
            1,
        )
        reassigned_tag = source.replace(
            "          python scripts/ci/publish-planned-tag.py",
            '          RELEASE_TAG="$GITHUB_REF_NAME"\n'
            "          python scripts/ci/publish-planned-tag.py",
            1,
        )
        nonblocking_verification = source.replace(
            "--attempts 6 --sleep 10 --evidence release-completion-evidence.json",
            "--attempts 6 --sleep 10 --evidence release-completion-evidence.json || true",
            1,
        )
        variants = {
            "missing protected environment": source.replace(
                "environment: release-plan-publication", "environment: unprotected", 1
            ),
            "missing deploy key": source.replace(
                "secrets.RELEASE_PLAN_DEPLOY_KEY", "secrets.UNPROTECTED_KEY", 1
            ),
            "deploy key only in unrelated env": misplaced_deploy_key,
            "tag publisher defined but not executed": dormant_publisher,
            "release tag reassigned before publication": reassigned_tag,
            "public verification made nonblocking": nonblocking_verification,
            "tag publication after dispatch": deferred_publisher,
            "mutable tag publisher argument": source.replace(
                '--tag "$RELEASE_TAG"', '--tag "$GITHUB_REF_NAME"', 1
            ),
            "mismatched tag publisher commit": source.replace(
                '--commit "$RELEASE_COMMIT"', '--commit "$GITHUB_SHA"', 1
            ),
            "mutable planned tag binding": source.replace(
                "needs.discover.outputs.version", "github.ref_name"
            ),
            "mutable planned commit binding": source.replace(
                "needs.discover.outputs.commit", "github.sha"
            ),
            "different selected workflow": source.replace(
                "gh run list --workflow release.yml", "gh run list --workflow nightly.yml", 1
            ),
            "different dispatched workflow": source.replace(
                "gh workflow run release.yml", "gh workflow run nightly.yml", 1
            ),
            "incomplete run identity": source.replace(
                "headBranch,headSha,status", "headBranch,status", 1
            ),
            "mismatched selector tag": source.replace(
                '--release-tag "$RELEASE_TAG"', '--release-tag "$GITHUB_REF_NAME"', 1
            ),
            "mismatched selector commit": source.replace(
                '--release-commit "$RELEASE_COMMIT"', '--release-commit "$GITHUB_SHA"', 1
            ),
            "mismatched dispatch tag": source.replace(
                '-f release_tag="$RELEASE_TAG"', '-f release_tag="$GITHUB_REF_NAME"', 1
            ),
            "missing completed release verification": source.replace(
                "--component sdk-rust --plan recovery-input/release-plan.json",
                "--component sdk-rust --plan mutable-release-plan.json",
                1,
            ),
            "broad contents permission": source.replace("contents: read", "contents: write", 1),
            "repository token tag creation": repository_token_creation,
        }

        for label, variant in variants.items():
            with self.subTest(label):
                self.assertNotEqual(variant, source)
                self.assert_rejected(variant)

    def test_other_components_keep_the_contents_api_contract(self) -> None:
        self.recovery.verify_recovery_workflow_source("server", GENERIC_RECOVERY_WORKFLOW)

        protected_only = GENERIC_RECOVERY_WORKFLOW.replace(
            '-f ref="refs/tags/$RELEASE_TAG" -f sha="$RELEASE_COMMIT"',
            'python scripts/ci/publish-planned-tag.py --tag "$RELEASE_TAG" --commit "$RELEASE_COMMIT"',
        )
        with self.assertRaises(self.recovery.RecoveryError):
            self.recovery.verify_recovery_workflow_source("server", protected_only)

    def test_python_recovery_dispatches_publication_from_protected_main(self) -> None:
        recovery_source = RECOVERY_WORKFLOW.read_text()
        publish_source = PUBLISH_WORKFLOW.read_text()

        self.recovery.verify_recovery_workflow_source("sdk-python", recovery_source)
        self.assertIn("gh workflow run publish.yml --ref main", recovery_source)
        self.assertNotIn('gh workflow run publish.yml --ref "$RELEASE_TAG"', recovery_source)
        self.assertIn('-f release_tag="$RELEASE_TAG"', recovery_source)
        self.assertIn('-f release_commit="$RELEASE_COMMIT"', recovery_source)
        self.assertIn('--release-plan "$PLAN_TAG"', recovery_source)
        self.assertIn("&& 'Publish' || 'Build'", publish_source)
        self.assertIn("inputs.release_commit || github.sha", publish_source)
        self.assertIn("github.ref == 'refs/heads/main' && inputs.publish", publish_source)
        self.assertIn("format('refs/tags/{0}', inputs.release_tag)", publish_source)
        self.assertIn('if [ "$REQUESTED_TAG" != "$package_version" ]', publish_source)
        self.assertIn('tag_commit="$(git rev-list -n 1 "$REQUESTED_TAG")"', publish_source)
        self.assertIn('if [ "$tag_commit" != "$REQUESTED_COMMIT" ]', publish_source)

        invalid_recovery_sources = (
            recovery_source.replace("--ref main", '--ref "$RELEASE_TAG"', 1),
            recovery_source.replace('-f release_tag="$RELEASE_TAG"', '-f release_tag="$GITHUB_REF_NAME"', 1),
            recovery_source.replace('--release-plan "$PLAN_TAG"', '--release-plan "$RELEASE_TAG"', 1),
            recovery_source.replace('-f release_commit="$RELEASE_COMMIT"', '-f release_commit="$GITHUB_SHA"', 1),
            recovery_source.replace('-f publish=true', '-f publish=false', 1),
        )
        for invalid_source in invalid_recovery_sources:
            with self.assertRaises(self.recovery.RecoveryError):
                self.recovery.verify_recovery_workflow_source("sdk-python", invalid_source)


class PublicationRunSelectionTest(unittest.TestCase):
    RELEASE_TAG = "1.2.3"
    RELEASE_COMMIT = "1" * 40
    PLAN_TAG = "release-plan/continuity-plan-b"

    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_module()

    def run_metadata(
        self,
        run_id: int,
        *,
        status: str,
        conclusion: str | None,
        head_sha: str | None = None,
        plan: str | None = None,
        release_tag: str | None = None,
        release_commit: str | None = None,
        publish: bool = True,
        legacy_title: bool = False,
    ) -> dict[str, object]:
        exact_tag = release_tag or self.RELEASE_TAG
        exact_commit = release_commit or self.RELEASE_COMMIT
        exact_plan = plan or self.PLAN_TAG
        if legacy_title:
            display_title = f"Release {exact_tag} for {exact_plan}"
        else:
            action = "Publish" if publish else "Build"
            display_title = f"{action} {exact_tag}@{exact_commit} from {exact_plan}"
        return {
            "databaseId": run_id,
            "displayTitle": display_title,
            "headBranch": "main",
            "headSha": head_sha or "2" * 40,
            "status": status,
            "conclusion": conclusion,
        }

    def select(self, runs: list[dict[str, object]]) -> dict[str, object]:
        return self.recovery.select_publication_run(
            self.RELEASE_TAG,
            self.RELEASE_COMMIT,
            self.PLAN_TAG,
            runs,
        )

    def test_failed_plan_a_dispatches_current_plan_b_once(self) -> None:
        selection = self.select(
            [
                self.run_metadata(
                    1,
                    status="completed",
                    conclusion="failure",
                    plan="release-plan/continuity-plan-a",
                )
            ]
        )

        self.assertEqual(
            selection,
            {"action": "dispatch", "run_id": None, "status": None, "conclusion": None},
        )
        workflow = RECOVERY_WORKFLOW.read_text()
        self.assertEqual(workflow.count("gh workflow run publish.yml"), 1)
        self.assertIn("gh workflow run publish.yml --ref main", workflow)
        self.assertIn(
            "gh run list --workflow publish.yml --event workflow_dispatch --branch main",
            workflow,
        )
        self.assertIn('-f release_tag="$RELEASE_TAG" -f release_commit="$RELEASE_COMMIT"', workflow)
        self.assertIn('-f release_plan="$PLAN_TAG" -f publish=true', workflow)
        self.assertNotIn("gh run rerun", workflow)

    def test_active_exact_run_waits_and_successful_exact_run_completes(self) -> None:
        active = self.run_metadata(2, status="in_progress", conclusion=None)
        failed = self.run_metadata(1, status="completed", conclusion="failure")
        self.assertEqual(self.select([failed, active])["action"], "wait")

        successful = self.run_metadata(3, status="completed", conclusion="success")
        self.assertEqual(self.select([failed, successful])["action"], "complete")

    def test_successful_publish_false_run_does_not_complete_publication(self) -> None:
        build_only = self.run_metadata(4, status="completed", conclusion="success", publish=False)
        legacy_build_only = self.run_metadata(
            3,
            status="completed",
            conclusion="success",
            publish=False,
            legacy_title=True,
        )

        self.assertEqual(self.select([legacy_build_only, build_only])["action"], "dispatch")

    def test_non_main_or_different_release_identity_runs_are_ignored(self) -> None:
        tag_context = self.run_metadata(1, status="completed", conclusion="success")
        tag_context["headBranch"] = self.RELEASE_TAG
        different_tag = self.run_metadata(
            2,
            status="completed",
            conclusion="success",
            release_tag="9.9.9",
        )
        different_commit = self.run_metadata(
            3,
            status="completed",
            conclusion="success",
            release_commit="9" * 40,
        )
        different_plan = self.run_metadata(
            4,
            status="completed",
            conclusion="success",
            plan="release-plan/continuity-plan-a",
        )

        self.assertEqual(
            self.select([tag_context, different_tag, different_commit, different_plan])["action"],
            "dispatch",
        )

    def test_fresh_dispatch_keeps_partial_publication_idempotent(self) -> None:
        workflow = PUBLISH_WORKFLOW.read_text()

        self.assertIn("skip-existing: true", workflow)
        self.assertIn('if ! gh release view "$RELEASE_TAG"', workflow)


class PublishedReleaseRecoveryTest(unittest.TestCase):
    RELEASE_TAG = "0.4.101"
    RELEASE_COMMIT = "8aa0e86fe51edc1e7aba3d97ddf3dfda8009ee23"
    PLAN_TAG = "release-plan/alpha-continuity"

    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_module()

    def test_publication_authority_uses_exact_tag_distribution_and_release(self) -> None:
        client = mock.Mock(spec=self.recovery.PublicClient)
        identity = {"version": self.RELEASE_TAG, "commit": self.RELEASE_COMMIT}
        distribution_evidence = {"kind": "pypi", "source_files_compared": 12}
        release_evidence = {"tag_name": self.RELEASE_TAG}
        distribution_verifier = mock.Mock(return_value=distribution_evidence)

        with (
            mock.patch.object(self.recovery, "require_source_tag") as source_tag_verifier,
            mock.patch.object(
                self.recovery,
                "verify_github_release",
                return_value=release_evidence,
            ) as release_verifier,
            mock.patch.dict(self.recovery.VERIFIERS, {"pypi": distribution_verifier}),
        ):
            evidence = self.recovery.verify_component(client, "sdk-python", identity)

        source_tag_verifier.assert_called_once_with(client, "sdk-python", identity)
        distribution_verifier.assert_called_once_with(
            client,
            self.recovery.COMPONENTS["sdk-python"],
            self.RELEASE_TAG,
            self.RELEASE_COMMIT,
        )
        release_verifier.assert_called_once_with(client, "sdk-python", self.RELEASE_TAG)
        self.assertEqual(evidence["version"], self.RELEASE_TAG)
        self.assertEqual(evidence["commit"], self.RELEASE_COMMIT)
        self.assertEqual(evidence["distribution"], distribution_evidence)
        self.assertEqual(evidence["github_release"], release_evidence)

    def test_exact_public_release_completes_without_publication_dispatch(self) -> None:
        client = mock.Mock(spec=self.recovery.PublicClient)
        plan = {
            "plan": "alpha-continuity",
            "channel": "alpha",
            "components": {
                "server": {"version": "0.2.700", "commit": "2" * 40},
                "sdk-python": {"version": self.RELEASE_TAG, "commit": self.RELEASE_COMMIT},
            },
        }
        package_evidence = {
            "version": self.RELEASE_TAG,
            "commit": self.RELEASE_COMMIT,
            "distribution": {"kind": "pypi", "source_files_compared": 12},
            "github_release": {"tag_name": self.RELEASE_TAG},
        }
        preparation = {
            "components": {
                "sdk-python": {
                    "release_notes": {
                        "release_date": "2026-07-19",
                        "sha256": "a" * 64,
                        "source": {"kind": "changelog-unreleased"},
                    }
                }
            }
        }

        def verify_public_component(
            _client: object, component: str, _identity: dict[str, str]
        ) -> dict[str, object]:
            if component == "sdk-python":
                return package_evidence
            return {"version": plan["components"][component]["version"]}

        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "validate_release_preparation"),
            mock.patch.object(self.recovery, "verify_component", side_effect=verify_public_component),
            mock.patch.object(self.recovery, "resolve_tag", return_value=self.RELEASE_COMMIT),
            mock.patch.object(
                self.recovery,
                "require_python_source_manifest_version",
                return_value={"declared_version": self.RELEASE_TAG, "source_commit": self.RELEASE_COMMIT},
            ),
        ):
            state, outputs = self.recovery.resolve_component(
                client,
                "sdk-python",
                self.PLAN_TAG,
                "3" * 40,
                plan,
                preparation,
            )

        self.assertEqual(outputs["action"], "skip")
        self.assertEqual(state["phase"], "complete")
        self.assertEqual(state["outcome"], "verified")
        self.assertEqual(state["public_evidence"], package_evidence)

        workflow = RECOVERY_WORKFLOW.read_text()
        publication_step = workflow[workflow.index("Start or resume repository-owned publication") :]
        self.assertIn("if: steps.recovery.outputs.action == 'publish'", publication_step)


class PythonSourceManifestPreflightTest(unittest.TestCase):
    RELEASE_TAG = "0.4.100"
    RELEASE_COMMIT = "2018400368cf4251c58b24b3d53a99f0ca3512e3"
    PLAN_TAG = "release-plan/continuity-plan-b"

    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_module()

    def test_manifest_version_mismatch_hands_off_occupied_tag_before_publication(self) -> None:
        client = mock.Mock(spec=self.recovery.PublicClient)
        client.bytes.return_value = b'[project]\nname = "durable-workflow"\nversion = "0.4.99"\n'
        plan = {
            "plan": "continuity-plan-b",
            "channel": "alpha",
            "components": {
                "server": {"version": "0.2.666", "commit": "2" * 40},
                "sdk-python": {"version": self.RELEASE_TAG, "commit": self.RELEASE_COMMIT},
            },
        }
        publication_verifier = mock.Mock()
        preparation = {
            "components": {
                "sdk-python": {
                    "release_notes": {
                        "release_date": "2026-07-19",
                        "sha256": "a" * 64,
                        "source": {"kind": "changelog-unreleased"},
                    }
                }
            }
        }

        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "validate_release_preparation"),
            mock.patch.object(self.recovery, "verify_component", return_value={}),
            mock.patch.object(self.recovery, "resolve_tag", return_value=self.RELEASE_COMMIT),
            mock.patch.dict(self.recovery.VERIFIERS, {"pypi": publication_verifier}),
            self.assertRaises(self.recovery.RecoveryError) as caught,
        ):
            self.recovery.resolve_component(
                client,
                "sdk-python",
                self.PLAN_TAG,
                "3" * 40,
                plan,
                preparation,
            )

        error = caught.exception
        self.assertEqual(error.phase, "source-manifest-preflight")
        self.assertEqual(error.evidence["classification"], "source-manifest-version-conflict")
        self.assertEqual(error.evidence["source_manifest"]["declared_version"], "0.4.99")
        self.assertEqual(
            error.evidence["source_tag"],
            {"tag": self.RELEASE_TAG, "status": "present", "commit": self.RELEASE_COMMIT},
        )
        client.bytes.assert_called_once_with(
            f"https://api.github.com/repos/durable-workflow/sdk-python/contents/pyproject.toml"
            f"?ref={self.RELEASE_COMMIT}",
            accept="application/vnd.github.raw+json",
        )
        publication_verifier.assert_not_called()

        failure = self.recovery.resolution_failure_state(
            "sdk-python",
            self.PLAN_TAG,
            "3" * 40,
            plan,
            error,
        )
        self.assertEqual(failure["durable_evidence"]["failure"], error.evidence)
        self.assertIn("control-plane", failure["resume_action"])
        self.assertIn("successor allocation", failure["resume_action"])
        self.assertIn("immutable durable-workflow/sdk-python@0.4.100", failure["resume_action"])
        self.assertNotIn("Run durable-workflow/sdk-python Actions workflow", failure["resume_action"])


if __name__ == "__main__":
    unittest.main()
