#!/usr/bin/env python3
"""Focused regressions for release recovery workflow source verification."""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from cli_release_verifier_contract import (  # noqa: F401 - imported for unittest discovery
    CliRecoveryWorkflowSourceTest,
    CliReleaseAuthorityTest,
)
from recovery_workflow_authority import (
    SCHEMA as AUTHORITY_SCHEMA,
)
from recovery_workflow_authority import (
    SOURCE_IDENTITY,
    authority_ref_url,
    authority_url,
    qualification_runs_url,
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


AUTHORITY_COMMIT = "a" * 40


def continuity_resolution_qualification() -> dict[str, object]:
    return {
        "repository": "durable-workflow/.github",
        "workflow": ".github/workflows/beta-candidate.yml",
        "event": "push",
        "head_branch": "main",
        "head_sha": "9" * 40,
        "run_id": 987,
        "run_attempt": 2,
        "status": "completed",
        "conclusion": "success",
    }


def continuity_resolution_qualification_run() -> dict[str, object]:
    qualification = continuity_resolution_qualification()
    return {
        "id": qualification["run_id"],
        "run_attempt": qualification["run_attempt"],
        "repository": {"full_name": "durable-workflow/.github"},
        "head_repository": {"full_name": "durable-workflow/.github"},
        "path": ".github/workflows/beta-candidate.yml@main",
        "event": qualification["event"],
        "head_branch": qualification["head_branch"],
        "head_sha": qualification["head_sha"],
        "status": qualification["status"],
        "conclusion": qualification["conclusion"],
    }


def lifecycle_plan(module, channel: str = "alpha") -> dict[str, object]:
    prerelease = "alpha" if channel == "alpha" else "beta"
    return {
        "schema": module.SCHEMA,
        "plan": "component-recovery",
        "channel": channel,
        "foundation": {"tag": module.FOUNDATION_TAG, "commit": module.FOUNDATION_COMMIT},
        "components": {
            name: {
                "version": (f"2.0.0-{prerelease}.{index + 1}" if name in {"workflow", "waterline"} else f"1.0.{index}"),
                "commit": f"{index + 1:040x}",
            }
            for index, name in enumerate(module.COMPONENTS)
        },
        "beta_authorization": (
            {"tag": "beta-authorization/component-recovery", "commit": "f" * 40} if channel == "beta" else None
        ),
    }


def supersession_record(module, failed, successor, failed_commit: str) -> dict[str, object]:
    identity = failed["components"]["workflow"]
    observed_commit = "e" * 40
    environment_url = (
        "https://github.com/durable-workflow/.github/deployments/activity_log?"
        "environments_filter=release-plan-supersession"
    )
    protection = {
        "custom_branch_policies": [{"id": 22, "name": "main"}],
        "deployment_branch_policy": {
            "custom_branch_policies": True,
            "protected_branches": False,
        },
        "environment_id": 11,
        "environment_url": environment_url,
        "required_reviewer_rule_ids": [33],
    }
    return {
        "schema": "durable-workflow.release-plan-failure/v1",
        "outcome": "terminal-failure",
        "failed_plan": {
            "tag": f"release-plan/{failed['plan']}",
            "commit": failed_commit,
            "sha256": module.manifest_digest(failed),
        },
        "conflicts": [
            {
                "component": "workflow",
                "version": identity["version"],
                "planned_commit": identity["commit"],
                "observed_commit": observed_commit,
                "reason": "published-version-source-conflict",
                "github_release": {
                    "id": 44,
                    "url": "https://github.com/durable-workflow/workflow/releases/44",
                },
                "distribution": {
                    "kind": "composer",
                    "source_reference": observed_commit,
                    "dist_reference": observed_commit,
                },
            }
        ],
        "successor_plan": {
            "tag": f"release-plan/{successor['plan']}",
            "sha256": module.manifest_digest(successor),
        },
        "authorization": {
            "actor": "release-operator",
            "environment": "release-plan-supersession",
            "environment_approval": {
                "comment": "approved",
                "environments": [
                    {
                        "html_url": environment_url,
                        "id": 11,
                        "name": "release-plan-supersession",
                        "node_id": "environment-node",
                        "url": (
                            "https://api.github.com/repos/durable-workflow/.github/"
                            "environments/release-plan-supersession"
                        ),
                    }
                ],
                "run_attempt": 1,
                "run_id": 456,
                "state": "approved",
                "user": {
                    "html_url": "https://github.com/release-reviewer",
                    "id": 55,
                    "login": "release-reviewer",
                    "node_id": "reviewer-node",
                    "url": "https://api.github.com/users/release-reviewer",
                },
            },
            "environment_protection": protection,
            "repository": "durable-workflow/.github",
            "run_attempt": 1,
            "run_id": 456,
            "run_url": "https://github.com/durable-workflow/.github/actions/runs/456",
            "workflow_commit": "f" * 40,
            "workflow_ref": (
                "durable-workflow/.github/.github/workflows/release-plan-supersession.yml@refs/heads/main"
            ),
        },
    }


def captured_github_authority(module, record: dict[str, object]) -> list[object]:
    authorization = record["authorization"]
    protection = authorization["environment_protection"]
    approval = authorization["environment_approval"]
    return [
        {
            "id": protection["environment_id"],
            "html_url": protection["environment_url"],
            "protection_rules": [
                {
                    "id": protection["required_reviewer_rule_ids"][0],
                    "type": "required_reviewers",
                    "reviewers": [
                        {
                            "type": "User",
                            "reviewer": {
                                **approval["user"],
                                "avatar_url": "https://avatars.githubusercontent.com/u/55?v=4",
                                "site_admin": False,
                                "type": "User",
                            },
                        }
                    ],
                }
            ],
            "deployment_branch_policy": protection["deployment_branch_policy"],
        },
        {
            "total_count": 1,
            "branch_policies": [
                {**protection["custom_branch_policies"][0], "type": "branch"}
            ],
        },
        {
            "actor": {"login": authorization["actor"]},
            "conclusion": "success",
            "event": "workflow_dispatch",
            "head_branch": "main",
            "head_sha": authorization["workflow_commit"],
            "html_url": authorization["run_url"],
            "id": authorization["run_id"],
            "path": f"{module.SUPERSESSION_WORKFLOW}@main",
            "repository": {"full_name": module.CONTROL_REPOSITORY},
            "run_attempt": authorization["run_attempt"],
            "status": "completed",
        },
        [
            {
                "comment": approval["comment"],
                "environments": [
                    {
                        **approval["environments"][0],
                        "can_admins_bypass": True,
                        "created_at": "2026-07-23T00:00:00Z",
                        "updated_at": "2026-07-23T00:00:00Z",
                    }
                ],
                "state": approval["state"],
                "user": {
                    **approval["user"],
                    "avatar_url": "https://avatars.githubusercontent.com/u/55?v=4",
                    "site_admin": False,
                    "type": "User",
                },
            }
        ],
    ]


def qualification_run(
    status: str = "completed",
    conclusion: str | None = "success",
    *,
    head_sha: str = AUTHORITY_COMMIT,
    head_branch: str = "main",
    path: str = ".github/workflows/beta-candidate.yml",
) -> dict[str, object]:
    return {
        "id": 81,
        "run_attempt": 2,
        "name": "Beta candidate",
        "workflow_id": 37,
        "path": path,
        "event": "push",
        "head_branch": head_branch,
        "head_sha": head_sha,
        "status": status,
        "conclusion": conclusion,
        "url": "https://api.github.com/repos/durable-workflow/.github/actions/runs/81",
        "html_url": "https://github.com/durable-workflow/.github/actions/runs/81",
    }


class QualifiedAuthorityConsumerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_for_retry_tests()

    def authority(self) -> dict[str, object]:
        return {
            "schema": AUTHORITY_SCHEMA,
            "source": SOURCE_IDENTITY,
            "workflows": {
                name: {
                    "repository": component.repository,
                    "ref": f"refs/heads/{component.default_branch}",
                    "path": ".github/workflows/release-plan-recovery.yml",
                    "state": "active",
                    "sha256": "b" * 64,
                }
                for name, component in self.recovery.COMPONENTS.items()
            },
        }

    def client(self, runs: list[dict[str, object]]):
        authority_raw = json.dumps(self.authority()).encode("utf-8")

        class Client:
            def __init__(self) -> None:
                self.requests: list[tuple[str, str]] = []

            def json(self, url: str) -> dict[str, object]:
                self.requests.append(("json", url))
                if url == authority_ref_url():
                    return {"sha": AUTHORITY_COMMIT}
                if url == qualification_runs_url(AUTHORITY_COMMIT):
                    return {"total_count": len(runs), "workflow_runs": runs}
                raise AssertionError(f"peer source was read before authority qualification: {url}")

            def bytes(self, url: str, *, accept: str | None = None) -> bytes:
                self.requests.append(("bytes", url))
                if url != authority_url(AUTHORITY_COMMIT):
                    raise AssertionError(f"peer source was read before authority qualification: {url}")
                return authority_raw

        return Client(), authority_raw

    def test_green_qualification_binds_manifest_bytes_and_revision(self) -> None:
        client, authority_raw = self.client([qualification_run()])
        workflows, source = self.recovery.load_recovery_workflow_authority(client)

        self.assertEqual(set(self.recovery.COMPONENTS), set(workflows))
        self.assertEqual(AUTHORITY_COMMIT, source["commit"])
        self.assertEqual(hashlib.sha256(authority_raw).hexdigest(), source["sha256"])
        self.assertEqual(AUTHORITY_COMMIT, source["qualification"]["head_sha"])
        self.assertEqual(".github/workflows/beta-candidate.yml", source["qualification"]["path"])
        self.assertEqual("main", source["qualification"]["head_branch"])
        self.assertEqual(
            [
                ("json", authority_ref_url()),
                ("json", qualification_runs_url(AUTHORITY_COMMIT)),
                ("bytes", authority_url(AUTHORITY_COMMIT)),
            ],
            client.requests,
        )

    def test_non_green_fails_before_authority_or_peer_source_reads(self) -> None:
        cases = (
            ("pending", [qualification_run("in_progress", None)], "pending"),
            ("failed", [qualification_run("completed", "failure")], "failed"),
            ("cancelled", [qualification_run("completed", "cancelled")], "cancelled"),
            ("absent", [], "absent"),
            ("revision-mismatch", [qualification_run(head_sha="c" * 40)], "another commit"),
            (
                "wrong-workflow",
                [qualification_run(path=".github/workflows/source-qualification.yml")],
                "absent",
            ),
            ("wrong-ref", [qualification_run(head_branch="v2")], "absent"),
            (
                "wrong-path-ref",
                [qualification_run(path=".github/workflows/beta-candidate.yml@v2")],
                "absent",
            ),
        )
        for label, runs, message in cases:
            with self.subTest(state=label):
                client, _authority_raw = self.client(runs)
                with self.assertRaisesRegex(self.recovery.RecoveryError, message):
                    self.recovery.load_recovery_workflow_authority(client)
                self.assertEqual(
                    [
                        ("json", authority_ref_url()),
                        ("json", qualification_runs_url(AUTHORITY_COMMIT)),
                    ],
                    client.requests,
                )


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

    def test_authenticated_requests_preserve_endpoint_api_versions(self) -> None:
        cases = (
            ({"X-GitHub-Api-Version": self.recovery.SUPERSESSION_API_VERSION}, self.recovery.SUPERSESSION_API_VERSION),
            ({}, "2022-11-28"),
        )
        for headers, expected_version in cases:
            with self.subTest(expected_version=expected_version):
                client = self.recovery.PublicClient(token="test-token")
                response = mock.Mock()
                with mock.patch.object(
                    self.recovery.urllib.request, "urlopen", return_value=response
                ) as open_url:
                    self.assertIs(
                        response,
                        client.request(
                            "https://api.github.com/repos/durable-workflow/.github/actions/runs/456",
                            headers=headers,
                        ),
                    )
                request = open_url.call_args.args[0]
                request_headers = {key.lower(): value for key, value in request.header_items()}
                self.assertEqual("Bearer test-token", request_headers["authorization"])
                self.assertEqual(expected_version, request_headers["x-github-api-version"])

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
                        client.json("https://api.github.com/repos/durable-workflow/.github/releases?per_page=100"),
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

    def test_download_rejects_coercible_content_digest_before_publication_read(self) -> None:
        client = self.recovery.PublicClient()
        with (
            mock.patch.object(client, "_run") as public_read,
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "invalid expected SHA-256",
            ),
        ):
            client.download(
                "https://example.invalid/artifact",
                Path("unused-artifact"),
                expected_sha256=int("8" * 64),
            )
        public_read.assert_not_called()


class ImmutablePlanDiscoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.recovery = load_recovery_for_retry_tests()

    def test_updated_older_release_cannot_override_newer_immutable_plan(self) -> None:
        older = lifecycle_plan(self.recovery)
        older["plan"] = "older-alpha"
        newer = lifecycle_plan(self.recovery, "beta")
        newer["plan"] = "newer-beta"
        tags = ["release-plan/older-alpha", "release-plan/newer-beta"]
        commits = {tags[0]: "a" * 40, tags[1]: "b" * 40}
        recorded = {
            "a" * 40: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
            "b" * 40: dt.datetime(2026, 7, 22, tzinfo=dt.UTC),
        }

        with (
            mock.patch.object(
                self.recovery,
                "list_release_plan_tags",
                # The older Release may now appear first, but Release order is not authority.
                return_value=tags,
            ),
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=lambda _client, _repository, tag: commits[tag],
            ),
            mock.patch.object(
                self.recovery,
                "read_plan_authority",
                side_effect=[(older, None), (newer, None), (older, None), (newer, None)],
            ),
            mock.patch.object(
                self.recovery,
                "direct_plan_lifecycle",
                side_effect=[
                    ("actionable", None),
                    ("completed", None),
                    ("actionable", None),
                    ("completed", None),
                ],
            ),
            mock.patch.object(
                self.recovery,
                "immutable_plan_recorded_at",
                side_effect=lambda _client, commit: recorded[commit],
            ),
            mock.patch.object(
                self.recovery,
                "accepted_continuity_supersession",
                return_value=None,
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "no public release plan is available",
            ),
        ):
            self.recovery.select_implicit_plan_authority(mock.Mock())

    def test_equal_versions_with_different_source_commits_are_conflicting(self) -> None:
        first = lifecycle_plan(self.recovery, "beta")
        first["plan"] = "first-beta-authority"
        second = json.loads(json.dumps(first))
        second["plan"] = "conflicting-beta-authority"
        second["components"]["workflow"]["commit"] = "f" * 40
        authorities = [
            {"tag": f"release-plan/{first['plan']}", "plan": first},
            {"tag": f"release-plan/{second['plan']}", "plan": second},
        ]

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "conflicting current product trains",
        ):
            self.recovery.current_product_train_authorities(authorities)

    def test_validated_source_manifest_supersession_selects_successor(self) -> None:
        predecessor = lifecycle_plan(self.recovery, "beta")
        predecessor["plan"] = "source-manifest-predecessor"
        successor = json.loads(json.dumps(predecessor))
        successor["plan"] = "source-manifest-successor"
        successor["components"]["workflow"]["commit"] = "f" * 40
        successor_tag = f"release-plan/{successor['plan']}"
        successor_authority = {
            "tag": successor_tag,
            "plan": successor,
            "lifecycle": "actionable",
            "successor": None,
        }
        authorities = [
            {
                "tag": f"release-plan/{predecessor['plan']}",
                "plan": predecessor,
                "lifecycle": "superseded",
                "successor": {
                    "tag": successor_tag,
                    "sha256": self.recovery.manifest_digest(successor),
                    "plan": successor,
                },
            },
            successor_authority,
        ]

        self.assertEqual(
            [successor_authority],
            self.recovery.current_product_train_authorities(authorities),
        )

    def test_scheduled_recovery_without_actionable_plan_is_a_truthful_no_op(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "release-recovery-evidence.json"
            github_output = root / "github-output"
            arguments = [
                "component-release-recovery.py",
                "resolve",
                "--component",
                "workflow",
                "--plan-output",
                str(root / "release-plan.json"),
                "--preparation-output",
                str(root / "release-preparation.json"),
                "--evidence",
                str(evidence),
                "--github-output",
                str(github_output),
                "--allow-empty",
            ]

            with (
                mock.patch.object(sys, "argv", arguments),
                mock.patch.object(
                    self.recovery,
                    "discover_plan",
                    side_effect=self.recovery.RecoveryError(
                        "no public release plan is available",
                        "plan-discovery",
                    ),
                ),
                mock.patch.object(self.recovery, "resolve_component") as recover_component,
            ):
                self.assertEqual(0, self.recovery.main())

            recover_component.assert_not_called()
            state = json.loads(evidence.read_text())
            self.assertEqual("plan-discovery", state["phase"])
            self.assertEqual("idle", state["outcome"])
            self.assertEqual("action=none\n", github_output.read_text())

    def test_explicit_completed_plan_is_not_recovered(self) -> None:
        candidate = lifecycle_plan(self.recovery, "beta")
        tag = f"release-plan/{candidate['plan']}"
        commit = "a" * 40
        authority = {
            "tag": tag,
            "commit": commit,
            "recorded_at": dt.datetime(2026, 7, 24, tzinfo=dt.UTC),
            "plan": candidate,
            "preparation": None,
            "lifecycle": "completed",
            "successor": None,
        }
        with (
            mock.patch.object(self.recovery, "classify_plan_authorities", return_value=[authority]),
            self.assertRaisesRegex(self.recovery.RecoveryError, "is completed and cannot be recovered"),
        ):
            self.recovery.select_explicit_plan_authority(mock.Mock(), tag, commit, candidate, None)

    def test_concurrent_terminal_supersession_retries_before_returning_action(self) -> None:
        older = lifecycle_plan(self.recovery)
        older["plan"] = "older-plan"
        successor = lifecycle_plan(self.recovery)
        successor["plan"] = "successor-plan"
        older_tag = "release-plan/older-plan"
        successor_tag = "release-plan/successor-plan"
        commits = {older_tag: "a" * 40, successor_tag: "b" * 40}
        plans = {older_tag: older, successor_tag: successor}
        recorded = {
            commits[older_tag]: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
            commits[successor_tag]: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
        }
        terminal_failure: dict[str, object] = {}
        registry_reads = 0

        def list_tags(_client: mock.Mock) -> list[str]:
            nonlocal registry_reads
            registry_reads += 1
            if registry_reads == 2:
                terminal_failure.update(
                    {"outcome": "terminal-failure", "successor": successor_tag}
                )
            return (
                [older_tag, successor_tag]
                if terminal_failure
                else [older_tag]
            )

        def lifecycle(
            _client: mock.Mock,
            tag: str,
            _commit: str,
            _plan: dict[str, object],
            _preparation: None,
        ) -> tuple[str, object | None]:
            if tag == older_tag and terminal_failure:
                return "superseded", {
                    "tag": successor_tag,
                    "sha256": self.recovery.manifest_digest(successor),
                    "plan": successor,
                }
            return "actionable", None

        with (
            mock.patch.object(
                self.recovery,
                "list_release_plan_tags",
                side_effect=list_tags,
            ),
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=lambda _client, _repository, tag: commits[tag],
            ),
            mock.patch.object(
                self.recovery,
                "read_plan_authority",
                side_effect=lambda _client, tag, _commit: (plans[tag], None),
            ),
            mock.patch.object(
                self.recovery,
                "direct_plan_lifecycle",
                side_effect=lifecycle,
            ),
            mock.patch.object(
                self.recovery,
                "immutable_plan_recorded_at",
                side_effect=lambda _client, commit: recorded[commit],
            ),
            mock.patch.object(
                self.recovery,
                "accepted_continuity_supersession",
                return_value=None,
            ),
        ):
            selected = self.recovery.select_implicit_plan_authority(mock.Mock())

        self.assertEqual(successor_tag, selected["tag"])
        self.assertEqual("actionable", selected["lifecycle"])
        self.assertEqual(4, registry_reads)

    def test_convergence_rechecks_nonselected_lifecycle_authority(self) -> None:
        older = {"tag": "release-plan/older", "lifecycle": "completed"}
        changed_older = {**older, "lifecycle": "superseded"}
        latest = {"tag": "release-plan/latest", "lifecycle": "actionable"}
        current_snapshot = [changed_older, latest]

        with mock.patch.object(
            self.recovery,
            "classify_implicit_plan_authority",
            side_effect=[
                (latest, [older, latest]),
                (latest, current_snapshot),
                (latest, current_snapshot),
                (latest, current_snapshot),
            ],
        ) as classify:
            selected = self.recovery.select_implicit_plan_authority(mock.Mock())

        self.assertEqual(4, classify.call_count)
        self.assertEqual(current_snapshot, selected["authority_snapshot"])

    def test_final_implicit_boundary_rejects_continuity_pause_activated_after_initial_read(
        self,
    ) -> None:
        candidate = lifecycle_plan(self.recovery)
        candidate_preparation = {
            "components": {
                "workflow": {
                    "release_notes": {
                        "release_date": "2026-07-23",
                        "sha256": "c" * 64,
                        "source": {},
                    }
                }
            }
        }
        component = self.recovery.COMPONENTS["workflow"]
        selected = {"tag": "release-plan/current", "lifecycle": "actionable"}
        authority = {"authority_snapshot": [selected]}
        continuity = mock.Mock(
            side_effect=[
                None,
                {
                    "accepted_tag": f"beta-continuity/{candidate['plan']}/accepted",
                    "accepted_commit": "b" * 40,
                    "resumed_tag": f"beta-continuity/{candidate['plan']}/resumed",
                },
            ]
        )
        publication_preflight = mock.Mock(
            side_effect=self.recovery.NotFound("not published")
        )

        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "validate_release_preparation"),
            mock.patch.object(self.recovery, "resolve_tag", return_value=None),
            mock.patch.object(
                self.recovery,
                "classify_implicit_plan_authority",
                return_value=(selected, [selected]),
            ),
            mock.patch.object(
                self.recovery,
                "scheduled_continuity_pause",
                continuity,
            ),
            mock.patch.dict(
                self.recovery.VERIFIERS,
                {component.distribution: publication_preflight},
            ),
        ):
            self.assertIsNone(continuity(mock.Mock(), candidate))
            with self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "continuity pause authority changed during component preflight",
            ):
                self.recovery.resolve_component(
                    mock.Mock(),
                    "workflow",
                    selected["tag"],
                    "a" * 40,
                    candidate,
                    candidate_preparation,
                    authority,
                )

        self.assertEqual(2, continuity.call_count)
        self.assertEqual(1, publication_preflight.call_count)

    def test_final_implicit_boundary_rejects_stale_publish_but_explicit_actionable_recovery_does_not(
        self,
    ) -> None:
        candidate = lifecycle_plan(self.recovery)
        candidate_preparation = {
            "components": {
                "workflow": {
                    "release_notes": {
                        "release_date": "2026-07-23",
                        "sha256": "c" * 64,
                        "source": {},
                    }
                }
            }
        }
        component = self.recovery.COMPONENTS["workflow"]
        publication_preflight = mock.Mock(
            side_effect=self.recovery.NotFound("not published")
        )
        implicit_authority = {
            "authority_snapshot": [
                {"tag": "release-plan/older", "lifecycle": "actionable"}
            ]
        }
        current_snapshot = [
            {"tag": "release-plan/older", "lifecycle": "superseded"},
            {"tag": "release-plan/successor", "lifecycle": "actionable"},
        ]
        explicit_authority = {
            "selection": "explicit",
            "tag": "release-plan/older",
            "commit": "a" * 40,
            "recorded_at": dt.datetime(2026, 7, 23, tzinfo=dt.UTC),
            "plan": candidate,
            "preparation": candidate_preparation,
            "lifecycle": "actionable",
            "successor": None,
        }
        current_explicit_authority = {
            key: value
            for key, value in explicit_authority.items()
            if key != "selection"
        }

        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "validate_release_preparation"),
            mock.patch.object(self.recovery, "resolve_tag", return_value=None),
            mock.patch.object(
                self.recovery,
                "classify_implicit_plan_authority",
                return_value=(current_snapshot[-1], current_snapshot),
            ) as classify,
            mock.patch.object(
                self.recovery,
                "classify_plan_authorities",
                return_value=[current_explicit_authority],
            ) as classify_explicit,
            mock.patch.dict(
                self.recovery.VERIFIERS,
                {component.distribution: publication_preflight},
            ),
        ):
            with self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "refusing a stale recovery action",
            ):
                self.recovery.resolve_component(
                    mock.Mock(),
                    "workflow",
                    "release-plan/older",
                    "a" * 40,
                    candidate,
                    candidate_preparation,
                    implicit_authority,
                )

            for lifecycle in ("actionable", "interrupted"):
                with self.subTest(explicit_lifecycle=lifecycle):
                    explicit_authority["lifecycle"] = lifecycle
                    current_explicit_authority["lifecycle"] = lifecycle
                    state, outputs = self.recovery.resolve_component(
                        mock.Mock(),
                        "workflow",
                        "release-plan/older",
                        "a" * 40,
                        candidate,
                        candidate_preparation,
                        explicit_authority,
                    )
                    self.assertEqual("publish", outputs["action"])
                    self.assertEqual("publication", state["phase"])

        self.assertEqual(1, classify.call_count)
        self.assertEqual(2, classify_explicit.call_count)
        self.assertEqual(3, publication_preflight.call_count)

    def test_interrupted_plan_rejects_multiple_continuity_successors(self) -> None:
        interrupted = lifecycle_plan(self.recovery)
        interrupted["plan"] = "interrupted-plan"
        first_successor = json.loads(json.dumps(interrupted))
        first_successor["plan"] = "first-successor"
        second_successor = json.loads(json.dumps(interrupted))
        second_successor["plan"] = "second-successor"
        tags = [
            f"release-plan/{interrupted['plan']}",
            f"release-plan/{first_successor['plan']}",
            f"release-plan/{second_successor['plan']}",
        ]
        commits = {
            tags[0]: "a" * 40,
            tags[1]: "b" * 40,
            tags[2]: "c" * 40,
        }
        recorded = {
            commits[tags[0]]: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
            commits[tags[1]]: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
            commits[tags[2]]: dt.datetime(2026, 7, 22, tzinfo=dt.UTC),
        }
        interruption_tag = f"{self.recovery.CONTINUITY_TAG_PREFIX}{interrupted['plan']}/interrupted"
        interruption_commit = "d" * 40
        interruption_evidence = {"phase": "interrupted"}
        superseded_interruption = {
            "tag": interruption_tag,
            "commit": interruption_commit,
            "evidence_sha256": self.recovery.manifest_digest(interruption_evidence),
            "plan_sha256": self.recovery.manifest_digest(interrupted),
            "reason": self.recovery.CONTINUITY_SUPERSESSION_REASON,
        }

        with (
            mock.patch.object(
                self.recovery,
                "list_release_plan_tags",
                return_value=tags,
            ),
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=lambda _client, _repository, tag: (
                    interruption_commit if tag == interruption_tag else commits[tag]
                ),
            ),
            mock.patch.object(
                self.recovery,
                "read_plan_authority",
                side_effect=[
                    (interrupted, None),
                    (first_successor, None),
                    (second_successor, None),
                ],
            ),
            mock.patch.object(
                self.recovery,
                "direct_plan_lifecycle",
                side_effect=[
                    ("interrupted", interruption_tag),
                    ("completed", None),
                    ("completed", None),
                ],
            ),
            mock.patch.object(
                self.recovery,
                "immutable_plan_recorded_at",
                side_effect=lambda _client, commit: recorded[commit],
            ),
            mock.patch.object(
                self.recovery,
                "accepted_continuity_supersession",
                side_effect=[
                    None,
                    superseded_interruption,
                    superseded_interruption,
                ],
            ),
            mock.patch.object(
                self.recovery,
                "list_continuity_resolution_tags",
                return_value=[],
            ),
            mock.patch.object(
                self.recovery,
                "read_record",
                return_value=interruption_evidence,
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "multiple continuity successors",
            ),
        ):
            self.recovery.select_implicit_plan_authority(mock.Mock())

    def test_continuity_successor_fork_accepts_exact_digest_bound_resolution(self) -> None:
        interrupted_plan = {"plan": "interrupted"}
        interrupted = {
            "tag": "release-plan/interrupted",
            "commit": "a" * 40,
            "plan": interrupted_plan,
        }
        interruption = {
            "tag": "beta-continuity/interrupted/interrupted",
            "commit": "b" * 40,
            "evidence_sha256": "c" * 64,
        }
        successors = []
        for index, name in enumerate(("first-successor", "second-successor"), start=1):
            successors.append(
                {
                    "tag": f"release-plan/{name}",
                    "supersession": {
                        **interruption,
                        "continuity_claim": {
                            "plan": {
                                "tag": f"release-plan/{name}",
                                "commit": str(index) * 40,
                                "sha256": str(index + 2) * 64,
                            },
                            "acceptance": {
                                "tag": f"beta-continuity/{name}/accepted",
                                "commit": str(index + 4) * 40,
                                "sha256": str(index + 6) * 64,
                            },
                        },
                    },
                }
            )
        claims = [successor["supersession"]["continuity_claim"] for successor in successors]
        resolution = {
            "schema": self.recovery.CONTINUITY_RESOLUTION_SCHEMA,
            "qualification": continuity_resolution_qualification(),
            "interruption": {
                "plan": {
                    "tag": interrupted["tag"],
                    "commit": interrupted["commit"],
                    "sha256": self.recovery.manifest_digest(interrupted_plan),
                },
                "evidence": {
                    "tag": interruption["tag"],
                    "commit": interruption["commit"],
                    "sha256": interruption["evidence_sha256"],
                },
            },
            "successor_claims": claims,
            "selected_successor": claims[1]["plan"],
        }
        resolution_tag = (
            f"{self.recovery.CONTINUITY_RESOLUTION_TAG_PREFIX}interrupted/"
            f"{self.recovery.manifest_digest(resolution)}"
        )
        client = mock.Mock()
        client.json.return_value = continuity_resolution_qualification_run()
        with (
            mock.patch.object(
                self.recovery,
                "list_continuity_resolution_tags",
                return_value=[resolution_tag],
            ),
            mock.patch.object(self.recovery, "resolve_tag", return_value="f" * 40),
            mock.patch.object(self.recovery, "read_record", return_value=resolution),
        ):
            selected = self.recovery.resolve_continuity_successor_fork(
                client,
                interrupted,
                successors,
            )
        self.assertEqual("release-plan/second-successor", selected)
        valid_run = continuity_resolution_qualification_run()
        failures = (
            (None, "qualification is absent"),
            ({**valid_run, "status": "in_progress", "conclusion": None}, "qualification is pending"),
            ({**valid_run, "conclusion": "failure"}, "qualification failed"),
            ({**valid_run, "conclusion": "cancelled"}, "qualification was cancelled"),
            ({**valid_run, "head_sha": "8" * 40}, "another source revision"),
            ({**valid_run, "path": ".github/workflows/untrusted.yml@main"}, "untrusted workflow"),
        )
        with (
            mock.patch.object(self.recovery, "list_continuity_resolution_tags", return_value=[resolution_tag]),
            mock.patch.object(self.recovery, "resolve_tag", return_value="f" * 40),
            mock.patch.object(self.recovery, "read_record", return_value=resolution),
        ):
            for run, message in failures:
                with self.subTest(qualification=message):
                    client.json.return_value = run
                    with self.assertRaisesRegex(self.recovery.RecoveryError, message):
                        self.recovery.resolve_continuity_successor_fork(client, interrupted, successors)

    def test_terminal_failure_successor_requires_exact_authorized_plan_identity(self) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        authorized_successor = json.loads(json.dumps(failed))
        authorized_successor["plan"] = "successor-plan"
        authorized_successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        recorded_successor = json.loads(json.dumps(authorized_successor))
        recorded_successor["components"]["workflow"]["commit"] = "e" * 40
        failed_tag = f"release-plan/{failed['plan']}"
        successor_tag = f"release-plan/{authorized_successor['plan']}"
        failed_commit = "a" * 40
        successor_commit = "b" * 40
        failure_commit = "c" * 40
        failure = supersession_record(
            self.recovery,
            failed,
            authorized_successor,
            failed_commit,
        )

        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=[None, failure_commit],
            ),
            mock.patch.object(
                self.recovery,
                "read_record",
                side_effect=[failure, authorized_successor],
            ),
            mock.patch.object(self.recovery, "revalidate_supersession_authority"),
        ):
            lifecycle, successor_identity = self.recovery.direct_plan_lifecycle(
                mock.Mock(),
                failed_tag,
                failed_commit,
                failed,
                None,
            )

        self.assertEqual("superseded", lifecycle)
        self.assertEqual(
            {
                "tag": successor_tag,
                "sha256": self.recovery.manifest_digest(authorized_successor),
                "plan": authorized_successor,
            },
            successor_identity,
        )

        commits = {failed_tag: failed_commit, successor_tag: successor_commit}
        recorded = {
            failed_commit: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
            successor_commit: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
        }
        with (
            mock.patch.object(
                self.recovery,
                "list_release_plan_tags",
                return_value=[failed_tag, successor_tag],
            ),
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=lambda _client, _repository, tag: commits[tag],
            ),
            mock.patch.object(
                self.recovery,
                "read_plan_authority",
                side_effect=[(failed, None), (recorded_successor, None)],
            ),
            mock.patch.object(
                self.recovery,
                "direct_plan_lifecycle",
                side_effect=[
                    (lifecycle, successor_identity),
                    ("completed", None),
                ],
            ),
            mock.patch.object(
                self.recovery,
                "immutable_plan_recorded_at",
                side_effect=lambda _client, commit: recorded[commit],
            ),
            mock.patch.object(
                self.recovery,
                "accepted_continuity_supersession",
                return_value=None,
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "conflicting successor identity",
            ),
        ):
            self.recovery.select_implicit_plan_authority(mock.Mock())

    def test_terminal_failure_normalizes_captured_github_approval_shape(self) -> None:
        failed = lifecycle_plan(self.recovery)
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        record = supersession_record(self.recovery, failed, successor, "a" * 40)
        client = mock.Mock()
        client.json.side_effect = captured_github_authority(self.recovery, record)

        self.recovery.revalidate_supersession_authority(record, client)

        self.assertEqual(4, client.json.call_count)
        mutations = (
            ("run", "id", 999),
            ("run", "run_attempt", 2),
            ("environment", "id", 999),
            ("history", "state", "rejected"),
            ("reviewer", "id", 999),
        )
        for target, field, value in mutations:
            with self.subTest(target=target, field=field):
                responses = captured_github_authority(self.recovery, record)
                if target == "run":
                    responses[2][field] = value
                elif target == "environment":
                    responses[0][field] = value
                elif target == "history":
                    responses[3][0][field] = value
                else:
                    responses[3][0]["user"][field] = value
                client = mock.Mock()
                client.json.side_effect = responses
                with self.assertRaises(self.recovery.RecoveryError):
                    self.recovery.revalidate_supersession_authority(record, client)

    def test_terminal_failure_rejects_approval_history_for_a_rerun_attempt(self) -> None:
        failed = lifecycle_plan(self.recovery)
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        record = supersession_record(self.recovery, failed, successor, "a" * 40)
        authorization = record["authorization"]
        authorization["run_attempt"] = 2
        authorization["environment_approval"]["run_attempt"] = 2
        client = mock.Mock()
        client.json.side_effect = captured_github_authority(self.recovery, record)

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "approval history cannot bind.*rerun attempt",
        ):
            self.recovery.revalidate_supersession_authority(record, client)

        self.assertEqual(3, client.json.call_count)

    def test_terminal_failure_rejects_approver_outside_current_policy(self) -> None:
        failed = lifecycle_plan(self.recovery)
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        record = supersession_record(self.recovery, failed, successor, "a" * 40)
        responses = captured_github_authority(self.recovery, record)
        responses[0]["protection_rules"][0]["reviewers"][0]["reviewer"].update(
            {
                "html_url": "https://github.com/different-reviewer",
                "id": 77,
                "login": "different-reviewer",
                "node_id": "different-reviewer-node",
                "url": "https://api.github.com/users/different-reviewer",
            }
        )
        client = mock.Mock()
        client.json.side_effect = responses

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "approving user is not authorized by the current reviewer policy",
        ):
            self.recovery.revalidate_supersession_authority(record, client)

        self.assertEqual(4, client.json.call_count)

    def test_terminal_failure_rejects_incomplete_lifecycle_authority(self) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        failed_tag = f"release-plan/{failed['plan']}"
        failed_commit = "a" * 40
        incomplete = {
            "schema": "durable-workflow.release-plan-failure/v1",
            "outcome": "terminal-failure",
            "failed_plan": {
                "tag": failed_tag,
                "commit": failed_commit,
                "sha256": self.recovery.manifest_digest(failed),
            },
            "successor_plan": {
                "tag": f"release-plan/{successor['plan']}",
                "sha256": self.recovery.manifest_digest(successor),
            },
        }

        with (
            mock.patch.object(
                self.recovery,
                "resolve_tag",
                side_effect=[None, "c" * 40],
            ),
            mock.patch.object(
                self.recovery,
                "read_record",
                side_effect=[incomplete, successor],
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "record keys must be exactly",
            ),
        ):
            self.recovery.direct_plan_lifecycle(
                mock.Mock(),
                failed_tag,
                failed_commit,
                failed,
                None,
            )

    def test_terminal_failure_rejects_boolean_approval_run_identity(self) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        failed_tag = f"release-plan/{failed['plan']}"
        failed_commit = "a" * 40

        for field in ("run_id", "run_attempt"):
            with self.subTest(field=field):
                failure = supersession_record(
                    self.recovery,
                    failed,
                    successor,
                    failed_commit,
                )
                authorization = failure["authorization"]
                approval = authorization["environment_approval"]
                authorization[field] = 1
                approval[field] = True
                if field == "run_id":
                    authorization["run_url"] = "https://github.com/durable-workflow/.github/actions/runs/1"

                with (
                    mock.patch.object(
                        self.recovery,
                        "resolve_tag",
                        side_effect=[None, "c" * 40],
                    ),
                    mock.patch.object(
                        self.recovery,
                        "read_record",
                        side_effect=[failure, successor],
                    ),
                    self.assertRaisesRegex(
                        self.recovery.RecoveryError,
                        "lacks an approved deployment bound to its workflow run",
                    ),
                ):
                    self.recovery.direct_plan_lifecycle(
                        mock.Mock(),
                        failed_tag,
                        failed_commit,
                        failed,
                        None,
                    )

    def test_terminal_failure_rejects_malformed_authorization_json_types(self) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        failed_commit = "a" * 40
        valid_failure = supersession_record(
            self.recovery,
            failed,
            successor,
            failed_commit,
        )
        valid_failure["authorization"]["run_id"] = 1
        valid_failure["authorization"]["run_url"] = "https://github.com/durable-workflow/.github/actions/runs/1"
        valid_failure["authorization"]["environment_approval"]["run_id"] = 1
        self.recovery.validate_supersession_record(
            valid_failure,
            failed,
            failed_commit,
            successor,
        )
        mutations = (
            (("authorization", "actor"), True),
            (("authorization", "workflow_commit"), int("1" * 40)),
            (("authorization", "environment_approval", "run_id"), True),
            (("authorization", "environment_approval", "run_attempt"), True),
            (
                (
                    "authorization",
                    "environment_protection",
                    "deployment_branch_policy",
                    "custom_branch_policies",
                ),
                1,
            ),
            (
                (
                    "authorization",
                    "environment_protection",
                    "deployment_branch_policy",
                    "protected_branches",
                ),
                0,
            ),
        )

        for path, value in mutations:
            with self.subTest(field=".".join(path)):
                malformed = json.loads(json.dumps(valid_failure))
                target = malformed
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value

                with self.assertRaises(self.recovery.RecoveryError):
                    self.recovery.validate_supersession_record(
                        malformed,
                        failed,
                        failed_commit,
                        successor,
                    )

    def test_terminal_failure_rejects_numeric_commit_references(self) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        failed_commit = "a" * 40
        failure = supersession_record(
            self.recovery,
            failed,
            successor,
            failed_commit,
        )
        numeric_commit = int("1" * 40)
        conflict = failure["conflicts"][0]
        conflict["observed_commit"] = numeric_commit
        conflict["distribution"]["source_reference"] = numeric_commit
        conflict["distribution"]["dist_reference"] = numeric_commit

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "does not prove a different public source identity",
        ):
            self.recovery.validate_supersession_record(
                failure,
                failed,
                failed_commit,
                successor,
            )

    def test_authority_records_reject_coercible_commit_digest_and_tag_object_types(self) -> None:
        beta_plan = lifecycle_plan(self.recovery, "beta")
        beta_plan["beta_authorization"]["commit"] = int("1" * 40)
        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "beta plans require an immutable beta authorization",
        ):
            self.recovery.validate_plan(beta_plan)

        plan = lifecycle_plan(self.recovery)
        identity = plan["components"]["sdk-python"]
        specification = self.recovery.SOURCE_MANIFESTS["sdk-python"]
        source_manifest = {
            "declared_version": identity["version"],
            "package": specification["package"],
            "path": specification["path"],
            "sha256": int("2" * 64),
            "source_commit": identity["commit"],
            "url": (
                "https://github.com/durable-workflow/sdk-python/blob/"
                f"{identity['commit']}/{specification['path']}"
            ),
        }
        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "invalid source-manifest evidence",
        ):
            self.recovery.validate_source_manifest_evidence(
                source_manifest,
                "sdk-python",
                identity,
                must_match_version=True,
            )

        github_release, distribution = self.recovery.publication_absence_locations(
            "sdk-python",
            identity["version"],
        )
        occupied_conflict = {
            "source_tag": {
                "commit": identity["commit"],
                "repository": "durable-workflow/sdk-python",
                "tag": identity["version"],
                "tag_object": int("3" * 40),
                "url": f"https://github.com/durable-workflow/sdk-python/tree/{identity['version']}",
            },
            "github_release": github_release,
            "distribution": distribution,
        }
        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "occupied planned source tag",
        ):
            self.recovery.validate_occupied_source_manifest_evidence(
                occupied_conflict,
                "sdk-python",
                identity,
            )

        client = mock.Mock()
        client.json.return_value = {
            "object": {
                "type": "commit",
                "sha": int("4" * 40),
            }
        }
        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "does not resolve to a commit",
        ):
            self.recovery.resolve_tag(client, "durable-workflow/sdk-python", identity["version"])

    def test_continuity_authority_rejects_coercible_commit_and_digest_types(self) -> None:
        plan = lifecycle_plan(self.recovery)
        tag = f"release-plan/{plan['plan']}"
        digest = self.recovery.manifest_digest(plan)
        superseded = {
            "commit": "a" * 40,
            "evidence_sha256": "b" * 64,
            "plan_sha256": "c" * 64,
            "reason": self.recovery.CONTINUITY_SUPERSESSION_REASON,
            "tag": f"{self.recovery.CONTINUITY_TAG_PREFIX}{plan['plan']}/interrupted",
        }
        authority = {"tag": tag, "plan": plan}

        for field, value in (
            ("commit", int("5" * 40)),
            ("evidence_sha256", int("6" * 64)),
            ("plan_sha256", int("7" * 64)),
        ):
            with self.subTest(field=field):
                malformed_superseded = dict(superseded)
                malformed_superseded[field] = value
                evidence = {
                    "schema": self.recovery.CONTINUITY_EVIDENCE_SCHEMA,
                    "phase": "accepted",
                    "outcome": "accepted",
                    "release_plan": {"tag": tag, "sha256": digest},
                    "candidate_identity": {
                        "components": plan["components"],
                        "plan_sha256": digest,
                    },
                    "superseded_interruption": malformed_superseded,
                }
                with (
                    mock.patch.object(self.recovery, "resolve_tag", return_value="d" * 40),
                    mock.patch.object(self.recovery, "read_record", side_effect=[evidence, plan]),
                    self.assertRaisesRegex(
                        self.recovery.RecoveryError,
                        "invalid superseded interruption identity",
                    ),
                ):
                    self.recovery.accepted_continuity_supersession(mock.Mock(), authority)

    def assert_explicit_terminal_recovery_rejected(
        self,
        *,
        requested_tag: str,
        plans: dict[str, dict[str, object]],
        commits: dict[str, str],
        recorded_at: dict[str, dt.datetime],
        references: dict[str, str],
        records: dict[tuple[str, str, str], dict[str, object]],
        github_responses: list[object],
    ) -> None:
        candidate = plans[requested_tag]
        preparation = {
            "components": {
                "sdk-python": {
                    "release_notes": {
                        "release_date": "2026-07-23",
                        "sha256": "c" * 64,
                        "source": {},
                    }
                }
            }
        }
        component = self.recovery.COMPONENTS["sdk-python"]
        publication_preflight = mock.Mock(side_effect=self.recovery.NotFound("Python package is absent"))
        client = mock.Mock()
        client.json.side_effect = [{"tag_name": requested_tag}, *github_responses]

        def resolve_reference(
            _client: mock.Mock,
            repository: str,
            tag: str,
        ) -> str | None:
            if repository == self.recovery.CONTROL_REPOSITORY:
                return references.get(tag)
            if repository == component.repository:
                self.assertEqual(candidate["components"]["sdk-python"]["version"], tag)
                return None
            raise AssertionError(f"unexpected tag lookup for {repository}@{tag}")

        def read_plan(
            _client: mock.Mock,
            tag: str,
            commit: str,
        ) -> tuple[dict[str, object], dict[str, object]]:
            self.assertEqual(commits[tag], commit)
            return plans[tag], preparation

        def read_lifecycle_record(
            _client: mock.Mock,
            tag: str,
            commit: str,
            filename: str,
        ) -> dict[str, object]:
            return records[(tag, commit, filename)]

        with tempfile.TemporaryDirectory(prefix="explicit-terminal-recovery-") as temporary:
            root = Path(temporary)
            plan_output = root / "release-plan.json"
            preparation_output = root / "release-preparation.json"
            evidence_output = root / "release-recovery-evidence.json"
            github_output = root / "github-output"
            argv = [
                "component-release-recovery.py",
                "resolve",
                "--component",
                "sdk-python",
                "--plan-tag",
                requested_tag,
                "--plan-output",
                str(plan_output),
                "--preparation-output",
                str(preparation_output),
                "--evidence",
                str(evidence_output),
                "--github-output",
                str(github_output),
            ]

            with (
                mock.patch.object(self.recovery, "PublicClient", return_value=client),
                mock.patch.object(self.recovery.sys, "argv", argv),
                mock.patch.object(
                    self.recovery,
                    "list_release_plan_tags",
                    return_value=list(plans),
                ),
                mock.patch.object(
                    self.recovery,
                    "resolve_tag",
                    side_effect=resolve_reference,
                ),
                mock.patch.object(
                    self.recovery,
                    "read_plan_authority",
                    side_effect=read_plan,
                ),
                mock.patch.object(
                    self.recovery,
                    "read_record",
                    side_effect=read_lifecycle_record,
                ),
                mock.patch.object(
                    self.recovery,
                    "immutable_plan_recorded_at",
                    side_effect=lambda _client, commit: recorded_at[commit],
                ),
                mock.patch.object(self.recovery, "validate_release_mirrors"),
                mock.patch.object(
                    self.recovery,
                    "verify_plan_authority",
                    return_value=({}, {}),
                ),
                mock.patch.object(self.recovery, "validate_release_preparation"),
                mock.patch.object(
                    self.recovery,
                    "verify_component",
                    return_value={"status": "present"},
                ),
                mock.patch.object(
                    self.recovery,
                    "require_python_source_manifest_version",
                    return_value=None,
                ),
                mock.patch.dict(
                    self.recovery.VERIFIERS,
                    {component.distribution: publication_preflight},
                ),
                mock.patch.object(self.recovery.sys, "stderr", io.StringIO()),
            ):
                exit_code = self.recovery.main()

            self.assertEqual(1, exit_code)
            self.assertFalse(plan_output.exists())
            self.assertFalse(preparation_output.exists())
            self.assertFalse(github_output.exists())
            failure = json.loads(evidence_output.read_bytes())
            self.assertEqual("plan-discovery", failure["phase"])
            self.assertEqual("failed", failure["outcome"])
            self.assertIn(
                "terminally superseded and cannot be recovered",
                failure["reason"],
            )

        publication_preflight.assert_not_called()

    def test_explicit_terminal_failure_with_absent_artifact_has_no_publish_handoff(
        self,
    ) -> None:
        failed = lifecycle_plan(self.recovery)
        failed["plan"] = "failed-plan"
        successor = json.loads(json.dumps(failed))
        successor["plan"] = "successor-plan"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        failed_tag = f"release-plan/{failed['plan']}"
        successor_tag = f"release-plan/{successor['plan']}"
        failed_commit = "a" * 40
        successor_commit = "b" * 40
        failure_commit = "c" * 40
        failure_tag = f"{self.recovery.FAILURE_TAG_PREFIX}{failed['plan']}"
        failure = supersession_record(
            self.recovery,
            failed,
            successor,
            failed_commit,
        )

        self.assert_explicit_terminal_recovery_rejected(
            requested_tag=failed_tag,
            plans={failed_tag: failed, successor_tag: successor},
            commits={
                failed_tag: failed_commit,
                successor_tag: successor_commit,
            },
            recorded_at={
                failed_commit: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
                successor_commit: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
            },
            references={
                failed_tag: failed_commit,
                successor_tag: successor_commit,
                failure_tag: failure_commit,
            },
            records={
                (
                    failure_tag,
                    failure_commit,
                    "release-plan-failure.json",
                ): failure,
                (
                    failure_tag,
                    failure_commit,
                    "successor-release-plan.json",
                ): successor,
            },
            github_responses=captured_github_authority(self.recovery, failure),
        )

    def test_explicit_continuity_supersession_with_absent_artifact_has_no_publish_handoff(
        self,
    ) -> None:
        interrupted = lifecycle_plan(self.recovery)
        interrupted["plan"] = "interrupted-plan"
        successor = json.loads(json.dumps(interrupted))
        successor["plan"] = "continuity-successor"
        successor["components"]["workflow"]["version"] = "2.0.0-alpha.2"
        interrupted_tag = f"release-plan/{interrupted['plan']}"
        successor_tag = f"release-plan/{successor['plan']}"
        interrupted_commit = "a" * 40
        successor_commit = "b" * 40
        interruption_tag = f"{self.recovery.CONTINUITY_TAG_PREFIX}{interrupted['plan']}/interrupted"
        interruption_commit = "c" * 40
        accepted_tag = f"{self.recovery.CONTINUITY_TAG_PREFIX}{successor['plan']}/accepted"
        accepted_commit = "d" * 40
        interrupted_digest = self.recovery.manifest_digest(interrupted)
        successor_digest = self.recovery.manifest_digest(successor)
        interruption_evidence = {
            "schema": self.recovery.CONTINUITY_EVIDENCE_SCHEMA,
            "phase": "interrupted",
            "outcome": "intentionally-interrupted",
            "release_plan": {
                "tag": interrupted_tag,
                "sha256": interrupted_digest,
            },
            "plan_record": {
                "tag": interrupted_tag,
                "commit": interrupted_commit,
                "sha256": interrupted_digest,
            },
        }
        accepted_evidence = {
            "schema": self.recovery.CONTINUITY_EVIDENCE_SCHEMA,
            "phase": "accepted",
            "outcome": "accepted",
            "release_plan": {
                "tag": successor_tag,
                "sha256": successor_digest,
            },
            "candidate_identity": {
                "components": successor["components"],
                "plan_sha256": successor_digest,
            },
            "superseded_interruption": {
                "tag": interruption_tag,
                "commit": interruption_commit,
                "evidence_sha256": self.recovery.manifest_digest(interruption_evidence),
                "plan_sha256": interrupted_digest,
                "reason": self.recovery.CONTINUITY_SUPERSESSION_REASON,
            },
        }

        self.assert_explicit_terminal_recovery_rejected(
            requested_tag=interrupted_tag,
            plans={
                interrupted_tag: interrupted,
                successor_tag: successor,
            },
            commits={
                interrupted_tag: interrupted_commit,
                successor_tag: successor_commit,
            },
            recorded_at={
                interrupted_commit: dt.datetime(2026, 7, 20, tzinfo=dt.UTC),
                successor_commit: dt.datetime(2026, 7, 21, tzinfo=dt.UTC),
            },
            references={
                interrupted_tag: interrupted_commit,
                successor_tag: successor_commit,
                interruption_tag: interruption_commit,
                accepted_tag: accepted_commit,
            },
            records={
                (
                    interruption_tag,
                    interruption_commit,
                    "continuity-evidence.json",
                ): interruption_evidence,
                (
                    interruption_tag,
                    interruption_commit,
                    "release-plan.json",
                ): interrupted,
                (
                    accepted_tag,
                    accepted_commit,
                    "continuity-evidence.json",
                ): accepted_evidence,
                (
                    accepted_tag,
                    accepted_commit,
                    "release-plan.json",
                ): successor,
            },
            github_responses=[],
        )

    def test_explicit_terminal_transition_during_preflight_cannot_publish(self) -> None:
        candidate = lifecycle_plan(self.recovery)
        preparation = {
            "components": {
                "workflow": {
                    "release_notes": {
                        "release_date": "2026-07-23",
                        "sha256": "c" * 64,
                        "source": {},
                    }
                }
            }
        }
        tag = f"release-plan/{candidate['plan']}"
        commit = "a" * 40
        component = self.recovery.COMPONENTS["workflow"]
        authority = {
            "selection": "explicit",
            "tag": tag,
            "commit": commit,
            "recorded_at": dt.datetime(2026, 7, 23, tzinfo=dt.UTC),
            "plan": candidate,
            "preparation": preparation,
            "lifecycle": "actionable",
            "successor": None,
        }
        superseded = {
            **authority,
            "lifecycle": "superseded",
            "successor": {
                "tag": "release-plan/successor",
                "sha256": "d" * 64,
                "plan": {"plan": "successor"},
            },
        }
        superseded.pop("selection")
        publication_preflight = mock.Mock(
            side_effect=self.recovery.NotFound("not published")
        )
        with (
            mock.patch.object(
                self.recovery, "verify_plan_authority", return_value=({}, {})
            ),
            mock.patch.object(self.recovery, "validate_release_preparation"),
            mock.patch.object(self.recovery, "resolve_tag", return_value=None),
            mock.patch.object(
                self.recovery, "classify_plan_authorities", return_value=[superseded]
            ),
            mock.patch.dict(
                self.recovery.VERIFIERS,
                {component.distribution: publication_preflight},
            ),
            self.assertRaisesRegex(
                self.recovery.RecoveryError,
                "became terminally superseded during component preflight",
            ),
        ):
            self.recovery.resolve_component(
                mock.Mock(),
                "workflow",
                tag,
                commit,
                candidate,
                preparation,
                authority,
            )
        self.assertEqual(1, publication_preflight.call_count)


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
                "select_explicit_plan_authority",
                return_value={"selection": "explicit"},
            ),
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

    def test_explicit_completed_release_still_resolves_to_skip(self) -> None:
        candidate = self.candidate()
        identity = candidate["components"]["workflow"]
        public_evidence = {"version": identity["version"], "commit": identity["commit"]}
        authority = {
            "selection": "explicit",
            "tag": "release-plan/missing-preparation",
            "commit": "b" * 40,
            "plan": candidate,
            "preparation": None,
            "lifecycle": "completed",
            "successor": None,
        }
        with (
            mock.patch.object(self.recovery, "verify_plan_authority", return_value=({}, {})),
            mock.patch.object(self.recovery, "resolve_tag", return_value=identity["commit"]),
            mock.patch.object(self.recovery, "verify_component", return_value=public_evidence),
            mock.patch.object(
                self.recovery,
                "classify_plan_authorities",
                return_value=[
                    {key: value for key, value in authority.items() if key != "selection"}
                ],
            ),
        ):
            state, outputs = self.recovery.resolve_component(
                mock.Mock(),
                "workflow",
                "release-plan/missing-preparation",
                "b" * 40,
                candidate,
                None,
                authority,
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
            self.recovery.verify_recovery_workflow_source(
                "sdk-rust",
                source,
                hashlib.sha256(CURRENT_RUST_RECOVERY_WORKFLOW.encode("utf-8")).hexdigest(),
            )
        self.assertEqual(caught.exception.phase, "default-branch-preflight")

    def test_accepts_only_the_current_protected_rust_workflow_identity(self) -> None:
        digest = hashlib.sha256(CURRENT_RUST_RECOVERY_WORKFLOW.encode("utf-8")).hexdigest()
        self.recovery.verify_recovery_workflow_source("sdk-rust", CURRENT_RUST_RECOVERY_WORKFLOW, digest)
        self.recovery.verify_recovery_workflow_source(
            "sdk-rust",
            CURRENT_RUST_RECOVERY_WORKFLOW.replace("\n", "\r\n"),
            digest,
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
            '          gh api --method POST "repos/$GITHUB_REPOSITORY/git/refs"\n'
            "          python scripts/ci/publish-planned-tag.py",
            1,
        )
        misplaced_deploy_key = source.replace(
            "          ssh-key: ${{ secrets.RELEASE_PLAN_DEPLOY_KEY }}",
            "          env:\n            UNUSED_DEPLOY_KEY: ${{ secrets.RELEASE_PLAN_DEPLOY_KEY }}",
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
            '          RELEASE_TAG="$GITHUB_REF_NAME"\n          python scripts/ci/publish-planned-tag.py',
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
            "missing deploy key": source.replace("secrets.RELEASE_PLAN_DEPLOY_KEY", "secrets.UNPROTECTED_KEY", 1),
            "deploy key only in unrelated env": misplaced_deploy_key,
            "tag publisher defined but not executed": dormant_publisher,
            "release tag reassigned before publication": reassigned_tag,
            "public verification made nonblocking": nonblocking_verification,
            "tag publication after dispatch": deferred_publisher,
            "mutable tag publisher argument": source.replace('--tag "$RELEASE_TAG"', '--tag "$GITHUB_REF_NAME"', 1),
            "mismatched tag publisher commit": source.replace(
                '--commit "$RELEASE_COMMIT"', '--commit "$GITHUB_SHA"', 1
            ),
            "mutable planned tag binding": source.replace("needs.discover.outputs.version", "github.ref_name"),
            "mutable planned commit binding": source.replace("needs.discover.outputs.commit", "github.sha"),
            "different selected workflow": source.replace(
                "gh run list --workflow release.yml", "gh run list --workflow nightly.yml", 1
            ),
            "different dispatched workflow": source.replace(
                "gh workflow run release.yml", "gh workflow run nightly.yml", 1
            ),
            "incomplete run identity": source.replace("headBranch,headSha,status", "headBranch,status", 1),
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
        expected_sha256 = hashlib.sha256(GENERIC_RECOVERY_WORKFLOW.encode("utf-8")).hexdigest()
        self.recovery.verify_recovery_workflow_source("server", GENERIC_RECOVERY_WORKFLOW, expected_sha256)

        protected_only = GENERIC_RECOVERY_WORKFLOW.replace(
            '-f ref="refs/tags/$RELEASE_TAG" -f sha="$RELEASE_COMMIT"',
            'python scripts/ci/publish-planned-tag.py --tag "$RELEASE_TAG" --commit "$RELEASE_COMMIT"',
        )
        with self.assertRaises(self.recovery.RecoveryError):
            self.recovery.verify_recovery_workflow_source("server", protected_only, expected_sha256)

    def test_python_recovery_dispatches_publication_from_protected_main(self) -> None:
        recovery_source = RECOVERY_WORKFLOW.read_text()
        publish_source = PUBLISH_WORKFLOW.read_text()
        expected_sha256 = hashlib.sha256(recovery_source.encode("utf-8")).hexdigest()
        discover_job = recovery_source[recovery_source.index("  discover:") : recovery_source.index("  publish:")]
        publication_job = recovery_source[recovery_source.index("  publish:") :]

        self.recovery.verify_recovery_workflow_source("sdk-python", recovery_source, expected_sha256)
        self.assertIn("contents: read", discover_job)
        self.assertNotIn("actions: write", discover_job)
        self.assertNotIn("contents: write", discover_job)
        self.assertNotIn("GH_TOKEN:", discover_job)
        self.assertNotIn("gh workflow run", discover_job)
        self.assertIn("needs: discover", publication_job)
        self.assertIn("needs.discover.outputs.action == 'publish'", publication_job)
        self.assertIn("actions: write", publication_job)
        self.assertIn("contents: write", publication_job)
        self.assertEqual(1, recovery_source.count("actions: write"))
        self.assertEqual(1, recovery_source.count("contents: write"))
        self.assertIn("gh workflow run publish.yml --ref main", publication_job)
        self.assertNotIn('gh workflow run publish.yml --ref "$RELEASE_TAG"', publication_job)
        self.assertIn('-f release_tag="$RELEASE_TAG"', publication_job)
        self.assertIn('-f release_commit="$RELEASE_COMMIT"', publication_job)
        self.assertIn('--release-plan "$PLAN_TAG"', publication_job)
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
            recovery_source.replace("-f publish=true", "-f publish=false", 1),
        )
        for invalid_source in invalid_recovery_sources:
            with self.assertRaises(self.recovery.RecoveryError):
                self.recovery.verify_recovery_workflow_source("sdk-python", invalid_source, expected_sha256)


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

    def test_publication_selection_rejects_boolean_run_identity(self) -> None:
        malformed = self.run_metadata(True, status="completed", conclusion="success")

        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "publication run metadata is incomplete",
        ):
            self.select([malformed])

    def test_publication_selection_rejects_non_string_commit_identity(self) -> None:
        with self.assertRaisesRegex(
            self.recovery.RecoveryError,
            "publication run selection requires an exact release identity",
        ):
            self.recovery.select_publication_run(
                self.RELEASE_TAG,
                int(self.RELEASE_COMMIT),
                self.PLAN_TAG,
                [],
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

        def verify_public_component(_client: object, component: str, _identity: dict[str, str]) -> dict[str, object]:
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
        publication_job = workflow[workflow.index("  publish:") :]
        self.assertIn("needs.discover.outputs.action == 'publish'", publication_job)


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
