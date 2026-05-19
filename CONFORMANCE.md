# Platform Conformance — Python SDK Claim

The `durable_workflow` Python SDK participates in the platform
conformance suite specified in
[`workflow/docs/architecture/platform-conformance-suite.md`](https://github.com/durable-workflow/workflow/blob/v2/docs/architecture/platform-conformance-suite.md)
and mirrored by `Workflow\V2\Support\PlatformConformanceSuite`. This
document is the per-repo claim: it lists the conformance targets the
SDK claims, the fixtures it serves, and the release gate that blocks
publication when conformance is broken.

## Claimed targets

The Python SDK claims two targets from the suite's matrix:

- `official_sdk` — first-party SDK distributed by the project. Implements
  `worker_protocol` (worker side), `history_event_wire_formats`
  (replay), and `official_sdks` (own row in the surface stability
  contract).
- `worker_protocol_implementation` — worker plane and frozen history
  event replay.

## Fixture sources served by this repo

| Category | Source path | Status |
| --- | --- | --- |
| `control_plane_request_response` | `tests/fixtures/control-plane/` | stable, parity-shared with `cli` |
| `signal_query_runtime_contract` | `tests/test_signals.py`, `tests/test_queries.py`, `tests/test_worker.py` | stable, parity-shared with PHP worker, CLI, and server routes |
| `worker_task_lifecycle` | `tests/fixtures/external-task-input/`, `tests/fixtures/external-task-result/` | stable |
| `history_replay_bundles` | `tests/fixtures/golden_history/` and the public replay scenario manifest at <https://durable-workflow.github.io/platform-conformance/replay-runtime-scenarios.json> | stable, parity-shared with `workflow` golden bundles and the full runtime replay scenario matrix |

The fixtures in this repo are exercised today by:

- `tests/test_control_plane_parity_fixtures.py`
- `tests/test_history_event_contract.py`
- `tests/test_signals.py`
- `tests/test_queries.py`
- `tests/test_worker.py`
- `scripts/check-cli-parity.py`
- the `cli-parity` job in `.github/workflows/ci.yml`

These are the per-repo gates that already enforce the contract; the
public conformance harness, when it lands, will read the same fixtures
from this repo's declared paths.

## Release gate

A release of `durable_workflow` (PyPI) must produce a passing harness
result document before tag, with the conformance level at `full` or
`provisional` (provisional categories enumerated in release notes).

| Field | Value |
| --- | --- |
| Required claimed targets | `official_sdk`, `worker_protocol_implementation` |
| Required suite version | `PlatformConformanceSuite::VERSION` (currently `3`, mirrored at `/platform-conformance-contract.json`) |
| CI job | `platform-conformance` (lands when the harness reference implementation publishes; until then `cli-parity` and `test_history_event_contract.py` cover the same ground) |
| Block on `nonconforming` | yes |
| Artifact attached to release | harness result document, schema `durable-workflow.v2.platform-conformance.result` |

A `nonconforming` result blocks the release. A failure in a provisional
category emits a warning and does not block.

## SDK neutrality posture

Python is the highest-value non-PHP first-party SDK and is treated as a
`priority` posture under the platform-wide SDK neutrality contract
(schema `durable-workflow.v2.sdk-neutrality.contract`, mirrored at
[`/sdk-neutrality-contract.json`](https://durable-workflow.github.io/sdk-neutrality-contract.json)
on the docs site). The contract's purpose is to keep public Durable
contracts implementable in any language without protocol redesign;
this SDK exists in part to validate that the worker protocol, control
plane, and replay fixtures behave the same way outside PHP.

Concretely, that means this SDK never persists Python-only state
across a public boundary: no `pickle`, no Python-only exception class
matching, no `__qualname__` leaking into the wire format. Failure
shapes use stable string codes; workflow, activity, child-workflow,
and exception types are identified by registered string names; replay
fixtures under `tests/fixtures/golden_history/` are JSON conforming to
the published `history_event_payloads` and `replay_bundle` JSON
Schemas. Any new test fixture or public helper added to this SDK that
would only round-trip through Python is a contract violation, even if
no test in this repo notices.

## Cross-references

- Authority spec: `workflow/docs/architecture/platform-conformance-suite.md`
- Authority manifest class: `Workflow\V2\Support\PlatformConformanceSuite`
  (PHP, mirrored in `static/platform-conformance-contract.json` on the
  docs site for language-neutral access)
- SDK neutrality authority:
  `workflow/docs/architecture/sdk-neutrality.md`, manifest class
  `Workflow\V2\Support\SdkNeutralityContract`, public docs page at
  <https://durable-workflow.github.io/docs/2.0/sdk-neutrality>
- Public conformance docs page:
  <https://durable-workflow.github.io/docs/2.0/platform-conformance>
- Public static suite manifest:
  <https://durable-workflow.github.io/platform-conformance-contract.json>
- Compatibility authority:
  <https://durable-workflow.github.io/docs/2.0/compatibility>
- Polyglot parity doc:
  <https://durable-workflow.github.io/docs/polyglot/cli-python-parity>
- Existing per-repo gates: `tests/test_control_plane_parity_fixtures.py`,
  `tests/test_history_event_contract.py`, `scripts/check-cli-parity.py`.
