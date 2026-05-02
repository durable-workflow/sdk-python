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
| `worker_task_lifecycle` | `tests/fixtures/external-task-input/`, `tests/fixtures/external-task-result/` | stable |
| `history_replay_bundles` | `tests/fixtures/golden_history/` | stable, parity-shared with `workflow` golden bundles |

The fixtures in this repo are exercised today by:

- `tests/test_control_plane_parity_fixtures.py`
- `tests/test_history_event_contract.py`
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
| Required suite version | `PlatformConformanceSuite::VERSION` (currently `1`) |
| CI job | `platform-conformance` (lands when the harness reference implementation publishes; until then `cli-parity` and `test_history_event_contract.py` cover the same ground) |
| Block on `nonconforming` | yes |
| Artifact attached to release | harness result document, schema `durable-workflow.v2.platform-conformance.result` |

A `nonconforming` result blocks the release. A failure in a provisional
category emits a warning and does not block.

## Cross-references

- Authority spec: `workflow/docs/architecture/platform-conformance-suite.md`
- Authority manifest class: `Workflow\V2\Support\PlatformConformanceSuite`
  (PHP, mirrored in `static/platform-conformance-contract.json` on the
  docs site for language-neutral access)
- Public docs page: <https://durable-workflow.github.io/docs/2.0/compatibility>
- Polyglot parity doc:
  <https://durable-workflow.github.io/docs/polyglot/cli-python-parity>
- Existing per-repo gates: `tests/test_control_plane_parity_fixtures.py`,
  `tests/test_history_event_contract.py`, `scripts/check-cli-parity.py`.
