# Changelog

All notable changes to the `durable-workflow` Python SDK are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Notes
- Production-readiness validation is in progress for the first
  `1.0.0` release candidate. The 0.4.x line is feature-complete for
  the documented Durable Workflow v2 surface and is being exercised
  end-to-end by external-user walkthroughs against the published
  Python SDK guide and the `examples/order_processing` Docker Compose
  sample.
- Remaining v2 follow-ups tracked for this line: server-routed Python
  query execution and pre-accept update validator routing. These are
  server-side capabilities; the SDK already records the receiver
  metadata required to participate when the server advertises support.

## [0.4.18] — 2026-05-08

### Added
- `durable-workflow-replay-verify` and `durable-workflow-history-bundle-verify`
  console scripts plus matching Python APIs (`verify_replay`,
  `verify_golden_history`, `simulate_bundles`,
  `verify_history_bundle`, `aggregate_verdicts`,
  `promotion_decision_for*`). These produce verdicts and
  `promotion_decision` values that match the platform replay
  contract, replay cross-runtime golden histories against registered
  workflow classes, and integrity-check exported history bundles for
  promotion gates.
- `InvocableActivityHandler`, `handle_invocable_http_request`,
  `handle_invocable_lambda_event`, and
  `lambda_invocable_activity_handler` for activity-grade external
  execution from HTTP servers and serverless runtimes. The carrier
  shares the external-task input/result envelope with first-party
  workers and rejects workflow-task inputs.
- `CONFORMANCE.md` — per-repo platform conformance claim listing the
  fixtures this SDK serves, the targets it claims (`official_sdk`,
  `worker_protocol_implementation`), and the release gate that blocks
  PyPI publication when conformance regresses.
- `docs/reference/invocable.md` covering the invocable adapter for
  the generated API reference site.

### Changed
- Worker registration enforces the `worker_protocol.version: "1.1"`
  feature floor advertised by `/api/cluster/info` and fails closed
  with a clear error when the server's advertised feature set is
  missing capabilities the SDK relies on.
- Workers and the serializer reject unsupported payload codecs at
  encode and decode time instead of silently passing the raw bytes
  through; misconfigured deployments surface a typed error instead
  of a downstream decode failure.
- Built-package smoke (`scripts/smoke-built-package.py`) now verifies
  installed wheel and source distribution metadata, the PEP 561
  `py.typed` marker, every name in `__all__`, that reference modules
  resolve from `site-packages` rather than the source checkout, and
  that the README quickstart still replays. The PyPI/TestPyPI publish
  workflow runs the smoke before uploading artifacts.

## [0.4.3] — 2026-04-24

### Added
- `Client.get_schedule_history(schedule_id, *, limit=None, after_sequence=None)`
  returns one `ScheduleHistoryPage` of the schedule's audit stream, and
  `Client.iter_schedule_history(...)` is an async iterator that walks every
  remaining `ScheduleHistoryEvent` with paging hidden. `ScheduleHandle`
  exposes the same surface as `handle.history(...)` and
  `handle.iter_history(...)`. History remains available for deleted
  schedules so post-mortem review still works after a schedule is
  removed.

## [0.4.2] — 2026-04-24

### Added
- `WorkflowEnvironment` now drives `continue_as_new` chains end-to-end.
  Each link's input, workflow type, history, and terminal command are
  exposed through the `runs` / `run_count` properties, signals can be
  queued for a specific link via `env.signal(..., run=N)`, and chains
  that switch workflow types use the new `env.register_workflow(cls)`
  registration. Chain length is bounded by `continue_as_new_limit`
  (default `50`); exceeding the limit raises `RuntimeError` so tests
  catch runaway continuations.

## [0.4.1] — 2026-04-23

### Changed
- `Client.set_namespace_external_storage` (and its sync facade) now takes the
  namespace as `name`, matching `describe_namespace`, `create_namespace`, and
  `update_namespace`. The 0.4.0 spelling `namespace=` is still accepted as a
  deprecated keyword alias that emits a `DeprecationWarning`; it will be
  removed in a future release. Passing both `name` and `namespace` raises
  `TypeError`.

## [0.4.0] — 2026-04-23

### Added
- Workflow control-plane parity across the async and sync clients for list,
  describe, cancel, terminate, history, history export, and run visibility,
  plus a public history replayer and released golden replay fixtures.
- Task-queue, worker, and namespace control-plane coverage for build-id rollout
  visibility, drain/resume mutation, worker build-id reporting, namespace
  controls, activity-task operations, schedule visibility/mutation, and search
  attribute management.
- External payload storage support for reference envelopes, object-store
  drivers, expiry metadata, retention/delete helpers, storage policy parity,
  and verified-byte caching.
- Bridge webhook client support, invocable activity carrier support, replay-safe
  UUIDv7 and patch-marker helpers, worker interceptors, payload codec batching,
  and explicit Avro payload adapters.

### Changed
- PyPI/TestPyPI publish builds now run the installed-package smoke before
  uploading artifacts, so release candidates verify the wheel and source
  distribution import from site-packages and replay the README quickstart.
- Polyglot parity coverage now spans CLI/Python shared control-plane fixtures,
  including workflow maintenance, task queues, storage drivers, and system
  maintenance endpoints, reducing drift between released SDK behavior and other
  Durable Workflow surfaces.

## [0.3.1] — 2026-04-21

### Changed
- **Breaking (pre-1.0):** `WorkflowCancelled` and `ActivityCancelled` now inherit
  from `BaseException` (not `DurableWorkflowError` / `Exception`), so a generic
  `except Exception:` block in activity code or result handlers no longer
  silently swallows cancellation. Callers that relied on catching cancellation
  via `except Exception:` or `except DurableWorkflowError:` must now either
  catch the class by name (e.g. `except (ActivityCancelled, WorkflowCancelled):`)
  or catch `BaseException`. Mirrors the standard-library precedent set by
  `asyncio.CancelledError` and `KeyboardInterrupt`.

## [0.3.0] — 2026-04-19

### Added
- Plane-scoped SDK bearer tokens: `Client(..., control_token=..., worker_token=...)`
  and the sync wrapper now support least-privilege server deployments where
  operator/admin credentials are separate from worker credentials. The existing
  `token=` argument remains the shared fallback.
- `Worker.run_until(workflow_id=..., timeout=...)` for examples, smoke tests,
  and single-workflow scripts that need to run a worker until one workflow
  reaches a terminal state.
- A Docker Compose order-processing example under `examples/order_processing`
  that starts a local server and runs a multi-activity Python workflow
  end-to-end.
- `ctx.wait_condition(...)` durable primitive with replayer support, for
  workflows that pause until a signal- or update-driven predicate holds.
- `@workflow.signal`, `@workflow.query`, and `@workflow.update` decorators
  with in-workflow dispatch: signals apply during replay, queries execute
  against a replayed workflow instance, and updates run on a worker with
  acceptance + application recorded in history.
- `ctx.sleep(seconds)` sugar over `StartTimer` for readability.
- In-process `WorkflowEnvironment` testing harness that boots a worker
  and client against a fake server for unit-style tests without Docker.
- Activity retry policy support: `ActivityRetryPolicy(...)` on
  `ctx.schedule_activity(...)` serializes retry bounds onto the
  server-side command.
- SDK metrics hooks (`MetricsRecorder` / `PrometheusMetricsRecorder`)
  for worker-side operational telemetry.

### Changed
- Worker compatibility checks now use `/api/cluster/info` protocol manifests
  as the authority instead of the top-level server app version. SDK 0.3.x
  requires `control_plane.version: "2"`,
  `control_plane.request_contract` schema
  `durable-workflow.v2.control-plane-request.contract` version `1`, and
  `worker_protocol.version: "1.0"`. Missing, unknown, or undiscoverable
  compatibility states fail closed.
- `Client.get_result()` now decodes `WorkflowCompleted` output with the event
  or workflow payload codec instead of assuming JSON.
- History-event decoding in `client.py` and `workflow.py` now requires the
  server's canonical PascalCase `event_type` values (`WorkflowCompleted`,
  `ActivityCompleted`, `TimerFired`, etc.). The prior snake_case fallback
  and the `output`-or-`result` key fallback on `WorkflowCompleted` have
  been removed; unknown event-type shapes are ignored instead of silently
  tolerated. (#432)

## [0.2.0] — 2026-04-17

### Added
- Runtime server version compatibility check at worker registration. On
  `Worker.run()`, the SDK now calls `/api/cluster/info` and refuses to
  register against a server whose major version falls outside the set the
  SDK knows how to talk to. This prevents a 0.2.x worker from silently
  attempting to drive a future breaking-release server. (#302)
- `Client.get_cluster_info()` — fetches the server version and declared
  capability manifest from `/api/cluster/info`.
- Avro payload codec support as a core runtime dependency.
  `serializer.encode()`, `serializer.decode()`, and
  `serializer.envelope()` now accept a `codec=` argument, and
  `decode_envelope()` honors the inner codec tag. The Worker decodes
  Avro-coded activity arguments and echoes the inbound codec on its
  `complete_activity_task` result. Wire format is the Durable Workflow
  generic-wrapper (base64 of `0x00` + Avro binary of a `{json: string,
  version: int}` record), byte-compatible with the PHP
  `Workflow\Serializers\Avro` serializer. (#362)

### Changed
- Avro is now the default codec for new payloads produced by the client,
  serializer helpers, schedules, workflow commands, and activity results.
  JSON payloads remain supported for compatibility with existing history.
- Replayed activity results now decode using the event payload codec.

## [0.1.0] — 2026-04-12

Initial PyPI release. HTTP+JSON worker and client for the Durable
Workflow server, covering workflow authoring, activity execution, signal
and update commands, and the worker protocol over long-poll HTTP.
