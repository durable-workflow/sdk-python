# Changelog

All notable changes to the `durable-workflow` Python SDK are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The Python SDK advances to prerelease `2.0.0-rc.35`. PyPI displays the normalized
PEP 440 identity `2.0.0rc35`. This Avro-only SDK release is qualified with Server
`2.0.0-rc.47`; JSON remains HTTP transport and is no longer a workflow payload
codec.
Earlier SDK versions remain historical releases and are not alternate supported
2.0 baselines; no prerelease compatibility shim is provided.

### Added
- `WorkflowContext.upsert_memo()` now authors validated memo patches, replays
  `MemoUpserted` identities, carries opaque payload envelopes, and fails before
  completion when runtime capability discovery does not advertise memo updates.
- List-yield parallel composition now accepts nested activity, child-workflow,
  and timer groups, schedules every durable leaf through the existing command
  protocol, restores the nested input shape on replay, and keeps stable full
  group paths across restart and duplicate terminal delivery.
- `WorkflowContext.saga()` provides deterministic reverse-order activity
  compensation for failures and cooperative cancellation.
  `SagaCompensationFailed` preserves both the initiating and compensation
  failures as structured diagnostics.
- Clients and workers now externalize large Avro payloads through the
  authenticated namespace runtime using only the runtime URL, namespace, and
  role credential. Opaque references are resolved before decode with bounded
  caching, size/SHA-256 verification, typed retryable and terminal failures,
  and no SDK delete authority over runtime-owned objects. Direct local, S3,
  GCS, and Azure adapters remain explicit self-hosted integrations and are
  never inferred from the runtime's backing driver.
- List-based activity, child-workflow, and timer fan-out now emits the stable
  language-neutral parallel-group identity and path metadata consumed by
  service-mode and embedded replay implementations.
- Declared update validators now run through durable, synchronous pre-accept
  worker tasks. Capability discovery fails closed, validation replay does not
  invoke the handler or commit workflow state, and typed failures preserve the
  Server's retryability and fencing outcomes.
- The optimized `fastavro` payload path now uses the fixed recursive
  `durable_workflow.protocol.Value` schema with explicit named branches and
  Avro single-object framing. Cross-language fixtures cover every branch,
  schema evolution, nested values, and stable policy failures.
- A repeatable benchmark reports JSON, legacy-wrapper, and fixed-schema raw and
  HTTP-envelope sizes plus adapter/encode/decode latency over the shared
  checked-in corpus, with an enforced production-path regression budget
  calibrated with explicit headroom.
- Schedule listing now exposes server-side status, workflow-type, visibility-
  query, page-size, and continuation-token filters on async and sync clients.
  `ScheduleListError` preserves typed filter and cursor refusal evidence.
- `durable-workflow-python-conformance --compose` now accepts nested host
  runner evidence tables, resolved artifact/source aliases, boolean `passed`
  result cells, nested protocol trace planes, and no-PHP audit check aliases
  when composing full published-artifact Python conformance results.
- `durable-workflow-python-conformance --compose` now normalizes runbook-style
  scenario and capability identifiers such as `server-up` and
  `result-returned`, plus common status, trace-plane, and no-PHP audit aliases,
  before evaluating host evidence.
- `durable-workflow-python-conformance --compose` now accepts runner-native
  host evidence aliases such as `scenarioEvidence`, `capabilityResults`,
  `officialCli`, `firstUserFlow`, `traces`, and `languageNeutralityAudit`,
  so full published-artifact runs are not marked uncovered just because the
  runner records observations under natural collection names.
- `durable-workflow-python-conformance` now exposes a host-evidence
  composition contract and `--compose` mode so published-artifact runners can
  turn raw observations into a complete Python parity result document before
  evaluation.
- `durable-workflow-python-conformance` now exposes the Python SDK
  published-artifact parity contract and evaluates host result documents,
  rejecting smoke-only evidence unless the official CLI path, cold first-user
  setup, protocol traces, no-PHP-assumption audit, concrete artifact versions,
  and complete capability table are all present.
- `Client.delete_namespace()` now exercises the namespace lifecycle cleanup
  control-plane surface and returns the server's per-table cleanup counts on
  `NamespaceDescription.deleted`.

### Fixed
- Completed parallel activity, child-workflow, timer, and mixed groups now
  replay results and select failures by their durable yielded positions rather
  than terminal-event recording order. Chronological signals, updates, and
  condition-wait interactions retain their history order.
- Worker workflow, activity, query, update, and update-validation tasks now
  reject every missing, null, or otherwise non-exact root payload codec before
  decoding envelopes or invoking application code. Customer-owned Avro values
  and metadata remain free to use `codec` and `payload_codec` keys.
- Explicit JSON and unknown payload-codec tags now fail closed before null,
  absent, empty-argument, or already-decoded replay shortcuts. Untagged
  no-payload sentinels and Avro-encoded null values retain their intended
  behavior.
- The Python API reference now describes query decorators and query-state
  replay as parts of the shipped server-routed worker query path while keeping
  its Server capability negotiation requirements explicit.
- Workflow and update-validation discovery now use one Server-multiplexed long
  poll per workflow reservation. A worker with one workflow slot can discover
  either task kind promptly without leasing excess replay work, increasing idle
  request load, or hiding poll reservations from heartbeat slot reporting.
- Package publication now treats exact-version PyPI JSON as the authoritative
  integrity surface. The rendered project-page audit remains visible evidence,
  but a client challenge or presentation mismatch no longer blocks creation of
  the matching source prerelease after immutable artifacts have been verified.
- PyPI qualification now keeps exact-version metadata and exact and documented
  prerelease installs release-blocking without requiring the project root or a
  bare install to select a release candidate over historical final releases.
  Stable 2.0 qualification retains strict root and default-install checks while
  preserving non-yanked 0.x history.
- Package metadata now identifies the Python SDK as a release candidate in the
  Durable Workflow 2.0 train and describes the shipped worker-routed query
  path without overstating pre-accept update-validator support.
- Query calls and update wait stages now fail before dispatch with typed,
  actionable errors when Server discovery is missing, unavailable, or
  explicitly lacks the requested capability.
- Worker deregistration treats an already-absent registration as an idempotent
  success, allowing cleanup-first published-artifact conformance runs to
  continue without hiding authorization or other deregistration failures.
- API-reference deployment now installs its exact rendered command from public
  PyPI before handing a candidate to GitHub Pages. Package publication invokes
  the same fail-safe deployment after the release is installable, while a
  delayed or failed publication leaves the prior reference live.
- The published API reference now renders its exact prerelease installation
  command and qualified Server pairing from the package release manifest.
- Worker shutdown now quiesces inline `run_until()` work and fully stops the
  query-poller thread before deregistration. A worker that cannot stop every
  execution path by its shutdown deadline reports a failure and keeps its
  registration instead of allowing stale completions after lease recovery.
- Workers configured with only `worker_token` can now authenticate cluster
  discovery and complete registration, polling, heartbeat, and graceful
  deregistration without receiving a control credential. Actual worker and
  control operations remain fail-closed across credential roles.
- Successful worker registrations are now removed through the worker protocol
  after pollers and in-flight tasks drain. Shutdown is idempotent, surfaces
  authentication and protocol failures, and retains cleanup context without
  replacing a primary worker-loop failure.
- Scoped worker and control tokens are no longer substituted across protocol
  planes. Missing role-appropriate credentials fail before transport, while the
  shared token continues to authorize both planes.
- The MkDocs search overlay no longer widens the document beyond responsive
  viewports, keeping search and navigation usable without horizontal panning.
- Client construction now rejects base URLs with a terminal SDK-owned `/api`
  suffix, query, or fragment while preserving self-hosted and managed-runtime
  path prefixes. The quickstart also uses unique durable workflow identities
  and documents typed duplicate-start handling.
- Current condition-timeout history uses explicit timer and condition identity
  without adjacency inference, while the metadata-poor historical shape keeps a
  narrowly bounded replay fallback. Interleaved updates and signals no longer
  shift the ordinary activity cursor.
- Worker registration now advertises each workflow's declared signal, query,
  and update contract with typed parameter metadata, allowing the server to
  admit declared Python workflow updates while preserving typed refusals.
- Explicit release recovery rejects terminally superseded plans before and
  after publication preflight while keeping completed-plan verification
  idempotent.
- Synchronous clients now keep their async HTTP connection pool, returned
  handles, and cleanup on one client-owned event loop for the client's lifetime.
- Worker-routed queries now replay signal-woken condition waits that reopen the
  same logical wait until all recorded signals have been applied, so long-lived
  queryable workflows report the latest signal-mutated state.
- Python conformance CLI evidence now accepts terminal stdout/JSON captured on
  nested CLI command records while still rejecting generic scenario outputs as
  proof of the public CLI result path.
- Python conformance source-policy evidence now accepts the explicit
  `local_product_source_checkouts_used=false` published-artifact runner alias.
- `durable-workflow-python-conformance --compose` now accepts actual CLI
  terminal-result evidence from `workflow:start --wait`, `workflow:describe`,
  and `workflow:show-run --follow` instead of only generic result aliases.
- `durable-workflow-replay-conformance` now declares `outcome: pass` when
  every required Python replay shard scenario passes, so full replay evidence
  is no longer reported as non-passing.
- Worker query tasks now treat null or empty compact history payload fields as
  missing when a history export carries the durable activity result or signal
  arguments, so cold replay after worker restart does not answer queries from
  an unmodified workflow instance.
- Worker query tasks now replay from the bundled history export when the
  inline task history is empty or truncated, so cold query replay reconstructs
  activity-derived state instead of answering from a fresh workflow instance.
- Python parent workflows now decode successful child workflow completions from
  the server's documented `ChildRunCompleted.output` history payload, while
  still accepting the older `result` alias. This prevents completed child
  workflow returns from replaying as `None`.
- Condition-wait replay now binds signals that arrive during a leased
  workflow task to the next recorded wait when the server history records
  those signals before the task's `ConditionWaitOpened` row. This avoids
  applying rapid signal batches to the previous wait and prevents replay from
  completing with a later wait/timer history step left unconsumed.
- Condition-wait replay now prefers the recorded event-order wait binding over
  a signal's stored `workflow_sequence` when several signals arrive before a
  reopened wait. This handles server histories where later signals are accepted
  while the previous wait is still open but must replay against the next
  physical wait.
- Condition-wait replay now lets a true predicate finish the current wait
  before any following same-key wait is considered stale terminal history.
  This keeps query and signal replay aligned with histories that include
  unresolved `ConditionWaitOpened` plus condition-timeout `TimerScheduled`
  rows after a replayed false reopen, while preserving pending sequential
  same-key waits and resolved reopens that must remain replay history.
- Workflow workers now report unhandled workflow-task execution errors back to
  the server instead of leaving the leased task pending until the lease or CLI
  wait times out. This lets the server observe and retry or fail the task
  promptly when command serialization or interceptor code raises after a task
  has been claimed.
- Workflow-task completion now retries transient transport failures and server
  throttling/5xx rejections before preserving emitted commands or reporting a
  definite task failure, reducing stuck waiting runs when a signal-satisfied
  wait completes immediately after replay-driven query activity.
- Ambiguous workflow-task completion failures no longer get reported back as
  durable task failures after commands have been produced. Definite server
  rejections are still treated as failed workflow tasks even when the
  best-effort failure report cannot be sent, but transport/ownership ambiguity
  preserves the emitted commands so replay-driven signal completion is not
  converted into a stuck failed task.
- Repeated condition-wait openings for the same logical wait now replay through
  every matching signal before deciding whether the wait is still pending, so
  long-running signal/query workflows do not get stuck on the first signal.
- Signal and update receivers recorded while a condition wait is open now
  replay at that specific wait, so later signal-driven waits are not satisfied
  or consumed too early when no activity or timer result separates them.
- Signal and update receivers recorded after an activity result now replay after
  the workflow consumes that activity result, so receiver-mutated state is not
  overwritten by deterministic post-activity setup before a `wait_condition`.
- Python workflow replay now throws terminal `ActivityFailed` history events
  into the generator as a typed `ActivityFailed` exception, including the
  recorded activity and failure metadata. Activity-only saga workflows can
  catch the exception and schedule compensation instead of replaying the
  original activity command again.
- Worker heartbeat `process_metrics` now report instantaneous values
  instead of process-lifetime aggregates. `cpu_percent` is the share of
  wall time the worker spent on CPU during the interval since the
  previous heartbeat (previously the lifetime average, which converged
  to a fixed value within minutes and stopped tracking live load), and
  `memory_bytes` is the current resident set size sampled from
  `/proc/self/statm` on Linux (previously `ru_maxrss`, which is the
  process-lifetime high-water mark and never decreased after a startup
  spike). Platforms without `/proc` no longer report `memory_bytes`
  rather than reporting a misleading peak. The heartbeat protocol shape
  is unchanged; the server records whatever the worker sends, so the
  Worker Status surface starts showing accurate live numbers as soon as
  workers upgrade.

### Changed
- Release-plan recovery now consumes immutable, exact-version release-note
  preparation authority before publishing a newly recorded plan.
- `tests/test_client.py` now closes the `schedule.history` polyglot
  parity slice. `test_get_schedule_history_matches_polyglot_fixture`
  asserts the full decoded payload envelope per event (`id`,
  `recorded_at`, `payload`, plus the workflow attribution fields)
  so the Python parity check covers the same wire content the CLI
  parity check covers when it asserts the printed JSON envelope
  matches the shared fixture's `response_body`. A companion
  `test_iter_schedule_history_walks_polyglot_fixture` exercises
  `Client.iter_schedule_history` against the shared fixture to lock
  in the cursor-advance semantics across pages.

### Notes
- Stable 2.0 remains gated on published-artifact qualification. Server-routed
  Python queries and synchronous pre-accept update validation are implemented;
  validator-bearing workers fail closed when discovery cannot prove that the
  Server enforces the acceptance boundary.

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
