# Changelog

All notable changes to the `durable-workflow` Python SDK are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Changed
- Worker compatibility checks now use `/api/cluster/info` protocol manifests
  as the authority instead of the top-level server app version. SDK 0.2.x
  requires `control_plane.version: "2"`,
  `control_plane.request_contract` schema
  `durable-workflow.v2.control-plane-request.contract` version `1`, and
  `worker_protocol.version: "1.0"`. Missing, unknown, or undiscoverable
  compatibility states fail closed.
- `Client.get_result()` now decodes `WorkflowCompleted` output with the event
  or workflow payload codec instead of assuming JSON.

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
