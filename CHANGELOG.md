# Changelog

All notable changes to the `durable-workflow` Python SDK are documented here.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ctx.replay_now()` is available at ordinary workflow entry, before any
  durable step. It returns the run's canonical start from the workflow task's
  `replay_clock` carrier (`WorkerReplayClock` v1, sent by Server 3.6.0 and
  later, durable-workflow/server#1156). Every lease of the same run carries the
  same value, so replay is unchanged. A task without a valid carrier, or whose
  carrier disagrees with the history's start kind, keeps entry unavailable, and
  a redrive anchor that history already proves is never replaced. Update tasks
  rebuild the workflow with their own carrier. A query task or pre-accept
  update validation rebuilds with its own task's `replay_clock`. When that
  task has no `replay_clock` key (Servers without durable-workflow/server#1338),
  a rebuild in which the workflow reads the clock at entry
  fails the query or validation, even if the workflow catches
  `ReplayClockUnavailable`, rather than answering with state the workflow task
  never computed. An explicit `null`, or an object this SDK rejects, means
  the workflow task had no carrier either, so the rebuild proceeds. Upgrade note: an in-flight run whose code
  branches on entry unavailability takes the other branch after this upgrade,
  so version such code or drain those runs first. `replay()`,
  `Replayer.replay()`, `replay_history()` and `apply_update()` accept the
  task's object as `replay_clock_entry`.
  `ctx.now()` still returns the fixed start time. (durable-workflow/.github#215)
- Workers with cooperative cancellation enabled advertise the `timeout_cleanup`
  capability, version 1: `capabilities` lists it and `capability_manifest`
  carries `{supported: true, minimum_protocol_version: "1.20", version: 1}`. A
  native Server can then admit starts that reserve time for deadline-triggered
  timeout cleanup on their queue. When workflow code lets the delivered
  `WorkflowTimeoutCleanupRequested` escape, uncaught or re-raised, the
  `fail_workflow` command carries `failure.interruption: "timeout_cleanup"`,
  and the Server records the cleanup as completed. An exception the
  application constructs or wraps, a compensation failure and an ordinary or
  explicit `cause: "cancellation"` request stay ordinary failures, which the
  Server records as failed cleanup. `FailWorkflow` gains an optional
  `interruption` field. Workers without cooperative cancellation are unchanged
  and never send the marker; listing `timeout_cleanup` without
  `cooperative_cancellation` raises `ValueError`.
  ([Server #1201](https://github.com/durable-workflow/server/issues/1201))

## [2.23.1] - 2026-11-24

2.23.1 is the first published release containing the 2.23.0 changes. The
`2.23.0` tag exists but was never published to PyPI or GitHub Releases.

### Fixed

- Replay applies signal and update effects in durable receipt order when
  evaluating a condition wait with a timeout. It keeps the first true prefix and
  compares it with the original timer or the competing child terminal. Ordered
  authoring stays disabled until every Server process sharing the database
  supports marked waits. (durable-workflow/.github#230)

## [2.23.0] - 2026-11-23

### Added

- `ctx.continue_as_new(..., target_build="promoted")` can move the successor
  onto the queue's eligible promoted build. The current worker registration
  must advertise support. Plain continue-as-new keeps the predecessor's build.
  An unavailable promotion follows the normal workflow-task retry path.
  (durable-workflow/workflow#880)

## [2.22.1] - 2026-11-23

### Fixed

- Replay mismatches report the authored Python function, filename and source
  line when replay can identify the yielded operation. The replay verifier and
  worker show the same location alongside the existing mismatch details.

## [2.22.0] - 2026-11-21

### Added

- Standalone activity starts accept `duplicate_policy` with native Server
  3.14.0 or later. `use-existing` returns the active execution's identifiers.
  A duplicate under the default `fail` policy raises `ActivityAlreadyStarted`.

### Changed

- Workers record the parallel failure-order marker by default. Before
  upgrading, ensure every worker polling the same queue runs SDK 2.21.0 or
  later. Set `record_parallel_failure_order=False` on the worker or
  `DURABLE_WORKFLOW_RECORD_PARALLEL_FAILURE_ORDER=false` to turn off recording.
  All workers from 2.21.0 onward replay runs with a recorded marker. Drain
  marked runs before rolling back below 2.21.0. (#351)

## [2.21.2] - 2026-11-21

### Fixed

- A workflow handle bound to a run polls that run for its result. It follows
  the recorded continue-as-new successor and skips history when the final
  run description includes its result envelope. (#352)

## [2.21.1] - 2026-11-21

### Fixed

- A parallel list group schedules at the next free command sequence after a
  resolved selection or an applied update. Replay still assigns a cancelled
  selection member to its original selection when the Server omits the
  selection metadata from the cancellation event. (#350)

## [2.21.0] - 2026-11-21

### Added

- Opt-in first-recorded-failure selection for `all()` groups (`yield [...]`), as a two-phase rollout (#307). With `Worker(..., record_parallel_failure_order=True)` or `DURABLE_WORKFLOW_RECORD_PARALLEL_FAILURE_ORDER=true`, a new run records the version marker `durable_workflow.parallel_failure_order` (version 1) directly before its first group that contains an activity or child workflow. A marked run raises the failure recorded first in history, even when every member had settled before the workflow resumed. Workers up to 2.20.0 raise the lowest-position failure in that case, so the exception a workflow caught depended on whether its task ran before or after the other members finished. The first recorded failure is still raised as soon as it is recorded, and the other members keep running, are not cancelled, and have their outcomes ignored. Ties follow durable event order, as the shared parallel-group contract specifies. Workflow's embedded engine compares millisecond timestamps and then member index, so failures recorded in the same millisecond can be chosen differently (durable-workflow/.github#415). Any other value of the environment variable is refused when the `Worker` is created.
  - **Default off; enable it only after every worker on the task queue runs this release or later.** Published 2.20.0 and earlier cannot replay a marked run. The Server marks the workflow task replay-blocked with no retry, and the run waits until it is repaired, which can hand it to an old worker again. With recording off, new runs send exactly the commands 2.20.0 sends. Turning recording off again is safe. Rolling back below this release requires marked runs to be drained first. The next minor release turns recording on by default (#351).
  - **Replay safety:** every worker of this release follows a marker a run already recorded, whether or not recording is on, for every workflow task, query and update. A marked run needs no per-group decision, because the first failure a task sees in append-only history is the first every later replay sees. A run whose first such group was recorded without the marker keeps the earlier rule for its whole life. Its settled groups raise the lowest-position failure, and a failure raised while siblings are open still records the #338 decision. This holds even when a task recorded no command after catching the failure, or another worker answered an update in between. A run with a cancellation request when it reaches its first group records no marker.
  - **Regression evidence:** fixtures recorded by published 2.19.1 on a Server built from main replay under the earlier rule. They cover a settled group in flight, a catch followed by a task that recorded no command, an update answered by 2.19.1 before or between later tasks, and a select() cancellation. Marked runs recorded on native and PHP Server replay under the new rule without the setting. New fixtures cover the first task with recording on and off. Existing fixtures, including `parallel-group-metadata-emission`, the completion-limit fixtures and `business-clock-parallel-failure-selection-gap`, are unchanged.
- `ActivityFailed.lease_loss_reason()` tells workflow code whether an activity attempt failed because its worker lost the claim. It returns `"worker_lost"` when a worker released a claim it could not maintain (failure type `worker_lost`, see Fixed). For a lease the Server settled (`Workflow\V2\Exceptions\ActivityLeaseExpiredException`), it returns the recorded `failure_reason`, `"lease_expired"` or `"worker_deregistered"`, defaulting to `"lease_expired"`. It returns `None` for any other failure. `ActivityFailed.failure_reason` now carries that recorded field. The default activity retry policy is unchanged.

### Changed

- `ActivityLeaseAuthorityLost` is renamed `ActivityWorkerLost`. The old name stays importable from `durable_workflow.errors` as a deprecated alias of the same class, so existing `except ActivityLeaseAuthorityLost` handlers keep working. `durable_workflow.errors.WORKER_LOST_FAILURE_TYPE` (`"worker_lost"`) and `ACTIVITY_LEASE_EXPIRED_EXCEPTION_CLASS` name the two recorded forms of a lost claim.

### Fixed

- A completion refused as `413 payload_too_large` because the worker's cached payload policy is stale no longer loses the computed result.
  - **The bug:** the worker resolves the runtime external payload policy (`namespace.external_payload_storage`, including `threshold_bytes`) once and keeps it. When the Server lowered its limit mid-run, a result under the old threshold was sent inline and refused. Python did not exit, unlike PHP, but logged `failed to complete`. The result was lost, and the activity failed later as a lost lease.
  - **The fix:** on such a 413 for an activity completion, failure report or workflow-task completion, the worker rediscovers the policy (`GET /api/cluster/info`, retried through outages with the shared backoff while the lease lasts). If the policy changed, it re-encodes the request, externalising payloads the new threshold requires, and resends once. The 413 is refused before the request is processed, so the resend is safe. The worker never exits and never reruns the activity.
  - **Undeliverable results:** some activity results still cannot be delivered. That covers a result that is still too large after rediscovery, one with no external storage to offload to, and one over the upload limit. Instead of expiring silently, the attempt is now reported as a non-retryable failure with `failure.type` and `failure.class` set to `ActivityResultTooLarge`, the same as PHP and Rust, and a message naming the Server's limit. The worker keeps running.
  - **Unchanged:** an oversized workflow-task completion still fails the run with the structural-limit failure.
- A cluster-info read failure at worker startup no longer stops the worker.
  - **Retried:** `GET /api/cluster/info` has no side effects, so `Worker.run()` and `run_until()` now treat any failed connection and any 5xx or 429 answer to it as transient, unless the Server says `retryable: false`. Before, only the narrower transient set did (for example 503 `storage_unavailable`), so a plain `500` stopped the worker. The read is retried with the shared backoff `uniform(0, min(4, 0.25 * 2**(n - 1)))` seconds until the runtime answers.
  - **Logging:** startup retries now escalate like poll outages, to ERROR after 60 s and then every 5 minutes.
  - **Still fatal:** typed refusals keep their behavior. A 401 or 403 is fatal at startup.
- A worker answer that names a capabilities digest the worker does not hold no longer strands the request (PHP and Rust parity). Before, it raised `ServerCapabilitiesDigestError`, and a leased task sat until its lease expired. In a lab run with garbled digests, 0 of 10 workflows finished in 150 s.
  - **Non-idempotent requests are never resent.** For a completion, failure report or heartbeat, the client keeps the answer as received, since everything but the capabilities object is valid. It reads the capabilities with a read-only `GET /api/cluster/info` (`worker_protocol.server_capabilities`, the same object) and forgets its held digest, so later requests send `none` and learn a fresh copy. If discovery is unavailable, the field is dropped; no caller reads capabilities from those answers.
  - **Task polls** are safe to repeat, so a poll is instead resent once with `none` to get the full object. An unknown digest on that resend, or in an answer to a request that already sent `none`, is a protocol error.
  - **Error bodies:** an unknown digest in an error body is dropped. The real HTTP error surfaces unchanged and is not resent.
- A worker no longer exits when the runtime is warming up or briefly unavailable while it registers (same issue as durable-workflow/sdk-php#448). Before, `Worker.run()` and `Worker.run_until()` raised once the client's three transport attempts had failed on `GET /api/cluster/info` or `POST /api/worker/register`. That covered native Server's `503 {"reason":"storage_unavailable"}` during warm-up, a refused connection and an upstream 502/503/504.
  - **Retried:** discovery and registration are now repeated with the backoff shared across SDKs, `uniform(0, min(4, 0.25 * 2**(n - 1)))` seconds, logging a warning each time. The transient failures are: refused, reset or timed-out connections; upstream 502/503/504 without a protocol body; 503 `storage_unavailable` or `backend_unavailable`; and any 5xx or 429 with `retryable: true`.
  - **Still fatal:** `retryable: false`, HTTP 401/403, other 4xx refusals and an incompatible Server still stop the worker. It now logs `worker <id> stopping: registration failed with a non-retryable error: ...` at error level first.
  - **Activity outcomes:** an activity completion or failure report that hits one of those transients is now sent again with the same backoff while the attempt's lease covers the next wait. Before, it was dropped after the client's three quick attempts, with only a warning. A transient never turns a completion into a failure report. When the lease runs out, the worker stops resending and logs an error, and the Server settles the attempt at lease expiry.
  - **Duplicate completions:** a resend answered `409 stale_attempt` with `recorded: false`, the same task, attempt and lease owner, and activity, attempt and task status all `completed` is this worker's committed completion and counts as delivered. That is what native Server 3.17.0 answers after a transient hid the committed first completion, and it is the same rule as PHP. Any other `stale_attempt` is a closed attempt and is never treated as success.
  - **Poll-time auth refusals:** pollers still retry 401/403 so credentials can rotate under a running worker. PHP and Rust stop the worker instead. Every such refusal is now logged at ERROR level, not rate-limited as a warning, including refusals before a later recovery.
  - **Poll backoff:** poll retry waits now follow `uniform(0, min(10, 0.25 * 2**(n - 1)))` seconds, the shared full jitter with a 10 s cap, while still honouring a bounded Server retry delay. Before, the ceiling started at 1 s and doubled to 30 s, which slowed recovery.
  - **Long outages:** a poller failing continuously for 60 s now logs the outage at ERROR level, then every 5 minutes while it lasts. Before, it logged only a warning about once a minute. A successful poll resets this.
  - **Heartbeats:** worker heartbeats already rode out these failures.
- `WorkflowHandle.result()` and `Client.get_result()` on a handle bound to a run that continued as new no longer silently return `None` (#356). They now follow the continue-as-new chain to the run that actually ended, as the PHP and Rust SDKs do. That run's result is returned, or its typed failure is raised. The chain is followed through each `WorkflowContinuedAsNew` event's `continued_to_run_id`; when no successor is recorded, the newest run named by the status read is used. A chain that cannot be followed raises `WorkflowFailed`.
- An activity worker no longer silently drops a claimed task when lease renewal fails transiently (#352, same bug as durable-workflow/sdk-php#445). Before, if the `renew-lease` confirmation sent right after an activity poll timed out, hit a connection reset or got a 5xx/429, the poll raised `ActivityLeaseAuthorityLost("activity_initial_lease_unavailable")` and the worker discarded the task. The Server then held the lease (300 s by default) until it expired and, with the default single attempt, failed the workflow.
  - **Keeps working:** the activity now runs under the lease it already holds, and renewal is retried inside that lease. The backoff is the same in every SDK: the *n*-th retry waits `uniform(0, min(4, 0.25 * 2**n))` seconds, so 0.25 s doubling to a 4 s cap, with full jitter. Each renewal exchange is bounded to leave a reserve of the lease (at most 10 s, at most a fifth of the grant). An unconfirmed lease is charged from the poll's start, because the Server stamps it only after receiving the poll.
  - **Stops before it releases:** if renewal still has not succeeded when only the reserve is left, the worker stops the execution. An async activity is cancelled. A thread activity is asked to stop: its next `heartbeat_sync()` raises `ActivityWorkerLost`. A callback process is stopped. The worker waits for that stop for up to half of the reserve.
  - **Releases through the Server:** only once every execution of the claim is confirmed stopped does the worker release the claim, following the cross-SDK contract shared with PHP and Rust. It sends `POST /worker/activity-tasks/{id}/fail` with `failure.type = "worker_lost"` (recorded as `exception_type`, and matchable in `non_retryable_error_types`), `failure.class = "ActivityWorkerLost"`, `non_retryable = false`, the local cause `activity_lease_renewal_unavailable` in the message, and `details = {"reason": "worker_lost", "cause": "activity_lease_renewal_unavailable"}`. The retry policy then applies right away.
  - **Waits for expiry if it cannot stop:** if the execution does not stop in time, for example a thread that never heartbeats, nothing is sent. The Server settles the attempt at lease expiry (`lease_expired`), so two executions of one attempt never overlap. A claim that never started is released directly. A typed Server refusal (for example `lease_expired` or `run_closed`) still ends the claim as before. See "Activity lease renewal" in the SDK reference.
  - **Replay safety:** worker-side lease handling only. No workflow commands or recorded history change.

### Changed

- Workers send far fewer response bytes on a Server that supports the capabilities digest (#352, durable-workflow/server#1262).
  - **The header:** worker-plane requests now send `X-Durable-Workflow-Server-Capabilities-Digest` (`none` until the first full copy arrives).
  - **The Server's answer:** the Server replaces each repeated `server_capabilities` object (about 17 KB on native 3.15/3.16) with its digest, or sends a full copy tagged with a `digest` member.
  - **What callers see:** the SDK restores elided copies before any caller sees the answer, in success bodies, error bodies and negotiated poll envelopes, up to three levels deep. It strips the `digest` member, so callers still see complete capabilities.
  - **Overlapping requests:** each answer is resolved against the digest and object its own request was sent with. A request in flight during a rolling deploy that changes the capabilities restores the object it was sent with, while later requests use the newer digest.
  - **Errors:** a digest the worker does not hold raises the new `ServerCapabilitiesDigestError`, a protocol failure.
  - **Older Servers** ignore the header and answer in full as before.
- Response bodies are bounded after decoding: 128 MiB by default, configurable with `Client(max_response_body_bytes=...)` (and the sync client). httpx decodes `Content-Encoding: gzip` transparently, so before this a small compressed answer could expand without limit in memory.
  - **How:** each response stream is counted as it decodes, inflating at most 1 MiB at a time. It is refused with the new `ResponseBodyTooLarge` before httpx decodes past the limit, and that error is not retried. The bound also applies to runtime external-payload fetches.
  - **Encodings:** the client now asks only for `gzip`, which is what native Server sends. Any other response `Content-Encoding` is refused with `httpx.DecodingError`.
- `WorkflowHandle.result()` and `Client.get_result()` no longer read the run's history for a completed run when the status read already carries that run's `output_envelope`. The status read is `GET /workflows/{id}`, which the result poll makes anyway. Failed, cancelled, terminated and timed-out runs still read the history for their typed failure, and so does a completed run whose status read has no `output_envelope` or names a different run.

## [2.20.0] - 2026-11-19

Both additions need **native Server 3.15.0 or later**, the first Server release that advertises `capabilities.signal_with_start` and the `signal_external_workflow` worker command. Each is discovered from the Server's capabilities and refused before anything is sent on an older Server, so the SDK's supported Server baselines are unchanged and every other feature keeps working on older Servers. Worker protocol (1.20) and control-plane (2) versions are unchanged.

### Added

- `Client.signal_with_start()` (and the sync client's) starts a workflow or reuses its open run and records one signal on it in a single Server transaction, so the run cannot close between a separate start and signal (durable-workflow/.github#222, server#1133). It needs a Server that advertises `capabilities.signal_with_start` (native Server 3.15.0 or later) and raises `RuntimeCapabilityUnsupported` before sending otherwise. An open run, including the newest run after continue-as-new, is reused with its input and deadlines unchanged (`start_outcome == "returned_existing_active"`); `duplicate_policy="fail"` refuses it. A closed run is refused with `run_not_active` unless `closed_run_policy="start_new_run"`, which starts a fresh run after it. `request_id` is the signal's command key (generated as `sdk-python-signal-with-start-<uuid>` when omitted): a retry returns the original receipt, even after the run closed. Every refusal raises the new `SignalWithStartFailed` (a `SignalFailed`) with the Server's `reason`, and nothing was written.
- Workflow code can signal another workflow durably with `ctx.signal_external_workflow(workflow_id, signal_name, args=None, *, run_id=None)` (durable-workflow/.github#223, durable-workflow/server#1134). Yielding it sends the `signal_external_workflow` command and resumes with an `ExternalSignalResult` (`outcome`, `reason`, `target_run_id`, `target_command_id`, `target_signal_id`, `retained`, `delivered`). A missing, closed or refusing target is a returned outcome (`target_not_found`, `target_closed` or `rejected` with the Server's reason), not an exception, because the Server records it as a result and workflow code decides what to do. Arguments travel as an inline Avro payload envelope, like child workflow arguments. A signal name the Server would refuse (blank, a control character below U+0020 or U+007F, more than 191 characters or more than 255 UTF-8 bytes) raises the new `InvalidSignalName` (a `ValueError` with `reason`) in workflow code when the command is created, so nothing is sent and the task does not fail with a 422. The command is yielded on its own; a parallel group or selection member is refused.
  - **Replay safety:** replay matches `ExternalSignalRequested` at the command's sequence and returns the outcome from the following `ExternalSignalDelivered` or `ExternalSignalFailed`. A recorded signal is never sent again, including on a cold replacement worker and while its outcome is not yet recorded. A different target, run, signal name or arguments at that sequence raises `NonDeterministicReplayError`. Existing histories are unchanged.
  - **Server requirement:** native Server 3.15.0 or later. The Worker submits the command only when the Server advertises `signal_external_workflow` in `worker_protocol.server_capabilities` (and lists it in `supported_workflow_task_commands` when that list is present). Otherwise it fails the workflow task with `signal_external_workflow_unavailable` before submitting, so the signal is never sent outside history.

## [2.19.1] - 2026-11-18

### Fixed

- A discarded replay finalized during garbage collection could read the parent workflow's replay clock. Clock reads made while a collection runs now raise `ReplayClockUnavailable`, and recorded history is unchanged (#343, #344). In detail: code run by Python's cyclic garbage collector can no longer read the business replay clock. When a discarded replay (for example an outcome that was never closed and is only reachable from a reference cycle) is collected, CPython finalizes the objects in that cycle in no specified order. If the discarded workflow generator was finalized before the replay lifetime that owns it, its `finally` block ran outside discard handling and `ctx.replay_now()` on a still-active parent workflow returned the parent's time instead of raising `ReplayClockUnavailable`. Whether this happened depended on when an automatic collection had last run, so it showed up as an intermittent test failure on Python 3.10 (#343). `replay_now()` now raises `ReplayClockUnavailable` for every read made while a collection runs on the reading thread.
  - **Replay safety:** no command, history field or recorded value changes. Only reads made from finalizers or weakref callbacks during a collection are affected, and those never ran at a deterministic point in workflow code.

## [2.19.0] - 2026-11-18

### Added

- `WorkflowContext.schedule_origin` returns the schedule fire that started the run as a `ScheduleOrigin` (`schedule_id`, `occurrence_time`, `trigger_number`), or `None` for a run no schedule started (durable-workflow/.github#243). It reads the run's `ScheduleTriggered` history event, so it is available on the first workflow task and identical on replay. `occurrence_time` is the time the scheduler evaluated (the jittered fire time for a tick, the enumerated time for a backfill), not the run's start time, and is `None` for a manual trigger. Histories without the event read `None`, so existing runs replay unchanged.
- A 409 `request_id_ambiguous` refusal from native Server 3.9.2 and later now raises the typed `SignalFailed` or `UpdateFailed` with `reason == "request_id_ambiguous"` and the sent `request_id`, like `request_id_conflict` (durable-workflow/.github#216). It means the command key matches an ambiguous legacy retained identity (a raw legacy key and a hashed key, or keys differing only by case on a case-insensitive database). Nothing was written and no receipt was selected; it is never resent automatically. The new `errors.REQUEST_ID_AMBIGUOUS` names the reason. A legacy receipt retained before the Server recorded command intent is still returned as a receipt with `retained: True` and `intent_verified: False`. Signal and update docstrings now describe command keys as 1-255 Unicode characters, matching the Server limit.

### Fixed

- An `all()` group (`yield [...]`) whose member fails while other members are still running no longer blocks the run. Server wakes the workflow once, for the failure, and never again for that group. The worker used to send the group's whole scheduling batch again on that wake, and native Server refused the completion with `409 parallel_sequence_mismatch` (2.17.2 and 2.18.0 on native Server 3.12.0 and 3.13.0). The first recorded member failure is now raised at the yield as soon as it is recorded, as in Workflow's embedded engine. The other members keep running and their outcomes are ignored. A wake while a group is open, such as a signal, also no longer sends the group again. (#338)
  - **Recorded decision:** the task that raises a failure early first records which member it raised, as a side effect whose result is keyed `durable_workflow.parallel_failure_decision`, at the sequence right after the group. Replay raises exactly that member. History has no workflow-task boundaries, so without the record a member failing after the task read history but before its completion was written would make a later replay choose differently. Workflow code after such a group therefore starts one sequence later.
  - **Replay safety:** a history in which every member settled before the workflow did anything else replays as before, raising the failure with the lowest input position. This is now the documented default for groups with no recorded policy, matching Workflow's embedded engine and the PHP and Rust SDKs. The explicit policies remain tracked in #307.
  - **Visible effect:** when a member fails while a sibling is still running, workflow code after the group, such as a compensation, now starts one sequence later than it would have, because the decision record takes the sequence right after the group. Nothing else about the group changes: the same failure reaches the same `except` block.
  - **Runs already blocked under 2.18.0 or earlier recover after the upgrade.** Upgrade every worker on the task queue to 2.19.0, wait until the group's other members have finished, then repair the run (`WorkflowHandle.repair()` or `Client.repair_workflow()`). The repaired workflow task raises the group's failure on the upgraded worker instead of sending the group again, and the run continues. Upgrading alone is not enough, because native Server sends no new workflow task to a blocked run. Native Server 3.13.0 refuses the repair with `development_workflow_task_repair_active_work_unqualified` while a member is still running, and also when the blocked run has received a signal or an accepted update, so a run in that state stays blocked. Checked on native Server 3.13.0: a run blocked by a 2.18.0 worker stayed `waiting` after the upgrade until it was repaired, then ran its compensation and completed.

## [2.18.0] - 2026-11-18

### Added

- `ActivityRetryPolicy.exponential(...)` creates generated activity retry schedules with a finite 60-second default maximum interval (durable-workflow/.github#219). Omitting `maximum_interval_seconds` caps every generated delay at 60 seconds, so nine total attempts wait `1, 2, 4, 8, 16, 32, 60, 60` seconds. An explicit finite, non-negative `maximum_interval_seconds` selects a different cap; `None`, infinities and NaN are refused. `max_attempts` still counts the first attempt, and the command keeps the existing `backoff_seconds` wire shape and whole-second rounding, so the same policy works for Server-scheduled activities and, through `.to_dict()`, local activities.
  - **Who is affected:** only code that calls the new factory. Nothing changes for existing code. The legacy `ActivityRetryPolicy(...)` constructor keeps its original behavior: omitting `maximum_interval_seconds`, or passing `None`, still applies no cap (with 20 attempts the last generated delay is about three days). Keep using the constructor where an uncapped schedule is intended; the factory has no uncapped mode.
  - **Replay safety:** retry delays are generated when the command is first scheduled and are recorded in history as an explicit `backoff_seconds` list. Histories recorded under 2.17.2 and earlier, including uncapped schedules, replay with their recorded delays unchanged, and explicit backoff lists are never rewritten. Adopt the factory in new workflow definitions or new versions; do not change the policy of a command that an existing run must replay.
- Read-only acknowledged heartbeat checkpoint accessors: `activity.context().last_heartbeat_checkpoint()`, `last_heartbeat_details(result_type=...)`, `last_heartbeat_at()` and `heartbeat_resume_version()`, with the immutable `ActivityHeartbeatCheckpoint` and `ActivityHeartbeatResumeUnavailable`. Activity workers now advertise `activity_heartbeat_resume`. Only a claim that delivers the v1 contract for its own execution, attempt, workflow, run and namespace supplies a snapshot; claims from older or nonparticipating Servers, inline local activities and Server-free tests raise `ActivityHeartbeatResumeUnavailable` instead of inventing an empty checkpoint, and a supported claim with no checkpoint returns `None`. Received details, contract version and acknowledgement time stay immutable across heartbeat outcomes. Installed cold-replacement qualification remains tracked separately. (durable-workflow/.github#218, #333)

### Fixed

- Local activity retries keep their workflow task leased while they wait out a backoff. The worker renews the lease at least every 5 seconds during the wait. Before, a backoff as long as the Server's workflow-task lease (60 seconds by default on native Server, which equals the new generated default cap) let the lease lapse, so the Server reclaimed the task and the local activity ran again from its first attempt. The full backoff is still waited and recorded unchanged. (durable-workflow/.github#219)
- A workflow-task completion the Server refuses over its history-transaction budget (`409` with reason `history_transaction_limit_exceeded` or `completion_budget_exceeded`) now raises `WorkflowTaskHistoryLimitExceeded`, a subclass of `WorkflowTaskCompletionTooLarge`. The worker fails the run once with a non-retryable `fail_workflow` command whose message starts with `Structural limit exceeded:` and names the limit kind, measured value and configured limit when the Server reports them. Previously it reported a workflow-task failure, so a fan-out of about 1,000 × 22 KB arguments (a 30 MB completion, under the 32 MiB wire limit) left the run pending with a failed task and no recorded progress on native Server 3.12.0. This SDK change makes the refusal a typed structural-limit run failure instead of a replay block; it does not make such a fan-out fit. The full fix for documented-size fan-outs, raising native Server's default history-transaction budget above the 32 MiB completion cap, needs native Server 3.13.0 (durable-workflow/server#1231). (durable-workflow/.github#228, #337)

## [2.17.2] - 2026-11-13

### Fixed

- The query client guide, SDK reference and API docstring now document the Server receipt returned by `Client.query_workflow()`, `WorkflowHandle.query()` and the synchronous client. Read `receipt["result"]` for the decoded handler value, including a successful `None`. Runtime behavior is unchanged. (#316, #317)
- Query workers return valid Avro values containing bytes without also trying to serialize those bytes as plain JSON. The authoritative `result_envelope` is unchanged; JSON-compatible results retain their optional plain projection. (#319)

### Changed

- Replay refuses explicit `parallel_group_policy` histories before constructing application state until a qualified policy consumer is enabled. Absent-policy histories retain their wait, failure-selection and clock behavior. Explicit propagation is being prepared behind disabled worker admission. (#307)

### Added

- Experimental timeout-cleanup parser/exception preparation (workflow#863, workflow#1129, server#1201). This patch advertises no timeout-cleanup capability and does not qualify producer delivery or enable timeout cleanup. `CancellationContext.from_dict()` reads the context's optional top-level `cause`: absent or `"cancellation"` is an ordinary cancellation, `"timeout"` marks a timeout-cleanup request (the run passed its work deadline and is now in cleanup), and any other value raises `ValueError`. The context carries no other timeout fields: its existing `cleanup_deadline_at` is the cleanup deadline, and the timeout kind and work deadline stay on the `WorkflowTimeoutCleanupStarted` event. `cause` and `is_timeout` are readable on the context, and `to_dict()` writes `cause` back exactly as received, including an explicit `"cancellation"`, leaving ordinary contexts byte-identical. A timeout request is raised at the authored call as the new experimental `WorkflowTimeoutCleanupRequested`, a subclass of `WorkflowCancelled`, so existing `except WorkflowCancelled:` handlers, such as saga compensation, run on timeout too. It stays `BaseException`-rooted and is not an `asyncio.CancelledError`. Test `cause`, the cross-SDK discriminator, to tell the two apart. `cause` is never inherited: a child cancelled under a timeout request, scoped or not, receives an ordinary cancellation without `cause`.

## [2.17.1] - 2026-11-13

### Fixed

- `Client` refuses supplied `token`, `control_token` and `worker_token` values with edge whitespace or control characters before choosing a role or opening an HTTP client. The error names only the option. Values are never trimmed, and empty values retain their existing fallback behavior. (#304)

- Replay supports histories in which a run's own close settled the work it never awaited (server#1193, workflow#1117/#1121, Workflow 2.28.0). When a run completes, fails or continues as new, each still-open activity, such as a `select()` loser, gets an `ActivityCancelled` with `cancellation_reason: run_closed` just before the terminal event. Replay, query and update replay now skip that cancellation and leave the activity open, as the workflow saw it. It never resolves a selection member, never supplies replay time, and is never taken as a list group's first positional failure. Before this fix, a close's cancellation of a lower-index member could replace the failure the workflow handled. Replay also accepts the new `abandoned_activities` key on `WorkflowCompleted`, `WorkflowFailed` and `WorkflowContinuedAsNew`, which lists abandon-policy activities left running at close.
- Activity workers treat a `run_closed` refusal as final. A completion refused with `recorded: false, reason: run_closed` is split on its `activity_status`, as in the PHP worker. When it is `completed` or `failed`, the activity was an abandon-policy activity that kept running after its run closed and its own outcome closed it: the task counts as completed and logs at info level. Otherwise the close cancelled the activity: its outcome is dropped, the task is reported as aborted (never completed), and a warning is logged. A failure or cancellation report refused with `run_closed` is not reported again and logs at info level. A claim refused with `run_closed` counts as an answered poll, with no failure backoff. A heartbeat refused with `run_closed` raises `ActivityLeaseAuthorityLost` and publishes no outcome, as before.

## [2.17.0] - 2026-11-13

### Added

- `Client.update_outcome()` and `Client.update_outcome_receipt()` read an accepted update's outcome later by its update id with `GET /api/workflows/{workflow_id}/updates/{update_id}`, and never submit the update again (durable-workflow/.github#212, server#1131). `update_outcome()` waits for the outcome by default and returns the decoded result, including `None`, with optional `result_type` decoding. A still-pending update raises the new `UpdateStillPending`, which carries `workflow_id`, `run_id`, `update_name`, `update_id`, `request_id` and `update_status` and tells the caller to read again by update id; it is never returned as `None`. A failed update raises `UpdateFailed`, a rejected one `UpdateRejected`, and an unknown or pruned id the new `UpdateNotFound` (a subclass of `UpdateFailed`). `update_outcome_receipt()` returns the raw receipt and answers at once by default. Both need a Server that advertises `capabilities.update_outcome_reads` (native 3.6.0 and later); on an older Server they raise `RuntimeCapabilityUnsupported` after discovery, without reading or resubmitting anything. `WorkflowHandle` and the synchronous client and handle have the same methods.

- Update outcome reads check the request and answer. An empty, `.` or `..` workflow or update id, an unknown `wait_for` or a non-positive `wait_timeout_seconds` raises `ValueError` before any request, including discovery, so `..` can never resolve onto the workflow's describe route. A successful answer must name the requested `update_id`, carry a string `update_status` and, when it echoes a `workflow_id`, match it. `update_outcome()` returns a value only for a completed receipt. Any other answer raises the new `UnrecognizedUpdateOutcome` with `workflow_id`, `update_id` and `body`, and never returns a value. With `wait_timeout_seconds`, the HTTP timeout becomes that wait plus 30 seconds whenever that is longer than the client's `timeout`, so a long Server-side wait is not cut off.

### Changed

- The `Client.update_workflow()` and `WorkflowHandle.update()` docstrings now say what they always did: they return the Server receipt, not the decoded result, and the Server answers at `wait_for="accepted"` when it is omitted. A receipt with `update_status: "accepted"` is a pending update even though its `result` is `None`.

### Fixed

- A workflow-task completion the Server refuses as too large (Server 3.6.0's typed `413 payload_too_large` with `limit_kind: workflow_task_completion_bytes`, over 16 payload limits, 32 MiB by default) now raises `WorkflowTaskCompletionTooLarge` with the Server's `limit` and the encoded request `size` (durable-workflow/.github#228). The worker no longer reports this as a workflow-task failure, which made the Server retry the same oversized completion or block replay. It never resends the refused completion: it completes the same task once with a non-retryable `fail_workflow` command whose message starts with `Structural limit exceeded: workflow_task_completion_bytes:`, so the run fails with the `structural_limit` failure category and guidance to split the fan-out, use child workflows or continue-as-new. If that follow-up is rejected too, the worker logs it and never calls `/fail`. Other 413 responses and 5xx retries are unchanged.

## [2.16.0] - 2026-11-12

### Added

- Workers with a sticky cache (`sticky_cache_capacity > 0`) advertise `sticky_history_suffix` (#300, server#1017). On Server 3.5.0 or later, a sticky hit receives only the history after the sequence the worker cached, instead of the full history on every workflow task. A suffix is used only when it continues the cached history exactly; otherwise the worker drops the entry and refetches full history.

## [2.15.2] - 2026-11-11

### Fixed

- `Worker.run_until()` now records the schedule-to-start histogram for activity tasks, as `run()` already did (#188).

## [2.15.1] - 2026-11-11

### Fixed

- Local activities no longer retry with zero delay once their backoff list runs out. Each later retry repeats the final backoff entry, as Server-scheduled activities do (durable-workflow/.github#219).

## [2.15.0] - 2026-11-11

### Added

- Worker schedule-to-start histogram `durable_workflow_worker_task_schedule_to_start_seconds` for workflow and activity dispatches, tagged only with `task_kind` and `task_queue`. It requires Server's `claimed_task_ready_age` capability and a valid integer `ready_age_ms`, records Server-computed milliseconds as seconds, preserves real zero and omits missing/invalid samples. The shared metrics contract is pinned at revision 2. Published-Server and installed-package acceptance remains pending in #188.

- Condition wait opens carry a deterministic `python:condition-wait:{ordinal}` occurrence ID for each authored call, including selection members. False-wake reopens preserve the first open's ID and remaining timeout; legacy chains without an ID keep that absence even when later physical opens have an ID. Separate calls and grace waits receive new ordinals. These identities enable unresolved-open deduplication on supporting runtimes. (#259)

- Opt-in typed workflow values (#184). `@workflow.defn(..., typed=True)` decodes the arguments of `run` (after `ctx`) and of every signal, query, update and update-validator handler to their annotations. `ctx.start_child_workflow(..., result_type=T)` decodes a child's result, and `WorkflowHandle.result(result_type=T)` / `Client.get_result(..., result_type=T)` decode a workflow's result on the client. Dataclass and `Enum` values in client workflow, signal, update, query and schedule arguments, child and continue-as-new arguments, and workflow and update results are sent as their neutral values. A typed `TypedValueError` names the path but never the value. Untyped workflows receive plain values as before, so in-flight histories replay unchanged.

- Typed run-activity visibility through `Client.get_run_activities` and `WorkflowHandle.get_activities`, with activity/attempt metadata, exact run/namespace binding, explicit unavailable fields and preserved diagnostic authority. Standalone activity listing is unchanged. Shared delivery remains tracked in durable-workflow/.github#235.

- `ActivityInfo.activity_execution_id` exposes the received logical activity identity without task/attempt/session fallbacks. Prepared local callbacks receive their validated admission ID; inline local callbacks and older claims without an ID expose `None`. These IDs are public idempotency metadata, not live-claim or external-effect authority, and replace no credentials, heartbeat/current-attempt checks, per-effect suffixes or outcome reconciliation. Shared identity acceptance remains tracked in durable-workflow/.github#211.

- Lazy `Client.iter_workflows`, `Client.iter_history` and `WorkflowHandle.iter_history` follow opaque page tokens while retaining only the current page and bounded cursor bookkeeping. They preserve run/filter/namespace identity, follow empty continued pages, stop on early close, and surface HTTP, cancellation and invalid/cyclic cursor errors. Existing one-page methods keep their contracts. (durable-workflow/.github#247)

### Fixed

- Replay consumes an already-open duplicate of a condition wait when the older physical wait is satisfied with a true predicate. A wait opened after that satisfaction remains a separate authored call with its own timeout. (#256)

- Worker batch encoding converts dataclass and Enum workflow results, update results and continue-as-new arguments to neutral Avro values, matching individual command encoding. Typed results previously failed during real Worker submission. (#184)

## [2.14.1] - 2026-11-04

### Fixed

- `wait_condition` predicate fingerprints no longer change across Python versions or worker processes. Every earlier release hashed the predicate's bytecode and the `repr` of its constants. So upgrading Python (for example 3.12 to 3.13) failed replay of every in-flight run at its first wait, and a predicate containing a nested function (a generator expression, a nested lambda, or a comprehension on 3.10 and 3.11) got a new fingerprint in every process, so a restart or cache loss failed replay with `condition_fingerprint_changed`. New waits record a v2 fingerprint (`dwpy2:sha256:...`) computed from the predicate's module, qualified name and a canonical form of its source AST, with no bytecode, addresses, paths or line numbers. Without source (for example `eval` or a REPL) no fingerprint is sent, and the wait is identified by its key and call order. A v1 value recorded by an earlier release still matches when this process computes the same v1; otherwise it is accepted as unverifiable instead of failing replay, so drift in those predicates is no longer detected. A reopen sends the fingerprint its first open recorded, so a wait opened before the upgrade keeps its v1 value, and a keyless wait links a v1 value only within its own chain. A keyless wait without source links its false-wake reopens within its chain, as before. **Rollback and mixed fleets:** a run that opened a wait under this release records a v2 fingerprint, which 2.14.0 and earlier refuse on replay. Don't roll a task queue back below this release once such waits exist, and upgrade every worker on a queue together. (#258)

- Same-line `wait_condition` lambdas whose bodies contain list, dict or set comprehensions share one combined fingerprint on every Python version. Put those predicates on separate lines to detect a change from one candidate to another. (#258)

## [2.14.0] - 2026-11-04

**Upgrade from 2.13.2 and earlier.** Every earlier release fails replay when a condition wait has an orphaned duplicate open whose timer fires later, which Server 3.0.x can record (#253 below).

### Fixed

- Replay no longer fails when a condition wait has an orphaned duplicate open whose timer fires later. Server 3.0.x can record a second open of the same wait while the first is still open (server#1015). The first open's timeout used to end the logical wait in replay, while history continued with the reopened wait, so replay raised `NonDeterministicReplayError`. A wait now counts as reopened when the next open of the same wait was recorded before the earlier wait's own end. A wait that timed out before the next open is still finished, and the next open is a separate call. Every earlier release has this gap; it is not a 2.13.2 regression. (#253)
- Replay now refuses a recorded condition-wait reopen that records a longer timeout than the wait's first open, or that adds or drops the timeout, with a `NonDeterministicReplayError` whose detail starts `condition_wait_reopen_timeout_exceeded`. Earlier versions linked such a reopen silently. Reopens with the full timeout (written before 2.13.2) or with the time remaining (2.13.2 and later) still replay. This matches sdk-php#271. (#248)
- Typed-value refusals never repeat user data. A map entry is named by its position (`scores[entry 1]`), not its key, and a dataclass whose `__post_init__` raises `ValueError` or a payload nested too deeply fails with a non-retryable `TypedValueError` that does not carry the original message. (#252)

### Added

- Opt-in typed activity values (#184). `@activity.defn(..., typed=True)` decodes an activity's positional arguments to its parameter annotations, and `ctx.schedule_activity(..., result_type=T)` decodes the completed result in single calls, parallel groups and selections. Supported types are dataclasses, `Enum` (including `IntEnum` and `StrEnum`), `Optional[T]`, `list[T]`, `dict[str, T]` and scalars; other annotations raise `TypeError` up front. Undeclared keys are ignored and missing fields take their defaults. A mismatch raises the new `TypedValueError`, which names the field path but never the value. It is non-retryable in a typed activity and is raised at the `yield` for `result_type`. Dataclass and `Enum` activity arguments and results are now sent as their neutral values, the same bytes as `dataclasses.asdict()` with each enum's `.value`; these values were refused before. Untyped activities and calls receive plain values as before, so in-flight histories replay unchanged.

## [2.13.2] - 2026-11-04

### Fixed

- A `wait_condition` timeout now counts from when the wait first opens. Before, a signal that left the predicate false reopened the wait with the full `timeout` again, so every such signal pushed the timeout later: a 300-second wait signalled after 2 minutes timed out after 7. The reopen now carries the seconds left before the first open's deadline, rounded up and never below 0, measured at the latest recorded history time. Histories without recorded times keep the authored timeout, and histories written by earlier versions still replay. (#245)
- Replay now enforces the recorded search attributes (Workflow protocol 1.20 replay identity). A workflow whose `upsert_search_attributes` differs from its `SearchAttributesUpserted` history fails replay with a `NonDeterministicReplayError` whose detail starts `search_attribute_value_mismatch`, as memo replay already does for memos. Values are compared as Core's `UpsertSearchAttributesCall` normalizes them (trimmed strings and list items, a blank string as null) and with their types (`1`, `1.0`, `True` and `"1"` differ). An authored tuple is checked as the JSON list it is sent as. Values outside that portable domain, on either side, are refused rather than coerced: non-string or non-portable keys, objects, sets, bytes, maps, non-string list items, non-finite floats and out-of-range integers. A `SearchAttributesUpserted` event without `attributes` is refused rather than skipped, with a detail starting `search_attribute_update_missing`. One without a workflow sequence is refused with `durable_command_sequence_missing` (or `durable_command_sequence_invalid` for zero, negative, string, float or bool) rather than given one, and a second upsert at the same sequence with `duplicate_search_attribute_update`. An upsert that shares its workflow sequence with any other durable command record is refused with `durable_command_sequence_collision`. Every lifecycle row that carries a command's sequence claims it, including activity heartbeat, retry and cancellation-acknowledged rows, child cancellation rows, service operation scheduling, cancellation scope openings, continue-as-new, and cooperative and scope cancellation deliveries, which claim every sequence in their `sequence_span` (absent or null means 1, as in Core; any other value that is not a positive integer is refused with `durable_command_sequence_invalid`); events that only point at a step, such as a signal receipt, an update or a cancellation request, claim no sequence. When the history records `attribute_types`, it must type exactly the update's non-null keys, and each type must be a known type that can hold the value; anything else, including a malformed type value, fails with `search_attribute_type_mismatch`. The SDK's upsert declares no types and the Server records a key's namespace-registered type ahead of the inferred one, so a string may be recorded as `keyword`, `string` or `datetime` and an integer as `int` or `float`; search-attribute type identity is therefore not pinned for Python. History without `attribute_types` keeps value-only identity. Replay diagnostics name reasons, keys, JSON types, sequences and event types, never a recorded or authored value. (#235)

### Corrected

- **2.13.1 claim withdrawn (#237).** The 2.13.1 entry said "The cancellation clock prefers a reused step's `reused_recorded_at`." As an observable result, that is incorrect. The test behind it was not a history Workflow 2.19 can write: it had no redrive `WorkflowStarted` and delivered the cancellation before the reused step. Redrive writes the successor's start and its reused prefix together, so a successor cancellation always follows the prefix and `remaining()` is computed from that delivery. A reused event's `reused_recorded_at` therefore cannot reach the budget, and the preference changes no observable result. Replay behavior is unchanged. The 2.13.1 start-clock change (`ctx.now()` from `replayed_started_at`) is unaffected. Valid successor fixtures replace the evidence, and the 2.13.1 fixture `redrive-start-replayed-clock`, which has only a partial redrive envelope, is registered as non-qualifying.
- **Redrive successor evidence is checked against Core's full cancellation shape (#239).** The test check for a successor's own cancellation now requires what Workflow 28893f97 writes: a root `durable-workflow.cancellation-context/v1` context whose lineage is exactly the request's own address, a null `parent_request_id`, `requested_at` no later than the request, `command_type` `request_cancellation` on the request, and a single-call `call_kind` on the delivery that matches the recorded call. A delivery to a call resolved before the request is refused, and so is any event recorded at an unscheduled step the delivery consumed. The unreleased #236 fixture `redrive-reused-upserts-cancellation-clock` has no `command_type`, so it is registered as non-qualifying and `redrive-successor-reused-upserts-cancellation-clock` replaces it. Tests and fixtures only; replay behavior is unchanged.

### Added

- Replay of Workflow 2.19 redrive reuse is qualified for `MemoUpserted` and for `SearchAttributesUpserted` value identity. Neither event is a replay clock source: `ctx.now()` stays the persisted start time, which on a redriven run is the failed run's start, and the cancellation budget does not move on an upsert. Search-attribute type identity is not qualified. The SDK's upsert declares no `attribute_types`, and the Server records namespace-registered types the SDK cannot see, so recorded types are not compared with authored ones. They are only checked to be well formed and able to hold the recorded value. Signal waits (`SignalWaitOpened`, `SignalReceived`, `SignalApplied`) are scoped out for Python, because the SDK cannot author a Core signal wait. Reused signal-wait events replay for history validity only: a cold replay applies the recorded signal once, takes no time from these events and opens no wait again. This does not qualify Python workers for Core signal waits. (#235)

## [2.13.1] - 2026-11-03

### Fixed

- A run created by redrive now replays from the failed run's start time. `ctx.now()` and `ctx.uuid7()` read `WorkflowStarted.replayed_started_at` when `recovery_kind` is `redrive`, as Core does, so workflow code before the failed step sees the same clock as the failed run. An invalid value is refused rather than ignored. The cancellation clock prefers a reused step's `reused_recorded_at`. (#232)

## [2.13.0] - 2026-11-03

### Added

- `Worker.status()` returns a read-only `WorkerStatus` snapshot for health
  endpoints and orchestrator probes. Readiness requires this worker's own
  acknowledged registration, running pollers and a successful latest Server
  exchange per role; liveness reports event-loop progress, exited pollers and
  stalled requests. Snapshots perform no network I/O and can be read from
  another thread. Adds `WorkerState` and `WorkerRoleStatus`, a runnable probe
  example and a Kubernetes guide. Existing worker behavior is unchanged.

### Fixed

- History-bundle verification and bundle simulation now fail a
  retention-pruned export (`workflow.details_state: "pruned"`) with the
  `history.details_pruned` error, matching PHP Core, instead of reporting it
  as `ok`. Retained bundles and older exports without the marker are
  unchanged.

## [2.12.4] - 2026-11-02

### Added

- Declare minimum stable Server releases per major, including native Server 3.x,
  while preserving the legacy 2.x declaration. Include the compatibility record
  in installed wheels and source distributions and verify it against release
  source. Worker protocol and capability negotiation remain unchanged.

## [2.12.3] - 2026-11-02

### Fixed

- Report exceptions with empty or whitespace-only messages using meaningful
  fallback text, so workflow and activity failures reach callers instead of
  being rejected as invalid failure reports. Preserve the exception type,
  stack trace and task identity in ordinary and supervised activity workers.
- Apply the same blank-message handling to invocable activities, queries and
  update validators while keeping nonblank diagnostic text unchanged.

## [2.12.1] - 2026-10-29

### Fixed

- Preserve the committed condition reopen when another signal arrives during
  a signal's completion. Replacement workers replay both signals exactly once
  without skipping durable waits or weakening definition-drift checks.

## [2.12.0] - 2026-10-27

### Added

- Expose immutable received retry policy and original attempt metadata through
  `ActivityInfo.retry_info`, with explicit unknown, legacy-null and unlimited
  states and a last-configured-attempt check.
- Expose nullable original workflow/run IDs in remote, inline and prepared
  activity contexts without substituting task or activity-attempt IDs.
- Add an optional `run_id` to async and sync signal calls, with canonical
  activity guidance for stable business-event keys and delivery reconciliation.

### Fixed

- Preserve the bound run when `WorkflowHandle.signal` sends a signal, instead
  of silently targeting whichever run is current.

## [2.11.2] - 2026-10-25

### Fixed

- Bound each worker poll HTTP attempt with an absolute deadline, including
  responses that keep sending progress. Preserve cancellation, retry budgets
  and the original poll identity.
- Accept negotiated poll-progress responses alongside ordinary JSON, applying
  their original status and retry headers before normal error handling.
- Warn when a connection failure or gateway error cuts a poll short, naming
  the observed duration and configured wait.

## [2.11.1] - 2026-10-24

### Fixed

- Keep the bounded `Worker.run_until` helper polling after a matching typed
  backend-lock refusal. Backoff stays inside the original caller deadline,
  releases reserved capacity and stops promptly on cancellation. Malformed,
  foreign and fatal refusals retain their existing error behavior.

## [2.11.0] - 2026-10-23

### Fixed

- Bound failure messages, stacks and diagnostic payloads before reporting them.
  The complete encoded request defaults to 1 MiB, including JSON escaping and
  markers, and respects lower configured or discovered limits. Preserve failure
  type, retry policy and claim identity; refuse an identity that cannot fit.

## [2.10.1] - 2026-10-23

### Fixed

- Keep worker deregistration inside the remaining shutdown deadline. When the
  runtime advertises generation-fenced receipts, retry matching transient or
  lost responses with the original registration token. Without advertised
  fencing, use one legacy attempt. Exhausted or uncertain shutdown keeps
  registration removal unconfirmed instead of reporting success.

## [2.10.0] - 2026-10-23

### Added

- Activity and child mock failures in `WorkflowEnvironment` now reach authored
  workflow handlers through replay. Explicit retry budgets invoke each attempt
  without sleeping; per-call counts and a configurable cap keep tests bounded.
- Synchronous callable child workflow mocks alongside canned child results.
- Nexus operation mocks in `WorkflowEnvironment`, keyed by endpoint, service
  and operation. Register canned responses or synchronous callables; workflow
  code receives completion, acceptance or `NexusOperationFailed` through replay.
- Explicit verified `tls_context` configuration for async and synchronous
  clients, including cloned query clients and runtime payload transfers.
  Applications can add a private CA while retaining certifi's public roots.

### Fixed

- Name exception classes and static routes in poll warnings, including
  message-less transport failures, while preserving credential-safe summaries.
  Record local request cancellation as `CancelledError` instead of `pending`,
  including cancellation during transport retry backoff.

- Verify schema-2 history exports, including the lossless Avro memo authority
  and its JSON projection. Retain schema-1 support, reject malformed or unknown
  versions and preserve integrity-only promotion limits. Match the published
  canonical JSON encoding for Unicode and finite floats in checksums/signatures.
- Bound client metric routes to reviewed operation templates, including
  update-validation tasks, worker sessions, streams and Nexus operations.
  Unknown paths emit `unknown`; static poll routes retain `/poll`. Variable
  identifiers and query strings do not create metric series.
- Honor explicitly configured and inherited metric recorders even when they
  evaluate to false, rather than silently replacing them with the no-op.
- Back off workflow, activity and query polling independently after repeated
  HTTP or transport failures, with jitter and a 30-second cap. Successful polls
  reset the delay, and repeated authentication warnings are rate-limited.

## [2.9.1] - 2026-10-22

### Security

- Require a patched certifi trust bundle that removes the affected emSign root.
  Installing this version upgrades older certifi resolutions too. Normal TLS
  and hostname verification and explicit private certificate authorities remain
  unchanged.

## [2.9.0] - 2026-10-22

### Added

- Read-only `WorkflowContext.workflow_id` and `run_id` properties expose the
  original task-bound identity during workflow execution and cold replay.
  Continue-as-new successors keep the workflow ID and receive their own run ID.
  IDs in business arguments remain separate, including numeric strings.

## [2.8.0] - 2026-10-21

### Added

- Capability-gated activity runtime lease renewal, including automatic Worker
  maintenance during loading, callbacks and publication. Preserve the original
  task, attempt, owner and business deadlines without recording application
  progress or adding a protocol floor. Older runtimes retain their behavior.
- A bounded `Client.renew_activity_task_lease` method using the connected
  runtime's affirmative discovery and the Client's actual transport credentials.

### Fixed

- Stop settled activity maintenance independently of cancellation, including
  callbacks that catch cancellation. Late outcomes remain fenced, and a running
  synchronous callback keeps its executor slot until it physically exits.
- Stop default async and synchronous activities when a heartbeat explicitly
  refuses continued execution. Preserve the Server reason separately from
  cancellation and fence completion/failure even when user code catches the
  refusal. The worker frees its slot after the callback exits.

- Reject canonical version markers missing their change ID or supported-range
  bounds. Consistent historical duplicate ranges may still differ when each
  contains the frozen version.
- Share one frozen numeric version across `get_version`, `patched` and
  `deprecate_patch` for the same change ID, while validating each call's range
  and preserving retained duplicate-marker sequences.

## [2.7.0] - 2026-10-21

### Added

- Optional execution telemetry and standard logging correlation for workflow,
  query and activity scopes. The telemetry extra installs only the API;
  applications own providers, exporters, sampling and shutdown.
- Bounded W3C propagation gated by the Server's actual observability capability.
  Recover run parents and validated dispatch/command/outcome links only after
  original poll identities and existing authority fences. Baggage defaults
  empty and is never carried on links.
- Isolated async/thread contexts and fresh provider factories for supervised
  callbacks. Cold replay recovers the run parent without re-emitting completed
  activity attempts. Application request IDs and business values are preserved.

## [2.6.2] - 2026-10-20

### Fixed

- Preserve workflow entry argument-binding refusals before application code
  runs, with truthful Worker capability discovery for compatible deployment
  routing. Workflow body exceptions remain ordinary workflow failures.
- Run synchronous remote activities outside the asyncio event loop so other
  workflow tasks, polls and heartbeats can progress.
- Reject unsupported coroutine signal/update handlers and generator handlers
  before registration or replay, including inherited and wrapped handlers.
  Guard returned values without removing the Worker's existing asynchronous
  query and update-validator support. Local synchronous query/validation helpers
  refuse and close returned coroutines.

## [2.6.1] - 2026-10-19

### Fixed

- Raise `WorkflowAlreadyStarted` for the current Server's
  `instance_already_started` duplicate-start reason, retaining support for the
  historical `duplicate_not_allowed` reason.
- Correct start-policy documentation to `fail` and `use-existing`, including
  active versus closed instances and preservation of original input/deadlines.
  Start admission and workflow lifecycle behavior are unchanged.

## [2.6.0] - 2026-10-18

### Added

- Cooperatively cancel a completion-waiting Nexus service call at its original
  durable boundary when the runtime advertises the owned `workflow_run` binding.
  Preserve the original request, claim-release receipt and cleanup deadline.

### Fixed
- Commit workflow-authored Nexus admission with the original leased workflow
  task. Pending calls wait for durable service history, and cold replay retains
  the original call instead of invoking the operation from the Worker.
- Preserve typed terminal results, failures, external arguments and recorded
  legacy side-effect outcomes across Nexus replay.

## [2.5.1] - 2026-10-10

### Fixed
- Select the legacy patch branch when an existing unmarked history has already
  reached its next durable operation, preserving pending activities without
  scheduling them again.
- Reuse one version decision per change ID and validate each call's supported
  range. Repeated patch calls now record one marker on new histories.
- Replay consistent duplicate markers written by earlier Python SDKs without
  rewriting their histories or shifting committed cancellation boundaries.
  Conflicting decisions remain explicit replay errors.

## [2.5.0] - 2026-10-08

### Added
- Opt-in sticky execution with bounded entries, encoded history bytes and TTL.
  Retain durable history and validate prefixes before reusing Server-issued
  page cursors. Eviction, expiry, replacement and mismatches use cold replay.
- Cache counters and a runnable sticky execution example. Server 2.5.10 exposes
  the replay modes needed for warm history reuse. Workers keep the cache disabled
  unless `sticky_cache_capacity` is positive.

## [2.4.3] - 2026-10-08

### Fixed
- Preserve accepted update handler failures as `UpdateFailed`, including the
  failure message, HTTP status, Server response and workflow, run, update and
  failure identities. Existing `InvalidArgument` catches remain compatible.

## [2.4.2] - 2026-10-07

### Fixed
- Acknowledge replay waiting on an already pending timer, activity or child
  through the existing waiting-for-history protocol. Empty command lists no
  longer produce rejected workflow task completions.
- Reject message-stream completion metadata without commands before sending a
  waiting acknowledgement, preventing stream progress from being discarded.

## [2.4.1] - 2026-10-07

### Fixed
- Keep already scheduled scalar timers, remote activities and child workflows
  pending during signal wakeups and cold worker replay. Replay no longer emits
  another scheduling command for the same recorded operation, including
  shielded cancellation cleanup timers.
- Reject changed timer delays when history records the original delay, while
  preserving compatibility with older histories that omit it.

## [2.3.9] - 2026-10-04

### Fixed
- Honor the configured Client HTTP timeout for ordinary control, discovery and
  worker requests. Explicit long-poll timeouts retain their existing grace.

## [2.3.8] - 2026-10-03

### Fixed
- Preserve the configured worker poll window after Server discovery for workflow,
  activity and query tasks. HTTP grace remains separate, so short polls release
  Server admission slots when requested.

## [2.3.7] - 2026-09-29

### Fixed
- Request bounded workflow history pages in Python workers so long valid
  histories no longer require an unbounded Server poll response. A worker now
  stops rather than replaying partial history if a later page fetch fails.

## [2.3.6] - 2026-09-29

### Fixed
- Keep up to 1,024 verified external payload references in the default worker
  replay cache while retaining the 16 MiB byte limit. This avoids repeated
  downloads for histories with many distinct external payloads.

## [2.3.5] - 2026-09-27

### Fixed
- Return completed workflow results and typed failures when their terminal
  event is beyond the first Server history page. Clients and workflow handles
  can request further pages with the Server's opaque page token.

## [2.3.4] - 2026-09-26

### Fixed
- Retry workflow-task heartbeat after a compatible Server reports temporary
  database loss with an unknown outcome. The worker checks the exact task,
  lease owner, and attempt before resending the same request, including on
  path-prefixed managed runtime URLs. Server 2.4.11 adds this response; older
  Servers retain their existing behavior.

## [2.3.3] - 2026-09-26

### Fixed
- Workers recognize Server's typed long-poll capacity response for workflow,
  activity, and query polls. They release the poll reservation and wait for the
  advertised retry delay without duplicate transport retries or an error warning.
  Unrelated or malformed rate-limit responses keep their existing error path.

## [2.1.0] - 2026-09-23

### Added
- Async and sync clients and selected workflow handles can redrive a failed run
  by source run ID, with an optional idempotency request ID. This requires a
  Server that exposes the run-targeted redrive operation.

### Fixed
- Cold replay reuses completed activities copied into a redrive successor
  without scheduling them again. An uncaught recorded activity failure carries
  its failed-step identity; handled or translated failures do not claim one.

## [2.0.4] - 2026-09-08

### Fixed
- On supporting Servers, retry a draining refusal for a completion payload
  with its exact activity, workflow, or query lease and immutable payload slot.
  Client uploads, unknown capabilities, and hard storage fences remain blocked.
- Preserve computed outcomes during late upload pressure. Existing storage
  retries stay interruptible and do not execute handlers again; Server upload
  allowances, namespace quotas, and stale-lease errors remain authoritative.

## [2.0.3] - 2026-09-08

### Fixed
- Workers now retry retryable storage-admission refusals without re-executing
  handlers or changing task identities. Completed outcomes and prepared payload
  uploads survive acknowledgement retries; successful uploads are not repeated.
- Registration, polling, heartbeat, and completion retries remain interruptible
  during shutdown. Authentication and stale-lease failures remain terminal.

## [2.0.2] - 2026-09-06

### Fixed
- Persisted execution and run deadlines now raise the exported
  `WorkflowTimedOut` exception instead of returning `None` from workflow results.
  This terminal runtime outcome is distinct from the caller's polling
  `TimeoutError`; selected history and other terminal exceptions are unchanged.

## [2.0.1] - 2026-09-02

### Fixed
- Cold replay now treats an intervening durable step as the authoritative
  boundary for later signals and updates, even when they retain an older
  condition sequence. Post-condition activities can therefore complete before
  those receivers advance the workflow without rebinding them to a closed wait.

## [2.0.0] - 2026-09-01

The Python SDK `2.0.0` promotes the qualified release candidate to the stable
2.0 line. This Avro-only SDK release is supported with Server `2.0.0`; JSON
remains HTTP transport and is not a workflow payload codec.
Earlier SDK versions remain historical releases and are not alternate supported
2.0 baselines; no prerelease compatibility shim is provided.

### Added
- Worker registration now explicitly refuses local activities, worker
  sessions, and sticky execution at the portable worker-affinity protocol
  floor instead of advertising an unimplemented routing contract.
- `WorkflowContext.select()` starts independent durable operations and returns
  the first persisted winner with stable member identity. Non-winning handles
  remain available to await or cancel explicitly across replay and restart.
- `WorkflowContext.message_stream()` now consumes named repeated input in
  ordered bounded batches. Runtime-owned cursor and wait metadata survives
  replay, worker replacement, server restart, duplicates, and continue-as-new
  without exposing the reserved transport signal to workflow declarations.
- `WorkflowContext.upsert_memo()` now authors validated memo patches, replays
  `MemoUpserted` identities, carries opaque payload envelopes, and fails before
  completion when runtime capability discovery does not advertise memo updates.
- Workflow Streams are now a first-party client and workflow-authoring surface
  with list, describe, resumable at-least-once subscription, idempotent append,
  close, and error operations. Run-scoped stream items support runtime-mediated
  external payload references, and replay-safe workflow commands derive stable
  per-item idempotency keys from the durable command identity.
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
- The README and PyPI project description now require Server worker protocol
  `>=1.19,<2.0`. Package and API-reference release validation derive that
  interval from the shipped SDK protocol and reject semantic guidance drift.
- Cold replay now classifies an already-satisfied condition before binding the
  next durable history row. Signals and updates accepted across worker
  replacement can therefore advance through the transient wait without
  consuming the following activity or a later condition wait.
- Yielded continue-as-new commands now preserve pending memo and search-
  attribute upserts in the same order as returned continuations, while replayed
  metadata updates remain consumed exactly once.
- High-level workers now explicitly advertise memo-upsert and typed-search-
  attribute command support when registering with Server. Worker requests use
  protocol 1.19, while direct `Client.register_worker()` callers retain full
  control of their explicit capability list.
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
