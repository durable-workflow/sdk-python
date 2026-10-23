# Worker

See [Separate workflow and activity processes](../guides/separate-workers.md)
for a runnable deployment example with distinct registrations, independent
slots, remote activity retries and signal-driven shutdown.

## Activity execution

By default, ordinary synchronous `def` remote activities run in a dedicated
thread pool bounded by `max_concurrent_activity_tasks`. Each thread receives
its own `activity.context()`. A blocked synchronous call does not occupy the
worker event loop, so other activity slots, workflow tasks and worker
heartbeats can progress. Async `async def` activities run on that event loop
and must use nonblocking I/O. Blocking work inside an async function still
stalls the loop.

From a synchronous activity, call
`activity.context().heartbeat_sync(details)` to report progress with the
original task, attempt and worker identity. It waits for the owner's event
loop to send the request using the normal client transport timeouts and
raises `ActivityCancelled` when cancellation is reported. Async activities use
`await activity.context().heartbeat(details)`. Calling the synchronous method
on the owner event loop raises an error instead of deadlocking it.

Threads suit blocking I/O; they do not provide hard isolation or parallel
Python CPU execution. Use bounded downstream timeouts and cooperative
heartbeat checks. For CPU-heavy or untrusted callbacks, use separately
isolated worker processes and their resource limits. The separately
negotiated cooperative-cancellation capability retains its supervised process
execution; selecting a default thread does not enable that protocol.

## Bounded failure reports

This behavior is available in unreleased SDK source. Published packages still
need the compatible release before these limits apply.

Failure reports retain at most 62 KiB of UTF-8 message text, 128 KiB of stack
text and 64 KiB of serialized details and diagnostics. These budgets include
truncation markers. The complete encoded JSON request body defaults to at most
1 MiB, counting JSON escaping, claim identities, command batches and metadata.
The message budget reserves 2 KiB below the original 64 KiB ceiling for
runtime-derived summaries.
A lower discovered Server `limits.max_payload_bytes` also applies. Configure
`Client(..., failure_report_limit_bytes=...)` when a proxy or transport has a
lower request limit. This option can lower the default cap; it cannot raise it.
Query-client clones inherit it. Payload warning thresholds remain advisory.

Oversized diagnostics produce a visible `[truncated ...]` message marker with
structured omitted-field names and original byte sizes. Activity failures also
carry this information in `failure.diagnostics.failure_report`; terminal
workflow failures use `exception.diagnostics.failure_report`. Other existing
report shapes keep the diagnostic JSON in the message marker. Unknown sizes
are `null` when optional details could not be encoded. Avro details are kept as
an intact valid envelope or omitted whole, before an unnecessary payload upload.

The original failure type, retry/non-retryable choice, authored retry delay and
claim identity remain exact. Activity, workflow-task, query and update rejection
reports, terminal workflow/update failures and local-activity attempt reports
share the complete-request bound. A workflow-task replay failure still leaves
the run open for repair; it does not become a terminal workflow failure.

A local activity's failure is bounded before workflow code observes it. First
execution and cold replay therefore see the same persisted message. If a later
command batch or newly discovered lower limit cannot fit that observed value,
reporting fails explicitly rather than changing a value already used by the
workflow. Prepared local activities observe the runtime's canonical history.

If essential identity or semantic fields cannot fit even without optional
text, the client raises `FailureReportTooLarge` before sending the report.
This reporting refusal is separate from the application failure. The worker's
existing reporting-error log identifies it; shortening a task ID or replacing
the application type would change which failure the runtime records.

## Poll failures and recovery

This backoff requires Python SDK 2.10.0 or later. Earlier packages use a fixed
one-second poll-error wait.

Workflow, activity and query pollers back off independently after failed polls,
including HTTP errors, rejected credentials and unreachable runtimes. The
retry ceiling starts at 1 second, doubles on each consecutive failure and stops
at 30 seconds. Each delay is chosen randomly between half and all of that
ceiling, so workers do not retry in lockstep. Any successful poll, including an
empty response, resets the ceiling to 1 second. Shutdown interrupts the wait.

The first failure is logged immediately. An unchanged failure is logged at most
once per minute per poller until polling succeeds. HTTP 401 identifies rejected
authentication; HTTP 403 identifies denied authorization. Check the worker
credentials and their permissions. Once requests succeed again, polling
recovers without a worker restart. Poll-error metrics
still count every failed poll, including failures whose warning is suppressed.

Starting with Python SDK 2.10.0, warnings name the exception class and static
poll route even when an httpx exception has an empty message. Transport exception
messages are omitted because they can contain private request details. Server
refusals retain their status and bounded reason; authentication failures retain
their credential-safe summary. See [request outcomes and errors](metrics.md#request-outcomes-and-errors)
for local cancellation and transport retry behavior.

Typed capacity backpressure follows the Server's advertised retry delay.
Storage-admission failures retain their separate identity-preserving retry and
worker-stop behavior. Neither uses this generic poll-failure backoff.

## Lifecycle and shutdown

`Worker.run()` and `Worker.run_until()` register through the worker protocol
before polling. After registration succeeds, shutdown stops every poller and
waits for accepted work within the shared `shutdown_timeout` deadline before
removing that worker-plane registration. Work still running at the deadline is
cancelled. Calling `stop()` more than once shares the same shutdown operation
and result. No deregistration is sent if registration failed.

**Unreleased source behavior, prepared for SDK 2.10.1:** the final HTTP call and any recovery use the
original remaining shutdown budget. If Server advertises
`worker_deregistration_fencing`, Worker retains its registration token and uses
the fenced receipt endpoint. Matching transient responses and transport losses
can retry that same generation. A completed receipt can replay safely even if
a replacement now owns the worker ID. Authentication, malformed or mismatched
receipts, lost authority and expired receipts fail without retrying. Without
advertised fencing, Worker keeps one attempt through the legacy worker-plane
DELETE and does not replay an uncertain result. That legacy route has no
generation fence. Keep worker IDs unique to a process, and do not reuse an ID
until the prior worker has fully stopped.

If drain consumes the deadline, no late deregistration request is sent. A final
HTTP timeout or exhausted recovery raises `RuntimeError` and leaves removal
unconfirmed. The Server might have committed a request whose response was lost;
an exception is not proof that the registration still exists. Inspect the
runtime before taking an operator recovery action. These changes are not in a
published package yet.

Cancelling an await cannot stop a running synchronous activity thread. Its
physical slot remains occupied until the callback returns, and a late return
does not submit a result after its await was cancelled. If the callback has
not stopped by the shared shutdown deadline, shutdown raises `RuntimeError`
and retains the registration. Python can keep the process alive while that
thread remains running. Configure callback I/O timeouts and a supervisor
termination policy; a timeout is not proof that an external side effect was
cancelled or that the worker stopped gracefully.

An external `stop()` also interrupts `run_until()`; the pending `run_until()`
call ends with `asyncio.CancelledError` after its accepted workflow or activity
work has drained or been cancelled. If an async poller, the query-poller thread,
or accepted work cannot be stopped, `stop()` raises `RuntimeError` and leaves
the registration active. Treat that as an incomplete shutdown requiring
operator attention rather than continuing as though the worker stopped cleanly.

For a long-running process, coordinate shutdown with the same worker instance
and await both `stop()` and the run task:

```python
run_task = asyncio.create_task(worker.run())

try:
    await shutdown_requested.wait()
finally:
    try:
        await worker.stop()
    finally:
        await run_task
```

Deregistration authentication, protocol, and HTTP failures are raised from
`stop()` and `run()`; a process must not treat those failures as a clean
shutdown. When a worker-loop error and deregistration both fail, the loop error
remains the raised exception and the deregistration error is available through
its `__cause__`.

The worker-plane cleanup operation is separate from
`Client.deregister_worker()`. That existing control-plane method remains an
operator management action for retiring or recovering worker records and is not
used by normal `Worker` shutdown.

`run_until(timeout=...)` includes Server discovery and registration in its
workflow wait budget. A timeout before startup finishes identifies the worker
and workflow in the error. A timeout after startup reports that the workflow
has not reached a terminal state. Neither timeout cancels the durable run.
A replacement worker can continue that same run.

When `stop()` is called during startup, it waits for registration to finish
before starting the shared drain deadline. Server can record a registration
before its reply reaches the SDK. A worker that never receives that reply has
not begun polling and becomes stale under the Server's heartbeat policy.
Use the existing workflow handle and queue when bringing up a replacement.

::: durable_workflow.worker
