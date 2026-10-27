# Activity

Synchronous activities execute in the worker's bounded remote thread pool by
default. They can call `context().heartbeat_sync(details)` and inspect
`context().is_cancelled`; a heartbeat reporting cancellation raises
`ActivityCancelled`. Async activities use `await context().heartbeat(details)`.
See [activity execution and shutdown](worker.md#activity-execution) for thread
capacity, cancellation and process-isolation limits.

## Original attempt and received retry policy

Requires Python SDK 2.12.0 or later. Inside an activity, capture
`info = activity.context().info`. Its `attempt_number` is the original attempt
number. `info.retry_info` is an immutable snapshot of that number and the retry
policy received with the task. `retry_info.policy()` returns a fresh copy,
including unknown policy fields. Changing that copy does not change the task
or snapshot.

| Received task metadata | `policy_provided` | `max_attempts()` | `is_unlimited()` | `is_last_configured_attempt()` |
| --- | --- | --- | --- | --- |
| No `retry_policy` field | `False` | `None` | `False` | `None` |
| Root `retry_policy: null` | `True` | `None` | `False` | `None` |
| Policy object with `max_attempts: null` | `True` | `None` | `True` | `False` |
| Policy object with positive integer `max_attempts: N` | `True` | `N` | `False` | Whether the original attempt is at least `N` |

An absent, malformed or unknown limit stays unknown. The SDK does not infer a
policy from Server defaults. A last-configured-attempt result describes the
received budget. Cancellation, deadlines, workflow closure and non-retryable
failure can still end execution before that limit.

Ordinary remote and supervised callbacks receive the same original snapshot.
Inline and prepared local callbacks have their own attempt context and do not
inherit the parent's remote retry policy.

## Signal the originating run

`info.workflow_id` and `info.run_id` are nullable, original identities from the
received task. Inline and prepared callbacks inherit only known parent
identities. Task IDs and activity-attempt IDs are never substituted for missing
workflow/run identities. A standalone or older task can have no workflow
identity; handle that case explicitly.

Call this helper from an async activity, passing an existing application
`Client` authorized to signal the run. The handle pins the original run, so a
later continue-as-new run cannot receive the signal accidentally.

```python
from durable_workflow import Client, activity


async def signal_originating_run(client: Client, business_event_id: str) -> dict:
    info = activity.context().info
    if info.workflow_id is None or info.run_id is None:
        raise RuntimeError("This activity has no known originating workflow run")
    handle = client.get_workflow_handle(info.workflow_id, run_id=info.run_id)
    return await handle.signal(
        "payment_recorded",
        [business_event_id],
        request_id=f"payment-recorded:{business_event_id}",
    )
```

Use a stable business-event identity across activity retries. An attempt number,
task ID or random value generated on every retry identifies a different intent.
Keep the event ID within the Server's command-key length limit. A stable key
does not by itself guarantee signal deduplication: the currently published
Server does not retain these keys for every signal path. Make business-event
processing idempotent and reconcile uncertain delivery rather than assuming
that one key means one recorded signal. Track Server receipt-retention support
in [the portable command-key requirement](https://github.com/durable-workflow/.github/issues/216).

## Heartbeats during one blocking call

Capture `ctx = activity.context()` inside the activity before starting a helper
thread. A newly created thread does not automatically have the activity's
context. Calling `activity.context()` there can raise "called outside of an
activity execution". Pass the captured context instead. Its
`ctx.heartbeat_sync(details)` schedules the heartbeat on the owning worker loop
with the original task, attempt and worker identity. It can be called from a
helper thread, but must never run on that worker loop. Async activity code uses
`await ctx.heartbeat(details)`.

The complete [threaded-heartbeat example](https://github.com/durable-workflow/sdk-python/blob/main/examples/threaded_heartbeats.py)
provides `call_with_heartbeats(work)`. Copy it into your application and use it
from an ordinary synchronous activity:

```python
from durable_workflow import activity
from my_app.exports import export_service
from my_app.threaded_heartbeats import call_with_heartbeats


@activity.defn(name="wait_for_export")
def wait_for_export(export_id: str) -> dict:
    return call_with_heartbeats(
        lambda cancelled: export_service.wait(
            export_id, timeout=120, cancel_event=cancelled,
        ),
        interval=5,
    )
```

`export_service` is your application's client, not a Durable Workflow API. It
must implement the finite timeout and observe `cancel_event` promptly, returning
or raising when it is set. Use the equivalent timeout and cancellation mechanism
from your own library. If it cannot cooperate, give it a finite I/O deadline;
the helper cannot force a blocked thread to exit. A worker process may need
separate isolation for a call that cannot be bounded.

The helper records the original exception, sets the cancellation event, and
stops heartbeating when cancellation, lease-authority loss or a transport error
occurs. After the work unwinds, it joins the helper and re-raises that exception
before a result can be returned. In particular, do not swallow `ActivityCancelled`
or `ActivityLeaseAuthorityLost`, replace them with an application error, or
return a successful result after either. `ActivityCancelled` inherits from
`BaseException`, so `except Exception` alone does not catch it. Ordinary return
and work exceptions also stop and join the helper.

Keep the client and its worker event loop alive until the helper has joined.
An in-flight heartbeat still needs its bounded transport request to finish.
Use finite client timeouts and bounded retry policy as well as a deadline on
the business operation. A helper reports liveness even if that operation hangs,
so its heartbeat is not proof of progress. Choose the interval using the
configured heartbeat timeout and leave room for request, scheduling and retry
delays. Five seconds is an example, not a guaranteed safe cadence.

::: durable_workflow.activity
