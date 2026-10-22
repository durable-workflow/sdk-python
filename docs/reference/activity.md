# Activity

Synchronous activities execute in the worker's bounded remote thread pool by
default. They can call `context().heartbeat_sync(details)` and inspect
`context().is_cancelled`; a heartbeat reporting cancellation raises
`ActivityCancelled`. Async activities use `await context().heartbeat(details)`.
See [activity execution and shutdown](worker.md#activity-execution) for thread
capacity, cancellation and process-isolation limits.

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
