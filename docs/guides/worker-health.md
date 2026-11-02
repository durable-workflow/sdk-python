# Worker readiness and liveness

`Worker.status()` returns a read-only `WorkerStatus` snapshot for an
application health endpoint or an orchestrator probe. It reports this worker's
own registration and local progress. A healthy Server, or another worker on the
same queue, does not make this worker ready.

This API is in unreleased SDK source. Published packages need the release
that includes it.

## Ready versus live

| | Ready | Live |
|---|---|---|
| Question | Should this worker count as serving its queue? | Is this worker's local machinery progressing? |
| True when | Registration is acknowledged for its namespace and queue, every configured poller is running, the latest Server exchange of every role succeeded, and no request is overdue | The event loop advanced recently, no configured poller has exited, and no Server request has outlived its stall bound |
| Transient Server or network outage | `False` (`server_unavailable:<role>`) | `True` |
| Use it for | Readiness probes, load-balancer membership, rollout gates | Liveness probes that restart a stuck process |

The roles are `heartbeat`, plus `workflow_poll`, `activity_poll` and
`query_poll` when the worker registers workflows, activities or Server query
tasks. Readiness returns without a restart on the next successful exchange of
each failed role. A failed heartbeat stays unready until a later heartbeat
succeeds, so recovery can take up to one heartbeat interval.

An idle long poll is healthy. Two bounds apply to an outstanding poll or
heartbeat:

- `request_overdue_seconds` (readiness) defaults to one HTTP attempt deadline,
  the larger of the client request timeout and the poll's absolute HTTP
  deadline. A request unanswered for longer reports `server_unavailable` even
  while the client is still retrying it, because the Server did not answer in
  time.
- `request_stall_seconds` (liveness) defaults to twice the client's complete
  transport retry budget for one request. Only a request stuck beyond any
  legitimate retry window marks the worker not live, so a Server outage alone
  does not trigger a liveness restart.

Long-running activity handlers never affect either signal.
Poll-capacity backpressure is a Server answer for an admitted worker and keeps
the worker ready.

## States

| `state` | `ready` | `live` | Meaning |
|---|---|---|---|
| `starting` | `False` (`not_started` or `registering`) | `False` before `run()`, then `True` | Constructed, or waiting for the registration acknowledgement |
| `registered` | per the rules above | per the rules above | Polling and heartbeating |
| `draining` | `False` (`draining`) | `True` while the loop progresses | `stop()` was requested; in-flight work is draining |
| `stopped` | `False` | `False` | Shutdown finished and the registration was removed |
| `failed` | `False` | `False` (`failed:registration`, `failed:runtime` or `failed:shutdown`) | Registration was refused, a poller failed, or shutdown could not confirm cleanup |

A dead query-poller thread, a stalled event loop, a registration refusal and a
completed shutdown never report ready.

## Checking a stalled event loop

`status()` performs no network I/O, claims no tasks, renews no leases and does
not wait on the event loop. While the worker runs, a small local task advances a
progress tick once per second. Serve probes from a separate thread, as in the
example below, and the probe can answer even when the event loop is blocked:
`live` turns `False` with `event_loop_stalled` once the tick is older than
`event_loop_stall_seconds` (default 30). A probe handler that runs on the
worker's own event loop cannot report its own stall.

```python
status = worker.status()
status.ready, status.ready_reason   # (True, "ready")
status.live, status.live_reason     # (True, "live")
status.as_dict()                    # JSON-serializable; identity and progress only
```

The snapshot contains the worker ID, namespace, task queue, state, reasons,
ages in seconds, and per-role progress. It never contains payloads, task
arguments, exception messages (only exception class names) or credentials.

## Kubernetes example

`examples/worker_health_probe.py` runs a worker with a stdlib HTTP server on its
own thread, serving `/readyz`, `/livez` and `/status`. Each answers 200 or 503
with the snapshot as JSON. Bind it to the pod network only.

```yaml
containers:
  - name: worker
    image: example/python-worker:1.0.0
    env:
      - name: DW_HEALTH_HOST
        value: "0.0.0.0"
    ports:
      - name: health
        containerPort: 8081
    startupProbe:
      httpGet: {path: /readyz, port: health}
      periodSeconds: 2
      failureThreshold: 60
    readinessProbe:
      httpGet: {path: /readyz, port: health}
      periodSeconds: 5
      failureThreshold: 2
    livenessProbe:
      httpGet: {path: /livez, port: health}
      periodSeconds: 10
      failureThreshold: 6
terminationGracePeriodSeconds: 45
```

Give the liveness probe a failure window longer than ordinary event-loop pauses,
such as a large replay. Restarting a worker that is only unready because the
Server is unreachable does not help, so don't probe liveness with `/readyz`.

## Shutdown

Three different timers apply. Keep them distinct:

- `Worker(shutdown_timeout=...)` is how long `stop()` drains in-flight work
  before cancelling it and removing the registration.
- The orchestrator's stop grace (`terminationGracePeriodSeconds`) is how long
  the process may run after SIGTERM before it is killed. Make it longer than
  `shutdown_timeout`, plus time for deregistration.
- Handler termination is separate again. Cancelling an await cannot stop a
  running synchronous activity thread; if one is still running at the
  deadline, `stop()` raises and the status becomes `failed:shutdown`.

On SIGTERM, call `worker.stop()` and await the run task. While draining, the
worker reports `draining` (not ready, still live), so traffic moves away
without a liveness restart interrupting the drain. See
[Lifecycle and shutdown](../reference/worker.md#lifecycle-and-shutdown).
