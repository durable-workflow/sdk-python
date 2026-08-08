# Worker

## Lifecycle and shutdown

`Worker.run()` and `Worker.run_until()` register through the worker protocol
before polling. After registration succeeds, shutdown stops every poller and
waits for accepted work within the shared `shutdown_timeout` deadline before
removing that worker-plane registration. Work still running at the deadline is
cancelled. Calling `stop()` more than once is safe and sends at most one
deregistration request. No deregistration is sent if registration failed.

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

::: durable_workflow.worker
