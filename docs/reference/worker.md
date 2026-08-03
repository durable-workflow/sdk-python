# Worker

## Lifecycle and shutdown

`Worker.run()` and `Worker.run_until()` register through the worker protocol
before polling. After registration succeeds, shutdown stops the pollers, waits
up to `shutdown_timeout` for in-flight tasks, and then removes that worker-plane
registration. Calling `stop()` more than once is safe and sends at most one
deregistration request. No deregistration is sent if registration failed.

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
