# Activity

Synchronous activities execute in the worker's bounded remote thread pool by
default. They can call `context().heartbeat_sync(details)` and inspect
`context().is_cancelled`; a heartbeat reporting cancellation raises
`ActivityCancelled`. Async activities use `await context().heartbeat(details)`.
See [activity execution and shutdown](worker.md#activity-execution) for thread
capacity, cancellation and process-isolation limits.

::: durable_workflow.activity
