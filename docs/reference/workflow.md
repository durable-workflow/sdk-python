# Workflow

## Workflow and run identity

Workflow code reads its task-bound identity through the read-only
`ctx.workflow_id` and `ctx.run_id` properties. The values remain the same when
that run suspends and replays. After continue-as-new, the successor retains the
workflow ID and receives a new run ID; passing an older ID as workflow input
does not change the context identity.

Use these values to correlate an activity or log entry with its workflow and
run. To inspect status from outside workflow code, use
[`Client.describe_workflow()`](client.md) or a workflow query instead of an
activity that repeatedly polls its own run.

## Signal, update and query handlers

Handlers decorated with `@workflow.signal` or `@workflow.update` must be
synchronous functions. Coroutine
(`async def`), async-generator and generator handlers are rejected with
`InvalidWorkflowDefinition` when the workflow is defined, registered or replayed,
including inherited handlers. Returning an asynchronous or generator value from
a synchronous wrapper is also refused.

Queries and update validators may be synchronous or `async def`. The Worker
awaits their results; queries retain their original deadline and cancellation
authority. The local synchronous `query_state()` and `validate_update()` helpers
refuse asynchronous results with `QueryFailed` and `UpdateRejected` respectively
and close returned coroutines. Generator and async-generator query or validator
handlers are rejected at definition or registration.

Each replay constructs the workflow instance before executing `run()`. Signals
already present at workflow start are applied after `run()` reaches its first
yield, so constructor and pre-yield initialization happen before those handlers.
Signal and update handlers mutate that replayed state; queries inspect it and
update validators check a request before acceptance. Put durable activities,
timers and child workflow commands in `run()`, not in a handler.

An exception from a signal handler while replaying fails the workflow run.
Update-handler exceptions fail the update, validator exceptions reject it,
and query-handler exceptions fail the query. They do not silently succeed.

Workflow retry and timeout settings are durable command budgets. Use
`ActivityRetryPolicy` on `ctx.schedule_activity(...)` for activity attempts and
`ChildWorkflowRetryPolicy` on `ctx.start_child_workflow(...)` for child workflow
attempts. Use `TransportRetryPolicy` only for client HTTP retries.

Use `ctx.call_nexus_service(...)` from workflow code to start a Nexus service
operation durably. Admission uses the leased workflow command. Synchronous
completion waits remain suspended until a durable terminal service event,
then replay returns `NexusOperationResult` or raises `NexusOperationFailed`
at the yield point. Use `ctx.start_nexus_operation(...)` to return after
acceptance. Older recorded Nexus side-effect results still replay.

Yield a list for deterministic parallel composition. Lists may nest and may
mix activity, child-workflow, and timer commands; the resolved value has the
same nested shape and input order. Every leaf emits the shared
`parallel_group_*` metadata and uses its ordinary Server command.

Use `ctx.saga().run(forward)` for sequential reverse-order compensation.
Register each compensation only after its forward activity completes. The
helper compensates on failure and raises
`SagaCompensationFailed` if compensation itself fails.
Terminal `Client.cancel_workflow` does not resume workflow code to run it.

::: durable_workflow.workflow
