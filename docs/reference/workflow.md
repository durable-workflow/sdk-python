# Workflow

Workflow retry and timeout settings are durable command budgets. Use
`ActivityRetryPolicy` on `ctx.schedule_activity(...)` for activity attempts and
`ChildWorkflowRetryPolicy` on `ctx.start_child_workflow(...)` for child workflow
attempts. Use `TransportRetryPolicy` only for client HTTP retries.

Use `ctx.call_nexus_service(...)` from workflow code to start a Nexus service
operation durably. The Python worker records the accepted response or typed
service failure as a side-effect marker, then replay resumes with
`NexusOperationResult` or raises `NexusOperationFailed` at the yield point.

Yield a list for deterministic parallel composition. Lists may nest and may
mix activity, child-workflow, and timer commands; the resolved value has the
same nested shape and input order. Every leaf emits the shared
`parallel_group_*` metadata and uses its ordinary Server command.

Use `ctx.saga().run(forward)` for sequential reverse-order compensation.
Register each compensation only after its forward activity completes. The
helper compensates on failure or cooperative cancellation and raises
`SagaCompensationFailed` if compensation itself fails.

::: durable_workflow.workflow
