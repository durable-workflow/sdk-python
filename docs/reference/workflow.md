# Workflow

Workflow retry and timeout settings are durable command budgets. Use
`ActivityRetryPolicy` on `ctx.schedule_activity(...)` for activity attempts and
`ChildWorkflowRetryPolicy` on `ctx.start_child_workflow(...)` for child workflow
attempts. Use `TransportRetryPolicy` only for client HTTP retries.

::: durable_workflow.workflow
