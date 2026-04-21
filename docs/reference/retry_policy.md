# Retry policy

`TransportRetryPolicy` is the client HTTP retry policy. It retries transient
network/request failures while talking to the Durable Workflow server; it does
not retry workflow runs, workflow tasks, activities, child workflows, or user
code.

Durable retry budgets live on workflow commands instead:

- `ActivityRetryPolicy` with `ctx.schedule_activity(...)` controls attempts for
  one activity execution.
- `ChildWorkflowRetryPolicy` with `ctx.start_child_workflow(...)` controls
  attempts for one child workflow execution.
- Activity timeout fields and child workflow timeout fields are server-side
  durable budgets, not SDK HTTP request timeouts.

::: durable_workflow.retry_policy
