# Workflow

Workflow retry and timeout settings are durable command budgets. Use
`ActivityRetryPolicy` on `ctx.schedule_activity(...)` for activity attempts and
`ChildWorkflowRetryPolicy` on `ctx.start_child_workflow(...)` for child workflow
attempts. Use `TransportRetryPolicy` only for client HTTP retries.

Use `ctx.call_nexus_service(...)` from workflow code to start a Nexus service
operation durably. The Python worker records the accepted response or typed
service failure as a side-effect marker, then replay resumes with
`NexusOperationResult` or raises `NexusOperationFailed` at the yield point.

::: durable_workflow.workflow
