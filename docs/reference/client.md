# Client

`Client.execute_nexus_operation(...)` calls a registered Nexus service
operation through the service-catalog API and returns `NexusOperationResult`.
Workflow authors should normally yield `ctx.call_nexus_service(...)`; the
worker submits a leased service command and resumes from durable service
history. The client method is available for calls outside workflow execution.

::: durable_workflow.client
