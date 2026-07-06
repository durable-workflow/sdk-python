# Client

`Client.execute_nexus_operation(...)` calls a registered Nexus service
operation through the service-catalog API and returns `NexusOperationResult`.
Workflow authors should normally yield `ctx.call_nexus_service(...)`; the
worker uses this client method to execute once and record the durable result
or typed service failure.

::: durable_workflow.client
