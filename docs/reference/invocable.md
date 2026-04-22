# Invocable Carriers

`InvocableActivityHandler` is the reference adapter for activity-grade external
execution. It consumes the stable external-task input envelope, rejects
workflow-task inputs, dispatches a registered activity handler, and returns the
stable external-task result envelope.

Use it for HTTP, serverless, and other bounded carriers that should execute
activities without becoming workflow runtimes. Operators and agents can inspect
the same result contract as poll-based workers: success, retryability,
application failures, cancellations, decode failures, unsupported payloads, and
deadline-exceeded failures all stay in the structured result envelope.

```python
from durable_workflow import InvocableActivityHandler


async def handle_request(request_json: dict) -> dict:
    adapter = InvocableActivityHandler({"billing.charge-card": charge_card})
    return await adapter.handle(request_json)
```

::: durable_workflow.invocable
