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

For HTTP endpoints or API Gateway/Lambda style handlers, use the packaged
adapters so content type, body parsing, and result serialization stay aligned
with the carrier contract:

```python
from durable_workflow import handle_invocable_http_request, lambda_invocable_activity_handler


async def handle_http(body: bytes):
    return await handle_invocable_http_request(body, {"billing.charge-card": charge_card})


lambda_handler = lambda_invocable_activity_handler({"billing.charge-card": charge_card})
```

`handle_invocable_http_request` returns an `InvocableHttpResponse` with
`status_code`, `headers`, and `body`. On success (HTTP 200) the body is the
external-task result envelope with `Content-Type:
application/vnd.durable-workflow.external-task-result+json`. On a bad or
unparseable request body it returns HTTP 400 with a JSON error object — no
durable task identity is available in that case so no structured result envelope
can be built.

`lambda_invocable_activity_handler` wraps the async helper in a synchronous
AWS Lambda / API Gateway handler. It decodes base64 bodies when
`isBase64Encoded` is `true` so it works with both REST API and HTTP API
integrations without additional configuration.

::: durable_workflow.invocable
