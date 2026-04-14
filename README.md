# durable-workflow (Python SDK)

A Python SDK for the [Durable Workflow server](https://github.com/durable-workflow/server). Speaks the server's language-neutral HTTP/JSON worker protocol — no PHP runtime required.

Status: **MVP / experimental**. Supports starting workflows, registering a worker, polling workflow + activity tasks, and completing them with `schedule_activity` and `complete_workflow` commands. Signals/queries/updates/timers/child workflows are not yet implemented.

## Install

```
pip install -e .
```

## Quickstart

```python
from durable_workflow import Client, Worker, workflow, activity

@activity.defn(name="greet")
def greet(name: str) -> str:
    return f"hello, {name}"

@workflow.defn(name="greeter")
class GreeterWorkflow:
    def run(self, ctx, name):
        result = yield ctx.schedule_activity("greet", [name])
        return result

async def main():
    client = Client("http://server:8080", token="dev-token-123", namespace="default")
    worker = Worker(
        client,
        task_queue="python-workers",
        workflows=[GreeterWorkflow],
        activities=[greet],
    )
    handle = await client.start_workflow(
        workflow_type="greeter",
        workflow_id="greet-1",
        task_queue="python-workers",
        input=["world"],
    )
    await worker.run_until(workflow_id="greet-1")
    print(await client.get_result(handle))
```

## Protocol

The SDK implements [the server's HTTP protocol](https://github.com/durable-workflow/server#getting-started-end-to-end-workflow) directly using `httpx`. No gRPC, no protobuf, no PHP.

Known caveats (tracked upstream): the server currently PHP-`serialize()`s workflow start inputs and activity arguments. See [issues].
