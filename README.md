# durable-workflow (Python SDK)

A Python SDK for the [Durable Workflow server](https://github.com/durable-workflow/server). Speaks the server's language-neutral HTTP/JSON worker protocol — no PHP runtime required.

Status: **Alpha**. Core features implemented: workflows, activities, schedules, signals, queries, updates, timers, child workflows, continue-as-new, side effects, and version markers. Full language-neutral protocol support for cross-PHP/Python orchestration.

## Install

```bash
pip install durable-workflow
```

Or for development:

```bash
pip install -e '.[dev]'
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
    result = await client.get_result(handle)
    print(result)  # "hello, world"
```

## Features

- **Async-first**: Built on `httpx` and `asyncio`
- **Type-safe**: Full type hints, passes `mypy --strict`
- **Polyglot**: Works alongside PHP workers on the same task queue
- **HTTP/JSON protocol**: No gRPC, no protobuf dependencies
- **Codec envelopes**: Proper `{codec: "json", blob: "..."}` serialization for cross-language workflows

## Documentation

Full documentation is available at [durable-workflow.github.io/docs/2.0/sdks/python/](https://durable-workflow.github.io/docs/2.0/sdks/python/):

- [Quickstart](https://durable-workflow.github.io/docs/2.0/sdks/python/quickstart)
- [Client API](https://durable-workflow.github.io/docs/2.0/sdks/python/client)
- [Workflow Authoring](https://durable-workflow.github.io/docs/2.0/sdks/python/workflows)
- [Activity Authoring](https://durable-workflow.github.io/docs/2.0/sdks/python/activities)
- [Worker Configuration](https://durable-workflow.github.io/docs/2.0/sdks/python/workers)
- [Error Handling](https://durable-workflow.github.io/docs/2.0/sdks/python/errors)
- [Schedules (Cron)](https://durable-workflow.github.io/docs/2.0/sdks/python/schedules)

## Requirements

- Python ≥ 3.10
- A running [Durable Workflow server](https://github.com/durable-workflow/server)

## Development

```bash
# Install dev dependencies
pip install -e '.[dev]'

# Run tests
pytest

# Run integration tests (requires Docker)
pytest -m integration

# Type check
mypy src/durable_workflow/

# Lint
ruff check src/ tests/
```

## License

MIT
