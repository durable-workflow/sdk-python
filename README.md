# Durable Workflow Python SDK

[![CI](https://github.com/durable-workflow/sdk-python/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/durable-workflow/sdk-python/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/durable-workflow.svg)](https://pypi.org/project/durable-workflow/)
[![Python](https://img.shields.io/pypi/pyversions/durable-workflow.svg)](https://pypi.org/project/durable-workflow/)
[![License](https://img.shields.io/github/license/durable-workflow/sdk-python.svg)](LICENSE)

Build durable Python workflows and activities against [Durable Workflow
Cloud](https://cloud.durable-workflow.com/) or a
[self-hosted Server](https://github.com/durable-workflow/server). The SDK uses
the same language-neutral runtime protocol as the first-party PHP and Rust
SDKs.

## Install

```bash
pip install durable-workflow
```

Python 3.10 or newer is required.

## Quickstart

```python
import asyncio
from uuid import uuid4

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
    workflow_id = f"greet-{uuid4().hex}"
    async with Client(
        "http://server:8080",
        token="dev-token-123",
        namespace="default",
    ) as client:
        worker = Worker(
            client,
            task_queue="python-workers",
            workflows=[GreeterWorkflow],
            activities=[greet],
        )
        handle = await client.start_workflow(
            workflow_type="greeter",
            workflow_id=workflow_id,
            task_queue="python-workers",
            input=["world"],
        )
        await worker.run_until(workflow_id=workflow_id, timeout=30.0)
        result = await client.get_result(handle)
        print(result)  # "hello, world"

if __name__ == "__main__":
    asyncio.run(main())
```

Pass the Server origin to `Client` without a trailing `/api`. For Cloud, pass
the complete namespace runtime URL exactly as provisioned. Cloud client and
worker processes use separate runtime credentials:

```python
client = Client(
    runtime_url,
    control_token=client_token,
    worker_token=worker_token,
    namespace=namespace,
)
```

Keep the client token in application processes and the worker token in worker
processes when deploying them separately.

## Capabilities

- Workflows, activities, child workflows, timers, and continue-as-new
- Signals, queries, validated updates, schedules, and message streams
- Activity retries, timeouts, cancellation, and heartbeats
- Deterministic parallel work, side effects, version markers, and sagas
- Replay verification and an in-process workflow test environment
- Avro payloads, external payload storage, metrics, and interceptors

See the [capability matrix](https://durable-workflow.com/docs/2.0/capabilities/)
for the complete cross-SDK contract.

## Documentation

- [Python SDK portal and API reference](https://python.durable-workflow.com/)
- [Python SDK guide](https://durable-workflow.com/docs/2.0/polyglot/python/)
- [Complete SDK reference](docs/sdk-reference.md)
- [Runnable examples](examples/)
- [Symmetric SDK playground](https://github.com/durable-workflow/sample-app#symmetric-sdk-playground)

## Runtime choices

Use [Durable Workflow Cloud](https://cloud.durable-workflow.com/early-access)
for a managed namespace, or run the published
[`durableworkflow/server`](https://hub.docker.com/r/durableworkflow/server)
image yourself. Workflow and activity type names, task queues, and payloads are
portable between both runtime choices.

## Compatibility

Stable `2.x` SDK releases follow semantic versioning and negotiate runtime
capabilities with Server at startup. Use stable `2.x` SDK and Server channels
for new applications. The [compatibility guide](https://durable-workflow.com/docs/2.0/compatibility/)
documents protocol and upgrade guarantees.

## Development

```bash
pip install -e '.[dev]'
ruff check src/ tests/
mypy src/durable_workflow/
pytest tests/ -m "not integration"
```

Integration tests use Docker:

```bash
docker compose -f docker-compose.test.yml up -d --build --wait
pytest tests/integration/ -v
docker compose -f docker-compose.test.yml down -v
```

## License

[MIT](LICENSE)
