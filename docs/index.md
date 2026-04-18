# Durable Workflow Python SDK

API reference for [`durable-workflow`](https://pypi.org/project/durable-workflow/), the Python SDK for the Durable Workflow server. For conceptual documentation, tutorials, and the overall architecture, see the [main docs site](https://durable-workflow.com/docs/2.0/polyglot/python).

## Install

```bash
pip install durable-workflow
```

With the optional Prometheus metrics recorder:

```bash
pip install 'durable-workflow[prometheus]'
```

## Quick links

- **[Client](reference/client.md)** — start workflows, signal, query, update, wait for results, manage schedules.
- **[Worker](reference/worker.md)** — poll the server for workflow and activity tasks, dispatch to registered handlers.
- **[Workflow](reference/workflow.md)** — workflow-side primitives: `ActivityRetryPolicy`, `ContinueAsNew`, `StartChildWorkflow`, and the workflow decorator.
- **[Activity](reference/activity.md)** — activity decorator and execution context.
- **[Errors](reference/errors.md)** — typed exceptions raised by the client and worker.
- **[Retry policy](reference/retry_policy.md)** — HTTP transport retry configuration for the client.
- **[Metrics](reference/metrics.md)** — pluggable recorders, including a Prometheus adapter.
- **[Serializer](reference/serializer.md)** — payload encoding and decoding helpers.
- **[Sync helpers](reference/sync.md)** — blocking wrappers around the async client for scripts and tests.

## Versioning

This reference is generated from the `main` branch of [`durable-workflow/sdk-python`](https://github.com/durable-workflow/sdk-python) on every push. For the version installed in your project, check `durable_workflow.__version__`.
