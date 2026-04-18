# Order Processing Example

This sample runs a Python-authored workflow against a local Durable Workflow
server. It is intentionally larger than the greeter example: one workflow
coordinates inventory reservation, payment authorization, shipment creation,
and customer confirmation activities.

## Run

From this directory:

```bash
docker compose up --build --exit-code-from python-worker python-worker
docker compose down -v
```

The `python-worker` service starts a workflow, runs the Python worker until that
workflow reaches a terminal state, prints the completed workflow result as JSON,
and exits with a non-zero code if the workflow fails.

## Configuration

The Compose file uses `durableworkflow/server:0.2.2` by default. Override it
when testing a specific server release:

```bash
SERVER_IMAGE=durableworkflow/server:0.1.9 docker compose up --build --exit-code-from python-worker python-worker
```

Useful environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `SERVER_IMAGE` | `durableworkflow/server:0.2.2` | Server image used by bootstrap, API, and queue worker services |
| `SERVER_PORT` | `8080` | Host port for the server API |
| `WORKFLOW_SERVER_AUTH_TOKEN` | `sample-token` | Token accepted by the local server and sent by the Python SDK |
| `ORDER_ID` | generated | Order id used as the workflow id |
