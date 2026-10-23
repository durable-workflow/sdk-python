# Metrics

## Recorder contract

The expanded route normalization described here requires Python SDK 2.10.0
or later.

Client request, worker poll and task dispatch observations follow the
[shared SDK metrics contract](https://github.com/durable-workflow/.github/tree/main/sdk-metrics).
It defines six recorder names, counter or histogram instruments, count or
second units, and the exact tag keys. The default recorder drops observations.
A worker inherits its client's recorder unless given an explicit recorder.
Selection uses `None` to mean unspecified; an empty or false-valued recorder
still receives observations. Pass `NoopMetrics()` explicitly to discard them.

Each instrumented operation increments its counter by one and records one
nonnegative monotonic duration in seconds. Client observations include the
request's transport retries; they do not count each HTTP attempt. Poll and task
observations describe local worker activity, not durable execution counts,
workflow completion, billing units or schedule-to-start latency.

Client route tags use reviewed path templates, without `/api`, query strings,
or variable IDs. For example, update-validation results use
`/worker/update-validation-tasks/{task_id}/approve`, and session renewal uses
`/worker/sessions/{session_id}/heartbeat`. Static poll operations retain their
`/poll` route. Unknown operations emit the single `unknown` route value.
Names of workflows, runs, activities, workers, sessions, queues, streams and
service operations in paths are replaced with placeholders. Request payloads,
credentials and exception messages are not core metric labels.

The worker's configured `task_queue` label remains application-controlled.
Use bounded queue names rather than one per run. Exporter resource labels need
their own cardinality review. Missing observations do not establish zero work
or a healthy process; abrupt shutdown can prevent the final observation.

`PrometheusMetrics` uses normal Prometheus exporter conventions: counter series
end in `_total`, and duration histograms expose bucket, sum and count series.
Those suffixes do not change the recorder names. Install the `prometheus` extra
to use this adapter. Applications own scrape/export configuration and additional
resource labels. Custom recorders should remain fast and handle their exporter
failures without throwing into the client or worker.

## Request outcomes and errors

Named poll warnings and the local cancellation outcome below require Python
SDK 2.10.0 or later. Earlier packages may record a locally cancelled request
as `pending`.

The client counter and duration share one terminal `outcome` per logical
request, including its transport retries:

| Outcome | Meaning |
| --- | --- |
| `ok` | The client accepted the response. |
| `http_error` | An HTTP error response was surfaced through the SDK error mapping. |
| `bridge_rejected` | A webhook bridge returned its machine-readable HTTP 422 rejection. |
| Exception class, such as `ReadTimeout`, `ReadError` or `RemoteProtocolError` | The operation ended with that exception. |
| `CancelledError` | The Python request await was locally cancelled, including during retry backoff. |

`status_code=none` means the client did not capture a terminal HTTP status.
Exception messages, cancellation messages and request credentials are omitted
from these labels. Cancellation still propagates immediately and is not retried.

HTTP responses can carry a Server status and reason. Connection failures,
timeouts and peer disconnects instead use httpx transport exceptions. The
transport retry policy re-raises its last exception on exhaustion; individual
poll APIs also retain their documented timeout-to-empty behavior.

`TransportRetryPolicy` already retries `httpx.RemoteProtocolError`, including a
peer disconnect before a response, within its configured attempt budget. Worker
poll retries keep the same `poll_request_id`, namespace, credentials and
serialized body. `httpx.LocalProtocolError` fails without a retry. These HTTP
attempts do not renew durable execution budgets or rerun authored workflow code.

Local `asyncio.CancelledError` ends the Python await. Durable `WorkflowCancelled`
and `ActivityCancelled` describe separate workflow or activity cancellation
outcomes. Inspect the runtime or its history to establish durable effects after
an interrupted request.

::: durable_workflow.metrics
