# Separate workflow and activity processes

Run workflow coordination separately from remote activity callbacks when they
need different CPU, memory or concurrency limits. A `Worker` polls workflow
tasks only when it has workflow handlers, and activity tasks only when it has
activity handlers. Select the handler lists to separate roles. Both concurrency
settings must remain positive; setting one to zero is not a way to disable it.

The [complete example](https://github.com/durable-workflow/sdk-python/blob/main/examples/separate_workers.py)
has workflow-only, activity-only, start and inspection commands. It uses the
normal installed SDK, without importing the repository's `src` directory.

## Install and connect

Use a POSIX shell on Linux, macOS or WSL with Python 3.10 or newer. From a checkout
of the SDK repository:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install 'durable-workflow==2.6.0'
export SERVER_URL=http://localhost:8080
export WORKFLOW_NAMESPACE=default
export WORKFLOW_TOKEN=your-local-development-token
```

The qualification baseline is published Server **2.9.5** and Python SDK
**2.6.0**. Start Server using its [self-hosted installation guide](https://github.com/durable-workflow/server#run-server).
Its API, queue worker and scheduler must be running and `/api/ready` must be
ready. The Python processes talk to Server over HTTP and do not connect to its
database. `SERVER_URL` is the origin without `/api`. For Cloud, use the complete
provisioned namespace runtime URL and credentials instead.

The local example uses one development token. In a deployed application, set
`WORKFLOW_TOKEN` separately for each process: a worker credential for worker
commands, a client credential for `start`, and an operator credential for
`inspect`. Do not copy an operator token into workers to make inspection work.

## Start two workers

Activate the same environment and set the connection variables in each terminal.
Run the workflow role in the first terminal:

```bash
python examples/separate_workers.py workflow --worker-id python-workflows-1 --slots 4
```

Run the activity role in the second:

```bash
python examples/separate_workers.py activity --worker-id python-activities-1 --slots 2
```

Every live process needs its own worker ID. The two registrations use the same
`python-split` task queue and namespace, with different supported types:

| Worker | Workflow types | Activity types |
| --- | --- | --- |
| `python-workflows-1` | `examples.split-workers.greeter` | empty |
| `python-activities-1` | empty | `examples.split-workers.greet` |

In a third terminal, inspect the actual roster, then start a workflow:

```bash
python examples/separate_workers.py inspect
python examples/separate_workers.py start --name world
python examples/separate_workers.py start --name retry --retry-once
```

The final results are `hello, world` and `hello, retry`. The activity process
prints its worker ID, process ID and attempt number. The retry example fails
attempt 1 deliberately; Server schedules attempt 2 under the activity's recorded
two-attempt policy. The start command only submits and observes. It does not run
either handler itself, and its 30-second wait is not a cancellation request.

The roster reports both numeric slot settings even though each process has only
one handler family. Supported type lists determine which family it serves.
Do not count the inactive family's positive setting as usable handler capacity.

## Route activities to a distinct queue

The shared-queue example separates processes by supported types. It does not
create a separate activity queue. For an explicit queue split, start workers
with matching queue names:

```bash
# Workflow terminal
python examples/separate_workers.py workflow --worker-id python-workflows-orders --queue orders
# Activity terminal
python examples/separate_workers.py activity --worker-id python-activities-heavy --queue heavy
# Client terminal
python examples/separate_workers.py start --queue orders --activity-queue heavy --retry-once
```

The workflow passes `queue="heavy"` to `ctx.schedule_activity(...)`. With no
override, the activity uses the workflow's queue. An activity worker on `heavy`
will not serve work scheduled to `orders`. Namespace, queue and supported type
must all match. Build compatibility adds another routing constraint during
[worker rollouts](https://durable-workflow.com/docs/2.0/polyglot/worker-build-id-rollout/).

## Size and stop each process

`--slots` changes only that role's `max_concurrent_*_tasks`. Set container or
process CPU and memory limits independently. Activity slots must also fit the
database, HTTP connection pool, downstream rate limits and callback memory
budget. More processes multiply local capacity; use
[server-side admission budgets](https://durable-workflow.com/docs/2.0/polyglot/task-queue-admission/)
when the whole namespace or queue needs a ceiling.

An `async def` activity must yield while waiting. Blocking CPU or synchronous
I/O inside it blocks that process's event loop, including polling and heartbeat
work. Use suitable nonblocking libraries or bounded executors/processes for the
workload; a separate activity process does not remove downstream contention.

Press Ctrl-C in each worker terminal, or send SIGTERM to each worker process.
The example awaits `Worker.stop()` and its run task. It stops polling, drains
accepted work within its 10-second shutdown budget, and deregisters. Give the
process supervisor a termination grace period longer than this budget plus
HTTP cleanup time. Errors during deregistration or drain cause a nonzero exit
and must be investigated. Stopping the client does not stop workers or cancel
the workflow. A later `inspect` should no longer list successfully stopped
worker IDs; a missing process alone is not proof of successful deregistration.

## Local resources and recovery

- Ordinary `ctx.schedule_activity(...)` calls are remote activity tasks. They
  can be picked up by a compatible replacement activity worker after normal
  lease/retry handling. Application side effects still need idempotency.
- Opted-in local activities execute in the **workflow worker**. Register their
  callbacks on that worker; an activity-only process cannot supply them. They
  consume workflow-process resources and need their documented durable local
  activity and cancellation capabilities.
- Worker sessions intentionally bind work to the session owner while its lease
  is active. Process separation does not make an in-memory client, file handle
  or session durable or migrate it to any available process.
- Sticky history caches belong to workflow workers and are optional. Eviction
  or process replacement uses cold replay from Server history. Keep correctness
  independent of cache contents.

See [Worker lifecycle and shutdown](../reference/worker.md),
[local activities and sessions](../sdk-reference.md#local-activities), and
[sticky execution](sticky-execution.md) for the respective contracts.
