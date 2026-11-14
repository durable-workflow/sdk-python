# Resume a bounded batch

This source example uses the unreleased acknowledged-checkpoint API from
[SDK333](https://github.com/durable-workflow/sdk-python/pull/333). It requires a
Server advertising `activity_heartbeat_resume` v1. Its source tests do not prove
published-package compatibility, real process loss or Server persistence.

On Linux, from a source checkout with its development environment, point it at
an isolated development Server. Supply a **new** directory for each invocation.
The synthetic provider receipts remain there for inspection; existing
directories are refused.

```bash
export SERVER_URL=http://127.0.0.1:8080
export WORKFLOW_TOKEN=your-development-token
python -m examples.checkpointed_batch ./receipts-before before-commit
python -m examples.checkpointed_batch ./receipts-commit after-commit
python -m examples.checkpointed_batch ./receipts-checkpoint after-checkpoint
```

The importable `BatchActivity.execute` handler carries only serializable provider
configuration, so it also passes the SDK's cooperative callback preflight.
The activity processes three items, with a maximum of sixteen bounded items.
Its two attempts, one-second retry delay, ten-second heartbeat timeout,
thirty-second start-to-close timeout and sixty-second schedule-to-close timeout
stay fixed. Recovery does not extend those budgets. The example's worker wait
is bounded separately.

| First-attempt failure | Replacement action |
| --- | --- |
| Before provider commit | Commit item zero, then acknowledge progress |
| After commit, before acknowledgement | Reconcile the stable provider key, then acknowledge progress |
| After acknowledgement | Resume at the received next-item cursor |
| Acknowledgement response lost | The next claim may contain the checkpoint or none; reconcile any repeated provider operation |

`last_heartbeat_details(result_type=Progress)` reads the immutable received
snapshot. Unsupported claims refuse instead of inventing empty state. A
supported empty checkpoint starts from zero. The typed cursor binds the full
business input and must be within the item range.

Provider receipts are separate durable state. A key uses the business ID and
item index, never an attempt ID. Each receipt also binds the entire batch
fingerprint, so appending, truncating or reordering input under a repeated
business ID refuses even without a checkpoint. Revisited keys require the exact
receipt. Partial or conflicting receipts are preserved and require
reconciliation. File and directory entries are synchronized before commit or
reconciliation returns. This is a private synthetic provider, not an actual
payment or other external service.

The ordering is provider commit/reconcile, then `await ctx.heartbeat(...)`, then
continue. A failed heartbeat stops the attempt; an uncertain response does not
mean the checkpoint failed to commit. The SDK snapshot does not advance in
place. A checkpoint does not atomically commit an external business effect.
Use the real provider's idempotency, operation lookup and fencing contract,
retain the business ID across restarts of that operation, and choose a new ID
for a new business operation. The example does not promise exactly-once effects.

The source tests use synthetic files and acknowledgement callbacks without a
Server. They cover all three failure boundaries, both lost-response outcomes,
changed input without a checkpoint, malformed cursors and partial receipts:

```bash
pytest -q tests/test_checkpointed_batch_example.py
```
