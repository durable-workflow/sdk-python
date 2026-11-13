# Installed command-response recovery

This adapter exercises the published Python Client and actual Worker for
[shared command-key acceptance](https://github.com/durable-workflow/.github/issues/216).
It creates synthetic workflows and a disposable SQLite runtime. Its transport
fault comes from the independently reviewed shared loopback fixture.

The controller binds the qualified native image/source/platform from
[Server's publication receipt](https://github.com/durable-workflow/server/pull/1209#issuecomment-6858140177).
Its interpreter is the published official Python image index
`sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df`,
amd64 child `sha256:0f086103b026048cc50cb5da25ec435c023c5241a83fc44daea593377abc61c4`.
It starts a fresh private virtualenv in that image and installs all nine registry
packages from `consumer-requirements.txt`, with exact wheel-only hashes. The SDK
wheel is the reviewed published artifact. The interpreter/config and entire
installed environment are fingerprinted before and after the consumer.

Download the published SDK wheel identified by `WHEEL_SHA256` in the controller.
Download the shared proxy from
[`117118c8`](https://github.com/durable-workflow/.github/blob/117118c83093822469b3824ba9587ff4068ed29f/conformance/command-keys/committed_response_loss.py).
Its SHA-256 is
`bd1e8ccbbf76f63fc2eafa278a023eb181d405b68d9c30016392e395a1eff725`.
Store both as regular, single-link, owned files in owner-controlled directories.
Before the experiment, check host/Docker capacity and pull both exact images
with an empty Docker config. The controller itself does not pull images.

Run from an exact committed checkout:

```sh
python3 -I scripts/conformance/command_keys/run_response_loss.py \
  --runner-commit FULL_REVIEWED_RUNNER_COMMIT \
  --wheel /path/to/downloaded/durable_workflow-2.17.2-py3-none-any.whl \
  --proxy /path/to/verified/committed_response_loss.py \
  --output /path/to/private-disk-scratch/new-run
```

The controller and executable source inputs must equal that commit's Git blobs.
It opens inputs without following symlinks, checks owner/mode/single-link/file
bounds and retains descriptors plus captured bytes. The adapter, bootstrap and
proxy execute from those bytes. Wheel inspection uses the same captured bytes.
Pathname, inode, content and parent-directory identities are checked again before
launch and after return. A substituted program cannot certify its own output by
reporting the original hash. The host does not supply an interpreter or virtualenv.

The new private output directory must not exist; its parent must already exist.
One runtime and one consumer are each capped at 1 CPU, 1 GiB and 256 PIDs, with
read-only roots and `--init`. Bounded tmpfs paths remain within each memory cap.
The consumer's ordinary scratch is noexec. Its separate private wheel environment
permits native-extension mapping for the hash-locked codec wheel; it is not a
host mount and accepts no unverified executable input.
The runtime publishes one ephemeral loopback port. The consumer uses host
networking for the numeric loopback-only proxy and has no host mounts. Registry
installation precedes creation of its synthetic credential file. Its entire
bootstrap/input bundle travels through stdin; executable files are never reopened
from host paths. Controller readiness explicitly disables environment proxies.
The consumer has a ten-minute bound, and cleanup owns both containers, anonymous
volumes and the generated fixture credential.

Six workflows exercise eight intentionally lost complete upstream successes:
automatic/manual recovery with generated/explicit keys on both routes, including
continuation. Sender and proxy request bytes agree, four concurrent identical
retries retain one admission, and changed intent gets a typed conflict. Business
execution/admission counts and closed/continued original identities are checked.
Explicit keys contain 255 Unicode code points and exceed 255 UTF-8 bytes. Nested
typed values include integers, booleans, bytes and Unicode. Query receipts are
read through their `result` field.

Both `consumer.json` and `run.json` must report complete success, with captured
input and interpreter rechecks, equal environment inventories, no OOM and exact
cleanup, before qualification. Record the reviewed runner commit, input hashes,
tuple, timestamps and sanitized outcomes in GitHub. Keep failed attempts as
NONPASS and diagnostic captures private and temporary. The earlier host-venv
observation is retained under its Security provenance HOLD, not reused as the
corrected runner's qualification.

Synthetic legacy transformations and physical upgrades remain separate cells.
The accepted common stopped-copy helper is owned by
[SDK Rust260](https://github.com/durable-workflow/sdk-rust/pull/260); Server1210
owns the real historical seed. Namespace/run fences remain independently scoped.
This adapter alone does not complete shared216.

## Physical upgrade consumer

`physical_readback.py` consumes the producer-owned JSON receipt contract from
[Server1210](https://github.com/durable-workflow/server/pull/1210). Source/execution
reuse is held until that PR's final source review and checks qualify. This is
separate from the accepted synthetic-copy helper and its older manifest hash.

The consumer invokes the registry-only Python Client against an already upgraded
disposable runtime. The producer receipt must include all original signal/update
open/completed/continued cases, complete captured histories and original update
identities. The manifest includes the historical unkeyed-signal clarification.
It refuses a changed receipt hash, incomplete seed, missing identities or package
overlay before issuing a command.

An outer runner must bind the producer source, stopped backup and receipt hashes,
upgrade a private copy with the qualified image, preserve the original, disable
unrelated maintenance writes and compare complete logical snapshots around this
consumer. It owns the image/source check, exact runtime/database/credential
cleanup and the final qualification receipt. Never run against the retained
original database or reuse a production runtime credential.

`run_physical_readback.py` supplies that outer integration for the frozen producer
handoff. It binds the exact stopped-backup/JSON/helper hashes, copies into a new
private directory, checks native schema markers, runs the qualified image's
`schema-upgrade`, and verifies the empty intent table. It then brackets this
consumer with complete typed logical snapshots from the accepted helper.
The upgrade itself preserves every original business row and schema object.
Its only metadata exceptions are the native marker and a single SQLx journal
row, validated field by field against tagged description/checksum/version,
success, zero execution time and the actual timestamp window. The added empty
table/catalog comes from the hash-bound tagged SQL step in an isolated SQLite
catalog. No business row or migration-journal field is exempted from comparison.
The request window ends before shutdown; later repair work cannot be relabeled
as part of that zero-write window. The retained original backup is hash-checked
again after cleanup. Two sequential containers have the same 1 CPU/1 GiB/256-PID
limits, read-only roots and bounded tmpfs paths; no compile or historical reseed
occurs. Original seed/upgrade authority remains Server-owned.

Once the producer's final source gate permits execution reuse, invoke the outer
controller with `--python`, `--wheel`, `--baseline`, `--seed`, `--snapshot-helper`,
the tagged `--step-sql`,
and a new private `--output` directory. The original file inputs come from the
producer handoff and accepted common snapshot helper, not an invented fixture.
This draft has no installed physical result yet.

```sh
/path/to/registry-venv/bin/python -I \
  scripts/conformance/command_keys/physical_readback.py \
  --upstream http://127.0.0.1:12345 --server-version 3.9.2 \
  --credential-file /path/to/private/fixture.env \
  --seed /path/to/producer-receipt.json --seed-sha256 FULL_PRODUCER_RECEIPT_SHA256 \
  --wheel /path/to/downloaded/durable_workflow-2.17.2-py3-none-any.whl \
  --output /path/to/private/new-consumer-receipt.json
```

For each original keyed update it issues four concurrent retries with changed
input, requires original command/run/update/acceptance identities and explicit
`intent_verified: false`, checks a changed-name conflict, then uses GET-only
outcome reads to distinguish pending from completed-null. Python's update helper
uses its unscoped route; this does not claim a new explicit-run update API.
Historical NULL-key signals receive no fabricated retry key. Their compatibility
control reads the original history, including completed/continued boundaries.
Neither this consumer's completion flag nor a producer-only upgrade receipt alone
qualifies the installed SDK physical cell without the outer snapshot/cleanup proof.
