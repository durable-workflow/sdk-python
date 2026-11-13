# Installed command-response recovery

This adapter exercises the actual published Python Client and Worker for shared
[command-key acceptance](https://github.com/durable-workflow/.github/issues/216).
It creates synthetic workflows and a disposable SQLite runtime. It does not
modify SDK product source or provide a new transport fault implementation.

The source binds the published wheel and accepted shared proxy by SHA-256.
The controller binds the qualified native image index, source and amd64 platform
from [Server's independent publication review](https://github.com/durable-workflow/server/pull/1209#issuecomment-6858140177).
Expected platform receipt fields remain separate from observed local image
identity and runtime version. Stable aliases and final Release selection are
separate producer gates.

Prepare a registry-only virtualenv outside the checkout. Install
`durable-workflow==2.17.2` from PyPI with wheel-only dependencies and download its
wheel separately with `pip download --no-deps --only-binary=:all:`. Do not install
this checkout, an editable package or a local archive. The adapter refuses a
direct URL installation, unexpected wheel hash, mismatched runtime files or a
source import overlay. The controller invokes the virtualenv interpreter with
`-I`; assertions must remain enabled.

Download the frozen shared proxy from
[`117118c8`](https://github.com/durable-workflow/.github/blob/117118c83093822469b3824ba9587ff4068ed29f/conformance/command-keys/committed_response_loss.py).
Its bound SHA-256 is
`bd1e8ccbbf76f63fc2eafa278a023eb181d405b68d9c30016392e395a1eff725`.
The proxy accepts numeric loopback only and retains bounded request/response
counts and hashes. It receives only ephemeral synthetic credentials.

After checking host/Docker capacity, pull the controller's exact image with an
empty Docker config. The controller itself never pulls an image. Run:

```sh
python3 -I scripts/conformance/command_keys/run_response_loss.py \
  --python /path/to/registry-venv/bin/python \
  --wheel /path/to/downloaded/durable_workflow-2.17.2-py3-none-any.whl \
  --proxy /path/to/verified/committed_response_loss.py \
  --output /path/to/private-disk-scratch/new-run
```

The output parent must exist. The new private output directory must not exist.
One runtime is capped at 1 CPU, 1 GiB and 256 PIDs, with a read-only root,
`--init` and one ephemeral loopback port. Bounded tmpfs mounts supply writable
SQLite, storage and temporary paths inside the container's memory ceiling.
The consumer is sequential and bounded
to ten minutes. A private empty Docker config and minimal subprocess environment
exclude host credentials. The controller owns exact-name runtime/anonymous-volume
cleanup and removes its generated credential even after consumer failure.

Six workflow cells exercise eight genuinely lost upstream success responses:
automatic and manual recovery with generated and explicit keys, both signal and
update routes, and one continuation. The adapter independently compares sender
and proxy request bytes, requires successful complete upstream consumption,
checks four concurrent identical retries and a typed changed-intent conflict,
then verifies one business execution/admission and retained identities after
closure or continuation. Explicit keys use the corrected 255-code-point Unicode
boundary, exceeding 255 UTF-8 bytes. Typed nested values include integers, booleans, bytes
and Unicode. Query reads consume the receipt's `result` field.

Keep a failed attempt as NONPASS. `consumer.json` and `run.json` must both report
`complete: true`, with no OOM, exact package/runtime/source identities and verified
cleanup, before reporting this cell as passing. Record the public runner commit,
file hashes, tuple, timestamps and sanitized outcomes in the owning GitHub record.
Keep diagnostic logs private and temporary; do not commit generated payloads.

This does not qualify synthetic legacy transforms or physical upgrades. The
accepted common stopped-copy helper is separately maintained by
[SDK Rust260](https://github.com/durable-workflow/sdk-rust/pull/260), and Server1210
owns the real published historical seed/upgrade. Namespace/run fences also remain
distinct installed cells. No full shared216
PASS follows from this adapter alone.

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
