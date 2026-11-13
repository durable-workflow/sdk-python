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

Run from an exact committed checkout. Qualifying execution uses a literal trusted
bootstrap to verify the reviewed controller Git bytes before compiling them.
Set `REVIEWED_COMMIT` and `REVIEWED_SHA` from the independent review for the
chosen controller. The expected identity is supplied by that review.

```sh
BOOTSTRAP='import hashlib, os, re, subprocess, sys
from pathlib import Path
commit, relative, expected = sys.argv[1:4]
del sys.argv[1:4]
allowed = {"scripts/conformance/command_keys/" + name for name in
           ("run_response_loss.py", "run_physical_readback.py", "run_synthetic_legacy.py")}
if (re.fullmatch(r"[0-9a-f]{40}", commit) is None
        or re.fullmatch(r"[0-9a-f]{64}", expected) is None or relative not in allowed):
    raise ValueError("require reviewed controller identities")
data = subprocess.check_output(["/usr/bin/git", "show", commit + ":" + relative],
                               env={"PATH": os.defpath}, timeout=10)
if hashlib.sha256(data).hexdigest() != expected:
    raise ValueError("reviewed controller Git bytes differ")
context = {"__name__": "__main__", "__file__": str(Path.cwd() / relative)}
exec(compile(data, context["__file__"], "exec"), context)
'
```

The [frozen physical/loss invocation and pre-open substitution controls](https://github.com/durable-workflow/sdk-python/pull/321#issuecomment-6862370654)
show the complete recorded trust root. The adapter still captures and verifies
all host source/data files against the declared commit before creating its
workspace. Opening the host controller pathname first is insufficient for
qualification.

```sh
/usr/bin/python3 -I -B -c "$BOOTSTRAP" "$REVIEWED_COMMIT" \
  scripts/conformance/command_keys/run_response_loss.py "$REVIEWED_SHA" \
  --runner-commit "$REVIEWED_COMMIT" \
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
Creation returns full container IDs bound to per-run ownership labels. Start,
inspection, port lookup, removal and absence verification use those IDs. A reused
human name grants no cleanup authority over its replacement or volumes.

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

The producer-owned [frozen handoff](https://github.com/durable-workflow/server/pull/1210#issuecomment-6860138558)
is accepted at contract source `a09f156399416216d2f097c257a2164f83c7eda7`.
The actual original seed execution remains `09a4ccda3da11853606af031a8970bbde4b2d6ec`,
driver SHA-256 `01c0fa1251ac3c809723e58d71817881fe2c5c9498579bc4c2e6097324a3618b`.
These are different authorities and must remain distinct in every receipt.

`run_physical_readback.py` consumes that stopped backup/JSON plus the accepted
common snapshot helper and hash-bound tagged SQL step. It captures every input
with the same Git/owner/no-follow/held-identity boundary as the response-loss
runner. Only a new private copy is upgraded. Start/inspect/wait/log/remove/absence
checks use captured full creation IDs and ownership labels. Upgrade and runtime
are sequential; the runtime and registry consumer are the only concurrent pair.
The fixed image/bootstrap/hash-locked registry environment is shared with the
reviewed response-loss runner. No host interpreter or executable input pathname
is accepted.

```sh
/usr/bin/python3 -I -B -c "$BOOTSTRAP" "$REVIEWED_COMMIT" \
  scripts/conformance/command_keys/run_physical_readback.py "$REVIEWED_SHA" \
  --runner-commit "$REVIEWED_COMMIT" \
  --wheel /path/to/downloaded/durable_workflow-2.17.2-py3-none-any.whl \
  --baseline /path/to/producer/stopped-backup.sqlite \
  --seed /path/to/producer/seed-receipts.json \
  --snapshot-helper /path/to/accepted/sqlite_legacy.py \
  --step-sql /path/to/tagged/0013_command_intents.sql \
  --output /path/to/private-disk-scratch/new-physical-run
```

The controller binds the frozen backup/packet/helper/step hashes. It checks the
native marker transition and validates every business row/schema object, allowing
only the exact new empty intent catalog and source-defined marker/journal changes.
Every SQLx journal field is checked: version, description, tagged cumulative SQL
checksum, success, zero execution time and the actual upgrade timestamp window.
The installed Client's readbacks are bracketed by complete typed logical snapshots.
That interval ends before shutdown; subsequent repair work is not relabeled as
part of the zero-write claim. Original inputs are rechecked and preserved.

For each original keyed update in open/completed/continued state, four concurrent
changed-input retries must return the original command/run/update/acceptance
identities with explicit `intent_verified: false`. Changed names must conflict;
GET-only outcome reads distinguish pending from completed-null. The Python update
helper uses its unscoped public route, so no new explicit-run update API is claimed.
Historical NULL-key signals receive no fabricated retry key. Their compatibility
control preserves their original history and business identities only.

A producer upgrade receipt alone does not qualify this SDK. The exact consumer
source, installed proof, complete snapshots, immutable input/toolchain rechecks,
all three captured-ID cleanup records and independent review remain required.

The host workspace also retains no-follow descriptors for every ancestor,
private directory and created file. Config/credential bytes are immutable;
the copied SQLite file may change contents but must keep its owned single-link
inode. Docker configuration and credential reads use held
`/proc/<controller-pid>/fd` paths. The canonical database bind has a preflight
inside the immutable published image: it verifies the mounted directory/file
device, inode, owner, mode and single-link identities before executing the
native entrypoint. Host snapshots and lifecycle calls check workspace
identities before and after use. Cleanup uses an explicit empty kernel FD
configuration directory, so workspace substitution cannot block known-ID cleanup. Evidence is created exclusively relative to the
held directory, with the creation descriptor retained through verification.
A substituted parent, copied database, configuration, credential or report refuses
qualification. Known-container cleanup stays bound to captured IDs and preserves
replacement paths. Workspace substitution controls must pass before installed
response-loss or physical results qualify.

## Synthetic SQLite legacy cases

`run_synthetic_legacy.py` prepares real current-producer API admissions, stops
that owned runtime, then calls the unchanged accepted common helper on a private
stopped copy. Its helper source is Rust `4add871b175517f2031f71c868ce278bbfbba486`,
SHA-256 `6fc6054ce087c136a86d7b6b222daeefaa87c58fa0b613b155a5df39e6a26fad`.
The producer manifest is Server `d66e33bfb5c76f94c5b202f8c9f7a9e8cd75cf1c`,
SHA-256 `da1990000c611d7377690c7526487d6415fa4f9cb42d299f59585a7c2eebf769`.
The helper's manifest data lookup uses the captured accepted bytes. Its SQL,
preconditions, complete-copy comparison and exact allowed row changes are unchanged.
This setup is separate from the producer-owned physical historical seed.

```sh
/usr/bin/python3 -I -B -c "$BOOTSTRAP" "$REVIEWED_COMMIT" \
  scripts/conformance/command_keys/run_synthetic_legacy.py "$REVIEWED_SHA" \
  --runner-commit "$REVIEWED_COMMIT" \
  --state open --scenario readback \
  --wheel /path/to/downloaded/durable_workflow-2.17.2-py3-none-any.whl \
  --snapshot-helper /path/to/accepted/sqlite_legacy.py \
  --manifest /path/to/accepted/command-key-legacy-v1-vectors.json \
  --output /path/to/private-disk-scratch/new-synthetic-run
```

Run the readback scenario separately for `open`, `completed` and `continued`.
The actual installed Worker creates closed/continued states and null update
results before the copy. No Worker runs during the readback window. Four
concurrent changed-input retries must retain original identities and unknown
intent; changed names and explicit signal-run targets must conflict. The Python
update helper uses its existing unscoped route. GET-only outcomes preserve the
pending/completed-null distinction.

The other scenarios require `--state open`: `two-rows`, `legacy-token` and
`modern-raw` must refuse with explicit ambiguous/unverified/non-retryable fields.
`case-only` first checks exact-byte retained readback, then admits the lower-case
key in a separate write interval under SQLite's binary comparison. Complete
logical snapshots must match for readback/ambiguity windows. The distinct-key
interval must create two new command/intent identities and has no zero-write claim.

Every phase executes captured adapter/bootstrap bytes in the fixed published
Python image with all nine dependency wheel hashes and the reviewed SDK wheel.
Lifecycle and cleanup use captured full creation IDs plus ownership labels.
Source and copied runtimes are sequential; at most one runtime and one consumer
coexist, each capped at 1 CPU, 1 GiB and 256 PIDs. All declared inputs and the
interpreter are rechecked after return, and both private database copies are
removed after exact-ID container/volume cleanup. Record each state/scenario and
any NONPASS separately. Independent review, exact-head checks and actual
installed evidence are required before qualification; another SDK's result does
not qualify this adapter or complete shared216.
