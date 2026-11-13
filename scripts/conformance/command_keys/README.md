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
`--init` and one ephemeral loopback port. The consumer is sequential and bounded
to ten minutes. A private empty Docker config and minimal subprocess environment
exclude host credentials. The controller owns exact-name runtime/anonymous-volume
cleanup and removes its generated credential even after consumer failure.

Six workflow cells exercise eight genuinely lost upstream success responses:
automatic and manual recovery with generated and explicit keys, both signal and
update routes, and one continuation. The adapter independently compares sender
and proxy request bytes, requires successful complete upstream consumption,
checks four concurrent identical retries and a typed changed-intent conflict,
then verifies one business execution/admission and retained identities after
closure or continuation. Typed nested values include integers, booleans, bytes
and Unicode. Query reads consume the receipt's `result` field.

Keep a failed attempt as NONPASS. `consumer.json` and `run.json` must both report
`complete: true`, with no OOM, exact package/runtime/source identities and verified
cleanup, before reporting this cell as passing. Record the public runner commit,
file hashes, tuple, timestamps and sanitized outcomes in the owning GitHub record.
Keep diagnostic logs private and temporary; do not commit generated payloads.

This does not qualify synthetic legacy transforms or physical upgrades. The
accepted common stopped-copy helper is separately maintained by
[SDK Rust260](https://github.com/durable-workflow/sdk-rust/pull/260), and Server1210
owns the real published historical seed/upgrade. Namespace/run fences and maximum
Unicode key acceptance also remain distinct installed cells. No full shared216
PASS follows from this adapter alone.
