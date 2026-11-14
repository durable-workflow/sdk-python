The ordinary activity-identity acceptance cell installs the exact reviewed
registry wheel and uses the immutable published Server tuple in
`tests/fixtures/published-activity-acceptance.json`. It reads identity inside
real activity callbacks, deliberately fails the first attempt, and checks the
logical execution/workflow/run identity across the Server retry while task and
attempt identities change.

After source review and the normal PR gates, run it in GitHub Actions:

```bash
gh workflow run ci.yml --ref main -f published_activity_acceptance=true
```

The checked-in release approval supplies the SDK wheel digest. The complete
dependency set, including pytest and its plugin, is hash locked. Downloads
happen before the SDK consumer starts. The consumer installs those captured
wheels without network access into a fresh private environment, checks every
SDK wheel member, complete distribution metadata and installed RECORD, and uses
isolated Python with explicit pytest plugins. Installer-created metadata and
console scripts have exact expected bytes. The checker stays in the trusted
bootstrap's memory through the case. The checkout is never installed as the SDK.

The published native runtime and official Python consumer share one owned
internal network. Neither publishes a port or receives a host mount. The
controller checks both full container IDs, owner/role/run labels, image config
digests, resource limits and network attachments before admitting SDK activity
and after the result. The numeric runtime endpoint must remain unchanged. Each
container has a read-only root, an unprivileged user, all capabilities dropped,
no new privileges, one CPU, 1 GiB RAM with no additional swap and 256 PIDs.
Writable temporary storage stays inside that memory limit.

Central action-policy and lint gates precede execution. The controller has a
six-minute execution bound, followed by independent cleanup of each captured
ID and then the network. Label-bound reconciliation covers uncertain create
responses. A foreign replacement is preserved. A reserved result descriptor
retains a terminal failure even if the output path refuses further use.

The seven-day artifact contains the selected tuple, complete wheel hashes,
dependency versions, SDK inventory, before/after container and network facts,
actual callback observations, JUnit, logs and cleanup result. An installed
package or a source integration pass alone is not this published-tuple proof.
The cell does not qualify local/prepared/session activity behavior or external
side-effect recovery.
