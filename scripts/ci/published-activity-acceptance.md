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

The checked-in release approval supplies the wheel digest. The cell never
installs the checkout as an SDK; the targeted pytest invocation ignores the
project's source-path configuration and verifies the imported package origin.
Central action-policy and lint gates precede execution. The disposable runtime
has fixed CPU/RAM/PID limits, loopback HTTP and private temporary data. Cleanup
uses its captured full container ID and owner/run labels.

The seven-day artifact contains the selected tuple, dependency versions, actual
callback observations, JUnit, runtime log and cleanup result. An installed
package or a source integration pass alone is not this published-tuple proof.
The cell does not qualify local/prepared/session activity behavior or external
side-effect recovery.
