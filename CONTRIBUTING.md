# Contributing

Run Ruff, mypy, and focused pytest coverage for changed code.

The TLS tests use the OpenSSL CLI to create a throwaway private CA and run a
local HTTPS endpoint. Install `openssl` to run those controls locally; the
supported-version CI requires it before running tests.

Replay and payload-codec fixes also follow the organization
[regression-corpus contract](https://github.com/durable-workflow/.github/tree/main/regression-corpus).
Put minimal replay histories in the existing golden-history format when
possible, or use `tests/fixtures/replay_regressions/`. Put shared wire evidence
in `tests/fixtures/codec_regressions/` and copy it to every applicable official
binding.

Fixtures preserve the value and type, framing, and stable failure policy.
Existing evidence is append-only; protocol evolution adds a new fixture with a
`supersedes` identity. Run:

```bash
python scripts/ci/validate-regression-corpus.py --base-ref <target>
```

## Reviewed package publication

Build a release candidate without publishing:

```bash
gh workflow run publish.yml --ref <release-branch> -f publish=false
```

Before tagging, obtain Security review of that run's exact wheel and sdist and
qualify the installed candidate against the selected published Server. Record
the reviewed producer in `.github/release-candidates/<version>.json`, using the
closed `sdk-python.reviewed-release/v1` schema checked by
`.github/scripts/promote_release.py`: version, source commit/tree, run ID and
attempt, raw `dist-handoff.tar` artifact ID/SHA256, and the wheel/sdist filename
to SHA256 map. The approval file itself receives normal PR/Security review.

The final tag may differ from the reviewed build's source only by adding that
approval file. Any other source or workflow change requires a new build and
review. Tagged publication checks the successful same-repository producer and
promotes its exact distribution bytes through the existing protected handoff;
it does not rebuild them. Missing, expired, changed or unreviewed artifacts
refuse publication before upload. Finish registry installation and stable
portal readback after publication.
