# Contributing

Run Ruff, mypy, and focused pytest coverage for changed code.

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
