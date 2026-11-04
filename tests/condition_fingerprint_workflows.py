"""Workflows for the condition-fingerprint corpus fixtures (#258).

The v1 fixtures were recorded by durable-workflow 2.14.0 from PyPI on the
official python:3.12-slim image, replaying these classes; the v2 fixture uses
this branch's stable fingerprint. Keep this module's source unchanged: the
recorded fingerprints depend on it.
"""

from __future__ import annotations

from durable_workflow import workflow


@workflow.defn(name="tests.replay.fingerprint-approval")
class FingerprintApproval:
    """A keyless wait whose fingerprint alone identifies it."""

    def __init__(self) -> None:
        self.approved = False

    @workflow.signal("approve")
    def approve(self) -> None:
        self.approved = True

    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.approved)
        return "approved"


@workflow.defn(name="tests.replay.fingerprint-genexpr")
class FingerprintGenexpr:
    """A keyed wait whose v1 fingerprint differs in every process (nested genexpr)."""

    def __init__(self) -> None:
        self.items: list[int] = []

    @workflow.signal("add")
    def add(self, value: int) -> None:
        self.items.append(value)

    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: any(n > 10 for n in self.items), key="large")
        return list(self.items)


@workflow.defn(name="tests.replay.fingerprint-terminal-duplicate")
class FingerprintTerminalDuplicate(FingerprintApproval):
    @workflow.signal("note")
    def note(self) -> None:
        return None
