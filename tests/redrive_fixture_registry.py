"""Every redrive replay fixture, and whether it is redrive-successor evidence.

The replay corpus is immutable, so a fixture that is not a history Workflow
can write stays byte-identical and keeps replaying through the official
runner. This registry records that it is not qualification evidence: its
SHA-256 is pinned, it must keep failing ``check_redrive_successor`` with the
named error, and its named replacement must pass. The gate in
``tests/test_redrive_successor_evidence.py`` requires every fixture that
looks like a redrive to be registered here.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NonQualifying:
    sha256: str
    error: str
    replacement: str
    release: str
    reason: str


# Valid redrive successors and their resume_step_sequence.
VALID_SUCCESSORS: dict[str, int] = {
    # Added by #236 (unreleased at the time of #237).
    "redrive-reused-upserts-clock": 3,
    "redrive-reused-signal-wait-clock": 2,
    "redrive-reused-search-attribute-value-mismatch": 3,
    "redrive-reused-search-attribute-type-mismatch": 3,
    "redrive-reused-search-attribute-update-missing": 3,
    # Added by #237, built to Workflow 28893f97 FailedRunRedrivePlan::successorEvents.
    "redrive-successor-reused-markers": 4,
    "redrive-successor-reused-timer": 3,
    "redrive-successor-reused-cancellation-clock": 3,
    # Added by #239: redrive-reused-upserts-cancellation-clock with the
    # request's command_type, which Core WorkflowStub::recordCancellationRequest always writes.
    "redrive-successor-reused-upserts-cancellation-clock": 3,
}

NON_QUALIFYING: dict[str, NonQualifying] = {
    "redrive-start-replayed-clock": NonQualifying(
        sha256="70653171faf5ddba5ed264d0a7ce8bb1af97112dbd6e3bb0e39c6f155fee0fd2",
        error="history order",
        replacement="redrive-successor-reused-markers",
        release="2.13.1",
        reason=(
            "Partial redrive envelope (only recovery_kind and replayed_started_at, no history sequence, "
            "identity, link or resume step) and no reused prefix. It asserts only ctx.now(), which is "
            "correct; it is ambiguous rather than a clock defect. The replacement asserts the same "
            "start clock on a valid successor."
        ),
    ),
    "redrive-reused-upserts-cancellation-clock": NonQualifying(
        sha256="f8c40d18fa71a0656089290fd5be44e7df318fafd12721d7a6680e1113ec4856",
        error="the request's command_type is request_cancellation",
        replacement="redrive-successor-reused-upserts-cancellation-clock",
        release="unreleased (#236, after 2.13.1)",
        reason=(
            "Its CooperativeCancellationRequested has no command_type. Core WorkflowStub::recordCancellationRequest "
            "(workflow 28893f97) always writes command_type request_cancellation, so Workflow cannot "
            "write this request. Its replay result is correct; the replacement adds only that field."
        ),
    ),
}

# Fixtures for a run that redrive starts from. None in this corpus.
SOURCE_RUNS: frozenset[str] = frozenset()

# The 2.13.1 unit test test_a_reused_step_keeps_its_original_time_for_the_remaining_budget
# (tests/test_cancellation_remaining_time.py, #233) was not a corpus fixture.
# It delivered the cancellation before a "reused" step whose reused_recorded_at
# followed the delivery. #237 replaced it with the non-leakage evidence on
# redrive-successor-reused-cancellation-clock.
REPLACED_UNIT_TEST = "test_a_reused_step_keeps_its_original_time_for_the_remaining_budget"
