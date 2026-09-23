from durable_workflow import serializer, workflow
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, replay


@workflow.defn(name="tests.redrive")
class RedriveWorkflow:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        first = yield ctx.schedule_activity("tests.first", [])
        second = yield ctx.schedule_activity("tests.second", [first])
        return {"first": first, "second": second}


def completed(sequence: int, activity_type: str, result: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "sequence": sequence,
        "activity_type": activity_type,
        "result": serializer.encode(result, codec="avro"),
        "payload_codec": "avro",
    }
    if sequence == 1:
        payload["reused_from_run_id"] = "failed-run"
        payload["reused_activity_execution_id"] = "original-first"

    return {
        "event_type": "ActivityCompleted",
        "payload": payload,
    }


def test_redriven_history_reuses_completed_prefix_and_retries_failed_step() -> None:
    history = [completed(1, "tests.first", "recorded")]

    retry = replay(RedriveWorkflow, history, []).commands
    assert len(retry) == 1
    assert isinstance(retry[0], ScheduleActivity)
    assert retry[0].activity_type == "tests.second"
    assert retry[0].arguments == ["recorded"]

    completed_run = replay(
        RedriveWorkflow,
        history + [completed(2, "tests.second", "retried")],
        [],
    ).commands
    assert len(completed_run) == 1
    assert isinstance(completed_run[0], CompleteWorkflow)
    assert completed_run[0].result == {"first": "recorded", "second": "retried"}
