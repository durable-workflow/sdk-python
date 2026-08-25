"""Workflow-only examples for deterministic parallel groups and sagas."""

from durable_workflow import ChildWorkflowFailed, workflow


@workflow.defn(name="parallel-trip-quote")
class ParallelTripQuote:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        try:
            return (
                yield [
                    ctx.schedule_activity("trip.quote-flight", []),
                    [
                        ctx.start_child_workflow("trip.quote-hotel", []),
                        ctx.start_timer(1),
                    ],
                ]
            )
        except ChildWorkflowFailed as failure:
            return {"failed_child": failure.child_workflow_type, "message": str(failure)}


@workflow.defn(name="trip-booking-saga")
class TripBookingSaga:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        def forward(saga):  # type: ignore[no-untyped-def]
            flight = yield ctx.schedule_activity("trip.reserve-flight", [])
            saga.add_compensation("trip.cancel-flight", [flight])

            hotel = yield ctx.schedule_activity("trip.reserve-hotel", [])
            saga.add_compensation("trip.cancel-hotel", [hotel])

            ctx.throw_if_cancellation_requested()
            yield ctx.schedule_activity("trip.charge", [])
            return {"flight": flight, "hotel": hotel}

        # A worker restart or completed-history replay reconstructs every
        # registration. Compensation failure is surfaced as the exported
        # SagaCompensationFailed type with both failures retained.
        return (yield from ctx.saga().run(forward))
