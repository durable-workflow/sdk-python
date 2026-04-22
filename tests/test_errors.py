from __future__ import annotations

import pytest

from durable_workflow.errors import (
    ActivityCancelled,
    DurableWorkflowError,
    InvalidArgument,
    NamespaceNotFound,
    QueryFailed,
    ServerError,
    Unauthorized,
    UpdateRejected,
    WorkflowAlreadyStarted,
    WorkflowCancelled,
    WorkflowNotFound,
    _raise_for_status,
)


class TestRaiseForStatus:
    def test_200_noop(self) -> None:
        _raise_for_status(200, {})

    def test_401_unauthorized(self) -> None:
        with pytest.raises(Unauthorized):
            _raise_for_status(401, {"message": "bad token"})

    def test_404_workflow(self) -> None:
        with pytest.raises(WorkflowNotFound):
            _raise_for_status(404, {"reason": "workflow_not_found"}, context="wf-1")

    def test_404_instance_not_found(self) -> None:
        with pytest.raises(WorkflowNotFound):
            _raise_for_status(404, {"reason": "instance_not_found"}, context="wf-1")

    def test_404_query_not_found(self) -> None:
        with pytest.raises(QueryFailed):
            _raise_for_status(404, {"reason": "query_not_found", "message": "query [status] not declared"})

    def test_404_namespace(self) -> None:
        with pytest.raises(NamespaceNotFound):
            _raise_for_status(404, {"reason": "namespace_not_found", "message": "ns missing"})

    def test_404_generic(self) -> None:
        with pytest.raises(ServerError):
            _raise_for_status(404, {"reason": "other"})

    def test_409_duplicate(self) -> None:
        with pytest.raises(WorkflowAlreadyStarted):
            _raise_for_status(409, {"reason": "duplicate_not_allowed"}, context="wf-1")

    def test_409_query_rejected(self) -> None:
        with pytest.raises(QueryFailed):
            _raise_for_status(409, {"reason": "query_rejected", "message": "workflow unavailable"})

    def test_409_update_rejected(self) -> None:
        with pytest.raises(UpdateRejected):
            _raise_for_status(409, {"reason": "update_rejected", "message": "rejected by handler"})

    def test_409_generic(self) -> None:
        with pytest.raises(ServerError):
            _raise_for_status(409, {"reason": "other"})

    def test_422_invalid(self) -> None:
        with pytest.raises(InvalidArgument) as exc_info:
            _raise_for_status(422, {"message": "bad", "errors": {"f": ["req"]}})
        assert exc_info.value.errors == {"f": ["req"]}

    def test_500_server_error(self) -> None:
        with pytest.raises(ServerError) as exc_info:
            _raise_for_status(500, {"message": "internal"})
        assert exc_info.value.status == 500


class TestServerErrorReason:
    def test_reason_from_dict(self) -> None:
        e = ServerError(409, {"reason": "lease_expired"})
        assert e.reason() == "lease_expired"

    def test_reason_from_str(self) -> None:
        e = ServerError(500, "plain text")
        assert e.reason() is None


class TestCancellationContract:
    """Cancellation exceptions must not be caught by generic ``except Exception:``.

    Guards the pre-1.0 design decision that ``WorkflowCancelled`` and
    ``ActivityCancelled`` inherit from :class:`BaseException` directly, so that
    user worker/activity code cannot accidentally swallow a cancellation signal
    in a catch-all.
    """

    def test_workflow_cancelled_not_caught_by_exception(self) -> None:
        try:
            raise WorkflowCancelled("stop")
        except Exception:  # noqa: BLE001 — intentional: proves it does NOT catch
            pytest.fail("WorkflowCancelled must not be catchable via except Exception")
        except WorkflowCancelled:
            pass

    def test_activity_cancelled_not_caught_by_exception(self) -> None:
        try:
            raise ActivityCancelled("stop")
        except Exception:  # noqa: BLE001 — intentional: proves it does NOT catch
            pytest.fail("ActivityCancelled must not be catchable via except Exception")
        except ActivityCancelled:
            pass

    def test_workflow_cancelled_not_caught_by_durableworkflowerror(self) -> None:
        """Belt-and-braces: DurableWorkflowError is also an Exception subclass."""
        try:
            raise WorkflowCancelled("stop")
        except DurableWorkflowError:
            pytest.fail(
                "WorkflowCancelled must not be catchable via except DurableWorkflowError"
            )
        except WorkflowCancelled:
            pass

    def test_activity_cancelled_not_caught_by_durableworkflowerror(self) -> None:
        try:
            raise ActivityCancelled("stop")
        except DurableWorkflowError:
            pytest.fail(
                "ActivityCancelled must not be catchable via except DurableWorkflowError"
            )
        except ActivityCancelled:
            pass

    def test_cancellation_explicitly_caught_by_baseexception(self) -> None:
        """Callers that really want to catch everything still can — by naming BaseException."""
        for exc_cls in (WorkflowCancelled, ActivityCancelled):
            try:
                raise exc_cls("stop")
            except BaseException as e:  # noqa: BLE001 — intentional
                assert isinstance(e, exc_cls)

    def test_cancellation_inheritance_shape(self) -> None:
        assert issubclass(WorkflowCancelled, BaseException)
        assert issubclass(ActivityCancelled, BaseException)
        assert not issubclass(WorkflowCancelled, Exception)
        assert not issubclass(ActivityCancelled, Exception)
        assert not issubclass(WorkflowCancelled, DurableWorkflowError)
        assert not issubclass(ActivityCancelled, DurableWorkflowError)
