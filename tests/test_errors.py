from __future__ import annotations

import pytest

from durable_workflow.errors import (
    InvalidArgument,
    NamespaceNotFound,
    ServerError,
    Unauthorized,
    WorkflowAlreadyStarted,
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

    def test_404_namespace(self) -> None:
        with pytest.raises(NamespaceNotFound):
            _raise_for_status(404, {"reason": "namespace_not_found", "message": "ns missing"})

    def test_404_generic(self) -> None:
        with pytest.raises(ServerError):
            _raise_for_status(404, {"reason": "other"})

    def test_409_duplicate(self) -> None:
        with pytest.raises(WorkflowAlreadyStarted):
            _raise_for_status(409, {"reason": "duplicate_not_allowed"}, context="wf-1")

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
