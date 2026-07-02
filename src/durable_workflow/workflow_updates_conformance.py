"""Published-artifact workflow-update conformance shard for the Python SDK.

The host workflow-updates runner installs this module from the released
``durable-workflow`` package and executes it outside any source checkout. The
shard focuses on the Python client/worker surface: client request construction,
accepted update handler completion/failure, typed refusals, duplicate request
ids, terminal refusals, and payload envelope round trips.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

import httpx

from . import serializer, workflow
from .client import Client
from .errors import InvalidArgument, UpdateRejected
from .workflow import CompleteUpdate, FailUpdate, WorkflowContext, apply_update

SCHEMA = "durable-workflow.v2.workflow-updates.python-sdk-sidecar"
SCENARIO_ID = "python_client_worker_update_surface"
DEFAULT_OUTPUT_FILE = "python-sdk-workflow-updates-evidence.json"
WORKFLOW_ID = "wf-python-update-surface"
WORKFLOW_TYPE = "workflow-updates.python-sdk-probe"
TASK_QUEUE = "workflow-updates-python-sdk"

UPDATE_CELLS = (
    "accepted_update_control_plane_and_history",
    "completed_update_result_round_trip",
    "failed_update_outcome",
    "unknown_update_refusal",
    "invalid_input_refusal",
    "duplicate_request_idempotency",
    "terminal_workflow_update_behavior",
    "payload_envelope_round_trip",
)


@workflow.defn(name=WORKFLOW_TYPE)
class PythonUpdateSurfaceWorkflow:
    """Small update-capable workflow used by the package shard."""

    def __init__(self) -> None:
        self.done = False
        self.approved = False
        self.events: list[dict[str, Any]] = []
        self.payload: dict[str, Any] | None = None

    @workflow.update("accept")
    def accept(self, approved: bool, note: str = "") -> dict[str, Any]:
        self.approved = approved
        event = {"approved": self.approved, "note": note}
        self.events.append({"update": "accept", **event})
        return event

    @accept.validator  # type: ignore[attr-defined, untyped-decorator]
    def validate_accept(self, approved: bool, note: str = "") -> None:
        if not isinstance(approved, bool):
            raise ValueError("approved must be boolean")
        if note is not None and not isinstance(note, str):
            raise ValueError("note must be a string")

    @workflow.update("fail_update")
    def fail_update(self, message: str) -> None:
        raise RuntimeError(message)

    @workflow.update("round_trip")
    def round_trip(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.payload = dict(payload)
        result = {"echo": self.payload, "event_count": len(self.events)}
        self.events.append({"update": "round_trip", "payload": self.payload})
        return result

    @workflow.signal("finish")
    def finish(self) -> None:
        self.done = True

    @workflow.query("state")
    def state(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "events": list(self.events),
            "payload": self.payload,
            "done": self.done,
        }

    def run(self, ctx: WorkflowContext) -> Any:
        yield ctx.wait_condition(lambda: self.done, key="python-update-surface-finished", timeout=300)
        return self.state()


@dataclass
class ProbeRecorder:
    failures: list[str] = field(default_factory=list)
    covered_cells: list[str] = field(default_factory=list)
    unsupported_cells: list[str] = field(default_factory=list)
    typed_errors: list[dict[str, Any]] = field(default_factory=list)
    client_requests: dict[str, dict[str, Any]] = field(default_factory=dict)
    worker_observations: dict[str, dict[str, Any]] = field(default_factory=dict)
    decoded_result: dict[str, Any] | None = None

    def check(self, condition: bool, message: str) -> None:
        if not condition:
            self.failures.append(message)

    def mark(self, cell: str) -> None:
        if cell not in self.covered_cells:
            self.covered_cells.append(cell)

    def typed_error(
        self,
        cell: str,
        error_type: str,
        reason: str,
        *,
        unsupported: bool = False,
    ) -> None:
        if unsupported and cell not in self.unsupported_cells:
            self.unsupported_cells.append(cell)
        self.typed_errors.append(
            {
                "cell": cell,
                "typed_error": error_type,
                "refusal_reason": reason,
                "documented": True,
            }
        )


def installed_artifact_version() -> str:
    """Return the installed package version, or an empty string when absent."""

    try:
        return metadata.version("durable-workflow")
    except metadata.PackageNotFoundError:
        return ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _package_import_path() -> str:
    return str(Path(__file__).resolve())


def _looks_like_source_checkout() -> bool:
    package_file = Path(__file__).resolve()
    for parent in package_file.parents:
        if (parent / "pyproject.toml").is_file() and (parent / "src" / "durable_workflow").is_dir():
            return True
    return False


def _artifact_source(version: str) -> str:
    return f"pypi://durable-workflow=={version}" if version else "pypi://durable-workflow==unresolved"


def _request_body(request: httpx.Request) -> dict[str, Any]:
    if not request.content:
        return {}
    decoded = json.loads(request.content.decode("utf-8"))
    return decoded if isinstance(decoded, dict) else {}


def _response(request: httpx.Request, status: int, payload: Mapping[str, Any]) -> httpx.Response:
    return httpx.Response(status_code=status, json=dict(payload), request=request)


def _summarize_request(request: httpx.Request, response: Mapping[str, Any] | None = None) -> dict[str, Any]:
    body = _request_body(request)
    input_payload = body.get("input")
    decoded_input: Any = None
    if isinstance(input_payload, Mapping):
        decoded_input = serializer.decode_envelope(dict(input_payload))

    summary: dict[str, Any] = {
        "method": request.method,
        "path": request.url.path,
        "wait_for": body.get("wait_for"),
        "request_id": body.get("request_id"),
        "has_input_envelope": isinstance(input_payload, Mapping),
        "decoded_input": decoded_input,
        "namespace_header_present": bool(request.headers.get("X-Namespace")),
        "control_plane_version_header": request.headers.get("X-Durable-Workflow-Control-Plane-Version"),
    }
    if response is not None:
        summary["response"] = dict(response)
    return summary


def _accepted_event(update_id: str, update_name: str, args: list[Any]) -> dict[str, Any]:
    return {
        "event_type": "UpdateAccepted",
        "payload": {
            "update_id": update_id,
            "update_name": update_name,
            "arguments": serializer.encode(args, codec=serializer.AVRO_CODEC),
            "payload_codec": serializer.AVRO_CODEC,
        },
    }


def _command_summary(command: CompleteUpdate | FailUpdate) -> dict[str, Any]:
    if isinstance(command, CompleteUpdate):
        server_command = command.to_server_command(TASK_QUEUE)
        result = server_command.get("result")
        return {
            "command_type": "complete_update",
            "update_id": command.update_id,
            "decoded_result": serializer.decode_envelope(result) if isinstance(result, Mapping) else None,
            "result_envelope": result,
        }

    return {
        "command_type": "fail_update",
        "update_id": command.update_id,
        "message": command.message,
        "exception_type": command.exception_type,
        "non_retryable": command.non_retryable,
    }


def _run_worker_surface(recorder: ProbeRecorder) -> None:
    complete = apply_update(
        PythonUpdateSurfaceWorkflow,
        [_accepted_event("upd-worker-complete", "accept", [True, "worker-complete"])],
        [],
        "upd-worker-complete",
        workflow_id=WORKFLOW_ID,
        run_id="run-worker",
        payload_codec=serializer.AVRO_CODEC,
    )
    recorder.check(isinstance(complete, CompleteUpdate), "accepted Python update handler must complete the update")
    if isinstance(complete, CompleteUpdate):
        summary = _command_summary(complete)
        recorder.worker_observations["accepted"] = summary
        recorder.check(
            summary["decoded_result"] == {"approved": True, "note": "worker-complete"},
            "accepted update command must encode the handler result",
        )
        recorder.mark("completed_update_result_round_trip")

    failed = apply_update(
        PythonUpdateSurfaceWorkflow,
        [_accepted_event("upd-worker-failed", "fail_update", ["worker failure"])],
        [],
        "upd-worker-failed",
        workflow_id=WORKFLOW_ID,
        run_id="run-worker",
        payload_codec=serializer.AVRO_CODEC,
    )
    recorder.check(isinstance(failed, FailUpdate), "failing Python update handler must emit fail_update")
    if isinstance(failed, FailUpdate):
        recorder.worker_observations["failed"] = _command_summary(failed)
        recorder.check(failed.exception_type == "RuntimeError", "failed update must preserve RuntimeError")
        recorder.mark("failed_update_outcome")

    unknown = apply_update(
        PythonUpdateSurfaceWorkflow,
        [_accepted_event("upd-worker-unknown", "missing_update", [])],
        [],
        "upd-worker-unknown",
        workflow_id=WORKFLOW_ID,
        run_id="run-worker",
        payload_codec=serializer.AVRO_CODEC,
    )
    recorder.check(isinstance(unknown, FailUpdate), "unknown update must emit fail_update")
    if isinstance(unknown, FailUpdate):
        recorder.worker_observations["unknown"] = _command_summary(unknown)
        recorder.typed_error("unknown_update_refusal", unknown.exception_type or "UnknownUpdate", unknown.message)
        recorder.mark("unknown_update_refusal")

    invalid = apply_update(
        PythonUpdateSurfaceWorkflow,
        [_accepted_event("upd-worker-invalid", "accept", ["not-a-bool"])],
        [],
        "upd-worker-invalid",
        workflow_id=WORKFLOW_ID,
        run_id="run-worker",
        payload_codec=serializer.AVRO_CODEC,
    )
    recorder.check(isinstance(invalid, FailUpdate), "validator refusal must emit fail_update")
    if isinstance(invalid, FailUpdate):
        recorder.worker_observations["invalid_input"] = _command_summary(invalid)
        recorder.typed_error("invalid_input_refusal", invalid.exception_type or "ValueError", invalid.message)
        recorder.mark("invalid_input_refusal")

    payload = {"nested": {"items": ["alpha", 3, True]}, "codec": "avro"}
    round_trip = apply_update(
        PythonUpdateSurfaceWorkflow,
        [_accepted_event("upd-worker-payload", "round_trip", [payload])],
        [],
        "upd-worker-payload",
        workflow_id=WORKFLOW_ID,
        run_id="run-worker",
        payload_codec=serializer.AVRO_CODEC,
    )
    recorder.check(isinstance(round_trip, CompleteUpdate), "payload round-trip update must complete")
    if isinstance(round_trip, CompleteUpdate):
        summary = _command_summary(round_trip)
        recorder.worker_observations["payload_round_trip"] = summary
        expected = {"echo": payload, "event_count": 0}
        recorder.check(summary["decoded_result"] == expected, "worker payload round trip must preserve nested payload")
        recorder.decoded_result = expected
        recorder.mark("payload_envelope_round_trip")


class _RecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        self._handler = handler
        self.requests: list[tuple[str, dict[str, Any]]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = self._handler(request)
        label = request.url.path.rsplit("/", 1)[-1]
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        self.requests.append((label, {"request": request, "response": payload}))
        return response


async def run_surface_probe() -> ProbeRecorder:
    recorder = ProbeRecorder()

    _run_worker_surface(recorder)

    duplicate_response = {
        "outcome": "accepted",
        "update_status": "accepted",
        "update_id": "upd-duplicate",
        "request_id": "duplicate-key",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        body = _request_body(request)
        path = request.url.path

        if request.method == "POST" and path == f"/api/workflows/{WORKFLOW_ID}/update/accept":
            if body.get("request_id") == "duplicate-key":
                return _response(request, 200, duplicate_response)
            if body.get("request_id") == "terminal-key":
                return _response(
                    request,
                    409,
                    {
                        "reason": "update_rejected",
                        "message": "workflow run is terminal",
                        "terminal_workflow_status": "completed",
                        "control_plane": {"operation": "update"},
                    },
                )
            if body.get("wait_for") == "accepted":
                return _response(
                    request,
                    202,
                    {
                        "outcome": "accepted",
                        "update_status": "accepted",
                        "update_id": "upd-accepted",
                    },
                )
            return _response(
                request,
                200,
                {
                    "outcome": "completed",
                    "update_status": "completed",
                    "update_id": "upd-completed",
                    "result": serializer.envelope({"approved": True, "note": "client-completed"}),
                },
            )

        if request.method == "POST" and path == f"/api/workflows/{WORKFLOW_ID}/update/fail_update":
            return _response(
                request,
                200,
                {
                    "outcome": "failed",
                    "update_status": "failed",
                    "update_id": "upd-client-failed",
                    "failure": {
                        "type": "RuntimeError",
                        "message": "client-requested failure",
                    },
                },
            )

        if request.method == "POST" and path == f"/api/workflows/{WORKFLOW_ID}/update/missing_update":
            return _response(
                request,
                409,
                {
                    "reason": "update_rejected",
                    "message": "unknown update: missing_update",
                    "control_plane": {"operation": "update"},
                },
            )

        if request.method == "POST" and path == f"/api/workflows/{WORKFLOW_ID}/update/invalid_accept":
            return _response(
                request,
                422,
                {
                    "reason": "invalid_update_arguments",
                    "message": "invalid update arguments",
                    "errors": {"approved": ["approved must be boolean"]},
                    "control_plane": {"operation": "update"},
                },
            )

        if request.method == "POST" and path == f"/api/workflows/{WORKFLOW_ID}/update/round_trip":
            input_payload = body.get("input")
            decoded_input = serializer.decode_envelope(input_payload) if isinstance(input_payload, Mapping) else []
            first = decoded_input[0] if isinstance(decoded_input, list) and decoded_input else decoded_input
            return _response(
                request,
                200,
                {
                    "outcome": "completed",
                    "update_status": "completed",
                    "update_id": "upd-round-trip",
                    "result": serializer.envelope({"echo": first, "source": "python-sdk-client"}),
                },
            )

        return _response(request, 404, {"reason": "workflow_not_found", "message": path})

    client = Client(
        "http://workflow-updates.example.invalid",
        token="workflow-updates-token",
        namespace="workflow-updates-conformance",
    )
    original_http = client._http
    transport = _RecordingTransport(handler)
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=transport, timeout=client.timeout)
    await original_http.aclose()

    try:
        accepted = await client.update_workflow(
            WORKFLOW_ID,
            "accept",
            args=[True, "client-accepted"],
            wait_for="accepted",
            request_id="accepted-key",
        )
        recorder.check(
            accepted.get("update_status") == "accepted",
            "client accepted update must return accepted status",
        )
        recorder.mark("accepted_update_control_plane_and_history")

        completed = await client.update_workflow(
            WORKFLOW_ID,
            "accept",
            args=[True, "client-completed"],
            wait_for="completed",
            request_id="completed-key",
        )
        result = completed.get("result")
        decoded = serializer.decode_envelope(result) if isinstance(result, Mapping) else None
        recorder.check(
            decoded == {"approved": True, "note": "client-completed"},
            "client completed update result envelope must decode through the public serializer",
        )
        recorder.decoded_result = decoded if isinstance(decoded, dict) else recorder.decoded_result
        recorder.mark("completed_update_result_round_trip")

        failed = await client.update_workflow(
            WORKFLOW_ID,
            "fail_update",
            args=["client-requested failure"],
            wait_for="completed",
            request_id="failed-key",
        )
        recorder.check(failed.get("update_status") == "failed", "client failed update response must report failed")
        recorder.mark("failed_update_outcome")

        duplicate_first = await client.update_workflow(
            WORKFLOW_ID,
            "accept",
            args=[True, "duplicate-first"],
            wait_for="accepted",
            request_id="duplicate-key",
        )
        duplicate_second = await client.update_workflow(
            WORKFLOW_ID,
            "accept",
            args=[True, "duplicate-second"],
            wait_for="accepted",
            request_id="duplicate-key",
        )
        recorder.check(
            duplicate_first.get("update_id") == duplicate_second.get("update_id") == "upd-duplicate",
            "duplicate request id must produce a stable duplicate update id in the client surface",
        )
        recorder.mark("duplicate_request_idempotency")

        try:
            await client.update_workflow(WORKFLOW_ID, "missing_update", args=[], request_id="unknown-key")
            recorder.check(False, "unknown update request must raise a typed SDK refusal")
        except UpdateRejected as exc:
            recorder.typed_error("unknown_update_refusal", exc.__class__.__name__, str(exc))
            recorder.mark("unknown_update_refusal")

        try:
            await client.update_workflow(WORKFLOW_ID, "invalid_accept", args=["not-a-bool"], request_id="invalid-key")
            recorder.check(False, "invalid update request must raise a typed SDK refusal")
        except InvalidArgument as exc:
            recorder.typed_error("invalid_input_refusal", exc.__class__.__name__, str(exc))
            recorder.mark("invalid_input_refusal")

        try:
            await client.update_workflow(WORKFLOW_ID, "accept", args=[True, "terminal"], request_id="terminal-key")
            recorder.check(False, "terminal workflow update request must raise a typed SDK refusal")
        except UpdateRejected as exc:
            recorder.typed_error("terminal_workflow_update_behavior", exc.__class__.__name__, str(exc))
            recorder.mark("terminal_workflow_update_behavior")

        payload = {"nested": {"items": ["alpha", 3, True]}, "codec": "avro"}
        round_trip = await client.update_workflow(
            WORKFLOW_ID,
            "round_trip",
            args=[payload],
            wait_for="completed",
            request_id="payload-key",
        )
        round_trip_result = round_trip.get("result")
        round_trip_decoded = (
            serializer.decode_envelope(round_trip_result)
            if isinstance(round_trip_result, Mapping)
            else None
        )
        recorder.check(
            round_trip_decoded == {"echo": payload, "source": "python-sdk-client"},
            "client payload round trip must preserve nested payload",
        )
        recorder.decoded_result = (
            round_trip_decoded
            if isinstance(round_trip_decoded, dict)
            else recorder.decoded_result
        )
        recorder.mark("payload_envelope_round_trip")

        for label, captured in transport.requests:
            request_obj = captured.get("request")
            response_payload = captured.get("response")
            if isinstance(request_obj, httpx.Request) and isinstance(response_payload, Mapping):
                key = label
                request_id = _request_body(request_obj).get("request_id")
                if isinstance(request_id, str) and request_id:
                    key = request_id
                recorder.client_requests[key] = _summarize_request(request_obj, response_payload)
    finally:
        await client.aclose()

    return recorder


def build_evidence(
    recorder: ProbeRecorder,
    *,
    expected_version: str = "",
    force_published_artifact: bool = False,
) -> dict[str, Any]:
    version = installed_artifact_version()
    version_mismatch = bool(expected_version) and version.lstrip("v") != expected_version.lstrip("v")
    source_checkout_used = _looks_like_source_checkout() and not force_published_artifact

    if version == "":
        recorder.failures.append("durable-workflow package metadata was not available")
    if version_mismatch:
        recorder.failures.append(
            f"installed durable-workflow version {version!r} did not match expected {expected_version!r}"
        )
    if source_checkout_used:
        recorder.failures.append(
            "durable_workflow imported from a local source checkout; "
            "published-artifact evidence requires an installed wheel or sdist"
        )

    missing_cells = [
        cell
        for cell in UPDATE_CELLS
        if cell not in recorder.covered_cells and cell not in recorder.unsupported_cells
    ]
    if missing_cells:
        recorder.failures.append(f"workflow update cells missing evidence: {', '.join(missing_cells)}")

    status = "pass" if not recorder.failures else "fail"
    artifact_source = _artifact_source(version or expected_version)
    finding = {
        "finding_id": "workflow-updates-python-sdk-surface-product-gap",
        "finding_type": "product_behavior_gap",
        "classification": "product-gap",
        "scenario_id": SCENARIO_ID,
        "owning_surface": "sdk-python",
        "summary": "; ".join(recorder.failures),
        "next_acceptance_criterion": (
            "Publish a Python SDK artifact whose workflow update surface records accepted, completed, "
            "failed, refused, duplicate, terminal, and payload round-trip cells, then rerun "
            "workflow-updates conformance."
        ),
    }

    observed_outputs = {
        "sdk_python_artifact_version": version or expected_version,
        "python_worker_update_handler": {
            "workflow_type": WORKFLOW_TYPE,
            "registered_updates": ["accept", "fail_update", "round_trip"],
            "observations": recorder.worker_observations,
        },
        "python_client_update_request": {
            "workflow_id": WORKFLOW_ID,
            "requests": recorder.client_requests,
        },
        "covered_cells": sorted(recorder.covered_cells),
        "unsupported_cells": sorted(recorder.unsupported_cells),
        "typed_errors": recorder.typed_errors,
        "artifact_source": artifact_source,
        "pypi_package": "durable-workflow",
        "pypi_artifact_verified": status == "pass",
        "python_runtime": sys.version.split()[0],
        "package_import_path": _package_import_path(),
        "evidence_method": "pypi_installed_artifact_runtime_import_client_transport_and_worker_update_execution",
        "sdk_decoded_result": recorder.decoded_result,
        "failures": recorder.failures,
        "published_artifact_cell_executed": not source_checkout_used,
        "local_product_source_checkouts_used": source_checkout_used,
    }

    return {
        "schema": SCHEMA,
        "generated_at": _now_iso(),
        "runner": "published-python-sdk-workflow-updates-surface-probe",
        "runner_blocked": False,
        "source_policy": {
            "pass_requires_published_artifacts_only": True,
            "local_product_source_checkouts_used": source_checkout_used,
            "local_checkout_execution_counts_as_pass": False,
        },
        "scenario_results": {
            SCENARIO_ID: {
                "scenario_id": SCENARIO_ID,
                "status": status,
                "classification": "product-evidence" if status == "pass" else "product-gap",
                "published_artifact_cell_executed": not source_checkout_used,
                "local_product_source_checkouts_used": source_checkout_used,
                "observed_outputs": observed_outputs,
                "linked_findings": [] if status == "pass" else [finding],
            },
        },
        "findings": [] if status == "pass" else [finding],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Emit Python SDK workflow-update conformance shard evidence.")
    parser.add_argument(
        "--output",
        type=Path,
        help=f"Write evidence JSON to this path. Defaults to stdout; host runners commonly use {DEFAULT_OUTPUT_FILE}.",
    )
    parser.add_argument("--expected-version", default="", help="Expected durable-workflow package version.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args(argv)

    evidence = build_evidence(
        asyncio.run(run_surface_probe()),
        expected_version=args.expected_version,
    )

    text = json.dumps(evidence, indent=2 if args.pretty else None, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)

    scenario = evidence["scenario_results"][SCENARIO_ID]
    return 0 if scenario["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
