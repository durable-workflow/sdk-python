from __future__ import annotations

import asyncio

from durable_workflow.workflow_updates_conformance import (
    SCENARIO_ID,
    UPDATE_CELLS,
    build_evidence,
    installed_artifact_version,
    run_surface_probe,
)


def test_workflow_updates_surface_probe_records_required_python_cells() -> None:
    recorder = asyncio.run(run_surface_probe())
    evidence = build_evidence(
        recorder,
        expected_version=installed_artifact_version(),
        force_published_artifact=True,
    )

    scenario = evidence["scenario_results"][SCENARIO_ID]
    outputs = scenario["observed_outputs"]

    assert scenario["status"] == "pass"
    assert scenario["published_artifact_cell_executed"] is True
    assert scenario["local_product_source_checkouts_used"] is False
    assert set(UPDATE_CELLS).issubset(set(outputs["covered_cells"]))
    assert outputs["unsupported_cells"] == []
    assert outputs["python_worker_update_handler"]["observations"]["accepted"]["command_type"] == "complete_update"
    assert outputs["python_worker_update_handler"]["observations"]["failed"]["command_type"] == "fail_update"
    assert "accepted-key" in outputs["python_client_update_request"]["requests"]
    assert "duplicate-key" in outputs["python_client_update_request"]["requests"]
    assert any(error["cell"] == "unknown_update_refusal" for error in outputs["typed_errors"])
    assert any(error["cell"] == "terminal_workflow_update_behavior" for error in outputs["typed_errors"])
