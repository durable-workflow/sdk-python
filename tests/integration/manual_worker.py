"""Capabilities for integration fixtures that drive tasks without Worker.run()."""

MANUAL_WORKER_CAPABILITY_MANIFEST = {
    "local_activities": {
        "supported": False,
        "minimum_protocol_version": "1.18",
        "reason": "manual_worker_does_not_execute_record_local_activity",
    },
    "worker_sessions": {
        "supported": False,
        "minimum_protocol_version": "1.18",
        "reason": "manual_worker_has_no_session_lifecycle",
    },
    "sticky_execution": {
        "supported": False,
        "minimum_protocol_version": "1.18",
        "reason": "manual_worker_uses_complete_durable_history_replay",
    },
}
