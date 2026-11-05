-- Unexecuted MySQL 8.0 source proposal; fixture namespace and original IDs only.
-- Parameter binding, query deadline and row validation are not implemented here.
SELECT
    UTC_TIMESTAMP(6) AS database_time_utc,
    t.id AS original_task_id,
    t.workflow_run_id AS original_run_id,
    t.namespace,
    t.task_type,
    t.status,
    t.queue,
    t.compatibility,
    t.available_at,
    t.leased_at,
    t.lease_expires_at,
    t.lease_owner IS NOT NULL AS lease_owner_present,
    t.attempt_count,
    t.repair_count,
    t.last_dispatch_attempt_at,
    t.last_dispatched_at,
    t.last_claim_failed_at,
    t.repair_available_at,
    t.last_dispatch_error IS NOT NULL AS dispatch_error_present,
    t.last_claim_error IS NOT NULL AS claim_error_present,
    t.last_error IS NOT NULL AS task_error_present
FROM workflow_tasks AS t
WHERE CAST(t.namespace AS BINARY) = CAST(:namespace AS BINARY)
  AND CAST(t.workflow_run_id AS BINARY) = CAST(:original_run_id AS BINARY)
  AND CAST(t.id AS BINARY) = CAST(:original_task_id AS BINARY)
LIMIT 2;
