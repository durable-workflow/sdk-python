-- Separate from the task read: a missing task/run cannot disappear in a join.
-- This is an unexecuted proposal, not an installed comparator or driver.
SELECT
    UTC_TIMESTAMP(6) AS database_time_utc,
    r.id AS original_run_id,
    r.workflow_instance_id,
    r.namespace,
    r.status,
    r.execution_deadline_at,
    r.run_deadline_at
FROM workflow_runs AS r
WHERE CAST(r.namespace AS BINARY) = CAST(:namespace AS BINARY)
  AND CAST(r.workflow_instance_id AS BINARY) = CAST(:workflow_instance_id AS BINARY)
  AND CAST(r.id AS BINARY) = CAST(:original_run_id AS BINARY)
LIMIT 2;
