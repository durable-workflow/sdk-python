"""Exact logical delta for this fixture's two native SQLite fresh admissions.

Derived from published5614274d runtime/{store,updates,wait_projection,
terminal_summary,command_context}.rs. No Worker executes in this interval.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timezone

ULID = re.compile(r"[0-9A-HJKMNP-TV-Z]{26}")


def value(cell):
    if cell == ["null"]:
        return None
    if len(cell) != 2 or cell[0] not in {"text", "integer"}:
        raise ValueError("unexpected case-delta field type")
    return cell[1]


def typed(item):
    if item is None:
        return ["null"]
    return ["integer" if type(item) is int else "text", item]


def timestamp(item):
    parsed = datetime.fromisoformat(item.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def rows(snapshot, table):
    return [
        dict(zip(snapshot["tables"][table]["columns"], row, strict=True)) for row in snapshot["tables"][table]["rows"]
    ]


def one(snapshot, table, field, expected):
    found = [row for row in rows(snapshot, table) if value(row[field]) == expected]
    if len(found) != 1:
        raise ValueError("case delta needs one exact " + table + " identity")
    return found[0]


def stable_catalog(before, after):
    if (
        before["schema"] != after["schema"]
        or before["user_version"] != after["user_version"]
        or before["application_id"] != after["application_id"]
        or set(before["tables"]) != set(after["tables"])
    ):
        raise ValueError("case interval changed schema or table inventory")
    if any(before["tables"][name]["columns"] != after["tables"][name]["columns"] for name in before["tables"]):
        raise ValueError("case interval changed columns")


def finish_expected(expected, after):
    for table in expected["tables"].values():
        table["rows"].sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True))
    if expected != after:
        raise ValueError("case interval changed rows outside the exact admission delta")


def validate_restart(stopped, restarted):
    if stopped != restarted:
        raise ValueError("first restarted snapshot differs from the stopped copy")


def validate(before, after, seed, proof):
    stable_catalog(before, after)
    expected = deepcopy(before)
    run_id, workflow_id = seed["run_id"], seed["workflow_id"]
    run = one(before, "workflow_runs", "id", run_id)
    instance = one(before, "workflow_instances", "id", workflow_id)
    first_sequence = value(run["last_history_sequence"])
    events = proof["events"]
    cells = proof["cells"]
    if len(events) != 2 or [cell["kind"] for cell in cells] != ["signal", "update"]:
        raise ValueError("case interval lacks its two public admission proofs")
    began, ended = timestamp(proof["started_at"]), timestamp(proof["finished_at"])

    def valid_time(observed, authoritative):
        return began <= timestamp(value(observed)) == timestamp(authoritative) <= ended

    def append_row(table, old, new, changes, predicates=None):
        candidate = deepcopy(old)
        for field, item in changes.items():
            candidate[field] = typed(item)
        for field, predicate in (predicates or {}).items():
            if not predicate(new[field]):
                raise ValueError("case admission field differs: " + table + "." + field)
            candidate[field] = new[field]
        if candidate != new:
            raise ValueError("case admission changed unexpected new-row fields: " + table)
        expected["tables"][table]["rows"].append([new[column] for column in expected["tables"][table]["columns"]])

    def update_counter(table, identity, increments):
        original = one(before, table, "id", identity)
        target = expected["tables"][table]
        for row in target["rows"]:
            if row[target["columns"].index("id")] == original["id"]:
                for field, amount in increments.items():
                    row[target["columns"].index(field)] = typed(value(original[field]) + amount)
                return
        raise ValueError("missing original sequence row")

    event_bytes = 0
    for offset, (cell, event) in enumerate(zip(cells, events, strict=True), start=1):
        kind = cell["kind"]
        original = seed["commands"][kind]["original"]
        receipt = cell["receipt"]
        command_id = receipt["command_id"]
        if command_id == original["command_id"] or receipt["run_id"] != run_id:
            raise ValueError("case-distinct receipt selected an original or foreign run")
        payload = event["payload"]
        command = payload["command"]
        command_sequence = value(run["last_command_sequence"]) + offset
        message_sequence = value(instance["last_message_sequence"]) + offset
        if (
            event["sequence"] != first_sequence + offset
            or payload["workflow_command_id"] != command_id
            or command["id"] != command_id
            or command["sequence"] != command_sequence
            or command["message_sequence"] != message_sequence
        ):
            raise ValueError("new history sequence/receipt binding differs")
        old_command = one(before, "workflow_commands", "id", original["command_id"])
        new_command = one(after, "workflow_commands", "id", command_id)
        old_context = json.loads(value(old_command["context"]))
        new_context = json.loads(value(new_command["context"]))
        operation = new_context["server"]["operation_id"]
        if not ULID.fullmatch(operation) or operation == old_context["server"]["operation_id"]:
            raise ValueError("new admission operation identity is invalid")
        context = deepcopy(old_context)
        context["server"]["operation_id"] = operation
        context["server"]["metadata"]["request_id"] = "casesensitive"
        context["request"]["request_id"] = "casesensitive"
        context["request"]["fingerprint"] = cell["request_fingerprint"]
        if new_context != context:
            raise ValueError("case interval changed unexpected command attribution")
        if command["request_fingerprint"] != cell["request_fingerprint"]:
            raise ValueError("stored request fingerprint differs from installed sender")
        append_row(
            "workflow_commands",
            old_command,
            new_command,
            {
                "id": command_id,
                "request_id": kind + "-v1:" + hashlib.sha256(b"casesensitive").hexdigest(),
                "command_sequence": command_sequence,
                "message_sequence": message_sequence,
                "payload": command["payload"]["blob"],
                "context": value(new_command["context"]),
            },
            {"accepted_at": lambda actual, at=command["accepted_at"]: valid_time(actual, at)},
        )
        intent = one(after, "dw_command_intents", "command_id", command_id)
        if (
            set(intent) != {"command_id", "namespace", "intent_digest"}
            or value(intent["namespace"]) != value(instance["namespace"])
            or re.fullmatch(r"[0-9a-f]{64}", value(intent["intent_digest"])) is None
            or cell["same_intent_retained"] is not True
            or cell["changed_intent_conflict"] is not True
        ):
            raise ValueError("new intent row is not bound to the verified fresh admission")
        expected["tables"]["dw_command_intents"]["rows"].append(
            [intent[column] for column in expected["tables"]["dw_command_intents"]["columns"]]
        )
        old_event = one(before, "workflow_history_events", "workflow_command_id", original["command_id"])
        new_event = one(after, "workflow_history_events", "workflow_command_id", command_id)
        if not ULID.fullmatch(value(new_event["id"])) or new_event["id"] == old_event["id"]:
            raise ValueError("invalid new history identity")
        old_payload = json.loads(value(old_event["payload"]))
        desired = deepcopy(old_payload)
        desired.update(workflow_command_id=command_id, arguments=payload["arguments"], command=command)
        if kind == "signal":
            desired.update(signal_id=payload["signal_id"], signal_wait_id="signal-command:" + command_id)
        else:
            desired.update(update_id=receipt["update_id"], request_id="casesensitive")
        if desired != payload or json.loads(value(new_event["payload"])) != payload:
            raise ValueError("new history payload differs from the declared source-derived event")
        append_row(
            "workflow_history_events",
            old_event,
            new_event,
            {
                "id": value(new_event["id"]),
                "sequence": first_sequence + offset,
                "workflow_command_id": command_id,
                "payload": value(new_event["payload"]),
            },
            {"recorded_at": lambda actual, at=event["timestamp"]: valid_time(actual, at)},
        )
        # This fixture uses only ASCII strings, small integers, booleans/null
        # and opaque codec strings. Native's PHP-shaped event sizing is ordinary
        # compact JSON for these values (no numeric-key maps/floats/RawValue).
        event_bytes += len(event["event_type"].encode()) + len(
            json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()
        )
        if kind == "signal":
            old = one(before, "workflow_signal_records", "workflow_command_id", original["command_id"])
            new = one(after, "workflow_signal_records", "workflow_command_id", command_id)
            append_row(
                "workflow_signal_records",
                old,
                new,
                {
                    "id": payload["signal_id"],
                    "workflow_command_id": command_id,
                    "command_sequence": command_sequence,
                    "signal_wait_id": "signal-command:" + command_id,
                    "arguments": payload["arguments"]["blob"],
                },
                {"received_at": lambda actual, at=command["accepted_at"]: valid_time(actual, at)},
            )
        else:
            old = one(before, "workflow_updates", "id", original["update_id"])
            new = one(after, "workflow_updates", "id", receipt["update_id"])
            append_row(
                "workflow_updates",
                old,
                new,
                {
                    "id": receipt["update_id"],
                    "workflow_command_id": command_id,
                    "command_sequence": command_sequence,
                    "arguments": payload["arguments"]["blob"],
                },
                {"accepted_at": lambda actual, at=receipt["accepted_at"]: valid_time(actual, at)},
            )
            old_wait = one(before, "workflow_run_waits", "resume_source_id", original["update_id"])
            wait_id = "update:" + receipt["update_id"]
            new_wait = one(after, "workflow_run_waits", "resume_source_id", receipt["update_id"])
            append_row(
                "workflow_run_waits",
                old_wait,
                new_wait,
                {
                    "id": hashlib.sha256((run_id + "|" + wait_id).encode()).hexdigest(),
                    "wait_id": wait_id,
                    "position": len(rows(before, "workflow_run_waits")),
                    "resume_source_id": receipt["update_id"],
                    "command_id": command_id,
                    "command_sequence": command_sequence,
                },
                {
                    field: (lambda actual, at=event["timestamp"]: valid_time(actual, at))
                    for field in ("opened_at", "created_at", "updated_at")
                },
            )
    update_counter("workflow_runs", run_id, {"last_command_sequence": 2, "last_history_sequence": 2})
    update_counter("workflow_instances", workflow_id, {"last_message_sequence": 2})
    if any(value(row["id"]) == run_id for row in rows(before, "workflow_run_summaries")):
        update_counter("workflow_run_summaries", run_id, {"history_event_count": 2, "history_size_bytes": event_bytes})
    finish_expected(expected, after)
    return {
        "new_commands": 2,
        "new_intents": 2,
        "new_signals": 1,
        "new_updates": 1,
        "new_history_events": 2,
        "new_waits": 1,
        "history_bytes_added": event_bytes,
        "all_other_rows_preserved": True,
    }
