#!/usr/bin/env python3
"""Repeatable Avro Value protocol size and latency benchmark.

The benchmark compares compact JSON, the prerelease JSON-in-Avro wrapper, and
the fixed typed Value schema.  The production-path regression budget covers the
fixed codec only; the old wrapper remains here solely as migration evidence.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import statistics
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from durable_workflow._avro import (
    decode,
    encode,
)

OLD_WRAPPER_SCHEMA = parse_schema(
    {
        "type": "record",
        "name": "Payload",
        "namespace": "durable_workflow.protocol.prerelease",
        "fields": [{"name": "json", "type": "string"}, {"name": "version", "type": "int"}],
    }
)
CORPUS_PATH = Path(__file__).parents[1] / "schema" / "avro-value-benchmark-v1.json"
CORPUS_BYTES = CORPUS_PATH.read_bytes()
CORPUS = json.loads(CORPUS_BYTES)
JSON_SAMPLE: dict[str, Any] = CORPUS["value"]


def _adapt_bytes(value: Any) -> Any:
    if isinstance(value, dict) and list(value) == ["$avro_bytes"]:
        return base64.b64decode(value["$avro_bytes"], validate=True)
    if isinstance(value, dict):
        return {key: _adapt_bytes(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_adapt_bytes(item) for item in value]
    return value


SAMPLE: dict[str, Any] = _adapt_bytes(JSON_SAMPLE)


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"$type": "bytes", "base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value


def _compact_json(value: Any) -> bytes:
    return json.dumps(_json_value(value), ensure_ascii=False, separators=(",", ":")).encode()


def _old_encode(value: Any) -> bytes:
    datum = {"json": _compact_json(value).decode(), "version": 1}
    stream = io.BytesIO()
    stream.write(b"\x00")
    schemaless_writer(stream, OLD_WRAPPER_SCHEMA, datum)
    return stream.getvalue()


def _old_decode(payload: bytes) -> Any:
    datum = schemaless_reader(io.BytesIO(payload[1:]), OLD_WRAPPER_SCHEMA)
    return json.loads(datum["json"])


def _measure(action: Callable[[], Any], iterations: int) -> float:
    samples: list[float] = []
    for _ in range(5):
        started = time.perf_counter_ns()
        for _ in range(iterations):
            action()
        samples.append((time.perf_counter_ns() - started) / iterations / 1_000)
    return statistics.median(samples)


def _http_envelope_size(blob: str) -> int:
    return len(
        json.dumps({"codec": "avro", "blob": blob}, separators=(",", ":")).encode()
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=1_000)
    parser.add_argument("--enforce", action="store_true")
    args = parser.parse_args()

    json_bytes = _compact_json(JSON_SAMPLE)
    old_payload = _old_encode(JSON_SAMPLE)
    typed_blob = encode(SAMPLE)
    typed_payload = base64.b64decode(typed_blob)

    results = {
        "implementation": "fastavro",
        "corpus": {
            "schema": CORPUS["schema"],
            "case": CORPUS["case"],
            "sha256": hashlib.sha256(CORPUS_BYTES).hexdigest(),
        },
        "iterations": args.iterations,
        "sizes_bytes": {
            "plain_json": {
                "raw": len(json_bytes),
                "http_envelope": len(
                    json.dumps(
                        {"codec": "json", "blob": json_bytes.decode()},
                        separators=(",", ":"),
                    ).encode()
                ),
            },
            "old_json_wrapper": {
                "raw_datum": len(old_payload) - 1,
                "framed": len(old_payload),
                "http_envelope": _http_envelope_size(base64.b64encode(old_payload).decode()),
            },
            "fixed_typed_value": {
                "raw_datum": len(typed_payload) - 10,
                "single_object": len(typed_payload),
                "http_envelope": _http_envelope_size(typed_blob),
            },
        },
        "latency_us": {
            "plain_json_encode": _measure(
                lambda: _compact_json(JSON_SAMPLE), args.iterations
            ),
            "plain_json_decode": _measure(lambda: json.loads(json_bytes), args.iterations),
            "old_json_wrapper_encode": _measure(
                lambda: _old_encode(JSON_SAMPLE), args.iterations
            ),
            "old_json_wrapper_decode": _measure(
                lambda: _old_decode(old_payload), args.iterations
            ),
            "fixed_typed_value_encode": _measure(lambda: encode(SAMPLE), args.iterations),
            "fixed_typed_value_decode": _measure(lambda: decode(typed_blob), args.iterations),
        },
    }
    print(json.dumps(results, indent=2, sort_keys=True))

    if not args.enforce:
        return 0

    encode_budget = float(os.getenv("AVRO_VALUE_ENCODE_BUDGET_US", "125"))
    decode_budget = float(os.getenv("AVRO_VALUE_DECODE_BUDGET_US", "100"))
    failed = (
        results["latency_us"]["fixed_typed_value_encode"] > encode_budget
        or results["latency_us"]["fixed_typed_value_decode"] > decode_budget
    )
    if failed:
        print(
            "Avro Value production-path regression budget exceeded: "
            f"encode <= {encode_budget:g} us, decode <= {decode_budget:g} us."
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
