#!/usr/bin/env python3
"""Repeatable Avro Value protocol size and latency benchmark.

The generic CI guard compares the production codec with direct typed fastavro
work measured in the same warmed sample pair.  That ratio preserves regression
sensitivity without treating transient host contention as a codec regression.
Absolute timings remain available for a dedicated advisory benchmark job.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import math
import os
import statistics
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from durable_workflow import _avro

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

EXPECTED_CORPUS_SHA256 = "588771404977f2a95fe7d8969c24a15e1c7dd78fe498af9aa2406f82be54b666"
EXPECTED_TYPED_WIRE_SHA256 = "8e0ec373f4a8163830bc319cbc55e242e35bed2e7fd4dba2d7ba12fa62bd42e3"
EXPECTED_SIZES_BYTES = {
    "plain_json": {"raw": 193, "http_envelope": 249},
    "old_json_wrapper": {"raw_datum": 196, "framed": 197, "http_envelope": 290},
    "fixed_typed_value": {"raw_datum": 154, "single_object": 164, "http_envelope": 246},
}

Clock = Callable[[], int]


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


def _measure_sample(action: Callable[[], Any], iterations: int, clock: Clock) -> float:
    started = clock()
    for _ in range(iterations):
        action()
    return (clock() - started) / iterations / 1_000


def _warm(action: Callable[[], Any], iterations: int) -> None:
    for _ in range(iterations):
        action()


def _measure(
    action: Callable[[], Any],
    *,
    iterations: int,
    sample_count: int,
    warmup_iterations: int,
    clock: Clock = time.perf_counter_ns,
) -> list[float]:
    _warm(action, warmup_iterations)
    return [_measure_sample(action, iterations, clock) for _ in range(sample_count)]


def _measure_pair(
    reference: Callable[[], Any],
    candidate: Callable[[], Any],
    *,
    iterations: int,
    sample_count: int,
    warmup_iterations: int,
    clock: Clock = time.perf_counter_ns,
) -> tuple[list[float], list[float]]:
    """Measure paired samples, alternating order to limit load and drift bias."""
    _warm(reference, warmup_iterations)
    _warm(candidate, warmup_iterations)
    reference_samples: list[float] = []
    candidate_samples: list[float] = []
    for index in range(sample_count):
        if index % 2:
            candidate_sample = _measure_sample(candidate, iterations, clock)
            reference_sample = _measure_sample(reference, iterations, clock)
        else:
            reference_sample = _measure_sample(reference, iterations, clock)
            candidate_sample = _measure_sample(candidate, iterations, clock)
        reference_samples.append(reference_sample)
        candidate_samples.append(candidate_sample)
    return reference_samples, candidate_samples


def _percentile(samples: list[float], percentile: float) -> float:
    ordered = sorted(samples)
    index = max(0, math.ceil(percentile / 100 * len(ordered)) - 1)
    return ordered[index]


def _sample_summary(samples: list[float], *, unit: str = "us") -> dict[str, Any]:
    return {
        f"samples_{unit}": [round(sample, 6) for sample in samples],
        f"median_{unit}": round(statistics.median(samples), 6),
        f"p95_{unit}": round(_percentile(samples, 95), 6),
    }


def _ratio_summary(candidate_samples: list[float], reference_samples: list[float], budget: float) -> dict[str, Any]:
    ratios = [candidate / reference for candidate, reference in zip(candidate_samples, reference_samples, strict=True)]
    median = statistics.median(ratios)
    return {
        **_sample_summary(ratios, unit="ratio"),
        "budget_ratio": budget,
        "passed": median <= budget,
    }


def _http_envelope_size(blob: str) -> int:
    return len(json.dumps({"codec": "avro", "blob": blob}, separators=(",", ":")).encode())


def _wire_failures(
    *,
    corpus_sha256: str,
    typed_blob: str,
    typed_payload: bytes,
    sizes: dict[str, dict[str, int]],
) -> list[str]:
    failures: list[str] = []
    if corpus_sha256 != EXPECTED_CORPUS_SHA256:
        failures.append(f"corpus SHA-256 changed: expected {EXPECTED_CORPUS_SHA256}, measured {corpus_sha256}")
    wire_sha256 = hashlib.sha256(typed_payload).hexdigest()
    if wire_sha256 != EXPECTED_TYPED_WIRE_SHA256:
        failures.append(
            f"fixed typed wire SHA-256 changed: expected {EXPECTED_TYPED_WIRE_SHA256}, measured {wire_sha256}"
        )
    decoded = _avro.decode(typed_blob)
    if decoded != SAMPLE:
        failures.append("fixed typed wire no longer decodes to the benchmark corpus")
    if _avro.encode(decoded) != typed_blob:
        failures.append("fixed typed wire no longer re-encodes byte-for-byte")
    if sizes != EXPECTED_SIZES_BYTES:
        failures.append(
            "benchmark wire sizes changed: "
            f"expected {json.dumps(EXPECTED_SIZES_BYTES, sort_keys=True)}, "
            f"measured {json.dumps(sizes, sort_keys=True)}"
        )
    return failures


def _relative_failures(relative_guard: dict[str, dict[str, Any]]) -> list[str]:
    return [
        f"{name} median ratio {measurement['median_ratio']:.3f} exceeded {measurement['budget_ratio']:.3f}"
        for name, measurement in relative_guard.items()
        if not measurement["passed"]
    ]


def _absolute_failures(absolute_guard: dict[str, dict[str, Any]]) -> list[str]:
    return [
        f"{name} median {measurement['median_us']:.3f} us exceeded {measurement['budget_us']:.3f} us"
        for name, measurement in absolute_guard.items()
        if not measurement["passed"]
    ]


def _append_github_summary(results: dict[str, Any]) -> None:
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    latency = results["latency_us"]
    relative = results["relative_guard"]
    absolute = results["absolute_guard"]
    lines = [
        "### Avro Value benchmark",
        "",
        "| Path | Median (µs) | p95 (µs) | Relative median | Relative budget | Absolute budget (µs) |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for operation in ("encode", "decode"):
        name = f"fixed_typed_value_{operation}"
        lines.append(
            f"| {operation} | {latency[name]['median_us']:.3f} | {latency[name]['p95_us']:.3f} | "
            f"{relative[name]['median_ratio']:.3f} | {relative[name]['budget_ratio']:.3f} | "
            f"{absolute[name]['budget_us']:.3f} |"
        )
    failures = results["enforcement"]["failures"]
    if failures:
        lines.extend(["", "Guard findings:", *[f"- {failure}" for failure in failures]])
    with Path(summary_path).open("a", encoding="utf-8") as summary:
        summary.write("\n".join(lines) + "\n")


def _positive(parser: argparse.ArgumentParser, name: str, value: int) -> None:
    if value <= 0:
        parser.error(f"{name} must be greater than zero")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=1_000)
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--warmup-iterations", type=int, default=100)
    parser.add_argument("--enforce", action="store_true", help="enforce the calibrated relative codec guard")
    parser.add_argument(
        "--enforce-absolute",
        action="store_true",
        help="enforce advisory absolute latency budgets for a dedicated benchmark job",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    _positive(parser, "--iterations", args.iterations)
    _positive(parser, "--samples", args.samples)
    _positive(parser, "--warmup-iterations", args.warmup_iterations)

    encode_ratio_budget = float(os.getenv("AVRO_VALUE_ENCODE_RATIO_BUDGET", "1.75"))
    decode_ratio_budget = float(os.getenv("AVRO_VALUE_DECODE_RATIO_BUDGET", "2.00"))
    encode_absolute_budget = float(os.getenv("AVRO_VALUE_ENCODE_BUDGET_US", "125"))
    decode_absolute_budget = float(os.getenv("AVRO_VALUE_DECODE_BUDGET_US", "100"))

    json_bytes = _compact_json(JSON_SAMPLE)
    old_payload = _old_encode(JSON_SAMPLE)
    typed_blob = _avro.encode(SAMPLE)
    typed_payload = base64.b64decode(typed_blob)
    typed_schema = _avro._load_avro_schema()
    typed_datum = _avro._to_datum(SAMPLE)
    typed_datum_payload = typed_payload[10:]

    def direct_typed_encode() -> bytes:
        stream = io.BytesIO()
        schemaless_writer(stream, typed_schema, typed_datum)
        return stream.getvalue()

    def direct_typed_decode() -> Any:
        return schemaless_reader(io.BytesIO(typed_datum_payload), typed_schema, typed_schema)

    measurement_options = {
        "iterations": args.iterations,
        "sample_count": args.samples,
        "warmup_iterations": args.warmup_iterations,
    }
    direct_encode_samples, codec_encode_samples = _measure_pair(
        direct_typed_encode, lambda: _avro.encode(SAMPLE), **measurement_options
    )
    direct_decode_samples, codec_decode_samples = _measure_pair(
        direct_typed_decode, lambda: _avro.decode(typed_blob), **measurement_options
    )
    sizes = {
        "plain_json": {
            "raw": len(json_bytes),
            "http_envelope": len(
                json.dumps({"codec": "json", "blob": json_bytes.decode()}, separators=(",", ":")).encode()
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
    }
    corpus_sha256 = hashlib.sha256(CORPUS_BYTES).hexdigest()
    wire_failures = _wire_failures(
        corpus_sha256=corpus_sha256,
        typed_blob=typed_blob,
        typed_payload=typed_payload,
        sizes=sizes,
    )
    latency = {
        "plain_json_encode": _sample_summary(_measure(lambda: _compact_json(JSON_SAMPLE), **measurement_options)),
        "plain_json_decode": _sample_summary(_measure(lambda: json.loads(json_bytes), **measurement_options)),
        "old_json_wrapper_encode": _sample_summary(_measure(lambda: _old_encode(JSON_SAMPLE), **measurement_options)),
        "old_json_wrapper_decode": _sample_summary(_measure(lambda: _old_decode(old_payload), **measurement_options)),
        "fastavro_typed_encode_calibration": _sample_summary(direct_encode_samples),
        "fixed_typed_value_encode": _sample_summary(codec_encode_samples),
        "fastavro_typed_decode_calibration": _sample_summary(direct_decode_samples),
        "fixed_typed_value_decode": _sample_summary(codec_decode_samples),
    }
    relative_guard = {
        "fixed_typed_value_encode": _ratio_summary(codec_encode_samples, direct_encode_samples, encode_ratio_budget),
        "fixed_typed_value_decode": _ratio_summary(codec_decode_samples, direct_decode_samples, decode_ratio_budget),
    }
    absolute_guard = {
        "fixed_typed_value_encode": {
            **latency["fixed_typed_value_encode"],
            "budget_us": encode_absolute_budget,
            "passed": latency["fixed_typed_value_encode"]["median_us"] <= encode_absolute_budget,
        },
        "fixed_typed_value_decode": {
            **latency["fixed_typed_value_decode"],
            "budget_us": decode_absolute_budget,
            "passed": latency["fixed_typed_value_decode"]["median_us"] <= decode_absolute_budget,
        },
    }
    failures = list(wire_failures)
    if args.enforce:
        failures.extend(_relative_failures(relative_guard))
    if args.enforce_absolute:
        failures.extend(_absolute_failures(absolute_guard))

    results = {
        "implementation": "fastavro",
        "corpus": {
            "schema": CORPUS["schema"],
            "case": CORPUS["case"],
            "sha256": corpus_sha256,
        },
        "measurement": {
            "iterations_per_sample": args.iterations,
            "sample_count": args.samples,
            "warmup_iterations": args.warmup_iterations,
            "paired_order": "alternating",
        },
        "sizes_bytes": sizes,
        "wire_contract": {
            "typed_wire_sha256": hashlib.sha256(typed_payload).hexdigest(),
            "passed": not wire_failures,
        },
        "latency_us": latency,
        "relative_guard": relative_guard,
        "absolute_guard": absolute_guard,
        "enforcement": {
            "relative": args.enforce,
            "absolute": args.enforce_absolute,
            "failures": failures,
            "passed": not failures,
        },
    }
    rendered = json.dumps(results, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    _append_github_summary(results)

    if failures:
        failed_summary = {
            name: {
                "samples_us": latency[name]["samples_us"],
                "median_us": latency[name]["median_us"],
                "p95_us": latency[name]["p95_us"],
                "relative_samples": relative_guard[name]["samples_ratio"],
                "relative_median": relative_guard[name]["median_ratio"],
            }
            for name in ("fixed_typed_value_encode", "fixed_typed_value_decode")
        }
        print(
            "Avro Value benchmark guard failed. Measured sample summary: " + json.dumps(failed_summary, sort_keys=True),
            file=sys.stderr,
        )
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
