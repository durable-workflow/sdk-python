from __future__ import annotations

import base64

from benchmarks import avro_value


class FakeClock:
    def __init__(self) -> None:
        self.nanoseconds = 0

    def __call__(self) -> int:
        return self.nanoseconds

    def advance(self, nanoseconds: int) -> None:
        self.nanoseconds += nanoseconds


def test_material_intentional_codec_slowdown_fails_relative_guard() -> None:
    clock = FakeClock()

    def calibrated_fastavro_work() -> None:
        clock.advance(100)

    def intentionally_slow_codec() -> None:
        clock.advance(400)

    reference, candidate = avro_value._measure_pair(
        calibrated_fastavro_work,
        intentionally_slow_codec,
        iterations=10,
        sample_count=7,
        warmup_iterations=3,
        clock=clock,
    )
    guard = {
        "fixed_typed_value_decode": avro_value._ratio_summary(
            candidate,
            reference,
            budget=2.0,
        )
    }

    assert guard["fixed_typed_value_decode"]["samples_ratio"] == [4.0] * 7
    assert avro_value._relative_failures(guard) == ["fixed_typed_value_decode median ratio 4.000 exceeded 2.000"]


def test_checked_in_benchmark_wire_and_sizes_match_hard_contract() -> None:
    typed_blob = avro_value._avro.encode(avro_value.SAMPLE)
    typed_payload = base64.b64decode(typed_blob)
    json_bytes = avro_value._compact_json(avro_value.JSON_SAMPLE)
    old_payload = avro_value._old_encode(avro_value.JSON_SAMPLE)
    sizes = {
        "plain_json": {
            "raw": len(json_bytes),
            "http_envelope": len(
                avro_value.json.dumps(
                    {"codec": "avro", "blob": json_bytes.decode()},
                    separators=(",", ":"),
                ).encode()
            ),
        },
        "old_json_wrapper": {
            "raw_datum": len(old_payload) - 1,
            "framed": len(old_payload),
            "http_envelope": avro_value._http_envelope_size(base64.b64encode(old_payload).decode()),
        },
        "fixed_typed_value": {
            "raw_datum": len(typed_payload) - 10,
            "single_object": len(typed_payload),
            "http_envelope": avro_value._http_envelope_size(typed_blob),
        },
    }

    assert (
        avro_value._wire_failures(
            corpus_sha256=avro_value.hashlib.sha256(avro_value.CORPUS_BYTES).hexdigest(),
            typed_blob=typed_blob,
            typed_payload=typed_payload,
            sizes=sizes,
        )
        == []
    )
