#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generic_benchmark import GENERIC_WORKLOADS, minimum_auto_speedup, policy_order, verification_failures


class TestPolicyOrder(unittest.TestCase):
    def test_reverses_each_successive_pair(self) -> None:
        self.assertEqual(
            [policy_order(repeat) for repeat in range(1, 6)],
            [("off", "auto"), ("auto", "off"), ("off", "auto"), ("auto", "off"), ("off", "auto")],
        )

    def test_rejects_invalid_repeat(self) -> None:
        with self.assertRaises(ValueError):
            policy_order(0)


class TestSpeedupFloors(unittest.TestCase):
    def test_complementary_string_join_t4_floor_tracks_promoted_result(self) -> None:
        workload = next(
            workload for workload in GENERIC_WORKLOADS if workload["name"] == "join_string_complementary_grouped_sum"
        )
        self.assertEqual(minimum_auto_speedup(workload, 4), 1.24)
        self.assertEqual(
            workload["required_runtime_paths"],
            (
                "hash_join_probe.perfect_probe.direct_aggregate_consumer.compressed_uhugeint_predicate=",
                "hash_join_probe.perfect_probe.direct_aggregate_consumer.inline_string_identity_known_groups=",
                "hash_join_probe.perfect_probe.direct_aggregate_consumer.derived_build_index.contiguous_source=",
            ),
        )

    def test_scan_filter_t4_floor_tracks_source_filter_promotion(self) -> None:
        workload = next(workload for workload in GENERIC_WORKLOADS if workload["name"] == "scan_filter")
        self.assertEqual(minimum_auto_speedup(workload, 4), 1.85)

    def test_grouped_run_t1_floors_track_identity_address_promotion(self) -> None:
        floors = {
            workload["name"]: minimum_auto_speedup(workload, 1)
            for workload in GENERIC_WORKLOADS
            if workload["name"] in {"grouped_sorted_runs", "grouped_affine_sorted_runs", "grouped_sparse_sorted_runs"}
        }
        self.assertEqual(
            floors,
            {
                "grouped_sorted_runs": 3.10,
                "grouped_affine_sorted_runs": 2.75,
                "grouped_sparse_sorted_runs": 2.40,
            },
        )

    def test_scan_like_floors_track_normal_form_selection_promotion(self) -> None:
        workload = next(workload for workload in GENERIC_WORKLOADS if workload["name"] == "scan_like_fragments")
        self.assertEqual(minimum_auto_speedup(workload, 1), 1.55)
        self.assertEqual(minimum_auto_speedup(workload, 4), 1.50)


class TestRuntimeProofRequirements(unittest.TestCase):
    def test_requires_each_traced_auto_sample_to_prove_its_declared_path(self) -> None:
        workload = {
            "name": "packed_string",
            "minimum_auto_speedup": 0.0,
            "requires_compiled_auto": True,
            "required_runtime_paths": ("packed_path=",),
        }
        summary = [
            {"workload": "packed_string", "policy": "off", "correctness_diff": 0, "speedup_vs_off_median": 1.0},
            {
                "workload": "packed_string",
                "policy": "auto",
                "correctness_diff": 0,
                "speedup_vs_off_median": 1.0,
                "compile_errors": 0,
                "compiled_regions": 1,
                "runtime_events": 1,
            },
        ]
        missing_path_runs = [
            {"workload": "packed_string", "policy": "auto", "repeat": 1, "jit_runtime_path_counts": ""}
        ]
        failures = verification_failures(summary, missing_path_runs, (workload,), 1, True)
        self.assertTrue(any("packed_path=" in failure for failure in failures))

        proved_path_runs = [
            {"workload": "packed_string", "policy": "auto", "repeat": 1, "jit_runtime_path_counts": "packed_path=2048"}
        ]
        self.assertEqual(verification_failures(summary, proved_path_runs, (workload,), 1, True), [])


if __name__ == "__main__":
    unittest.main()
