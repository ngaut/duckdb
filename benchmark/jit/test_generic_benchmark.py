#!/usr/bin/env python3

from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generic_benchmark import (
    BASELINE_ABSOLUTE_NOISE_ALLOWANCE_US,
    BASELINE_RELATIVE_NOISE_ALLOWANCE,
    GENERIC_AUTO_BASELINE_MEDIAN_US_BY_THREADS,
    GENERIC_WORKLOADS,
    baseline_auto_median_us,
    maximum_auto_median_us,
    median_paired_speedup,
    minimum_auto_speedup,
    policy_order,
    verification_failures,
)


class TestPolicyOrder(unittest.TestCase):
    def test_reverses_each_successive_pair(self) -> None:
        self.assertEqual(
            [policy_order(repeat) for repeat in range(1, 6)],
            [("off", "auto"), ("auto", "off"), ("off", "auto"), ("auto", "off"), ("off", "auto")],
        )

    def test_rejects_invalid_repeat(self) -> None:
        with self.assertRaises(ValueError):
            policy_order(0)

    def test_speedup_uses_within_repeat_pairs(self) -> None:
        off = [
            {"repeat": 1, "query_time_us": 200},
            {"repeat": 2, "query_time_us": 100},
        ]
        auto = [
            {"repeat": 1, "query_time_us": 100},
            {"repeat": 2, "query_time_us": 50},
        ]
        self.assertEqual(median_paired_speedup(off, auto), 2.0)


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
                "grouped_sorted_runs": 3.00,
                "grouped_affine_sorted_runs": 2.65,
                "grouped_sparse_sorted_runs": 3.10,
            },
        )

    def test_selective_grouped_floors_track_independent_shape_promotion(self) -> None:
        names = {
            "grouped_selective_multi_aggregate",
            "grouped_selective_conjunction_multi_aggregate",
            "grouped_selective_three_way_conjunction_multi_aggregate",
        }
        workloads = {workload["name"]: workload for workload in GENERIC_WORKLOADS if workload["name"] in names}
        self.assertEqual(
            {
                name: (minimum_auto_speedup(workload, 1), minimum_auto_speedup(workload, 4))
                for name, workload in workloads.items()
            },
            {
                "grouped_selective_multi_aggregate": (1.40, 1.30),
                "grouped_selective_conjunction_multi_aggregate": (1.40, 1.28),
                "grouped_selective_three_way_conjunction_multi_aggregate": (1.38, 1.23),
            },
        )
    def test_wide_grouped_floor_tracks_proof_owned_finalization_promotion(self) -> None:
        workload = next(workload for workload in GENERIC_WORKLOADS if workload["name"] == "grouped_wide_sorted_runs")
        self.assertEqual(
            (minimum_auto_speedup(workload, 1), minimum_auto_speedup(workload, 4)),
            (2.80, 3.00),
        )
    def test_exact_filter_join_tracks_direct_dictionary_reduction_promotion(self) -> None:
        workload = next(workload for workload in GENERIC_WORKLOADS if workload["name"] == "join_exact_filter_build")
        self.assertEqual(
            (minimum_auto_speedup(workload, 1), minimum_auto_speedup(workload, 4)),
            (1.35, 1.09),
        )
        self.assertEqual(
            workload["required_runtime_paths"],
            (
                "hash_join_probe.regular_probe.all_valid.flat.single_key.no_chain."
                "direct_ungrouped_aggregate_consumer=",
                "aggregate_update.join_output_probe_consumer_ungrouped_aggregate.dictionary_source=",
            ),
        )


class TestRawRuntimeBaselines(unittest.TestCase):
    def test_every_production_workload_has_t1_and_t4_baselines(self) -> None:
        workload_names = {workload["name"] for workload in GENERIC_WORKLOADS}
        self.assertEqual(set(GENERIC_AUTO_BASELINE_MEDIAN_US_BY_THREADS), workload_names)
        for workload in GENERIC_WORKLOADS:
            for threads in (1, 4):
                baseline = baseline_auto_median_us(workload, threads)
                expected_allowance = max(
                    BASELINE_ABSOLUTE_NOISE_ALLOWANCE_US,
                    math.ceil(baseline * BASELINE_RELATIVE_NOISE_ALLOWANCE),
                )
                self.assertGreater(baseline, 0)
                self.assertEqual(maximum_auto_median_us(workload, threads), baseline + expected_allowance)

    def test_missing_baseline_is_an_error_not_a_disabled_gate(self) -> None:
        with self.assertRaisesRegex(ValueError, "no raw JIT baseline"):
            maximum_auto_median_us({"name": "unqualified_workload"}, 1)


class TestPerformanceGates(unittest.TestCase):
    def test_raw_auto_ceiling_is_independent_of_speedup_normalization(self) -> None:
        workload = {
            "name": "raw_runtime_guard",
            "minimum_auto_speedup": 2.0,
            "baseline_auto_median_us_by_threads": {1: 37000},
            "max_auto_slowdown": 1.05,
            "requires_compiled_auto": True,
        }
        summary = [
            {
                "workload": "raw_runtime_guard",
                "policy": "off",
                "correctness_diff": 0,
                "median_s": 0.100,
                "paired_speedup_median": 1.0,
            },
            {
                "workload": "raw_runtime_guard",
                "policy": "auto",
                "correctness_diff": 0,
                "median_s": 0.040,
                "paired_speedup_median": 2.5,
                "compile_errors": 0,
                "compiled_regions": 1,
                "runtime_events": 1,
            },
        ]
        failures = verification_failures(summary, [], (workload,), 1, False)
        self.assertTrue(any("exceeds raw ceiling" in failure for failure in failures))

        summary[1]["median_s"] = 0.036
        summary[1]["paired_speedup_median"] = 1.5
        failures = verification_failures(summary, [], (workload,), 1, False)
        self.assertTrue(any("below required" in failure for failure in failures))

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
            {"workload": "packed_string", "policy": "off", "correctness_diff": 0, "paired_speedup_median": 1.0},
            {
                "workload": "packed_string",
                "policy": "auto",
                "correctness_diff": 0,
                "paired_speedup_median": 1.0,
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
