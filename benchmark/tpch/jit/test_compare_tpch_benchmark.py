#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_tpch_benchmark import (
    compare_auto_speed,
    compare_runtime_components,
    paired_policy_speedups,
    policy_runtime_upper_bounds,
)


def gap(auto_s: str, off_s: str = "0.100") -> dict[str, str]:
    return {
        "query": "18",
        "auto_median_s": auto_s,
        "off_median_s": off_s,
        "auto_speedup_vs_off": str(Decimal(off_s) / Decimal(auto_s)),
        "auto_compiled_regions": "1",
        "auto_unsupported_decisions": "0",
        "auto_skipped_decisions": "0",
        "auto_runner_cost_selected_accelerated_runner_count": "1",
    }


def runtime_summary(runtime_us: int) -> dict[tuple[str, str], dict[str, str]]:
    return {
        ("18", "auto"): {
            "runtime_regions": "1",
            "runtime_time_us": str(runtime_us),
            "generated_runtime_time_us": str(runtime_us),
        }
    }


class TestExactRegressionThresholds(unittest.TestCase):
    def test_exact_absolute_slowdown_boundary_passes(self) -> None:
        failures = compare_auto_speed(
            {"18": gap("0.097")},
            {"18": gap("0.099")},
            ["18"],
            Decimal("1.02"),
            Decimal("0.002"),
            Decimal("0.98"),
        )
        self.assertEqual(failures, [])

    def test_slowdown_beyond_absolute_boundary_fails(self) -> None:
        failures = compare_auto_speed(
            {"18": gap("0.097")},
            {"18": gap("0.099001")},
            ["18"],
            Decimal("1.02"),
            Decimal("0.002"),
            Decimal("0.98"),
        )
        self.assertEqual([failure["category"] for failure in failures], ["auto_runtime"])

    def test_exact_runtime_ratio_boundary_passes(self) -> None:
        failures = compare_runtime_components(
            runtime_summary(1000), runtime_summary(1100), ["18"], ("runtime_time_us",), Decimal("1.10"), 99
        )
        self.assertEqual(failures, [])

    def test_runtime_ratio_beyond_boundary_fails(self) -> None:
        failures = compare_runtime_components(
            runtime_summary(1000), runtime_summary(1101), ["18"], ("runtime_time_us",), Decimal("1.10"), 99
        )
        self.assertEqual([failure["category"] for failure in failures], ["runtime_component"])

    def test_paired_speedup_does_not_hide_raw_runtime_regression(self) -> None:
        paired = paired_policy_speedups(
            [
                {"query": "18", "repeat": "1", "policy": "off", "query_time_us": "100000"},
                {"query": "18", "repeat": "1", "policy": "auto", "query_time_us": "90000"},
                {"query": "18", "repeat": "2", "policy": "auto", "query_time_us": "1000000"},
                {"query": "18", "repeat": "2", "policy": "off", "query_time_us": "1200000"},
                {"query": "18", "repeat": "3", "policy": "off", "query_time_us": "100000"},
                {"query": "18", "repeat": "3", "policy": "auto", "query_time_us": "90000"},
            ]
        )
        failures = compare_auto_speed(
            {"18": gap("0.090")},
            {"18": gap("0.120")},
            ["18"],
            Decimal("1.02"),
            Decimal("0.002"),
            Decimal("0.98"),
            paired,
        )
        self.assertEqual([failure["category"] for failure in failures], ["auto_runtime"])

    def test_shared_off_and_auto_slowdown_fails_raw_runtime_gate(self) -> None:
        failures = compare_auto_speed(
            {"18": gap("0.090", "0.100")},
            {"18": gap("0.180", "0.200")},
            ["18"],
            Decimal("1.02"),
            Decimal("0.002"),
            Decimal("0.98"),
        )
        self.assertEqual([failure["category"] for failure in failures], ["auto_runtime"])

    def test_raw_runtime_regression_fails_without_decision_counters(self) -> None:
        base = gap("0.090", "0.100")
        candidate = gap("0.120", "0.200")
        for row in (base, candidate):
            row["auto_compiled_regions"] = "0"
            row["auto_runner_cost_selected_accelerated_runner_count"] = "0"
        failures = compare_auto_speed(
            {"18": base},
            {"18": candidate},
            ["18"],
            Decimal("1.02"),
            Decimal("0.002"),
            Decimal("0.98"),
        )
        self.assertEqual([failure["category"] for failure in failures], ["auto_runtime"])

    def test_baseline_observed_max_defines_raw_runtime_envelope(self) -> None:
        failures = compare_auto_speed(
            {"18": gap("0.090")},
            {"18": gap("0.102")},
            ["18"],
            Decimal("1.02"),
            Decimal("0.002"),
            Decimal("0.98"),
            baseline_auto_runtime_upper_bounds={"18": Decimal("0.100")},
        )
        self.assertEqual(failures, [])

    def test_observed_max_does_not_let_normalization_hide_raw_regression(self) -> None:
        failures = compare_auto_speed(
            {"18": gap("0.090", "0.100")},
            {"18": gap("0.102001", "0.200")},
            ["18"],
            Decimal("1.02"),
            Decimal("0.002"),
            Decimal("0.98"),
            baseline_auto_runtime_upper_bounds={"18": Decimal("0.100")},
        )
        self.assertEqual([failure["category"] for failure in failures], ["auto_runtime"])
        self.assertIn("observed max=0.100000000s", failures[0]["message"])

    def test_policy_runtime_upper_bounds_preserve_raw_high_water(self) -> None:
        upper_bounds = policy_runtime_upper_bounds(
            [
                {"query": "18", "policy": "auto", "query_time_us": "90000"},
                {"query": "18", "policy": "off", "query_time_us": "150000"},
                {"query": "18", "policy": "auto", "query_time_us": "110000"},
                {"query": "06", "policy": "auto", "query_time_us": "200000"},
            ],
            "auto",
        )
        self.assertEqual(
            upper_bounds,
            {"18": Decimal("0.11"), "06": Decimal("0.2")},
        )

    def test_paired_slowdown_still_fails(self) -> None:
        failures = compare_auto_speed(
            {"18": gap("0.090")},
            {"18": gap("0.120")},
            ["18"],
            Decimal("1.02"),
            Decimal("0.002"),
            Decimal("0.98"),
            {"18": Decimal("0.90")},
        )
        self.assertEqual(
            [failure["category"] for failure in failures],
            ["auto_runtime", "auto_speedup"],
        )


if __name__ == "__main__":
    unittest.main()
