#!/usr/bin/env python3

from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tpch_common import TPCHConfigurationError
from run_tpch_regression_gate import apply_baseline_state_contract, selected_auto_queries


class TestBaselineStateContract(unittest.TestCase):
    def args(self, scale_factor: float | None) -> SimpleNamespace:
        return SimpleNamespace(
            baseline_state=Path("accepted_state.json"),
            scale_factor=scale_factor,
            threads=1,
            timing_mode="production",
            queries=["01", "22"],
        )

    def state(self) -> dict:
        return {
            "scale_factor": 10.0,
            "threads": 1,
            "timing_mode": "production",
            "queries": [f"{query:02d}" for query in range(1, 23)],
        }

    def test_uses_accepted_scale_factor_when_not_explicit(self) -> None:
        args = self.args(None)
        apply_baseline_state_contract(args, self.state())
        self.assertEqual(args.scale_factor, 10.0)

    def test_rejects_candidate_scale_factor_mismatch(self) -> None:
        with self.assertRaisesRegex(TPCHConfigurationError, "does not match accepted baseline"):
            apply_baseline_state_contract(self.args(1.0), self.state())


class TestRuntimeContractQuerySelection(unittest.TestCase):
    def write_summary(self, rows: list[dict[str, str]]) -> Path:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        out_dir = Path(temporary_directory.name)
        with (out_dir / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=(
                    "query",
                    "policy",
                    "compiled_regions",
                    "runner_cost_selected_accelerated_runner_count",
                ),
            )
            writer.writeheader()
            writer.writerows(rows)
        return out_dir

    def test_selects_compiled_and_admitted_queries(self) -> None:
        out_dir = self.write_summary(
            [
                {
                    "query": "1",
                    "policy": "auto",
                    "compiled_regions": "1",
                    "runner_cost_selected_accelerated_runner_count": "0",
                },
                {
                    "query": "9",
                    "policy": "auto",
                    "compiled_regions": "0",
                    "runner_cost_selected_accelerated_runner_count": "1",
                },
                {
                    "query": "18",
                    "policy": "auto",
                    "compiled_regions": "0",
                    "runner_cost_selected_accelerated_runner_count": "0",
                },
                {
                    "query": "13",
                    "policy": "off",
                    "compiled_regions": "1",
                    "runner_cost_selected_accelerated_runner_count": "1",
                },
            ]
        )
        self.assertEqual(selected_auto_queries(out_dir, ["1", "9", "13", "18"]), ["1", "9"])

    def test_malformed_admission_counters_do_not_create_false_proof(self) -> None:
        out_dir = self.write_summary(
            [
                {
                    "query": "7",
                    "policy": "auto",
                    "compiled_regions": "invalid",
                    "runner_cost_selected_accelerated_runner_count": "1",
                }
            ]
        )
        with self.assertRaises(TPCHConfigurationError):
            selected_auto_queries(out_dir, ["7"])


if __name__ == "__main__":
    unittest.main()
