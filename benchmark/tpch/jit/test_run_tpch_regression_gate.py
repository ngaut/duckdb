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
from run_tpch_regression_gate import (
    apply_baseline_state_contract,
    candidate_qualifies_for_direct_promotion,
    merge_rechecked_csv_artifact,
    promotion_recheck_repeats,
    selected_auto_queries,
    triage_recheck_repeats,
)


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


class TestPromotionRepeats(unittest.TestCase):
    def test_default_is_ten_repeats(self) -> None:
        self.assertEqual(
            promotion_recheck_repeats(SimpleNamespace(repeats=5, promotion_repeats=None)),
            10,
        )

    def test_default_never_reduces_candidate_sample_count(self) -> None:
        self.assertEqual(
            promotion_recheck_repeats(SimpleNamespace(repeats=12, promotion_repeats=None)),
            12,
        )

    def test_explicit_repeat_count_wins(self) -> None:
        self.assertEqual(
            promotion_recheck_repeats(SimpleNamespace(repeats=5, promotion_repeats=7)),
            7,
        )

    def test_triage_reuses_candidate_sample_count_at_or_above_ten(self) -> None:
        self.assertEqual(
            triage_recheck_repeats(
                SimpleNamespace(repeats=10, triage_repeats=None)
            ),
            10,
        )

    def promotion_args(self, repeats: int) -> SimpleNamespace:
        return SimpleNamespace(
            repeats=repeats,
            promotion_repeats=10,
            timing_mode="production",
            event_log_size=0,
            trace_decisions=False,
            trace_runtime=False,
        )

    def test_reuses_passing_ten_repeat_production_candidate(self) -> None:
        self.assertTrue(
            candidate_qualifies_for_direct_promotion(self.promotion_args(10), True)
        )

    def test_five_repeat_candidate_still_requires_promotion_run(self) -> None:
        self.assertFalse(
            candidate_qualifies_for_direct_promotion(self.promotion_args(5), True)
        )

    def test_failed_candidate_comparison_is_never_promoted_directly(self) -> None:
        self.assertFalse(
            candidate_qualifies_for_direct_promotion(self.promotion_args(10), False)
        )


class TestPromotionArtifactMerge(unittest.TestCase):
    def write_rows(self, path: Path, rows: list[dict[str, str]]) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=("query", "policy", "median_s"))
            writer.writeheader()
            writer.writerows(rows)

    def test_replaces_only_focused_query_rows(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        candidate = root / "candidate.csv"
        focused = root / "focused.csv"
        merged = root / "merged.csv"
        self.write_rows(
            candidate,
            [
                {"query": "18", "policy": "auto", "median_s": "0.76"},
                {"query": "20", "policy": "off", "median_s": "0.61"},
                {"query": "20", "policy": "auto", "median_s": "0.53"},
            ],
        )
        self.write_rows(
            focused,
            [
                {"query": "20", "policy": "off", "median_s": "0.62"},
                {"query": "20", "policy": "auto", "median_s": "0.52"},
            ],
        )

        merge_rechecked_csv_artifact(candidate, focused, merged, ["20"])

        with merged.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(
            rows,
            [
                {"query": "18", "policy": "auto", "median_s": "0.76"},
                {"query": "20", "policy": "off", "median_s": "0.62"},
                {"query": "20", "policy": "auto", "median_s": "0.52"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
