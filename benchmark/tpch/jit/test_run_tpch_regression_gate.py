#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tpch_common import TPCHConfigurationError
from run_tpch_regression_gate import (
    apply_baseline_state_contract,
    benchmark_command,
    candidate_qualifies_for_direct_promotion,
    merge_rechecked_csv_artifact,
    load_baseline_state,
    parse_args,
    promotion_recheck_repeats,
    provision_gate_database,
    selected_auto_queries,
    triage_recheck_repeats,
    validate_baseline_write_configuration,
    write_baseline_state,
)


class TestGateDatabaseReuse(unittest.TestCase):
    def args(self, root: Path) -> SimpleNamespace:
        return SimpleNamespace(
            db=None,
            use_existing_db=False,
            keep_db=False,
            duckdb=root / "duckdb",
            queries=["01"],
            policies=["off", "auto"],
            repeats=5,
            timing_mode="production",
            scale_factor=10.0,
            threads=1,
            event_log_size=0,
            trace_decisions=False,
            trace_runtime=False,
            jit_verify=False,
            jit_cbo_setting=[],
        )

    def test_provisions_one_database_for_all_gate_phases(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        out_dir = root / "artifact"
        out_dir.mkdir()
        args = self.args(root)

        database_dir = provision_gate_database(args)
        self.addCleanup(lambda: database_dir.rmdir())
        self.assertTrue(database_dir.name.startswith("duckdb_jit_tpch_gate_"))
        self.assertEqual(args.db, database_dir / "tpch.duckdb")

        candidate = benchmark_command(args, out_dir)
        proof = benchmark_command(args, out_dir / "proof", reuse_database=True)
        self.assertNotIn("--use-existing-db", candidate)
        self.assertIn("--use-existing-db", proof)
        self.assertEqual(candidate[candidate.index("--db") + 1], str(args.db))
        self.assertEqual(proof[proof.index("--db") + 1], str(args.db))

    def test_reuses_explicit_database_without_keep_flag(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = self.args(root)
        args.db = root / "tpch.duckdb"

        self.assertIsNone(provision_gate_database(args))
        proof = benchmark_command(args, root / "proof", reuse_database=True)
        self.assertIn("--use-existing-db", proof)
        self.assertNotIn("--keep-db", proof)


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

    def test_baseline_state_records_production_jit_configuration(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = SimpleNamespace(
            allow_partial_baseline=True,
            baseline_state=root / "state.json",
            queries=["01"],
            scale_factor=1.0,
            threads=1,
            repeats=10,
            timing_mode="production",
            event_log_size=0,
            trace_decisions=False,
            trace_runtime=False,
            jit_verify=False,
            jit_cbo_setting=[],
            duckdb=root / "duckdb",
        )
        artifact = root / "artifact"
        artifact.mkdir()
        for filename in ("summary.csv", "runs.csv", "counters.csv", "performance_gaps.csv"):
            (artifact / filename).write_text("query\n", encoding="utf-8")

        write_baseline_state(args, artifact, "test")

        with args.baseline_state.open(encoding="utf-8") as handle:
            state = json.load(handle)
        self.assertEqual(state["event_log_size"], 0)
        self.assertFalse(state["trace_decisions"])
        self.assertFalse(state["trace_runtime"])
        self.assertFalse(state["jit_verify"])
        self.assertEqual(state["jit_cbo_settings"], [])
        self.assertFalse(Path(state["current_baseline"]).is_absolute())
        accepted_baseline = load_baseline_state(args.baseline_state)
        self.assertEqual(accepted_baseline.parent, args.baseline_state.parent.resolve())
        self.assertEqual(
            sorted(path.name for path in accepted_baseline.iterdir()),
            ["counters.csv", "performance_gaps.csv", "runs.csv", "summary.csv"],
        )


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
    def test_triage_is_opt_in(self) -> None:
        with mock.patch.object(sys, "argv", ["run_tpch_regression_gate.py"]):
            self.assertFalse(parse_args().triage_failures)

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
            triage_recheck_repeats(SimpleNamespace(repeats=10, triage_repeats=None)),
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
            jit_verify=False,
            jit_cbo_setting=[],
        )

    def test_reuses_passing_ten_repeat_production_candidate(self) -> None:
        self.assertTrue(candidate_qualifies_for_direct_promotion(self.promotion_args(10), True))

    def test_five_repeat_candidate_still_requires_promotion_run(self) -> None:
        self.assertFalse(candidate_qualifies_for_direct_promotion(self.promotion_args(5), True))

    def test_failed_candidate_comparison_is_never_promoted_directly(self) -> None:
        self.assertFalse(candidate_qualifies_for_direct_promotion(self.promotion_args(10), False))

    def test_non_production_options_never_qualify_for_promotion(self) -> None:
        overrides = (
            ("timing_mode", "profile"),
            ("event_log_size", 10000),
            ("trace_decisions", True),
            ("trace_runtime", True),
            ("jit_verify", True),
            ("jit_cbo_setting", ["jit_cbo_generated_stage_benefit=1"]),
        )
        for field, value in overrides:
            with self.subTest(field=field):
                args = self.promotion_args(10)
                setattr(args, field, value)
                self.assertFalse(candidate_qualifies_for_direct_promotion(args, True))
                with self.assertRaises(TPCHConfigurationError):
                    validate_baseline_write_configuration(args)


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

    def test_keeps_counter_rows_created_only_during_focused_recheck(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        candidate = root / "candidate.csv"
        focused = root / "focused.csv"
        merged = root / "merged.csv"
        self.write_rows(
            candidate,
            [{"query": "18", "policy": "auto", "median_s": "existing-counter"}],
        )
        self.write_rows(
            focused,
            [{"query": "20", "policy": "auto", "median_s": "new-counter"}],
        )

        merge_rechecked_csv_artifact(candidate, focused, merged, ["20"], require_query_rows=False)

        with merged.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(
            rows,
            [
                {"query": "18", "policy": "auto", "median_s": "existing-counter"},
                {"query": "20", "policy": "auto", "median_s": "new-counter"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
