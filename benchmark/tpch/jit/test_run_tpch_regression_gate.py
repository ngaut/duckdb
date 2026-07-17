#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tpch_common import TPCHConfigurationError
from run_tpch_regression_gate import (
    apply_baseline_state_contract,
    benchmark_command,
    candidate_qualifies_for_direct_promotion,
    clone_cached_database,
    database_cache_paths,
    gate_database,
    merge_rechecked_csv_artifact,
    load_baseline_state,
    parse_args,
    promotion_recheck_repeats,
    run_gate,
    run_timed_benchmark,
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
            database_cache=True,
            database_cache_dir=root / "cache",
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

    def test_reuses_one_cached_database_for_all_gate_phases(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        out_dir = root / "artifact"
        out_dir.mkdir()
        args = self.args(root)

        def create_database(_args, database_path: Path) -> None:
            database_path.touch()

        with (
            mock.patch("run_tpch_regression_gate.create_tpch_database", side_effect=create_database) as create,
            mock.patch("run_tpch_regression_gate.validate_tpch_database") as validate,
            gate_database(args),
        ):
            working_database = args.db
            database_path, manifest_path, lock_path = database_cache_paths(args)
            self.assertNotEqual(working_database, database_path)
            self.assertTrue(working_database.is_file())
            self.assertTrue(database_path.is_file())
            self.assertFalse(lock_path.exists())
            candidate = benchmark_command(args, out_dir)
            proof = benchmark_command(args, out_dir / "proof", reuse_database=True)
        create.assert_called_once()
        validate.assert_called_once()
        self.assertTrue(database_path.is_file())
        self.assertTrue(manifest_path.is_file())
        self.assertFalse(lock_path.exists())
        self.assertFalse(working_database.parent.exists())
        self.assertIn("--use-existing-db", candidate)
        self.assertIn("--use-existing-db", proof)
        self.assertEqual(candidate[candidate.index("--db") + 1], str(working_database))
        self.assertEqual(proof[proof.index("--db") + 1], str(working_database))

    def test_existing_cache_is_validated_without_regeneration(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = self.args(root)
        database_path, manifest_path, _ = database_cache_paths(args)
        database_path.parent.mkdir(parents=True)
        database_path.touch()
        manifest_path.write_text(
            json.dumps(
                {
                    "format_version": 2,
                    "database_role": "immutable_tpch_template",
                    "scale_factor": 10.0,
                }
            )
            + "\n",
            encoding="utf-8",
        )

        with (
            mock.patch("run_tpch_regression_gate.create_tpch_database") as create,
            mock.patch("run_tpch_regression_gate.validate_tpch_database") as validate,
            gate_database(args),
        ):
            pass
        create.assert_not_called()
        validate.assert_called_once_with(args, database_path)

    def test_clone_falls_back_when_copy_on_write_is_unavailable(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        source = root / "source.duckdb"
        target = root / "target.duckdb"
        source.write_text("template", encoding="utf-8")

        with (
            mock.patch("run_tpch_regression_gate.sys.platform", "darwin"),
            mock.patch("run_tpch_regression_gate.subprocess.run", side_effect=FileNotFoundError),
        ):
            clone_cached_database(source, target)
        self.assertEqual(target.read_text(encoding="utf-8"), "template")

    def test_live_cache_lock_fails_before_database_work(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = self.args(root)
        _, _, lock_path = database_cache_paths(args)
        lock_path.parent.mkdir(parents=True)
        lock_path.write_text(f"{os.getpid()}\n", encoding="utf-8")

        with self.assertRaisesRegex(TPCHConfigurationError, "cache is in use"):
            with gate_database(args):
                pass

    def test_stale_cache_lock_is_recovered(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = self.args(root)
        _, _, lock_path = database_cache_paths(args)
        lock_path.parent.mkdir(parents=True)
        lock_path.write_text("99999999\n", encoding="utf-8")

        def create_database(_args, database_path: Path) -> None:
            database_path.touch()

        with (
            mock.patch("run_tpch_regression_gate.process_exists", return_value=False),
            mock.patch("run_tpch_regression_gate.create_tpch_database", side_effect=create_database),
            mock.patch("run_tpch_regression_gate.validate_tpch_database"),
            gate_database(args),
        ):
            self.assertTrue(args.db.is_file())
        self.assertFalse(lock_path.exists())

    def test_invalid_manifest_rebuilds_cache(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = self.args(root)
        database_path, manifest_path, _ = database_cache_paths(args)
        database_path.parent.mkdir(parents=True)
        database_path.write_text("stale", encoding="utf-8")
        manifest_path.write_text('{"format_version": 1, "scale_factor": 1}\n', encoding="utf-8")

        def create_database(_args, path: Path) -> None:
            path.write_text("fresh", encoding="utf-8")

        with (
            mock.patch("run_tpch_regression_gate.create_tpch_database", side_effect=create_database) as create,
            mock.patch("run_tpch_regression_gate.validate_tpch_database"),
            gate_database(args),
        ):
            self.assertEqual(database_path.read_text(encoding="utf-8"), "fresh")
        create.assert_called_once()

    def test_cache_can_be_disabled_for_a_private_disposable_database(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = self.args(root)
        args.database_cache = False

        def create_database(_args, database_path: Path) -> None:
            database_path.touch()

        with (
            mock.patch("run_tpch_regression_gate.create_tpch_database", side_effect=create_database) as create,
            mock.patch("run_tpch_regression_gate.validate_tpch_database") as validate,
            gate_database(args),
        ):
            database_path = args.db
            database_dir = database_path.parent
            self.assertTrue(database_path.is_file())
            self.assertTrue(args.use_existing_db)
        create.assert_called_once_with(args, database_path)
        validate.assert_called_once_with(args, database_path)
        self.assertFalse(database_dir.exists())

    def test_reuses_explicit_database_without_keep_flag(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = self.args(root)
        args.db = root / "tpch.duckdb"

        def create_database(_args, database_path: Path) -> None:
            database_path.touch()

        with (
            mock.patch("run_tpch_regression_gate.create_tpch_database", side_effect=create_database) as create,
            mock.patch("run_tpch_regression_gate.validate_tpch_database") as validate,
            gate_database(args),
        ):
            proof = benchmark_command(args, root / "proof", reuse_database=True)
        create.assert_called_once_with(args, args.db)
        validate.assert_called_once_with(args, args.db)
        self.assertIn("--use-existing-db", proof)
        self.assertNotIn("--keep-db", proof)

    def test_host_admission_runs_after_database_setup_and_before_measurement(self) -> None:
        events = []
        args = SimpleNamespace(no_build=True, skip_architecture=True, host_quiescence=True)

        @contextmanager
        def database_context(_args):
            events.append("database")
            yield
            events.append("cleanup")

        with (
            mock.patch("run_tpch_regression_gate.gate_database", side_effect=database_context),
            mock.patch(
                "run_tpch_regression_gate.wait_for_host_quiescence",
                side_effect=lambda: events.append("quiescence"),
            ),
            mock.patch(
                "run_tpch_regression_gate.run_benchmark_gate",
                side_effect=lambda *_args: events.append("benchmark") or 0,
            ),
        ):
            self.assertEqual(run_gate(args, None, Path("artifact")), 0)
        self.assertEqual(events, ["database", "quiescence", "benchmark", "cleanup"])

    def test_timed_benchmark_requires_clean_post_measurement_host(self) -> None:
        events = []
        args = SimpleNamespace(host_quiescence=True)
        with (
            mock.patch(
                "run_tpch_regression_gate.run_command",
                side_effect=lambda *_args: events.append("benchmark"),
            ),
            mock.patch(
                "run_tpch_regression_gate.require_host_quiescence",
                side_effect=lambda: events.append("post-check"),
            ),
        ):
            run_timed_benchmark(args, ["benchmark"], "benchmark")
        self.assertEqual(events, ["benchmark", "post-check"])


class TestBaselineStateContract(unittest.TestCase):
    def args(self, scale_factor: float | None) -> SimpleNamespace:
        return SimpleNamespace(
            baseline_state=Path("accepted_state.json"),
            scale_factor=scale_factor,
            threads=1,
            timing_mode="production",
            policies=["off", "auto"],
            queries=["01", "22"],
        )

    def state(self) -> dict:
        return {
            "scale_factor": 10.0,
            "threads": 1,
            "timing_mode": "production",
            "policies": ["off", "auto"],
            "queries": [f"{query:02d}" for query in range(1, 23)],
        }

    def test_uses_accepted_scale_factor_when_not_explicit(self) -> None:
        args = self.args(None)
        apply_baseline_state_contract(args, self.state())
        self.assertEqual(args.scale_factor, 10.0)

    def test_rejects_candidate_scale_factor_mismatch(self) -> None:
        with self.assertRaisesRegex(TPCHConfigurationError, "does not match accepted baseline"):
            apply_baseline_state_contract(self.args(1.0), self.state())

    def test_rejects_missing_or_mismatched_policy_contract(self) -> None:
        missing = self.state()
        del missing["policies"]
        with self.assertRaisesRegex(TPCHConfigurationError, "must contain policies"):
            apply_baseline_state_contract(self.args(None), missing)

        args = self.args(None)
        args.policies = ["auto"]
        with self.assertRaisesRegex(TPCHConfigurationError, "do not match accepted baseline"):
            apply_baseline_state_contract(args, self.state())

    def test_baseline_state_records_production_jit_configuration(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        root = Path(temporary_directory.name)
        args = SimpleNamespace(
            allow_partial_baseline=True,
            baseline_state=root / "state.json",
            queries=["01"],
            policies=["off", "auto"],
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
        self.assertEqual(state["policies"], ["off", "auto"])
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
            policies=["off", "auto"],
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

    def test_partial_policy_set_never_qualifies_for_promotion(self) -> None:
        args = self.promotion_args(10)
        args.policies = ["auto"]
        self.assertFalse(candidate_qualifies_for_direct_promotion(args, True))
        with self.assertRaisesRegex(TPCHConfigurationError, "require policies"):
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
