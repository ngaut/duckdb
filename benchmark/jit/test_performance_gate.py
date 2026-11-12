#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import io
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import performance_gate
from duckdb_shell import Measurement
from performance_gate import SUITES, TRACED, Sample, parse_args, run_workload, summary_row, workload_failures
from process_usage import ProcessUsage

WORKLOAD = {
    "name": "workload",
    "sql": "SELECT 42",
    "floor": {1: 1.5},
    "ceiling": {1: 1100},
    "compiled": {1: True},
}


def counter_row(status: str, **fields) -> dict:
    return {
        "status": status,
        "count": "1",
        "backend_name": "jit_sljit",
        "kernel_id": "7",
        "execution_mode": "native",
        "selected_runner": "compiled_vectorized",
        "blocker": "",
        "runner_cost_selection_reason": "",
        "runner_cost_profile": False,
        "jit_runtime_path_counts": "",
        **fields,
    }


def sample(
    policy: str, repeat: int | str, instructions: int, counters: list[dict], correctness_diff: int = 0
) -> Sample:
    # every sample runs on performance cores at one cycle per instruction
    usage = ProcessUsage(0, instructions, instructions, instructions)
    return Sample(policy, repeat, Measurement(instructions * 1000, usage), correctness_diff, counters)


def samples(off: int = 2000, auto: int = 1000, compiled: bool = True, traced: list[dict] | None = None) -> list[Sample]:
    counters = [counter_row("compiled")] if compiled else []
    return [
        sample("off", 1, off, []),
        sample("auto", 1, auto, counters),
        sample("auto", 2, auto, counters),
        sample("off", 2, off, []),
        sample("auto", TRACED, 9 * auto, counters + [counter_row("executed")] if traced is None else traced),
    ]


def failures(workload: dict, measured: list[Sample]) -> list[str]:
    return workload_failures(workload, 1, summary_row(workload, 1, measured), measured)


class SummaryTest(unittest.TestCase):
    def test_the_cost_of_a_policy_is_the_work_of_its_timed_samples(self) -> None:
        summary = summary_row(WORKLOAD, 1, samples())
        self.assertEqual((summary["off_cycles"], summary["auto_cycles"]), (2000, 1000))
        self.assertEqual(summary["speedup"], 2.0)
        self.assertEqual((summary["floor"], summary["ceiling"]), (1.5, 1100))
        self.assertEqual((summary["off_wall_time_us"], summary["auto_wall_time_us"]), (2000, 1000))


class WorkloadFailuresTest(unittest.TestCase):
    def assert_fails(self, workload: dict, measured: list[Sample], failure: str) -> None:
        self.assertEqual(failures(workload, measured), [f"workload: {failure}"])

    def test_a_workload_within_its_thresholds_passes(self) -> None:
        self.assertEqual(failures(WORKLOAD, samples()), [])

    def test_a_result_that_differs_from_the_native_engine_fails(self) -> None:
        measured = samples()
        measured[1] = sample("auto", 1, 1000, [counter_row("compiled")], correctness_diff=3)
        self.assert_fails(WORKLOAD, measured, "auto repeat 1: 3 rows differ from the native engine's result")

    def test_the_native_engine_records_no_jit_counters(self) -> None:
        measured = samples()
        measured[0] = sample("off", 1, 2000, [counter_row("compiled")])
        self.assert_fails(WORKLOAD, measured, "off repeat 1: the native engine recorded JIT counters")

    def test_a_compile_error_fails(self) -> None:
        measured = samples()
        measured[2] = sample("auto", 2, 1000, [counter_row("compiled"), counter_row("error", blocker="backend")])
        self.assert_fails(WORKLOAD, measured, "auto repeat 2: JIT compile errors")

    def test_a_workload_compiles_exactly_when_it_is_expected_to(self) -> None:
        measured = samples()
        measured[1] = sample("auto", 1, 1000, [])
        self.assert_fails(WORKLOAD, measured, "auto repeat 1: compiled 0 regions")
        native = {**WORKLOAD, "compiled": {1: False}, "floor": {1: 0.96}}
        self.assertEqual(failures(native, samples(compiled=False, traced=[])), [])
        measured = samples(compiled=False, traced=[])
        measured[2] = sample("auto", 2, 1000, [counter_row("compiled")])
        self.assert_fails(native, measured, "auto repeat 2: compiled 1 regions")

    def test_a_compiled_region_must_run(self) -> None:
        self.assert_fails(
            WORKLOAD, samples(traced=[counter_row("compiled")]), "the traced run executed no compiled region"
        )

    def test_a_required_runtime_path_must_be_taken(self) -> None:
        workload = {**WORKLOAD, "required_runtime_paths": ("aggregate_update.direct",)}
        self.assert_fails(workload, samples(), "the traced run did not take runtime path 'aggregate_update.direct'")
        taken = [
            counter_row("compiled"),
            counter_row("executed", jit_runtime_path_counts="aggregate_update.direct=4"),
        ]
        self.assertEqual(failures(workload, samples(traced=taken)), [])

    def test_the_counter_contract_is_checked_on_the_traced_run(self) -> None:
        traced = [counter_row("compiled"), counter_row("executed"), counter_row("unsupported")]
        self.assert_fails(WORKLOAD, samples(traced=traced), "unsupported compiled_vectorized kernel 7 (): no blocker")

    def test_a_speedup_below_the_floor_fails(self) -> None:
        self.assert_fails(WORKLOAD, samples(off=1400), "speedup 1.400 is below the floor 1.500")

    def test_a_cost_above_the_ceiling_fails(self) -> None:
        self.assert_fails(WORKLOAD, samples(off=4000, auto=1101), "JIT cost 1101 cycles exceeds the ceiling 1100")


class RunWorkloadTest(unittest.TestCase):
    def test_the_policies_alternate_in_which_runs_first_and_the_traced_sample_is_last(self) -> None:
        with mock.patch.object(performance_gate, "run_sample", side_effect=lambda *arguments: arguments[4:]) as run:
            order = run_workload(Path("duckdb"), Path("fixture"), WORKLOAD, 1, 3)
        self.assertEqual(order, [("off", 1), ("auto", 1), ("auto", 2), ("off", 2), ("off", 3), ("auto", 3), ("auto",)])
        self.assertEqual(run.call_count, 7)


class ArgumentsTest(unittest.TestCase):
    def rejected(self, *argv: str) -> str:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit):
            parse_args(list(argv))
        return stderr.getvalue()

    def test_a_suite_runs_all_its_workloads_by_default(self) -> None:
        self.assertEqual(parse_args(["generic", "--threads", "4"]).workloads, list(SUITES["generic"]))

    def test_selected_workloads_run_in_the_order_of_the_suite(self) -> None:
        names = [workload["name"] for workload in SUITES["generic"]]
        args = parse_args(["generic", "--threads", "1", "--workloads", names[3], names[0]])
        self.assertEqual([workload["name"] for workload in args.workloads], [names[0], names[3]])

    def test_an_unknown_workload_is_rejected(self) -> None:
        self.assertIn(
            "unknown generic workloads: q01", self.rejected("generic", "--threads", "1", "--workloads", "q01")
        )

    def test_a_thread_count_without_thresholds_is_rejected(self) -> None:
        self.assertIn("no tpch workloads are qualified on 4 threads", self.rejected("tpch", "--threads", "4"))
        self.assertIn(
            "no thresholds on 4 threads for: decimal_grouped",
            self.rejected("encodings", "--threads", "4", "--workloads", "decimal_grouped"),
        )

    def test_a_suite_runs_the_workloads_qualified_for_the_thread_count(self) -> None:
        qualified = parse_args(["encodings", "--threads", "4"]).workloads
        self.assertTrue(qualified)
        self.assertTrue(all(4 in workload["floor"] for workload in qualified))
        self.assertLess(len(qualified), len(SUITES["encodings"]))


class WorkloadsTest(unittest.TestCase):
    def test_every_workload_has_its_thresholds_on_the_same_thread_counts(self) -> None:
        for suite, threads in (("generic", {1, 4}), ("encodings", {1, 4}), ("tpch", {1})):
            for workload in SUITES[suite]:
                with self.subTest(workload=workload["name"]):
                    qualified = set(workload["floor"])
                    self.assertTrue(qualified and qualified <= threads)
                    self.assertEqual(set(workload["ceiling"]), qualified)
                    self.assertEqual(set(workload["compiled"]), qualified)
                    self.assertTrue(all(isinstance(compiled, bool) for compiled in workload["compiled"].values()))

    def test_workload_names_are_unique(self) -> None:
        for suite, workloads in SUITES.items():
            names = [workload["name"] for workload in workloads]
            self.assertEqual(len(names), len(set(names)), suite)

    def test_workloads_that_share_a_fixture_are_adjacent(self) -> None:
        for suite, workloads in SUITES.items():
            fixtures = [workload.get("setup_sql", "") for workload in workloads]
            runs = [fixture for index, fixture in enumerate(fixtures) if index == 0 or fixtures[index - 1] != fixture]
            self.assertEqual(len(runs), len(set(runs)), suite)


if __name__ == "__main__":
    unittest.main()
