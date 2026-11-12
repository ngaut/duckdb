#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from counter_contract import RUNTIME_WORK_FIELDS, counter_contract_failures

COUNTER_FIELDS = {
    "count": "1",
    "backend_name": "jit_sljit",
    "kernel_id": "7",
    "execution_mode": "native",
    "selected_runner": "compiled_vectorized",
    "blocker": "",
    "runner_cost_selection_reason": "",
    "runner_cost_required_runtime_proofs": "",
    "jit_runtime_proof_counts": "",
    **{field: "" for field in RUNTIME_WORK_FIELDS},
}


def selected_row(proofs: str, **fields) -> dict:
    """The decision row of a runner the cost model selected and the backend compiled."""
    return {
        **COUNTER_FIELDS,
        "status": "compiled",
        "runner_cost_profile": True,
        "runner_cost_selected_accelerated_runner_count": "1",
        "runner_cost_selection_reason": "admitted_saved_work",
        "runner_cost_rows": "4096",
        "runner_cost_batches": "2",
        "runner_cost_required_runtime_proofs": proofs,
        **fields,
    }


def runtime_row(proofs: str, **fields) -> dict:
    """The row of the invocations of the selected runner's kernel."""
    return {
        **COUNTER_FIELDS,
        "status": "executed",
        "invocation_count": "1",
        "jit_runtime_proof_counts": proofs,
        **fields,
    }


class SelectedRunnerTest(unittest.TestCase):
    def assert_holds(self, rows: list[dict]) -> None:
        self.assertEqual(counter_contract_failures(rows), [])

    def assert_fails(self, rows: list[dict], failure: str) -> None:
        failures = counter_contract_failures(rows)
        self.assertTrue(any(failure in reported for reported in failures), failures)

    def test_a_runtime_proof_of_the_owed_work_holds(self) -> None:
        self.assert_holds(
            [
                selected_row("backend_owned_work", runner_cost_native_operator_work="4096"),
                runtime_row("backend_owned_work=1"),
            ]
        )

    def test_a_compile_failure_that_names_its_blocker_holds(self) -> None:
        failed = selected_row(
            "full_pipeline_ownership", runner_cost_full_pipeline_work="4096", status="error", kernel_id="0"
        )
        self.assert_holds([{**failed, "blocker": "backend_compile_error"}])
        self.assert_fails([failed], "no blocker")
        self.assert_fails([failed], "neither compiled nor reported a failure")

    def test_credited_work_owes_a_proof(self) -> None:
        self.assert_fails(
            [selected_row("", runner_cost_generated_stage_work="100")], "credits work that owes no runtime proof"
        )

    def test_a_kernel_that_never_ran_proves_nothing(self) -> None:
        self.assert_fails([selected_row("full_pipeline_ownership")], "compiled kernel has no runtime counters")

    def test_a_proof_of_other_work_does_not_count(self) -> None:
        self.assert_fails(
            [selected_row("full_pipeline_ownership"), runtime_row("generated_stage_work=1")],
            "no runtime proof of full_pipeline_ownership",
        )
        for proofs in ("delegated_runtime_work=10", "materialization_elision=10;backend_owned_work=10"):
            with self.subTest(proofs=proofs):
                self.assert_fails(
                    [selected_row("generated_stage_work"), runtime_row(proofs)],
                    "no runtime proof of generated_stage_work",
                )

    def test_a_runtime_error_fails_whatever_the_other_invocations_proved(self) -> None:
        self.assert_fails(
            [
                selected_row("full_pipeline_ownership"),
                runtime_row("full_pipeline_ownership=1"),
                runtime_row("", status="error", blocker="runtime_error"),
            ],
            "kernel recorded a runtime error",
        )

    def test_an_unknown_requirement_fails(self) -> None:
        self.assert_fails(
            [selected_row("unknown_proof"), runtime_row("unknown_proof=1")],
            "unknown runtime proof requirement unknown_proof",
        )

    def test_stage_counters_do_not_prove_stage_work(self) -> None:
        for proof in ("accelerated_stage_work", "backend_owned_work", "generated_stage_work"):
            with self.subTest(proof=proof):
                self.assert_fails(
                    [selected_row(proof), runtime_row("", generated_stage_count_breakdown="native_sink_update=1")],
                    f"no runtime proof of {proof}",
                )

    def test_a_proof_is_a_positive_count_under_the_exact_name(self) -> None:
        for proofs in (
            "generated_stage_work=0",
            "generated_stage_work=-1",
            "generated_stage_work",
            "native.generated_stage_work=1",
        ):
            with self.subTest(proofs=proofs):
                self.assert_fails(
                    [selected_row("generated_stage_work"), runtime_row(proofs)],
                    "no runtime proof of generated_stage_work",
                )

    def test_a_kernel_that_handed_its_pipeline_over_owes_no_proof(self) -> None:
        self.assert_holds([selected_row("generated_stage_work"), runtime_row("native_continuation=2048")])
        self.assert_holds([selected_row("materialization_elision"), runtime_row("native_continuation=2048")])

    def test_a_kernel_that_received_no_work_owes_no_proof_of_it(self) -> None:
        self.assert_holds([selected_row("generated_stage_work"), runtime_row("no_work=1")])
        self.assert_holds([selected_row("full_pipeline_ownership"), runtime_row("no_work=1")])
        self.assert_fails(
            [selected_row("generated_stage_work"), runtime_row("no_work=1", lazy_code_size="64")],
            "no runtime proof of generated_stage_work",
        )

    def test_materialization_elision_admits_only_the_delegations_of_the_native_pipeline_handoff(self) -> None:
        delegations = "projection.native_tail_delegation=10;aggregate_update.native_sink_update=10"

        def rows(handed_off: str) -> list[dict]:
            return [
                selected_row("materialization_elision"),
                runtime_row(
                    "materialization_elision=87;backend_owned_work=87;delegated_runtime_work=20",
                    jit_runtime_path_counts=handed_off,
                    jit_runtime_delegation_counts=delegations,
                ),
            ]

        self.assert_holds(rows("aggregate_update.no_group_runs_native_pipeline=10"))
        for handed_off in ("", "aggregate_update.no_group_runs_native_pipeline=9", "no_group_runs_native_pipeline=10"):
            with self.subTest(handed_off=handed_off):
                self.assert_fails(rows(handed_off), "materialization-elision kernel delegated work")

    def test_materialization_elision_admits_no_materialization_work(self) -> None:
        self.assert_fails(
            [
                selected_row("materialization_elision"),
                runtime_row("materialization_elision=1", generated_stage_count_breakdown="buffer_append=3"),
            ],
            "materialization-elision kernel did materialization work: generated_stage_count_breakdown buffer_append",
        )


class DecisionTest(unittest.TestCase):
    def test_a_selection_reason_agrees_with_the_selection(self) -> None:
        rejected = selected_row("", runner_cost_selected_accelerated_runner_count="0")
        self.assertIn("selection reason contradicts the selection", counter_contract_failures([rejected])[0])
        self.assertEqual(
            counter_contract_failures([{**rejected, "runner_cost_selection_reason": "rejected_no_saved_work"}]), []
        )

    def test_saved_work_has_a_cost_component(self) -> None:
        row = selected_row("generated_stage_work", runner_cost_saved_work_per_batch="100")
        failures = counter_contract_failures([row, runtime_row("generated_stage_work=1")])
        self.assertIn("saved work has no cost component", failures[0])

    def test_a_costed_decision_costs_rows(self) -> None:
        failures = counter_contract_failures([selected_row("", runner_cost_rows="0")])
        self.assertIn("costed no rows", failures[0])


if __name__ == "__main__":
    unittest.main()
