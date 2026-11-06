#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_tpch_benchmark import verify_cbo_runtime_counter_contract


def compiled_row(requirements: str, **work: str) -> dict[str, str]:
    row = {
        "query": "1",
        "policy": "auto",
        "repeat": "1",
        "backend_name": "jit_sljit",
        "kernel_id": "7",
        "status": "compiled",
        "execution_mode": "native",
        "selected_runner": "compiled_vectorized",
        "runner_cost_profile": "true",
        "runner_cost_selected_accelerated_runner_count": "1",
        "runner_cost_required_runtime_proofs": requirements,
    }
    row.update(work)
    return row


def runtime_row(proofs: str, status: str = "executed") -> dict[str, str]:
    return {
        "query": "1",
        "policy": "auto",
        "repeat": "1",
        "backend_name": "jit_sljit",
        "kernel_id": "7",
        "status": status,
        "execution_mode": "native",
        "jit_runtime_proof_counts": proofs,
        "invocation_count": "1",
    }


class TestTypedRuntimeProofLedger(unittest.TestCase):
    def test_selected_compile_failure_is_an_explicit_outcome(self) -> None:
        row = compiled_row("full_pipeline_ownership", runner_cost_full_pipeline_work="4096")
        row.update({"kernel_id": "0", "status": "error", "blocker": "backend_compile_error"})
        verify_cbo_runtime_counter_contract([row], require_runtime_proof=True)

    def test_selected_noncompiled_row_requires_explicit_failure(self) -> None:
        row = compiled_row("full_pipeline_ownership", runner_cost_full_pipeline_work="4096")
        row.update({"kernel_id": "0", "status": "error", "blocker": ""})
        with self.assertRaises(AssertionError):
            verify_cbo_runtime_counter_contract([row], require_runtime_proof=True)

    def test_declared_requirement_accepts_matching_runtime_proof(self) -> None:
        rows = [
            compiled_row("backend_owned_work", runner_cost_native_operator_work="4096"),
            runtime_row("backend_owned_work=1"),
        ]
        verify_cbo_runtime_counter_contract(rows, require_runtime_proof=True)

    def test_credited_work_without_requirement_is_rejected(self) -> None:
        rows = [compiled_row("", runner_cost_generated_stage_work="100")]
        with self.assertRaises(AssertionError):
            verify_cbo_runtime_counter_contract(rows, require_runtime_proof=False)

    def test_missing_declared_runtime_proof_is_rejected(self) -> None:
        rows = [
            compiled_row("full_pipeline_ownership", runner_cost_full_pipeline_work="4096"),
            runtime_row("generated_stage_work=1"),
        ]
        with self.assertRaises(AssertionError):
            verify_cbo_runtime_counter_contract(rows, require_runtime_proof=True)

    def test_runtime_error_does_not_exempt_sibling_invocations(self) -> None:
        rows = [
            compiled_row("full_pipeline_ownership", runner_cost_full_pipeline_work="4096"),
            runtime_row("full_pipeline_ownership=1"),
            runtime_row("", status="error"),
        ]
        with self.assertRaises(AssertionError):
            verify_cbo_runtime_counter_contract(rows, require_runtime_proof=True)

    def test_unknown_requirement_is_rejected(self) -> None:
        rows = [compiled_row("unknown_proof"), runtime_row("unknown_proof=1")]
        with self.assertRaises(AssertionError):
            verify_cbo_runtime_counter_contract(rows, require_runtime_proof=True)

    def test_stage_timers_cannot_prove_execution(self) -> None:
        for proof in (
            "accelerated_stage_work",
            "backend_owned_work",
            "generated_stage_work",
        ):
            row = runtime_row("")
            row.update(
                {
                    "input_rows": "10",
                    "generated_stage_count_breakdown": "native_sink_update=1",
                }
            )
            with self.subTest(proof=proof), self.assertRaises(AssertionError):
                verify_cbo_runtime_counter_contract([compiled_row(proof), row], require_runtime_proof=True)

    def test_delegation_and_materialization_do_not_prove_generated_execution(
        self,
    ) -> None:
        for proofs in (
            "delegated_runtime_work=10",
            "materialization_elision=10;backend_owned_work=10",
        ):
            with self.subTest(proofs=proofs), self.assertRaises(AssertionError):
                verify_cbo_runtime_counter_contract(
                    [compiled_row("generated_stage_work"), runtime_row(proofs)],
                    require_runtime_proof=True,
                )

    def test_materialization_elision_accepts_only_the_declared_native_pipeline_handoff(self) -> None:
        def handoff_row(handed_off: str, delegations: str) -> dict[str, str]:
            row = runtime_row("materialization_elision=87;backend_owned_work=87;delegated_runtime_work=20")
            row["jit_runtime_path_counts"] = handed_off
            row["jit_runtime_delegation_counts"] = delegations
            return row

        delegations = "projection.native_tail_delegation=10;aggregate_update.native_sink_update=10"
        verify_cbo_runtime_counter_contract(
            [
                compiled_row("materialization_elision"),
                handoff_row("aggregate_update.no_group_runs_native_pipeline=10", delegations),
            ],
            require_runtime_proof=True,
        )
        for handed_off in ("", "aggregate_update.no_group_runs_native_pipeline=9", "no_group_runs_native_pipeline=10"):
            with self.subTest(handed_off=handed_off), self.assertRaises(AssertionError):
                verify_cbo_runtime_counter_contract(
                    [compiled_row("materialization_elision"), handoff_row(handed_off, delegations)],
                    require_runtime_proof=True,
                )

    def test_proof_requires_a_positive_exact_typed_counter(self) -> None:
        for counter in (
            "generated_stage_work=0",
            "generated_stage_work=-1",
            "generated_stage_work",
            "generated_stage_work=invalid",
            "native.generated_stage_work=1",
        ):
            with self.subTest(counter=counter), self.assertRaises(AssertionError):
                verify_cbo_runtime_counter_contract(
                    [compiled_row("generated_stage_work"), runtime_row(counter)],
                    require_runtime_proof=True,
                )


if __name__ == "__main__":
    unittest.main()
