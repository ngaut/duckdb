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


def adaptive_fallback_row() -> dict[str, str]:
    return {
        "query": "1",
        "policy": "auto",
        "repeat": "1",
        "backend_name": "jit_sljit",
        "kernel_id": "7",
        "status": "executed",
        "execution_mode": "native",
        "runtime_result": "adaptive_ab",
        "reason": "adaptive_ab verdict=fallback_native compiled_leg_us=375 native_leg_us=93",
    }


class TestTypedRuntimeProofLedger(unittest.TestCase):
    def test_adaptive_fallback_marker_does_not_exempt_missing_proof(self) -> None:
        rows = [
            compiled_row("full_pipeline_ownership", runner_cost_full_pipeline_work="4096"),
            adaptive_fallback_row(),
        ]
        with self.assertRaises(AssertionError):
            verify_cbo_runtime_counter_contract(rows, require_runtime_proof=True)

    def test_adaptive_fallback_accepts_deferred_compiled_leg_proof(self) -> None:
        rows = [
            compiled_row("full_pipeline_ownership", runner_cost_full_pipeline_work="4096"),
            runtime_row("full_pipeline_ownership=1", status="skipped"),
            adaptive_fallback_row(),
        ]
        verify_cbo_runtime_counter_contract(rows, require_runtime_proof=True)

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
            compiled_row("generated_backend_work", runner_cost_native_operator_work="4096"),
            runtime_row("generated_backend_work=1"),
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


if __name__ == "__main__":
    unittest.main()
