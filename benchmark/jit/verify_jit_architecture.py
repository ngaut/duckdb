#!/usr/bin/env python3
#
# Structural verifier for DuckDB execution-region JIT.
# Keep this file small: verify production contracts, not implementation prose.

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8", errors="replace")


def files(globs: tuple[str, ...]) -> list[Path]:
    result: list[Path] = []
    for pattern in globs:
        result.extend(path for path in ROOT.glob(pattern) if path.is_file())
    return sorted(set(result))


def reject_regex(name: str, patterns: tuple[str, ...], globs: tuple[str, ...], allowed: tuple[str, ...] = ()) -> None:
    allowed_paths = {(ROOT / path).resolve() for path in allowed}
    compiled = [(pattern, re.compile(pattern)) for pattern in patterns]
    for path in files(globs):
        if path.resolve() in allowed_paths:
            continue
        data = path.read_text(encoding="utf-8", errors="replace")
        for pattern, regex in compiled:
            if regex.search(data):
                raise AssertionError(f"{rel(path)}: {name}: {pattern}")


def verify_layer_boundaries() -> None:
    reject_regex(
        "SLJIT dependency in core DuckDB",
        (r"\bsljit\b", r"\bSljit\b", r"\bjit_sljit\b"),
        ("src/**/*.hpp", "src/**/*.cpp"),
        ("src/main/extension/extension_helper.cpp",),
    )
    reject_regex(
        "backend reaches into operator-private implementation",
        (r'#include "duckdb/execution/operator/',),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
    )


def verify_no_benchmark_shaped_logic() -> None:
    reject_regex(
        "benchmark-shaped production JIT logic",
        (
            r"\bQ[0-9]+(?:-like)?\b",
            r"\bq[0-9]+_",
            r"\bTPC-H\b",
            r"\btpch\b",
            r"\blineitem\b",
            r"\bpartsupp\b",
            r"\bsupplier\b",
            r"\bcustomer\b",
            r"\bnation\b",
            r"Brand#",
        ),
        (
            "extension/jit_sljit/**/*.hpp",
            "extension/jit_sljit/**/*.cpp",
            "src/execution/execution_region*.cpp",
            "src/include/duckdb/execution/execution_region*.hpp",
            "src/planner/cost_model.cpp",
            "src/include/duckdb/planner/cost_model.hpp",
        ),
    )
    reject_regex(
        "benchmark-shaped JIT API coverage",
        (
            r"\bQ[0-9]+(?:-like)?\b",
            r"\bTPC-H\b",
            r"\btpch\b",
            r"\bjit_q[0-9]",
            r"\bl_returnflag\b",
            r"\bl_linestatus\b",
            r"\bl_shipdate\b",
            r"\bl_extendedprice\b",
            r"\bl_discount\b",
            r"\bl_quantity\b",
            r"\bl_tax\b",
            r"\bp_type\b",
            r"\bnation_id\b",
            r"\bnation_name\b",
        ),
        ("test/api/test_jit*.cpp", "test/api/test_jit_helpers.hpp"),
    )


def verify_no_whole_executor_delegation() -> None:
	reject_regex(
		"whole-executor delegation in compiled layers",
        (
            r"\bExecuteInterpreted\b",
            r"\bValue::Evaluate\b",
            r"\bExecutionCreateHashJoinProbeState\b",
            r"\bExecutionProbeHashJoin\b",
            r"\bExecutionSinkAggregateUpdate\b",
            r"\bfallback-native\b",
            r"\bwhole[-_ ]executor\b",
        ),
        (
            "src/execution/**/*.cpp",
            "src/include/duckdb/execution/**/*.hpp",
            "extension/jit_sljit/**/*.cpp",
            "extension/jit_sljit/**/*.hpp",
		),
	)


def verify_projection_aggregate_prefix_is_generic() -> None:
	reject_regex(
		"projection-aggregate prefix capped at single/two join shape",
		(
			r"\bSINGLE_JOIN\b",
			r"\bTWO_JOIN\b",
			r"\bTwoJoin\b",
			r"\btwo-join\b",
			r"\bHasPreJoinProjection\b",
			r"\bHasSecondHashJoin\b",
			r"\bHasBetweenProjection\b",
			r"\bPreJoinProjectionIdx\b",
			r"\bBetweenProjectionIdx\b",
			r"\bFirstHashJoinIdx\b",
			r"\bSecondHashJoinIdx\b",
			r"\bpre_join_projection_idx\b",
			r"\bbetween_projection_idx\b",
			r"\bsecond_hash_join_idx\b",
		),
		(
			"extension/jit_sljit/include/sljit_projection_aggregate_recipe*.hpp",
			"extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
		),
	)
	facts = read("extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp")
	if "vector<SljitProjectionAggregateJoinPrefixStep> joins" not in facts:
		raise AssertionError("projection-aggregate prefix facts must store join stages as a generic list")


def verify_no_stale_runtime_expectation_helpers() -> None:
	reject_regex(
		"stale JIT runtime expectation",
		(
			r"\bsource_batch_boundary\b",
			r"\bprojected_compact_aggregate_input\b",
			r"\bRequireCurrentMaterializationElisionProofName\b",
			r"\bRequireCurrentMaterializationElisionRuntimeProof\b",
		),
		(
			"test/api/test_jit*.cpp",
			"test/api/test_jit_helpers.hpp",
			"benchmark/jit/**/*.py",
			"benchmark/tpch/jit/**/*.py",
			"extension/jit_sljit/**/*.hpp",
			"extension/jit_sljit/**/*.cpp",
		),
	)


def verify_runtime_proofs_are_typed() -> None:
	runtime_header = read("src/include/duckdb/execution/execution_region_runtime.hpp")
	if "enum class ExecutionRegionJitRuntimeProof" not in runtime_header:
		raise AssertionError("JIT runtime proof contract must use typed semantic proof ids")
	if "FULL_PIPELINE_OWNERSHIP" not in runtime_header:
		raise AssertionError("full-pipeline CBO credit must have a typed runtime proof id")
	runtime_trace = read("extension/jit_sljit/sljit_region_runtime_trace.cpp")
	if "RecordSljitRegionMaterializationElisionPath" not in runtime_trace:
		raise AssertionError("SLJIT materialization-elision proof must have an explicit trace API")
	source_fetch = read("extension/jit_sljit/include/sljit_source_fetch_primitive_runtime.hpp")
	if "ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP" not in source_fetch:
		raise AssertionError("SLJIT source-fetch ownership must emit full-pipeline runtime proof")
	tpch_verifier = read("benchmark/tpch/jit/verify_tpch_benchmark.py")
	if "verify_selected_generated_stage_runtime_contract" not in tpch_verifier:
		raise AssertionError("TPC-H verifier must compare generated-stage CBO credit with runtime counters")
	if "verify_selected_native_operator_runtime_contract" not in tpch_verifier:
		raise AssertionError("TPC-H verifier must compare native-operator CBO credit with runtime proof")
	if "full_pipeline_ownership" not in tpch_verifier:
		raise AssertionError("TPC-H verifier must compare full-pipeline CBO credit with runtime proof")
	reject_regex(
		"stringly typed JIT runtime proof emission",
		(r"RecordJitRuntimeProof\(\s*\"",),
		(
			"src/**/*.hpp",
			"src/**/*.cpp",
			"extension/jit_sljit/**/*.hpp",
			"extension/jit_sljit/**/*.cpp",
			"test/api/test_jit*.cpp",
			"test/api/test_jit_helpers.hpp",
		),
	)
	reject_regex(
		"string-inferred materialization-elision proof",
		(
			r"SljitAggregateRuntimePathProvesMaterializationElision",
			r"proof_path\.find",
			r"RecordSljitRegionRuntimePath[^{]+ExecutionRegionJitRuntimeProof::MATERIALIZATION_ELISION",
		),
		(
			"extension/jit_sljit/**/*.hpp",
			"extension/jit_sljit/**/*.cpp",
		),
	)


def main() -> None:
	verify_layer_boundaries()
	verify_no_benchmark_shaped_logic()
	verify_no_whole_executor_delegation()
	verify_projection_aggregate_prefix_is_generic()
	verify_no_stale_runtime_expectation_helpers()
	verify_runtime_proofs_are_typed()
	print("Execution-region architecture verification passed")


if __name__ == "__main__":
    main()
