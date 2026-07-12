#!/usr/bin/env python3
#
# Structural verifier for DuckDB execution-region JIT.
# Keep this file small: verify production contracts, not implementation prose.

from __future__ import annotations

import json
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


def verify_aggregate_payload_descriptor_owns_typed_abi() -> None:
	reject_regex(
		"aggregate typed payload ABI reconstructed outside descriptor binding",
		(r"\baggregate\.child_types\[0\]", r"\baggregate\.primitive_update_input_type\b"),
		(
			"extension/jit_sljit/sljit_aggregate_fused_codegen.cpp",
			"extension/jit_sljit/sljit_aggregate_typed_payload_codegen.cpp",
			"extension/jit_sljit/sljit_aggregate_grouped_typed_codegen.cpp",
			"extension/jit_sljit/sljit_aggregate_ungrouped_codegen.cpp",
			"extension/jit_sljit/sljit_aggregate_ungrouped_filtered_codegen.cpp",
			"extension/jit_sljit/include/sljit_region_aggregate_payload_fusion.hpp",
		),
	)
	reject_regex(
		"aggregate payload adapter depends on planner aggregate metadata",
		(r"\bExecutionRegionAggregateInput\b", r"\bprimitive_update_kind\b"),
		(
			"extension/jit_sljit/include/sljit_aggregate_filtered_payload_runtime.hpp",
			"extension/jit_sljit/include/sljit_aggregate_fused_payload_sources.hpp",
			"extension/jit_sljit/include/sljit_aggregate_payload_runtime.hpp",
			"extension/jit_sljit/include/sljit_aggregate_perfect_hash_payload_runtime.hpp",
		),
	)
	payload_sources = read("extension/jit_sljit/include/sljit_aggregate_fused_payload_sources.hpp")
	if "const vector<SljitAggregatePayloadDescriptor> &descriptors" not in payload_sources:
		raise AssertionError("fused aggregate payload source classification must consume canonical descriptors")
	reject_regex(
		"aggregate runtime reconstructs payload metadata from planner aggregate records",
		(r"\bExecutionRegionAggregateInput\b", r"\bprimitive_update_kind\b"),
		("extension/jit_sljit/include/*runtime*.hpp",),
	)
	runtime_state = read("extension/jit_sljit/include/sljit_region_runtime_state.hpp")
	if "AggregatePayloadLanes(idx_t op_idx, const vector<SljitAggregatePayloadDescriptor> &payload_descriptors" not in runtime_state:
		raise AssertionError("aggregate runtime lane lookup must be keyed by canonical payload descriptors")


def verify_grouped_reduction_lane_binding() -> None:
	bound_runtime = read("extension/jit_sljit/include/sljit_region_execution_scratch_helpers.hpp")
	if "optional_ptr<const vector<SljitGroupedReductionLaneBinding>> reduction_lanes" not in bound_runtime:
		raise AssertionError("grouped aggregate sink binding must reference cached validated reduction lanes")
	if "struct SljitAggregateOperatorScratch" not in bound_runtime or "bound_grouped_update" not in bound_runtime:
		raise AssertionError("aggregate operator scratch must own the grouped sink binding")
	if "enum class SljitBoundGroupedAggregateStrategy" not in bound_runtime:
		raise AssertionError("grouped aggregate execution family must be an immutable bound strategy")
	grouped_executor = read("extension/jit_sljit/include/sljit_grouped_primitive_aggregate_update_runtime.hpp")
	if "switch (bound.strategy)" not in grouped_executor:
		raise AssertionError("grouped aggregate chunks must dispatch through the bound strategy")
	if "if (op.aggregate_update.fused_payload_update_function)" in grouped_executor:
		raise AssertionError("grouped aggregate execution must not rediscover its payload strategy per chunk")
	for stale_parallel_state in (
		"vector<bool> payload_lanes_ready",
		"vector<bool> grouped_reduction_lanes_ready",
		"SljitDirectAggregateUpdateTracker direct_new {8,",
	):
		if stale_parallel_state in bound_runtime:
			raise AssertionError("aggregate scratch must not use parallel operator-indexed lifetime arrays")
	reject_regex(
		"runtime-local grouped aggregate sink binding",
		(r"SljitBoundGroupedPrimitiveAggregateUpdate\s+(?!bound_grouped_update\b)\w+",),
		("extension/jit_sljit/include/*runtime*.hpp",),
	)
	lane_contract = read("extension/jit_sljit/include/sljit_aggregate_payload_lane_contract.hpp")
	if "SljitAggregatePayloadDescriptorMatchesLane" not in lane_contract:
		raise AssertionError("aggregate payload runtime must have one canonical descriptor-to-lane validator")
	runtime_state = read("extension/jit_sljit/include/sljit_region_runtime_state.hpp")
	if "GroupedReductionLanes(idx_t op_idx" not in runtime_state:
		raise AssertionError("grouped reduction lanes must be cached in executor runtime scratch")
	reject_regex(
		"chunk-local grouped reduction lane rebinding",
		(r"vector<SljitGroupedReductionLaneBinding>\s+reduction_lanes",),
		("extension/jit_sljit/include/*runtime*.hpp",),
	)
	for runtime_path in (
		"extension/jit_sljit/include/sljit_aggregate_payload_runtime.hpp",
		"extension/jit_sljit/include/sljit_aggregate_perfect_hash_payload_runtime.hpp",
	):
		runtime_text = read(runtime_path)
		if "const vector<SljitAggregatePayloadDescriptor> &payload_descriptors" not in runtime_text:
			raise AssertionError(f"{runtime_path}: fused grouped adapter must receive payload descriptors")
		if "const vector<SljitGroupedReductionLaneBinding> &reduction_lanes" not in runtime_text:
			raise AssertionError(f"{runtime_path}: fused grouped adapter must consume cached reduction lanes")
	reject_regex(
		"stale grouped primitive layout validator",
		(r"\bSljitValidateGroupedPrimitiveLane(?:Layout|State)\b",),
		("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
	)


def verify_production_contract_ownership() -> None:
	shell_timer = read("tools/shell/shell.cpp")
	for timing_contract in (
		"std::chrono::steady_clock::now()",
		"std::chrono::microseconds",
		'Run Time (s): real %.6f',
	):
		if timing_contract not in shell_timer:
			raise AssertionError("production benchmark shell timing must be monotonic and microsecond-resolution")
	settings = json.loads(read("src/common/settings.json"))
	settings_by_name = {setting["name"]: setting for setting in settings}
	settings_header = read("src/include/duckdb/main/settings.hpp")
	for name in (
		"jit_cbo_full_pipeline_benefit",
		"jit_cbo_generated_stage_benefit",
		"jit_cbo_materialization_elision_benefit",
		"jit_cbo_native_operator_stage_benefit",
		"jit_cbo_source_contract_scan_filter_penalty",
		"jit_cbo_startup_base_cost",
		"jit_cbo_startup_margin_basis_points",
	):
		if name not in settings_by_name:
			raise AssertionError(f"settings.json must own {name}")
		name_offset = settings_header.find(f'Name = "{name}"')
		if name_offset < 0:
			raise AssertionError(f"generated settings header is missing {name}")
		setting_block = settings_header[name_offset : name_offset + 1000]
		default_match = re.search(r'DefaultValue = "([^"]+)"', setting_block)
		if not default_match or default_match.group(1) != settings_by_name[name]["default_value"]:
			raise AssertionError(f"generated settings default drift for {name}")

	lazy_artifact = read("extension/jit_sljit/include/sljit_compiled_function.hpp")
	lazy_runtime = read("extension/jit_sljit/sljit_region_runtime.cpp")
	if (
		"state->ready.load(std::memory_order_acquire)" not in lazy_artifact
		or "state->ready.store(true, std::memory_order_release)" not in lazy_artifact
		or "std::call_once(state->once" not in lazy_artifact
		or "codegen_lock" in lazy_runtime
	):
		raise AssertionError("lazy machine code must use one-time publication without double-checked locking")
	kernel_header = read("src/include/duckdb/execution/execution_region_kernel.hpp")
	if "std::atomic<idx_t> trace_lazy_code_size" not in kernel_header:
		raise AssertionError("lazy code size telemetry must be atomic and separate from immutable kernel metadata")

	backend_header = read("src/include/duckdb/execution/execution_region_backend.hpp")
	for declaration in (
		"class DUCKDB_API ExecutionRegionBackendPlan",
		"struct DUCKDB_API ExecutionRegionCompilationInput",
		"struct DUCKDB_API ExecutionRegionCompileResult",
		"class DUCKDB_API ExecutionRegionBackend",
	):
		if declaration not in backend_header:
			raise AssertionError(f"loadable execution-region ABI is missing export: {declaration}")

	regression_gate = read("benchmark/tpch/jit/run_tpch_regression_gate.py")
	if 'summary_counter(\n                row, "runner_cost_selected_accelerated_runner_count"' not in regression_gate:
		raise AssertionError("runtime proof selection must include CBO-admitted accelerated runners")
	if "apply_baseline_state_contract(args, state)" not in regression_gate or '"--scale-factor",\n        type=float,\n        default=None' not in regression_gate:
		raise AssertionError("TPC-H candidate configuration must inherit and validate the accepted baseline contract")
	comparator = read("benchmark/tpch/jit/compare_tpch_benchmark.py")
	runtime_gate = comparator[comparator.index("def auto_runtime_preserved") : comparator.index("def has_auto_decision")]
	if "off_normalized_candidate_auto_s" in runtime_gate or "return raw_slowdown_s <= allowed_s" not in runtime_gate:
		raise AssertionError("baseline acceptance must require the independent raw auto-runtime ceiling")
	reject_regex(
		"aggregate descriptor-to-lane ABI reconstructed outside canonical validator",
		(
			r"\bdescriptor\.(?:primitive_kind|aggregate_index|state_size|state_value_offset|state_is_set_offset|input_type)\s*!=\s*(?:runtime_)?lane",
		),
		(
			"extension/jit_sljit/include/sljit_aggregate_primitive_payload_runtime.hpp",
			"extension/jit_sljit/include/sljit_grouped_reduction_lane.hpp",
		),
	)
	reject_regex(
		"grouped reduction state ABI reconstructed outside lane binding",
		(
			r"\baggregate\.primitive_update_(?:kind|state_size|state_value_offset|state_is_set_offset|input_type)\b",
			r"\blane->(?:aggregate_index|kind|state_offset|state_value_offset|state_is_set_offset)\b",
		),
		(
			"extension/jit_sljit/include/sljit_grouped_aggregate_direct_update_capability_runtime.hpp",
			"extension/jit_sljit/include/sljit_grouped_aggregate_input_vector_run_update_runtime.hpp",
			"extension/jit_sljit/include/sljit_grouped_aggregate_preaggregated_update_runtime.hpp",
			"extension/jit_sljit/include/sljit_grouped_count_star_update_runtime.hpp",
			"extension/jit_sljit/include/sljit_string_set_complementary_sum_runtime.hpp",
			"extension/jit_sljit/sljit_aggregate_perfect_hash_commit_codegen.cpp",
		),
	)


def verify_runtime_proofs_are_typed() -> None:
	common_header = read("src/include/duckdb/execution/execution_region_common.hpp")
	if "enum class ExecutionRegionJitRuntimeProof" not in common_header:
		raise AssertionError("JIT runtime proof contract must use typed semantic proof ids")
	if "FULL_PIPELINE_OWNERSHIP" not in common_header:
		raise AssertionError("full-pipeline CBO credit must have a typed runtime proof id")
	cost_profile = read("src/include/duckdb/planner/cost_model.hpp")
	if "ExecutionRegionJitRuntimeProofMask required_runtime_proofs" not in cost_profile:
		raise AssertionError("CBO cost profiles must declare typed runtime proof requirements")
	cost_model = read("src/planner/cost_model.cpp")
	if "PhysicalRunnerBuildRuntimeProofRequirements" not in cost_model:
		raise AssertionError("CBO credited work must build its runtime proof ledger centrally")
	runtime_trace = read("extension/jit_sljit/sljit_region_runtime_trace.cpp")
	if "RecordSljitRegionMaterializationElisionPath" not in runtime_trace:
		raise AssertionError("SLJIT materialization-elision proof must have an explicit trace API")
	source_fetch = read("extension/jit_sljit/include/sljit_source_fetch_primitive_runtime.hpp")
	if "ExecutionRegionJitRuntimeProof::FULL_PIPELINE_OWNERSHIP" not in source_fetch:
		raise AssertionError("SLJIT source-fetch ownership must emit full-pipeline runtime proof")
	tpch_verifier = read("benchmark/tpch/jit/verify_tpch_benchmark.py")
	if "runtime_proof_requirements" not in tpch_verifier or "runtime_proof_requirement_satisfied" not in tpch_verifier:
		raise AssertionError("TPC-H verifier must consume the CBO runtime proof ledger")
	if "verify_selected_generated_stage_runtime_contract" in tpch_verifier:
		raise AssertionError("TPC-H verifier must not reconstruct proof requirements from individual CBO columns")
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


def verify_compiled_artifact_ownership() -> None:
	artifact = read("extension/jit_sljit/include/sljit_compiled_function.hpp")
	for immutable_contract in (
		"static SljitCompiledFunction TryCreate",
		"FUNCTION Function() const",
	):
		if immutable_contract not in artifact:
			raise AssertionError(f"compiled artifacts are missing immutable construction contract: {immutable_contract}")
	for mutable_api in (
		"shared_ptr<ExecutionRegionCodeHandle> &Code()",
		"FUNCTION &Function()",
		"void Set(",
	):
		if mutable_api in artifact:
			raise AssertionError(f"compiled artifacts must not expose split mutable state: {mutable_api}")
	eager_artifact = artifact.split("class SljitLazyCompiledFunction", 1)[0]
	if "class BUILD" in eager_artifact:
		raise AssertionError("compiled artifact ownership must not template code-generation policy into the artifact")
	reject_regex(
		"split compiled artifact publication",
		(
			r"\.(?:Code|Function)\(\)\s*=",
			r"\.Set\(\s*std::move\([^\n]+code",
		),
		(
			"extension/jit_sljit/**/*.hpp",
			"extension/jit_sljit/**/*.cpp",
		),
	)


def verify_scan_filter_ownership() -> None:
	common = read("src/include/duckdb/execution/execution_region_common.hpp")
	for mode in ("NONE", "ALL", "DYNAMIC_ONLY"):
		if mode not in common:
			raise AssertionError(f"scan-filter ownership is missing explicit mode: {mode}")
	lowering = read("src/include/duckdb/execution/execution_region_lowering.hpp")
	if "ExecutionRegionScanFilterMode scan_filter_mode" not in lowering:
		raise AssertionError("backend lowering must publish explicit scan-filter ownership")
	source_plan = read("extension/jit_sljit/sljit_region_source_plan.cpp")
	for contract in (
		"source-strategy=mixed-source-filter",
		"source_contract_filter_pushdown=dynamic-only",
		"ExecutionRegionScanFilterMode::DYNAMIC_ONLY",
	):
		if contract not in source_plan:
			raise AssertionError(f"SLJIT mixed source filtering is missing contract: {contract}")
	scan = read("src/execution/operator/scan/physical_table_scan.cpp")
	if "UsesDynamicScanFiltersOnly()" not in scan or "GetFinalTableFilters(op, nullptr)" not in scan:
		raise AssertionError("table scan must derive dynamic-only filters from finalized runtime-filter state")
	executable = read("extension/jit_sljit/include/sljit_region_executable.hpp")
	if "SljitCompiledFunction" not in executable or "SljitLazyCompiledFunction" not in executable:
		raise AssertionError("SLJIT executable regions must own code and callables through typed artifacts")
	if "unique_ptr<ExecutionRegionCodeHandle>" in executable:
		raise AssertionError("SLJIT executable structs must not expose raw code-handle ownership")
	if "SljitLazyCodegenPublication" in executable:
		raise AssertionError("lazy publication must be owned by the typed compiled artifact")
	if "SljitHashJoinProbeSpecializationKey::SPECIALIZATION_COUNT" not in executable:
		raise AssertionError("hash-join compiled variants must use one keyed specialization cache")
	runtime = read("extension/jit_sljit/sljit_region_runtime.cpp")
	ensure = runtime[runtime.index("EnsureLazyHashJoinProbeCode") : runtime.index("EnsurePerfectHashJoinProbeCode")]
	if "unique_ptr<ExecutionRegionCodeHandle> &" in ensure or "SljitLazyCodegenPublication" in ensure:
		raise AssertionError("lazy code generation must publish one typed artifact, not independent fields")
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
	verify_aggregate_payload_descriptor_owns_typed_abi()
	verify_grouped_reduction_lane_binding()
	verify_compiled_artifact_ownership()
	verify_scan_filter_ownership()
	verify_runtime_proofs_are_typed()
	verify_production_contract_ownership()
	print("Execution-region architecture verification passed")


if __name__ == "__main__":
    main()
