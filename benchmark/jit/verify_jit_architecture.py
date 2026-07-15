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


def reject_regex(
    name: str,
    patterns: tuple[str, ...],
    globs: tuple[str, ...],
    allowed: tuple[str, ...] = (),
) -> None:
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
        (
            r"\baggregate\.child_types\[0\]",
            r"\baggregate\.primitive_update_input_type\b",
        ),
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
    if (
        "AggregatePayloadLanes(idx_t op_idx, const vector<SljitAggregatePayloadDescriptor> &payload_descriptors"
        not in runtime_state
    ):
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
        "Run Time (s): real %.6f",
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
    region_runner = read("src/execution/execution_region_runner.cpp")
    if "kernel.AddTraceCodeSize(metrics.code_size);" not in region_runner:
        raise AssertionError("core execution runtime must account for every backend's lazily published machine code")
    reject_regex(
        "backend-owned lazy code-size accounting",
        (r"\bAddTraceCodeSize\(",),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
    )
    capabilities = read("extension/jit_sljit/include/sljit_codegen_capabilities.hpp")
    if "sljitLir.h" in capabilities or "sljit_function_types.hpp" in capabilities:
        raise AssertionError("backend capability tests must not import SLJIT's private machine-code ABI")
    executable = read("extension/jit_sljit/include/sljit_region_executable.hpp")
    for artifact in (
        "SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> compiled",
        "SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> nullable_compiled",
        "SljitLazyCompiledFunction<SljitNativePrimitiveRunFunction> multi_lane_compiled",
    ):
        if artifact not in executable:
            raise AssertionError(f"primitive grouped runs must own deferred typed artifacts: {artifact}")
    aggregate_codegen = read("extension/jit_sljit/sljit_executable_aggregate_codegen.cpp")
    plan = aggregate_codegen[aggregate_codegen.index("void SljitPlanExecutablePrimitiveRunUpdate") :]
    ensure_marker = "SljitEnsureExecutablePrimitiveRunUpdate("
    plan = plan[: plan.index(ensure_marker)]
    if "BuildSljitNativePrimitiveRun" in plan:
        raise AssertionError("primitive grouped-run planning must not eagerly generate machine code")
    ensure = aggregate_codegen[aggregate_codegen.index(ensure_marker) :]
    ensure = ensure[: ensure.index("void SljitSelectExecutableAggregateDirectUpdatePlan")]
    for contract in ("artifact.Ensure", "runtime.RecordLazyCodegen(metrics)"):
        if contract not in ensure:
            raise AssertionError(f"runtime-selected primitive grouped runs are missing lazy publication: {contract}")
    preaggregation = read("extension/jit_sljit/include/sljit_grouped_aggregate_pending_preaggregation_runtime.hpp")
    economics = preaggregation.index("SljitTryInputVectorHasProfitablePrimitiveRuns<TARGET_TYPE>")
    generated_execution = preaggregation.index("TryExecuteGeneratedPrimitiveRunsIntoPending<TARGET_TYPE>", economics)
    if economics >= generated_execution:
        raise AssertionError("primitive grouped-run economics must run before lazy code generation")

    backend_header = read("src/include/duckdb/execution/execution_region_backend.hpp")
    for declaration in (
        "class DUCKDB_API ExecutionRegionBackendPlan",
        "struct DUCKDB_API ExecutionRegionCompilationInput",
        "struct DUCKDB_API ExecutionRegionCompileResult",
        "class DUCKDB_API ExecutionRegionBackend",
    ):
        if declaration not in backend_header:
            raise AssertionError(f"loadable execution-region ABI is missing export: {declaration}")
    if (
        "EXECUTION_REGION_BACKEND_ABI_VERSION" not in backend_header
        or "uint64_t backend_abi_version" not in backend_header
    ):
        raise AssertionError("loadable execution-region backends must register with an explicit C++ ABI version")
    manager_header = read("src/include/duckdb/execution/execution_region_manager.hpp")
    if (
        "RegisterBackend(unique_ptr<ExecutionRegionBackend> backend, uint64_t backend_abi_version)"
        not in manager_header
    ):
        raise AssertionError("execution-region manager registration must not bypass backend ABI validation")
    manager = read("src/execution/execution_region_manager.cpp")
    if "backend_abi_version != EXECUTION_REGION_BACKEND_ABI_VERSION" not in manager:
        raise AssertionError("execution-region manager must reject incompatible backend ABI versions before use")
    for backend_registration in (
        "extension/jit_sljit/sljit_backend.cpp",
        "extension/jit_metal/metal_backend.mm",
    ):
        if "EXECUTION_REGION_BACKEND_ABI_VERSION" not in read(backend_registration):
            raise AssertionError(f"{backend_registration}: backend registration must declare its ABI version")

    adapter = read("src/parallel/execution_region_pipeline_adapter.cpp")
    take_blocked = adapter[adapter.index("void ExecutionRegionPipelineAdapter::TakeBlockedSinkChunk") :]
    take_blocked = take_blocked[: take_blocked.index("void ExecutionRegionPipelineAdapter::FinishProcessing")]
    if "chunk.Copy(executor.final_chunk);" not in take_blocked or "chunk.Reset();" not in take_blocked:
        raise AssertionError("blocked native sink input must transfer to one core-owned continuation")
    region_runner = read("src/execution/execution_region_runner.cpp")
    if "pipeline.TakeBlockedSinkChunk(chunk);" not in region_runner or "RecordBlockedSinkChunk" in region_runner:
        raise AssertionError("compiled sink blocking must leave retry ownership exclusively with the core executor")
    pipeline_executor = read("src/parallel/pipeline_executor.cpp")
    retry = pipeline_executor[pipeline_executor.index("} else if (remaining_sink_chunk) {") :]
    retry = retry[: retry.index("} else if (!in_process_operators.empty()")]
    if "return PipelineExecuteResult::NOT_FINISHED;" not in retry:
        raise AssertionError("a core-owned blocked sink retry must yield before crossing source execution modes")
    radix_aggregate = read("src/execution/radix_partitioned_hashtable.cpp")
    for finalize_contract in (
        "RadixHTFinalizeStrategy::DISJOINT_PROVEN_RANGES",
        "source_contract.hash_aggregate_state_scan.finalize.disjoint_proven_ranges",
        "finalize_strategy_recorded.exchange(true)",
    ):
        if finalize_contract not in radix_aggregate:
            raise AssertionError(
                f"parallel aggregate finalize shortcuts need an observable receipt: {finalize_contract}"
            )
    aggregate_test = read("test/api/test_jit_aggregate.cpp")
    if "REQUIRE(disjoint_finalize_receipt);" not in aggregate_test:
        raise AssertionError("parallel proven-unique aggregate finalization must be covered by a runtime receipt test")
    affine_scratch = read("extension/jit_sljit/include/sljit_preaggregated_primitive_scratch.hpp")
    affine_runtime = read("extension/jit_sljit/include/sljit_aggregate_preaggregated_update_runtime.hpp")
    affine_runs = read("extension/jit_sljit/include/sljit_grouped_aggregate_pending_preaggregation_runtime.hpp")
    affine_codegen = read("extension/jit_sljit/sljit_aggregate_run_codegen.cpp")
    if "shared_value_is_wide" not in affine_scratch or "shared_int64_values" not in affine_scratch:
        raise AssertionError("shared affine values must promote from canonical machine words only on actual overflow")
    if "shared_valid_counts_are_row_counts" not in affine_scratch or "valid_count_row_alias" not in affine_runs:
        raise AssertionError("all-valid shared affine runs must reuse their represented row-count fact")
    if (
        "shared_affine_canonical_int64_states" not in affine_runtime
        or "lanes_form_arithmetic_progression" not in affine_runtime
    ):
        raise AssertionError("shared affine state expansion must consume bound layout and lane progression facts")
    if "if (payload_nullable)" not in affine_codegen or "output_shared_valid_counts" not in affine_codegen:
        raise AssertionError("nullable affine kernels must retain an independent generated valid-count output")
    if "shared_hugeint_values[output_idx]" in affine_runs:
        raise AssertionError("generated affine runs must not widen every compact group during publication")

    regression_gate = read("benchmark/tpch/jit/run_tpch_regression_gate.py")
    if not re.search(
        r'summary_counter\(\s*row,\s*"runner_cost_selected_accelerated_runner_count"\s*\)',
        regression_gate,
    ):
        raise AssertionError("runtime proof selection must include CBO-admitted accelerated runners")
    if "apply_baseline_state_contract(args, state)" not in regression_gate or not re.search(
        r'"--scale-factor",\s*type=float,\s*default=None', regression_gate
    ):
        raise AssertionError("TPC-H candidate configuration must inherit and validate the accepted baseline contract")
    comparator = read("benchmark/tpch/jit/compare_tpch_benchmark.py")
    runtime_gate = comparator[
        comparator.index("def auto_runtime_preserved") : comparator.index("def has_auto_decision")
    ]
    if (
        "off_normalized_candidate_auto_s" in runtime_gate
        or "baseline_auto_upper_s" not in runtime_gate
        or "return raw_slowdown_s <= allowed_s" not in runtime_gate
    ):
        raise AssertionError("baseline acceptance must require the independent raw auto-runtime ceiling")
    if (
        'baseline_auto_runtime_upper_bounds = policy_runtime_upper_bounds(base_runs, "auto")' not in comparator
        or "baseline_auto_runtime_upper_bounds," not in comparator
        or "baseline runs.csv missing positive" not in comparator
    ):
        raise AssertionError("baseline raw-runtime acceptance must retain the qualified run envelope")
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


def verify_benchmark_repetition_budget() -> None:
    generic_benchmark = read("benchmark/jit/generic_benchmark.py")
    if "choices=(5, 10)" not in generic_benchmark:
        raise AssertionError("generic benchmark candidates must use the explicit five-or-ten repetition budget")
    if (
        "def policy_order(repeat: int)" not in generic_benchmark
        or "for policy in policy_order(repeat):" not in generic_benchmark
    ):
        raise AssertionError("generic benchmark pairs must alternate the leading policy")
    if "triage-repeats" in generic_benchmark:
        raise AssertionError("generic benchmark candidates must not silently escalate into triage repetitions")
    if "return failures" not in generic_benchmark:
        raise AssertionError("generic benchmark must fail from the original candidate sample without a retry path")

    tpch_gate = read("benchmark/tpch/jit/run_tpch_regression_gate.py")
    if '"--triage-failures"' not in tpch_gate or "default=False" not in tpch_gate:
        raise AssertionError("TPC-H focused triage must be opt-in")
    refactor_guard = read("benchmark/jit/run_jit_refactor_guard.py")
    if "if args.tpch_triage_failures:" not in refactor_guard:
        raise AssertionError("refactor guard must pass TPC-H triage only when explicitly requested")
    if 'parser.add_argument("--tpch-repeats", type=int, default=10)' not in refactor_guard:
        raise AssertionError("pre-push SF10 comparison must balance both alternating policy orders")
    guard_main = refactor_guard[refactor_guard.index("def main() -> int:") :]
    if guard_main.index("if should_run_tpch(args):") > guard_main.index("if should_run_generic(args):"):
        raise AssertionError("historically compared TPC-H timing must run before the generic production heat load")


def verify_bound_direct_join_terminal_contract() -> None:
    recipe_state = read("extension/jit_sljit/include/sljit_full_pipeline_recipe_state.hpp")
    for required in (
        "struct SljitHashJoinDirectAggregateConsumerContract",
        "idx_t probe_step_idx = DConstants::INVALID_INDEX",
        "idx_t terminal_step_idx = DConstants::INVALID_INDEX",
        "idx_t probe_input_filter_idx = DConstants::INVALID_INDEX",
        "SljitBindHashJoinDirectAggregateConsumerContract",
        "recipe.direct_aggregate_consumer =",
    ):
        if required not in recipe_state:
            raise AssertionError(f"recipe binding is missing the immutable direct-terminal contract: {required}")

    selection_runtime = read("extension/jit_sljit/include/sljit_hash_join_probe_selection_primitive_runtime.hpp")
    for required in (
        "optional_ptr<const SljitHashJoinDirectAggregateConsumerContract> direct_consumer_contract",
        "if (direct_consumer_contract)",
        '"hash_join_probe.direct_aggregate_consumer_candidate"',
    ):
        if required not in selection_runtime:
            raise AssertionError(f"hash-join selection must consume the bound direct-terminal contract: {required}")

    source_runtime = read("extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp")
    for required in (
        "recipe.direct_aggregate_consumer.probe_step_idx == step_idx",
        "direct_consumer_contract, try_execute_direct_consumer",
        "TryExecuteHashJoinProbeConsumer(runtime, ops, scratch, contract",
    ):
        if required not in source_runtime:
            raise AssertionError(f"source execution is missing bound direct-terminal dispatch: {required}")
    for stale in (
        "filter_then_terminal",
        "direct_consumer_nonterminal_recorded",
        "direct_aggregate_consumer_miss.non_terminal_successor",
    ):
        if stale in source_runtime:
            raise AssertionError(f"source execution must not probe runtime successor shape: {stale}")

    terminal_runtime = read("extension/jit_sljit/include/sljit_full_pipeline_terminal_runtime.hpp")
    if "const SljitHashJoinDirectAggregateConsumerContract &contract" not in terminal_runtime:
        raise AssertionError("terminal execution must receive the bound direct-terminal contract")
    if "direct_aggregate_consumer_miss.terminal_kind" in terminal_runtime:
        raise AssertionError("terminal execution must not rediscover terminal kind at runtime")

    recipe_binding_header = read("extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp")
    recipe_binding_cpp = read("extension/jit_sljit/sljit_full_pipeline_recipe_binding.cpp")
    recipe_builder = read("extension/jit_sljit/sljit_full_pipeline_recipe.cpp")
    recipe_families = (
        "SourceFilterAggregate",
        "JoinFilterAggregate",
        "SourceHashJoinBuildSink",
        "HashJoinAppendSink",
        "HashJoinBuildSink",
    )
    for family in recipe_families:
        if f"TryMake{family}Recipe" not in recipe_binding_header:
            raise AssertionError(f"{family} recipe admission and construction must have one shared binder")
        if re.search(rf"\b(?:CanMake|Make){family}Recipe\b", recipe_binding_header + recipe_binding_cpp):
            raise AssertionError(f"{family} recipe shape must not be admitted and reconstructed through duplicate APIs")
        if f"binding.TryMake{family}Recipe(facts, recipe)" not in recipe_builder:
            raise AssertionError(f"recipe builder must consume the shared {family} binder")


def verify_perfect_hash_predicate_cache_ownership() -> None:
    classifier = read("extension/jit_sljit/include/sljit_perfect_hash_predicate_classification.hpp")
    for contract in (
        "class SljitSharedPerfectHashPredicateClassificationCache",
        "buffer_ptr<DictionaryEntry> dictionary",
        "shared_ptr<const SljitPerfectHashPredicateClassificationArtifact> published",
        "struct SljitPerfectHashPredicateClassificationObservation",
        "bool started_dictionary_epoch = false",
        "bool activation_pending = false",
        "idx_t observed_probe_rows = 0",
        "MaxValue<idx_t>(STANDARD_VECTOR_SIZE * 64, dictionary->data.size())",
        "result.classifications.size() != dictionary->data.size()",
        "state->published.atomic_load(std::memory_order_acquire)",
        "state->published.atomic_store(published)",
    ):
        if contract not in classifier:
            raise AssertionError(f"perfect-hash classifier is missing shared immutable ownership: {contract}")
    if re.search(r"state->published(?!\.atomic_load|\.atomic_store)", classifier):
        raise AssertionError("perfect-hash classifier must not read or publish its owner non-atomically")

    state = read("extension/jit_sljit/include/sljit_direct_join_output_aggregate_state.hpp")
    if "SljitPerfectHashPredicateClassificationCache" in state or "local_classifications" in state:
        raise AssertionError("direct aggregate state must not retain the replaced local classifier")

    executable = read("extension/jit_sljit/include/sljit_region_executable.hpp")
    if "SljitSharedPerfectHashPredicateClassificationCache shared_predicate_classification" not in executable:
        raise AssertionError("the executable perfect-hash probe must own the shared immutable classifier")

    local_state = read("extension/jit_sljit/sljit_region_runtime.cpp")
    for contract in (
        "class SljitNativeRegionLocalState : public ExecutionRegionLocalState",
        "SljitFullPipelineTerminalRuntimeState terminal",
        "return make_uniq<SljitNativeRegionLocalState>(allocator, ops)",
    ):
        if contract not in local_state:
            raise AssertionError("local state must retain only mutable terminal execution state")
    if "shared_predicate_classification" in local_state:
        raise AssertionError("shared predicate classification must not be stored in pipeline-local state")

    consumer = read("extension/jit_sljit/include/sljit_hash_join_probe_aggregate_consumer_runtime.hpp")
    if "shared_predicate_classification.Observe(predicate_dictionary_entry, count" not in consumer:
        raise AssertionError("perfect-hash probe must feed its executable classifier after the activation threshold")
    if "SljitPerfectHashPredicateNeedsDictionaryClassification" not in consumer:
        raise AssertionError("perfect-hash classifier admission must model predicate comparison cost")
    for contract in (
        "return field.compressed_size == 0;",
        "a byte classification replaces every remaining",
        "Compressed fields are already narrower than the classifier value.",
    ):
        if contract not in consumer:
            raise AssertionError(f"perfect-hash classifier admission is missing: {contract}")
    if "UsesLocalCache" in consumer or "BindExecutionMode" in consumer or "PARALLEL_RAW" in consumer:
        raise AssertionError("perfect-hash probe must not retain a thread-count cache policy")
    for contract in (
        "SljitSharedPerfectHashDictionaryComplementarySumRHSMatcher",
        "direct_aggregate_consumer.shared_predicate_cache",
        "shared_predicate_classifier_dictionary_epoch",
        "shared_predicate_classifier_observing",
    ):
        if contract not in consumer:
            raise AssertionError(f"perfect-hash probe must consume the published classifier: {contract}")
    if "SljitLocalPerfectHashDictionaryComplementarySumRHSMatcher" in consumer:
        raise AssertionError("perfect-hash probe must not retain its replaced local classifier")

    test = read("test/api/test_jit_join.cpp")
    for contract in (
        "direct_aggregate_consumer.shared_predicate_cache=",
        "!StringUtil::Contains(paths, \"hash_join_probe.perfect_probe.direct_aggregate_consumer.all_valid_rhs=\")",
        "jit_perfect_complementary_compact_orders",
        "REQUIRE(StringUtil::Contains(",
    ):
        if contract not in test:
            raise AssertionError(f"direct perfect-hash tests must prove shared nullable classification: {contract}")

    string_membership = read("extension/jit_sljit/include/sljit_string_set_case_projection_runtime.hpp")
    if "SljitStringEqualsEitherConstant" not in string_membership:
        raise AssertionError("two-constant string membership must share its string layout header load")
    if "return SljitStringEqualsEitherConstant(predicate, classification.constants[0]" not in consumer:
        raise AssertionError("direct perfect-hash predicate classification must use single-header membership")
    for contract in (
        "SljitPerfectHashAllValidCompressedRHSMatcher",
        "SljitPerfectHashAllValidByteRHSMatcher",
        "SljitPerfectHashAllValidUhugeintRHSMatcher",
        "compressed_byte_predicate",
        "compressed_uhugeint_predicate",
        "const STORAGE *data",
        "STORAGE first",
        "STORAGE second",
    ):
        if contract not in consumer:
            raise AssertionError("compressed perfect-hash predicates must bind immutable storage once")

    generic_benchmark = read("benchmark/jit/generic_benchmark.py")
    for contract in (
        '"required_runtime_paths"',
        "compressed_uhugeint_predicate=",
        "inline_string_identity_known_groups=",
        "derived_build_index.contiguous_source=",
        "required runtime path",
    ):
        if contract not in generic_benchmark:
            raise AssertionError("generic packed-string coverage must require its runtime receipt")

    group_loader = read("extension/jit_sljit/include/sljit_selected_input_vector_group_key.hpp")
    if "staged fallible group transforms" not in local_state or "CONVERT::STAGE_TRANSFORMED_KEYS" not in group_loader:
        raise AssertionError("fallible direct group transforms must declare whether checked keys are staged")


def verify_perfect_hash_identity_selected_view() -> None:
    contract = read("extension/jit_sljit/include/sljit_hash_join_probe_execution_contract.hpp")
    for required in (
        "IDENTITY_PREFERRED_SELECTED_VIEW",
        "IDENTITY_PREFERRED_DIRECT_CONSUMER",
        "SljitHashJoinProbePrefersIdentitySelection",
        "SljitHashJoinProbePrefersDirectConsumerOutput",
        "SljitHashJoinProbeProducesSelectedView",
    ):
        if required not in contract:
            raise AssertionError("perfect-hash identity selection must be an explicit selected-view contract")

    generated_probe = read("extension/jit_sljit/sljit_hash_join_probe_perfect_codegen.cpp")
    for required in (
        "SljitPerfectHashJoinProbeCodegenConfig",
        "config.emit_match_selection",
        "config.emit_build_selection",
        "EmitLoadWidePerfectHashJoinInvariantBounds",
        "SljitWidePerfectHashJoinBoundOffset",
    ):
        if required not in generated_probe:
            raise AssertionError("generated perfect-hash probe must make transient selections contract-owned")

    executor = read("extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp")
    for required in (
        "identity_selection_elided",
        "build_selection_elided",
        "identity_selection_retry",
        "native_input.selected_count != input.size()",
        "SljitCanDerivePerfectHashBuildSelectionFromIdentity",
        "SljitExecuteNativeFunction(function, native_input)",
        "hash_join_probe.perfect_probe.exact_source_filter",
    ):
        if required not in executor:
            raise AssertionError("perfect-hash identity selection must retry compact output on a miss")
    if "SljitPopulateExactPerfectHashJoinSelections" in executor:
        raise AssertionError("exact perfect-hash probes must not duplicate the generated membership kernel")

    consumer = read("extension/jit_sljit/include/sljit_hash_join_probe_aggregate_consumer_runtime.hpp")
    for required in (
        "SljitHashJoinProbeOutputContract::IDENTITY_PREFERRED_DIRECT_CONSUMER",
        "SljitWithPerfectHashIdentityBuildIndex",
        "SljitPerfectHashContiguousIdentityBuildIndex",
        "perfect_build_selection_is_key_offset",
        "derived_build_index.contiguous_source",
    ):
        if required not in consumer:
            raise AssertionError("direct perfect-hash terminal must use the proof-backed build-index contract")

    exact_test = read("test/api/test_jit.cpp")
    for required in (
        "JIT Bloom dynamic filters preserve bitpacked scan semantics",
        "hash_join_probe.perfect_probe.exact_source_filter=",
        "const string cast_query",
    ):
        if required not in exact_test:
            raise AssertionError("exact perfect-hash membership must cover sparse, nullable, and casted scans")
    direct_test = read("test/api/test_jit_join.cpp")
    for required in (
        "hash_join_probe.perfect_probe.identity_selection_elided=",
        "hash_join_probe.perfect_probe.build_selection_elided=",
    ):
        if required not in direct_test:
            raise AssertionError("direct perfect-hash terminal must cover the identity build-index receipt")


def verify_perfect_hash_all_valid_complementary_accumulator() -> None:
    accumulator = read("extension/jit_sljit/include/sljit_join_input_complementary_sum_accumulator.hpp")
    for required in (
        "template <class T, bool PREDICATE_ALL_VALID = false>",
        "bool AccumulateAllValid",
        "bool HasOneOrTwoGroups() const",
        "void AddAllValidKnownGroup",
        "sum of the two complementary lanes",
        "accumulator.predicate_all_valid != PREDICATE_ALL_VALID",
    ):
        if required not in accumulator:
            raise AssertionError("all-valid complementary accumulator must own its reduced accounting contract")

    consumer = read("extension/jit_sljit/include/sljit_hash_join_probe_aggregate_consumer_runtime.hpp")
    for required in (
        "bool AllValid() const",
        "bool MatchAllValid(sel_t build_idx)",
        "SljitExecutePerfectHashComplementarySumProbeConsumer<true>",
        "SljitTryExecutePerfectHashInlineStringKnownGroupConsumer",
        "SljitInlineStringStorageSignatureSupported<GROUP_TYPE>::value",
        "direct_aggregate_consumer.all_valid_rhs",
        "direct_aggregate_consumer.inline_string_known_groups",
        "direct_aggregate_consumer.inline_string_identity_known_groups",
        "direct_aggregate_consumer.one_or_two_known_groups",
        "ConsumeOneOrTwoKnownGroups",
    ):
        if required not in consumer:
            raise AssertionError("perfect-hash consumer must dispatch all-valid predicates without nullable accounting")

    state = read("extension/jit_sljit/include/sljit_direct_join_output_aggregate_state.hpp")
    if "bool predicate_all_valid" not in state:
        raise AssertionError("complementary accumulator state must retain its validity contract")

    direct_test = read("test/api/test_jit_join.cpp")
    if "jit_perfect_complementary_nullable_orders" not in direct_test:
        raise AssertionError("direct perfect-hash complementary aggregation must retain nullable RHS coverage")
    if "direct_aggregate_consumer.all_valid_rhs=" not in direct_test:
        raise AssertionError("all-valid complementary aggregation must have a runtime receipt")
    if "direct_aggregate_consumer.one_or_two_known_groups=" not in direct_test:
        raise AssertionError("compact complementary group accumulation must have a runtime receipt")
    if "direct_aggregate_consumer.inline_string_known_groups=" not in direct_test:
        raise AssertionError("inline string known-group accumulation must have a runtime receipt")


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
            raise AssertionError(
                f"compiled artifacts are missing immutable construction contract: {immutable_contract}"
            )
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
    source_ir = read("src/execution/execution_region_ir.cpp")
    for contract in (
        "ExecutionRegionConstantIsSafeIntegralDivisor",
        "ExecutionRegionExpressionIsExceptionFree",
        "expression.binary_op != ExecutionExpressionBinaryOp::MODULO",
        "expression.binary_op != ExecutionExpressionBinaryOp::INTEGER_DIVIDE",
    ):
        if contract not in source_ir:
            raise AssertionError(f"generated source-filter admission is missing exception proof: {contract}")
    if "source filter expression contains unsupported arithmetic" in source_ir:
        raise AssertionError("generated source-filter admission must not reject all arithmetic without exception proof")
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


def verify_partial_predicate_simd_contract() -> None:
    lowering = read("extension/jit_sljit/sljit_region_expression_plan.cpp")
    if (
        "expr.kind == SljitNativeRegionExpressionKind::PREDICATE" not in lowering
        or "AttachSljitNativeExpressionTree(root, expr)" not in lowering
    ):
        raise AssertionError("specialized predicates must retain their original typed IR as an auxiliary plan")

    executable_inputs = read("extension/jit_sljit/sljit_executable_expression_inputs.cpp")
    for contract in (
        "RemapSljitAuxiliaryExpressionTreeToExecutableInputs",
        "semantic.expression_tree_source_indices = expr.input_source_indices",
        "SLJIT auxiliary expression-tree source is absent from native predicate inputs",
    ):
        if contract not in executable_inputs:
            raise AssertionError(f"partial-predicate typed IR must share the dense executable input ABI: {contract}")

    executable_codegen = read("extension/jit_sljit/sljit_executable_expression_codegen.cpp")
    if "semantic.expression_tree.get()" not in executable_codegen:
        raise AssertionError("native predicate selection codegen must receive the remapped typed predicate IR")

    planner = read("extension/jit_sljit/sljit_native_predicate_simd_plan.cpp")
    for contract in (
        "root.conjunction_op != ExecutionExpressionConjunctionOp::AND",
        "TryPlanSljitTypedExpressionTreeSimd(*root.children[prefix_count]).supported",
        "prefix_count == 0 || prefix_count == root.children.size()",
        "SljitTypedExpressionTreeSimdHybridFilterProfitable(simd)",
        "TryBuildNativePredicate(*residual_ir, residual)",
    ):
        if contract not in planner:
            raise AssertionError(f"partial-predicate SIMD planning must preserve ordered AND semantics: {contract}")

    predicate_codegen = read("extension/jit_sljit/sljit_predicate_codegen.cpp")
    if "EmitSljitTypedExpressionTreeSimdHybridFilterLoop" not in predicate_codegen:
        raise AssertionError("partial-predicate execution must use the shared single-pass SIMD hybrid loop")
    for stale_two_pass_contract in (
        "EmitSljitTypedExpressionTreeSimdCompactionLoop",
        "original_execute_sel_offset",
        "scalar_prefix",
    ):
        if stale_two_pass_contract in predicate_codegen:
            raise AssertionError("partial-predicate execution must not mutate selections through a two-pass adapter")

    simd_codegen = read("extension/jit_sljit/sljit_typed_expression_simd_codegen.cpp")
    for contract in (
        "sljit_emit_op1(compiler, SLJIT_CTZ32, SLJIT_R2, 0, SLJIT_R3, 0)",
        "sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_IMM, 1)",
        "sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0)",
    ):
        if contract not in simd_codegen:
            raise AssertionError(
                f"mixed SIMD masks must dispatch only set lanes through sparse bit iteration: {contract}"
            )

    hybrid_loop = simd_codegen.split("void EmitSljitTypedExpressionTreeSimdHybridFilterLoop", 1)[-1]
    for callback_register in ("SLJIT_S3", "SLJIT_S4", "SLJIT_S6"):
        if callback_register in hybrid_loop:
            raise AssertionError(
                f"shared hybrid SIMD loop must preserve callback-owned saved register: {callback_register}"
            )


def verify_string_batch_selection_contract() -> None:
    runtime = read("extension/jit_sljit/sljit_predicate_string_runtime.cpp")
    for contract in (
        "__attribute__((noinline, cold)) idx_t SljitVerifyLikeFragmentPairCandidates",
        "SljitVerifyLikeFragmentPairCandidates(sdata, fragment, fragment_length, pair_anchor, position)",
        "bool selection_materialized = false;",
        "if (!selection_materialized)",
        "return selection_materialized ? selected_count : count;",
    ):
        if contract not in runtime:
            raise AssertionError(f"two-fragment LIKE batch selection is missing hot-path contract: {contract}")

    batch_loop = runtime.split("static idx_t SljitSelectStringLikeBatchLoop", 1)[-1].split(
        "static idx_t SljitSelectStringLikeBatchNegation", 1
    )[0]
    for stale_two_pass_contract in ("rejected_count", "copy_range", "rejected_idx"):
        if stale_two_pass_contract in batch_loop:
            raise AssertionError(
                f"negated two-fragment LIKE selection must not retain duplicate-pass state: {stale_two_pass_contract}"
            )

    scalar_test = read("test/api/test_jit_scalar.cpp")
    for required in (
        "s NOT LIKE '%ab%tail%' ORDER BY id",
        "s NOT LIKE '%never%present%'",
    ):
        if required not in scalar_test:
            raise AssertionError(f"two-fragment LIKE batch selection is missing correctness coverage: {required}")


def verify_hash_join_null_fact_ownership() -> None:
    hash_table = read("src/include/duckdb/execution/join_hashtable.hpp")
    for contract in (
        "bool has_filtered_null;",
        "bool has_stored_null;",
    ):
        if contract not in hash_table:
            raise AssertionError(f"hash-table NULL state must retain independent runtime facts: {contract}")
    if re.search(r"\bbool\s+has_null\s*;", hash_table):
        raise AssertionError("hash-table NULL state must not conflate filtered and physically stored NULLs")

    layout = read("src/include/duckdb/execution/execution_hash_join_runtime.hpp")
    if "bool stored_keys_have_null = false;" not in layout:
        raise AssertionError("backend hash-table layout must export actual retained-key NULL state")
    if "stored_keys_can_have_null" in layout:
        raise AssertionError("backend hash-table layout must not substitute nullable shape for observed NULL state")

    probe_runtime = read("extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp")
    if "layout.stored_keys_have_null" not in probe_runtime:
        raise AssertionError("native hash probes must specialize from actual retained-key NULL state")


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
    verify_partial_predicate_simd_contract()
    verify_string_batch_selection_contract()
    verify_hash_join_null_fact_ownership()
    verify_runtime_proofs_are_typed()
    verify_production_contract_ownership()
    verify_benchmark_repetition_budget()
    verify_bound_direct_join_terminal_contract()
    verify_perfect_hash_predicate_cache_ownership()
    verify_perfect_hash_identity_selected_view()
    verify_perfect_hash_all_valid_complementary_accumulator()
    print("Execution-region architecture verification passed")


if __name__ == "__main__":
    main()
