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
    for stale_virtual_lookup in (
        "entry.backend->Name()",
        "entry.backend->Description()",
        "entry.backend->RunnerKind()",
        "entry.backend->SupportsRegions()",
        "entry.backend->IsAvailable()",
    ):
        if stale_virtual_lookup in manager:
            raise AssertionError("backend registry locks must protect frozen metadata, not backend virtual callbacks")
    for registry_contract in (
        "string normalized_name;",
        "ExecutionRunnerKind runner_kind;",
        "bool supports_regions;",
    ):
        if registry_contract not in manager_header:
            raise AssertionError(f"backend registration must freeze static metadata: {registry_contract}")
    manager_test = read("test/api/test_jit.cpp")
    for registry_test in (
        "Execution region manager freezes backend metadata at registration",
        "Execution region backend availability does not hold the registry lock",
    ):
        if registry_test not in manager_test:
            raise AssertionError(f"backend registry lock ownership requires direct coverage: {registry_test}")

    sljit_cmake = read("third_party/sljit/CMakeLists.txt")
    for allocator_contract in (
        "if(APPLE)",
        'CMAKE_SYSTEM_NAME STREQUAL "Linux"',
        "SLJIT_PROT_EXECUTABLE_ALLOCATOR=1",
        "elseif(WIN32 OR UNIX)",
        "SLJIT_WX_EXECUTABLE_ALLOCATOR=1",
        "no W^X executable-memory policy",
    ):
        if allocator_contract not in sljit_cmake:
            raise AssertionError(f"SLJIT executable-memory platform policy is incomplete: {allocator_contract}")
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
    if "return PipelineExecuteResult::RUNNER_HANDOFF;" not in retry:
        raise AssertionError("a core-owned blocked sink retry must request selected-runner re-entry")
    unbounded_execute = pipeline_executor[
        pipeline_executor.index("PipelineExecuteResult PipelineExecutor::Execute()") :
    ]
    unbounded_execute = unbounded_execute[: unbounded_execute.index("void PipelineExecutor::FinishProcessing")]
    if (
        "while (true)" not in unbounded_execute
        or "Execute(NumericLimits<idx_t>::Maximum())" not in unbounded_execute
        or "result != PipelineExecuteResult::RUNNER_HANDOFF" not in unbounded_execute
    ):
        raise AssertionError("unbounded pipeline execution must consume only explicit runner handoffs")
    pipeline_header = read("src/include/duckdb/parallel/pipeline.hpp")
    pipeline = read("src/parallel/pipeline.cpp")
    if (
        "bool execution_region_plan_built;" not in pipeline_header
        or "void EnsureExecutionRegionPlanBuiltLocked();" not in pipeline_header
        or "execution_region_plan_built = true;" not in pipeline
        or "EnsureExecutionRegionPlanBuiltLocked();" not in pipeline
    ):
        raise AssertionError("recursive rescheduling must reuse one pipeline-scoped execution-region plan")
    if "void BuildExecutionRegionPlan();" in pipeline_header:
        raise AssertionError("pipeline-scoped execution-region plans must not expose a public rebuild operation")
    runtime_test = read("test/api/test_jit_runtime.cpp")
    blocked_sink_test = runtime_test[runtime_test.index("JIT blocked append sink transfers retry ownership") :]
    blocked_sink_test = blocked_sink_test[
        : blocked_sink_test.index("JIT full pipeline uses append sink contract for CTE")
    ]
    if "ConfigureSljitForCoverageSettings(con, true, true, true, 10000, 4);" not in blocked_sink_test:
        raise AssertionError("blocked compiled sink continuation must retain parallel streaming coverage")
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
    executable_builder = read("extension/jit_sljit/sljit_region_executable.cpp")
    input_groups = read("extension/jit_sljit/include/sljit_grouped_aggregate_input_vector_groups.hpp")
    grouped_runtime = read("extension/jit_sljit/include/sljit_grouped_direct_aggregate_update_runtime_state.hpp")
    if (
        "SljitExecutableIntegralGroupKeyRange" not in executable
        or "integral_group_key_ranges" not in executable
        or "SljitBuildExecutableIntegralGroupKeyRanges" not in executable_builder
        or "SljitApplyExecutableIntegralGroupKeyRangeProofs" not in input_groups
    ):
        raise AssertionError("signed group-key range facts must be bound once and consumed by grouped runtimes")
    cast_fit = input_groups[
        input_groups.index("static bool SljitInputVectorGroupBatchFitsCast") : input_groups.index(
            "struct SljitInputVectorGroupBatchCastFitDispatch"
        )
    ]
    if (
        "!source_format.sel->IsSet()" not in cast_fit
        or "source_format.validity.CheckAllValid(count)" not in cast_fit
        or not re.search(r"fits\s*&=\s*static_cast<uint8_t>", cast_fit)
    ):
        raise AssertionError("flat all-valid group cast checks must use the SIMD-friendly direct-data reduction")
    if "ProjectedDenseDomainProvesGroupCast" in grouped_runtime:
        raise AssertionError(
            "signed group-key cast proofs must not depend on the non-negative dense-domain special case"
        )
    if "ExecuteSljitSingleLaneCanonicalSumInitialization" not in affine_runtime:
        raise AssertionError("canonical single-lane SUM state initialization must bind layout outside the group loop")
    aggregate_hashtable = read("src/execution/aggregate_hashtable.cpp")
    producer_summary = aggregate_hashtable[
        aggregate_hashtable.index(
            "static bool AggregateTryContinueProducerProvenUniqueSummaryTyped"
        ) : aggregate_hashtable.index("static bool AggregateTryContinueProvenUniqueAppendTyped")
    ]
    if "append_proof.groups_strictly_increasing" not in aggregate_hashtable:
        raise AssertionError("grouped append summaries must require an explicit producer ordering proof")
    for contract in (
        "AggregateRecordProvenUniqueRange",
        "one conservative",
        "intervals separate until the bounded summary fills",
    ):
        if contract not in producer_summary:
            raise AssertionError(f"producer-proven grouped summary contract is incomplete: {contract}")
    if "ToUnifiedFormat" in producer_summary or "for (" in producer_summary:
        raise AssertionError("producer-proven grouped summaries must not rescan generated keys")
    generated_run_publish = affine_runs[
        affine_runs.index("static bool SljitExecuteBoundGeneratedPrimitiveRunsIntoPending") : affine_runs.index(
            "static bool TryExecuteGeneratedPrimitiveRunsIntoPending"
        )
    ]
    for contract in (
        "SljitPendingPreaggregatedPrimitiveContinuesTailStep",
        "pending_preaggregated_group_progression_boundary_flush",
    ):
        if contract not in generated_run_publish:
            raise AssertionError(f"generated grouped summaries must preserve scheduler gaps: {contract}")
    if generated_run_publish.index("SljitPendingPreaggregatedPrimitiveContinuesTailStep") > generated_run_publish.index(
        "const bool output_bound"
    ):
        raise AssertionError("generated grouped progression boundaries must flush before output binding")
    if "sparse_disjoint_group_id" not in aggregate_test:
        raise AssertionError("parallel sparse grouped finalization must retain a disjoint-range runtime receipt")
    if "aggregate_update.proven_unique_append.producer_order_proof=8192" not in aggregate_test:
        raise AssertionError("sparse grouped correctness must prove producer-owned monotonic publication")
    if (
        "const auto address_sel = single_partition_append ? nullptr : row_sel.data();" not in aggregate_hashtable
        or "if (!address_sel && !execute_sel)" not in affine_runtime
    ):
        raise AssertionError("input-order grouped addresses must use the identity callback contract")
    if "if (skip_lookups) {\n\t\treturn false;\n\t}" not in aggregate_hashtable:
        raise AssertionError("append-only grouped aggregates must not reserve an unused pointer table")
    if "!ht.LookupsSkipped() && ht.Count() + STANDARD_VECTOR_SIZE >= resize_threshold" not in radix_aggregate:
        raise AssertionError("append-only grouped aggregates must not abandon on unused pointer-table capacity")
    for deferred_handoff_contract in (
        "TryAcquireProvenUniqueAppendData",
        "deferred_proven_unique_data",
        "data->Repartition(context, *gstate.uncombined_data, state_remap);",
        "publish_partition(data->GetUnpartitioned());",
    ):
        if deferred_handoff_contract not in aggregate_hashtable + radix_aggregate:
            raise AssertionError(
                "proven-unique aggregate handoff must defer radix work until global proof failure: "
                + deferred_handoff_contract
            )
    deferred_acquire = aggregate_hashtable[
        aggregate_hashtable.index(
            "GroupedAggregateHashTable::TryAcquireProvenUniqueAppendData"
        ) : aggregate_hashtable.index("void GroupedAggregateHashTable::Abandon")
    ]
    if "proven_unique_append_ranges_coalesced" in deferred_acquire:
        raise AssertionError("coalesced local uniqueness proofs must reach the global deferred-handoff decision")
    if "enable_hll = false;" not in aggregate_hashtable:
        raise AssertionError("lookup-free aggregate ownership must disable obsolete HLL adaptation work")

    cost_input = read("src/execution/execution_region_cost_input.cpp")
    scan_cost = cost_input[cost_input.index("static bool TryAccumulateExecutionRegionPhysicalScanCost") :]
    scan_cost = scan_cost[: scan_cost.index("static ExecutionRegionSourceKind")]
    if "scan.GetExecutionContract()" in scan_cost:
        raise AssertionError("scan cost extraction must not construct the final execution contract")
    if "GetExecutionRegionTableScanSourceInputType(scan, filter_idx)" not in scan_cost:
        raise AssertionError("scan cost extraction must read only the source input type it prices")
    graph = read("src/execution/execution_region_graph.cpp")
    distinct_count = graph[graph.index("idx_t GetExecutionRegionTableScanDistinctCount") :]
    distinct_count = distinct_count[: distinct_count.index("static optional_ptr<const Expression>")]
    if "scan.GetExecutionContract()" in distinct_count:
        raise AssertionError("scan distinct-count extraction must not construct the final execution contract")
    if "GetExecutionRegionTableScanSourceCardinality(scan)" not in distinct_count:
        raise AssertionError("scan distinct-count extraction must consume the narrow cardinality fact")

    physical_operator = read("src/include/duckdb/execution/physical_operator.hpp")
    if "GetExecutionContract(ExecutionRegionOperatorSlot slot, bool render_diagnostics)" not in physical_operator:
        raise AssertionError("physical operators must expose slot-directed execution contracts")
    if "GetExecutionContract()" in physical_operator:
        raise AssertionError("the obsolete all-slot physical execution contract API must stay deleted")
    if "op.GetExecutionContract(slot, render_diagnostics)" not in graph:
        raise AssertionError("region graph construction must request only the operator slot it consumes")
    execution_contract = read("src/execution/execution_contract.cpp")
    hash_join_contract = execution_contract[execution_contract.index("ExecutionContract PhysicalHashJoin::") :]
    hash_join_contract = hash_join_contract[: hash_join_contract.index("ExecutionContract PhysicalNestedLoopJoin::")]
    for owned_slot in (
        "case ExecutionRegionOperatorSlot::SOURCE:",
        "case ExecutionRegionOperatorSlot::OPERATOR:",
        "case ExecutionRegionOperatorSlot::SINK:",
    ):
        if owned_slot not in hash_join_contract:
            raise AssertionError(f"hash joins must construct contracts by requested ownership slot: {owned_slot}")
    if "GetExecutionContract(ExecutionRegionOperatorSlot::SINK, false)" not in cost_input:
        raise AssertionError("physical CBO must request sink-only contracts for aggregate state updates")
    pending_groups = read("extension/jit_sljit/include/sljit_pending_preaggregated_group_batch_runtime.hpp")
    proof_update = pending_groups.index("SljitUpdateProvenUniqueAppendContract(runtime, op, pending")
    proof_execution = pending_groups.index(
        "TryExecutePreaggregatedGroupedPrimitiveAggregateUpdateBatches(", proof_update
    )
    proof_window = pending_groups[proof_update:proof_execution]
    if not re.search(
        r"if \(!pending\.proven_unique_append_active\) \{\s*" r"SljitTryReserveGroupedAggregateGroups\(",
        proof_window,
    ):
        raise AssertionError("proven-unique pending groups must not request lookup-table reservation")
    proof_call = pending_groups[proof_execution : proof_execution + 500]
    if not re.search(r"pending\.represented_row_count,\s*false,\s*true\)", proof_call):
        raise AssertionError("proven-unique pending execution must not re-request reservation in its batch helper")
    if (
        "JIT executable group ranges prove signed narrowing casts once" not in aggregate_test
        or "JIT canonical single-lane sums initialize fresh states directly" not in aggregate_test
        or "Grouped aggregate append callbacks expose identity address order directly" not in aggregate_test
        or "REQUIRE(grouped_aggregate_reserve_target <= STANDARD_VECTOR_SIZE);" not in aggregate_test
        or "REQUIRE_FALSE(grouped_aggregate_reserve_resized);" not in aggregate_test
    ):
        raise AssertionError("group range and input-order fresh-state fast paths require direct correctness coverage")

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
        comparator.index("def auto_runtime_preserved") : comparator.index("def has_auto_accelerated_runner")
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
    if "has_auto_decision(base)" in comparator or "has_auto_decision(candidate)" in comparator:
        raise AssertionError("raw auto-runtime acceptance must not depend on decision telemetry")
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
    benchmark_common = read("benchmark/jit/benchmark_common.py")
    if "choices=(5, 10)" not in generic_benchmark:
        raise AssertionError("generic benchmark candidates must use the explicit five-or-ten repetition budget")
    if (
        "def policy_order(repeat: int)" not in generic_benchmark
        or "for policy in policy_order(repeat):" not in generic_benchmark
    ):
        raise AssertionError("generic benchmark pairs must alternate the leading policy")
    for paired_contract in (
        "def median_paired_speedup(",
        '"paired_speedup_median"',
        'speedup = float(auto["paired_speedup_median"])',
    ):
        if paired_contract not in generic_benchmark:
            raise AssertionError(f"generic speedup gates must consume alternating pairs: {paired_contract}")
    if '"speedup_vs_off_median"' in generic_benchmark:
        raise AssertionError("generic benchmark must not label a ratio-of-medians as paired speedup")
    if "triage-repeats" in generic_benchmark:
        raise AssertionError("generic benchmark candidates must not silently escalate into triage repetitions")
    if "return failures" not in generic_benchmark:
        raise AssertionError("generic benchmark must fail from the original candidate sample without a retry path")
    if "class BenchmarkScript:" not in benchmark_common or 'args.duckdb, Path(":memory:")' not in benchmark_common:
        raise AssertionError("benchmark timing must use one ordered shell script")
    generic_matrix = generic_benchmark[
        generic_benchmark.index("def run_workload_matrix(") : generic_benchmark.index("def minimum_auto_speedup(")
    ]
    if (
        generic_matrix.count("BenchmarkScript(db_path)") != 1
        or generic_matrix.count("script.execute(") != 1
        or "run_duckdb(" in generic_matrix
    ):
        raise AssertionError("generic samples must use exactly one DuckDB shell process")

    tpch_benchmark = read("benchmark/tpch/jit/tpch_benchmark.py")
    production_matrix = tpch_benchmark[
        tpch_benchmark.index("def run_production_matrix(") : tpch_benchmark.index("def run_profile_matrix(")
    ]
    if (
        production_matrix.count("BenchmarkScript(db_path)") != 1
        or production_matrix.count("script.execute(") != 1
        or "run_duckdb(" in production_matrix
    ):
        raise AssertionError("TPC-H production samples must use exactly one DuckDB shell process")
    counter_tail = production_matrix[
        production_matrix.index("for job in counter_jobs:") : production_matrix.index("query_times = script.execute(")
    ]
    if "script.measure(" in counter_tail or "script.run_untimed(" not in counter_tail:
        raise AssertionError("TPC-H traced proof collection must remain untimed after the production matrix")

    tpch_gate = read("benchmark/tpch/jit/run_tpch_regression_gate.py")
    for stale_retry_contract in (
        "triage_failed_comparison",
        "focused_recheck",
        "merge_rechecked_csv_artifact",
        "merge_promoted_baseline_artifact",
        "comparison_passed",
        '"--triage-failures"',
    ):
        if stale_retry_contract in tpch_gate:
            raise AssertionError(f"TPC-H failed candidates must not have a retry verdict path: {stale_retry_contract}")
    gate_flow = tpch_gate[tpch_gate.index("def run_benchmark_gate(") : tpch_gate.index("def run_gate(")]
    comparison_offset = gate_flow.index('"baseline comparison"')
    promotion_offset = gate_flow.index("if args.promote_baseline:")
    publication_offset = gate_flow.index("write_baseline_state(", promotion_offset)
    if not comparison_offset < promotion_offset < publication_offset:
        raise AssertionError("TPC-H baseline publication must follow a passing candidate comparison")
    for database_reuse_contract in (
        "def gate_database(",
        "with exclusive_database_cache_lock(lock_path):",
        "ensure_cached_database(args, template_path, manifest_path)",
        "clone_cached_database(template_path, working_database)",
        "create_tpch_database(args, args.db)",
        "validate_tpch_database(args, args.db)",
        "def run_timed_benchmark(",
        "args.use_existing_db = True",
        'add_bool_flag(command, args.use_existing_db, "--use-existing-db")',
        "with gate_database(args):",
    ):
        if database_reuse_contract not in tpch_gate:
            raise AssertionError(
                f"TPC-H gate must yield one ready reusable working database: {database_reuse_contract}"
            )
    if "reuse_database and args.keep_db" in tpch_gate:
        raise AssertionError("TPC-H database reuse must not depend on retaining the database after the gate")
    if "reuse_database" in tpch_gate or 'add_bool_flag(command, args.keep_db, "--keep-db")' in tpch_gate:
        raise AssertionError("TPC-H gate must own database reuse and retention without child lifecycle overrides")
    refactor_guard = read("benchmark/jit/run_jit_refactor_guard.py")
    benchmark_host = read("benchmark/jit/benchmark_host.py")
    for host_quiescence_contract in (
        "def require_host_quiescence(",
        "def wait_for_host_quiescence(",
        "HOST_QUIESCENCE_MAX_CPU_FRACTION = 0.10",
        "MACOS_SECURITY_MAX_CPU_PERCENT = 5.0",
        'process_name == "syspolicyd" or process_name.startswith("xprotect")',
    ):
        if host_quiescence_contract not in benchmark_host:
            raise AssertionError(f"shared performance host admission is incomplete: {host_quiescence_contract}")
    if "if args.host_quiescence and (should_run_tpch(args) or should_run_generic(args)):" not in refactor_guard:
        raise AssertionError("combined performance guard must reject a busy host before setup")
    tpch_gate_main = tpch_gate[tpch_gate.index("def run_gate(") :]
    database_offset = tpch_gate_main.index("with gate_database(args):")
    quiescence_offset = tpch_gate_main.index("wait_for_host_quiescence()")
    benchmark_offset = tpch_gate_main.index("return run_benchmark_gate(")
    if not database_offset < quiescence_offset < benchmark_offset:
        raise AssertionError("TPC-H host admission must run after database setup and before measurement")
    if 'command.append("--no-host-quiescence")' not in refactor_guard:
        raise AssertionError("combined guard must propagate explicit host-admission disablement")
    if "result = run_command(" not in refactor_guard or "require_host_quiescence()" not in refactor_guard:
        raise AssertionError("generic timing must retain an immediate post-measurement host check")
    for receipt_contract in (
        "def validate_performance_receipt_configuration(",
        "skipped pre-commit checks require an exact-tree pre-commit receipt",
        "def write_performance_receipt(",
    ):
        if receipt_contract not in refactor_guard:
            raise AssertionError(f"performance receipt ownership is incomplete: {receipt_contract}")
    for stale_guard_contract in (
        "tpch_triage",
        "unit_baseline",
        "unit ratchet",
        "failed_tests",
        "unit_execution",
        "def baseline_from_state(",
        "def tpch_baseline_configured(",
    ):
        if stale_guard_contract in refactor_guard:
            raise AssertionError(
                f"refactor guard must not preserve failed correctness or timing paths: {stale_guard_contract}"
            )
    if "run_unit_suite(args, artifact_dir)" not in refactor_guard:
        raise AssertionError("refactor guard must run the JIT unit suite directly")
    for candidate_budget in (
        'parser.add_argument("--generic-repeats", type=int, choices=(5, 10), default=5)',
        'parser.add_argument("--tpch-repeats", type=int, choices=(5, 10), default=5)',
    ):
        if candidate_budget not in refactor_guard:
            raise AssertionError(f"pre-push candidates must default to five pairs and allow ten: {candidate_budget}")
    guard_main = refactor_guard[refactor_guard.index("def main() -> int:") :]
    quiescence_offset = guard_main.index("wait_for_host_quiescence()")
    tpch_offset = guard_main.index("if should_run_tpch(args):")
    generic_offset = guard_main.index("if should_run_generic(args):")
    if quiescence_offset > tpch_offset:
        raise AssertionError("host quiescence admission must run before production performance measurement")
    if tpch_offset > generic_offset:
        raise AssertionError("historically compared TPC-H timing must run before the generic production heat load")
    for hook_path in (
        "benchmark/jit/git_hooks/pre-commit",
        "benchmark/jit/git_hooks/pre-push",
    ):
        hook = read(hook_path)
        if "benchmark/jit/local_baselines/pre_commit_verified_tree" not in hook:
            raise AssertionError(
                f"Git-hook verification state must use the ignored local baseline directory: {hook_path}"
            )
        if "benchmark/jit/tmp/pre_commit_verified_tree" in hook:
            raise AssertionError(
                f"Git-hook verification state must not drift back to disposable candidates: {hook_path}"
            )
    pre_push_hook = read("benchmark/jit/git_hooks/pre-push")
    for stale_hook_contract in ("DUCKDB_JIT_TPCH_TRIAGE_FAILURES", "--tpch-triage-failures"):
        if stale_hook_contract in pre_push_hook:
            raise AssertionError(f"pre-push must not expose a retry verdict path: {stale_hook_contract}")
    if "pre_push_verified_tree" not in pre_push_hook or "--performance-receipt" not in pre_push_hook:
        raise AssertionError("pre-push must reuse only exact-tree production performance verification")


def verify_bound_direct_join_terminal_contract() -> None:
    recipe_binding = read("extension/jit_sljit/sljit_full_pipeline_recipe_binding.cpp")
    append_primitive = read("extension/jit_sljit/include/sljit_append_sink_primitive.hpp")
    for stale_sentinel_arithmetic in (
        "facts.sink_idx + 1",
        "facts.sink_idx + 3",
    ):
        if stale_sentinel_arithmetic in recipe_binding:
            raise AssertionError("recipe admission must range-check sentinel indices before arithmetic")
    if "hash_join_idx + 1 != sink_idx" in append_primitive:
        raise AssertionError("append binding must compare validated adjacent indices without sentinel overflow")

    recipe_state = read("extension/jit_sljit/include/sljit_full_pipeline_recipe_state.hpp")
    for required in (
        "struct SljitHashJoinDirectAggregateConsumerContract",
        "idx_t probe_step_idx = DConstants::INVALID_INDEX",
        "idx_t terminal_step_idx = DConstants::INVALID_INDEX",
        "idx_t probe_input_filter_idx = DConstants::INVALID_INDEX",
        "idx_t hash_join_idx = DConstants::INVALID_INDEX",
        "idx_t aggregate_idx = DConstants::INVALID_INDEX",
        "SljitMakeHashJoinDirectAggregateConsumerContract",
        "SljitValidateHashJoinDirectAggregateConsumerContract",
    ):
        if required not in recipe_state:
            raise AssertionError(f"recipe binding is missing the immutable direct-terminal contract: {required}")
    for stale in (
        "SljitBindHashJoinDirectAggregateConsumerContract",
        "for (idx_t step_idx = 0; step_idx < sequence.Count(); step_idx++)",
    ):
        if stale in recipe_state:
            raise AssertionError(f"recipe construction must not rediscover direct-terminal shape: {stale}")
    for contract in (
        "probe_step.hash_join_probe_selection.hash_join_idx != contract.hash_join_idx",
        "terminal_step.post_join_projection_aggregate.aggregate_idx != contract.aggregate_idx",
        "filter_step.generated_filter.filter_idx != contract.probe_input_filter_idx",
        "contract.HasAnyBinding()",
    ):
        if contract not in recipe_state:
            raise AssertionError(f"recipe publication must validate explicit direct-terminal ownership: {contract}")

    for required in (
        "enum class SljitFullPipelineRuntimeKind",
        "enum class SljitFullPipelineRecipePlanKind",
        "bool UsesSelectedHashJoinSinkRuntime() const",
        "bool OwnsFusedFilter(idx_t filter_idx) const",
        "bool HasRecipe() const",
        "SljitFullPipelineRecipePlanKind Kind() const",
        "const SljitFullPipelineRecipe &Recipe() const",
    ):
        if required not in recipe_state:
            raise AssertionError(f"recipe publication is missing immutable runtime ownership: {required}")
    if "bool has_recipe" in recipe_state:
        raise AssertionError("recipe plan kind must not be encoded as an independent boolean")

    primitive_contract = read("extension/jit_sljit/sljit_full_pipeline_primitive_contract.cpp")
    for required in (
        "SljitFinalizeFullPipelinePrimitiveRecipe(",
        "recipe.direct_aggregate_consumer = direct_aggregate_consumer",
        "recipe.fused_filter_owners = std::move(fused_filter_owners)",
    ):
        if required not in primitive_contract:
            raise AssertionError(f"recipe publication must finalize immutable ownership once: {required}")
    for stale in (
        "SljitFullPipelinePrimitiveSequenceIsExecutable",
        "SljitFullPipelineSourceFetchNeedsPartitionPreservingChunks",
        "SljitFullPipelineIsSelectedHashJoinSinkSequence",
        "SljitFullPipelineFilterHasFusedOwner",
        "SljitFullPipelinePrimitiveStepOwnsOps",
    ):
        if stale in primitive_contract:
            raise AssertionError(f"recipe publication must not rescan independent shape contracts: {stale}")

    primitive_sequence = read("extension/jit_sljit/include/sljit_full_pipeline_primitive_sequence.hpp")
    for stale in ("op_indices", "op_count", "idx_t Op(", "SLJIT_FULL_PIPELINE_MAX_PRIMITIVE_STEP_OPS"):
        if stale in primitive_sequence:
            raise AssertionError(f"primitive steps must not duplicate typed operator ownership: {stale}")
    if "idx_t native_tail_start_idx = DConstants::INVALID_INDEX" not in primitive_sequence:
        raise AssertionError("native-tail primitive ownership must be explicit and typed")

    for recipe_binding in (
        "extension/jit_sljit/sljit_full_pipeline_recipe_binding.cpp",
        "extension/jit_sljit/sljit_projection_aggregate_recipe_binding.cpp",
    ):
        binding = read(recipe_binding)
        if "SljitMakeHashJoinDirectAggregateConsumerContract(" not in binding:
            raise AssertionError(
                f"direct-terminal recipe must bind its semantic identities explicitly: {recipe_binding}"
            )

    selection_runtime = read("extension/jit_sljit/include/sljit_hash_join_probe_selection_primitive_runtime.hpp")
    for required in (
        "optional_ptr<const SljitHashJoinDirectAggregateConsumerContract> direct_consumer_contract",
        "SljitHashJoinAggregateConsumerDispatch &direct_consumer_dispatch",
        "direct_consumer_dispatch != SljitHashJoinAggregateConsumerDispatch::MATERIALIZED",
        "direct_consumer_dispatch = SljitHashJoinAggregateConsumerDispatch::DIRECT",
        "direct_consumer_dispatch = SljitHashJoinAggregateConsumerDispatch::HYBRID",
        '"hash_join_probe.direct_aggregate_consumer.materialized_dispatch"',
        '"hash_join_probe.direct_aggregate_consumer.hybrid_dispatch"',
    ):
        if required not in selection_runtime:
            raise AssertionError(f"hash-join selection must consume the bound direct-terminal contract: {required}")
    for stale in (
        '"hash_join_probe.direct_aggregate_consumer_candidate"',
        "direct_aggregate_consumer_miss.",
        "direct_consumer_blocker",
    ):
        if stale in selection_runtime:
            raise AssertionError(f"hash-join selection must bind direct/materialized dispatch once: {stale}")

    materialize_runtime = read("extension/jit_sljit/include/sljit_hash_join_probe_materialize_primitive_runtime.hpp")
    native_tail_runtime = read("extension/jit_sljit/include/sljit_native_tail_delegation_runtime.hpp")
    for runtime_source in (selection_runtime, materialize_runtime, native_tail_runtime):
        if ".Op(" in runtime_source:
            raise AssertionError("primitive runtime must consume typed operator ownership")

    append_primitive = read("extension/jit_sljit/include/sljit_append_sink_primitive.hpp")
    append_runtime = read("extension/jit_sljit/include/sljit_append_sink_runtime.hpp")
    post_join_runtime = read("extension/jit_sljit/include/sljit_post_join_projection_aggregate_runtime.hpp")
    if "SljitCanBindAppendSinkPrimitive" in append_primitive + append_runtime:
        raise AssertionError("published append primitives must not retain a runtime admission API")
    if "SljitTryBindAppendSinkPrimitive" not in append_primitive:
        raise AssertionError("append primitive must expose one non-throwing binder")
    for stale_append_binder in (
        "SljitCanBindSelectedHashJoinAppendSinkPrimitive",
        "SljitTryBindSelectedHashJoinAppendSinkPrimitive",
        "SljitBindSelectedHashJoinAppendSinkPrimitive",
    ):
        if stale_append_binder in append_primitive:
            raise AssertionError(f"append primitive must not preserve duplicate binder APIs: {stale_append_binder}")
    if "SljitCanBindPostJoinProjectionAggregatePrimitive" in post_join_runtime:
        raise AssertionError("post-join aggregate runtime must not re-admit a published primitive")
    for runtime_source in (append_runtime, post_join_runtime):
        if "void Prepare(" not in runtime_source:
            raise AssertionError("published terminal preparation must not expose a recipe miss result")

    dead_throwing_binders = {
        "extension/jit_sljit/include/sljit_hash_join_probe_primitive.hpp": (
            "SljitBindHashJoinProbeMaterializePrimitive(",
            "SljitBindHashJoinBuildSinkPrimitive(",
            "SljitBindMarkProbeFilterBoundaryPrimitive(",
        ),
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp": (
            "SljitBindGroupedAggregateUpdatePrimitive(",
            "SljitBindFilteredGroupedAggregateUpdatePrimitive(",
        ),
        "extension/jit_sljit/include/sljit_post_join_projection_strategy.hpp": (
            "SljitBindPostJoinProjectionPrimitive(",
        ),
    }
    for path, stale_binders in dead_throwing_binders.items():
        binding_source = read(path)
        for stale_binder in stale_binders:
            if stale_binder in binding_source:
                raise AssertionError(f"unused throwing primitive binder must stay deleted: {stale_binder}")

    source_runtime = read("extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp")
    for required in (
        "recipe.direct_aggregate_consumer.probe_step_idx == step_idx",
        "direct_consumer_contract, direct_aggregate_consumer_dispatch",
        "TryExecuteHashJoinProbeConsumer(runtime, ops, scratch, contract",
    ):
        if required not in source_runtime:
            raise AssertionError(f"source execution is missing bound direct-terminal dispatch: {required}")
    for stale in (
        "filter_then_terminal",
        "direct_consumer_nonterminal_recorded",
        "direct_aggregate_consumer_miss.non_terminal_successor",
        "PrimitiveSequenceIsExecutable()",
        "SljitFullPipelineIsSelectedHashJoinSinkSequence(recipe.primitive_sequence)",
        '#include "sljit_full_pipeline_primitive_contract.hpp"',
    ):
        if stale in source_runtime:
            raise AssertionError(f"source execution must not probe runtime successor shape: {stale}")

    source_fetch = read("extension/jit_sljit/include/sljit_source_fetch_primitive_runtime.hpp")
    for stale in (
        "SljitFullPipelineSourceFetchOwnsSinkAdvance",
        "SljitFullPipelineIsSelectedHashJoinSinkSequence",
        "SljitFullPipelineSourceFetchNeedsPartitionPreservingChunks",
        "const SljitFullPipelinePrimitiveSequence &sequence",
    ):
        if stale in source_fetch:
            raise AssertionError(f"source fetch must consume published recipe ownership instead of shape: {stale}")
    for required in ("selected_hash_join_sink", "preserves_partitioned_source_chunks"):
        if required not in source_fetch:
            raise AssertionError(f"source fetch is missing published runtime ownership: {required}")

    terminal_runtime = read("extension/jit_sljit/include/sljit_full_pipeline_terminal_runtime.hpp")
    if "const SljitHashJoinDirectAggregateConsumerContract &contract" not in terminal_runtime:
        raise AssertionError("terminal execution must receive the bound direct-terminal contract")
    if "SljitHashJoinAggregateConsumerDispatch direct_aggregate_consumer_dispatch" not in terminal_runtime:
        raise AssertionError("direct/materialized consumer dispatch must be retained by pipeline-local runtime state")
    if "direct_aggregate_consumer_miss.terminal_kind" in terminal_runtime:
        raise AssertionError("terminal execution must not rediscover terminal kind at runtime")

    direct_consumer_runtime = read("extension/jit_sljit/include/sljit_hash_join_probe_aggregate_consumer_runtime.hpp")
    if "direct_aggregate_consumer_miss." in direct_consumer_runtime:
        raise AssertionError("direct-consumer physical binding must not expose per-chunk recipe miss paths")

    recipe_binding_header = read("extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp")
    recipe_binding_cpp = read("extension/jit_sljit/sljit_full_pipeline_recipe_binding.cpp")
    recipe_builder = read("extension/jit_sljit/sljit_full_pipeline_recipe.cpp")
    recipe_families = (
        "SourceFilterAggregate",
        "JoinFilterAggregate",
        "SourceHashJoinBuildSink",
        "HashJoinDelimJoinSink",
        "HashJoinAppendSink",
        "HashJoinBuildSink",
    )
    for family in recipe_families:
        if f"TryMake{family}Recipe" not in recipe_binding_header:
            raise AssertionError(f"{family} recipe admission and construction must have one shared binder")
        if re.search(
            rf"\b(?:CanMake|Make){family}Recipe\b",
            recipe_binding_header + recipe_binding_cpp,
        ):
            raise AssertionError(f"{family} recipe shape must not be admitted and reconstructed through duplicate APIs")
        if f"binding.TryMake{family}Recipe(facts, recipe)" not in recipe_builder:
            raise AssertionError(f"recipe builder must consume the shared {family} binder")

    sequence_binding = read("extension/jit_sljit/include/sljit_full_pipeline_recipe_sequence_builder.hpp")
    projection_binding_header = read("extension/jit_sljit/include/sljit_projection_aggregate_recipe_binding.hpp")
    projection_binding = read("extension/jit_sljit/sljit_projection_aggregate_recipe_binding.cpp")
    projected_grouped_binding = read(
        "extension/jit_sljit/include/sljit_projected_grouped_aggregate_update_primitive.hpp"
    )
    binding_surface = "\n".join(
        (
            recipe_binding_header,
            recipe_binding_cpp,
            sequence_binding,
            projection_binding_header,
            projection_binding,
            recipe_builder,
            projected_grouped_binding,
        )
    )
    for stale in (
        "CanMakeNativeTailRecipe",
        "CanMakeProjectionAggregateTailRecipe",
        "ProjectionAggregateHasDedicatedBackend",
        "SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive",
        "SljitBindProjectedInputGroupedAggregateUpdatePrimitive",
    ):
        if re.search(rf"\b{re.escape(stale)}\b", binding_surface):
            raise AssertionError(f"recipe families must not split admission from descriptor construction: {stale}")
    if "ProjectionAggregateBinding()" in binding_surface:
        raise AssertionError("the full-pipeline binder must not reconstruct a recipe-family binder per query")
    for unified_binder in (
        "TryMakeNativeTailRecipe",
        "TryMakeSourceProjectionAggregateTailRecipe",
        "TryMakeJoinDirectProjectionAggregateRecipe",
        "TryMakeJoinProjectionAggregateTailRecipe",
        "TryMakeMarkFilterProjectionAggregateRecipe",
        "TryMakeMarkFilterNativeTailRecipe",
        "SljitTryBindProjectedInputGroupedAggregateUpdatePrimitive",
    ):
        if unified_binder not in binding_surface:
            raise AssertionError(f"recipe family is missing failure-atomic binding: {unified_binder}")
    if "SljitProjectionAggregateRecipeBinding projection_aggregate_recipes" not in recipe_binding_header:
        raise AssertionError("the full-pipeline binder must own one projection-aggregate family binder")
    if "ProjectionAggregateRecipes().TryMakeMarkFilterProjectionNativeTailRecipe" not in recipe_builder:
        raise AssertionError("native-tail selection must call the owning recipe-family binder directly")
    for stale_facade in (
        "extension/jit_sljit/sljit_projection_aggregate_recipe.cpp",
        "extension/jit_sljit/include/sljit_projection_aggregate_recipe.hpp",
        "extension/jit_sljit/sljit_native_tail_recipe.cpp",
        "extension/jit_sljit/include/sljit_native_tail_recipe.hpp",
        "extension/jit_sljit/sljit_hash_join_delim_join_sink_recipe.cpp",
        "extension/jit_sljit/include/sljit_hash_join_delim_join_sink_recipe.hpp",
    ):
        if (ROOT / stale_facade).exists():
            raise AssertionError(f"one-caller recipe facade must stay folded into the recipe builder: {stale_facade}")
    delim_primitive = read("extension/jit_sljit/include/sljit_delim_join_sink_primitive.hpp")
    for stale in (
        "SljitCanBindProjectedDelimJoinSinkPrimitive",
        "SljitBindProjectedDelimJoinSinkPrimitive",
        "SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive",
        "SljitBindSelectedHashJoinDelimJoinSinkPrimitive",
    ):
        if stale in delim_primitive:
            raise AssertionError(f"delimiter sink admission must build its descriptor once: {stale}")
    delim_runtime = read("extension/jit_sljit/include/sljit_delim_join_sink_runtime.hpp")
    if "SljitCanBind" in delim_runtime:
        raise AssertionError("delimiter sink runtime must consume the published descriptor without rebinding")
    region_contract_test = read("test/api/test_jit_region_contracts.cpp")
    for failure_atomic_receipt in (
        "TryMakeGeneratedFilterProjectionNativeTailRecipe",
        "TryMakeMarkFilterProjectionNativeTailRecipe",
        "TryMakeSourceProjectionAggregateTailRecipe",
        "TryMakeJoinDirectProjectionAggregateRecipe",
        "TryMakeJoinProjectionAggregateTailRecipe",
        "TryMakeMarkFilterProjectionAggregateRecipe",
        "TryMakeMarkFilterNativeTailRecipe",
    ):
        if failure_atomic_receipt not in region_contract_test:
            raise AssertionError(f"recipe binder lacks failure-atomic test coverage: {failure_atomic_receipt}")

    executable_builder = read("extension/jit_sljit/sljit_region_executable.cpp")
    region_runtime = read("extension/jit_sljit/sljit_region_runtime.cpp")
    if executable_builder.count("BuildSljitFullPipelineRecipePlan(") != 1:
        raise AssertionError("executable binding must publish exactly one full-pipeline recipe plan")
    if "BuildSljitFullPipelineRecipePlan(" in region_runtime:
        raise AssertionError("runtime kernel construction must not rebuild the published recipe plan")
    if "recipe_plan.HasRecipe() && recipe_plan.Recipe().OwnsFusedFilter(op_idx)" not in executable_builder:
        raise AssertionError("selector codegen must consume the published fused-filter ownership set")
    dispatcher = read("extension/jit_sljit/include/sljit_full_pipeline_dispatch_runtime.hpp")
    if "switch (recipe_plan.Kind())" not in dispatcher or "native_only_runtime_path.empty()" in dispatcher:
        raise AssertionError("runtime dispatch must consume the tagged recipe plan without sentinel state")


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
        '!StringUtil::Contains(paths, "hash_join_probe.perfect_probe.direct_aggregate_consumer.all_valid_rhs=")',
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


def verify_perfect_hash_payload_group_shape_ownership() -> None:
    runtime = read("extension/jit_sljit/include/sljit_aggregate_perfect_hash_payload_runtime.hpp")
    for contract in (
        "const bool payload_flat_no_selection",
        "const bool payload_all_valid",
        "const bool group_flat_no_selection",
        "const bool group_all_valid",
        "native_input.expression_tree_flat_all_valid = payload_flat_all_valid",
        "payload_flat_no_selection && group_flat_no_selection",
        "payload_all_valid && group_all_valid",
    ):
        if contract not in runtime:
            raise AssertionError(
                f"perfect-hash runtime must keep payload and group vector facts independent: {contract}"
            )
    if re.search(
        r"payload_sources\.FlatNoSelection\([^;]+&&\s*group_sources\.FlatNoSelection",
        runtime,
    ):
        raise AssertionError("perfect-hash runtime must not publish a combined vector shape as the payload shape")

    native_input = read("extension/jit_sljit/include/sljit_native_types.hpp")
    for fact in (
        "perfect_hash_group_flat_all_valid",
        "perfect_hash_group_all_valid",
        "perfect_hash_inputs_flat_no_selection",
        "perfect_hash_inputs_all_valid",
        "group_selection_all_present",
    ):
        if fact not in native_input:
            raise AssertionError(f"generated perfect-hash dispatch is missing runtime fact: {fact}")

    loops = read("extension/jit_sljit/sljit_aggregate_perfect_hash_update_loops.cpp")
    dispatch = loops[loops.index("void EmitSljitPerfectHashFusedUpdateLoops") :]
    for contract in (
        "EmitSljitPerfectHashFlatPayloadDictionaryGroupLoop",
        "EmitSljitPerfectHashFlatPayloadSelectedGroupLoop",
        "offsetof(SljitNativeVectorInput, perfect_hash_group_flat_all_valid)",
        "offsetof(SljitNativeVectorInput, perfect_hash_group_all_valid)",
        "offsetof(SljitNativeVectorInput, perfect_hash_inputs_flat_no_selection)",
        "offsetof(SljitNativeVectorInput, perfect_hash_inputs_all_valid)",
        "offsetof(SljitNativeVectorInput, group_selection_all_present)",
        "use_uncached_selected_group_loop",
    ):
        if contract not in dispatch:
            raise AssertionError(f"generated perfect-hash dispatch is missing independent group handling: {contract}")
    payload_gate = dispatch.index("offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid)")
    group_gate = dispatch.index("offsetof(SljitNativeVectorInput, perfect_hash_group_flat_all_valid)")
    direct_loop = dispatch.index("EmitSljitPerfectHashFlatFastLoop")
    dictionary_loop = dispatch.index("EmitSljitPerfectHashFlatPayloadDictionaryGroupLoop")
    selected_loop = dispatch.index("EmitSljitPerfectHashFlatPayloadSelectedGroupLoop")
    if not payload_gate < group_gate < direct_loop < dictionary_loop < selected_loop:
        raise AssertionError("perfect-hash fast loops must dispatch from payload shape and then group representation")

    test = read("test/api/test_jit_aggregate.cpp")
    if "JIT perfect hash keeps payload and group vector shapes independent" not in test:
        raise AssertionError("independent perfect-hash payload/group shape dispatch needs persisted parallel coverage")


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


def verify_cache_keys_use_stable_identities() -> None:
    """Fast-path caches must not key validity on reusable addresses.

    A buffer or entry address can be recycled by the allocator with an equal
    element count, replaying stale cached state against different content (the
    2026-07-18 dictionary-filter ABA bug). Cache validity must compare stable
    identities (dictionary ids, value keys, epochs), never raw addresses.
    """
    filter_state = read("src/include/duckdb/planner/table_filter_state.hpp")
    if "fast_dictionary_matches_dictionary_id" not in filter_state:
        raise AssertionError("dictionary filter match cache must be keyed by the stable dictionary id")
    reject_regex(
        "address-keyed cache validity in filter state",
        (r"const void \*fast_\w*(?:cache|match|entry)",),
        ("src/include/duckdb/planner/table_filter_state.hpp",),
    )
    aggregate_ht = read("src/execution/aggregate_hashtable.cpp")
    for relocating_event in (
        "void GroupedAggregateHashTable::Abandon",
        "void GroupedAggregateHashTable::Repartition",
        "GroupedAggregateHashTable::AcquirePartitionedData",
        "void GroupedAggregateHashTable::Combine",
    ):
        event_body = aggregate_ht.split(relocating_event, 1)
        if len(event_body) < 2 or "dense_single_field_target_cache.Disable()" not in event_body[1][:2500]:
            raise AssertionError(
                f"row-relocating event must disable the dense address cache: {relocating_event}"
            )


def verify_runner_cost_schema_single_authority() -> None:
    """Every summable runner-cost field must be visited by the schema walks.

    The walks in cost_model.hpp are the single authority the totals accumulation,
    system-table work columns, and profiler emission derive from; an int64 profile
    field missing from both walks would silently vanish from every surface.
    """
    cost_header = read("src/include/duckdb/planner/cost_model.hpp")
    profile_body = cost_header.split("struct PhysicalRunnerCostProfile", 1)[1].split("};", 1)[0]
    fields = re.findall(r"int64_t\s+(\w+)\s*=", profile_body)
    walk_bodies = "".join(
        cost_header.split(marker, 1)[1].split("\n}\n", 1)[0]
        for marker in ("void ForEachPhysicalRunnerCostShapeField", "void ForEachPhysicalRunnerCostWorkField")
    )
    for field in fields:
        if re.search(r"\b" + field + r"\b", walk_bodies):
            continue
        raise AssertionError(f"runner cost field is missing from the schema walks: {field}")
    breakdown_body = cost_header.split("struct PhysicalRunnerAxisCostBreakdown", 1)[1].split("};", 1)[0]
    for field in re.findall(r"int64_t\s+(\w+)\s*=", breakdown_body):
        for axis in ("compiled_vectorized", "gpu"):
            if not re.search(r"\b" + axis + r"\." + field + r"\b", walk_bodies):
                raise AssertionError(f"axis breakdown field is missing from the work walk: {axis}.{field}")
    for consumer, marker in (
        ("src/execution/execution_region_telemetry_log.cpp", "ForEachPhysicalRunnerCostShapeField"),
        ("src/execution/execution_region_telemetry_log.cpp", "ForEachPhysicalRunnerCostWorkField"),
        ("src/function/table/system/execution_region_table_function_utils.hpp", "ForEachPhysicalRunnerCostWorkField"),
        ("src/main/query_profiler.cpp", "ForEachPhysicalRunnerCostShapeField"),
        ("src/main/query_profiler.cpp", "ForEachPhysicalRunnerCostWorkField"),
    ):
        if marker not in read(consumer):
            raise AssertionError(f"runner cost surface must derive from the schema walks: {consumer}")


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
    hash_join_runtime = read("src/include/duckdb/execution/execution_hash_join_runtime.hpp")
    filter_layout = hash_join_runtime.split("struct ExecutionPerfectHashJoinFilterLayout", 1)[1].split("};", 1)[0]
    for owning_type in ("LogicalType", "shared_ptr", "vector<", "buffer_ptr", "string"):
        if owning_type in filter_layout:
            raise AssertionError(f"per-vector perfect-hash filter layout must remain allocation-free: {owning_type}")
    perfect_executor = read("src/execution/operator/join/perfect_hash_join_executor.cpp")
    if "PublishExecutionPerfectHashJoinFilterLayout" not in perfect_executor:
        raise AssertionError("full and scan-only perfect-hash layouts must share one primitive layout binder")
    perfect_executor_header = read("src/include/duckdb/execution/operator/join/perfect_hash_join_executor.hpp")
    if "ExecutionPerfectHashJoinFilterLayout execution_filter_layout" not in perfect_executor_header:
        raise AssertionError("perfect-hash finalization must own one immutable scan-filter contract")
    if "PublishExecutionPerfectHashJoinFilterLayout()" not in perfect_executor:
        raise AssertionError("perfect-hash finalization must publish the scan-filter contract once")
    if "attempted_filter_layout" in perfect_executor:
        raise AssertionError("perfect-hash consumers must not reconstruct unpublished finalization state")
    filter_binding = read("src/include/duckdb/planner/filter/table_filter_functions.hpp")
    if "optional_ptr<const ExecutionPerfectHashJoinFilterLayout> filter_layout" not in filter_binding:
        raise AssertionError("the published perfect-hash dynamic filter must own its immutable membership view")
    filter_plan = read("src/storage/table/column_segment.cpp")
    if "perfect_data->filter_layout" not in filter_plan:
        raise AssertionError("scan-filter admission must consume the dynamic filter's finalized membership view")
    filter_state = read("src/include/duckdb/planner/table_filter_state.hpp")
    for contract in (
        "struct FastInternalFilterScanPlan",
        "idx_t primary_operation_count",
        "optional_ptr<const ExecutionPerfectHashJoinFilterLayout> perfect_hash_join_layout",
    ):
        if contract not in filter_state:
            raise AssertionError(f"thread-local scan planning must bind immutable filter invariants once: {contract}")
    if "BuildFastInternalFilterScanPlan(state);" not in filter_plan:
        raise AssertionError("internal filter analysis must publish its compression scan plan once")
    bitpacking = read("src/storage/compression/bitpacking.cpp")
    perfect_filter = bitpacking.split("static bool TryBitpackingPerfectHashJoinFilter(", 1)[1].split(
        "template <class T, bool HAS_RESIDUAL_RANGES>", 1
    )[0]
    if "scan_plan.perfect_hash_join_layout" not in perfect_filter:
        raise AssertionError("bitpacking scan filters must consume the once-bound perfect-hash membership view")
    for repeated_invariant in (
        "perfect_hash_join_data->filter_layout",
        "build_validity_word_count !=",
        "while (!track_primary_selectivity",
    ):
        if repeated_invariant in perfect_filter:
            raise AssertionError(
                f"bitpacking scan filters must not rebind immutable perfect-hash invariants: {repeated_invariant}"
            )
    if "GetExecutionPerfectHashJoinFilterLayout" in perfect_filter:
        raise AssertionError("bitpacking scan filters must not rebind finalized membership state per vector")
    if "ExecutionPerfectHashJoinTableLayout" in perfect_filter:
        raise AssertionError("bitpacking scan filters must not copy the materialization table layout per vector")
    bloom_filter = bitpacking.split("static bool TryBitpackingBloomFilter(", 1)[1].split(
        "template <class T>\nvoid BitpackingFilter", 1
    )[0]
    if "while (!track_primary_selectivity" in bloom_filter:
        raise AssertionError("bitpacking Bloom filters must consume the once-bound primary operation prefix")
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
        "idx_t selected_count = 0;",
        "result[selected_count++] = logical_index;",
        "return selected_count;",
    ):
        if contract not in runtime:
            raise AssertionError(f"two-fragment LIKE batch selection is missing hot-path contract: {contract}")

    batch_loop = runtime.split("static idx_t SljitSelectStringLikeBatchLoop", 1)[-1].split(
        "static idx_t SljitSelectStringLikeBatchNegation", 1
    )[0]
    for stale_selection_contract in (
        "if constexpr (NEGATE && !HAS_EXECUTE_SELECTION)",
        "selection_materialized",
        "rejected_count",
        "copy_range",
        "rejected_idx",
    ):
        if stale_selection_contract in batch_loop:
            raise AssertionError(
                f"negated two-fragment LIKE selection must use normal-form one-pass compaction: {stale_selection_contract}"
            )

    filter_runtime = read("extension/jit_sljit/include/sljit_filter_runtime.hpp")
    for contract in (
        "if (selected_count == input.size())",
        "output.Reference(input);",
    ):
        if contract not in filter_runtime:
            raise AssertionError(f"all-match batch selection must reuse the unchanged input view: {contract}")

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


def verify_regular_hash_join_direct_aggregate_storage_contract() -> None:
    layout = read("src/include/duckdb/execution/execution_hash_join_runtime.hpp")
    for contract in (
        "enum class ExecutionHashJoinRHSFixedColumnStorageKind",
        "ROW, DICTIONARY",
        "ExecutionHashJoinRHSFixedColumnTypeSupported",
        "idx_t dictionary_index_offset = DConstants::INVALID_INDEX",
        "const_data_ptr_t dictionary_data = nullptr",
        "const validity_t *dictionary_validity = nullptr",
        "idx_t dictionary_count = 0",
    ):
        if contract not in layout:
            raise AssertionError(f"fixed hash-join RHS ABI is missing physical storage contract: {contract}")

    hash_table = read("src/execution/join_hashtable.cpp")
    for contract in (
        "source.storage_kind = ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY",
        "source.dictionary_index_offset = pointer_offset",
        "source.dictionary_data = FlatVector::GetData(dictionary)",
        "source.storage_kind = ExecutionHashJoinRHSFixedColumnStorageKind::ROW",
        "source.layout_offset = layout_offsets[output_col_idx]",
    ):
        if contract not in hash_table:
            raise AssertionError(f"hash table must expose both fixed RHS storage forms: {contract}")
    fixed_source = hash_table.split("bool JoinHashTable::TryGetRHSFixedColumnSource", 1)[1].split(
        "void JoinHashTable::GatherRHSColumn", 1
    )[0]
    if fixed_source.index("source.ready = true;") < fixed_source.index("source.dictionary_count = dictionary.size();"):
        raise AssertionError(
            "hash-table fixed-column sources must publish ready only after physical storage is complete"
        )

    state = read("extension/jit_sljit/include/sljit_join_projection_aggregate_state.hpp")
    direct_descriptor = state.split("struct SljitHashJoinDirectUngroupedAggregateDescriptor", 1)[1].split("};", 1)[0]
    for contract in (
        "AggregatePrimitiveUpdateKind primitive_kind",
        "idx_t rhs_output_idx",
        "LogicalType rhs_type",
        "PhysicalType rhs_physical_type",
    ):
        if contract not in direct_descriptor:
            raise AssertionError(f"direct aggregate semantic descriptor is missing immutable identity: {contract}")
    if "ExecutionHashJoinRHSFixedColumnSource" in direct_descriptor:
        raise AssertionError("direct aggregate semantic descriptors must not cache mutable physical storage")

    descriptor_binding = read("extension/jit_sljit/include/sljit_projection_aggregate_ungrouped_descriptor.hpp")
    for contract in (
        "SljitBindHashJoinDirectUngroupedAggregateDescriptor",
        "SljitTryGetExecutableReferenceInputIndex",
        "ExecutionHashJoinRHSFixedColumnTypeSupported",
        "descriptor.direct_ungrouped_aggregate",
    ):
        if contract not in descriptor_binding:
            raise AssertionError(f"canonical aggregate descriptor must bind the direct semantic contract: {contract}")

    runtime = read("extension/jit_sljit/include/sljit_hash_join_probe_ungrouped_aggregate_consumer_runtime.hpp")
    for contract in (
        "SljitHashJoinDirectUngroupedAggregateProbeConsumer",
        "strategy.descriptor.direct_ungrouped_aggregate",
        "ExecutionGetHashJoinRHSFixedColumnSource(hash_join_binding, plan.rhs_output_idx, rhs_source)",
        "TryExecuteAllValidSingleKeyNoChainProbeWithConsumer<SELECTED>",
        "join_output_probe_consumer_ungrouped_aggregate.dictionary_source",
        "join_output_probe_consumer_ungrouped_aggregate.row_source",
    ):
        if contract not in runtime:
            raise AssertionError(f"direct regular-probe reduction is missing fused storage dispatch: {contract}")
    if "SljitHashJoinMatchedRowBatchConsumer" in runtime:
        raise AssertionError("direct ungrouped reduction must not stage matched row-pointer microbatches")
    for stale in (
        "SljitTryBuildHashJoinDirectUngroupedAggregatePlan",
        "direct_ungrouped_aggregate_consumer_miss",
        '"duckdb/execution/join_hashtable.hpp"',
        "JoinHashTable::ValidityBytes",
    ):
        if stale in runtime:
            raise AssertionError(
                f"direct ungrouped runtime must not reconstruct recipes or depend on hash-table internals: {stale}"
            )

    rhs_runtime = read("extension/jit_sljit/include/sljit_hash_join_rhs_fixed_column_runtime.hpp")
    for stale in (
        '"duckdb/execution/join_hashtable.hpp"',
        "JoinHashTable::ValidityBytes",
    ):
        if stale in rhs_runtime:
            raise AssertionError(
                f"fixed-column backend helpers must depend only on the exported execution ABI: {stale}"
            )

    group_sources = read("extension/jit_sljit/include/sljit_grouped_aggregate_group_key_source.hpp")
    if "rhs_source.storage_kind != ExecutionHashJoinRHSFixedColumnStorageKind::ROW" not in group_sources:
        raise AssertionError("row-pointer grouped sources must explicitly reject dictionary-backed RHS storage")

    for row_only_runtime in (
        "extension/jit_sljit/include/sljit_hash_join_rhs_projection_runtime.hpp",
        "extension/jit_sljit/include/sljit_join_input_row_pointer_complementary_sum_runtime.hpp",
    ):
        if "ExecutionHashJoinRHSFixedColumnStorageKind::ROW" not in read(row_only_runtime):
            raise AssertionError(f"row-layout consumers must explicitly reject dictionary storage: {row_only_runtime}")

    join_test = read("test/api/test_jit_join.cpp")
    for receipt in (
        "JIT reduces regular hash matches directly from row and dictionary RHS storage",
        "join_output_probe_consumer_ungrouped_aggregate.dictionary_source=",
        "join_output_probe_consumer_ungrouped_aggregate.row_source=",
        "join_output_probe_consumer_ungrouped_aggregate.source_none=",
    ):
        if receipt not in join_test:
            raise AssertionError(f"direct regular-probe reduction is missing correctness receipt: {receipt}")

    benchmark = read("benchmark/jit/generic_benchmark.py")
    for receipt in (
        "direct_ungrouped_aggregate_consumer=",
        "aggregate_update.join_output_probe_consumer_ungrouped_aggregate.dictionary_source=",
        '"minimum_auto_speedup_by_threads": {1: 1.35, 4: 1.09}',
        '"join_exact_filter_build": {1: 9643, 4: 5935}',
    ):
        if receipt not in benchmark:
            raise AssertionError(f"direct regular-probe performance proof is not ratcheted: {receipt}")



def verify_deferral_legality_and_handoff_single_authority() -> None:
    """Runner handoff must be impossible with rows in flight, and the recipe
    strategies that refuse it must be classified in one place.

    A mid-stream deferral abandons the contract cursor's partially-read row
    group (measured 2026-07-19: one full row group silently lost per defer), so
    the runtime accepts a deferral only at kernel entry or through the
    declined-claim boundary path and throws otherwise. The strategy-level
    handoff classification lives next to the strategy enum; the kernel derives
    its capability from it instead of pattern-matching strategy names locally.
    """
    runner = read("src/execution/execution_region_runner.cpp")
    if "requested a mid-stream deferral" not in runner:
        raise AssertionError("the runtime must reject mid-stream deferral requests loudly")
    if runner.count("DeferAtClaimBoundary(") < 3:
        raise AssertionError(
            "declined-claim boundary deferrals must use the boundary-only path, not the public Defer"
        )
    if "CanDeferAtEntry" not in runner:
        raise AssertionError("the runtime must expose entry-deferral legality to kernels")
    kernel = read("extension/jit_sljit/sljit_region_runtime.cpp")
    if "DISTINCT_KEY_SINK" in kernel:
        raise AssertionError(
            "the kernel must not pattern-match strategy names; handoff capability is the "
            "conjunction of SljitFullPipelinePrimitiveStepSupportsRunnerHandoff over recipe steps"
        )
    if "SljitFullPipelinePrimitiveStepSupportsRunnerHandoff" not in kernel:
        raise AssertionError("the kernel capability must derive from the step-level single authority")
    if "SljitTryDeferNotReadyNativeJoinProbesAtEntry" not in kernel:
        raise AssertionError("the kernel must probe native join readiness in its entry prologue")
    strategy_header = read("extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp")
    if ("enum class SljitGroupedAggregateUpdateStrategyKind" not in strategy_header
            or "SljitGroupedAggregateUpdateStrategySupportsRunnerHandoff" not in strategy_header):
        raise AssertionError(
            "the strategy handoff classification must live beside the strategy enum definition"
        )
    if runner.count("pipeline.VectorizedSourceCursorDirty()") < 2:
        raise AssertionError(
            "both compiled entry points in the adaptive state machine must bounce executors whose "
            "vectorized cursor did an unmanaged fetch (the executor-side layer of the claim-point law)"
        )
    executor_header = read("src/include/duckdb/parallel/pipeline_executor.hpp")
    if "enum class SourceCursorState" not in executor_header:
        raise AssertionError("the source-cursor state machine must own runner-switch legality")
    executor = read("src/parallel/pipeline_executor.cpp")
    if "SourceCursorState::VECTORIZED_UNMANAGED" not in executor:
        raise AssertionError(
            "FetchFromSource must degrade unmanaged vectorized fetches to VECTORIZED_UNMANAGED so dirty "
            "cursors never enter compiled execution"
        )
    if "compiled source contract fetch while a vectorized cursor state is in flight" not in executor:
        raise AssertionError(
            "FetchFromSourceContract must reject contract fetches over an in-flight vectorized cursor"
        )
    if ".phase.store(" in runner:
        raise AssertionError(
            "adaptive phase transitions must live in ExecutionRegionAdaptiveAbState methods, not in the "
            "runner: split-brain phase writes are how the runner-switch bugs were born"
        )
    builder = read("extension/jit_sljit/sljit_full_pipeline_recipe_sequence_builder.cpp")
    if "NativeTailInputLayoutMatches" not in builder:
        raise AssertionError(
            "native-tail admission must prove the materialized view layout against the tail operator's "
            "declared input where derivable; a declared mismatch must reject at construction, not at runtime"
        )
    contract = read("extension/jit_sljit/sljit_full_pipeline_primitive_contract.cpp")
    for validation_site, name in ((builder, "sequence builder"), (contract, "primitive contract")):
        if "DeclaredInputConstraints" not in validation_site:
            raise AssertionError(
                f"the {name} layout validation must consume DeclaredInputConstraints — the single "
                "authority for what an op's execution dereferences — instead of kind-specific fields"
            )
    metal = read("extension/jit_metal/metal_backend.mm")
    if "SupportsRunnerHandoff" not in metal:
        raise AssertionError(
            "the metal kernel must refuse runner handoff: it batches source chunks without a per-chunk "
            "flush, so a declined claim boundary would strand claimed-but-unsunk rows"
        )
    column_segment = read("src/storage/table/column_segment.cpp")
    if "RegisterTransientMemory" not in column_segment:
        raise AssertionError(
            "ColumnSegment::Resize must register the grown block as transient memory: a plain allocation "
            "is destroy-on-eviction and loses live table segments under memory pressure"
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
    verify_partial_predicate_simd_contract()
    verify_string_batch_selection_contract()
    verify_hash_join_null_fact_ownership()
    verify_regular_hash_join_direct_aggregate_storage_contract()
    verify_runtime_proofs_are_typed()
    verify_runner_cost_schema_single_authority()
    verify_cache_keys_use_stable_identities()
    verify_deferral_legality_and_handoff_single_authority()
    verify_production_contract_ownership()
    verify_benchmark_repetition_budget()
    verify_bound_direct_join_terminal_contract()
    verify_perfect_hash_predicate_cache_ownership()
    verify_perfect_hash_payload_group_shape_ownership()
    verify_perfect_hash_identity_selected_view()
    verify_perfect_hash_all_valid_complementary_accumulator()
    print("Execution-region architecture verification passed")


if __name__ == "__main__":
    main()
