#!/usr/bin/env python3
"""Static dependency and ownership boundaries for execution-region JIT.

Behavior belongs in C++ or Python tests, and generated-file drift belongs in
the owning generators. This verifier is deliberately limited to constraints
that compilation and runtime tests cannot express: forbidden dependencies,
workload leakage, duplicated ownership APIs, and platform/extension ABI seams.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CORE_SOURCES = ("src/**/*.hpp", "src/**/*.cpp")
BACKEND_SOURCES = ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp")
JIT_SOURCES = CORE_SOURCES + BACKEND_SOURCES


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8", errors="replace")


def matching_files(globs: tuple[str, ...]) -> list[Path]:
    paths: set[Path] = set()
    for pattern in globs:
        paths.update(path for path in ROOT.glob(pattern) if path.is_file())
    return sorted(paths)


def reject_regex(
    rule: str,
    patterns: tuple[str, ...],
    globs: tuple[str, ...],
    allowed: tuple[str, ...] = (),
) -> None:
    allowed_paths = {(ROOT / path).resolve() for path in allowed}
    compiled = [(pattern, re.compile(pattern)) for pattern in patterns]
    for path in matching_files(globs):
        if path.resolve() in allowed_paths:
            continue
        source = path.read_text(encoding="utf-8", errors="replace")
        for pattern, regex in compiled:
            if regex.search(source):
                raise AssertionError(f"{relative(path)}: {rule}: {pattern}")


def require_text(path: str, rule: str, snippets: tuple[str, ...]) -> None:
    source = read(path)
    missing = [snippet for snippet in snippets if snippet not in source]
    if missing:
        raise AssertionError(f"{path}: {rule}: missing {missing}")


def require_absent_files(paths: tuple[str, ...]) -> None:
    for path in paths:
        if (ROOT / path).exists():
            raise AssertionError(
                f"{path}: obsolete one-caller recipe facade must stay deleted"
            )


def verify_dependency_boundaries() -> None:
    reject_regex(
        "SLJIT dependency in core DuckDB",
        (r"\bsljit\b", r"\bSljit\b", r"\bjit_sljit\b"),
        CORE_SOURCES,
        ("src/main/extension/extension_helper.cpp",),
    )
    reject_regex(
        "backend dependency on operator-private implementation",
        (
            r'#include\s+"duckdb/execution/operator/',
            r'#include\s+"duckdb/execution/join_hashtable\.hpp"',
            r'#include\s+"duckdb/execution/ht_entry\.hpp"',
            r'#include\s+"duckdb/planner/filter/table_filter_functions\.hpp"',
            r"\bht_entry_t\b",
            r"\bBloomFilter\b",
            r"\bJoinHashTable\b",
            r"\b(?:binding|probe)\.hash_table->",
        ),
        BACKEND_SOURCES,
    )
    reject_regex(
        "aggregate source scan must not publish sink-state layout",
        (
            r"result\.source\.aggregate_contract\s*=",
            r"result\.source\.aggregates\s*=",
            r"result\.source\.groups\s*=",
        ),
        ("src/execution/execution_contract.cpp",),
    )
    require_text(
        "src/include/duckdb/execution/execution_region_ir.hpp",
        "aggregate state-scan IR must publish semantic lanes instead of physical offsets",
        (
            "struct ExecutionRegionPrimitiveAggregateStateLane",
            "idx_t source_output_index",
            "vector<ExecutionRegionPrimitiveAggregateStateLane> primitive_aggregate_lanes",
        ),
    )
    require_text(
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        "aggregate state-scan runtime must remain opaque and semantic",
        (
            "class DUCKDB_API ExecutionAggregateStateScanBatch",
            "virtual bool CombinePrimitive",
        ),
    )
    require_text(
        "src/execution/join_hashtable.cpp",
        "physical hash table must use the canonical execution row-layout builder",
        ("BuildExecutionHashJoinRowLayout(condition_types, build_types",),
    )
    require_text(
        "src/execution/execution_contract.cpp",
        "semantic hash-join contract must use the canonical execution row-layout builder",
        (
            "BuildExecutionHashJoinRowLayout(result.condition_types, result.payload_types",
        ),
    )
    reject_regex(
        "regular hash-join layout validation below operator binding",
        (r"\bSljitValidateRegularHashJoinProbeExecutionLayout\s*\(",),
        BACKEND_SOURCES,
        (
            "extension/jit_sljit/include/sljit_hash_join_runtime.hpp",
            "extension/jit_sljit/include/sljit_native_binding_runtime.hpp",
            "extension/jit_sljit/sljit_hash_join_runtime.cpp",
        ),
    )
    reject_regex(
        "per-use hash-join RHS source rebinding",
        (r"\bbinding\.hash_table->TryGetRHSFixedColumnSource\s*\(",),
        ("src/execution/operator/join/physical_hash_join.cpp",),
    )
    reject_regex(
        "all-valid hash-probe template dispatch outside its runtime owner",
        (r'#include\s+"sljit_hash_join_all_valid_probe_dispatch_runtime\.hpp"',),
        BACKEND_SOURCES,
        ("extension/jit_sljit/sljit_hash_join_all_valid_probe_fast_path_runtime.cpp",),
    )
    reject_regex(
        "all-valid hash-probe matcher policy outside dispatch",
        (r'#include\s+"sljit_hash_join_all_valid_probe_matcher_runtime\.hpp"',),
        BACKEND_SOURCES,
        (
            "extension/jit_sljit/include/sljit_hash_join_all_valid_mark_selection_probe_dispatch_runtime.hpp",
            "extension/jit_sljit/include/sljit_hash_join_all_valid_probe_dispatch_runtime.hpp",
        ),
    )
    reject_regex(
        "full-pipeline template graph outside its runtime owner",
        (r'#include\s+"sljit_full_pipeline_dispatch_runtime\.hpp"',),
        BACKEND_SOURCES,
        ("extension/jit_sljit/sljit_region_pipeline_runtime.cpp",),
    )
    reject_regex(
        "grouped-preaggregation template graph outside its runtime owner",
        (
            r'#include\s+"sljit_grouped_aggregate_pending_preaggregation_runtime\.hpp"',
            r'#include\s+"sljit_grouped_aggregate_run_preaggregation_runtime\.hpp"',
        ),
        BACKEND_SOURCES,
        ("extension/jit_sljit/sljit_grouped_aggregate_preaggregation_runtime.cpp",),
    )
    reject_regex(
        "hash-join aggregate-consumer template graph outside its runtime owner",
        (r'#include\s+"sljit_hash_join_probe_aggregate_consumer_runtime\.hpp"',),
        BACKEND_SOURCES,
        ("extension/jit_sljit/sljit_hash_join_aggregate_consumer_runtime.cpp",),
    )
    reject_regex(
        "join-input complementary row-loop graph outside aggregate-consumer implementation",
        (r'#include\s+"sljit_join_input_row_pointer_complementary_sum_runtime\.hpp"',),
        BACKEND_SOURCES,
        (
            "extension/jit_sljit/include/sljit_hash_join_probe_aggregate_consumer_runtime.hpp",
        ),
    )
    reject_regex(
        "join-input complementary accumulator templates outside their row-loop implementation",
        (r'#include\s+"sljit_join_input_complementary_sum_accumulator\.hpp"',),
        BACKEND_SOURCES,
        (
            "extension/jit_sljit/include/sljit_join_input_row_pointer_complementary_sum_runtime.hpp",
        ),
    )
    reject_regex(
        "artifact-cache policy below the planner/manager layer",
        (r"\bExecutionRegionArtifactCache\b",),
        (
            "src/include/duckdb/execution/physical_operator.hpp",
            "src/include/duckdb/execution/execution_region_kernel.hpp",
        ),
    )


def verify_workload_independence() -> None:
    workload_patterns = (
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
    )
    reject_regex(
        "benchmark-shaped production JIT logic",
        workload_patterns,
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


def verify_backend_and_platform_abi() -> None:
    backend_header = "src/include/duckdb/execution/execution_region_backend.hpp"
    require_text(
        backend_header,
        "loadable backend ABI must export every cross-extension type",
        (
            "class DUCKDB_API ExecutionRegionArtifact",
            "class DUCKDB_API ExecutionRegionBackendPlan",
            "struct DUCKDB_API ExecutionRegionCompilationInput",
            "struct DUCKDB_API ExecutionRegionCompileResult",
            "class DUCKDB_API ExecutionRegionBackend",
            "EXECUTION_REGION_BACKEND_ABI_VERSION",
            "uint64_t backend_abi_version",
        ),
    )
    require_text(
        "src/include/duckdb/execution/execution_region_manager.hpp",
        "backend registration must carry the ABI version",
        (
            "RegisterBackend(unique_ptr<ExecutionRegionBackend> backend, uint64_t backend_abi_version)",
        ),
    )
    for backend in (
        "extension/jit_sljit/sljit_backend.cpp",
        "extension/jit_metal/metal_backend.mm",
    ):
        require_text(
            backend,
            "backend registration must declare its ABI version",
            ("EXECUTION_REGION_BACKEND_ABI_VERSION",),
        )

    reject_regex(
        "target ABI admission outside the platform capability owner",
        (
            r"\bSLJIT_CONFIG_",
            r"\bSLJIT_NUMBER_OF_(?:SAVED_)?REGISTERS\b",
            r"\bSLJIT_32BIT_ARCHITECTURE\b",
            r"\bsljit_has_cpu_feature\s*\(\s*SLJIT_HAS_SIMD\s*\)",
            r"\bSLJIT_S\s*\(",
        ),
        BACKEND_SOURCES,
        ("extension/jit_sljit/sljit_platform.cpp",),
    )
    reject_regex(
        "high saved-register role outside the semantic register-layout owner",
        (r"\bSLJIT_S(?:[6-9]|[1-9][0-9]+)\b",),
        BACKEND_SOURCES,
        ("extension/jit_sljit/sljit_register_layout.cpp",),
    )
    require_text(
        "third_party/sljit/CMakeLists.txt",
        "supported platforms need an explicit W^X executable-memory policy",
        (
            "SLJIT_PROT_EXECUTABLE_ALLOCATOR=1",
            "SLJIT_WX_EXECUTABLE_ALLOCATOR=1",
            "no W^X executable-memory policy",
        ),
    )


def verify_no_executor_delegation() -> None:
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
        JIT_SOURCES,
    )


def verify_recipe_ownership() -> None:
    reject_regex(
        "projection-aggregate prefix encoded as a fixed join count",
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
    reject_regex(
        "runtime recipe re-admission or untyped operator ownership",
        (
            r"\bSljitCanBind",
            r"\bSljitFullPipelinePrimitiveSequenceIsExecutable\b",
            r"\bSljitFullPipelineIsSelectedHashJoinSinkSequence\b",
            r"\bSljitFullPipelineFilterHasFusedOwner\b",
            r"\bSljitFullPipelinePrimitiveStepOwnsOps\b",
            r"\.Op\(",
        ),
        (
            "extension/jit_sljit/include/*runtime*.hpp",
            "extension/jit_sljit/sljit_region*_runtime.cpp",
        ),
    )
    reject_regex(
        "duplicated recipe admission and construction API",
        (
            r"\bCanMake(?:NativeTail|ProjectionAggregateTail)Recipe\b",
            r"\bProjectionAggregateHasDedicatedBackend\b",
            r"\bSljitBindProjectedInputGroupedAggregateUpdatePrimitive\b",
            r"\bSljitCanBindProjectedInputGroupedAggregateUpdatePrimitive\b",
        ),
        BACKEND_SOURCES,
    )
    require_absent_files(
        (
            "extension/jit_sljit/sljit_projection_aggregate_recipe.cpp",
            "extension/jit_sljit/include/sljit_projection_aggregate_recipe.hpp",
            "extension/jit_sljit/sljit_native_tail_recipe.cpp",
            "extension/jit_sljit/include/sljit_native_tail_recipe.hpp",
            "extension/jit_sljit/sljit_hash_join_delim_join_sink_recipe.cpp",
            "extension/jit_sljit/include/sljit_hash_join_delim_join_sink_recipe.hpp",
        )
    )


def verify_runtime_abi_ownership() -> None:
    reject_regex(
        "aggregate runtime reconstructs planner payload metadata",
        (
            r"\bExecutionRegionAggregateInput\b",
            r"\bprimitive_update_kind\b",
        ),
        ("extension/jit_sljit/include/*runtime*.hpp",),
    )
    reject_regex(
        "aggregate payload ABI reconstructed outside descriptor binding",
        (
            r"\baggregate\.child_types\[0\]",
            r"\baggregate\.primitive_update_input_type\b",
        ),
        (
            "extension/jit_sljit/sljit_aggregate_*codegen.cpp",
            "extension/jit_sljit/include/sljit_region_aggregate_payload_fusion.hpp",
        ),
    )
    reject_regex(
        "chunk-local grouped reduction-lane binding",
        (
            r"vector<SljitGroupedReductionLaneBinding>\s+reduction_lanes",
            r"\bSljitValidateGroupedPrimitiveLane(?:Layout|State)\b",
        ),
        BACKEND_SOURCES,
    )
    reject_regex(
        "aggregate descriptor-to-lane ABI reconstructed outside its binder",
        (
            r"\bdescriptor\.(?:primitive_kind|aggregate_index|state_size|state_value_offset|"
            r"state_is_set_offset|input_type)"
            r"\s*!=\s*(?:runtime_)?lane",
            r"\baggregate\.primitive_update_(?:kind|state_size|state_value_offset|state_is_set_offset|input_type)\b",
        ),
        (
            "extension/jit_sljit/include/sljit_aggregate_primitive_payload_runtime.hpp",
            "extension/jit_sljit/include/sljit_grouped_reduction_lane.hpp",
            "extension/jit_sljit/include/sljit_grouped_aggregate_direct_update_capability_runtime.hpp",
            "extension/jit_sljit/include/sljit_grouped_aggregate_input_vector_run_update_runtime.hpp",
            "extension/jit_sljit/include/sljit_grouped_aggregate_preaggregated_update_runtime.hpp",
            "extension/jit_sljit/include/sljit_grouped_count_star_update_runtime.hpp",
            "extension/jit_sljit/include/sljit_string_set_complementary_sum_runtime.hpp",
            "extension/jit_sljit/sljit_aggregate_perfect_hash_commit_codegen.cpp",
        ),
    )


def verify_publication_and_cache_ownership() -> None:
    artifact = read("extension/jit_sljit/include/sljit_compiled_function.hpp")
    for mutable_api in (
        "shared_ptr<ExecutionRegionCodeHandle> &Code()",
        "FUNCTION &Function()",
        "void Set(",
    ):
        if mutable_api in artifact:
            raise AssertionError(
                f"compiled artifact exposes split mutable state: {mutable_api}"
            )
    reject_regex(
        "split compiled-artifact publication",
        (r"\.(?:Code|Function)\(\)\s*=", r"\.Set\(\s*std::move\([^\n]+code"),
        BACKEND_SOURCES,
    )
    reject_regex(
        "backend-owned lazy code-size accounting",
        (r"\bAddTraceCodeSize\(",),
        BACKEND_SOURCES,
    )
    reject_regex(
        "raw-address cache identity",
        (r"const void \*fast_\w*(?:cache|match|entry)",),
        ("src/include/duckdb/planner/table_filter_state.hpp",),
    )
    reject_regex(
        "query-specific classifier retained by a reusable artifact",
        (r"\bshared_predicate_classification\b",),
        ("extension/jit_sljit/include/sljit_region_executable.hpp",),
    )


def verify_obsolete_paths_stay_deleted() -> None:
    reject_regex(
        "stringly typed JIT runtime proof",
        (r'RecordJitRuntimeProof\(\s*"', r"\bproof_path\.find\b"),
        JIT_SOURCES + ("test/api/test_jit*.cpp", "test/api/test_jit_helpers.hpp"),
    )
    reject_regex(
        "stale JIT runtime expectation",
        (
            r"\bsource_batch_boundary\b",
            r"\bprojected_compact_aggregate_input\b",
            r"\bRequireCurrentMaterializationElisionProofName\b",
            r"\bRequireCurrentMaterializationElisionRuntimeProof\b",
        ),
        JIT_SOURCES
        + (
            "test/api/test_jit*.cpp",
            "test/api/test_jit_helpers.hpp",
            "benchmark/jit/**/*.py",
            "benchmark/tpch/jit/**/*.py",
        ),
    )
    reject_regex(
        "obsolete all-slot execution-contract API",
        (r"\bGetExecutionContract\(\s*\)",),
        (
            "src/include/duckdb/execution/physical_operator.hpp",
            "src/execution/execution_region*.cpp",
            "extension/jit_sljit/**/*.hpp",
            "extension/jit_sljit/**/*.cpp",
        ),
    )
    reject_regex(
        "runner-owned adaptive phase mutation",
        (r"\.phase\.store\(",),
        ("src/execution/execution_region_runner.cpp",),
    )
    reject_regex(
        "kernel pattern-matches a grouped strategy to decide handoff",
        (r"\bDISTINCT_KEY_SINK\b",),
        (
            "extension/jit_sljit/sljit_region_runtime.cpp",
            "extension/jit_sljit/sljit_region_pipeline_runtime.cpp",
        ),
    )
    reject_regex(
        "duplicate unchecked narrow probe consumer loop",
        (r"\bTryExecuteAllValidUncheckedInt64ToInt32SingleKeyNoChainProbeConsumer\b",),
        BACKEND_SOURCES,
    )
    reject_regex(
        "stale prototype-kernel cache API",
        (
            r"\bCloneForExecution\b",
            r"\bExecutionRegionKernelCache\b",
            r"\bExecutionRegionKernelCacheValue\b",
        ),
        (
            "src/include/duckdb/execution/execution_region_kernel.hpp",
            "src/include/duckdb/execution/physical_operator.hpp",
            "src/execution/execution_region_planner.cpp",
        ),
    )


def main() -> None:
    verify_dependency_boundaries()
    verify_workload_independence()
    verify_backend_and_platform_abi()
    verify_no_executor_delegation()
    verify_recipe_ownership()
    verify_runtime_abi_ownership()
    verify_publication_and_cache_ownership()
    verify_obsolete_paths_stay_deleted()
    print("Execution-region JIT boundary verification passed")


if __name__ == "__main__":
    main()
