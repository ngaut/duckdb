#!/usr/bin/env python3
#
# Structural verifier for DuckDB native compiled regions.
# It checks production architecture invariants, not route-era implementation trivia.

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8", errors="replace")


def existing(globs: tuple[str, ...]) -> list[Path]:
    result: list[Path] = []
    for pattern in globs:
        result.extend(path for path in ROOT.glob(pattern) if path.is_file())
    return sorted(set(result))


def require_file(path: str) -> None:
    if not (ROOT / path).is_file():
        raise AssertionError(f"required file missing: {path}")


def require_absent(path: str) -> None:
    if (ROOT / path).exists():
        raise AssertionError(f"stale file still exists: {path}")


def require_text(path: str, snippets: tuple[str, ...]) -> None:
    data = read(path)
    missing = [snippet for snippet in snippets if snippet not in data]
    if missing:
        raise AssertionError(f"{path}: missing required architecture text {missing}")


def reject_text(path: str, snippets: tuple[str, ...]) -> None:
    data = read(path)
    present = [snippet for snippet in snippets if snippet in data]
    if present:
        raise AssertionError(f"{path}: stale or forbidden text remains {present}")


def reject_scoped_text(path: str, start: str, end: str, snippets: tuple[str, ...]) -> None:
    data = read(path)
    start_idx = data.find(start)
    if start_idx == -1:
        raise AssertionError(f"{path}: missing scope start {start!r}")
    end_idx = data.find(end, start_idx + len(start))
    if end_idx == -1:
        raise AssertionError(f"{path}: missing scope end {end!r}")
    scope = data[start_idx:end_idx]
    present = [snippet for snippet in snippets if snippet in scope]
    if present:
        raise AssertionError(f"{path}: stale or forbidden text remains in scoped block {present}")


def require_scoped_text(path: str, start: str, end: str, snippets: tuple[str, ...]) -> None:
    data = read(path)
    start_idx = data.find(start)
    if start_idx == -1:
        raise AssertionError(f"{path}: missing scope start {start!r}")
    end_idx = data.find(end, start_idx + len(start))
    if end_idx == -1:
        raise AssertionError(f"{path}: missing scope end {end!r}")
    scope = data[start_idx:end_idx]
    missing = [snippet for snippet in snippets if snippet not in scope]
    if missing:
        raise AssertionError(f"{path}: missing required architecture text in scoped block {missing}")


def reject_regex(name: str, patterns: tuple[str, ...], globs: tuple[str, ...], allowed: tuple[str, ...] = ()) -> None:
    allowed_paths = {ROOT / path for path in allowed}
    compiled = [(pattern, re.compile(pattern)) for pattern in patterns]
    for path in existing(globs):
        if path in allowed_paths:
            continue
        data = path.read_text(encoding="utf-8", errors="replace")
        for pattern, regex in compiled:
            if regex.search(data):
                raise AssertionError(f"{rel(path)}: {name}: {pattern}")


def verify_required_design_files() -> None:
    for path in (
        "JIT_ARCHITECTURE.md",
        "benchmark/tpch/jit/JIT_PRODUCTION_RECIPE_DESIGN.md",
        "extension/jit_sljit/include/sljit_runtime_batch_view.hpp",
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_state.hpp",
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        "extension/jit_sljit/include/sljit_full_pipeline_recipe.hpp",
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
        "extension/jit_sljit/include/sljit_full_pipeline_primitive_sequence.hpp",
        "extension/jit_sljit/include/sljit_full_pipeline_primitive_contract.hpp",
        "extension/jit_sljit/include/sljit_full_pipeline_terminal_runtime.hpp",
        "extension/jit_sljit/include/sljit_generated_filter_primitive.hpp",
        "extension/jit_sljit/include/sljit_generated_filter_primitive_runtime.hpp",
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_primitive.hpp",
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        "extension/jit_sljit/include/sljit_mark_probe_filter_boundary.hpp",
        "extension/jit_sljit/include/sljit_mark_probe_filter_boundary_runtime.hpp",
        "extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp",
        "extension/jit_sljit/include/sljit_hash_join_probe_materialize_primitive_runtime.hpp",
        "extension/jit_sljit/include/sljit_hash_join_probe_selection_primitive_runtime.hpp",
        "extension/jit_sljit/include/sljit_hash_join_projection_source_runtime.hpp",
        "extension/jit_sljit/include/sljit_projection_aggregate_descriptor.hpp",
        "extension/jit_sljit/include/sljit_projection_chain_runtime.hpp",
        "extension/jit_sljit/include/sljit_projection_chain_primitive_runtime.hpp",
        "extension/jit_sljit/include/sljit_selected_hash_join_input_runtime.hpp",
        "extension/jit_sljit/include/sljit_source_batch_boundary_runtime.hpp",
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp",
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime_state.hpp",
        "extension/jit_sljit/include/sljit_ungrouped_aggregate_update_primitive.hpp",
        "src/include/duckdb/execution/aggregate_hashtable.hpp",
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        "src/execution/aggregate_hashtable.cpp",
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
    ):
        require_file(path)
    require_absent("extension/jit_sljit/include/sljit_selected_join_aggregate_recipe.hpp")
    require_absent("extension/jit_sljit/include/sljit_distinct_aggregate_update_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_direct_join_output_aggregate_descriptor.hpp")
    require_absent("extension/jit_sljit/include/sljit_deferred_pre_projection_filter_build_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_projected_grouped_aggregate_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_projection_count_star_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_two_join_projection_aggregate_primitive.hpp")
    require_absent("extension/jit_sljit/include/sljit_two_join_drain_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_two_join_drain_helpers_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_two_join_direct_projection_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_two_join_final_projection_aggregate_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_two_join_runtime_state.hpp")
    require_absent("extension/jit_sljit/include/sljit_two_join_layout.hpp")
    require_absent("extension/jit_sljit/include/sljit_between_join_sidecar_state.hpp")
    require_absent("extension/jit_sljit/include/sljit_between_join_sidecar_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_between_join_sidecar_plan_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_between_join_sidecar_plan_common_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_between_join_compressed_group_key_plan_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_between_join_precomputed_payload_plan_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_pre_join_projection_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_decimal64_payload_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_decimal64_payload_state.hpp")
    require_absent("extension/jit_sljit/include/sljit_direct_reference_projection_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_direct_reference_projection_compaction_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_final_projection_aggregate_descriptor.hpp")
    require_absent("extension/jit_sljit/include/sljit_final_projection_aggregate_state.hpp")
    require_absent("extension/jit_sljit/include/sljit_hash_join_projected_aggregate_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_join_projection_aggregate_primitive.hpp")
    require_absent("extension/jit_sljit/include/sljit_aggregate_count_star_preaggregation.hpp")
    require_absent("extension/jit_sljit/include/sljit_direct_projection_fixed_runtime.hpp")
    require_absent("extension/jit_sljit/include/sljit_hash_join_all_valid_probe_runtime.hpp")
    require_absent("extension/jit_sljit/sljit_region_source_filter_plan.cpp")
    require_text(
        "benchmark/tpch/jit/JIT_PRODUCTION_RECIPE_DESIGN.md",
        (
            "Recipe construction is an admission boundary",
            "non-empty executable primitive sequence",
            "construction\nerrors, not executor fallbacks",
            "executor fallbacks",
            "Projection Aggregate Descriptor",
            "direct join-output is a producer mode",
            "selected reference projections can be sliced into a batch view",
            "Consumers of a selected hash-join view must normalize through that producer map",
            "materialize only referenced producer output columns",
            "every group/payload source from producer facts",
            "A descriptor miss is not a CBO bug and must not throw",
            "A projection-fed two-join recipe is also a selected-view consumer",
            "preserve the first join as a selected producer whenever the\nselection primitive can bind",
            "Full first-join output materialization is\nthe generic fallback only for first probes that cannot publish a selected view",
            "descriptor layer must not depend on direct join-output producer state",
            "projection-filter-projection native-tail prefix",
            "Native-tail topology recognition is a facts pass",
            "`FILTER -> PROJECTION -> native tail`",
            "`PROJECTION -> FILTER -> PROJECTION -> native tail`",
            "Hash-join build sinks are native-tail capability boundaries",
            "native hash-build sink protocol penalty must not erase\ngenerated compute-prefix benefit by default",
            "must not keep local route-predicate helpers for each prefix",
            "ProjectionChain\nGeneratedFilter\nProjectionChain\nNativeTailHandoff",
            "flush materializing primitive batches in\nprimitive step order",
            "must not track a single global pending projection or\nmaterialization slot",
            "extend both source fetches and materialized downstream\nbatches by the same computed recipe budget",
            "must not independently extend source fetches or\nindependently clamp terminal row budgets",
            "physical key-input remap",
            "remapped plan/operator-info view",
            "explicit reference-preserving source-column remap",
            "hash-probe key-source",
            "Regular and perfect probe backends both load",
            "preserve cast-overflow semantics before probe filtering",
            "Row-pointer update batching is an explicit aggregate update schedule",
            "expected payloads per group\nexceed the inline per-group payload capacity",
            "`(group,payload)` pair-set backend",
            "distinct pair insertion the explicit backend shape",
            "aggregate input estimate as a monotonic reserve target",
            "projection-chain composition is a primitive binding responsibility",
            "stored in the bound `ProjectionChain` primitive",
            "execution only resolves that bound projection",
            "The recipe builder uses a recipe-pattern registry",
            "Source-batch native-tail execution is a registry\nentry",
            "Projection aggregate\nvariants live in one projection-aggregate registry",
            "Projected grouped aggregate recipes require a dedicated aggregate backend",
            "Source-fetch sink advancement is also a primitive contract",
            "`SourceBatchBoundary` owns advancement for its coalesced source batch",
            "must not be\ninferred from a route or terminal aggregate helper",
            "A scan-filtered source is not, by itself, a reason to insert\nan opaque materialization boundary",
            "DuckDB-owned table-scan filter pushdown stays in the DuckDB scan contract",
            "A scan-filtered primitive aggregate update is real generated/backend work",
            "must not downgrade primitive aggregate updates to weak\naccelerated work just because the source batch came from DuckDB scan filters",
            "high-cardinality batches that cannot fit the\nlocal compact preaggregation table",
            "row-delta path is part of the\ndedicated count-star backend",
            "generated\naggregate update backend is generated/backend work",
            "must not be counted as native\naggregate operator work",
            "`native_aggregate_stage_count` is reserved for actual\nnative aggregate work",
            "Generated backend stages are costed separately from generated expression stages",
            "`generated_backend_stage_count` fact",
            "Exact source cardinality is a runtime contract fact",
            "Physical-pipeline runner cost must use that exact count",
            "row-expanding operator such as a join breaks the cap",
            "Scan-filtered source estimates are already estimates of the source output",
            "NativeTailHandoff",
            "SljitAggregateGroupReservePlan",
            "propagated\ngroup-key distinct reserve facts",
            "source cardinality is available to cap the\nestimate",
            "backend does not reinterpret raw `approx_unique` as an exact group\ncount",
            "ReserveGroups(total_group_count)",
            "one vector of append slack",
            "records the bound reserve target as a JIT runtime counter",
            "once-only flag belongs to the execution-region runtime",
            "not per-call scratch",
            "It must not reserve from `estimated_input_count`, `compact_groups.size()`,\nselected row count",
            "empty MARK probe must populate the selection-vector mark\nflags for every input row",
            "MARK join followed by filter/projection and an arbitrary native sink",
            "MarkProbeFilterBoundary\nProjectionChain*\nNativeTailHandoff",
            "must not materialize full MARK join output before the marker filter",
            "one boolean marker vector only when a downstream consumer still reads marker\n  semantics",
            "downstream projection\nthat does not reference the marker consumes an LHS-only view",
            "`mark_filter_lhs_view`",
            "no `mark_filter_vector` is built",
            "`FILTERED_MARK_MATCHES`",
            "`mark_match_selection_reference`",
            "must not write\nper-input marker flags only to rescan them",
            "Negative marker filters remain a\nseparate null-aware capability",
        ),
    )
    reject_text(
        "benchmark/tpch/jit/JIT_PRODUCTION_RECIPE_DESIGN.md",
        ("regular hash-table probe",),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_runtime.hpp",
        (
            "SljitStopFullPipelineAfterFinalize",
            "SljitRunFullPipelineSourceContractLoopAfterFlush",
            "SljitRunFullPipelineSourceContractLoopAfterFinalize",
            "SljitPrepareSourceChunkAsJoinInput",
            "SljitSourceChunkJoinInput",
            "SljitMaterializedChunkJoinInput",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_projection_executor_runtime.hpp",
        ("SljitPrepareOptionalPreJoinProjectionInput",),
    )


def verify_no_benchmark_shaped_production_logic() -> None:
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


def verify_no_benchmark_shaped_jit_api_tests() -> None:
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
        (
            "test/api/test_jit*.cpp",
            "test/api/test_jit_helpers.hpp",
        ),
    )


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


def verify_no_deprecated_verification_pragmas() -> None:
    reject_regex(
        "deprecated PRAGMA enable_verification",
        (r"\bPRAGMA\s+enable_verification\b",),
        ("benchmark/**/*.py", "benchmark/**/*.sql", "benchmark/**/*.test", "test/**/*.test", "test/**/*.cpp"),
        ("benchmark/jit/verify_jit_architecture.py",),
    )


def verify_stale_route_code_removed() -> None:
    for path in (
        "extension/jit_sljit/include/sljit_full_pipeline_route_kind.hpp",
        "extension/jit_sljit/include/sljit_region_runtime_routes.hpp",
        "extension/jit_sljit/include/sljit_join_aggregate_route_common.hpp",
        "extension/jit_sljit/include/sljit_join_aggregate_route_state.hpp",
        "extension/jit_sljit/include/sljit_mark_join_aggregate_runtime.hpp",
        "extension/jit_sljit/include/sljit_projection_runtime.hpp",
        "extension/jit_sljit/include/sljit_direct_second_join_input_runtime.hpp",
        "extension/jit_sljit/include/sljit_hash_join_all_valid_probe_core_runtime.hpp",
        "extension/jit_sljit/include/sljit_filter_projection_build_runtime.hpp",
    ):
        require_absent(path)
    reject_regex(
        "route-era runtime names",
        (
            r"FullPipelineRoute",
            r"RouteKind",
            r"JoinAggregateRoute",
            r"MarkJoinAggregate",
            r"source_route",
            r"route_local",
            r"HASH_JOIN_PROJECTED_GROUPED_AGGREGATE_UPDATE",
            r"POST_JOIN_PROJECTED_GROUPED_AGGREGATE_UPDATE",
            r"TWO_HASH_JOIN_PROJECTED_GROUPED_AGGREGATE_UPDATE",
            r"SljitFilterProjectionJoinInput",
            r"SljitPrepareFilterProjectionJoinInput",
            r"SljitJoinProjectionAggregateUpdateInputKind",
            r"SljitMakeTwoJoinSourceAggregateUpdatePrimitive",
            r"two_join_source",
            r"TWO_JOIN_SOURCE_INPUT",
            r"sljit_two_join_projection_aggregate_primitive",
            r"sljit_two_join_drain_runtime",
            r"sljit_two_join_drain_helpers_runtime",
            r"sljit_two_join_direct_projection_runtime",
            r"sljit_two_join_final_projection_aggregate_runtime",
            r"sljit_two_join_runtime_state",
            r"sljit_two_join_layout",
            r"sljit_between_join_",
            r"SljitTwoJoinProjectionAggregate",
            r"SljitTwoJoinProjectionAggregateKind::DIRECT_PROJECTION",
            r"SljitTwoJoinProjectionAggregateKind::PROJECTION_CHAIN",
            r"SljitHashJoinHashJoinProjectionLayout",
            r"SljitHashJoinHashJoinProjectionProjectionLayout",
            r"SljitTwoJoinProjectionAggregateSourceKind",
            r"projection_projection",
            r"single_projection",
            r"ExecuteDirectProjection",
            r"FlushDirectProjection",
            r"DirectSecondJoinBoundaries",
            r"direct_row_pointer_reference",
            r"direct_selection_reference",
            r"direct_mark_flags",
        ),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
        ("extension/jit_sljit/include/sljit_region_description.hpp",),
    )
    reject_regex(
        "stale primitive executor helper paths",
        (
            r"\bNextStepTraceOp\b",
            r"\bTryExecuteSelectedReferenceProjection\b",
            r"projected_grouped_selected_reference_view",
            r"\bSljitBuildProjectionChainSemanticProjection\b",
            r"\bSljitTryBuildProjectionChainExpression\b",
            r"\bSljitBuildProjectionChainReferenceSourceMap\b",
            r"\bSljitTryResolveProjectionChainReferenceSource\b",
            r"\bSljitTryRemapHashJoinProjectionExpressionInputSources\b",
            r"\bSljitTryMaterializeSelectedProjectionToBatch\b",
            r"\bSljitTryBuildProjectionInputRowPointerAggregateDescriptor\b",
            r"\bSljitTryBuildProjectionInputRowPointerGroupDescriptor\b",
            r"\bSljitProjectOptionalPostJoinProjectionChain\b",
            r"\bSljitTryMaterializeHashJoinOutputReferenceToBatch\b",
            r"\bSljitTryExecutePreaggregatedCountStarGroupedAggregateUpdate\b",
            r"\bSljitTryBuildProjectionInputVectorGroups\b",
            r"\bSljitProjectionInputVectorGroupSourcesCanMaterialize\b",
            r"\bSljitTryMaterializeProjectionInputVectorGroupSource\b",
            r"\bSljitProjectionCanMaterializeInputVectorGroupSource\b",
            r"\bSljitTryExecuteDistinctCountPointerGroupKeyAggregateUpdate\b",
            r"\bSljitProjectionTargetCanReceiveExpression\b",
            r"\bSljitOrProjectionSkips\b",
            r"\bSljitTryBuildMarkProbeFilterProjectionBoundary\b",
            r"\bSljitDirectProjectionBatchPassthrough\b",
            r"\bSljitFindDirectProjectionBatchPassthrough\b",
            r"\bSljitTryCopyDirectProjectionPassthroughToBatch\b",
            r"\bdirect_batch_passthrough_projection\b",
            r"\bSljitBindNativeTailHandoffInput\b",
            r"\bSljitExecuteNativeTailHandoffIntoSink\b",
            r"\bSljitNativeTailHandoffPrimitive\b",
            r"\bSljitCanBindNativeTailHandoffPrimitive\b",
            r"\bSljitBindNativeTailHandoffPrimitive\b",
            r"\bSljitExecuteNativeTailHandoffBatch\b",
            r"\bSljitBindGeneratedFilterInput\b",
            r"\bSljitBindProjectionChainInput\b",
            r"\bSljitBindGroupedAggregateUpdateInputView\b",
            r"\bSljitBindMaterializedGroupedAggregateUpdateInput\b",
            r"\bSljitJoinProjectionAggregatePrimitive\b",
            r"\bSljitJoinProjectionAggregateSourceKind\b",
        ),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        (
            "BindJoinProjectionAggregatePrimitive",
            "MakeJoinProjectionAggregateRecipe(const SljitJoinProjectionAggregatePrimitive",
            "BindPostJoinProjectionAggregatePrimitive(const SljitJoinProjectionAggregatePrimitive",
            "primitive.input_kind",
            "join-projection aggregate recipe has an unknown input kind",
        ),
    )


def verify_runtime_batch_view() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_runtime_batch_view.hpp",
        (
            "struct SljitRuntimeBatchView",
            "DataChunk *chunk",
            "const SelectionVector *selection",
            "idx_t count",
            "SljitRuntimeBatchOwnership ownership",
            "SljitRuntimeBatchViewFromChunk",
            "SljitBindRuntimeBatchInput",
            "SljitBindMaterializedRuntimeBatchInput",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_projection_source_runtime.hpp",
        (
            "SljitTryMaterializeSelectedHashJoinOutputColumns",
            "const vector<uint8_t> &referenced_columns",
            "SljitSelectedHashJoinSelectionIsIdentity",
            "ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD",
            "referenced_columns[output_col]",
            "vector<idx_t> full_source_map = source_map",
            "source_binding.output_types.size()",
            "full_source_map.push_back(source_idx)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_source_runtime.hpp",
        (
            "TryReadProjectionSourceReferenceIndex",
            "SljitNativeRegionExpressionKind::REFERENCE",
            "ExecutionExpressionIRKind::REFERENCE",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp",
        (
            "struct SljitGroupedAggregateUpdatePrimitive",
            "enum class SljitGroupedAggregateUpdateStrategyKind",
            "SljitChooseGroupedAggregateUpdateStrategy",
            "SljitGroupedAggregateUpdateHasDedicatedBackend",
            "SljitBindGroupedAggregateUpdatePrimitive",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime_state.hpp",
        (
            "SljitGroupedAggregateUpdateRuntimeState",
            "SljitBindGroupedPrimitiveAggregateUpdate",
            "SljitExecuteBoundGroupedPrimitiveAggregateUpdate",
            "SljitBoundGroupedPrimitiveAggregateUpdate bound_direct_update",
            "SljitBoundGroupedPrimitiveAggregateUpdate bound_projected_direct_update",
            "ExecuteCountStarPreaggregation",
        ),
    )
    reject_regex(
        "projected grouped aggregate terminal primitive",
        (
            r"SljitProjectedGroupedAggregateUpdatePrimitive",
            r"SljitBindProjectedGroupedAggregateUpdatePrimitive",
            r"SljitProjectedGroupedAggregateUpdateRuntimeState",
            r"PROJECTED_GROUPED_AGGREGATE_UPDATE",
            r"projected_grouped_reference_view",
            r"projected_grouped_batch_append",
        ),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
    )
    require_text(
        "extension/jit_sljit/include/sljit_direct_join_output_aggregate_runtime.hpp",
        (
            "SljitTryExecuteDirectJoinOutputAggregate",
            "SljitTryMaterializeHashJoinProjectionAggregateInputsToChunk",
            "strategy.last_failure",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_descriptor.hpp",
        (
            "SljitTryBuildProjectionAggregateRequiredOutputs",
            "required_projection_outputs.assign(projection_op.projections.size(), 0)",
            "SljitTryBuildAggregatePayloadSourceIndices",
            "mark_payload_source",
            "mark_direct_payload",
            "optional_ptr<const vector<uint8_t>>(&required_projection_outputs)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_projection_aggregate_input_runtime.hpp",
        (
            "SljitTryMaterializeHashJoinProjectionAggregateInputsToChunk",
            "binding.perfect_layout.rhs_dictionary_buffers",
            "const SelectionVector &build_selection",
            "target.Dictionary(binding.perfect_layout.rhs_dictionary_buffers[rhs_col_idx], build_selection",
            "SljitTryMaterializePerfectHashJoinComputedRHSProjectionToBatch",
            "SljitTryBuildSingleSourceProjectionExpression(source_expr, remapped_reference, join_output_source_index)",
            "SljitProjectionIsSingleSourceReferenceLike(remapped_reference.plan)",
            "binding, remapped_reference.plan, rhs_col_idx",
            "rhs_reference_gather_projection_",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_rhs_projection_runtime.hpp",
        (
            "SljitReferenceProjectionTypesMatch",
            "source_type.InternalType() == PhysicalType::VARCHAR && target_type.InternalType() == PhysicalType::VARCHAR",
            "!SljitReferenceProjectionTypesMatch(plan.return_type, target.GetType())",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_group_key_source.hpp",
        (
            "SljitTryBuildHashJoinOutputVectorGroupKeySource",
            "SljitTryBuildSingleSourceProjectionExpression",
            "join_output_source_index >= binding.output_types.size()",
            "SljitInitializeInputVectorGroupKeySource(join_output_source_index, source_type, group.type",
            "input_vector_repeats_with_row_pointer",
            "ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE",
            "SljitTryFinalizeRowPointerGroupKeySource",
        ),
    )
    require_text(
        "src/include/duckdb/execution/execution_aggregate_runtime.hpp",
        ("input_vector_repeats_with_row_pointer",),
    )
    reject_regex(
        "batch-local distinct count-pointer payload strategy selection",
        (
            r"\bSljitSelectDistinctCountPointerPayloadStorageStrategy\b",
            r"MaxValue\(op\.aggregate_update\.plan\.estimated_input_count,\s*count\)",
            r"payload_set_target\s*=\s*estimated_payload_count",
        ),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
    )
    require_text(
        "extension/jit_sljit/sljit_executable_stats.cpp",
        (
            "BuildSljitHashJoinProbeOutputDistinctCounts",
            "contract.lhs_output_column_indices",
            "SljitNativeRegionOpKind::HASH_JOIN_PROBE",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_executable_range_stats.cpp",
        (
            "BuildSljitHashJoinProbeOutputRanges",
            "contract.lhs_output_column_indices",
            "SljitNativeRegionOpKind::HASH_JOIN_PROBE",
        ),
    )
    require_text(
        "src/execution/aggregate_hashtable.cpp",
        (
            "AggregateDescriptorSourcesRepeatWithRowPointer",
            "source.input_vector_repeats_with_row_pointer",
            "AggregateDescriptorSourcesCanMaterialize",
            "AggregateDescriptorSourcesCanUseDirectTargets",
            "AggregateDescriptorSourcesRepeatBySourceIndex",
            "format.sel->get_index(row_idx) == format.sel->get_index(other_row_idx)",
            "can_reuse_row_pointer_hashes",
            "can_reuse_source_index_hashes",
            "descriptor_sources_repeat_with_row_pointer",
            "bool use_consecutive_reuse = descriptor_sources_repeat_with_row_pointer",
            "compact_row_pointer_groups",
            "TryFindOrCreateDescriptorGroupStateTargetsDirect(compact_input,",
            "TryFindOrCreateInputVectorGroupStateTargetsFast",
            "AggregateRebuildDenseSingleFieldTargetCacheFromData",
            "find_or_create_input_vector_dense.cache_rebuild",
            "optional_ptr<const ExecutionDenseGroupDomain> dense_domain",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp",
        (
            "SljitExecuteEmptyHashJoinProbe",
            "SljitHashJoinProbeOutputContract",
            "MATERIALIZED_OUTPUT",
            "SELECTED_VIEW",
            "FILTERED_MARK_MATCHES",
            "FILTERED_MARK_NON_MATCHES",
            "SljitHashJoinProbeOutputIsFilteredMarkMatches",
            "SljitHashJoinProbeOutputIsFilteredMarkNonMatches",
            "SljitHashJoinMarkSelectionModeForOutputContract",
            "SljitHashJoinProbeProducesSelectedView(output_contract)",
            "ExecutionHashJoinProbeOutputMode::MARK_PROBE",
            "match_selection.set_index(row_idx, 0)",
            "\"row_pointer_selection_reference\"",
            "\"perfect_selection_reference\"",
            "\"mark_flags\"",
            "\"mark_match_selection_reference\"",
            "\"mark_nonmatch_selection_reference\"",
            "\"mark_nonmatch_empty_due_to_build_null\"",
            "!probe.hash_table->has_null",
            "state, output_contract",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_loop_codegen.hpp",
        (
            "EmitJumpIfRegularHashJoinBloomMiss",
            "bloom_filter_bits",
            "bloom_filter_bitmask",
            "EmitBloomFilterMask",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_regular_key_codegen.hpp",
        (
            "SljitRegularHashJoinProbeHashJumps",
            "config.UsesBloomFilter()",
            "EmitJumpIfRegularHashJoinBloomMiss",
            "jumps.source_is_null.push_back",
            "jumps.no_match.push_back",
            "sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R3",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_region_executable.hpp",
        (
            "bloom_code",
            "bloom_function",
            "mark_match_selection_code",
            "mark_match_selection_bloom_code",
            "mark_match_all_valid_specializations",
            "MarkMatchAllValidSpecializationFor",
            "mark_nonmatch_selection_code",
            "mark_nonmatch_selection_bloom_code",
            "mark_nonmatch_all_valid_specializations",
            "MarkNonMatchAllValidSpecializationFor",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_path_runtime.hpp",
        (
            "uses_bloom_filter",
            "regular.bloom_function",
            "mark_selection_mode",
            "rhs_keys_all_valid",
            "mark_match_selection_function",
            "mark_nonmatch_selection_function",
            "EnsureAllValidRegularHashJoinProbeCode(runtime, hash_join_probe, key, mark_selection_mode)",
            "SljitExecuteAllValidRegularHashJoinMarkSelectionProbePath",
            "SljitGeneratedAllValidRegularHashJoinProbeStage(SELECTED, mark_selection_mode)",
            "SljitGeneratedRegularHashJoinProbeStage(uses_bloom_filter, mark_selection_mode)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_mark_probe_filter_boundary.hpp",
        (
            "SljitMarkProbeFilterBoundaryMarkerMode::OMIT_MARKER",
            "SljitMarkProbeFilterBoundaryOmitsMarker",
            "allow_marker_omission && !projection_reads_marker",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_primitive.hpp",
        (
            "bool allow_marker_omission = false",
            "marker omission requires an applied filter",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_mark_probe_filter_boundary_runtime.hpp",
        (
            "class SljitMarkProbeFilterBoundaryRuntime",
            "BoundaryOutputChunk",
            "lhs_boundary_outputs",
            "lhs_boundary_output_types",
            "selected_hash_join_inputs.TryPrepareMarkProbeInput",
            "SljitHashJoinProbeOutputContract::FILTERED_MARK_MATCHES",
            "SljitHashJoinProbeOutputContract::FILTERED_MARK_NON_MATCHES",
            "\"direct_mark_probe_match_selection\"",
            "\"direct_mark_probe_nonmatch_selection\"",
            "\"mark_filter_lhs_selected_view\"",
            "\"mark_filter_lhs_view\"",
            "\"mark_filter_vector\"",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "const auto max_recipe_batches = recipe.uses_extended_source_fetch_budget",
            "terminal_runtime.BudgetReached(runtime, TerminalStep(), max_recipe_batches)",
            "fetched_chunks >= max_recipe_batches",
            "SljitMarkProbeFilterBoundaryRuntime mark_probe_filter_boundary",
            "mark_probe_filter_boundary.Execute(step_idx, step, input, execute_hash_join_probe, execute_next_step)",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "MarkProbeFilterBoundaryOutputChunk",
            "mark_probe_lhs_boundary_outputs",
            "mark_probe_lhs_boundary_output_types",
            "SljitHashJoinProbeOutputContract::FILTERED_MARK_MATCHES",
            "SljitHashJoinProbeOutputContract::FILTERED_MARK_NON_MATCHES",
            "\"direct_mark_probe_match_selection\"",
            "\"direct_mark_probe_nonmatch_selection\"",
            "\"mark_filter_lhs_selected_view\"",
            "\"mark_filter_lhs_view\"",
            "\"mark_filter_vector\"",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());",
            "recipe.uses_extended_source_fetch_budget ? max_source_fetches : runtime.MaxChunks()",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_terminal_runtime.hpp",
        ("BudgetReached(ExecutionRegionRuntime &runtime, const SljitFullPipelinePrimitiveStep &terminal_step,\n"
         "\t                   idx_t max_recipe_batches)",),
    )
    require_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        ("selected_join_output.BudgetReached(runtime, max_recipe_batches)",),
    )
    require_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        (
            "SljitDownstreamRowBudgetReached(processed_output_rows, max_recipe_batches)",
            "SljitDataChunkBatch projected_batch",
            "processed_output_rows, projected_batch",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projected_grouped_aggregate_sink.hpp",
        (
            "SljitDataChunkBatch &projected_batch",
            "projected_batch.Ensure(runtime.GetAllocator(), projection_op.output_types)",
            "SljitAppendChunkToInitializedBatch",
            "SljitFlushDataChunkBatch(projected_batch.chunk",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_projected_grouped_aggregate_sink.hpp",
        (
            "SljitProjectedGroupedAggregateProgressMode",
            "SOURCE_FETCHES",
            "RunSourceLoop",
            "BudgetReached() const",
        ),
    )
    reject_regex(
        "global source-contract output batch",
        (
            r"\bPendingSourceContractBatch\b",
            r"\bPrepareSourceContractBatch\b",
            r"\bResetSourceContractBatch\b",
            r"\bexecution_source_output_batch\b",
            r"\bSljitFlushRuntimePendingBatch\b",
            r"\bSljitAppendChunkToRuntimeBatch\b",
            r"\bSljitTryAppendDirectChunkToRuntimeBatch\b",
        ),
        (
            "src/**/*.cpp",
            "src/**/*.hpp",
            "extension/jit_sljit/**/*.cpp",
            "extension/jit_sljit/**/*.hpp",
        ),
    )
    require_text(
        "test/api/test_jit_join.cpp",
        (
            "JIT mark filter projection native tail uses boundary primitive",
            "JIT first hash join native tail uses source batch boundary recipe",
            "JIT mark match filter emits selected boundary without marker flags",
            "hash_join_probe.source_batch_boundary=",
            "hash_join_probe.row_pointer_selection_reference=",
            "hash_join_probe.mark_flags=",
            "hash_join_probe.mark_match_selection_reference=",
            "hash_join_probe.mark_nonmatch_selection_reference=",
            "hash_join_probe.mark_filter_lhs_selected_view=",
            "ContainsMarkMatchProbePath",
            "ContainsMarkNonmatchProbePath",
            "hash_join_probe.fast_regular_probe_mark_match",
            "hash_join_probe.generated_regular_probe_mark_match",
            "hash_join_probe.fast_regular_probe_mark_nonmatch",
            "hash_join_probe.generated_regular_probe_mark_nonmatch",
            "filter.direct_mark_probe_match_selection=",
            "filter.direct_mark_probe_nonmatch_selection=",
            "REQUIRE_FALSE(StringUtil::Contains(boundaries, \"hash_join_probe.final_output=\"));",
            "REQUIRE_FALSE(StringUtil::Contains(boundaries, \"hash_join_probe.mark_flags=\"));",
            "REQUIRE_FALSE(StringUtil::Contains(boundaries, \"hash_join_probe.mark_filter_vector=\"));",
        ),
    )


def verify_runtime_proof_ownership() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_runtime_batch_view.hpp",
        (
            "bool source_key0_int64_to_int32_matches_are_proven",
            "SljitRuntimeBatchViewFromHashJoinSelection",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_direct_join_output_aggregate_state.hpp",
        (
            "struct SljitPendingRowPointerAggregateBatch",
            "bool source_key0_int64_to_int32_unchecked = false",
        ),
    )
    reject_regex(
        "post-join aggregate primitive owns source-key proof",
        (r"(?s)struct\s+SljitPostJoinProjectionAggregatePrimitive\s*\{[^}]*source_key0_int64_to_int32",),
        ("extension/jit_sljit/include/sljit_join_projection_aggregate_update_primitive.hpp",),
    )
    reject_regex(
        "direct aggregate strategy owns pending-batch source-key proof",
        (r"(?s)struct\s+SljitDirectJoinOutputAggregateStrategy\s*\{[^}]*source_key0_int64_to_int32",),
        ("extension/jit_sljit/include/sljit_direct_join_output_aggregate_state.hpp",),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        ("source_key0_int64_to_int32_unchecked || source_key0_int64_to_int32_matches_are_proven",),
    )


def verify_hash_probe_key_source_contract() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_native_types.hpp",
        (
            "struct SljitNativePerfectHashJoinProbeInput",
            "bool source_key0_int64_to_int32 = false",
            "bool source_key0_int64_to_int32_unchecked = false",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_hash_join_runtime.cpp",
        (
            "perfect_layout.key_physical_type == PhysicalType::INT32",
            "UnifiedVectorFormat::GetData<int64_t>",
            "native_input.source_key0_int64_to_int32 = source_key0_int64_to_int32",
            "native_input.source_key0_int64_to_int32_unchecked =",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_hash_join_probe_perfect_codegen.cpp",
        (
            "offsetof(SljitNativePerfectHashJoinProbeInput, source_key0_int64_to_int32)",
            "offsetof(SljitNativePerfectHashJoinProbeInput, source_key0_int64_to_int32_unchecked)",
            "EmitCheckedPerfectHashJoinInt64ToInt32Range",
            "EmitAbortPerfectHashJoinProbeWithCastError",
            "SLJIT_MEM2(SLJIT_S4, SLJIT_R1), 3",
            "EmitLoadHashJoinKey(compiler, key.key_kind",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp",
        ("native_input.source_key0_int64_to_int32",),
    )
    reject_regex(
        "regular-only remapped hash probe contract",
        (
            r"requires_regular_hash_probe",
            r"hash_probe_key_input_remap_requires_regular",
            r"regular hash-table probe",
        ),
        (
            "extension/jit_sljit/**/*.cpp",
            "extension/jit_sljit/**/*.hpp",
        ),
    )


def verify_projection_aggregate_descriptor_boundary() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_descriptor.hpp",
        (
            "SljitTryBuildPostJoinProjectionAggregateDescriptor",
            "post_join_projection.first_projection_idx == post_join_projection.final_projection_idx",
            "SljitTryBuildProjectionChainAggregateDescriptor",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_descriptor.hpp",
        (
            "sljit_direct_join_output_aggregate_state.hpp",
            "SljitDirectJoinOutputAggregateStrategy",
            "SljitDirectJoinOutputAggregateMode",
            "SljitTryBuildJoinProjectionAggregateDescriptorForDirectOutput",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_direct_join_output_aggregate_state.hpp",
        (
            "struct SljitPendingRowPointerAggregateBatch",
            "SljitJoinProjectionAggregateDescriptor descriptor",
            "SljitPendingRowPointerAggregateBatch pending_batch",
            "SljitAggregateUpdateHasDedicatedCompiledBackend",
            "op.aggregate_update.plan.use_primitive_payloads",
            "SljitAggregateUpdateHasDedicatedCompiledBackend(ops[aggregate_idx])",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_direct_join_output_aggregate_state.hpp",
        (
            "enum class SljitDirectJoinOutputAggregateUpdateSchedule",
            "PENDING_ROW_POINTER_BATCH",
            "IMMEDIATE_ROW_POINTER_UPDATE",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_recipe.hpp",
        (
            "DirectJoinProjectionAggregateHasDedicatedBackend",
            "!DirectJoinProjectionAggregateHasDedicatedBackend(shape)",
        ),
    )
    reject_regex(
        "projection composition encoded as aggregate mode",
        (r"\bSljitDirectJoinOutputAggregateMode\b",),
        (
            "extension/jit_sljit/**/*.hpp",
            "extension/jit_sljit/**/*.cpp",
        ),
    )


def verify_cost_fact_ownership_boundary() -> None:
    require_text(
        "src/execution/execution_region_cost_input.cpp",
        (
            "RemoveExecutionRegionGeneratedAggregateUpdateNativeCost",
            "mark_probe_filter_count",
            "mark_probe_materialized_tail_count",
            "ExecutionRegionMarkProbeMaterializedTailCostCount",
            "result.generated_backend_stage_count += candidate.traits.mark_probe_filter_count",
            "traits.generated_aggregate_update_count",
            "generated_backend_stage_count",
            "facts.native_aggregate_stage_count -= aggregate_decrement",
            "facts.native_grouped_aggregate_stage_count -= grouped_decrement",
            "idx_t generated_aggregate_update_count = 0",
            "facts.generated_aggregate_update_count++",
            "cost_input.native_aggregate_stage_count > 0 || facts.generated_aggregate_update_count > 0",
            "exact_source_cardinality_bounds_pipeline",
            "contract.source.estimated_source_cardinality_exact",
            "cost_input.estimated_cardinality = contract.source.estimated_source_cardinality",
        ),
    )
    require_text(
        "src/execution/execution_region_ir.cpp",
        (
            "ExecutionRegionFilterReadsMarkProbeMarker",
            "ExecutionRegionNodeIsMarkHashJoinProbe",
            "native_probe_output_mode ==",
            "ExecutionHashJoinProbeOutputMode::MARK_PROBE",
            "traits.mark_probe_filter_count++",
        ),
    )
    require_text(
        "src/execution/execution_region_description.cpp",
        (
            "mark_probe_filters=",
            "mark_probe_materialized_tails=",
        ),
    )
    require_text(
        "src/planner/cost_model.cpp",
        (
            "PhysicalRunnerMaterializationElisionBenefitCanPay",
            "PhysicalRunnerGeneratedBackendStageBenefitCanPay",
            "PhysicalRunnerHasGeneratedComputePrefix",
            "PhysicalRunnerHashJoinBuildSinkProtocolPenaltyApplies",
            "PhysicalRunnerCostedGeneratedBackendStageCount",
            "input.native_hash_join_build_sink_count > 0",
            "generated_backend_stage_count",
            "generated_expression_stage_count",
            "generated_backend_stage_work",
            "profile.generated_stage_work = AddCost(generated_expression_stage_work, profile.generated_backend_stage_work)",
        ),
    )
    reject_text(
        "src/planner/cost_model.cpp",
        (
            "for (idx_t filter_idx = 0; filter_idx < filter_count; filter_idx++)",
            "rows = MaxValue<idx_t>((rows + 9) / 10, 1)",
        ),
    )
    require_text(
        "src/execution/execution_region_decision.cpp",
        (
            "PhysicalPipelineHashJoinBuildNeedsRegionGraph",
            "cost_input.native_hash_join_build_sink_count > 0",
            "cost_input.generated_work_class != PhysicalRunnerGeneratedWorkClass::NONE",
            "cost_input.generated_work_class != PhysicalRunnerGeneratedWorkClass::PROJECTION_GLUE",
            "duckdb_cbo requires execution-region graph for hash-join build sink decision",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_region_source_plan.cpp",
        (
            "vectorized table scan filters;source-strategy=duckdb-scan-filtered-source-contract",
            "source_contract_filter_pushdown=true",
            "SljitDuckDBScanFilteredSourceContractPlan()",
        ),
    )
    reject_text(
        "extension/jit_sljit/sljit_region_source_plan.cpp",
        (
            "generated table scan source filters",
            "TryPlanSljitSourceFilters",
            "generated_source_filter_blocker",
            "requires_source_contract_input_layout",
        ),
    )
    reject_text(
        "extension/jit_sljit/sljit_region_plan_facts.cpp",
        ("SljitRegionPlanHasScanFilteredAggregateTerminal",),
    )
    reject_text(
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
        (
            "HashAggregateCanPlanDistinctCountPointerKeys",
            "ExecutionRegionSettings::Enabled(context)",
            "ExecutionRegionSettings::Policy(context) != ExecutionRegionPolicyMode::OFF",
        ),
    )
    reject_text(
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
        (
            "HashAggregateCanPlanJitDistinctCountPointerKeys",
            "HashAggregateCanPlanDistinctCountPointerKeys(ClientContext &context) {\n"
            "\treturn ExecutionRegionSettings::Enabled(context) &&\n"
            "\t       ExecutionRegionSettings::Policy(context) != ExecutionRegionPolicyMode::OFF &&\n"
            "\t       ExecutionRegionSettings::ShouldRecordDetailedTelemetry(context);",
        ),
    )
    require_text(
        "src/function/table/system/execution_region_table_function_utils.hpp",
        (
            "runner_cost_generated_backend_stage_count",
            "runner_cost_generated_backend_stage_work",
        ),
    )


def verify_preaggregated_primitive_batch_contract() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_region_adapter_scratch.hpp",
        (
            "vector<idx_t> group_row_counts",
            "CanSlicePreaggregatedPrimitiveScratch",
            "SlicePreaggregatedPrimitiveScratch",
            "PreaggregatedPrimitiveRepresentedRowCount",
            "target_payload.value_is_set.insert",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_aggregate_primitive_preaggregation_runtime.hpp",
        (
            "scratch.group_row_counts.push_back(0)",
            "scratch.group_row_counts[group_idx]++",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_aggregate_row_pointer_preaggregation.hpp",
        (
            "enum class SljitRowPointerPreaggregationStrategy",
            "struct SljitRowPointerPreaggregationDecision",
            "SljitChooseRowPointerPreaggregationStrategy",
            "UseConsecutiveGroups()",
            "SljitTryCollectRowPointerPreaggregationSampleStats",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_aggregate_row_pointer_preaggregation.hpp",
        (
            "SljitRowPointerDescriptorsHaveConsecutiveRepeat",
            "SljitRowPointerPreaggregationProfitableOnSample",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_row_pointer_grouped_aggregate_update_runtime.hpp",
        (
            "enum class SljitRowPointerGroupedAggregateUpdateStrategy",
            "struct SljitRowPointerGroupedAggregateUpdateDecision",
            "SljitChooseRowPointerGroupedAggregateUpdateStrategies",
            "SljitTryExecuteRowPointerGroupedAggregateUpdateStrategy",
            "TryExecuteDirectRowPointerPreaggregatedPrimitiveUpdate",
            "SljitTryExecuteInputVectorGroupedAggregateUpdate",
            "TryFindOrCreateInputVectorGroupStateTargets",
            "targets, recorder, dense_domain",
            "direct_input_vector_group_count_one_lookup",
            "direct_input_vector_group_count_one_update",
            "TARGET_PAYLOAD_UPDATE",
            "SPLIT_PAYLOAD_UPDATE",
            "uses_generated_payload_preaggregation",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_row_pointer_grouped_aggregate_update_runtime.hpp",
        (
            "SljitTryExecuteRowPointerPreaggregatedGroupedAggregateUpdateStrategy",
            "FUSED_TARGET_PAYLOAD",
            "prefer_direct_sparse_row_pointer_target_update",
            "(void)dense_domain;",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_aggregate_flat_single_preaggregation.hpp",
        (
            "scratch.group_row_counts.push_back(0)",
            "scratch.group_row_counts[group_count - 1]++",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_direct_update_runtime.hpp",
        (
            "TryExecutePreaggregatedGroupedPrimitiveAggregateUpdateBatches",
            "TryExecutePreaggregatedGroupedPrimitiveAppendSuffixWithPrefixUpdate",
            "total_represented_row_count != preaggregated_row_count",
            "preaggregated_primitive_group_batches",
            "preaggregated_suffix_append_prefix_update",
            "AggregatePreaggregatedGroupSlice",
            "AggregatePreaggregateScratchSlice",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime.hpp",
        ("reserve_group_capacity",),
    )


def verify_perfect_hash_aggregate_capability_contract() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_region_aggregate_payload_fusion.hpp",
        (
            "SljitGroupedStateAddressPayloadsSupported",
            "TryBuildSljitPerfectHashGroupPlans",
            "contract.perfect_required_bits_total >= 8 * sizeof(idx_t)",
            "SljitFusedGroupedPrimitiveAggregatePayloadSupported",
            "BuildSljitFusedTypedAggregateCodegenPlan",
            "SljitFusedGroupedTypedAggregatePayloadSupported",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_region_aggregate_partial_fusion.cpp",
        (
            "!use_perfect_hash_group_lookup && !SljitGroupedStateAddressPayloadsSupported",
            "use_perfect_hash_group_lookup = SljitPerfectHashGroupLookupSupported",
            "grouped_state_lookup=native-state-address",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_region_aggregate_payload_fusion.hpp",
        ("AggregatePrimitiveUpdateRequiresPayload(aggregate.primitive_update_kind)",),
    )


def verify_regular_hash_aggregate_lookup_contract() -> None:
    require_text(
        "benchmark/tpch/jit/JIT_PRODUCTION_RECIPE_DESIGN.md",
        (
            "Regular hash aggregate lookup must not be modeled as a generated backend stage",
            "grouped state-address resolution plus generated primitive payload update",
            "Perfect hash",
        ),
    )
    reject_regex(
        "stale regular hash aggregate generated-lookup contract",
        (
            "native_hash_aggregate_lookup",
            "hash_lookup_layout",
            "hash_aggregate_lookup_",
            "blocked_hash_aggregate_lookup_count",
            "ExecutionHashAggregateLookupLayout",
            "GetExecutionHashAggregateLookupLayout",
        ),
        (
            "src/**/*.cpp",
            "src/include/duckdb/**/*.hpp",
            "extension/jit_sljit/**/*.cpp",
            "extension/jit_sljit/**/*.hpp",
            "test/**/*.cpp",
            "test/**/*.test",
        ),
    )


def verify_row_pointer_grouped_lookup_contract() -> None:
    require_text(
        "src/include/duckdb/execution/aggregate_hashtable.hpp",
        (
            "descriptor_group_hashes",
        ),
    )
    require_text(
        "src/execution/aggregate_hashtable.cpp",
        (
            "AggregateFillCompactDescriptorGroupChunk",
            "AggregateFillCompactRowPointerFieldDescriptorGroupChunk",
            "source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE",
            "state.descriptor_group_hashes",
            "AggregateDescriptorSourcesUseOnlyRowPointerFields(group_sources)",
            "AggregateDescriptorSourcesAreAllValid(group_sources)",
            "descriptor_sources_are_all_valid",
            "AggregateDescriptorSourceRowMatchesStored(sources, group_sources, *layout_ptr, row_location",
        ),
    )
    reject_text(
        "src/execution/aggregate_hashtable.cpp",
        (
            "AggregateRowPointerIdentityTargetCacheUseful",
            "IDENTITY_TARGET_CACHE_MIN_REUSE_NUMERATOR",
            "IDENTITY_TARGET_CACHE_MIN_REUSE_DENOMINATOR",
            "row_pointer_target_cache",
            "RowPointerIdentityTargetCacheKind",
            "try_get_row_pointer_identity_target",
            "set_row_pointer_identity_target",
            "RowPointerIdentityTargetCacheKind::NEW_GROUP",
        ),
    )


def verify_executable_source_fact_contract() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_region_executable.hpp",
        (
            "vector<bool> input_source_not_null",
            "SljitInputSourceKnownNotNull",
            "SljitSourceKnownNotNull",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_executable_expression_inputs.cpp",
        (
            "PopulateSljitExecutableInputSourceFacts",
            "expr.input_source_not_null = local_source_not_null",
            "PopulateSljitExecutableInputSourceFacts(expr.input_source_indices, expr.input_source_not_null",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_executable_aggregate_codegen.cpp",
        (
            "PopulateSljitExecutableAggregatePayloadSourceFacts",
            "payload.input_source_not_null.push_back(SljitSourceKnownNotNull",
            "payload.input_source_not_null = payloads.combined_source_not_null",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_executable_aggregate_filtered_codegen.cpp",
        (
            "filtered_update.input_source_not_null = combined_source_not_null",
            "payload.input_source_not_null = combined_source_not_null",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_region_runtime_source.hpp",
        (
            "bool source_known_not_null = false",
            "PrepareDictionarySource",
            "PrepareConstantSource",
            "PrepareTypedExpressionSource",
            "PrepareIntegerSource",
            "PrepareValiditySource",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_region_runtime_source.cpp",
        (
            "SljitDirectSourceValidityData",
            "source_known_not_null || validity.CannotHaveNull()",
            "execute_sel ? execute_sel->get_index(row_idx) : row_idx",
            "PrepareDictionarySource",
            "PrepareConstantSource",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_aggregate_payload_lane_runtime.hpp",
        (
            "const vector<bool> *source_not_null = nullptr",
            "SljitInputSourceKnownNotNull(*source_not_null, source_idx)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_aggregate_filtered_payload_runtime.hpp",
        ("&filtered_update.input_source_not_null",),
    )
    require_text(
        "extension/jit_sljit/include/sljit_aggregate_primitive_payload_runtime.hpp",
        (
            "SljitInputSourceKnownNotNull(payload.input_source_not_null",
            "SljitNormalizedSourceValidityData(source_format",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_aggregate_primitive_payload_runtime.hpp",
        ("source_format.validity.GetData()", "right_source_format.validity.GetData()"),
    )


def verify_primitive_sequence() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_primitive_sequence.hpp",
        (
            "enum class SljitFullPipelinePrimitiveKind",
            "SOURCE_FETCH",
            "SOURCE_BATCH_BOUNDARY",
            "GENERATED_FILTER",
            "HASH_JOIN_PROBE_MATERIALIZE",
            "HASH_JOIN_PROBE_SELECTION",
            "MARK_PROBE_FILTER_BOUNDARY",
            "PROJECTION_CHAIN",
            "JOIN_PROJECTION_AGGREGATE_UPDATE",
            "UNGROUPED_AGGREGATE_UPDATE",
            "GROUPED_AGGREGATE_UPDATE",
            "NATIVE_TAIL_HANDOFF",
            "SljitGeneratedFilterPrimitive generated_filter",
            "SljitHashJoinProbeSelectionPrimitive hash_join_probe_selection",
            "SljitProjectionChainPrimitive projection_chain",
            "SljitJoinProjectionAggregateUpdatePrimitive join_projection_aggregate_update",
            "SljitUngroupedAggregateUpdatePrimitive ungrouped_aggregate_update",
            "SljitGroupedAggregateUpdatePrimitive grouped_aggregate_update",
            "SourceBatchBoundary(idx_t op_idx)",
            "GeneratedFilter(const SljitGeneratedFilterPrimitive &primitive)",
            "HashJoinProbeSelection(const SljitHashJoinProbeSelectionPrimitive &primitive)",
            "ProjectionChain(const SljitProjectionChainPrimitive &primitive)",
            "JoinProjectionAggregateUpdate(const SljitJoinProjectionAggregateUpdatePrimitive &primitive)",
            "UngroupedAggregateUpdate(const SljitUngroupedAggregateUpdatePrimitive &primitive)",
            "GroupedAggregateUpdate(const SljitGroupedAggregateUpdatePrimitive &primitive)",
            "step.op_count >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVE_STEP_OPS",
            "exceeds the maximum operator count",
            "vector<SljitFullPipelinePrimitiveStep> steps",
            "Count() >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES",
            "idx_t Count() const",
            "const SljitFullPipelinePrimitiveStep &Step(idx_t step_idx) const",
            "exceeds the maximum step count",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_primitive_contract.hpp",
        (
            "SljitFullPipelineSourcePrimitiveIsExecutable",
            "SljitFullPipelineIntermediatePrimitiveIsExecutable",
            "SljitFullPipelineTerminalPrimitiveIsExecutable",
            "SljitFullPipelinePrimitiveSequenceIsExecutable",
            "SljitFullPipelinePrimitiveSequenceTerminalStep",
            "SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE",
            "SljitFullPipelinePrimitiveKind::UNGROUPED_AGGREGATE_UPDATE",
            "SljitCanBindUngroupedAggregateUpdatePrimitive",
            "step.ungrouped_aggregate_update",
            "SljitCanBindGroupedAggregateUpdatePrimitive",
            "step.grouped_aggregate_update",
            "SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF",
            "SljitNativeTailHandoffCanConsumeTail",
            "tail.aggregate_update.plan.use_primitive_payloads",
            "SljitNativeTailHandoffCanConsumeTail(ops, step.Op(0))",
            "step.generated_filter.filter_idx == step.Op(0)",
            "step.projection_chain.first_projection_idx == step.Op(0)",
            "step.projection_chain.final_projection_idx == step.Op(1)",
            "SljitCanBindProjectionChainPrimitive(ops, step.projection_chain.first_projection_idx",
            "SljitFullPipelinePrimitiveOwnsSourceBatchAdvance",
            "SljitFullPipelineSourceFetchOwnsSinkAdvance",
            "source-fetch sink ownership requires an executable primitive sequence",
            "SljitFullPipelinePrimitiveKind::SOURCE_BATCH_BOUNDARY",
            "return true",
            "SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION",
            "SljitFullPipelinePrimitiveKind::JOIN_PROJECTION_AGGREGATE_UPDATE",
            "SljitCanBindHashJoinProbeSelectionPrimitive",
            "SljitCanBindJoinProjectionAggregateUpdatePrimitive",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_primitive_contract.hpp",
        ("SljitFullPipelinePrimitiveConsumesSourceSinkAdvance",),
    )
    require_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_primitive.hpp",
        (
            "struct SljitJoinProjectionAggregateUpdatePrimitive",
            "SljitPostJoinProjectionAggregatePrimitive selected_join_output",
            "SljitMakeSelectedJoinOutputAggregateUpdatePrimitive",
            "SljitCanBindJoinProjectionAggregateUpdatePrimitive",
            "SljitCanBindPostJoinProjectionAggregatePrimitive(ops, primitive.selected_join_output)",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_primitive.hpp",
        ("ConsumesSourceSinkAdvance",),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_chain_runtime.hpp",
        (
            "SljitTryPrepareSelectedHashJoinProjectionChainInput",
            "input.hash_join_output_column_map",
            "SljitTryBuildHashJoinMappedProjection",
            "SljitBuildProjectionSourceColumnSet",
            "SljitTryMaterializeSelectedHashJoinOutputColumns",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_selected_hash_join_input_runtime.hpp",
        (
            "class SljitSelectedHashJoinInputRuntime",
            "TryPrepareMarkProbeInput",
            "TryPrepareHashProbeInput",
            "TryPrepareInput",
            "MarkTargetProbeInputColumns",
            "SljitTryMaterializeSelectedHashJoinOutputColumns",
            "selected_hash_join_mark_input",
            "selected_hash_join_probe_input",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "SljitSelectedHashJoinInputRuntime selected_hash_join_inputs",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_mark_probe_filter_boundary_runtime.hpp",
        (
            "selected_hash_join_inputs.TryPrepareMarkProbeInput",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_mark_probe_filter_boundary_runtime.hpp",
        (
            "selected_hash_join_inputs.BuildMarkOutputView",
            "ExecutePreservedSelectedHashJoinBoundary",
            "preserve_selected_hash_join",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_selected_hash_join_input_runtime.hpp",
        (
            "BuildMarkOutputView",
            "selected_mark_source_selection",
            "selected_mark_build_selection",
            "selected_mark_row_pointers",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "TryPrepareSelectedHashJoinOutputForMarkProbeInput",
            "TryPrepareSelectedHashJoinOutputForHashProbeInput",
            "BuildSelectedHashJoinMarkOutputView",
            "selected_hash_join_mark_input",
            "selected_hash_join_probe_input",
            "selected_mark_source_selection",
            "selected_mark_build_selection",
            "selected_mark_row_pointers",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
        (
            "enum class SljitProjectionAggregatePrefixKind",
            "SljitProjectionAggregatePrefixKind Kind() const",
            "SljitProjectionAggregatePrefixKind::SOURCE",
            "SljitProjectionAggregatePrefixKind::SINGLE_JOIN",
            "SljitProjectionAggregatePrefixKind::TWO_JOIN",
            "struct SljitMarkFilterProjectionNativeTailFacts",
            "struct SljitGeneratedFilterProjectionNativeTailFacts",
            "struct SljitProjectionFilterProjectionNativeTailFacts",
            "SljitTryAnalyzeMarkFilterProjectionNativeTail",
            "SljitTryAnalyzeGeneratedFilterProjectionNativeTail",
            "SljitTryAnalyzeProjectionFilterProjectionNativeTail",
            "SljitIsMarkProbeMarkerFilter(ops[0], ops[1])",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
        (
            "SljitProjectionAggregateCanUseSourceCountStarGroupedAggregate",
            "SljitProjectionAggregateCanUseSingleJoinMarkFilterBoundary",
            "SljitProjectionAggregateCanUseSingleJoinSourceFilterProjection",
            "SljitProjectionAggregateCanUseSingleJoinPreJoinProjection",
            "SljitProjectionAggregateCanUseSingleJoinSingleProjection",
            "SljitProjectionAggregateCanUseSingleJoinProjectionChain",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_recipe.hpp",
        (
            "const auto prefix_kind = plan.prefix.Kind()",
            "SljitProjectionAggregatePrefixKind prefix_kind",
            "registry[entry_idx].prefix_kind != prefix_kind",
            "SljitProjectionAggregatePrefixKind::SOURCE",
            "SljitProjectionAggregatePrefixKind::SINGLE_JOIN",
            "SljitProjectionAggregatePrefixKind::TWO_JOIN",
            "&SljitProjectionAggregateRecipeBuilder::TryBuildSourceProjectionAggregate",
            "&SljitProjectionAggregateRecipeBuilder::TryBuildMarkBoundary",
            "&SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinPreparedRecipe",
            "&SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinMaterializedTail",
            "&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinSourcePrefixRecipe",
            "&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinProjectedPrefixRecipe",
            "&SljitProjectionAggregateRecipeBuilder::TryBuildPlainTwoJoinDirectAggregate",
            "&SljitProjectionAggregateRecipeBuilder::TryBuildPlainTwoJoinProjectionAggregateTail",
            "HasMarkFilterBoundary",
            "SingleJoinCanUseDirectAggregate",
            "CanBindSecondHashJoinSelection",
            "TwoJoinHasPreparedPrefix",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_recipe.hpp",
        (
            "const char *name",
            "TryBuildSourcePrefix",
            "TryBuildSingleJoinPrefix",
            "TryBuildTwoJoinPrefix",
            "TryBuildSingleJoinProjectionAggregate",
            "TryBuildTwoJoinProjectionAggregate",
            "SingleJoinHasMarkFilterBoundary",
            "TwoJoinHasMarkFilterBoundary",
            "TryBuildSingleJoinMarkBoundary",
            "TryBuildTwoJoinMarkBoundary",
            "TryBuildTwoJoinPreparedRecipe",
            "TryBuildTwoJoinDirectAggregate",
            "TryBuildTwoJoinMaterializedTail",
            "TwoJoinCanUseSelection",
            "TwoJoinCanUseProjectionAggregatePattern",
            "SelectSingleJoinStrategy",
            "SelectTwoJoinStrategy",
            "SljitProjectionAggregateSingleJoinStrategy",
            "SljitProjectionAggregateTwoJoinStrategy",
            "source_grouped_aggregate",
            "single_join_mark_filter",
            "single_join_source_filter_projection",
            "single_join_pre_projection",
            "single_join_direct_projection",
            "single_join_projection_chain",
            "two_join_mark_filter",
            "two_join_filtered_projection_prefix",
            "two_join_pre_projection",
            "two_join_between_projection",
            "two_join_direct_projection",
            "two_join_projection_chain",
            "TryBuildSourceGroupedAggregate",
            "TryBuildSingleJoinMarkFilter",
            "TryBuildSingleJoinSourceFilterProjection",
            "TryBuildSingleJoinPreProjection",
            "TryBuildSingleJoinDirectProjection",
            "TryBuildSingleJoinProjectionChain",
            "TryBuildTwoJoinMarkFilter",
            "TryBuildTwoJoinSourceFilterProjection",
            "TryBuildTwoJoinPreProjection",
            "TryBuildTwoJoinBetweenProjection",
            "TryBuildTwoJoinDirectProjection",
            "TryBuildTwoJoinProjectionChain",
            "SourcePrefixHasDedicatedGroupedBackend",
            "SingleJoinHasPrefixOperator",
            "TwoJoinHasPrefixOperator",
            "SljitProjectionAggregateCanUseSourceCountStarGroupedAggregate",
            "SljitProjectionAggregateCanUseSingleJoinMarkFilterBoundary",
            "SljitProjectionAggregateCanUseSingleJoinSourceFilterProjection",
            "SljitProjectionAggregateCanUseSingleJoinPreJoinProjection",
            "SljitProjectionAggregateCanUseSingleJoinSingleProjection",
            "SljitProjectionAggregateCanUseSingleJoinProjectionChain",
            "{&SljitProjectionAggregateRecipeBuilder::TryBuildSourceGroupedAggregate}",
            "{&SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinMarkFilter}",
            "{&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinMarkFilter}",
            "plan.prefix.Kind() == SljitProjectionAggregatePrefixKind::SOURCE",
            "facts.Kind() == SljitProjectionAggregatePrefixKind::SINGLE_JOIN",
            "facts.Kind() == SljitProjectionAggregatePrefixKind::TWO_JOIN",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_native_tail_recipe.hpp",
        (
            "SljitTryAnalyzeMarkFilterProjectionNativeTail(ops, facts)",
            "SljitTryAnalyzeGeneratedFilterProjectionNativeTail(ops, facts)",
            "SljitTryAnalyzeProjectionFilterProjectionNativeTail(ops, facts)",
            "SljitTryAnalyzeSourceBatchNativeTail(schedule_facts, facts)",
            "class SljitNativeTailRecipeBuilder",
            "TryBuildFactsRecipe<SljitMarkFilterProjectionNativeTailFacts>",
            "TryBuildFactsRecipe<SljitGeneratedFilterProjectionNativeTailFacts>",
            "TryBuildFactsRecipe<SljitProjectionFilterProjectionNativeTailFacts>",
            "TryBuildFactsRecipe<SljitSourceBatchNativeTailFacts>",
            "bool TryBuildFactsRecipe(SljitFullPipelineRecipe &recipe) const",
            "bool AnalyzeFacts(SljitMarkFilterProjectionNativeTailFacts &facts) const",
            "SljitFullPipelineRecipe MakeFactsRecipe(const SljitSourceBatchNativeTailFacts &facts) const",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_native_tail_recipe.hpp",
        (
            "TryBuildMarkFilterProjection",
            "TryBuildGeneratedFilterProjection",
            "TryBuildProjectionFilterProjection",
            "TryBuildSourceBatch",
            "const char *name",
            "\"mark_filter_projection_native_tail\"",
            "\"generated_filter_projection_native_tail\"",
            "\"projection_filter_projection_native_tail\"",
            "\"source_batch_native_tail\"",
            "uses_scan_filters",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe.hpp",
        (
            "ProjectionFilterProjectionNativeTail()",
            "GeneratedFilterProjectionNativeTail()",
            "SljitFullPipelineOpsPrefixIs",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        (
            "MakeProjectionFilterProjectionNativeTailRecipe",
            "MakeMarkFilterProjectionNativeTailRecipe",
            "MakeProjectionChainStep(idx_t projection_idx) const",
            "MakeProjectionChainStep(idx_t first_projection_idx",
            "AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence, idx_t projection_idx) const",
            "AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence, idx_t first_projection_idx",
            "AddProjectionChainStep(sequence, facts.pre_projection_idx)",
            "SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx)",
            "AddProjectionChainStep(sequence, facts.projection_idx)",
            "SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter)",
            "MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx)",
            "MakeMarkFilterPrefix(facts.hash_join_idx, facts.filter_idx, true, facts.first_projection_idx)",
            "MakeProjectionNativeTailRecipe(std::move(sequence), facts.first_projection_idx",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_generated_filter_primitive_runtime.hpp",
        (
            "class SljitGeneratedFilterPrimitiveRuntime",
            "SljitExecuteGeneratedFilterPrimitive(runtime, scratch, ops, step.generated_filter, input",
            "execute_output_view(filtered_input)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_selection_primitive_runtime.hpp",
        (
            "class SljitHashJoinProbeSelectionPrimitiveRuntime",
            "selected_hash_join_inputs.TryPrepareHashProbeInput",
            "step.hash_join_probe_selection",
            "SljitRuntimeBatchViewFromHashJoinSelection(",
            "SljitDrainHashJoinProbeOutputsWithState(",
            "SljitHashJoinProbeOutputContract::SELECTED_VIEW",
            "optional_ptr<const SljitHashJoinProbeInputRemap>(&primitive.input_remap)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_materialize_primitive_runtime.hpp",
        (
            "class SljitHashJoinProbeMaterializePrimitiveRuntime",
            "step.hash_join_probe_materialize",
            "SljitDrainHashJoinProbeOutputs(scratch, hash_join_idx, hash_join_op, join_input, join_output",
            "SljitAppendChunkToInitializedBatch(",
            "\"hash_join_materialize_batch_append\"",
            "SljitFlushDataChunkBatch(hash_join_materialize_batch.chunk, execute_output_batch)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_chain_primitive_runtime.hpp",
        (
            "class SljitProjectionChainPrimitiveRuntime",
            "projection_chain_batches",
            "selected_hash_join_inputs",
            "SljitExecuteProjectionChainPrimitive(runtime, scratch, ops, step.projection_chain, input",
            "SljitFlushDataChunkBatch(projection_chain_batch.chunk, execute_output_batch)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_source_batch_boundary_runtime.hpp",
        (
            "class SljitSourceBatchBoundaryRuntime",
            "boundary_batches",
            "EnsureFromChunk(runtime.GetAllocator(), chunk)",
            "ShouldBatch",
            "CanCoalesce",
            "TypeIsConstantSize(vector.GetType().InternalType())",
            "SljitAdvanceSinkBatchBlocked(runtime, batch, batch_has_more_output)",
            "RecordSljitRegionRuntimePath(runtime, trace_op.kind, \"source_batch_boundary\", chunk.size())",
            "\"source_batch_boundary_reference_handoff\"",
            "RecordSljitRegionStageRuntime(runtime, op_idx, trace_op.kind, \"source_batch_boundary_append\", stage_start)",
            "RecordSljitRegionMaterializationBoundary(runtime, trace_op.kind, \"source_batch\", chunk.size())",
            "SljitFlushDataChunkBatch(boundary_batch.chunk, execute_batch)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "SljitFullPipelinePrimitiveSequenceBatchExecutor",
            "SljitFullPipelinePrimitiveSequenceIsExecutable",
            "SljitFullPipelinePrimitiveSequenceTerminalStep",
            "SljitGeneratedFilterPrimitiveRuntime generated_filter",
            "SljitHashJoinProbeMaterializePrimitiveRuntime hash_join_materialize",
            "SljitHashJoinProbeSelectionPrimitiveRuntime hash_join_selection",
            "SljitProjectionChainPrimitiveRuntime projection_chain",
            "SljitSourceBatchBoundaryRuntime source_batch_boundary",
            "const auto max_recipe_batches = recipe.uses_extended_source_fetch_budget",
            "SljitBatchedSourceContractFetchBudget(runtime.MaxChunks())",
            "processed_batches >= max_recipe_batches",
            "terminal_runtime.BudgetReached(runtime, TerminalStep(), max_recipe_batches)",
            "fetched_chunks >= max_recipe_batches",
            "for (idx_t step_idx = 1; step_idx + 1 < recipe.primitive_sequence.Count(); step_idx++)",
            "FlushMaterializingStep(step_idx, step)",
            "bool FlushMaterializingStep(idx_t step_idx, const SljitFullPipelinePrimitiveStep &step)",
            "generated_filter.Execute(step, input, execute_output_view)",
            "hash_join_materialize.Execute(step_idx, step, input, execute_hash_join_probe, execute_output_batch)",
            "hash_join_materialize.Flush(step_idx, execute_output_batch)",
            "hash_join_selection.Execute(step, input, execute_hash_join_probe, execute_next_step)",
            "projection_chain.Execute(step_idx, step, input, execute_output_batch)",
            "projection_chain.Flush(step_idx, execute_output_batch)",
            "source_batch_boundary.Execute(step_idx, step, input, have_more_output, execute_output_batch)",
            "source_batch_boundary.Flush(step_idx, execute_output_batch)",
            "ExecuteSourceBatchBoundary",
            "SljitFullPipelineTerminalRuntime<EXECUTE_NATIVE_FULL_PIPELINE_FROM, EXECUTE_HASH_JOIN_PROBE> terminal_runtime",
            "terminal_runtime.Prepare",
            "terminal_runtime.Execute",
            "terminal_runtime.Flush",
            "ExecuteHashJoinProbeSelection",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "SljitExecuteGeneratedFilterPrimitive(runtime, scratch, ops, step.generated_filter, input",
            "step.generated_filter",
            "FlushHashJoinMaterializeBatch(step_idx)",
            "FlushProjectionChainBatch(step_idx)",
            "FlushSourceBoundaryBatch(step_idx)",
            "SljitExecuteNativeTailHandoffBatch(runtime, result, scratch, step.Op(0), input",
            "terminal_step.kind == SljitFullPipelinePrimitiveKind::NATIVE_TAIL_HANDOFF",
            "execute_native_full_pipeline_from.Finalize(scratch);\n\t\t\treturn false;",
            "EXECUTE_NATIVE_FULL_PIPELINE_FROM &execute_native_full_pipeline_from;",
            "hash_join_materialize_batches",
            "AppendHashJoinMaterializeBatch",
            "SljitAppendChunkToInitializedBatch(",
            "\"hash_join_materialize_batch_append\"",
            "selected_hash_join_inputs.TryPrepareHashProbeInput",
            "SljitRuntimeBatchViewFromHashJoinSelection(",
            "SljitDrainHashJoinProbeOutputsWithState(",
            "SljitHashJoinProbeOutputContract::SELECTED_VIEW",
            "optional_ptr<const SljitHashJoinProbeInputRemap>(&primitive.input_remap)",
            "projection_chain_batches",
            "selected_hash_join_projection_inputs",
            "SljitExecuteProjectionChainPrimitive(runtime, scratch, ops, step.projection_chain, input",
            "SljitSourceBatchBoundaryShouldBatch",
            "ExecuteBatchBoundary",
            "source_boundary_batches",
            "\"source_batch_boundary\"",
            "\"source_batch_boundary_append\"",
            "\"source_batch\"",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "SljitTryExecuteFullPipelineUnbatched",
            "EXECUTE_UNBATCHED",
            "execute_unbatched",
            "return execute_unbatched()",
            "materialize_mark_output_fallback",
            "PrepareTerminalPrimitives",
            "TerminalBudgetReached",
            "projection_count_star_grouped_aggregate",
            "projected_grouped_aggregate_update",
            "distinct_aggregate_update",
            "batch_source_hash_join",
            "source_hash_join_batch",
            "row_pointer_distinct_aggregate_update",
            "join_projected_grouped_aggregate_update",
            "two_hash_join_projected_grouped_aggregate_update",
            "SljitFullPipelinePrimitiveKind::HASH_JOIN_PROJECTED_GROUPED_AGGREGATE_UPDATE",
            "SljitFullPipelinePrimitiveKind::TWO_HASH_JOIN_PROJECTED_GROUPED_AGGREGATE_UPDATE",
            "SljitFullPipelinePrimitiveKind::GROUPED_AGGREGATE_UPDATE",
            "SljitFullPipelinePrimitiveKind::PRE_JOIN_PROJECTION",
            "SljitFullPipelinePrimitiveKind::PRE_PROJECTED_FILTER_PROJECTION",
            "ExecutePreJoinProjection",
            "ExecutePreProjectedFilterProjection",
            "pre_projected_filter_projection",
            "pending_batch_consumer_step",
            "pending_projection_chain_step",
            "pending_materialize_boundary_step",
            "pending_materialize_boundary_consumer_step",
            "SljitGeneratedFilterPrimitive primitive",
            "SljitProjectionChainPrimitive primitive",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_chain_runtime.hpp",
        (
            "struct SljitProjectionChainPrimitive",
            "SljitCanBindProjectionChainPrimitive",
            "SljitBindProjectionChainPrimitive",
            "idx_t first_projection_idx",
            "idx_t final_projection_idx",
            "shared_ptr<SljitExecutableRegionOp> bound_composed_projection",
            "HasBoundComposedProjection",
            "primitive.bound_composed_projection = std::move(composed_projection)",
            "SljitBindRuntimeBatchInput(input, \"SLJIT projection-chain primitive\")",
            "SljitTryPrepareSelectedHashJoinProjectionChainInput",
            "unique_ptr<SljitExecutableRegionOp> &mapped_projection",
            "SljitResolveBoundProjectionChain",
            "SljitExecuteProjectionChainPrimitiveSequential",
            "SljitExecuteProjectionChainPrimitive",
            "SljitDataChunkBatch &projection_chain_batch",
            "SljitDataChunkBatch &selected_hash_join_input",
            "projection_chain_batch.Ensure(runtime.GetAllocator(), projection_op->output_types)",
            "SljitTryDirectMaterializeFixedProjectionToBatch",
            "SljitProjectionHasVariableWidthOutput",
            "SljitTrySliceReferenceProjection",
            "reference_view_handoff",
            "SljitFlushDataChunkBatch(batch, execute_output_batch)",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_projection_chain_runtime.hpp",
        (
            "runtime.PrepareSourceContractBatch(projection_op.output_types)",
            "SljitFlushRuntimePendingBatch(runtime, execute_output_batch)",
            "SljitBuildExecutableProjectionChain",
            "SljitExecutableRegionOp mapped_projection;",
            "SljitExecutableRegionOp composed_projection;",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_generated_filter_primitive.hpp",
        (
            "SljitProjectionChainPrimitive",
            "SljitCanBindProjectionChainPrimitive",
            "SljitBindProjectionChainPrimitive",
            "SljitExecuteProjectionChainPrimitive",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_terminal_runtime.hpp",
        (
            "class SljitFullPipelineTerminalRuntime",
            "ExecuteGroupedAggregateUpdate",
            "ExecuteNativeTailHandoff",
            "ungrouped_aggregate_update.Execute",
            "ungrouped_aggregate_update.Flush",
            "grouped_aggregate_update.Execute",
            "grouped_aggregate_update.Flush",
            "join_projection_aggregate_update.Execute",
            "join_projection_aggregate_update.Flush",
            "SljitBindMaterializedRuntimeBatchInput(input, \"SLJIT native tail handoff\")",
            "execute_native_full_pipeline_from(scratch, terminal_step.Op(0), chunk)",
            "SljitNativeSinkResultStopsExecution(runtime, sink_result, result)",
            "execute_native_full_pipeline_from.Finalize(scratch)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        (
            "struct SljitJoinProjectionAggregateUpdateRuntimeState",
            "selected_join_output.Prepare",
            "selected_join_output.Execute",
            "selected_join_output.Flush",
            "selected_join_output.BudgetReached",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        (
            "source_join.ExecuteSourceChunk",
            "SljitJoinProjectionAggregateUpdateInputKind::SOURCE_JOIN_INPUT",
            "SljitJoinProjectionAggregateRuntimeState source_join",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        (
            "input.hash_join_output_column_map",
            "input.hash_join_output_projection_idx",
            "SLJIT mapped selected join-output aggregate descriptor failed",
            "direct_join_output_aggregate_strategy->last_failure",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        (
            "auto &producer_input = scratch.TemporaryChunk(output_projection_idx)",
            "SljitExecuteProjection(scratch, output_projection_idx",
            "materialization_input = &producer_input",
            "SLJIT mapped selected join output has no producer projection",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_direct_join_output_aggregate_runtime.hpp",
        (
            "optional_ptr<const vector<idx_t>> output_column_map",
            "const bool has_projection_chain = post_join_projection.HasProjectionChain()",
            "SljitTryBuildPostJoinProjectionAggregateDescriptor",
            "SljitTryBuildSelectedJoinAggregateInputDescriptor",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_pre_join_projection_descriptor.hpp",
        (
            "struct SljitPreJoinProjectionViewDescriptor",
            "projected_to_source",
            "hash_probe_key_source_indices",
            "residual_probe_source_indices",
            "hash_probe_key_inputs_match_source",
            "hash_probe_key_inputs_can_remap",
            "residual_probe_sources_can_remap",
            "SljitTryBuildPreJoinResidualProbeSourceRemap",
            "CanElideProjectionWithCurrentHashProbe",
            "SljitTryBuildPreJoinProjectionViewDescriptor",
            "SljitTryBuildPreJoinProjectionViewColumn",
            "SljitPreJoinProjectionViewColumnKind::INT64_TO_INT32_CAST",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_primitive.hpp",
        (
            "struct SljitHashJoinProbeInputRemap",
            "key_input_indices",
            "residual_probe_source_indices",
            "prepared_plan",
            "has_prepared_plan",
            "SljitPrepareHashJoinProbeInputRemap",
            "SljitApplyPreparedHashJoinResidualProbeSourceRemap",
            "input_remap.prepared_plan.operator_info.hash_join_keys[key_idx].input_index",
            "SLJIT hash join selection remap requires prepared input types",
            "vector<idx_t> output_column_map",
            "HasOutputColumnMap",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp",
        (
            "struct SljitHashJoinProbeExecutionContractView",
            "SljitBuildHashJoinProbeExecutionContractView",
            "input_remap->HasRemap()",
            "input_remap->has_prepared_plan",
            "view.plan = &input_remap->prepared_plan",
            "view.operator_info = &input_remap->prepared_plan.operator_info",
            "prepared hash join probe remap requires selected-view execution",
            "hash join probe remap was not prepared during primitive binding",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp",
        (
            "SljitApplyHashJoinResidualProbeSourceRemap",
            "SljitRemappedHashJoinProbeKeySourceSupported",
            "remapped_plan",
            "remapped_operator_info",
            "view.remapped_plan",
            "view.remapped_operator_info",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_selection_primitive_runtime.hpp",
        (
            "primitive.HasOutputColumnMap()",
            "primitive.output_column_map",
            "optional_ptr<const SljitHashJoinProbeInputRemap>",
            "&primitive.input_remap",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        (
            "MakeJoinProjectionAggregateTerminal",
            "SourceKey0RangeFitsInt32",
            "TryBuildPreJoinProjectionView",
            "BindElidedPreJoinHashJoinProbeSelection",
            "SljitPreJoinProjectionViewDescriptor pre_join_view",
            "SljitTryBuildPreJoinProjectionViewDescriptor",
            "pre_join_view.CanElideProjectionWithCurrentHashProbe()",
            "input_remap.key_input_indices = pre_join_view.hash_probe_key_source_indices",
            "input_remap.residual_probe_source_indices = pre_join_view.residual_probe_source_indices",
            "pre_join_view.source_key0_int64_to_int32_unchecked",
            "pre_join_view.projected_to_source",
            "remapped_hash_join_selection",
        ),
    )
    require_scoped_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        "MakePreProjectionJoinProjectionAggregateRecipe",
        "MakeFilterProjectionJoinProjectionAggregateRecipe",
        (
            "SljitPreJoinProjectionViewDescriptor pre_join_view",
            "TryBuildPreJoinProjectionView",
            "optional_ptr<const SljitPreJoinProjectionViewDescriptor> pre_join_view_ptr",
            "pre_join_view_ptr = pre_join_view",
            "MakePreJoinProjectionHashJoinSelectionSequence(pre_join_projection_idx, hash_join_idx, true,",
            "pre_join_view_ptr);",
        ),
    )
    require_scoped_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        "MakePreJoinProjectionHashJoinSelectionSequence",
        "SljitFullPipelinePrimitiveSequence MakeSourceHashJoinProjectionInputSequence",
        (
            "pre_join_view",
            "TryAppendElidedPreJoinHashJoinProbeSelection",
            "AddMaterializedPreJoinProjectionHashJoinSelection",
        ),
    )
    require_scoped_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        "TryAppendElidedPreJoinHashJoinProbeSelection(SljitFullPipelinePrimitiveSequence &sequence,",
        "AddMaterializedPreJoinProjectionHashJoinSelection",
        (
            "pre_join_view.CanElideProjectionWithCurrentHashProbe()",
            "BindElidedPreJoinHashJoinProbeSelection",
        ),
    )
    require_scoped_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        "AddMaterializedPreJoinProjectionHashJoinSelection",
        "SljitHashJoinProbeSelectionPrimitive",
        (
            "AddProjectionChainStep(sequence, pre_join_projection_idx)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_string_set_case_projection_runtime.hpp",
        (
            "const SljitPreJoinProjectionViewDescriptor &pre_join_view",
            "pre_join_view.columns[0].kind != SljitPreJoinProjectionViewColumnKind::INT64_TO_INT32_CAST",
            "pre_join_view.columns[1].kind != SljitPreJoinProjectionViewColumnKind::REFERENCE",
            "auto &join = ops[pre_join_view.hash_join_idx].hash_join_probe.plan",
        ),
    )
    reject_regex(
        "stale hard-coded pre-join projection descriptor",
        (
            r"\bSljitInt64ToInt32PreJoinProjection\b",
            r"\bSljitTryBuildInt64ToInt32PreJoinProjection\b",
            r"\bHasInt64ToInt32Projection\b",
            r"\bUsesUncheckedKeyCast\b",
        ),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_projection_source_runtime.hpp",
        (
            "SljitTryBuildHashJoinMappedProjection",
            "SljitTryRemapHashJoinProjectionExpressionSources",
            "remap_projection_sources",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_primitive_sequence.hpp",
        (
            "HASH_JOIN_PROJECTED_GROUPED_AGGREGATE_UPDATE",
            "POST_JOIN_PROJECTED_GROUPED_AGGREGATE_UPDATE",
            "TWO_HASH_JOIN_PROJECTED_GROUPED_AGGREGATE_UPDATE",
            "HashJoinProjectedGroupedAggregateUpdate",
            "PostJoinProjectedGroupedAggregateUpdate",
            "TwoHashJoinProjectedGroupedAggregateUpdate",
            "hash_join_projected_grouped_aggregate_update",
            "post_join_projected_grouped_aggregate_update",
            "two_hash_join_projected_grouped_aggregate_update",
            "DISTINCT_AGGREGATE_UPDATE",
            "DistinctAggregateUpdate",
            "distinct_aggregate_update",
            "row_pointer_distinct_aggregate_update",
            "projected_distinct_aggregate_update",
            "PRE_PROJECTED_FILTER_PROJECTION",
            "PreProjectedFilterProjection",
            "pre_projected_filter_projection",
            "PROJECTED_GROUPED_AGGREGATE_UPDATE",
            "SljitProjectedGroupedAggregateUpdatePrimitive",
            "ProjectedGroupedAggregateUpdate",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_generated_filter_primitive.hpp",
        (
            "SljitPreProjectedFilterProjectionPrimitive",
            "SljitCanBindPreProjectedFilterProjectionPrimitive",
            "SljitBindPreProjectedFilterProjectionPrimitive",
            "SljitDeferredPreProjectionFilterBuildShape",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe.hpp",
        (
            "TryBuildPreProjectedFilterProjectionNativeTailRecipe",
            "PreProjectedFilterProjectionNativeTail",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        (
            "MakeProjectionGroupedAggregateRecipe",
            "AddProjectionChainStep(sequence, shape.first_projection_idx, shape.final_projection_idx)",
            "AddProjectionChainStep(sequence, first_projection_idx, final_projection_idx)",
            "SljitBindGroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx)",
            "SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update)",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        ("for (idx_t projection_idx = shape.first_projection_idx", "for (idx_t projection_idx = first_projection_idx"),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_dispatch_runtime.hpp",
        (
            "const SljitFullPipelineRecipePlan &recipe_plan",
            "recipe_plan.has_recipe",
            "recipe_plan.uses_extended_source_fetch_budget",
            "recipe_plan.recipe",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_mark_probe_filter_boundary.hpp",
        ("SljitMaterializeMarkProbeOutputFallback", "SljitMaterializeMarkProbeFallbackAndHandoffIntoSink"),
    )


def verify_recipe_builder() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_state.hpp",
        (
            "struct SljitFullPipelineRecipe",
            "struct SljitFullPipelineRecipePlan",
            "bool has_recipe = false",
            "recipe.primitive_sequence.Count() == 0",
            "primitive recipe cannot be empty",
            "SljitFullPipelinePrimitiveSequence primitive_sequence",
            "SljitMakeFullPipelinePrimitiveRecipe",
            "SljitMakeFullPipelinePrimitiveRecipePlan",
            "SljitMakeFullPipelineNativeOnlyPlan",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        (
            "class SljitFullPipelineRecipeBinding",
            "MakeNativeOnlyPlan",
            "MakePrimitiveRecipePlan",
            "SljitFullPipelinePrimitiveSequenceIsExecutable(ops, recipe.primitive_sequence)",
            "invalid full-pipeline primitive sequence",
            "uses_extended_source_fetch_budget",
            "MakePrimitiveSequence",
            "MakeSourceSequence",
            "MakeNativeTailRecipe",
            "MakeSourceBatchNativeTailRecipe",
            "MakeSourceUngroupedAggregateRecipe",
            "const SljitSourceBatchNativeTailFacts &facts",
            "const SljitSourceUngroupedAggregateFacts &facts",
            "SljitFullPipelinePrimitiveStep::SourceBatchBoundary(facts.boundary_op_idx)",
            "SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate",
            "facts.tail_start_idx",
            "MakeHashJoinProbeMaterializeStep",
            "MakeMarkFilterPrefix",
            "MakeTwoJoinMarkFilterPrefix",
            "MakeMarkFilterNativeTailRecipe",
            "MakeSourceProjectionAggregateRecipe",
            "MakeSingleJoinProjectionAggregateTailRecipe",
            "MakeProjectionAggregateRecipe",
            "MakeProjectionGroupedAggregateRecipe",
            "MakeProjectionAggregateTailRecipe",
            "MakeProjectionNativeTailRecipe",
            "ProjectionAggregateHasDedicatedBackend",
            "ProjectionGroupedAggregateHasDedicatedBackend",
            "SljitCanBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx)",
            "SljitFullPipelinePrimitiveSequence sequence",
            "SljitFullPipelinePrimitiveStep::SourceBatchBoundary",
            "SljitFullPipelinePrimitiveStep::HashJoinProbeSelection",
            "SljitFullPipelinePrimitiveStep::ProjectionChain",
            "SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate",
            "SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate",
            "SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate",
            "BindPostJoinProjectionAggregatePrimitive",
            "MakeSourceHashJoinProbeSelectionSequence",
            "MakeSourceHashJoinProjectionInputSequence",
            "MakeTwoJoinDirectProjectionAggregateRecipe",
            "MakeTwoJoinProjectionAggregateRecipe",
            "const SljitProjectionAggregatePrefixFacts &facts",
            "facts.HasSourceFilterProjection()",
            "facts.HasPreJoinProjection()",
            "facts.HasBetweenProjection()",
            "AddProjectionChainStep(sequence, facts.between_projection_idx)",
            "MakeHashJoinProbeProjectionInputStep",
            "SourceBatchBoundaryCanCoalesce",
            "AddSourceBatchBoundaryIfUseful",
            "TypeIsConstantSize(type.InternalType())",
            "source_output_types",
            "sequence.Add(MakeHashJoinProbeProjectionInputStep(facts.first_hash_join_idx))",
            "MakeHashJoinProbeSelectionStep(facts.second_hash_join_idx)",
            "SljitMakeSelectedJoinOutputAggregateUpdatePrimitive",
            "return MakeProjectionAggregateTailRecipe(std::move(sequence), shape)",
            "return MakeProjectionAggregateRecipe(std::move(sequence), shape)",
            "return MakeProjectionGroupedAggregateRecipe(std::move(sequence), shape,",
            "return MakeProjectionNativeTailRecipe(std::move(sequence), shape)",
            "SljitFullPipelinePrimitiveStep::NativeTailHandoff",
        ),
    )
    require_scoped_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        "MakeSourceUngroupedAggregateRecipe",
        "MakeGeneratedFilterProjectionNativeTailRecipe",
        (
            "auto sequence = MakeSourceSequence();",
            "AddSourceBatchBoundaryIfUseful(sequence, facts.aggregate_idx);",
            "SljitBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx)",
            "sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        (
            "MakeFilterProjectionTwoJoinProjectionAggregateRecipe",
            "MakeBetweenProjectionTwoJoinProjectionAggregateRecipe",
            "MakePreProjectionTwoJoinProjectionAggregateRecipe",
            "MakeTwoJoinProjectionChainAggregateRecipe",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
        (
            "struct SljitSourceUngroupedAggregateFacts",
            "SljitTryAnalyzeSourceUngroupedAggregate",
            "SljitFullPipelineOpIsUngroupedPrimitiveAggregateUpdate(ops[0])",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe.hpp",
        (
            "TryBuildSourceUngroupedAggregateRecipe",
            "SljitTryAnalyzeSourceUngroupedAggregate",
            "binding.MakeSourceUngroupedAggregateRecipe",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_ungrouped_aggregate_update_primitive.hpp",
        (
            "struct SljitUngroupedAggregateUpdatePrimitive",
            "struct SljitBoundUngroupedPrimitiveAggregateUpdate",
            "SljitCanBindUngroupedAggregateUpdatePrimitive",
            "ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE",
            "SljitBindUngroupedPrimitiveAggregateUpdate",
            "SljitExecuteBoundUngroupedPrimitiveAggregateUpdate",
            "SljitBoundSingleFusedPrimitiveAggregatePayloadUpdate single_fused_payload_update",
            "SljitBindSingleFusedPrimitiveAggregatePayloadUpdate",
            "SljitExecuteBoundSingleFusedPrimitiveAggregatePayloadUpdate",
            "SljitBindNativeSink",
            "RecordSinkResult(input.count, sink_result)",
            "SljitUngroupedAggregateUpdateRuntimeState",
            "SljitBoundUngroupedPrimitiveAggregateUpdate bound_update",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        (
            "SljitMakeSourceJoinProjectionAggregateUpdatePrimitive",
            "source_join.ExecuteSourceChunk",
            "compact_source_contract_output",
            "SljitFullPipelinePrimitiveKind::MATERIALIZE_BOUNDARY",
            "SljitFullPipelinePrimitiveStep::MaterializeBoundary",
            "sequence.Add(SljitFullPipelinePrimitiveStep::MaterializeBoundary(hash_join_idx));",
            "MakeSourceMaterializeNativeTailRecipe",
            "UsesExtendedSourceFetchBudget",
            "UsesScanFilteredAggregateTerminal",
            "uses_scan_filters",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_primitive_sequence.hpp",
        (
            "PRE_JOIN_PROJECTION",
            "SljitPreJoinProjectionPrimitive pre_join_projection",
            "PreJoinProjection(const SljitPreJoinProjectionPrimitive &primitive)",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_primitive_contract.hpp",
        (
            "SljitFullPipelinePrimitiveKind::PRE_JOIN_PROJECTION",
            "SljitCanBindPreJoinProjectionPrimitive",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        ("SljitFullPipelinePrimitiveStep::PreJoinProjection",),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe.hpp",
        (
            "BuildSljitFullPipelineRecipePlan",
            "SljitFullPipelineRecipeBuilder",
            "SljitFullPipelineRecipeBinding binding",
            "SljitAnalyzeFullPipelineScheduleFacts(ops_p)",
            "schedule_facts.uses_extended_source_fetch_budget",
            '#include "sljit_hash_join_delim_join_sink_recipe.hpp"',
            "SljitTryAnalyzeHashJoinDelimJoinSink(ops, facts)",
            "SljitHashJoinDelimJoinSinkRecipeBuilder(ops, binding).Build(recipe, facts)",
            "SljitTryAnalyzeProjectionAggregatePlan",
            "SljitProjectionAggregateRecipeBuilder(ops, binding).Build(recipe, plan)",
            "SljitNativeTailRecipeBuilder(ops, schedule_facts, binding).Build(recipe)",
            "struct SljitFullPipelineRecipeRegistryEntry",
            "RecipeRegistry",
            "TryBuildNativeTailRecipe",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe.hpp",
        (
            "uses_scan_filters_p",
            "uses_scan_filters",
            "sljit_selected_join_aggregate_recipe.hpp",
            "SljitTryAnalyzeSelectedJoinAggregate",
            "SljitSelectedJoinAggregateRecipeBuilder",
            "facts.HasSecondHashJoin()",
            "facts.HasPreJoinProjection()",
            "MakeTwoJoinSelectedAggregateRecipe(",
            "MakePreProjectionSelectedJoinAggregateRecipe(",
            "MakeSelectedJoinAggregateRecipe(",
            "facts.sink_idx + 3 > SLJIT_FULL_PIPELINE_MAX_PRIMITIVES",
            "SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive(ops, facts.final_hash_join_idx, facts.sink_idx)",
            "for (idx_t hash_join_idx = facts.first_hash_join_idx;",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe.hpp",
        (
            "const char *name",
            "\"selected_join_aggregate\"",
            "\"hash_join_delim_join_sink\"",
            "\"projection_aggregate\"",
            "\"native_tail\"",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_delim_join_sink_recipe.hpp",
        (
            "class SljitHashJoinDelimJoinSinkRecipeBuilder",
            "CanFitPrimitiveSequence",
            "CanBindDelimSink",
            "CanBindHashJoinInputs",
            "SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive",
            "SljitCanBindHashJoinProbeSelectionPrimitive",
            "SljitCanBindHashJoinProbeMaterializePrimitive",
            "MakeHashJoinDelimJoinSinkRecipe",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_hash_join_delim_join_sink_recipe.hpp",
        (
            "const char *name",
            "switch (",
            "enum class SljitHashJoinDelimJoinSink",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_recipe.hpp",
        (
            "class SljitProjectionAggregateRecipeBuilder",
            "TryBuildProjectionAggregateRecipeFunction",
            "struct RegistryEntry",
            "RecipeRegistry",
            "SljitProjectionAggregatePrefixKind prefix_kind",
            "registry[entry_idx].prefix_kind != prefix_kind",
            "registry[entry_idx].try_build",
            "(this->*registry[entry_idx].try_build)(recipe, plan)",
            "TryBuildSourceProjectionAggregate",
            "TryBuildMarkBoundary",
            "TryBuildSingleJoinPreparedRecipe",
            "TryBuildSingleJoinMaterializedTail",
            "TryBuildTwoJoinSourcePrefixRecipe",
            "TryBuildTwoJoinProjectedPrefixRecipe",
            "TryBuildPlainTwoJoinDirectAggregate",
            "TryBuildPlainTwoJoinProjectionAggregateTail",
            "HasMarkFilterBoundary",
            "SingleJoinCanUseProjectionAggregateTail",
            "plan.ProjectionCount() == 0",
            "projection_count != 0 && projection_count != 1",
            "CanBindSecondHashJoinSelection",
            "TwoJoinHasPreparedPrefix",
            "CanBindHashJoinProbeProjectionInput",
            "CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx)",
            "ProjectionAggregateHasDedicatedBackend(shape)",
            "shape.ProjectionCount() != 0",
            "MakeMarkFilterNativeTailRecipe",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
        (
            "struct SljitProjectionAggregatePlanFacts",
            "struct SljitProjectionAggregatePrefixFacts",
            "struct SljitFullPipelineScheduleFacts",
            "has_source_batch_native_tail",
            "source_batch_boundary_op_idx",
            "source_batch_tail_start_idx",
            "SljitFullPipelineOpIsUngroupedPrimitiveAggregateUpdate",
            "struct SljitHashJoinDelimJoinSinkFacts",
            "struct SljitSourceBatchNativeTailFacts",
            "SljitAnalyzeFullPipelineScheduleFacts",
            "SljitTryAnalyzeHashJoinDelimJoinSink",
            "SljitTryAnalyzeSourceBatchNativeTail",
            "SljitTryAnalyzeProjectionAggregatePlan",
            "shape.ProjectionCount() == 0 ? shape.aggregate_idx : shape.first_projection_idx",
            "SljitTryAnalyzeMarkFilterProjectionNativeTail",
            "SljitTryAnalyzeGeneratedFilterProjectionNativeTail",
            "SljitTryAnalyzeProjectionFilterProjectionNativeTail",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
        (
            "SljitFullPipelineUsesScanFilteredAggregateTerminal",
            "uses_scan_filters",
            "struct SljitSelectedJoinAggregateFacts",
            "SljitTryAnalyzeSelectedJoinAggregate",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe.hpp",
        (
            "SljitProjectionAggregateRecipeStrategy",
            "SljitSelectProjectionAggregateRecipeStrategy",
            "TryBuildProjectionAggregateRecipeFunction",
            "struct RegistryEntry",
            "TryBuildProjectionAggregateSingleMarkFilter",
            "TryBuildProjectionAggregateTwoJoinMarkFilter",
            "TryBuildMarkFilterProjectionNativeTailRecipe",
            "TryBuildGeneratedFilterProjectionNativeTailRecipe",
            "TryBuildProjectionFilterProjectionNativeTailRecipe",
            "TryBuildSourceBatchNativeTailRecipe",
            "enum class SljitProjectionAggregateRecipeKind",
            "ProjectionAggregateRecipeKind(plan)",
            "registry[entry_idx].kind != kind",
            "enum class SljitProjectionAggregateRecipeVariant",
            "SljitProjectionAggregateRecipeVariant::",
            "TryBuildProjectionAggregateRecipeVariant",
            "registry[entry_idx].variant",
            "switch (variant)",
            "TryBuildSingleJoinProjectionAggregate",
            "TryBuildTwoJoinProjectionAggregate",
            "SingleJoinHasMarkFilterBoundary",
            "TwoJoinHasMarkFilterBoundary",
            "TryBuildSingleJoinMarkBoundary",
            "TryBuildTwoJoinMarkBoundary",
            "TryBuildTwoJoinPreparedRecipe",
            "TryBuildTwoJoinDirectAggregate",
            "TryBuildTwoJoinMaterializedTail",
            "TwoJoinCanUseSelection",
            "TwoJoinCanUseProjectionAggregatePattern",
            "struct SljitFullPipelineRecipeCandidate",
            "RecipeCandidates",
            "struct SljitProjectionAggregateRecipeCandidate",
            "SingleJoinProjectionAggregateCandidates",
            "TwoJoinProjectionAggregateCandidates",
            "TryBuildSourceProjectionAggregateRecipe",
            "TryBuildSingleJoinMarkFilterRecipe",
            "TryBuildSingleJoinSourceFilterProjectionRecipe",
            "TryBuildSingleJoinPreProjectionRecipe",
            "TryBuildSingleJoinDirectProjectionRecipe",
            "TryBuildSingleJoinProjectionChainRecipe",
            "TryBuildTwoJoinMarkFilterRecipe",
            "TryBuildTwoJoinSourceFilterProjectionRecipe",
            "TryBuildTwoJoinPreProjectionRecipe",
            "TryBuildTwoJoinBetweenProjectionRecipe",
            "TryBuildTwoJoinDirectProjectionRecipe",
            "TryBuildTwoJoinProjectionChainRecipe",
            "TryBuildSingleJoinProjectionAggregateRecipe",
            "TryBuildTwoJoinProjectionAggregateRecipe",
            "SljitFullPipelineRecipeKind",
            "projection_aggregate_shape",
            "SljitFullPipelinePrimitiveSequence::From",
            "SljitFullPipelineRecipeAssembler",
            "MakeUnbatched",
            "HasPrimitiveSequence",
            "FilteredSourceAggregate() const",
            "filtered_source_aggregate",
            "TryBuildFilteredSourceAggregateRecipe",
            "SljitTryAnalyzeFilteredSourceAggregate",
            "SljitFullPipelineOpsAre",
            "SljitFullPipelineIsAggregateUpdateAt",
            "ShouldUseSourceBatchNativeTailRecipe",
            "MakePrimitiveSequence",
            "MakeProjectedAggregateTerminalStep",
            "HashJoinProjectedGroupedAggregateUpdate",
            "PostJoinProjectedGroupedAggregateUpdate",
            "TwoHashJoinProjectedGroupedAggregateUpdate",
            "SljitBind",
            "SljitFullPipelinePrimitiveStep::",
            "DistinctAggregateUpdate",
            "row_pointer_distinct",
            "projected_distinct",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
        (
            "SljitFilteredSourceAggregateFacts",
            "SljitTryAnalyzeFilteredSourceAggregate",
        ),
    )
    reject_regex(
        "projection-fed two-join recipes must not force full first-join materialization",
        (
            r"(?s)MakeTwoJoinProjectionAggregateRecipe.*?"
            r"sequence\.Add\(MakeHashJoinProbeMaterializeStep\(facts\.first_hash_join_idx\)\);\s*"
            r"if \(facts\.HasBetweenProjection\(\)\)",
            r"(?s)MakeTwoJoinProjectionAggregateRecipe.*?"
            r"sequence\.Add\(MakeHashJoinProbeMaterializeStep\(facts\.first_hash_join_idx\)\);\s*"
            r"sequence\.Add\(MakeHashJoinProbeSelectionStep\(facts\.second_hash_join_idx\)\)",
        ),
        ("extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_state.hpp",
        (
            "SljitFullPipelineRecipeAssembler",
            "compact_source_contract_output",
            "MakeUnbatched",
            "HasPrimitiveSequence",
            "SljitFullPipelinePrimitiveSequence::From",
            "HasSteps",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_dispatch_runtime.hpp",
        ("compact_source_contract_output",),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "compact_source_contract_output",
            "source_contract_compact_batch",
            "source_contract_compact_batch_append",
        ),
    )


def verify_native_tail_and_deferred_finish() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_native_pipeline_runtime.hpp",
        (
            "struct SljitNativePipelineGroupedFinishState",
            "SljitNativePipelineAggregateCanDeferGroupedFinish",
            "grouped_finish.Finish(runtime, scratch)",
            "grouped_finish->Prepare(op_idx)",
            "grouped_finish->Prepare(op_idx + 1)",
            "SljitExecuteNativeFullPipelineFrom",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_state_runtime.hpp",
        (
            "SljitFinishDeferredGroupedAggregateUpdate",
            "MarkDeferredGroupedFinish",
        ),
    )


def verify_distinct_aggregate_backend() -> None:
    require_absent("src/include/duckdb/execution/operator/aggregate/distinct_count_pointer_set.hpp")
    require_absent("src/execution/operator/aggregate/distinct_count_pointer_set.cpp")
    reject_text(
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        (
            "ExecutionDistinctCountPointerUpdateState",
            "ExecutionDistinctCountPointerUpdateBinding",
            "distinct_count_pointer",
            "TryResolveDistinctCountPointerAddresses",
            "AddPayloads",
            "TryUpdateNewGroupsWithStateAddresses",
            "TryResolveDistinctCountPointerGroupAddresses",
        ),
    )
    reject_text(
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
        (
            "HashAggregateDistinctCountPointerUpdateState",
            "BindHashAggregateDistinctCountPointerUpdate",
            "TryResolveDistinctCountPointerAddresses",
            "HashAggregateUsesDistinctCountPointerKeys",
            "HashAggregateCanPlanDistinctCountPointerKeys",
            "distinct_count_pointer_keys",
            "TryUpdateNewGroupsWithStateAddresses",
            "TryResolveDistinctCountPointerGroupAddresses",
        ),
    )
    reject_text(
        "src/include/duckdb/execution/radix_partitioned_hashtable.hpp",
        (
            "TryUpdateNewGroupsWithStateAddresses",
            "ResolveGroupStateAddresses",
            "TryResolveNewGroupAddressesFromGroups",
        ),
    )
    reject_text(
        "src/execution/radix_partitioned_hashtable.cpp",
        (
            "TryUpdateNewGroupsWithStateAddresses",
            "ResolveGroupStateAddresses",
            "TryResolveNewGroupAddressesFromGroups",
            "direct_new_state_address",
            "direct_new_group_address",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp",
        (
            "enum class SljitGroupedAggregateUpdateStrategyKind",
            "SljitChooseGroupedAggregateUpdateStrategy",
            "SljitGroupedAggregateUpdateHasDedicatedBackend",
            "SljitGroupedAggregateUpdateCanUseCountStarPreaggregation",
            "SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION",
            "SljitGroupedAggregateUpdateCanUseDirectPrimitivePayloadUpdate",
            "SljitGroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE",
            "SljitGroupedAggregateUpdateStrategyKind::INVALID",
            "primitive.strategy",
            "shared_ptr<SljitExecutableRegionOp> projected_count_star_group_projection",
            "shared_ptr<SljitProjectedInputGroupedAggregateDescriptor> projected_direct_update",
            "make_shared_ptr<SljitExecutableRegionOp>",
            "make_shared_ptr<SljitProjectedInputGroupedAggregateDescriptor>",
            "SljitTryBindProjectedCountStarGroupedAggregateStrategy",
            "SljitTryBindProjectedDirectPrimitivePayloadUpdateStrategy",
            "SljitTryBindProjectedInputGroupedAggregateUpdateStrategy",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime_state.hpp",
        (
            "SljitGroupedAggregateUpdateRuntimeState",
            "SljitBindGroupedPrimitiveAggregateUpdate",
            "SljitExecuteBoundGroupedPrimitiveAggregateUpdate",
            "SljitBoundGroupedPrimitiveAggregateUpdate bound_direct_update",
            "SljitBoundGroupedPrimitiveAggregateUpdate bound_projected_direct_update",
            "ExecuteCountStarPreaggregation",
            "primitive_grouped_count_star_row_update",
            "projected_direct_update = primitive.projected_direct_update",
            "projected_count_star_group_projection = primitive.projected_count_star_group_projection",
        ),
    )
    reject_scoped_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime_state.hpp",
        "bool Prepare(ExecutionRegionRuntime &runtime",
        "bool Execute(ExecutionRegionRuntime &runtime",
        (
            "SljitTryBuildProjectedInputGroupedAggregateDescriptor(",
            "SljitBuildProjectionChainComposedProjection(",
            "SljitTryBuildCountStarGroupProjection(",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp",
        (
            "SljitGroupedAggregateUpdateRuntimeState",
            "SljitExecutePrimitiveAggregateUpdate",
            "ExecuteCountStarPreaggregation",
            "primitive_grouped_count_star_row_update",
            "SljitNativeSinkResultStopsExecution",
            "projected_direct_update = primitive.projected_direct_update",
            "projected_count_star_group_projection = primitive.projected_count_star_group_projection",
            "sljit_aggregate_count_star_fixed_preaggregation.hpp",
            "sljit_aggregate_count_star_string_preaggregation.hpp",
            "sljit_full_pipeline_runtime.hpp",
            "sljit_grouped_aggregate_update_runtime.hpp",
            "sljit_grouped_count_star_update_runtime.hpp",
            "SljitGroupedAggregateUpdateStrategyKind::STANDARD",
            "SljitGroupedAggregateUpdateStrategyKind::DISTINCT_COUNT_POINTER",
            "SljitChooseProjectedInputGroupedAggregateUpdateStrategy",
            "ExecuteStandard",
            "ExecuteDistinctCountPointer",
            "SljitExecuteNativeAggregateUpdate",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime.hpp",
        (
            "struct SljitBoundGroupedPrimitiveAggregateUpdate",
            "SljitBindGroupedPrimitiveAggregateUpdate",
            "SljitExecuteBoundGroupedPrimitiveAggregateUpdate",
            "SljitBindNativeSink",
            "optional_ptr<ExecutionGroupedAggregateStateAddressBinding> grouped_state",
            "needs_grouped_state_address_plan",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projected_grouped_aggregate_sink.hpp",
        (
            "SljitBindGroupedPrimitiveAggregateUpdate",
            "SljitExecuteBoundGroupedPrimitiveAggregateUpdate",
            "optional_ptr<SljitBoundGroupedPrimitiveAggregateUpdate> bound_grouped_update",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        (
            "SljitBoundGroupedPrimitiveAggregateUpdate bound_projected_grouped_update",
            "bound_grouped_update = &bound_projected_grouped_update",
        ),
    )
    reject_regex(
        "removed distinct count-pointer backend",
        (
            r"\bdistinct_count_pointer_keys\b",
            r"\bdistinct_count_pointer_payload_storage\b",
            r"\bDistinctCountPointer\b",
            r"\bExecutionDistinctCountPointer\b",
            r"\bSljit(?:Try)?ExecuteDistinctCountPointer\b",
            r"\bUseGlobalPayloadSet\b",
            r"\bGLOBAL_PAIR_SET\b",
        ),
        (
            "src/**/*.cpp",
            "src/**/*.hpp",
            "extension/jit_sljit/**/*.cpp",
            "extension/jit_sljit/**/*.hpp",
            "benchmark/jit/**/*.py",
            "benchmark/tpch/jit/**/*.py",
            "test/api/**/*.cpp",
        ),
        ("benchmark/jit/verify_jit_architecture.py", "test/api/test_jit_runtime.cpp"),
    )
    reject_regex(
        "projection count-star terminal primitive",
        (
            r"PROJECTION_COUNT_STAR_GROUPED_AGGREGATE",
            r"SljitProjectionCountStarGroupedAggregate",
            r"projection_count_star_grouped_aggregate",
            r"count_star_grouped_row_update",
            r"direct_count_star_projection_elided",
            r"cross_chunk_preaggregated_count_star_update",
        ),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
    )
    require_text(
        "test/api/test_jit_aggregate.cpp",
        (
            "JIT count-star grouped aggregate uses row-delta backend for high-cardinality batches",
            "aggregate_update.primitive_grouped_count_star_row_update=2048",
        ),
    )
    require_absent("extension/jit_sljit/include/sljit_distinct_aggregate_update_runtime.hpp")
    reject_regex(
        "stale SLJIT route-era distinct aggregate primitive",
        (
            r"\bSljit(?:Projected|RowPointer)?DistinctAggregateUpdatePrimitive\b",
            r"\bDISTINCT_AGGREGATE_UPDATE\b",
            r"distinct_count_pointer_direct_update",
            r"row_pointer_distinct_count_pointer",
            r"projected_distinct_count_pointer",
        ),
        (
            "extension/jit_sljit/**/*.cpp",
            "extension/jit_sljit/**/*.hpp",
        ),
    )


def verify_exact_source_cardinality_contract() -> None:
    require_text(
        "src/include/duckdb/execution/perfect_aggregate_hashtable.hpp",
        (
            "idx_t OccupiedCount() const",
            "idx_t occupied_count",
        ),
    )
    require_text(
        "src/execution/perfect_aggregate_hashtable.cpp",
        (
            "occupied_count(0)",
            "occupied_count++",
            "idx_t PerfectAggregateHashTable::OccupiedCount() const",
        ),
    )
    require_text(
        "src/include/duckdb/execution/radix_partitioned_hashtable.hpp",
        ("optional_idx FinalizedCount(GlobalSinkState &sink) const",),
    )
    require_text(
        "src/execution/radix_partitioned_hashtable.cpp",
        (
            "optional_idx RadixPartitionedHashTable::FinalizedCount(GlobalSinkState &sink_p) const",
            "if (!sink.finalized)",
            "partition->state != AggregatePartitionState::READY_TO_SCAN",
            "partition->data_contains_duplicate_rows",
            "result += partition->data->Count()",
        ),
    )
    require_text(
        "src/include/duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp",
        ("optional_idx FinalizedSourceCardinality() const",),
    )
    require_text(
        "src/include/duckdb/execution/operator/aggregate/physical_perfecthash_aggregate.hpp",
        ("optional_idx FinalizedSourceCardinality() const",),
    )
    require_text(
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
        (
            "optional_idx PhysicalHashAggregate::FinalizedSourceCardinality() const",
            "table_data.FinalizedCount",
        ),
    )
    require_text(
        "src/execution/operator/aggregate/physical_perfecthash_aggregate.cpp",
        (
            "optional_idx PhysicalPerfectHashAggregate::FinalizedSourceCardinality() const",
            "return gstate.ht->OccupiedCount()",
        ),
    )
    require_text(
        "src/execution/execution_contract.cpp",
        ("ApplyExecutionContractFinalizedSourceCardinality(result, FinalizedSourceCardinality())",),
    )
    require_text(
        "src/execution/execution_region_ir.cpp",
        (
            "EstimateExecutionRegionCandidateCardinality",
            "source.kind == ExecutionRegionNodeKind::SOURCE && source.estimated_cardinality_exact",
            "exact source cardinality must cap the runner cost model",
            "return MinValue(source.estimated_cardinality, downstream_estimate)",
        ),
    )


def verify_group_estimate_contract() -> None:
    require_text(
        "extension/jit_sljit/include/sljit_region_plan.hpp",
        (
            "struct SljitAggregateGroupReservePlan",
            "bool has_group_count = false",
            "bool CanReserve() const",
            "SljitAggregateGroupReservePlan group_reserve",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_region_executable.cpp",
        (
            "SljitTryBuildExecutableAggregateGroupReservePlan",
            "const vector<idx_t> &current_distinct_counts",
            "group.supported_reference",
            "group.input_index",
            "reserve.group_count = reserve_count",
            "plan.estimated_input_count > 0 && reserve_count >= plan.estimated_input_count",
        ),
    )
    require_text(
        "src/include/duckdb/execution/execution_region_ir.hpp",
        (
            "source_contract_input_distinct_counts",
            "source_contract_input_distinct_reserve_counts",
        ),
    )
    require_text(
        "src/execution/execution_region_graph.cpp",
        (
            "BuildExecutionRegionDistinctCount",
            "BuildExecutionRegionDistinctReserveCount",
            "source_contract_input_distinct_counts",
            "source_contract_input_distinct_reserve_counts",
            "source_cardinality == 0",
            "contract.estimated_source_cardinality",
            "MinValue(stats.GetDistinctCount(), source_cardinality)",
            "distinct_count == 0 || distinct_count >= source_cardinality",
            "return distinct_count",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_region_plan.cpp",
        (
            "BuildSljitSourceDistinctCountsForContractPlan",
            "BuildSljitSourceDistinctReserveCountsForContractPlan",
            "source_contract_input_distinct_counts",
            "source_contract_input_distinct_reserve_counts",
            "native_region.source_distinct_counts =",
            "native_region.source_distinct_reserve_counts =",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_region_executable.cpp",
        (
            "current_distinct_reserve_counts",
            "SljitTryBuildExecutableAggregateDenseGroupDomain(op.aggregate_update, current_distinct_counts",
            "SljitTryBuildExecutableAggregateGroupReservePlan(op.aggregate_update, current_distinct_reserve_counts",
            "SljitUpdateExecutableCurrentDistinctReserveCounts(op, current_distinct_reserve_counts",
            "current_distinct_counts",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_executable_stats.cpp",
        (
            "BuildSljitHashJoinProbeOutputDistinctReserveCounts",
            "SljitHashJoinProbeMayDuplicateProbeRows",
            "SljitTryGetHashJoinRHSOutputConditionIndex",
            "SljitTryGetHashJoinProbeKeyInputIndex",
            "rhs_output_column_indices",
            "ExecutionRegionJoinType::INNER",
            "ExecutionRegionJoinType::LEFT",
            "ExecutionRegionJoinType::RIGHT",
            "ExecutionRegionJoinType::OUTER",
            "input_distinct_reserve_counts",
            "input_distinct_counts",
        ),
    )
    require_text(
        "src/include/duckdb/execution/execution_region_ir.hpp",
        ("vector<idx_t> rhs_output_column_indices",),
    )
    require_text(
        "src/execution/execution_contract.cpp",
        (
            "result.rhs_output_column_indices = join.rhs_output_columns.col_idxs",
            "rhs_output_column_indices=",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_executable_range_stats.cpp",
        (
            "SljitTryCastHashJoinEqualityRangeValue",
            "SljitTryGetHashJoinRHSOutputConditionIndex",
            "SljitTryGetHashJoinProbeKeyInputIndex",
            "ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_state_address_update_runtime.hpp",
        (
            "SljitTryReserveGroupedAggregateGroups",
            "auto &reserve = op.aggregate_update.plan.group_reserve",
            "runtime.TryMarkOnce(ExecutionRegionRuntimeOnceFlag::AGGREGATE_GROUP_RESERVE, op_idx)",
            "const auto reserve_group_count = MaxValue<idx_t>(reserve.group_count, STANDARD_VECTOR_SIZE)",
            "grouped_aggregate_reserve_target",
            "ReserveGroups(reserve_group_count, recorder)",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_direct_update_runtime.hpp",
        ("SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state)",),
    )
    require_text(
        "extension/jit_sljit/include/sljit_row_pointer_grouped_aggregate_update_runtime.hpp",
        (
            "SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, binding.aggregate_update.grouped_state)",
            "SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state)",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_direct_update_runtime.hpp",
        (
            "TryReservePreaggregatedGroupedPrimitiveGroups",
            "preaggregated_grouped_primitive_reserve_target",
        ),
    )
    require_text(
        "test/api/test_jit_aggregate.cpp",
        (
            "JIT preaggregated grouped aggregate avoids source-row reserve",
            "JIT join-expanded unique group keys reserve input-vector aggregate groups",
            "VACUUM jit_preaggregated_group_reserve",
            "aggregate_update.grouped_aggregate_reserve_target=200000",
            "aggregate_update.grouped_aggregate_reserve_target=",
            "aggregate_update.grouped_aggregate_reserve_target=400000",
            "grouped_aggregate_reserve.reserve_groups.resize=",
            "direct_append_preaggregated_grouped_primitive_update.find_new.resize",
        ),
    )
    require_text(
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        ("virtual bool ReserveGroups(idx_t group_count",),
    )
    require_text(
        "src/include/duckdb/execution/execution_region_runtime.hpp",
        (
            "enum class ExecutionRegionRuntimeOnceFlag",
            "AGGREGATE_GROUP_RESERVE",
            "virtual idx_t MaxThreads() const = 0",
            "virtual bool TryMarkOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index)",
        ),
    )
    require_text(
        "src/include/duckdb/parallel/execution_region_pipeline_adapter.hpp",
        ("idx_t MaxThreads() const",),
    )
    require_text(
        "src/parallel/execution_region_pipeline_adapter.cpp",
        (
            "idx_t ExecutionRegionPipelineAdapter::MaxThreads() const",
            "return executor.pipeline.GetMaxThreads()",
        ),
    )
    require_text(
        "src/execution/execution_region_runner.cpp",
        (
            "idx_t MaxThreads() const override",
            "return pipeline.MaxThreads()",
        ),
    )
    require_text(
        "src/include/duckdb/parallel/pipeline_executor.hpp",
        ("execution_region_runtime_once_flags",),
    )
    require_text(
        "src/parallel/pipeline_executor.cpp",
        (
            "TryMarkExecutionRegionRuntimeOnceFlag",
            "execution_region_runtime_once_flags",
            "flags[index] = true",
        ),
    )
    require_text(
        "src/execution/aggregate_hashtable.cpp",
        (
            "bool GroupedAggregateHashTable::ReserveGroups(idx_t group_count)",
            "STANDARD_VECTOR_SIZE",
            "const auto target_count = group_count + append_slack",
            "GetCapacityForCount(target_count)",
        ),
    )
    reject_text(
        "src/execution/radix_partitioned_hashtable.cpp",
        ("group_count <= ht.Count() || (group_count <= ht.Capacity() && group_count <= ht.ResizeThreshold())",),
    )
    reject_regex(
        "estimated aggregate group reservation in JIT runtime",
        (
            "estimated_group_count",
            "SljitApplyAggregateReserveUpperBound",
            "TryReserveGroups",
            "reserve_group_capacity",
            r"\bReserveAdditionalGroups\b",
            r"reserve_additional_groups",
            r"ReserveGroups\([^;\n]*compact_groups\.size\(",
            r"ReserveGroups\([^;\n]*input\.size\(",
            r"ReserveGroups\([^;\n]*estimated_input_count",
            r"group_reserve_applied",
            r"AggregateGroupReserveApplied",
            r"MarkAggregateGroupReserveApplied",
        ),
        (
            "extension/jit_sljit/**/*.cpp",
            "extension/jit_sljit/**/*.hpp",
            "src/include/duckdb/execution/execution_operator_runtime.hpp",
            "src/include/duckdb/execution/radix_partitioned_hashtable.hpp",
            "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
            "src/execution/radix_partitioned_hashtable.cpp",
        ),
    )


def verify_no_whole_executor_fallbacks() -> None:
    reject_regex(
        "whole-executor fallback in compiled layers",
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


def main() -> None:
    verify_required_design_files()
    verify_no_benchmark_shaped_production_logic()
    verify_no_benchmark_shaped_jit_api_tests()
    verify_layer_boundaries()
    verify_no_deprecated_verification_pragmas()
    verify_stale_route_code_removed()
    verify_runtime_batch_view()
    verify_runtime_proof_ownership()
    verify_hash_probe_key_source_contract()
    verify_projection_aggregate_descriptor_boundary()
    verify_cost_fact_ownership_boundary()
    verify_preaggregated_primitive_batch_contract()
    verify_perfect_hash_aggregate_capability_contract()
    verify_regular_hash_aggregate_lookup_contract()
    verify_row_pointer_grouped_lookup_contract()
    verify_executable_source_fact_contract()
    verify_primitive_sequence()
    verify_recipe_builder()
    verify_native_tail_and_deferred_finish()
    verify_distinct_aggregate_backend()
    verify_exact_source_cardinality_contract()
    verify_group_estimate_contract()
    verify_no_whole_executor_fallbacks()
    print("Execution-region architecture verification passed")


if __name__ == "__main__":
    main()
