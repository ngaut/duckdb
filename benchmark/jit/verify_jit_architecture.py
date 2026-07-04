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
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_primitive.hpp",
        "extension/jit_sljit/include/sljit_join_projection_aggregate_update_runtime.hpp",
        "extension/jit_sljit/include/sljit_mark_probe_filter_boundary.hpp",
        "extension/jit_sljit/include/sljit_native_tail_handoff_runtime.hpp",
        "extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp",
        "extension/jit_sljit/include/sljit_hash_join_projection_source_runtime.hpp",
        "extension/jit_sljit/include/sljit_projection_aggregate_descriptor.hpp",
        "extension/jit_sljit/include/sljit_projection_chain_runtime.hpp",
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp",
        "src/include/duckdb/execution/aggregate_hashtable.hpp",
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        "src/execution/aggregate_hashtable.cpp",
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
    ):
        require_file(path)
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


def verify_no_benchmark_shaped_production_logic() -> None:
    reject_regex(
        "benchmark-shaped production JIT logic",
        (
            r"\bQ[0-9]+(?:-like)?\b",
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
        ),
        ("extension/jit_sljit/**/*.hpp", "extension/jit_sljit/**/*.cpp"),
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
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp",
        (
            "struct SljitGroupedAggregateUpdatePrimitive",
            "enum class SljitGroupedAggregateUpdateStrategyKind",
            "SljitChooseGroupedAggregateUpdateStrategy",
            "SljitGroupedAggregateUpdateHasDedicatedBackend",
            "SljitBindGroupedAggregateUpdatePrimitive",
            "SljitGroupedAggregateUpdateRuntimeState",
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
            "SljitTryExecuteDistinctCountPointerRowPointerGroupAggregateUpdate",
            "direct_projection_distinct_count_pointer_row_pointer_update",
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
        (
            "input_vector_repeats_with_row_pointer",
            "EXECUTION_DISTINCT_COUNT_POINTER_INLINE_PAYLOAD_CAPACITY",
        ),
    )
    require_text(
        "src/include/duckdb/execution/operator/aggregate/distinct_count_pointer_set.hpp",
        (
            "UseGlobalPayloadSet",
            "use_global_payload_set",
        ),
    )
    require_text(
        "src/execution/operator/aggregate/distinct_count_pointer_set.cpp",
        (
            "DistinctCountPointerSet::UseGlobalPayloadSet",
            "ReserveGlobalPayloadEntries",
            "global_payload_set_reserve_target",
            "use_global_payload_set = true",
            "PromoteToOverflow(group)",
            "groups.back().uses_overflow = use_global_payload_set",
        ),
    )
    require_text(
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        ("UseGlobalPayloadSet",),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime.hpp",
        (
            "SljitDistinctCountPointerPayloadStorageStrategy",
            "SljitSelectDistinctCountPointerPayloadStorageStrategy",
            "ADAPTIVE_INLINE_GROUP_PAYLOADS",
            "GLOBAL_PAIR_SET",
            "EXECUTION_DISTINCT_COUNT_POINTER_INLINE_PAYLOAD_CAPACITY",
            "inline_target_payload_capacity",
            "!reserve.CanReserve()",
            "op.aggregate_update.plan.estimated_input_count",
            "distinct.state->UseGlobalPayloadSet(global_payload_set_target)",
            "distinct_count_pointer_global_payload_set",
        ),
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
            "SljitGeneratedAllValidRegularHashJoinProbeStage(false, mark_selection_mode)",
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
            "marker omission requires a filtered projection",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "const auto max_recipe_batches = recipe.uses_extended_source_fetch_budget",
            "terminal_runtime.BudgetReached(runtime, TerminalStep(), max_recipe_batches)",
            "fetched_chunks >= max_recipe_batches",
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
        "extension/jit_sljit/include/sljit_hash_join_projected_aggregate_runtime.hpp",
        ("SljitDownstreamRowBudgetReached(processed_output_rows, max_recipe_batches)",),
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
    require_text(
        "test/api/test_jit_join.cpp",
        (
            "JIT mark filter projection native tail uses boundary primitive",
            "JIT first hash join native tail uses source batch boundary recipe",
            "JIT mark match filter emits selected boundary without marker flags",
            "JIT two-join mark distinct aggregate uses row-pointer distinct backend",
            "JIT distinct aggregate uses global pair set for high-payload probe groups",
            "aggregate_update.distinct_count_pointer_row_pointer_group_key_update=",
            "aggregate_update.direct_projection_distinct_count_pointer_row_pointer_update=",
            "aggregate_update.direct_projection_aggregate_input.projection_output=",
            "aggregate_update.direct_projection_aggregate_input.hash_join_lhs_input=",
            "aggregate_update.distinct_count_pointer_global_payload_set=",
            "hash_join_probe.selected_mark_probe_input=",
            "hash_join_probe.mark_flags=",
            "hash_join_probe.mark_match_selection_reference=",
            "hash_join_probe.mark_nonmatch_selection_reference=",
            "hash_join_probe.mark_filter_lhs_selected_view=",
            "hash_join_probe.generated_regular_probe_mark_match",
            "hash_join_probe.generated_regular_probe_mark_nonmatch_flat_all_valid_",
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
        ("extension/jit_sljit/include/sljit_join_projection_aggregate_primitive.hpp",),
    )
    reject_regex(
        "direct aggregate strategy owns pending-batch source-key proof",
        (r"(?s)struct\s+SljitDirectJoinOutputAggregateStrategy\s*\{[^}]*source_key0_int64_to_int32",),
        ("extension/jit_sljit/include/sljit_direct_join_output_aggregate_state.hpp",),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_hash_join_projected_aggregate_runtime.hpp",
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
            "enum class SljitDirectJoinOutputAggregateUpdateSchedule",
            "PENDING_ROW_POINTER_BATCH",
            "IMMEDIATE_ROW_POINTER_UPDATE",
            "SljitAggregateUpdateHasDedicatedCompiledBackend",
            "distinct_count_pointer_keys",
            "op.aggregate_update.plan.use_primitive_payloads",
            "SljitAggregateUpdateHasDedicatedCompiledBackend(ops[aggregate_idx])",
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
            "result.generated_backend_stage_count += candidate.traits.mark_probe_filter_count",
            "traits.generated_aggregate_update_count",
            "traits.generated_distinct_count_pointer_aggregate_update_count",
            "candidate.traits.generated_distinct_count_pointer_aggregate_update_count",
            "input.generated_distinct_count_pointer_aggregate_update_count +=",
            "generated_backend_stage_count",
            "facts.native_aggregate_stage_count -= aggregate_decrement",
            "facts.native_grouped_aggregate_stage_count -= grouped_decrement",
            "idx_t generated_aggregate_update_count = 0",
            "idx_t generated_distinct_count_pointer_aggregate_update_count = 0",
            "facts.generated_aggregate_update_count++",
            "facts.generated_distinct_count_pointer_aggregate_update_count++",
            "cost_input.generated_distinct_count_pointer_aggregate_update_count +=",
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
            "traits.generated_distinct_count_pointer_aggregate_update_count++",
        ),
    )
    require_text(
        "src/execution/execution_region_description.cpp",
        (
            "mark_probe_filters=",
            "generated_distinct_count_pointer_aggregate_updates=",
        ),
    )
    require_text(
        "src/planner/cost_model.cpp",
        (
            "PhysicalRunnerUsesGeneratedDistinctCountPointerBackend",
            "input.generated_distinct_count_pointer_aggregate_update_count > 0",
            "PhysicalRunnerMaterializationElisionBenefitCanPay",
            "PhysicalRunnerGeneratedBackendStageBenefitCanPay",
            "PhysicalRunnerCostedGeneratedBackendStageCount",
            "input.native_hash_join_build_sink_count > 0",
            "input.generated_distinct_count_pointer_aggregate_update_count == 0",
            "generated_backend_stage_count",
            "generated_expression_stage_count",
            "generated_backend_stage_work",
            "profile.generated_stage_work = AddCost(generated_expression_stage_work, profile.generated_backend_stage_work)",
        ),
    )
    reject_text(
        "src/execution/execution_region_cost_input.cpp",
        (
            "input.native_distinct_count_pointer_aggregate_stage_count +=\n\t    candidate.traits.generated_distinct_count_pointer_aggregate_update_count",
            "cost_input.native_distinct_count_pointer_aggregate_stage_count +=\n\t    facts.generated_distinct_count_pointer_aggregate_update_count",
        ),
    )
    require_text(
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
            "SljitTryExecuteRowPointerPreaggregatedGroupedAggregateUpdateStrategy",
            "SljitTryExecuteInputVectorGroupedAggregateUpdate",
            "TryFindOrCreateInputVectorGroupStateTargets",
            "targets, recorder, dense_domain",
            "direct_input_vector_group_count_one_lookup",
            "direct_input_vector_group_count_one_update",
            "FUSED_TARGET_PAYLOAD",
            "SPLIT_PAYLOAD",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_row_pointer_grouped_aggregate_update_runtime.hpp",
        (
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
            "GROUPED_AGGREGATE_UPDATE",
            "NATIVE_TAIL_HANDOFF",
            "SljitGeneratedFilterPrimitive generated_filter",
            "SljitHashJoinProbeSelectionPrimitive hash_join_probe_selection",
            "SljitProjectionChainPrimitive projection_chain",
            "SljitJoinProjectionAggregateUpdatePrimitive join_projection_aggregate_update",
            "SljitGroupedAggregateUpdatePrimitive grouped_aggregate_update",
            "SourceBatchBoundary(idx_t op_idx)",
            "GeneratedFilter(const SljitGeneratedFilterPrimitive &primitive)",
            "HashJoinProbeSelection(const SljitHashJoinProbeSelectionPrimitive &primitive)",
            "ProjectionChain(const SljitProjectionChainPrimitive &primitive)",
            "JoinProjectionAggregateUpdate(const SljitJoinProjectionAggregateUpdatePrimitive &primitive)",
            "GroupedAggregateUpdate(const SljitGroupedAggregateUpdatePrimitive &primitive)",
            "step.op_count >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVE_STEP_OPS",
            "exceeds the maximum operator count",
            "count >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES",
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
            "SljitCanBindGroupedAggregateUpdatePrimitive",
            "step.grouped_aggregate_update",
            "SljitCanBindNativeTailHandoffPrimitive",
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
            '{"source", &SljitProjectionAggregateRecipeBuilder::TryBuildSourcePrefix}',
            '{"single_join", &SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinPrefix}',
            '{"two_join", &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinPrefix}',
            "SourcePrefixHasCountStarGroupedBackend",
            "SelectSingleJoinStrategy",
            "SelectTwoJoinStrategy",
            "SingleJoinHasMarkFilterBoundary",
            "TwoJoinHasMarkFilterBoundary",
            "SljitProjectionAggregatePrefixKind::SINGLE_JOIN",
            "SljitProjectionAggregatePrefixKind::TWO_JOIN",
            "!facts.HasMarkFilter()",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_recipe.hpp",
        (
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
            "TryBuildSourceGrouped",
            "TryBuildSingleMarkFilter",
            "TryBuildSingleSourceFilterProjection",
            "TryBuildSinglePreProjection",
            "TryBuildSingleDirectProjection",
            "TryBuildSingleProjectionChain",
            "TryBuildTwoJoinMarkFilter",
            "TryBuildTwoJoinSourceFilterProjection",
            "TryBuildTwoJoinPreProjection",
            "TryBuildTwoJoinBetweenProjection",
            "TryBuildTwoJoinDirectProjection",
            "TryBuildTwoJoinProjectionChain",
            "SljitProjectionAggregateCanUseSourceCountStarGroupedAggregate",
            "SljitProjectionAggregateCanUseSingleJoinMarkFilterBoundary",
            "SljitProjectionAggregateCanUseSingleJoinSourceFilterProjection",
            "SljitProjectionAggregateCanUseSingleJoinPreJoinProjection",
            "SljitProjectionAggregateCanUseSingleJoinSingleProjection",
            "SljitProjectionAggregateCanUseSingleJoinProjectionChain",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_native_tail_recipe.hpp",
        (
            "SljitTryAnalyzeMarkFilterProjectionNativeTail(ops, facts)",
            "SljitTryAnalyzeGeneratedFilterProjectionNativeTail(ops, facts)",
            "SljitTryAnalyzeProjectionFilterProjectionNativeTail(ops, facts)",
            "SljitTryAnalyzeSourceBatchNativeTail(ops, uses_scan_filters, facts)",
            "class SljitNativeTailRecipeBuilder",
            "mark_filter_projection_native_tail",
            "source_batch_native_tail",
            "TryBuildSourceBatch",
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
            "SljitBindProjectionChainPrimitive(ops, facts.pre_projection_idx)",
            "SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx)",
            "SljitBindProjectionChainPrimitive(ops, facts.projection_idx)",
            "SljitFullPipelinePrimitiveStep::ProjectionChain(pre_projection)",
            "SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter)",
            "SljitFullPipelinePrimitiveStep::ProjectionChain(projection)",
            "MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx)",
            "MakeMarkFilterPrefix(facts.hash_join_idx, facts.filter_idx, true, facts.first_projection_idx)",
            "MakeProjectionNativeTailRecipe(std::move(sequence), facts.first_projection_idx",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
        (
            "SljitFullPipelinePrimitiveSequenceBatchExecutor",
            "SljitFullPipelinePrimitiveSequenceIsExecutable",
            "SljitFullPipelinePrimitiveSequenceTerminalStep",
            "step.generated_filter",
            "step.projection_chain",
            "hash_join_materialize_batches",
            "projection_chain_batches",
            "const auto max_recipe_batches = recipe.uses_extended_source_fetch_budget",
            "SljitBatchedSourceContractFetchBudget(runtime.MaxChunks())",
            "processed_batches >= max_recipe_batches",
            "terminal_runtime.BudgetReached(runtime, TerminalStep(), max_recipe_batches)",
            "fetched_chunks >= max_recipe_batches",
            "for (idx_t step_idx = 1; step_idx + 1 < recipe.primitive_sequence.count; step_idx++)",
            "FlushHashJoinMaterializeBatch(step_idx)",
            "FlushProjectionChainBatch(step_idx)",
            "FlushSourceBoundaryBatch(step_idx)",
            "AppendHashJoinMaterializeBatch(step_idx, step, output, next_step_idx)",
            "hash_join_materialize_batch_append",
            "ExecuteSourceBatchBoundary",
            "source_batch_boundary",
            "source_batch_boundary_append",
            "SljitFullPipelineTerminalRuntime<EXECUTE_HASH_JOIN_PROBE> terminal_runtime",
            "terminal_runtime.Prepare",
            "terminal_runtime.Execute",
            "terminal_runtime.Flush",
            "execute_native_full_pipeline_from.Finalize(scratch)",
            "ExecuteHashJoinProbeSelection",
            "SljitRuntimeBatchViewFromHashJoinSelection",
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
            "SljitBindProjectionChainInput",
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
        "extension/jit_sljit/include/sljit_generated_filter_projection_runtime.hpp",
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
            "grouped_aggregate_update.Execute",
            "grouped_aggregate_update.Flush",
            "join_projection_aggregate_update.Execute",
            "join_projection_aggregate_update.Flush",
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
        "extension/jit_sljit/include/sljit_hash_join_projected_aggregate_runtime.hpp",
        (
            "input.hash_join_output_column_map",
            "input.hash_join_output_projection_idx",
            "SLJIT mapped selected join-output aggregate descriptor failed",
            "direct_join_output_aggregate_strategy->last_failure",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_hash_join_projected_aggregate_runtime.hpp",
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
            "SljitTryBuildInt64ToInt32PreJoinProjection",
            "SljitPreJoinProjectionViewColumnKind::INT64_TO_INT32_CAST",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_primitive.hpp",
        (
            "struct SljitHashJoinProbeInputRemap",
            "key_input_indices",
            "residual_probe_source_indices",
            "vector<idx_t> output_column_map",
            "HasOutputColumnMap",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_hash_join_probe_executor_runtime.hpp",
        (
            "struct SljitHashJoinProbeExecutionContractView",
            "SljitApplyHashJoinResidualProbeSourceRemap",
            "SljitBuildHashJoinProbeExecutionContractView",
            "plan.Copy(false)",
            "view.remapped_operator_info.hash_join_keys[key_idx].input_index",
            "view.remapped_plan.operator_info = view.remapped_operator_info",
            "remapped hash join probe input requires selected-view execution",
            "remapped hash join residual probe source type mismatch",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_source_pipeline_runtime.hpp",
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
            "SljitPreJoinProjectionViewDescriptor pre_join_view",
            "pre_join_view.CanElideProjectionWithCurrentHashProbe()",
            "input_remap.key_input_indices = pre_join_view.hash_probe_key_source_indices",
            "input_remap.residual_probe_source_indices = pre_join_view.residual_probe_source_indices",
            "pre_join_view.projected_to_source",
            "remapped_hash_join_selection",
        ),
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
        "extension/jit_sljit/include/sljit_generated_filter_projection_runtime.hpp",
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
            "SljitBindProjectionChainPrimitive(ops, shape.first_projection_idx",
            "SljitBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)",
            "SljitFullPipelinePrimitiveStep::ProjectionChain(projection)",
            "SljitBindGroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx)",
            "SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update)",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_binding.hpp",
        ("for (idx_t projection_idx = shape.first_projection_idx", "for (idx_t projection_idx = first_projection_idx"),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime.hpp",
        (
            "distinct_count_pointer_selected_payload_update",
            "TryUpdateNewGroupsWithSelectedStateAddresses",
            "AddSelectedPayloads",
            "distinct_count_pointer_selected_payload_update_miss",
        ),
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
            "recipe.primitive_sequence.count == 0",
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
            "const SljitSourceBatchNativeTailFacts &facts",
            "SljitFullPipelinePrimitiveStep::SourceBatchBoundary(facts.boundary_op_idx)",
            "facts.tail_start_idx",
            "MakeHashJoinProbeMaterializeStep",
            "MakeMarkFilterPrefix",
            "MakeTwoJoinMarkFilterPrefix",
            "MakeMarkFilterNativeTailRecipe",
            "MakeSourceProjectionGroupedAggregateRecipe",
            "MakeProjectionGroupedAggregateRecipe",
            "MakeProjectionAggregateTailRecipe",
            "MakeProjectionNativeTailRecipe",
            "SelectedProjectionAggregateHasDedicatedBackend",
            "SljitFullPipelinePrimitiveSequence sequence",
            "SljitFullPipelinePrimitiveStep::SourceBatchBoundary",
            "SljitFullPipelinePrimitiveStep::HashJoinProbeSelection",
            "SljitFullPipelinePrimitiveStep::ProjectionChain",
            "SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate",
            "SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate",
            "BindPostJoinProjectionAggregatePrimitive",
            "MakeSourceHashJoinProbeSelectionSequence",
            "MakeSourceHashJoinProjectionInputSequence",
            "MakeTwoJoinDirectProjectionAggregateRecipe",
            "MakeHashJoinProbeProjectionInputStep",
            "sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(hash_join_idx))",
            "sequence.Add(MakeHashJoinProbeProjectionInputStep(first_hash_join_idx))",
            "MakeHashJoinProbeSelectionStep(second_hash_join_idx)",
            "SljitMakeSelectedJoinOutputAggregateUpdatePrimitive",
            "MakeFilterProjectionTwoJoinProjectionAggregateRecipe",
            "MakeBetweenProjectionTwoJoinProjectionAggregateRecipe",
            "MakePreProjectionTwoJoinProjectionAggregateRecipe",
            "return MakeProjectionAggregateTailRecipe(std::move(sequence), shape)",
            "return MakeProjectionGroupedAggregateRecipe(std::move(sequence), shape)",
            "return MakeProjectionNativeTailRecipe(std::move(sequence), shape)",
            "SljitFullPipelinePrimitiveStep::NativeTailHandoff",
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
            "SljitAnalyzeFullPipelineScheduleFacts(ops_p, uses_scan_filters_p)",
            "schedule_facts.uses_extended_source_fetch_budget",
            "SljitTryAnalyzeSelectedJoinAggregate(ops, facts)",
            "SljitTryAnalyzeHashJoinDelimJoinSink(ops, facts)",
            "SljitTryAnalyzeProjectionAggregatePlan",
            "SljitProjectionAggregateRecipeBuilder(ops, binding).Build(recipe, plan)",
            "SljitNativeTailRecipeBuilder(ops, uses_scan_filters, binding).Build(recipe)",
            "struct SljitFullPipelineRecipeRegistryEntry",
            "RecipeRegistry",
            "native_tail",
            "TryBuildNativeTailRecipe",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_projection_aggregate_recipe.hpp",
        (
            "class SljitProjectionAggregateRecipeBuilder",
            "TryBuildProjectionAggregateRecipeFunction",
            "struct RegistryEntry",
            "RecipeRegistry",
            "registry[entry_idx].try_build",
            "(this->*registry[entry_idx].try_build)(recipe, plan)",
            "TryBuildSourcePrefix",
            "TryBuildSingleJoinPrefix",
            "TryBuildTwoJoinPrefix",
            "SelectSingleJoinStrategy",
            "SelectTwoJoinStrategy",
            "CanBindHashJoinProbeProjectionInput",
            "CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx)",
            "SelectedProjectionAggregateHasDedicatedBackend(shape)",
            "MakeMarkFilterNativeTailRecipe",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_full_pipeline_recipe_facts.hpp",
        (
            "struct SljitProjectionAggregatePlanFacts",
            "struct SljitProjectionAggregatePrefixFacts",
            "struct SljitFullPipelineScheduleFacts",
            "struct SljitSelectedJoinAggregateFacts",
            "struct SljitHashJoinDelimJoinSinkFacts",
            "struct SljitSourceBatchNativeTailFacts",
            "SljitAnalyzeFullPipelineScheduleFacts",
            "SljitFullPipelineUsesScanFilteredAggregateTerminal",
            "SljitTryAnalyzeSelectedJoinAggregate",
            "SljitTryAnalyzeHashJoinDelimJoinSink",
            "SljitTryAnalyzeSourceBatchNativeTail",
            "SljitTryAnalyzeProjectionAggregatePlan",
            "SljitTryAnalyzeMarkFilterProjectionNativeTail",
            "SljitTryAnalyzeGeneratedFilterProjectionNativeTail",
            "SljitTryAnalyzeProjectionFilterProjectionNativeTail",
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
            r"(?s)MakeBetweenProjectionTwoJoinProjectionAggregateRecipe.*?"
            r"sequence\.Add\(MakeHashJoinProbeMaterializeStep\(first_hash_join_idx\)\);\s*"
            r"sequence\.Add\(SljitFullPipelinePrimitiveStep::ProjectionChain\(between_projection\)\)",
            r"(?s)MakePreProjectionTwoJoinProjectionAggregateRecipe.*?"
            r"sequence\.Add\(SljitFullPipelinePrimitiveStep::ProjectionChain\(pre_join_projection\)\);\s*"
            r"sequence\.Add\(MakeHashJoinProbeMaterializeStep\(first_hash_join_idx\)\);\s*"
            r"sequence\.Add\(SljitFullPipelinePrimitiveStep::ProjectionChain\(between_projection\)\)",
            r"(?s)MakeFilterProjectionTwoJoinProjectionAggregateRecipe.*?"
            r"sequence\.Add\(SljitFullPipelinePrimitiveStep::ProjectionChain\(source_projection\)\);\s*"
            r"sequence\.Add\(MakeHashJoinProbeMaterializeStep\(first_hash_join_idx\)\);\s*"
            r"sequence\.Add\(MakeHashJoinProbeSelectionStep\(second_hash_join_idx\)\)",
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
        "extension/jit_sljit/include/sljit_native_tail_handoff_runtime.hpp",
        (
            "SljitBindNativeTailHandoffPrimitive",
            "SljitExecuteNativeTailHandoffBatch",
            "SljitBindNativeTailHandoffInput",
        ),
    )
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
    require_text(
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        (
            "struct ExecutionDistinctCountPointerUpdateState",
            "ExecutionDistinctCountPointerUpdateBinding distinct_count_pointer",
            "TryResolveDistinctCountPointerAddresses",
            "AddPayloads",
        ),
    )
    require_text(
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
        (
            "HashAggregateDistinctCountPointerUpdateState",
            "BindHashAggregateDistinctCountPointerUpdate",
            "TryResolveDistinctCountPointerAddresses",
            "HashAggregateUsesDistinctCountPointerKeys",
        ),
    )
    reject_text(
        "src/include/duckdb/execution/execution_operator_runtime.hpp",
        (
            "TryUpdateNewGroupsWithStateAddresses",
            "TryResolveDistinctCountPointerGroupAddresses",
        ),
    )
    reject_text(
        "src/execution/operator/aggregate/physical_hash_aggregate.cpp",
        (
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
        "extension/jit_sljit/sljit_region_aggregate_sink_plan.cpp",
        (
            "contract.distinct_count_pointer_keys",
            "payload_update=duckdb-distinct-count-pointer",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_runtime.hpp",
        (
            "SljitExecuteDistinctCountPointerAggregateUpdate",
            "TryResolveDistinctCountPointerAddresses",
            "distinct.state->AddPayloads",
            "distinct_count_pointer_payload_set_update",
            "MarkDeferredGroupedFinish",
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
            "SljitGroupedAggregateUpdateStrategyKind::DISTINCT_COUNT_POINTER",
            "SljitGroupedAggregateUpdateStrategyKind::INVALID",
            "primitive.strategy",
            "ExecuteDistinctCountPointer",
            "ExecuteCountStarPreaggregation",
            "primitive_grouped_count_star_row_update",
        ),
    )
    reject_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_update_primitive.hpp",
        (
            "SljitGroupedAggregateUpdateStrategyKind::STANDARD",
            "ExecuteStandard",
            "SljitExecuteNativeAggregateUpdate",
        ),
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
        ),
    )
    require_text(
        "src/include/duckdb/execution/execution_region_ir.hpp",
        ("source_contract_input_distinct_reserve_counts",),
    )
    require_text(
        "src/execution/execution_region_graph.cpp",
        (
            "BuildExecutionRegionDistinctReserveCount",
            "source_contract_input_distinct_reserve_counts",
            "source_cardinality == 0",
            "DistinctStatistics::SampleRate",
            "contract.estimated_source_cardinality",
            "MinValue(stats.GetDistinctCount(), source_cardinality)",
            "std::ceil(static_cast<double>(distinct_count) / sample_rate)",
        ),
    )
    require_text(
        "extension/jit_sljit/sljit_region_plan.cpp",
        (
            "BuildSljitSourceDistinctReserveCountsForContractPlan",
            "source_contract_input_distinct_reserve_counts",
            "native_region.source_distinct_counts =",
        ),
    )
    require_text(
        "extension/jit_sljit/include/sljit_grouped_aggregate_direct_update_runtime.hpp",
        (
            "auto &reserve = op.aggregate_update.plan.group_reserve",
            "runtime.TryMarkOnce(ExecutionRegionRuntimeOnceFlag::AGGREGATE_GROUP_RESERVE, op_idx)",
            "preaggregated_grouped_primitive_reserve_target",
            "ReserveGroups(reserve.group_count, recorder)",
        ),
    )
    require_text(
        "test/api/test_jit_aggregate.cpp",
        (
            "JIT preaggregated grouped aggregate reserves source distinct groups once",
            "VACUUM jit_preaggregated_group_reserve",
            "aggregate_update.preaggregated_grouped_primitive_reserve_target=200000",
            "preaggregated_grouped_primitive_reserve.reserve_groups.resize=1",
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
            "virtual bool TryMarkOnce(ExecutionRegionRuntimeOnceFlag flag, idx_t index)",
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
