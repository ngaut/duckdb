//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_count_star_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_count_star_preaggregation.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_count_star_update_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

template <class T, class EXECUTE_UNBATCHED, class EXECUTE_NATIVE_AGGREGATE_UPDATE>
static bool SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    EXECUTE_UNBATCHED &&execute_unbatched, EXECUTE_NATIVE_AGGREGATE_UPDATE &&execute_native_aggregate_update) {
	static constexpr idx_t PROJECTION_IDX = 0;
	static constexpr idx_t AGGREGATE_IDX = 1;
	auto &projection_op = ops[PROJECTION_IDX];
	auto &aggregate_op = ops[AGGREGATE_IDX];
	auto &native_runtime = runtime.ExecutionOperators();
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);

	vector<LogicalType> group_types;
	group_types.reserve(sink_info.groups.size());
	for (auto &group : sink_info.groups) {
		group_types.push_back(group.type);
	}
	DataChunk accumulated_groups;
	accumulated_groups.Initialize(runtime.GetAllocator(), group_types);
	SljitCountStarGroupedAggregateUpdateDescriptor count_star_update;
	if (!SljitTryPrepareCountStarGroupedAggregateUpdate(runtime, native_runtime, scratch, AGGREGATE_IDX, aggregate_op,
	                                                    accumulated_groups, count_star_update)) {
		return execute_unbatched();
	}

	std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> accumulated_keys;
	std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> accumulated_counts;
	idx_t accumulated_group_count = 0;
	idx_t accumulated_input_count = 0;
	vector<int64_t> accumulated_deltas;
	DataChunk chunk_groups;
	chunk_groups.Initialize(runtime.GetAllocator(), group_types);
	vector<int64_t> chunk_deltas;

	auto flush_accumulated_groups = [&]() {
		if (accumulated_group_count == 0) {
			return;
		}
		MaterializePreaggregatedCountStarGroups(accumulated_keys, accumulated_counts, accumulated_group_count,
		                                        accumulated_groups, accumulated_deltas);
		if (!TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		        runtime, scratch, AGGREGATE_IDX, aggregate_op, accumulated_groups, accumulated_deltas,
		        count_star_update, accumulated_input_count, false, nullptr)) {
			throw InternalException("SLJIT projection count-star grouped accumulator failed to flush");
		}
		RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "cross_chunk_preaggregated_count_star_update",
		                             accumulated_input_count);
		accumulated_group_count = 0;
		accumulated_input_count = 0;
	};

	auto merge_preaggregated_groups = [&](DataChunk &preaggregated_groups, const vector<int64_t> &preaggregated_deltas,
	                                      idx_t input_count) -> bool {
		if (!MergePreaggregatedFixedWidthCountStarGroupsTemplated<T>(preaggregated_groups, preaggregated_deltas,
		                                                             accumulated_keys, accumulated_counts,
		                                                             accumulated_group_count)) {
			flush_accumulated_groups();
			if (!MergePreaggregatedFixedWidthCountStarGroupsTemplated<T>(preaggregated_groups, preaggregated_deltas,
			                                                             accumulated_keys, accumulated_counts,
			                                                             accumulated_group_count)) {
				return false;
			}
		}
		accumulated_input_count += input_count;
		return true;
	};

	auto execute_projected_fallback = [&](DataChunk &projected) -> bool {
		flush_accumulated_groups();
		auto sink_result = execute_native_aggregate_update(scratch, AGGREGATE_IDX, aggregate_op, projected);
		return SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
	};

	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	idx_t fetched_chunks = 0;

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return SljitStopFullPipelineAfterFinalize(result, ExecutionRegionResult::INTERRUPTED,
			                                          flush_accumulated_groups);
		}
		if (source_chunk.size() > 0) {
			auto direct_preaggregate_stage_start = SljitRegionStageStart(runtime);
			if (TryPreaggregateProjectedFixedWidthCountStarGroups(projection_op, source_chunk, chunk_groups,
			                                                      chunk_deltas) &&
			    merge_preaggregated_groups(chunk_groups, chunk_deltas, source_chunk.size())) {
				RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
				                              "direct_source_preaggregate_count_star_groups",
				                              direct_preaggregate_stage_start);
				RecordSljitRegionRuntimePath(runtime, projection_op.kind, "direct_count_star_projection_elided",
				                             source_chunk.size());
				RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind,
				                                         "direct_count_star_projection_elided", source_chunk.size());
			} else {
				auto &projected = scratch.TemporaryChunk(PROJECTION_IDX);
				projected.Reset();
				auto projection_stage_start = SljitRegionStageStart(runtime);
				SljitExecuteProjection(scratch, PROJECTION_IDX, projection_op, source_chunk, projected);
				RecordSljitRegionStageRuntime(runtime, PROJECTION_IDX, projection_op.kind, projection_stage_start);
				if (projected.size() > 0) {
					auto preaggregate_stage_start = SljitRegionStageStart(runtime);
					if (!TryPreaggregateFixedWidthCountStarGroups(projected, chunk_groups, chunk_deltas) ||
					    !merge_preaggregated_groups(chunk_groups, chunk_deltas, projected.size())) {
						if (execute_projected_fallback(projected)) {
							return true;
						}
					} else {
						RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
						                              "cross_chunk_preaggregate_count_star_groups",
						                              preaggregate_stage_start);
					}
				}
			}
		}
		return false;
	};

	return SljitRunFullPipelineSourceContractLoopAfterFinalize(
	    runtime, result, fetched_chunks, [&]() { return fetched_chunks >= max_source_fetches; }, execute_source_chunk,
	    flush_accumulated_groups);
}

template <class EXECUTE_UNBATCHED, class EXECUTE_NATIVE_AGGREGATE_UPDATE>
static bool SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    EXECUTE_UNBATCHED &&execute_unbatched, EXECUTE_NATIVE_AGGREGATE_UPDATE &&execute_native_aggregate_update) {
	auto &group_type = ops[1].aggregate_update.plan.sink_info.groups[0].type;
	switch (group_type.InternalType()) {
	case PhysicalType::INT8:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<int8_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::INT16:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<int16_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::INT32:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<int32_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::INT64:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<int64_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::INT128:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<hugeint_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::UINT8:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uint8_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::UINT16:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uint16_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::UINT32:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uint32_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::UINT64:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uint64_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	case PhysicalType::UINT128:
		return SljitTryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uhugeint_t>(
		    runtime, result, ops, execute_unbatched, execute_native_aggregate_update);
	default:
		return execute_unbatched();
	}
}

} // namespace duckdb
