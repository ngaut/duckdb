//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_mark_join_aggregate_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_count_star_preaggregation.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_count_star_update_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_projected_grouped_aggregate_sink.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

template <class EXECUTE_HASH_JOIN_PROBE, class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitTryExecuteFullPipelineMarkHashJoinFilterProjectionGroupedAggregateBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	static constexpr idx_t HASH_JOIN_IDX = 0;
	static constexpr idx_t FILTER_IDX = 1;
	static constexpr idx_t PROJECTION_IDX = 2;
	static constexpr idx_t AGGREGATE_IDX = 3;

	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_output_rows = 0;
	bool deferred_grouped_finish = false;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	auto &hash_join_op = ops[HASH_JOIN_IDX];
	auto &filter_op = ops[FILTER_IDX];
	auto &projection_op = ops[PROJECTION_IDX];
	auto &aggregate_op = ops[AGGREGATE_IDX];
	auto &native_runtime = runtime.ExecutionOperators();
	DataChunk compact_groups;
	compact_groups.Initialize(runtime.GetAllocator(), projection_op.output_types);
	vector<int64_t> preaggregated_count_deltas;
	SljitCountStarGroupedAggregateUpdateDescriptor count_star_update;
	SljitTryPrepareCountStarGroupedAggregateUpdate(runtime, native_runtime, scratch, AGGREGATE_IDX, aggregate_op,
	                                               compact_groups, count_star_update);
	SljitProjectedGroupedAggregateSink aggregate_sink(
	    ops, runtime, native_runtime, scratch, result, PROJECTION_IDX, projection_op, AGGREGATE_IDX, aggregate_op,
	    deferred_grouped_finish, processed_output_rows, SljitProjectedGroupedAggregateProgressMode::ROWS,
	    "direct_mark_batch_append", "direct_mark_batch");

	auto try_preaggregated_count_star_batch = [&](DataChunk &projected) -> bool {
		if (projected.size() == 0 || !count_star_update.Ready()) {
			return false;
		}
		auto preaggregate_stage_start = SljitRegionStageStart(runtime);
		if (!TryPreaggregateFixedWidthCountStarGroups(projected, compact_groups, preaggregated_count_deltas)) {
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind, "local_preaggregate_count_star_groups",
		                              preaggregate_stage_start);
		if (!TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
		        runtime, scratch, AGGREGATE_IDX, aggregate_op, compact_groups, preaggregated_count_deltas,
		        count_star_update, projected.size(), true, aggregate_sink.DeferredGroupedFinishPtr())) {
			return false;
		}
		aggregate_sink.Charge(projected.size());
		return true;
	};

	auto materialize_mark_output_fallback = [&](DataChunk &join_input, idx_t mark_count, DataChunk &join_output) {
		if (!scratch.HasOperatorBinding(HASH_JOIN_IDX)) {
			throw InternalException("SLJIT MARK hash join fallback has no hash join binding");
		}
		auto &binding = scratch.OperatorBinding(HASH_JOIN_IDX).hash_join_probe;
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(runtime, HASH_JOIN_IDX, hash_join_op.kind,
		                                  "materialize_mark_output_fallback");
		RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "final_output", mark_count);
		ExecutionMaterializeHashJoinProbe(binding, join_input, scratch.HashJoinRowPointers(HASH_JOIN_IDX),
		                                  scratch.FilterSelection(HASH_JOIN_IDX), mark_count, join_output,
		                                  runtime.TraceRuntime() ? &recorder : nullptr);
		RecordSljitRegionStageRuntime(runtime, HASH_JOIN_IDX, hash_join_op.kind, "materialize_mark_output_fallback",
		                              materialize_stage_start);
	};

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return aggregate_sink.StopAfterFlushAndFinish(ExecutionRegionResult::INTERRUPTED);
		}

		SljitHashJoinProbeDrainState state;
		auto &join_output = scratch.TemporaryChunk(HASH_JOIN_IDX);
		auto &projected = scratch.TemporaryChunk(PROJECTION_IDX);
		do {
			join_output.Reset();
			string deferred_reason;
			auto bind_result = execute_hash_join_probe(scratch, HASH_JOIN_IDX, hash_join_op, source_chunk, join_output,
			                                           state, deferred_reason, false, true);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				return aggregate_sink.DeferAfterFlushAndFinish(deferred_reason);
			}
			const auto mark_count = join_output.size();
			if (mark_count == 0) {
				continue;
			}

			const bool referenced_mark_input =
			    scratch.HasOperatorBinding(HASH_JOIN_IDX) &&
			    SljitTryReferenceMarkProbeFilterInput(scratch.OperatorBinding(HASH_JOIN_IDX).hash_join_probe,
			                                          source_chunk, mark_count, join_output);
			if (referenced_mark_input && count_star_update.Ready()) {
				idx_t preaggregated_selected_count = 0;
				auto direct_preaggregate_stage_start = SljitRegionStageStart(runtime);
				if (TryPreaggregateProjectedMarkedCountStarGroups(
				        projection_op, join_output, scratch.FilterSelection(HASH_JOIN_IDX), mark_count, compact_groups,
				        preaggregated_count_deltas, preaggregated_selected_count)) {
					RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
					                              "direct_mark_preaggregate_count_star_groups",
					                              direct_preaggregate_stage_start);
					RecordSljitRegionRuntimePath(runtime, filter_op.kind, "direct_mark_preaggregate_selection");
					if (preaggregated_selected_count == 0) {
						continue;
					}
					if (TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
					        runtime, scratch, AGGREGATE_IDX, aggregate_op, compact_groups, preaggregated_count_deltas,
					        count_star_update, preaggregated_selected_count, true,
					        aggregate_sink.DeferredGroupedFinishPtr())) {
						aggregate_sink.Charge(preaggregated_selected_count);
						continue;
					}
				}
			}

			auto selection_stage_start = SljitRegionStageStart(runtime);
			auto &mark_selection = scratch.FilterSelection(FILTER_IDX);
			auto selected_count =
			    SljitSelectMarkProbeMatches(scratch.FilterSelection(HASH_JOIN_IDX), mark_count, mark_selection);
			RecordSljitRegionStageRuntimePath(runtime, FILTER_IDX, filter_op.kind, "direct_mark_selection",
			                                  selection_stage_start);
			if (selected_count == 0) {
				continue;
			}

			if (!referenced_mark_input) {
				materialize_mark_output_fallback(source_chunk, mark_count, join_output);
				auto sink_result = execute_native_full_pipeline_from(scratch, FILTER_IDX, join_output);
				if (aggregate_sink.StopAfterSinkResult(sink_result)) {
					return true;
				}
				continue;
			}

			const auto *execute_sel = selected_count == mark_count ? nullptr : &mark_selection;
			auto direct_preaggregate_stage_start = SljitRegionStageStart(runtime);
			if (count_star_update.Ready() &&
			    TryPreaggregateProjectedCountStarGroups(projection_op, join_output, execute_sel, selected_count,
			                                            compact_groups, preaggregated_count_deltas)) {
				RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
				                              "direct_preaggregate_count_star_groups", direct_preaggregate_stage_start);
				if (TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
				        runtime, scratch, AGGREGATE_IDX, aggregate_op, compact_groups, preaggregated_count_deltas,
				        count_star_update, selected_count, true, aggregate_sink.DeferredGroupedFinishPtr())) {
					aggregate_sink.Charge(selected_count);
					continue;
				}
			}

			projected.Reset();
			auto projection_stage_start = SljitRegionStageStart(runtime);
			SljitExecuteProjection(scratch, PROJECTION_IDX, projection_op, join_output, projected, execute_sel,
			                       selected_count);
			RecordSljitRegionStageRuntime(runtime, PROJECTION_IDX, projection_op.kind, "direct_mark_projection",
			                              projection_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "direct_mark_projection",
			                                         selected_count);
			if (try_preaggregated_count_star_batch(projected)) {
				continue;
			}
			if (aggregate_sink.AppendProjectedBatch(projected)) {
				return true;
			}
		} while (!SljitHashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
		return false;
	};

	return aggregate_sink.RunSourceLoopAfterFlushAndFinish(fetched_chunks, max_source_fetches, execute_source_chunk);
}

} // namespace duckdb
