//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projected_aggregate_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_probe_executor_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_join_projection_aggregate_primitive.hpp"
#include "sljit_post_join_projection_runtime.hpp"
#include "sljit_pre_join_projection_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projected_grouped_aggregate_sink.hpp"

namespace duckdb {

static void SljitAttachDirectJoinOutputAggregateSourceStats(SljitDirectJoinOutputAggregateStrategy &strategy,
                                                            const vector<idx_t> &source_distinct_counts,
                                                            const vector<Value> &source_min_values,
                                                            const vector<Value> &source_max_values) {
	strategy.source_distinct_counts = &source_distinct_counts;
	strategy.source_min_values = &source_min_values;
	strategy.source_max_values = &source_max_values;
}

struct SljitPostJoinProjectionAggregateRuntimeState {
	bool Prepare(ExecutionRegionRuntime &, vector<SljitExecutableRegionOp> &ops,
	             const SljitPostJoinProjectionAggregatePrimitive &primitive,
	             const vector<idx_t> &source_distinct_counts, const vector<Value> &source_min_values,
	             const vector<Value> &source_max_values) {
		if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, primitive)) {
			return false;
		}
		post_join_projection = primitive.post_join_projection.MakeStrategy();
		direct_join_output_aggregate_strategy =
		    make_uniq<SljitDirectJoinOutputAggregateStrategy>(primitive.direct_join_output_aggregate.aggregate_idx,
		                                                      primitive.direct_join_output_aggregate.update_schedule);
		SljitAttachDirectJoinOutputAggregateSourceStats(*direct_join_output_aggregate_strategy, source_distinct_counts,
		                                                source_min_values, source_max_values);
		direct_join_output_aggregate = SljitDirectJoinOutputAggregatePolicy(*direct_join_output_aggregate_strategy);
		prepared = true;
		return true;
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitPostJoinProjectionAggregatePrimitive &primitive,
	             const SljitRuntimeBatchView &input) {
		if (!input.HasHashJoinSelection() || input.hash_join_idx != primitive.post_join_projection.hash_join_idx) {
			throw InternalException("SLJIT post-join aggregate primitive received an invalid hash-join selection view");
		}
		auto &join_output = scratch.TemporaryChunk(input.hash_join_idx);
		join_output.SetChildCardinality(input.count);
		return ExecuteSelectedJoinOutput(runtime, result, ops, scratch, input.Chunk(), join_output, *input.selection,
		                                 *input.hash_join_build_selection, *input.hash_join_row_pointers,
		                                 input.source_key0_int64_to_int32_matches_are_proven,
		                                 optional_ptr<const vector<idx_t>>(input.hash_join_output_column_map),
		                                 input.hash_join_output_projection_idx);
	}

	bool ExecuteSelectedJoinOutput(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                               vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                               DataChunk &join_input, DataChunk &join_output,
	                               const SelectionVector &match_selection, const SelectionVector &build_selection,
	                               Vector &row_pointers, bool source_key0_int64_to_int32_matches_are_proven,
	                               optional_ptr<const vector<idx_t>> output_column_map = nullptr,
	                               idx_t output_projection_idx = DConstants::INVALID_INDEX) {
		if (!prepared) {
			throw InternalException("SLJIT post-join aggregate runtime state was not prepared");
		}
		if (join_output.size() == 0) {
			return false;
		}
		auto hash_join_idx = post_join_projection.hash_join_idx;
		auto &hash_join_op = ops[hash_join_idx];
		const bool source_key0_int64_to_int32_safe_for_output = source_key0_int64_to_int32_matches_are_proven;
		if (!post_join_projection.HasProjectionChain()) {
			if (SljitTryExecuteDirectJoinOutputAggregate(
			        runtime, ops, scratch, direct_join_output_aggregate, post_join_projection, join_input,
			        match_selection, build_selection, row_pointers, join_output, nullptr,
			        source_key0_int64_to_int32_safe_for_output, output_column_map, output_projection_idx)) {
				processed_output_rows += join_output.size();
				return false;
			}
			SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, scratch, hash_join_idx, hash_join_op, join_input,
			                                                 match_selection, build_selection, row_pointers,
			                                                 join_output.size(), join_output);
			auto aggregate_idx = AggregateIndex();
			auto &aggregate_op = ops[aggregate_idx];
			auto sink_result = SljitExecuteNativeAggregateUpdate(runtime, runtime.ExecutionOperators(), scratch,
			                                                     aggregate_idx, aggregate_op, join_output);
			if (SljitSinkResultStopsPipeline(sink_result)) {
				return SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
			}
			processed_output_rows += join_output.size();
			return false;
		}

		auto aggregate_sink = MakeAggregateSink(runtime, result, ops, scratch);
		auto &projected = scratch.TemporaryChunk(post_join_projection.final_projection_idx);
		bool direct_projected = false;
		if (SljitTryExecuteDirectJoinOutputAggregate(
		        runtime, ops, scratch, direct_join_output_aggregate, post_join_projection, join_input, match_selection,
		        build_selection, row_pointers, join_output, aggregate_sink.DeferredGroupedFinishPtr(),
		        source_key0_int64_to_int32_safe_for_output, output_column_map, output_projection_idx)) {
			aggregate_sink.Charge(join_output.size());
			return false;
		}
		if (output_column_map) {
			auto &failure = direct_join_output_aggregate_strategy->last_failure;
			const auto reason = failure.empty() ? string("unknown") : failure;
			throw InternalException("SLJIT mapped selected join-output aggregate descriptor failed: " + reason);
		}
		if (aggregate_sink.TryAppendDirectProjectedBatch(join_output, direct_projected, [&](DataChunk &batch) {
			    return SljitTryDirectMaterializeJoinProjectionChainToBatch(runtime, ops, scratch, post_join_projection,
			                                                               &join_input, &match_selection, &row_pointers,
			                                                               join_output, batch);
		    })) {
			return true;
		}
		if (direct_projected) {
			return false;
		}
		SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, scratch, hash_join_idx, hash_join_op, join_input,
		                                                 match_selection, build_selection, row_pointers,
		                                                 join_output.size(), join_output);
		if (aggregate_sink.TryAppendDirectProjectedBatch(join_output, direct_projected, [&](DataChunk &batch) {
			    return SljitTryDirectMaterializeJoinProjectionChainToBatch(
			        runtime, ops, scratch, post_join_projection, nullptr, nullptr, nullptr, join_output, batch);
		    })) {
			return true;
		}
		if (direct_projected) {
			return false;
		}
		projected.Reset();
		if (!SljitTryFastProjectJoinOutput(runtime, ops, post_join_projection, join_output, projected)) {
			SljitProjectPostJoinProjectionChain(runtime, scratch, ops, post_join_projection.first_projection_idx,
			                                    post_join_projection.final_projection_idx, join_output, projected);
		}
		return aggregate_sink.AppendProjectedBatch(projected);
	}

	bool Flush(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch) {
		if (!prepared) {
			return false;
		}
		if (!post_join_projection.HasProjectionChain()) {
			return false;
		}
		auto aggregate_sink = MakeAggregateSink(runtime, result, ops, scratch);
		if (aggregate_sink.FlushProjectedBatch()) {
			return true;
		}
		aggregate_sink.FinishDeferredGroupedUpdate();
		return false;
	}

	bool BudgetReached(ExecutionRegionRuntime &runtime, idx_t max_recipe_batches) const {
		(void)runtime;
		return prepared && SljitDownstreamRowBudgetReached(processed_output_rows, max_recipe_batches);
	}

private:
	idx_t AggregateIndex() const {
		return direct_join_output_aggregate_strategy->aggregate_idx;
	}

	SljitProjectedGroupedAggregateSink MakeAggregateSink(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                                     vector<SljitExecutableRegionOp> &ops,
	                                                     SljitRegionExecutionScratch &scratch) {
		auto aggregate_idx = AggregateIndex();
		auto final_projection_idx = post_join_projection.final_projection_idx;
		return SljitProjectedGroupedAggregateSink(
		    ops, runtime, runtime.ExecutionOperators(), scratch, result, final_projection_idx,
		    ops[final_projection_idx], aggregate_idx, ops[aggregate_idx], deferred_grouped_finish,
		    processed_output_rows, projected_batch, "post_join_batch_append", "copied_post_join_batch",
		    optional_ptr<SljitDirectJoinOutputAggregatePolicy>(&direct_join_output_aggregate));
	}

private:
	bool prepared = false;
	bool deferred_grouped_finish = false;
	idx_t processed_output_rows = 0;
	SljitDataChunkBatch projected_batch;
	SljitPostJoinProjectionStrategy post_join_projection;
	unique_ptr<SljitDirectJoinOutputAggregateStrategy> direct_join_output_aggregate_strategy;
	SljitDirectJoinOutputAggregatePolicy direct_join_output_aggregate;
};

} // namespace duckdb
