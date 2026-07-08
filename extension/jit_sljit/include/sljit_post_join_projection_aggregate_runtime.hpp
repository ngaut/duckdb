//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_post_join_projection_aggregate_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_join_output_aggregate_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_post_join_projection_runtime.hpp"
#include "sljit_post_join_projection_aggregate_primitive.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projected_aggregate_sink.hpp"

namespace duckdb {

struct SljitPostJoinProjectionAggregateRuntimeState {
	bool Prepare(ExecutionRegionRuntime &, vector<SljitExecutableRegionOp> &ops,
	             const SljitPostJoinProjectionAggregatePrimitive &primitive,
	             const vector<idx_t> &source_distinct_counts, const vector<Value> &source_min_values,
	             const vector<Value> &source_max_values) {
		if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, primitive)) {
			return false;
		}
		post_join_projection = primitive.post_join_projection.MakeStrategy();
		direct_join_output_aggregate_strategy = make_uniq<SljitDirectJoinOutputAggregateStrategy>(
		    primitive.aggregate_idx, source_distinct_counts, source_min_values, source_max_values);
		prepared = true;
		return true;
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitPostJoinProjectionAggregatePrimitive &primitive,
	             const SljitRuntimeBatchView &input) {
		auto selected = input.BindHashJoinSelection("SLJIT post-join aggregate primitive");
		if (selected.hash_join_idx != primitive.post_join_projection.hash_join_idx) {
			throw InternalException("SLJIT post-join aggregate primitive received an invalid hash-join selection view");
		}
		auto &join_output = scratch.TemporaryChunk(selected.hash_join_idx);
		join_output.SetChildCardinality(selected.count);
		return ExecuteSelectedJoinOutput(runtime, result, ops, scratch, selected.Input(), join_output,
		                                 selected.MatchSelection(), selected.BuildSelection(), selected.RowPointers(),
		                                 selected.source_key0_int64_to_int32_matches_are_proven,
		                                 selected.OutputColumnMap(), selected.output_projection_idx);
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
		auto direct_strategy = DirectAggregateStrategyPtr();
		if (!post_join_projection.HasProjectionChain()) {
			if (SljitTryExecuteDirectJoinOutputAggregate(
			        runtime, ops, scratch, direct_strategy, post_join_projection, join_input, match_selection,
			        build_selection, row_pointers, join_output, nullptr, source_key0_int64_to_int32_safe_for_output,
			        output_column_map, output_projection_idx)) {
				processed_output_rows += join_output.size();
				return false;
			}
			if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, scratch, hash_join_idx, hash_join_op,
			                                                      join_input, match_selection, build_selection,
			                                                      row_pointers, join_output.size(), join_output)) {
				return false;
			}
			return ExecuteMaterializedAggregateBatch(runtime, result, ops, scratch, join_output);
		}

		auto aggregate_sink = MakeAggregateSink(runtime, result, ops, scratch);
		auto &projected = scratch.TemporaryChunk(post_join_projection.final_projection_idx);
		bool direct_projected = false;
		if (SljitTryExecuteDirectJoinOutputAggregate(
		        runtime, ops, scratch, direct_strategy, post_join_projection, join_input, match_selection,
		        build_selection, row_pointers, join_output, aggregate_sink.DeferredGroupedFinishPtr(),
		        source_key0_int64_to_int32_safe_for_output, output_column_map, output_projection_idx)) {
			aggregate_sink.Charge(join_output.size());
			return false;
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
		if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, scratch, hash_join_idx, hash_join_op, join_input,
		                                                      match_selection, build_selection, row_pointers,
		                                                      join_output.size(), join_output)) {
			return false;
		}
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

	optional_ptr<SljitDirectJoinOutputAggregateStrategy> DirectAggregateStrategyPtr() {
		return optional_ptr<SljitDirectJoinOutputAggregateStrategy>(direct_join_output_aggregate_strategy.get());
	}

	optional_ptr<SljitBoundGroupedPrimitiveAggregateUpdate>
	BoundGroupedUpdatePtr(SljitExecutableRegionOp &aggregate_op) {
		if (SljitGroupedPrimitiveAggregateSinkKind(aggregate_op.aggregate_update.plan.sink_info.kind) &&
		    aggregate_op.aggregate_update.plan.use_primitive_payloads) {
			return &bound_grouped_update;
		}
		return nullptr;
	}

	bool ExecuteMaterializedAggregateBatch(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                       vector<SljitExecutableRegionOp> &ops,
	                                       SljitRegionExecutionScratch &scratch, DataChunk &input) {
		auto aggregate_idx = AggregateIndex();
		auto &aggregate_op = ops[aggregate_idx];
		if (SljitExecuteProjectedAggregateBatch(runtime, runtime.ExecutionOperators(), scratch, aggregate_idx,
		                                        aggregate_op, input, deferred_grouped_finish, result,
		                                        BoundGroupedUpdatePtr(aggregate_op))) {
			return true;
		}
		processed_output_rows += input.size();
		return false;
	}

	SljitProjectedAggregateSink MakeAggregateSink(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                              vector<SljitExecutableRegionOp> &ops,
	                                              SljitRegionExecutionScratch &scratch) {
		auto aggregate_idx = AggregateIndex();
		auto final_projection_idx = post_join_projection.final_projection_idx;
		auto &aggregate_op = ops[aggregate_idx];
		return SljitProjectedAggregateSink(ops, runtime, runtime.ExecutionOperators(), scratch, result,
		                                   final_projection_idx, ops[final_projection_idx], aggregate_idx,
		                                   aggregate_op, deferred_grouped_finish, processed_output_rows,
		                                   projected_batch, "post_join_projection_buffer_append", DirectAggregateStrategyPtr(),
		                                   BoundGroupedUpdatePtr(aggregate_op));
	}

private:
	bool prepared = false;
	bool deferred_grouped_finish = false;
	idx_t processed_output_rows = 0;
	SljitDataChunkBatch projected_batch;
	SljitPostJoinProjectionStrategy post_join_projection;
	unique_ptr<SljitDirectJoinOutputAggregateStrategy> direct_join_output_aggregate_strategy;
	SljitBoundGroupedPrimitiveAggregateUpdate bound_grouped_update;
};

} // namespace duckdb
