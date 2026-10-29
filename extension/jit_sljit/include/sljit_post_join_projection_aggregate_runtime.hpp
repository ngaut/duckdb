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
#include "sljit_hash_join_aggregate_consumer_api.hpp"
#include "sljit_hash_join_probe_input_filter_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_post_join_projection_aggregate_primitive.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_projection_reference_runtime.hpp"
#include "sljit_projected_aggregate_sink.hpp"

namespace duckdb {

static void SljitProjectProjectionStep(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                       vector<SljitExecutableRegionOp> &ops, idx_t projection_idx, DataChunk &input,
                                       DataChunk &output) {
	auto &projection_op = ops[projection_idx];
	output.Reset();
	auto projection_stage_start = SljitRegionStageStart(runtime);
	if (SljitTryReferenceProjection(output, input, projection_op)) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.Kind(), "post_join_reference_projection",
		                              projection_stage_start);
		return;
	}
	SljitExecuteProjection(runtime, scratch, projection_idx, projection_op, input, output);
	RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.Kind(), "post_join_batch_projection",
	                              projection_stage_start);
}

//! Generic post-join projection fallback for batches the direct join-output
//! aggregate does not own: project the materialized join output step by step.
static void SljitProjectPostJoinProjectionChain(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                                vector<SljitExecutableRegionOp> &ops, idx_t first_projection_idx,
                                                idx_t final_projection_idx, DataChunk &input, DataChunk &projected) {
	DataChunk *current = &input;
	for (idx_t projection_idx = first_projection_idx; projection_idx <= final_projection_idx; projection_idx++) {
		auto &projection_output =
		    projection_idx == final_projection_idx ? projected : scratch.TemporaryChunk(projection_idx);
		SljitProjectProjectionStep(runtime, scratch, ops, projection_idx, *current, projection_output);
		current = &projection_output;
		if (current->size() == 0) {
			projected.Reset();
			return;
		}
	}
}

struct SljitPostJoinProjectionAggregateRuntimeState {
	void Prepare(vector<SljitExecutableRegionOp> &ops, const SljitPostJoinProjectionAggregatePrimitive &primitive,
	             const vector<idx_t> &source_distinct_counts, const vector<Value> &source_min_values,
	             const vector<Value> &source_max_values) {
		post_join_projection = primitive.post_join_projection.MakeStrategy();
		post_join_projection.aggregate_projection = primitive.projection.get();
		direct_join_output_aggregate_strategy = make_uniq<SljitDirectJoinOutputAggregateStrategy>(
		    primitive.aggregate_idx, source_distinct_counts, source_min_values, source_max_values);
		prepared = true;
	}

	SljitHashJoinAggregateConsumerResult
	TryExecuteHashJoinProbeConsumer(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	                                SljitRegionExecutionScratch &scratch,
	                                const SljitHashJoinDirectAggregateConsumerContract &contract,
	                                const SljitHashJoinProbeSelectionPrimitive &probe_primitive, DataChunk &join_input,
	                                SljitSharedPerfectHashPredicateClassificationCache &shared_predicate_classification,
	                                SljitNativeRegionHashJoinProbeExecutor &execute_hash_join_probe,
	                                idx_t probe_input_filter_idx = DConstants::INVALID_INDEX) {
		SljitHashJoinAggregateConsumerResult result;
		if (!prepared) {
			throw InternalException("SLJIT bound direct aggregate terminal was not prepared");
		}
		if (!contract.IsBound() || contract.hash_join_idx != probe_primitive.hash_join_idx ||
		    contract.hash_join_idx != post_join_projection.hash_join_idx) {
			throw InternalException("SLJIT bound direct aggregate terminal has inconsistent hash-join ownership");
		}
		auto strategy_ptr = DirectAggregateStrategyPtr();
		if (strategy_ptr && strategy_ptr->aggregate_idx != contract.aggregate_idx) {
			throw InternalException("SLJIT bound direct aggregate terminal has inconsistent aggregate ownership");
		}
		if (!strategy_ptr || strategy_ptr->disabled) {
			return result;
		}
		strategy_ptr->last_failure.clear();
		auto &hash_join_op = ops[probe_primitive.hash_join_idx];
		result = SljitTryExecuteHashJoinAggregateConsumer(
		    runtime, runtime.ExecutionOperators(), ops, scratch, probe_primitive, hash_join_op, *strategy_ptr,
		    join_input, post_join_projection, probe_input_filter_idx, probe_input_filter_cache,
		    shared_predicate_classification, execute_hash_join_probe);
		return result;
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
		return ExecuteSelectedJoinOutput(runtime, result, ops, scratch, selected.SourceInput(), join_output,
		                                 selected.MatchSelection(), selected.BuildSelection(), selected.RowPointers(),
		                                 selected.output_proof, selected.OutputLayout());
	}

	bool ExecuteSelectedJoinOutput(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                               vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                               DataChunk &join_input, DataChunk &join_output,
	                               const SelectionVector &match_selection, const SelectionVector &build_selection,
	                               Vector &row_pointers, ExecutionHashJoinProbeOutputProof output_proof,
	                               optional_ptr<const SljitBatchLayout> output_layout = nullptr) {
		if (!prepared) {
			throw InternalException("SLJIT post-join aggregate runtime state was not prepared");
		}
		if (join_output.size() == 0) {
			return false;
		}
		auto hash_join_idx = post_join_projection.hash_join_idx;
		auto &hash_join_op = ops[hash_join_idx];
		auto direct_strategy = DirectAggregateStrategyPtr();
		if (!post_join_projection.HasProjectionChain()) {
			if (SljitTryExecuteDirectJoinOutputAggregate(runtime, ops, scratch, direct_strategy, post_join_projection,
			                                             join_input, match_selection, build_selection, row_pointers,
			                                             join_output, output_proof, output_layout)) {
				return false;
			}
			auto source = SljitBindHashJoinOutputSource(runtime, ops, scratch, hash_join_idx, join_input, output_proof,
			                                            output_layout);
			if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, hash_join_idx, hash_join_op, source,
			                                                      match_selection, build_selection, row_pointers,
			                                                      join_output.size(), join_output)) {
				return false;
			}
			return ExecuteMaterializedAggregateBatch(runtime, result, ops, scratch, join_output);
		}

		auto aggregate_sink = MakeAggregateSink(runtime, result, ops, scratch);
		auto &projected = scratch.TemporaryChunk(post_join_projection.final_projection_idx);
		if (SljitTryExecuteDirectJoinOutputAggregate(runtime, ops, scratch, direct_strategy, post_join_projection,
		                                             join_input, match_selection, build_selection, row_pointers,
		                                             join_output, output_proof, output_layout)) {
			return false;
		}
		auto source = SljitBindHashJoinOutputSource(runtime, ops, scratch, hash_join_idx, join_input, output_proof,
		                                            output_layout);
		if (!SljitMaterializeSelectionOnlyHashJoinProbeOutput(runtime, hash_join_idx, hash_join_op, source,
		                                                      match_selection, build_selection, row_pointers,
		                                                      join_output.size(), join_output)) {
			return false;
		}
		projected.Reset();
		SljitProjectPostJoinProjectionChain(runtime, scratch, ops, post_join_projection.first_projection_idx,
		                                    post_join_projection.final_projection_idx, join_output, projected);
		return aggregate_sink.AppendProjectedBatch(projected);
	}

	bool Flush(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch) {
		if (!prepared) {
			return false;
		}
		// The direct join-output aggregate batches rows whether or not a projection chain follows the join
		SljitFlushDirectJoinOutputAggregate(runtime, ops, DirectAggregateStrategyPtr());
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

private:
	idx_t AggregateIndex() const {
		return direct_join_output_aggregate_strategy->aggregate_idx;
	}

	optional_ptr<SljitDirectJoinOutputAggregateStrategy> DirectAggregateStrategyPtr() {
		return optional_ptr<SljitDirectJoinOutputAggregateStrategy>(direct_join_output_aggregate_strategy.get());
	}

	bool ExecuteMaterializedAggregateBatch(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                       vector<SljitExecutableRegionOp> &ops, SljitRegionExecutionScratch &scratch,
	                                       DataChunk &input) {
		auto aggregate_idx = AggregateIndex();
		auto &aggregate_op = ops[aggregate_idx];
		if (SljitExecuteProjectedAggregateBatch(runtime, runtime.ExecutionOperators(), scratch, aggregate_idx,
		                                        aggregate_op, input, result)) {
			return true;
		}
		return false;
	}

	SljitProjectedAggregateSink MakeAggregateSink(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                              vector<SljitExecutableRegionOp> &ops,
	                                              SljitRegionExecutionScratch &scratch) {
		auto aggregate_idx = AggregateIndex();
		auto final_projection_idx = post_join_projection.final_projection_idx;
		auto &aggregate_op = ops[aggregate_idx];
		return SljitProjectedAggregateSink(ops, runtime, runtime.ExecutionOperators(), scratch, result,
		                                   final_projection_idx, ops[final_projection_idx], aggregate_idx, aggregate_op,
		                                   projected_batch, "post_join_projection_buffer_append",
		                                   DirectAggregateStrategyPtr());
	}

private:
	bool prepared = false;
	SljitDataChunkBatch projected_batch;
	SljitPostJoinProjectionStrategy post_join_projection;
	unique_ptr<SljitDirectJoinOutputAggregateStrategy> direct_join_output_aggregate_strategy;
	SljitHashJoinProbeInputFilterCache probe_input_filter_cache;
};

} // namespace duckdb
