//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_two_join_drain_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_direct_second_join_input_runtime.hpp"
#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_hash_join_probe_executor_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_post_join_projection_runtime.hpp"
#include "sljit_projected_grouped_aggregate_sink.hpp"
#include "sljit_two_join_drain_helpers_runtime.hpp"

#include <utility>

namespace duckdb {

struct SljitProjectionTwoJoinProjectionChainRouteConfig {
	bool bypass_pre_join_projection = false;
	bool first_join_unchecked_key_cast = false;
	bool direct_first_join_to_second_join = false;
	SljitDirectSecondJoinInputProjection direct_second_join_projection;
};

template <class KERNEL>
static bool SljitTryExecuteHashJoinProjectionHashJoinProjectionsGroupedAggregateBatched(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
    vector<SljitExecutableRegionOp> &ops) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_batches = 0;
	bool deferred_grouped_finish = false;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	SljitTwoJoinGroupedAggregateRouteLayout layout(ops.size());
	layout.AssertOperatorBounds(ops.size());
	auto &final_projection_op = ops[layout.final_projection_idx];
	auto &aggregate_op = ops[layout.aggregate_idx];
	auto &native_runtime = runtime.ExecutionOperators();
	SljitTwoJoinGroupedAggregateRouteState route_state;
	route_state.Initialize(runtime.GetAllocator(), ops[layout.between_projection_idx].output_types);
	auto &second_join_batch = route_state.second_join_batch;
	SljitProjectedGroupedAggregateSink aggregate_sink(
	    ops, runtime, native_runtime, scratch, result, layout.final_projection_idx, final_projection_op,
	    layout.aggregate_idx, aggregate_op, deferred_grouped_finish, processed_batches,
	    SljitProjectedGroupedAggregateProgressMode::BATCHES, "post_join_batch_append", "copied_post_join_batch",
	    nullptr, true);

	SljitHashJoinSelectionOnlyMaterializationBoundaries second_join_materialization_boundaries;
	second_join_materialization_boundaries.regular_first = "final_output";
	second_join_materialization_boundaries.regular_second = "row_pointer_reference";
	second_join_materialization_boundaries.perfect_first = "final_output";
	second_join_materialization_boundaries.perfect_second = nullptr;

	auto execute_recorded_hash_join_probe =
	    SljitMakeFixedScratchRecordedHashJoinProbeCallback(kernel, runtime, native_runtime, scratch);

	auto flush_second_join_batch = [&]() -> bool {
		if (second_join_batch.size() == 0) {
			return false;
		}
		if (SljitDrainSecondHashJoinProjectionAggregateRoute(runtime, native_runtime, ops, scratch, layout, route_state,
		                                                     second_join_materialization_boundaries, second_join_batch,
		                                                     aggregate_sink, execute_recorded_hash_join_probe)) {
			return true;
		}
		second_join_batch.Reset();
		route_state.ResetCompressedPassthroughs();
		route_state.ResetPrecomputedPayloads();
		return false;
	};

	auto flush_second_join_and_projected_batch = [&]() -> bool {
		return flush_second_join_batch() || aggregate_sink.FlushProjectedBatch();
	};

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) {
		return SljitExecuteTwoJoinGroupedAggregateSourceChunk(
		    runtime, native_runtime, ops, scratch, layout, route_state, second_join_materialization_boundaries,
		    source_chunk, have_more_output, flush_second_join_batch, aggregate_sink, execute_recorded_hash_join_probe);
	};

	return aggregate_sink.RunSourceLoopAfterBudgetOrFinishedFlushAndFinish(
	    fetched_chunks, max_source_fetches, execute_source_chunk, flush_second_join_and_projected_batch);
}

template <class EXECUTE_HASH_JOIN_PROBE>
static bool SljitTryExecuteProjectionTwoJoinProjectionChainGroupedAggregateBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    const SljitProjectionTwoJoinProjectionChainRouteConfig &config,
    EXECUTE_HASH_JOIN_PROBE &&execute_recorded_hash_join_probe) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_batches = 0;
	bool deferred_grouped_finish = false;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	const idx_t pre_join_projection_idx = 0;
	const idx_t first_hash_join_idx = 1;
	const idx_t between_projection_idx = 2;
	const idx_t second_hash_join_idx = 3;
	const idx_t first_post_join_projection_idx = 4;
	const idx_t final_projection_idx = 5;
	const idx_t aggregate_idx = 6;
	auto &pre_join_projection_op = ops[pre_join_projection_idx];
	auto &first_hash_join_op = ops[first_hash_join_idx];
	auto &between_join_projection_op = ops[between_projection_idx];
	auto &final_projection_op = ops[final_projection_idx];
	auto &aggregate_op = ops[aggregate_idx];
	auto &native_runtime = runtime.ExecutionOperators();
	DataChunk first_join_batch;
	first_join_batch.Initialize(runtime.GetAllocator(), first_hash_join_op.output_types);
	DataChunk direct_second_join_batch;
	if (config.direct_first_join_to_second_join) {
		direct_second_join_batch.Initialize(runtime.GetAllocator(), between_join_projection_op.output_types);
	}
	SljitProjectedGroupedAggregateSink aggregate_sink(
	    ops, runtime, native_runtime, scratch, result, final_projection_idx, final_projection_op, aggregate_idx,
	    aggregate_op, deferred_grouped_finish, processed_batches, SljitProjectedGroupedAggregateProgressMode::BATCHES,
	    "post_join_batch_append", "copied_post_join_batch");

	auto execute_hash_join_probe = [&](idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
	                                   DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
	                                   bool source_key0_int64_to_int32_unchecked, bool selection_only) {
		return execute_recorded_hash_join_probe(scratch, hash_join_idx, hash_join_op, input, output, state,
		                                        deferred_reason, source_key0_int64_to_int32_unchecked, selection_only);
	};

	auto flush_direct_second_join_batch = [&]() -> bool {
		if (!config.direct_first_join_to_second_join) {
			return false;
		}
		return SljitFlushDataChunkBatch(direct_second_join_batch, [&](DataChunk &batch) {
			return SljitDrainSecondHashJoinProjectionChain(
			    runtime, ops, scratch, second_hash_join_idx, first_post_join_projection_idx, final_projection_idx,
			    batch, aggregate_sink, execute_hash_join_probe, "post_second_join_reference_projection",
			    "post_second_join_batch_projection");
		});
	};

	auto append_direct_first_join_selection = [&](DataChunk &first_join_input, idx_t selected_count) -> bool {
		if (selected_count == 0) {
			return false;
		}
		if (direct_second_join_batch.size() + selected_count > STANDARD_VECTOR_SIZE) {
			if (flush_direct_second_join_batch()) {
				return true;
			}
		}
		const auto target_offset = direct_second_join_batch.size();
		auto projection_stage_start = SljitRegionStageStart(runtime);
		if (!SljitTryDirectBuildSecondJoinInput(config.direct_second_join_projection, scratch, first_join_input,
		                                        scratch.FilterSelection(first_hash_join_idx),
		                                        scratch.HashJoinRowPointers(first_hash_join_idx), selected_count,
		                                        direct_second_join_batch, target_offset)) {
			throw InternalException("SLJIT direct first-join selection projection failed");
		}
		RecordSljitRegionStageRuntime(runtime, between_projection_idx, between_join_projection_op.kind,
		                              "between_join_direct_selection_projection", projection_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
		                                         "direct_row_pointer_projection", selected_count);
		if (direct_second_join_batch.size() == STANDARD_VECTOR_SIZE && flush_direct_second_join_batch()) {
			return true;
		}
		return false;
	};

	auto process_first_join_batch = [&](DataChunk &batch) -> bool {
		if (batch.size() == 0) {
			return false;
		}
		auto &second_join_input = scratch.TemporaryChunk(between_projection_idx);
		second_join_input.Reset();
		auto projection_stage_start = SljitRegionStageStart(runtime);
		if (SljitTryReferenceProjection(second_join_input, batch, between_join_projection_op)) {
			RecordSljitRegionStageRuntime(runtime, between_projection_idx, between_join_projection_op.kind,
			                              "between_join_reference_projection", projection_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
			                                         "reference_post_join_projection", second_join_input.size());
		} else {
			SljitExecuteProjection(scratch, between_projection_idx, between_join_projection_op, batch,
			                       second_join_input);
			RecordSljitRegionStageRuntime(runtime, between_projection_idx, between_join_projection_op.kind,
			                              "between_join_batch_projection", projection_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
			                                         "copied_post_join_projection", second_join_input.size());
		}
		return SljitDrainSecondHashJoinProjectionChain(
		    runtime, ops, scratch, second_hash_join_idx, first_post_join_projection_idx, final_projection_idx,
		    second_join_input, aggregate_sink, execute_hash_join_probe, "post_second_join_reference_projection",
		    "post_second_join_batch_projection");
	};

	auto flush_first_join_batch = [&]() -> bool {
		return SljitFlushDataChunkBatch(first_join_batch, process_first_join_batch);
	};

	auto flush_join_and_projected_batches = [&]() -> bool {
		return flush_first_join_batch() || flush_direct_second_join_batch() || aggregate_sink.FlushProjectedBatch();
	};

	auto append_first_join_batch = [&](DataChunk &first_join_output) -> bool {
		return SljitAppendChunkToInitializedBatch(runtime, first_join_batch, first_join_output, first_hash_join_idx,
		                                          optional_ptr<const SljitExecutableRegionOp>(&first_hash_join_op),
		                                          "first_join_batch_append", "copied_post_join_batch",
		                                          flush_first_join_batch, process_first_join_batch);
	};

	auto prepare_first_join_input = [&](DataChunk &source_chunk, DataChunk *&first_join_input) {
		SljitPrepareOptionalPreJoinProjectionInput(runtime, scratch, pre_join_projection_idx, pre_join_projection_op,
		                                           source_chunk, config.bypass_pre_join_projection, first_join_input);
	};

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return aggregate_sink.StopAfterFinish(ExecutionRegionResult::INTERRUPTED);
		}

		DataChunk *first_join_input = nullptr;
		prepare_first_join_input(source_chunk, first_join_input);
		if (!first_join_input || first_join_input->size() == 0) {
			return false;
		}

		SljitHashJoinProbeDrainState first_state;
		auto &first_join_output = scratch.TemporaryChunk(first_hash_join_idx);
		do {
			first_join_output.Reset();
			string deferred_reason;
			auto bind_result = execute_hash_join_probe(
			    first_hash_join_idx, first_hash_join_op, *first_join_input, first_join_output, first_state,
			    deferred_reason, config.first_join_unchecked_key_cast, config.direct_first_join_to_second_join);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				return aggregate_sink.DeferAfterFinish(deferred_reason);
			}
			if (config.direct_first_join_to_second_join) {
				if (first_join_output.size() != 0 &&
				    append_direct_first_join_selection(*first_join_input, first_join_output.size())) {
					return true;
				}
			} else if (append_first_join_batch(first_join_output)) {
				return true;
			}
		} while (!SljitHashJoinProbeDrainFinished(first_hash_join_op.hash_join_probe.plan.output_mode, first_state));
		return false;
	};

	return aggregate_sink.RunSourceLoopAfterBudgetOrFinishedFlushAndFinish(
	    fetched_chunks, max_source_fetches, execute_source_chunk, flush_join_and_projected_batches);
}

template <class EXECUTE_HASH_JOIN_PROBE>
static bool SljitTryExecuteHashJoinHashJoinProjectionGroupedAggregateBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    EXECUTE_HASH_JOIN_PROBE &&execute_recorded_hash_join_probe) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_output_rows = 0;
	bool deferred_grouped_finish = false;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	const idx_t first_hash_join_idx = 0;
	const idx_t second_hash_join_idx = 1;
	const idx_t final_projection_idx = 2;
	const idx_t aggregate_idx = 3;
	auto &first_hash_join_op = ops[first_hash_join_idx];
	auto &final_projection_op = ops[final_projection_idx];
	auto &aggregate_op = ops[aggregate_idx];
	auto &native_runtime = runtime.ExecutionOperators();
	SljitProjectedGroupedAggregateSink aggregate_sink(
	    ops, runtime, native_runtime, scratch, result, final_projection_idx, final_projection_op, aggregate_idx,
	    aggregate_op, deferred_grouped_finish, processed_output_rows, SljitProjectedGroupedAggregateProgressMode::ROWS,
	    "post_join_batch_append", "copied_post_join_batch");

	SljitHashJoinSelectionOnlyMaterializationBoundaries second_join_materialization_boundaries;
	second_join_materialization_boundaries.perfect_first = "final_output";
	second_join_materialization_boundaries.perfect_second = nullptr;

	auto execute_hash_join_probe = [&](idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
	                                   DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
	                                   bool source_key0_int64_to_int32_unchecked, bool selection_only) {
		return execute_recorded_hash_join_probe(scratch, hash_join_idx, hash_join_op, input, output, state,
		                                        deferred_reason, source_key0_int64_to_int32_unchecked, selection_only);
	};

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return aggregate_sink.StopAfterFlushAndFinish(ExecutionRegionResult::INTERRUPTED);
		}

		SljitHashJoinProbeDrainState first_state;
		auto &first_join_output = scratch.TemporaryChunk(first_hash_join_idx);
		do {
			first_join_output.Reset();
			string deferred_reason;
			auto bind_result = execute_hash_join_probe(first_hash_join_idx, first_hash_join_op, source_chunk,
			                                           first_join_output, first_state, deferred_reason, false, false);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				return aggregate_sink.DeferAfterFlushAndFinish(deferred_reason);
			}
			if (first_join_output.size() == 0) {
				continue;
			}
			if (SljitDrainSelectionOnlySecondHashJoinProjection(
			        runtime, ops, scratch, second_hash_join_idx, final_projection_idx, first_join_output,
			        second_join_materialization_boundaries, aggregate_sink, execute_hash_join_probe)) {
				return true;
			}
		} while (!SljitHashJoinProbeDrainFinished(first_hash_join_op.hash_join_probe.plan.output_mode, first_state));
		return false;
	};

	return aggregate_sink.RunSourceLoopAfterFlushAndFinish(fetched_chunks, max_source_fetches, execute_source_chunk);
}

template <class EXECUTE_HASH_JOIN_PROBE>
static bool SljitTryExecuteHashJoinHashJoinProjectionProjectionGroupedAggregateBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    EXECUTE_HASH_JOIN_PROBE &&execute_recorded_hash_join_probe) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_batches = 0;
	bool deferred_grouped_finish = false;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	const idx_t first_hash_join_idx = 0;
	const idx_t second_hash_join_idx = 1;
	const idx_t first_projection_idx = 2;
	const idx_t final_projection_idx = 3;
	const idx_t aggregate_idx = 4;
	auto &first_hash_join_op = ops[first_hash_join_idx];
	auto &final_projection_op = ops[final_projection_idx];
	auto &aggregate_op = ops[aggregate_idx];
	auto &native_runtime = runtime.ExecutionOperators();
	SljitRouteChunkBatch source_batch(runtime);
	SljitProjectedGroupedAggregateSink aggregate_sink(
	    ops, runtime, native_runtime, scratch, result, final_projection_idx, final_projection_op, aggregate_idx,
	    aggregate_op, deferred_grouped_finish, processed_batches, SljitProjectedGroupedAggregateProgressMode::BATCHES,
	    "post_join_batch_append", "copied_post_join_batch");

	auto execute_hash_join_probe = [&](idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
	                                   DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
	                                   bool source_key0_int64_to_int32_unchecked, bool selection_only) {
		return execute_recorded_hash_join_probe(scratch, hash_join_idx, hash_join_op, input, output, state,
		                                        deferred_reason, source_key0_int64_to_int32_unchecked, selection_only);
	};

	auto execute_source_batch = [&](DataChunk &source_chunk) -> bool {
		SljitHashJoinProbeDrainState first_state;
		auto &first_join_output = scratch.TemporaryChunk(first_hash_join_idx);
		do {
			first_join_output.Reset();
			string deferred_reason;
			auto bind_result = execute_hash_join_probe(first_hash_join_idx, first_hash_join_op, source_chunk,
			                                           first_join_output, first_state, deferred_reason, false, false);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				return aggregate_sink.DeferAfterFinish(deferred_reason);
			}
			if (first_join_output.size() == 0) {
				continue;
			}
			if (SljitDrainSecondHashJoinProjectionChain(runtime, ops, scratch, second_hash_join_idx,
			                                            first_projection_idx, final_projection_idx, first_join_output,
			                                            aggregate_sink, execute_hash_join_probe)) {
				return true;
			}
		} while (!SljitHashJoinProbeDrainFinished(first_hash_join_op.hash_join_probe.plan.output_mode, first_state));
		return false;
	};

	auto flush_source_batch = [&]() -> bool {
		return source_batch.Flush(execute_source_batch);
	};

	auto flush_source_and_projected_batches = [&]() -> bool {
		return flush_source_batch() || aggregate_sink.FlushProjectedBatch();
	};

	auto append_source_batch = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return aggregate_sink.StopAfterFlushAndFinish(ExecutionRegionResult::INTERRUPTED,
			                                              flush_source_and_projected_batches);
		}
		return source_batch.Append(source_chunk, source_chunk.GetTypes(), execute_source_batch);
	};

	return aggregate_sink.RunSourceLoopAfterBudgetOrFinishedFlushAndFinish(
	    fetched_chunks, max_source_fetches, append_source_batch, flush_source_and_projected_batches);
}

} // namespace duckdb
