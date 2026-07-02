//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_projected_aggregate_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_direct_join_output_aggregate_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_projection_runtime.hpp"
#include "sljit_post_join_projection_runtime.hpp"
#include "sljit_projected_grouped_aggregate_sink.hpp"

namespace duckdb {

template <class PREPARE_JOIN_INPUT, class EXECUTE_HASH_JOIN_PROBE>
static bool SljitTryExecuteHashJoinProjectedGroupedAggregateBatchedRoute(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    idx_t aggregate_idx, PREPARE_JOIN_INPUT &&prepare_join_input, SljitPostJoinProjectionStrategy &post_join_projection,
    SljitDirectJoinOutputAggregatePolicy &direct_join_output_aggregate, bool source_key0_int64_to_int32_unchecked,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_output_rows = 0;
	bool deferred_grouped_finish = false;
	const auto hash_join_idx = post_join_projection.hash_join_idx;
	const auto final_projection_idx = post_join_projection.final_projection_idx;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	auto &hash_join_op = ops[hash_join_idx];
	auto &final_projection_op = ops[final_projection_idx];
	auto &aggregate_op = ops[aggregate_idx];
	auto &native_runtime = runtime.ExecutionOperators();
	SljitRouteChunkBatch pending_join_input(runtime, hash_join_idx,
	                                        optional_ptr<const SljitExecutableRegionOp>(&hash_join_op),
	                                        "source_input_batch_append", "source_input_batch");
	SljitProjectedGroupedAggregateSink aggregate_sink(
	    ops, runtime, native_runtime, scratch, result, final_projection_idx, final_projection_op, aggregate_idx,
	    aggregate_op, deferred_grouped_finish, processed_output_rows, SljitProjectedGroupedAggregateProgressMode::ROWS,
	    "post_join_batch_append", "copied_post_join_batch",
	    optional_ptr<SljitDirectJoinOutputAggregatePolicy>(&direct_join_output_aggregate));

	auto append_direct_projected_batch = [&](DataChunk &join_output, bool &handled) -> bool {
		return aggregate_sink.TryAppendDirectProjectedBatch(join_output, handled, [&](DataChunk &batch) {
			return SljitTryDirectMaterializeJoinProjectionChainToBatch(runtime, ops, scratch, post_join_projection,
			                                                           nullptr, nullptr, nullptr, join_output, batch);
		});
	};

	auto append_direct_projected_batch_from_probe = [&](DataChunk &join_input, const SelectionVector &match_selection,
	                                                    Vector &row_pointers, DataChunk &join_output,
	                                                    bool &handled) -> bool {
		handled = false;
		if (join_output.size() == 0) {
			return false;
		}
		if (SljitTryExecuteDirectJoinOutputAggregate(runtime, ops, scratch, direct_join_output_aggregate,
		                                             post_join_projection, join_input, match_selection, row_pointers,
		                                             join_output, aggregate_sink.DeferredGroupedFinishPtr())) {
			aggregate_sink.Charge(join_output.size());
			handled = true;
			return false;
		}
		return aggregate_sink.TryAppendDirectProjectedBatch(join_output, handled, [&](DataChunk &batch) {
			return SljitTryDirectMaterializeJoinProjectionChainToBatch(runtime, ops, scratch, post_join_projection,
			                                                           &join_input, &match_selection, &row_pointers,
			                                                           join_output, batch);
		});
	};

	auto execute_join_input_batch = [&](DataChunk &join_input) -> bool {
		if (join_input.size() == 0) {
			return false;
		}
		SljitHashJoinProbeDrainState state;
		auto &join_output = scratch.TemporaryChunk(hash_join_idx);
		auto &projected = scratch.TemporaryChunk(final_projection_idx);
		do {
			join_output.Reset();
			string deferred_reason;
			auto bind_result =
			    execute_hash_join_probe(scratch, hash_join_idx, hash_join_op, join_input, join_output, state,
			                            deferred_reason, source_key0_int64_to_int32_unchecked, true);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				auto flush_direct_aggregate = [&]() {
					return aggregate_sink.FlushDirectAggregate();
				};
				return aggregate_sink.DeferAfterFlushAndFinish(deferred_reason, flush_direct_aggregate);
			}
			if (join_output.size() == 0) {
				continue;
			}
			bool direct_projected = false;
			if (append_direct_projected_batch_from_probe(join_input, scratch.FilterSelection(hash_join_idx),
			                                             scratch.HashJoinRowPointers(hash_join_idx), join_output,
			                                             direct_projected)) {
				return true;
			}
			if (direct_projected) {
				continue;
			}
			SljitMaterializeSelectionOnlyHashJoinProbeOutput(
			    runtime, scratch, hash_join_idx, hash_join_op, join_input, scratch.FilterSelection(hash_join_idx),
			    scratch.HashJoinBuildSelection(hash_join_idx), scratch.HashJoinRowPointers(hash_join_idx),
			    join_output.size(), join_output);
			if (append_direct_projected_batch(join_output, direct_projected)) {
				return true;
			}
			if (direct_projected) {
				continue;
			}
			if (!SljitTryFastProjectJoinOutput(runtime, ops, post_join_projection, join_output, projected)) {
				SljitProjectPostJoinProjectionChain(runtime, scratch, ops, post_join_projection.first_projection_idx,
				                                    final_projection_idx, join_output, projected);
			}
			if (aggregate_sink.AppendProjectedBatch(projected)) {
				return true;
			}
		} while (!SljitHashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
		return false;
	};

	auto flush_join_input_batch = [&]() -> bool {
		return pending_join_input.Flush(execute_join_input_batch);
	};

	auto flush_pending_batches = [&]() -> bool {
		return flush_join_input_batch() || aggregate_sink.FlushProjectedBatch();
	};

	auto append_join_input_batch = [&](DataChunk &join_input) -> bool {
		return pending_join_input.Append(join_input, join_input.GetTypes(), execute_join_input_batch);
	};

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		DataChunk *join_input = nullptr;
		auto source_result = have_more_output ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
		if (prepare_join_input(scratch, source_chunk, source_result, join_input)) {
			if (flush_pending_batches()) {
				return true;
			}
			return true;
		}
		if (!join_input) {
			return false;
		}
		return append_join_input_batch(*join_input);
	};

	return aggregate_sink.RunSourceLoopAfterFlushAndFinish(fetched_chunks, max_source_fetches, execute_source_chunk,
	                                                       flush_pending_batches);
}

} // namespace duckdb
