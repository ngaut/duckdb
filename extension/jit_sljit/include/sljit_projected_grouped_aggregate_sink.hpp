//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projected_grouped_aggregate_sink.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_direct_join_output_aggregate_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

#include <utility>

namespace duckdb {

enum class SljitProjectedGroupedAggregateProgressMode : uint8_t { ROWS, BATCHES };

static bool SljitExecuteDeferredGroupedAggregateBatch(ExecutionRegionRuntime &runtime,
                                                      ExecutionOperatorRuntime &native_runtime,
                                                      SljitRegionExecutionScratch &scratch, idx_t aggregate_idx,
                                                      SljitExecutableRegionOp &aggregate_op, DataChunk &input,
                                                      bool &deferred_grouped_finish, ExecutionRegionResult &result,
                                                      optional_ptr<SinkResultType> sink_result_out = nullptr) {
	if (input.size() == 0) {
		return false;
	}
	auto sink_result =
	    SljitExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, aggregate_idx, aggregate_op, input, nullptr,
	                                      DConstants::INVALID_INDEX, true, &deferred_grouped_finish);
	if (sink_result_out) {
		*sink_result_out = sink_result;
	}
	if (SljitSinkResultStopsPipeline(sink_result)) {
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, aggregate_idx, deferred_grouped_finish);
		return SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
	}
	return false;
}

struct SljitProjectedGroupedAggregateSink {
	SljitProjectedGroupedAggregateSink(vector<SljitExecutableRegionOp> &ops_p, ExecutionRegionRuntime &runtime_p,
	                                   ExecutionOperatorRuntime &native_runtime_p,
	                                   SljitRegionExecutionScratch &scratch_p, ExecutionRegionResult &result_p,
	                                   idx_t projection_idx_p, SljitExecutableRegionOp &projection_op_p,
	                                   idx_t aggregate_idx_p, SljitExecutableRegionOp &aggregate_op_p,
	                                   bool &deferred_grouped_finish_p, idx_t &processed_p,
	                                   SljitProjectedGroupedAggregateProgressMode progress_mode_p,
	                                   const char *append_phase_p, const char *boundary_phase_p,
	                                   optional_ptr<SljitDirectJoinOutputAggregatePolicy> direct_aggregate_p = nullptr,
	                                   bool record_sink_result_p = false)
	    : ops(ops_p), runtime(runtime_p), native_runtime(native_runtime_p), scratch(scratch_p), result(result_p),
	      projection_idx(projection_idx_p), projection_op(projection_op_p), aggregate_idx(aggregate_idx_p),
	      aggregate_op(aggregate_op_p), deferred_grouped_finish(deferred_grouped_finish_p), processed(processed_p),
	      progress_mode(progress_mode_p), append_phase(append_phase_p), boundary_phase(boundary_phase_p),
	      direct_aggregate(direct_aggregate_p), record_sink_result(record_sink_result_p) {
	}

	bool BudgetReached() const {
		if (progress_mode == SljitProjectedGroupedAggregateProgressMode::ROWS) {
			return SljitDownstreamRowBudgetReached(processed, runtime.MaxChunks());
		}
		return processed >= runtime.MaxChunks();
	}

	bool FlushDirectAggregate() {
		if (!direct_aggregate) {
			return false;
		}
		return SljitFlushDirectJoinOutputAggregate(runtime, ops, *direct_aggregate);
	}

	void Charge(idx_t rows) {
		if (progress_mode == SljitProjectedGroupedAggregateProgressMode::ROWS) {
			SljitChargeDownstreamRows(processed, rows);
			return;
		}
		processed++;
	}

	bool ExecuteProjectedBatch(DataChunk &projected) {
		if (FlushDirectAggregate()) {
			return true;
		}
		if (projected.size() == 0) {
			return false;
		}
		SinkResultType sink_result = SinkResultType::NEED_MORE_INPUT;
		optional_ptr<SinkResultType> sink_result_out;
		if (record_sink_result) {
			sink_result_out = &sink_result;
		}
		if (SljitExecuteDeferredGroupedAggregateBatch(runtime, native_runtime, scratch, aggregate_idx, aggregate_op,
		                                              projected, deferred_grouped_finish, result, sink_result_out)) {
			return true;
		}
		if (record_sink_result) {
			native_runtime.RecordSinkResult(projected.size(), sink_result);
		}
		Charge(projected.size());
		return false;
	}

	bool FlushProjectedBatch() {
		if (FlushDirectAggregate()) {
			return true;
		}
		return SljitFlushRuntimePendingBatch(runtime,
		                                     [&](DataChunk &projected) { return ExecuteProjectedBatch(projected); });
	}

	bool AppendProjectedBatch(DataChunk &projected) {
		return SljitAppendChunkToRuntimeBatch(
		    runtime, projected, projection_op.output_types, projection_idx, projection_op, append_phase, boundary_phase,
		    [&]() { return FlushProjectedBatch(); }, [&](DataChunk &batch) { return ExecuteProjectedBatch(batch); });
	}

	template <class MATERIALIZE_BATCH>
	bool TryAppendDirectProjectedBatch(DataChunk &projected, bool &handled, MATERIALIZE_BATCH &&materialize_batch) {
		handled = false;
		if (projected.size() == 0) {
			return false;
		}
		if (FlushDirectAggregate()) {
			return true;
		}
		return SljitTryAppendDirectChunkToRuntimeBatch(
		    runtime, projected, projection_op.output_types, handled, [&]() { return FlushProjectedBatch(); },
		    std::forward<MATERIALIZE_BATCH>(materialize_batch));
	}

	void FinishDeferredGroupedUpdate() {
		SljitFinishDeferredGroupedAggregateUpdate(runtime, scratch, aggregate_idx, deferred_grouped_finish);
	}

	bool StopAfterFinish(ExecutionRegionResult stop_result) {
		FinishDeferredGroupedUpdate();
		return SljitStopFullPipeline(result, stop_result);
	}

	template <class FLUSH_BATCH>
	bool StopAfterFlushAndFinish(ExecutionRegionResult stop_result, FLUSH_BATCH &&flush_batch) {
		if (flush_batch()) {
			return true;
		}
		return StopAfterFinish(stop_result);
	}

	bool StopAfterFlushAndFinish(ExecutionRegionResult stop_result) {
		return StopAfterFlushAndFinish(stop_result, [&]() { return FlushProjectedBatch(); });
	}

	bool DeferAfterFinish(string &deferred_reason) {
		FinishDeferredGroupedUpdate();
		return SljitDeferFullPipelineResult(runtime, deferred_reason, result);
	}

	template <class FLUSH_BATCH>
	bool DeferAfterFlushAndFinish(string &deferred_reason, FLUSH_BATCH &&flush_batch) {
		if (flush_batch()) {
			return true;
		}
		return DeferAfterFinish(deferred_reason);
	}

	bool DeferAfterFlushAndFinish(string &deferred_reason) {
		return DeferAfterFlushAndFinish(deferred_reason, [&]() { return FlushProjectedBatch(); });
	}

	bool StopAfterSinkResult(SinkResultType sink_result) {
		if (!SljitSinkResultStopsPipeline(sink_result)) {
			return false;
		}
		FinishDeferredGroupedUpdate();
		return SljitNativeSinkResultStopsExecution(runtime, sink_result, result);
	}

	template <class EXECUTE_SOURCE_CHUNK, class STOP_NOT_FINISHED, class STOP_BLOCKED, class STOP_FINISHED>
	bool RunSourceLoop(idx_t &fetched_chunks, idx_t max_source_fetches, EXECUTE_SOURCE_CHUNK &&execute_source_chunk,
	                   STOP_NOT_FINISHED &&stop_not_finished, STOP_BLOCKED &&stop_blocked,
	                   STOP_FINISHED &&stop_finished) {
		return SljitRunFullPipelineSourceContractLoop(
		    runtime, fetched_chunks, [&]() { return BudgetReached() || fetched_chunks >= max_source_fetches; },
		    [&](DataChunk &source_chunk, bool have_more_output) {
			    return source_chunk.size() > 0 && execute_source_chunk(source_chunk, have_more_output);
		    },
		    std::forward<STOP_NOT_FINISHED>(stop_not_finished), std::forward<STOP_BLOCKED>(stop_blocked),
		    std::forward<STOP_FINISHED>(stop_finished));
	}

	template <class EXECUTE_SOURCE_CHUNK>
	bool RunSourceLoopAfterFlushAndFinish(idx_t &fetched_chunks, idx_t max_source_fetches,
	                                      EXECUTE_SOURCE_CHUNK &&execute_source_chunk) {
		return RunSourceLoop(
		    fetched_chunks, max_source_fetches, std::forward<EXECUTE_SOURCE_CHUNK>(execute_source_chunk),
		    [&]() { return StopAfterFlushAndFinish(ExecutionRegionResult::NOT_FINISHED); },
		    [&]() { return StopAfterFlushAndFinish(ExecutionRegionResult::INTERRUPTED); },
		    [&]() { return StopAfterFlushAndFinish(ExecutionRegionResult::FINISHED); });
	}

	template <class EXECUTE_SOURCE_CHUNK, class FLUSH_BATCH>
	bool RunSourceLoopAfterFlushAndFinish(idx_t &fetched_chunks, idx_t max_source_fetches,
	                                      EXECUTE_SOURCE_CHUNK &&execute_source_chunk, FLUSH_BATCH &&flush_batch) {
		return RunSourceLoop(
		    fetched_chunks, max_source_fetches, std::forward<EXECUTE_SOURCE_CHUNK>(execute_source_chunk),
		    [&]() { return StopAfterFlushAndFinish(ExecutionRegionResult::NOT_FINISHED, flush_batch); },
		    [&]() { return StopAfterFlushAndFinish(ExecutionRegionResult::INTERRUPTED, flush_batch); },
		    [&]() { return StopAfterFlushAndFinish(ExecutionRegionResult::FINISHED, flush_batch); });
	}

	template <class EXECUTE_SOURCE_CHUNK, class FLUSH_BATCH>
	bool RunSourceLoopAfterBudgetOrFinishedFlushAndFinish(idx_t &fetched_chunks, idx_t max_source_fetches,
	                                                      EXECUTE_SOURCE_CHUNK &&execute_source_chunk,
	                                                      FLUSH_BATCH &&flush_batch) {
		return RunSourceLoop(
		    fetched_chunks, max_source_fetches, std::forward<EXECUTE_SOURCE_CHUNK>(execute_source_chunk),
		    [&]() { return StopAfterFlushAndFinish(ExecutionRegionResult::NOT_FINISHED, flush_batch); },
		    [&]() { return StopAfterFinish(ExecutionRegionResult::INTERRUPTED); },
		    [&]() { return StopAfterFlushAndFinish(ExecutionRegionResult::FINISHED, flush_batch); });
	}

	optional_ptr<bool> DeferredGroupedFinishPtr() {
		return optional_ptr<bool>(&deferred_grouped_finish);
	}

	vector<SljitExecutableRegionOp> &ops;
	ExecutionRegionRuntime &runtime;
	ExecutionOperatorRuntime &native_runtime;
	SljitRegionExecutionScratch &scratch;
	ExecutionRegionResult &result;
	idx_t projection_idx;
	SljitExecutableRegionOp &projection_op;
	idx_t aggregate_idx;
	SljitExecutableRegionOp &aggregate_op;
	bool &deferred_grouped_finish;
	idx_t &processed;
	SljitProjectedGroupedAggregateProgressMode progress_mode;
	const char *append_phase;
	const char *boundary_phase;
	optional_ptr<SljitDirectJoinOutputAggregatePolicy> direct_aggregate;
	bool record_sink_result;
};

} // namespace duckdb
