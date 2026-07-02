//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_aggregate_route_state.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <chrono>
#include <cstring>
#include <limits>
#include <utility>

namespace duckdb {

static bool SljitTryFastAppendFixedFlatAllValid(DataChunk &target, DataChunk &source) {
	const auto append_count = source.size();
	if (append_count == 0) {
		return true;
	}
	if (target.ColumnCount() != source.ColumnCount()) {
		return false;
	}
	const auto target_count = target.size();
	const auto new_count = target_count + append_count;
	if (new_count > STANDARD_VECTOR_SIZE) {
		return false;
	}
	for (idx_t col_idx = 0; col_idx < target.ColumnCount(); col_idx++) {
		auto &target_vector = target.data[col_idx];
		auto &source_vector = source.data[col_idx];
		if (target_vector.GetType() != source_vector.GetType()) {
			return false;
		}
		if (!TypeIsConstantSize(target_vector.GetType().InternalType())) {
			return false;
		}
		if (target_vector.GetVectorType() != VectorType::FLAT_VECTOR ||
		    source_vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		if (FlatVector::GetCapacity(target_vector) < new_count) {
			return false;
		}
		if (FlatVector::Validity(target_vector).CanHaveNull() ||
		    !FlatVector::Validity(source_vector).CheckAllValid(append_count)) {
			return false;
		}
	}
	for (idx_t col_idx = 0; col_idx < target.ColumnCount(); col_idx++) {
		auto &target_vector = target.data[col_idx];
		auto &source_vector = source.data[col_idx];
		const auto type_size = GetTypeIdSize(target_vector.GetType().InternalType());
		auto target_data = FlatVector::GetDataMutable(target_vector) + target_count * type_size;
		auto source_data = FlatVector::GetData(source_vector);
		memcpy(target_data, source_data, append_count * type_size);
		FlatVector::SetSize(target_vector, new_count);
	}
	target.CheckCardinality(new_count);
	return true;
}

static idx_t SljitBatchedSourceContractFetchBudget(idx_t max_chunks) {
	constexpr idx_t SOURCE_FETCHES_PER_DOWNSTREAM_BATCH = 64;
	const auto max_idx = NumericLimits<idx_t>::Maximum();
	if (max_chunks >= max_idx / SOURCE_FETCHES_PER_DOWNSTREAM_BATCH) {
		return max_idx;
	}
	return max_chunks * SOURCE_FETCHES_PER_DOWNSTREAM_BATCH;
}

static void SljitChargeDownstreamRows(idx_t &processed_rows, idx_t rows) {
	const auto max_idx = NumericLimits<idx_t>::Maximum();
	if (processed_rows >= max_idx - rows) {
		processed_rows = max_idx;
		return;
	}
	processed_rows += rows;
}

static bool SljitDownstreamRowBudgetReached(idx_t processed_rows, idx_t max_chunks) {
	const auto max_idx = NumericLimits<idx_t>::Maximum();
	if (max_chunks >= max_idx / STANDARD_VECTOR_SIZE) {
		return false;
	}
	return processed_rows >= max_chunks * STANDARD_VECTOR_SIZE;
}

static bool SljitSinkResultStopsPipeline(SinkResultType sink_result) {
	return sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED;
}

static bool SljitNativeSinkResultStopsExecution(ExecutionRegionRuntime &runtime, SinkResultType sink_result,
                                                ExecutionRegionResult &result) {
	if (!SljitSinkResultStopsPipeline(sink_result)) {
		return false;
	}
	if (sink_result == SinkResultType::BLOCKED) {
		result =
		    runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED : ExecutionRegionResult::DEFERRED;
		return true;
	}
	result = ExecutionRegionResult::FINISHED;
	return true;
}

static bool SljitStopFullPipeline(ExecutionRegionResult &result, ExecutionRegionResult stop_result) {
	result = stop_result;
	return true;
}

template <class FLUSH_BATCH>
static bool SljitStopFullPipelineAfterFlush(ExecutionRegionResult &result, ExecutionRegionResult stop_result,
                                            FLUSH_BATCH flush_batch) {
	if (flush_batch()) {
		return true;
	}
	return SljitStopFullPipeline(result, stop_result);
}

template <class FINALIZE>
static bool SljitStopFullPipelineAfterFinalize(ExecutionRegionResult &result, ExecutionRegionResult stop_result,
                                               FINALIZE finalize) {
	finalize();
	return SljitStopFullPipeline(result, stop_result);
}

template <class BUDGET_REACHED, class EXECUTE_SOURCE_CHUNK, class STOP_NOT_FINISHED, class STOP_BLOCKED,
          class STOP_FINISHED>
static bool SljitRunFullPipelineSourceContractLoop(ExecutionRegionRuntime &runtime, idx_t &fetched_chunks,
                                                   BUDGET_REACHED &&budget_reached,
                                                   EXECUTE_SOURCE_CHUNK &&execute_source_chunk,
                                                   STOP_NOT_FINISHED &&stop_not_finished, STOP_BLOCKED &&stop_blocked,
                                                   STOP_FINISHED &&stop_finished) {
	while (true) {
		if (budget_reached()) {
			return stop_not_finished();
		}

		DataChunk *source_chunk = nullptr;
		auto source_result = runtime.FetchSourceContract(source_chunk);
		if (source_result == SourceResultType::BLOCKED) {
			return stop_blocked();
		}
		fetched_chunks++;

		if (source_chunk && execute_source_chunk(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
			return true;
		}
		if (source_result == SourceResultType::FINISHED) {
			return stop_finished();
		}
	}
}

template <class BUDGET_REACHED, class EXECUTE_SOURCE_CHUNK>
static bool SljitRunFullPipelineSourceContractLoop(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                                   idx_t &fetched_chunks, BUDGET_REACHED &&budget_reached,
                                                   EXECUTE_SOURCE_CHUNK &&execute_source_chunk) {
	return SljitRunFullPipelineSourceContractLoop(
	    runtime, fetched_chunks, std::forward<BUDGET_REACHED>(budget_reached),
	    std::forward<EXECUTE_SOURCE_CHUNK>(execute_source_chunk),
	    [&]() { return SljitStopFullPipeline(result, ExecutionRegionResult::NOT_FINISHED); },
	    [&]() { return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED); },
	    [&]() { return SljitStopFullPipeline(result, ExecutionRegionResult::FINISHED); });
}

template <class BUDGET_REACHED, class EXECUTE_SOURCE_CHUNK, class FLUSH_BATCH>
static bool SljitRunFullPipelineSourceContractLoopAfterFlush(ExecutionRegionRuntime &runtime,
                                                             ExecutionRegionResult &result, idx_t &fetched_chunks,
                                                             BUDGET_REACHED &&budget_reached,
                                                             EXECUTE_SOURCE_CHUNK &&execute_source_chunk,
                                                             FLUSH_BATCH &&flush_batch) {
	return SljitRunFullPipelineSourceContractLoop(
	    runtime, fetched_chunks, std::forward<BUDGET_REACHED>(budget_reached),
	    std::forward<EXECUTE_SOURCE_CHUNK>(execute_source_chunk),
	    [&]() { return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::NOT_FINISHED, flush_batch); },
	    [&]() { return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::INTERRUPTED, flush_batch); },
	    [&]() { return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::FINISHED, flush_batch); });
}

template <class BUDGET_REACHED, class EXECUTE_SOURCE_CHUNK, class FINALIZE>
static bool
SljitRunFullPipelineSourceContractLoopAfterFinalize(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                                    idx_t &fetched_chunks, BUDGET_REACHED &&budget_reached,
                                                    EXECUTE_SOURCE_CHUNK &&execute_source_chunk, FINALIZE &&finalize) {
	return SljitRunFullPipelineSourceContractLoop(
	    runtime, fetched_chunks, std::forward<BUDGET_REACHED>(budget_reached),
	    std::forward<EXECUTE_SOURCE_CHUNK>(execute_source_chunk),
	    [&]() { return SljitStopFullPipelineAfterFinalize(result, ExecutionRegionResult::NOT_FINISHED, finalize); },
	    [&]() { return SljitStopFullPipelineAfterFinalize(result, ExecutionRegionResult::INTERRUPTED, finalize); },
	    [&]() { return SljitStopFullPipelineAfterFinalize(result, ExecutionRegionResult::FINISHED, finalize); });
}

static bool SljitDeferFullPipelineResult(ExecutionRegionRuntime &runtime, string &deferred_reason,
                                         ExecutionRegionResult &result) {
	runtime.Defer(std::move(deferred_reason));
	result = ExecutionRegionResult::DEFERRED;
	return true;
}

static bool SljitDeferBlockedSinkResult(ExecutionRegionRuntime &runtime, string &deferred_reason,
                                        SinkResultType &sink_result) {
	runtime.Defer(std::move(deferred_reason));
	sink_result = SinkResultType::BLOCKED;
	return true;
}

static SinkResultType SljitDeferBlockedSinkResult(ExecutionRegionRuntime &runtime, string &deferred_reason) {
	runtime.Defer(std::move(deferred_reason));
	return SinkResultType::BLOCKED;
}

static bool SljitAdvanceSinkBatchBlocked(ExecutionRegionRuntime &runtime, DataChunk &source_chunk,
                                         bool have_more_output) {
	return runtime.AdvanceSinkBatch(source_chunk, have_more_output) == SinkNextBatchType::BLOCKED;
}

static bool SljitPrepareSourceChunkAsJoinInput(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                               DataChunk &source_chunk, SourceResultType source_result,
                                               DataChunk *&join_input) {
	if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
		return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
	}
	join_input = &source_chunk;
	return false;
}

template <class EXECUTE_BATCH>
static bool SljitFlushRuntimePendingBatch(ExecutionRegionRuntime &runtime, EXECUTE_BATCH execute_batch) {
	auto batch = runtime.PendingSourceContractBatch();
	if (!batch) {
		return false;
	}
	if (execute_batch(*batch)) {
		return true;
	}
	runtime.ResetSourceContractBatch();
	return false;
}

template <class FLUSH_BATCH, class EXECUTE_BATCH>
static bool SljitAppendChunkToInitializedBatch(ExecutionRegionRuntime &runtime, DataChunk &batch, DataChunk &chunk,
                                               idx_t trace_op_idx, optional_ptr<const SljitExecutableRegionOp> trace_op,
                                               const char *append_phase, const char *boundary_phase,
                                               FLUSH_BATCH flush_batch, EXECUTE_BATCH execute_batch) {
	if (chunk.size() == 0) {
		return false;
	}
	if (batch.ColumnCount() != chunk.ColumnCount()) {
		if (flush_batch()) {
			return true;
		}
		return execute_batch(chunk);
	}
	if (batch.size() + chunk.size() > STANDARD_VECTOR_SIZE) {
		if (flush_batch()) {
			return true;
		}
	}
	if (chunk.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
		return execute_batch(chunk);
	}
	const bool trace_append = trace_op && append_phase;
	std::chrono::steady_clock::time_point append_stage_start;
	if (trace_append) {
		append_stage_start = SljitRegionStageStart(runtime);
	}
	if (!SljitTryFastAppendFixedFlatAllValid(batch, chunk)) {
		batch.Append(chunk);
	}
	if (trace_append) {
		RecordSljitRegionStageRuntime(runtime, trace_op_idx, trace_op->kind, append_phase, append_stage_start);
	}
	if (trace_op && boundary_phase) {
		RecordSljitRegionMaterializationBoundary(runtime, trace_op->kind, boundary_phase, chunk.size());
	}
	if (batch.size() == STANDARD_VECTOR_SIZE) {
		if (flush_batch()) {
			return true;
		}
	}
	return false;
}

template <class FLUSH_BATCH, class EXECUTE_BATCH>
static bool SljitAppendChunkToRuntimeBatch(ExecutionRegionRuntime &runtime, DataChunk &chunk,
                                           const vector<LogicalType> &batch_types, idx_t trace_op_idx,
                                           const SljitExecutableRegionOp &trace_op, const char *append_phase,
                                           const char *boundary_phase, FLUSH_BATCH flush_batch,
                                           EXECUTE_BATCH execute_batch) {
	auto &batch = runtime.PrepareSourceContractBatch(batch_types);
	return SljitAppendChunkToInitializedBatch(runtime, batch, chunk, trace_op_idx,
	                                          optional_ptr<const SljitExecutableRegionOp>(&trace_op), append_phase,
	                                          boundary_phase, flush_batch, execute_batch);
}

template <class FLUSH_BATCH, class MATERIALIZE_BATCH>
static bool SljitTryAppendDirectChunkToRuntimeBatch(ExecutionRegionRuntime &runtime, DataChunk &chunk,
                                                    const vector<LogicalType> &batch_types, bool &handled,
                                                    FLUSH_BATCH flush_batch, MATERIALIZE_BATCH materialize_batch) {
	handled = false;
	if (chunk.size() == 0) {
		return false;
	}
	auto &batch = runtime.PrepareSourceContractBatch(batch_types);
	if (batch.size() + chunk.size() > STANDARD_VECTOR_SIZE) {
		if (flush_batch()) {
			return true;
		}
	}
	if (!materialize_batch(batch)) {
		return false;
	}
	handled = true;
	if (batch.size() == STANDARD_VECTOR_SIZE) {
		if (flush_batch()) {
			return true;
		}
	}
	return false;
}

template <class EXECUTE_BATCH>
static bool SljitFlushDataChunkBatch(DataChunk &batch, EXECUTE_BATCH execute_batch) {
	if (batch.size() == 0) {
		return false;
	}
	if (execute_batch(batch)) {
		return true;
	}
	batch.Reset();
	return false;
}

struct SljitRouteChunkBatch {
	explicit SljitRouteChunkBatch(ExecutionRegionRuntime &runtime_p) : runtime(runtime_p) {
	}

	SljitRouteChunkBatch(ExecutionRegionRuntime &runtime_p, idx_t trace_op_idx_p,
	                     optional_ptr<const SljitExecutableRegionOp> trace_op_p, const char *append_phase_p,
	                     const char *boundary_phase_p)
	    : runtime(runtime_p), trace_op_idx(trace_op_idx_p), trace_op(trace_op_p), append_phase(append_phase_p),
	      boundary_phase(boundary_phase_p) {
	}

	template <class EXECUTE_BATCH>
	bool Flush(EXECUTE_BATCH execute_batch) {
		if (batch.Empty()) {
			return false;
		}
		return SljitFlushDataChunkBatch(batch.chunk, execute_batch);
	}

	template <class EXECUTE_BATCH>
	bool Append(DataChunk &chunk, const vector<LogicalType> &batch_types, EXECUTE_BATCH execute_batch) {
		if (chunk.size() == 0) {
			return false;
		}
		batch.Ensure(runtime.GetAllocator(), batch_types);
		auto flush_batch = [&]() {
			return Flush(execute_batch);
		};
		return SljitAppendChunkToInitializedBatch(runtime, batch.chunk, chunk, trace_op_idx, trace_op, append_phase,
		                                          boundary_phase, flush_batch, execute_batch);
	}

	SljitDataChunkBatch &Batch() {
		return batch;
	}

private:
	ExecutionRegionRuntime &runtime;
	SljitDataChunkBatch batch;
	idx_t trace_op_idx = 0;
	optional_ptr<const SljitExecutableRegionOp> trace_op;
	const char *append_phase = nullptr;
	const char *boundary_phase = nullptr;
};

} // namespace duckdb
