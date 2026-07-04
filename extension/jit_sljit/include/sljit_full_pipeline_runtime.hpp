//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_runtime_batch_state.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

#include <limits>
#include <utility>

namespace duckdb {

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

struct SljitSourceChunkJoinInput {
	SljitSourceChunkJoinInput(ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p)
	    : runtime(runtime_p), result(result_p) {
	}

	bool operator()(SljitRegionExecutionScratch &, DataChunk &source_chunk, SourceResultType source_result,
	                DataChunk *&join_input) {
		return SljitPrepareSourceChunkAsJoinInput(runtime, result, source_chunk, source_result, join_input);
	}

	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
};

struct SljitMaterializedChunkJoinInput {
	bool operator()(SljitRegionExecutionScratch &, DataChunk &source_chunk, SourceResultType,
	                DataChunk *&join_input) {
		join_input = &source_chunk;
		return false;
	}
};

} // namespace duckdb
