//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_source_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_projection_build_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_projection_executor_runtime.hpp"
#include "sljit_filter_runtime.hpp"
#include "sljit_projection_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

template <class EXECUTE_NATIVE_FULL_PIPELINE>
static bool SljitTryExecuteFullPipelineSourceInputBatched(ExecutionRegionRuntime &runtime,
                                                          ExecutionRegionResult &result,
                                                          vector<SljitExecutableRegionOp> &ops,
                                                          EXECUTE_NATIVE_FULL_PIPELINE &&execute_native_full_pipeline) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_batches = 0;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
	idx_t trace_op_idx = 0;
	for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
		if (ops[op_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			trace_op_idx = op_idx;
			break;
		}
	}
	auto &trace_op = ops[trace_op_idx];
	SljitRouteChunkBatch source_batch(runtime, trace_op_idx, optional_ptr<const SljitExecutableRegionOp>(&trace_op),
	                                  "source_input_batch_append", "source_input_batch");

	auto execute_source_batch = [&](DataChunk &input) -> bool {
		if (input.size() == 0) {
			return false;
		}
		auto sink_result = execute_native_full_pipeline(scratch, input);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	};

	auto flush_source_batch = [&]() -> bool {
		return source_batch.Flush(execute_source_batch);
	};

	auto append_source_batch = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::INTERRUPTED, flush_source_batch);
		}
		return source_batch.Append(source_chunk, source_chunk.GetTypes(), execute_source_batch);
	};

	return SljitRunFullPipelineSourceContractLoopAfterFlush(
	    runtime, result, fetched_chunks,
	    [&]() { return processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches; },
	    append_source_batch, flush_source_batch);
}

template <class EXECUTE_NATIVE_FULL_PIPELINE>
static bool SljitTryExecuteFullPipelineUnbatched(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                                 vector<SljitExecutableRegionOp> &ops,
                                                 bool uses_extended_source_fetch_budget,
                                                 EXECUTE_NATIVE_FULL_PIPELINE &&execute_native_full_pipeline) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t processed_chunks = 0;
	idx_t fetched_chunks = 0;
	const auto max_chunks = uses_extended_source_fetch_budget
	                            ? SljitBatchedSourceContractFetchBudget(runtime.MaxChunks())
	                            : runtime.MaxChunks();
	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
		}
		if (source_chunk.size() > 0) {
			auto sink_result = execute_native_full_pipeline(scratch, source_chunk);
			if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
				return true;
			}
			processed_chunks++;
		}
		return false;
	};
	return SljitRunFullPipelineSourceContractLoop(
	    runtime, result, fetched_chunks, [&]() { return processed_chunks >= max_chunks; }, execute_source_chunk);
}

template <class EXECUTE_NATIVE_FULL_PIPELINE>
static bool SljitTryExecuteFullPipelineBatched(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                               vector<SljitExecutableRegionOp> &ops,
                                               EXECUTE_NATIVE_FULL_PIPELINE &&execute_native_full_pipeline) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_batches = 0;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());

	auto execute_chunk = [&](DataChunk &input, bool have_more_output) -> bool {
		if (input.size() == 0) {
			return false;
		}
		if (SljitAdvanceSinkBatchBlocked(runtime, input, have_more_output)) {
			return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
		}
		auto sink_result = execute_native_full_pipeline(scratch, input);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	};

	auto flush_batch = [&](bool have_more_output) -> bool {
		return SljitFlushRuntimePendingBatch(runtime,
		                                     [&](DataChunk &batch) { return execute_chunk(batch, have_more_output); });
	};

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (source_chunk.size() > 0) {
			auto &batch = runtime.PrepareSourceContractBatch(source_chunk.GetTypes());
			if (batch.size() + source_chunk.size() > STANDARD_VECTOR_SIZE) {
				if (flush_batch(true)) {
					return true;
				}
			}
			if (source_chunk.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
				if (execute_chunk(source_chunk, have_more_output)) {
					return true;
				}
			} else {
				if (!SljitTryFastAppendFixedFlatAllValid(batch, source_chunk)) {
					batch.Append(source_chunk);
				}
				if (batch.size() == STANDARD_VECTOR_SIZE) {
					if (flush_batch(have_more_output)) {
						return true;
					}
				}
			}
		}
		return false;
	};

	return SljitRunFullPipelineSourceContractLoop(
	    runtime, fetched_chunks,
	    [&]() { return processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches; },
	    execute_source_chunk, [&]() { return SljitStopFullPipeline(result, ExecutionRegionResult::NOT_FINISHED); },
	    [&]() { return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED); },
	    [&]() {
		    return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::FINISHED,
		                                           [&]() { return flush_batch(false); });
	    });
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitTryExecuteFullPipelineGeneratedFilterBatched(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
	idx_t fetched_chunks = 0;
	idx_t processed_batches = 0;
	const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());

	auto execute_batch = [&](DataChunk &input) -> bool {
		if (input.size() == 0) {
			return false;
		}
		auto sink_result = execute_native_full_pipeline_from(scratch, 2, input);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	};

	auto flush_batch = [&]() -> bool {
		return SljitFlushRuntimePendingBatch(runtime, execute_batch);
	};

	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
		}
		if (source_chunk.size() > 0) {
			auto &filter_selection = scratch.FilterSelection(0);
			auto filter_stage_start = SljitRegionStageStart(runtime);
			auto selected_count =
			    SljitSelectFilter(ops[0], source_chunk, filter_selection, scratch.ExpressionAdapterScratch(0, 0));
			RecordSljitRegionStageRuntime(runtime, 0, ops[0].kind, "selection", filter_stage_start);
			if (selected_count > 0) {
				auto &batch = runtime.PrepareSourceContractBatch(ops[1].output_types);
				if (batch.size() + selected_count > STANDARD_VECTOR_SIZE) {
					if (flush_batch()) {
						return true;
					}
				}
				auto append_stage_start = SljitRegionStageStart(runtime);
				if (!SljitTryAppendReferenceProjectionToBatch(batch, source_chunk, ops[1], filter_selection,
				                                              selected_count)) {
					auto &filtered = scratch.TemporaryChunk(1);
					filtered.Reset();
					SljitExecuteProjection(scratch, 1, ops[1], source_chunk, filtered, &filter_selection,
					                       selected_count);
					if (!SljitTryFastAppendFixedFlatAllValid(batch, filtered)) {
						batch.Append(filtered);
					}
				}
				RecordSljitRegionStageRuntime(runtime, 1, ops[1].kind, "batch_append", append_stage_start);
				if (batch.size() == STANDARD_VECTOR_SIZE) {
					if (flush_batch()) {
						return true;
					}
				}
			}
		}
		return false;
	};

	return SljitRunFullPipelineSourceContractLoop(
	    runtime, fetched_chunks,
	    [&]() { return processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches; },
	    execute_source_chunk, [&]() { return SljitStopFullPipeline(result, ExecutionRegionResult::NOT_FINISHED); },
	    [&]() { return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED); },
	    [&]() { return SljitStopFullPipelineAfterFlush(result, ExecutionRegionResult::FINISHED, flush_batch); });
}

} // namespace duckdb
