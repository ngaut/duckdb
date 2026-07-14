//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_only_pipeline_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

template <class EXECUTE_NATIVE_FULL_PIPELINE>
static bool SljitTryExecuteFullPipelineNativeOnly(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                                  vector<SljitExecutableRegionOp> &ops,
                                                  EXECUTE_NATIVE_FULL_PIPELINE &&execute_native_full_pipeline,
                                                  SljitRegionExecutionScratch &scratch) {
	idx_t processed_chunks = 0;
	idx_t fetched_chunks = 0;
	const auto max_chunks = runtime.MaxChunks();
	auto execute_native_batch = [&](DataChunk &batch, bool have_more_output) {
		if (SljitAdvanceSinkBatchBlocked(runtime, batch, have_more_output)) {
			return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
		}
		if (batch.size() > 0) {
			auto sink_result = execute_native_full_pipeline(scratch, batch);
			if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
				execute_native_full_pipeline.Finalize(scratch);
				return true;
			}
			processed_chunks++;
		}
		return false;
	};
	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) {
		return execute_native_batch(source_chunk, have_more_output);
	};
	auto stop_after_flush = [&](ExecutionRegionResult stop_result) {
		execute_native_full_pipeline.Finalize(scratch);
		return SljitStopFullPipeline(result, stop_result);
	};
	return SljitRunFullPipelineSourceContractLoop(
	    runtime, fetched_chunks, [&]() { return processed_chunks >= max_chunks; }, execute_source_chunk,
	    [&]() { return stop_after_flush(ExecutionRegionResult::NOT_FINISHED); },
	    [&]() { return stop_after_flush(ExecutionRegionResult::INTERRUPTED); },
	    [&]() { return stop_after_flush(ExecutionRegionResult::FINISHED); });
}

} // namespace duckdb
