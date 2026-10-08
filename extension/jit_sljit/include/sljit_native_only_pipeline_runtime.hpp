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
#include "sljit_source_fetch_primitive_runtime.hpp"

namespace duckdb {

template <class EXECUTE_NATIVE_FULL_PIPELINE>
static bool SljitTryExecuteFullPipelineNativeOnly(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                                  vector<SljitExecutableRegionOp> &ops,
                                                  EXECUTE_NATIVE_FULL_PIPELINE &&execute_native_full_pipeline,
                                                  SljitRegionExecutionScratch &scratch) {
	idx_t processed_chunks = 0;
	idx_t fetched_chunks = 0;
	const auto max_chunks = runtime.MaxChunks();
	const auto max_fetched_chunks = SljitBatchedSourceContractFetchBudget(max_chunks);
	// Small source chunks (a selective scan filter) batch before the native operators run, so they pay the
	// per-chunk operator and sink cost once per batch
	SljitSourceFetchPrimitiveRuntime source_fetch(runtime);
	auto execute_native_batch = [&](const SljitRuntimeBatchView &input, bool) {
		auto &batch = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT native-only source batch");
		auto sink_result = execute_native_full_pipeline(scratch, batch);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			execute_native_full_pipeline.Finalize(scratch);
			return true;
		}
		processed_chunks++;
		return false;
	};
	auto flush = [&]() {
		if (source_fetch.Flush(true, execute_native_batch)) {
			return true;
		}
		execute_native_full_pipeline.Finalize(scratch);
		return false;
	};
	auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) {
		if (source_chunk.size() == 0) {
			return false;
		}
		// The sink takes rows under the partition current when they sink: rows batched from earlier source chunks
		// sink before this chunk moves the sink to its partition
		if (runtime.AdvancesSinkBatch(source_chunk, have_more_output) && flush()) {
			return true;
		}
		if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, have_more_output)) {
			return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
		}
		return source_fetch.Execute(source_chunk, have_more_output, execute_native_batch);
	};
	auto stop_after_flush = [&](ExecutionRegionResult stop_result) {
		return flush() || SljitStopFullPipeline(result, stop_result);
	};
	return SljitRunFullPipelineSourceContractLoop(
	    runtime, fetched_chunks,
	    [&]() { return processed_chunks >= max_chunks || fetched_chunks >= max_fetched_chunks; },
	    execute_source_chunk, [&]() { return stop_after_flush(ExecutionRegionResult::NOT_FINISHED); },
	    [&]() { return stop_after_flush(SljitBlockedSourceStopResult(runtime)); },
	    [&]() { return stop_after_flush(ExecutionRegionResult::FINISHED); });
}

} // namespace duckdb
