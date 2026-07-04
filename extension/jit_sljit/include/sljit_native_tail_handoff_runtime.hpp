//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_tail_handoff_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_runtime_batch_view.hpp"

#include "duckdb/execution/execution_region_runtime.hpp"

namespace duckdb {

struct SljitNativeTailHandoffPrimitive {
	idx_t start_op_idx = 0;
};

static bool SljitCanBindNativeTailHandoffPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t start_op_idx) {
	return start_op_idx < ops.size();
}

static SljitNativeTailHandoffPrimitive SljitBindNativeTailHandoffPrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                           idx_t start_op_idx) {
	if (!SljitCanBindNativeTailHandoffPrimitive(ops, start_op_idx)) {
		throw InternalException("SLJIT native tail handoff primitive cannot bind requested operator");
	}
	SljitNativeTailHandoffPrimitive primitive;
	primitive.start_op_idx = start_op_idx;
	return primitive;
}

static DataChunk &SljitBindNativeTailHandoffInput(const SljitRuntimeBatchView &input) {
	if (!input.HasChunk()) {
		throw InternalException("SLJIT native tail handoff requires a materialized input chunk");
	}
	if (!input.IsMaterializedChunk()) {
		throw InternalException("SLJIT native tail handoff requires a materialized batch view");
	}
	auto &chunk = input.Chunk();
	if (input.count != chunk.size()) {
		throw InternalException("SLJIT native tail handoff count does not match input chunk cardinality");
	}
	return chunk;
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static SinkResultType
SljitExecuteNativeTailHandoff(SljitRegionExecutionScratch &scratch, idx_t start_op_idx,
                              const SljitRuntimeBatchView &input,
                              EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	auto &chunk = SljitBindNativeTailHandoffInput(input);
	if (input.count == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	return execute_native_full_pipeline_from(scratch, start_op_idx, chunk);
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static SinkResultType
SljitExecuteNativeTailHandoff(SljitRegionExecutionScratch &scratch, const SljitNativeTailHandoffPrimitive &primitive,
                              const SljitRuntimeBatchView &input,
                              EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	return SljitExecuteNativeTailHandoff(scratch, primitive.start_op_idx, input, execute_native_full_pipeline_from);
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitExecuteNativeTailHandoffBatch(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                               SljitRegionExecutionScratch &scratch, idx_t start_op_idx,
                                               const SljitRuntimeBatchView &input, idx_t &processed_batches,
                                               EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	if (input.count == 0) {
		return false;
	}
	auto sink_result = SljitExecuteNativeTailHandoff(scratch, start_op_idx, input, execute_native_full_pipeline_from);
	if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
		return true;
	}
	processed_batches++;
	return false;
}

template <class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitExecuteNativeTailHandoffBatch(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
                                               SljitRegionExecutionScratch &scratch,
                                               const SljitNativeTailHandoffPrimitive &primitive,
                                               const SljitRuntimeBatchView &input, idx_t &processed_batches,
                                               EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	return SljitExecuteNativeTailHandoffBatch(runtime, result, scratch, primitive.start_op_idx, input,
	                                          processed_batches, execute_native_full_pipeline_from);
}

template <class STOP_AFTER_SINK_RESULT, class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool
SljitExecuteNativeTailHandoffIntoSink(SljitRegionExecutionScratch &scratch, idx_t start_op_idx,
                                      const SljitRuntimeBatchView &input,
                                      STOP_AFTER_SINK_RESULT &&stop_after_sink_result,
                                      EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	auto sink_result = SljitExecuteNativeTailHandoff(scratch, start_op_idx, input, execute_native_full_pipeline_from);
	return stop_after_sink_result(sink_result);
}

template <class STOP_AFTER_SINK_RESULT, class EXECUTE_NATIVE_FULL_PIPELINE_FROM>
static bool SljitExecuteNativeTailHandoffIntoSink(
    SljitRegionExecutionScratch &scratch, const SljitNativeTailHandoffPrimitive &primitive,
    const SljitRuntimeBatchView &input, STOP_AFTER_SINK_RESULT &&stop_after_sink_result,
    EXECUTE_NATIVE_FULL_PIPELINE_FROM &&execute_native_full_pipeline_from) {
	return SljitExecuteNativeTailHandoffIntoSink(scratch, primitive.start_op_idx, input, stop_after_sink_result,
	                                             execute_native_full_pipeline_from);
}

} // namespace duckdb
