//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_pre_join_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"
#include "sljit_projection_executor_runtime.hpp"

namespace duckdb {

static bool SljitPrepareSourceChunkWithOptionalPreJoinProjection(
    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, SljitRegionExecutionScratch &scratch,
    idx_t pre_join_projection_idx, SljitExecutableRegionOp &pre_join_projection_op,
    const SljitInt64ToInt32PreJoinProjection &pre_join_projection, DataChunk &source_chunk,
    SourceResultType source_result, DataChunk *&join_input) {
	if (SljitAdvanceSinkBatchBlocked(runtime, source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
		return SljitStopFullPipeline(result, ExecutionRegionResult::INTERRUPTED);
	}
	SljitPrepareOptionalPreJoinProjectionInput(runtime, scratch, pre_join_projection_idx, pre_join_projection_op,
	                                           source_chunk, pre_join_projection.HasInt64ToInt32Projection(),
	                                           join_input);
	return false;
}

struct SljitPreJoinProjectionJoinInput {
	SljitPreJoinProjectionJoinInput(ExecutionRegionRuntime &runtime_p, ExecutionRegionResult &result_p,
	                                idx_t pre_join_projection_idx_p, SljitExecutableRegionOp &pre_join_projection_op_p,
	                                const SljitInt64ToInt32PreJoinProjection &pre_join_projection_p)
	    : runtime(runtime_p), result(result_p), pre_join_projection_idx(pre_join_projection_idx_p),
	      pre_join_projection_op(pre_join_projection_op_p), pre_join_projection(pre_join_projection_p) {
	}

	bool operator()(SljitRegionExecutionScratch &scratch, DataChunk &source_chunk, SourceResultType source_result,
	                DataChunk *&join_input) {
		return SljitPrepareSourceChunkWithOptionalPreJoinProjection(runtime, result, scratch, pre_join_projection_idx,
		                                                            pre_join_projection_op, pre_join_projection,
		                                                            source_chunk, source_result, join_input);
	}

	ExecutionRegionRuntime &runtime;
	ExecutionRegionResult &result;
	idx_t pre_join_projection_idx;
	SljitExecutableRegionOp &pre_join_projection_op;
	const SljitInt64ToInt32PreJoinProjection &pre_join_projection;
};

} // namespace duckdb
