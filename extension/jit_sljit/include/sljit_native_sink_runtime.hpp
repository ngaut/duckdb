//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_sink_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_update_runtime.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_projection_expression_runtime.hpp"

namespace duckdb {

static SinkResultType SljitExecuteNativeHashJoinBuild(ExecutionRegionRuntime &runtime,
                                                      ExecutionOperatorRuntime &native_runtime,
                                                      SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                      SljitExecutableRegionOp &op, DataChunk &input,
                                                      DataChunk &source_chunk, Vector &hash_values,
                                                      SelectionVector &build_sel) {
	auto &binding = SljitBindRecordedNativeSink(runtime, native_runtime, scratch, op_idx, op.Kind(), input,
	                                            op.HashJoinBuild().plan.sink_info,
	                                            "hash-join-build-runtime-binding-failed", "SLJIT hash join build sink");
	if (!binding.ready || !binding.hash_join_build.ready) {
		throw InternalException("SLJIT hash join build sink binding did not return a ready build state");
	}
	return SljitExecuteNativeHashJoinBuildUpdate(runtime, op_idx, op.Kind(), binding.hash_join_build, input,
	                                             source_chunk, hash_values, build_sel);
}

} // namespace duckdb
