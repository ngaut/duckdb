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
	auto &binding = SljitBindRecordedNativeSink(runtime, native_runtime, scratch, op_idx, op.kind, input,
	                                            op.hash_join_build.plan.sink_info,
	                                            "hash-join-build-runtime-binding-failed", "SLJIT hash join build sink");
	if (!binding.ready || !binding.hash_join_build.ready) {
		throw InternalException("SLJIT hash join build sink binding did not return a ready build state");
	}
	return SljitExecuteNativeHashJoinBuildUpdate(runtime, op_idx, op.kind, binding.hash_join_build, input, source_chunk,
	                                             hash_values, build_sel);
}

static SinkResultType SljitExecuteNativeAppendSink(ExecutionOperatorRuntime &native_runtime,
                                                   SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                   SljitExecutableRegionOp &op, DataChunk &input) {
	auto &binding = SljitBindNativeSink(native_runtime, scratch, op_idx, input, op.append_sink.plan.sink_info,
	                                    "append-sink-runtime-binding-failed", "SLJIT append sink");
	if (!binding.ready || !binding.append_sink.ready) {
		throw InternalException("SLJIT append sink binding did not return a ready append state");
	}
	return native_runtime.AppendSink(binding.append_sink, input);
}

static SinkResultType SljitExecuteNativeOrderSink(ExecutionRegionRuntime &runtime,
                                                  ExecutionOperatorRuntime &native_runtime,
                                                  SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                  SljitExecutableRegionOp &op, DataChunk &input, DataChunk &order_keys,
                                                  DataChunk &payload) {
	if (op.order_sink.order_keys.size() != order_keys.ColumnCount()) {
		throw InternalException("SLJIT ordered sink key expression count mismatch");
	}
	auto key_stage_start = SljitRegionStageStart(runtime);
	order_keys.Reset();
	for (idx_t key_idx = 0; key_idx < op.order_sink.order_keys.size(); key_idx++) {
		SljitExecuteProjectionExpression(op.order_sink.order_keys[key_idx], input, order_keys.data[key_idx], nullptr,
		                                 input.size(), scratch.ExpressionAdapterScratch(op_idx, key_idx));
	}
	order_keys.SetChildCardinality(input.size());
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "key_projection", key_stage_start);

	auto sink_stage_start = SljitRegionStageStart(runtime);
	auto &binding = SljitBindNativeSink(native_runtime, scratch, op_idx, input, op.order_sink.plan.sink_info,
	                                    "ordered-sink-runtime-binding-failed", "SLJIT ordered sink");
	if (!binding.ready || !binding.ordered_sink.ready) {
		throw InternalException("SLJIT ordered sink binding did not return a ready ordered sink state");
	}
	payload.Reset();
	payload.Reference(input);
	auto result = ExecutionSinkOrdered(binding.ordered_sink, order_keys, payload);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "sink_update", sink_stage_start);
	return result;
}

static bool SljitTryExecuteNativeTerminalNonAggregateSink(ExecutionRegionRuntime &runtime,
                                                          ExecutionOperatorRuntime &native_runtime,
                                                          SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                          SljitExecutableRegionOp &op, DataChunk &input,
                                                          bool is_final_operator, SinkResultType &sink_result) {
	switch (op.kind) {
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		if (!is_final_operator) {
			throw InternalException("SLJIT hash join build sink must be the final full pipeline operator");
		}
		sink_result = SljitExecuteNativeHashJoinBuild(
		    runtime, native_runtime, scratch, op_idx, op, input, scratch.HashJoinBuildSourceChunk(op_idx),
		    scratch.HashJoinBuildHashValues(op_idx), scratch.HashJoinBuildSelection(op_idx));
		return true;
	case SljitNativeRegionOpKind::ORDER_SINK:
		if (!is_final_operator) {
			throw InternalException("SLJIT ordered sink must be the final full pipeline operator");
		}
		sink_result = SljitExecuteNativeOrderSink(runtime, native_runtime, scratch, op_idx, op, input,
		                                          scratch.OrderKeyChunk(op_idx), scratch.OrderPayloadChunk(op_idx));
		return true;
	case SljitNativeRegionOpKind::APPEND_SINK: {
		if (!is_final_operator) {
			throw InternalException("SLJIT append sink must be the final full pipeline operator");
		}
		auto stage_start = SljitRegionStageStart(runtime);
		sink_result = SljitExecuteNativeAppendSink(native_runtime, scratch, op_idx, op, input);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
		return true;
	}
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return false;
	default:
		return false;
	}
}

static bool SljitTryExecuteNativeTerminalSink(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                              SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                              SljitExecutableRegionOp &op, DataChunk &input, bool is_final_operator,
                                              bool defer_grouped_finish, SinkResultType &sink_result) {
	if (SljitTryExecuteNativeTerminalNonAggregateSink(runtime, native_runtime, scratch, op_idx, op, input,
	                                                  is_final_operator, sink_result)) {
		return true;
	}
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	if (!is_final_operator) {
		throw InternalException("SLJIT aggregate update sink must be the final full pipeline operator");
	}
	if (defer_grouped_finish) {
		sink_result = SljitExecuteNativePipelineAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input,
		                                                        nullptr, DConstants::INVALID_INDEX, true);
	} else {
		sink_result = SljitExecuteNativePipelineAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input);
	}
	return true;
}

static bool SljitTryExecuteNativeTailTerminalSink(ExecutionRegionRuntime &runtime,
                                                  ExecutionOperatorRuntime &native_runtime,
                                                  SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                  SljitExecutableRegionOp &op, DataChunk &input, bool is_final_operator,
                                                  bool defer_grouped_finish, SinkResultType &sink_result) {
	if (SljitTryExecuteNativeTerminalNonAggregateSink(runtime, native_runtime, scratch, op_idx, op, input,
	                                                  is_final_operator, sink_result)) {
		return true;
	}
	if (op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
		return false;
	}
	if (!is_final_operator) {
		throw InternalException("SLJIT aggregate update sink must be the final full pipeline operator");
	}
	if (defer_grouped_finish) {
		throw InternalException("SLJIT native tail aggregate sink cannot defer generated grouped finish");
	}
	sink_result = SljitExecuteDuckDBAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input);
	return true;
}

} // namespace duckdb
