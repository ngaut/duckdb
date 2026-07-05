//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_sink_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_filter_runtime.hpp"
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

static SinkResultType SljitExecuteNativeNestedLoopJoinBuild(ExecutionOperatorRuntime &native_runtime,
                                                            SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                            SljitExecutableRegionOp &op, DataChunk &input,
                                                            DataChunk &right_condition) {
	if (op.nested_loop_join_build.rhs_conditions.size() != right_condition.ColumnCount()) {
		throw InternalException("SLJIT nested loop join build condition expression count mismatch");
	}
	right_condition.Reset();
	for (idx_t condition_idx = 0; condition_idx < op.nested_loop_join_build.rhs_conditions.size(); condition_idx++) {
		SljitExecuteProjectionExpression(op.nested_loop_join_build.rhs_conditions[condition_idx], input,
		                                 right_condition.data[condition_idx], nullptr, input.size(),
		                                 scratch.ExpressionAdapterScratch(op_idx, condition_idx));
	}
	right_condition.SetChildCardinality(input.size());

	auto &binding = SljitBindNativeSink(
	    native_runtime, scratch, op_idx, input, op.nested_loop_join_build.plan.sink_info,
	    "nested-loop-join-build-native-runtime-binding-failed", "SLJIT native nested loop join build sink");
	if (!binding.ready || !binding.nested_loop_join_build.ready) {
		throw InternalException("SLJIT native nested loop join build sink binding did not return a ready build state");
	}
	return ExecutionSinkNestedLoopJoinBuild(binding.nested_loop_join_build, input, right_condition);
}

static SinkResultType SljitExecuteNativeAppendSink(ExecutionOperatorRuntime &native_runtime,
                                                   SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                   SljitExecutableRegionOp &op, DataChunk &input) {
	auto &binding = SljitBindNativeSink(native_runtime, scratch, op_idx, input, op.append_sink.plan.sink_info,
	                                    "append-sink-runtime-binding-failed", "SLJIT append sink");
	if (!binding.ready || !binding.append_sink.ready) {
		throw InternalException("SLJIT append sink binding did not return a ready append state");
	}
	return ExecutionSinkAppend(binding.append_sink, input);
}

static SinkResultType SljitExecuteNativeDelimJoinSink(ExecutionOperatorRuntime &native_runtime,
                                                      SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                      SljitExecutableRegionOp &op, DataChunk &input) {
	auto &binding = SljitBindNativeSink(native_runtime, scratch, op_idx, input, op.delim_join_sink.plan.sink_info,
	                                    "delim-join-sink-runtime-binding-failed", "SLJIT delimiter join sink");
	if (!binding.ready || !binding.delim_join_sink.ready) {
		throw InternalException("SLJIT delimiter join sink binding did not return a ready delimiter state");
	}
	return ExecutionSinkDelimJoin(binding.delim_join_sink, input);
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
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD: {
		if (!is_final_operator) {
			throw InternalException("SLJIT nested loop join build sink must be the final full pipeline operator");
		}
		auto stage_start = SljitRegionStageStart(runtime);
		sink_result = SljitExecuteNativeNestedLoopJoinBuild(native_runtime, scratch, op_idx, op, input,
		                                                    scratch.NestedLoopConditionChunk(op_idx));
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
		return true;
	}
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
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK: {
		if (!is_final_operator) {
			throw InternalException("SLJIT delimiter join sink must be the final full pipeline operator");
		}
		auto stage_start = SljitRegionStageStart(runtime);
		sink_result = SljitExecuteNativeDelimJoinSink(native_runtime, scratch, op_idx, op, input);
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
                                              SinkResultType &sink_result,
                                              optional_ptr<bool> deferred_grouped_finish = nullptr) {
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
	if (deferred_grouped_finish) {
		sink_result = SljitExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, nullptr,
		                                                DConstants::INVALID_INDEX, true, deferred_grouped_finish);
	} else {
		sink_result = SljitExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input);
	}
	return true;
}

static bool SljitTryExecuteNativeTailTerminalSink(ExecutionRegionRuntime &runtime,
                                                  ExecutionOperatorRuntime &native_runtime,
                                                  SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                  SljitExecutableRegionOp &op, DataChunk &input,
                                                  bool is_final_operator, SinkResultType &sink_result,
                                                  optional_ptr<bool> deferred_grouped_finish = nullptr) {
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
	if (deferred_grouped_finish) {
		throw InternalException("SLJIT native tail aggregate sink cannot defer generated grouped finish");
	}
	sink_result = SljitExecuteDuckDBAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input);
	return true;
}

static bool SljitCanExecuteFilterAggregateUpdate(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
	if (op_idx + 2 != ops.size()) {
		return false;
	}
	return ops[op_idx].kind == SljitNativeRegionOpKind::FILTER &&
	       ops[op_idx + 1].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	       ops[op_idx + 1].aggregate_update.plan.use_primitive_payloads;
}

static bool SljitCanExecuteGeneratedFilterAggregateUpdate(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
	return SljitCanExecuteFilterAggregateUpdate(ops, op_idx) &&
	       ops[op_idx + 1].aggregate_update.filtered_update.IsExecutable();
}

static bool SljitTryExecuteNativeFilterAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    vector<SljitExecutableRegionOp> &ops, idx_t op_idx, DataChunk &input, SinkResultType &sink_result,
    bool &record_sink_result, optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (SljitCanExecuteGeneratedFilterAggregateUpdate(ops, op_idx)) {
		auto &aggregate_op = ops[op_idx + 1];
		sink_result = SljitExecuteNativeFilteredAggregateUpdate(runtime, native_runtime, scratch, op_idx + 1,
		                                                        aggregate_op, input);
		record_sink_result = true;
		return true;
	}
	if (!SljitCanExecuteFilterAggregateUpdate(ops, op_idx)) {
		return false;
	}

	auto &filter_op = ops[op_idx];
	auto &aggregate_op = ops[op_idx + 1];
	auto &filter_selection = scratch.FilterSelection(op_idx);
	auto filter_stage_start = SljitRegionStageStart(runtime);
	auto selected_count =
	    SljitSelectFilter(filter_op, input, filter_selection, scratch.ExpressionAdapterScratch(op_idx, 0));
	RecordSljitRegionStageRuntime(runtime, op_idx, filter_op.kind, "selection", filter_stage_start);
	if (selected_count == 0) {
		sink_result = SinkResultType::NEED_MORE_INPUT;
		record_sink_result = false;
		return true;
	}
	if (deferred_grouped_finish) {
		sink_result =
		    SljitExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, op_idx + 1, aggregate_op, input,
		                                      &filter_selection, selected_count, true, deferred_grouped_finish);
	} else {
		sink_result = SljitExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, op_idx + 1, aggregate_op,
		                                                input, &filter_selection, selected_count);
	}
	record_sink_result = true;
	return true;
}

} // namespace duckdb
