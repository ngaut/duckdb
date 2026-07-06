//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_ungrouped_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_runtime_batch_view.hpp"

#include <chrono>

namespace duckdb {

enum class SljitUngroupedAggregateUpdateStrategyKind : uint8_t {
	INVALID,
	DIRECT_PRIMITIVE_PAYLOAD_UPDATE,
	FILTERED_PRIMITIVE_PAYLOAD_UPDATE
};

struct SljitUngroupedAggregateUpdatePrimitive {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
	idx_t filter_idx = DConstants::INVALID_INDEX;
	SljitUngroupedAggregateUpdateStrategyKind strategy =
	    SljitUngroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE;
};

static bool SljitCanBindUngroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                          idx_t aggregate_idx) {
	return aggregate_idx < ops.size() && ops[aggregate_idx].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	       ops[aggregate_idx].aggregate_update.plan.sink_info.kind ==
	           ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	       ops[aggregate_idx].aggregate_update.plan.use_primitive_payloads;
}

static bool SljitCanBindFilteredUngroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                                  idx_t filter_idx, idx_t aggregate_idx) {
	return filter_idx < ops.size() && ops[filter_idx].kind == SljitNativeRegionOpKind::FILTER &&
	       SljitCanBindUngroupedAggregateUpdatePrimitive(ops, aggregate_idx) &&
	       ops[aggregate_idx].aggregate_update.filtered_update.IsExecutable();
}

static SljitUngroupedAggregateUpdatePrimitive
SljitBindUngroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t aggregate_idx) {
	if (!SljitCanBindUngroupedAggregateUpdatePrimitive(ops, aggregate_idx)) {
		throw InternalException("SLJIT ungrouped aggregate update primitive cannot bind requested operator");
	}
	SljitUngroupedAggregateUpdatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	primitive.strategy = SljitUngroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE;
	return primitive;
}

static SljitUngroupedAggregateUpdatePrimitive
SljitBindFilteredUngroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t filter_idx,
                                                   idx_t aggregate_idx) {
	if (!SljitCanBindFilteredUngroupedAggregateUpdatePrimitive(ops, filter_idx, aggregate_idx)) {
		throw InternalException("SLJIT filtered ungrouped aggregate update primitive cannot bind requested operators");
	}
	SljitUngroupedAggregateUpdatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	primitive.filter_idx = filter_idx;
	primitive.strategy = SljitUngroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE;
	return primitive;
}

static bool SljitCanBindUngroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                          const SljitUngroupedAggregateUpdatePrimitive &primitive) {
	switch (primitive.strategy) {
	case SljitUngroupedAggregateUpdateStrategyKind::DIRECT_PRIMITIVE_PAYLOAD_UPDATE:
		return primitive.filter_idx == DConstants::INVALID_INDEX &&
		       SljitCanBindUngroupedAggregateUpdatePrimitive(ops, primitive.aggregate_idx);
	case SljitUngroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE:
		return SljitCanBindFilteredUngroupedAggregateUpdatePrimitive(ops, primitive.filter_idx,
		                                                             primitive.aggregate_idx);
	case SljitUngroupedAggregateUpdateStrategyKind::INVALID:
		break;
	}
	return false;
}

struct SljitBoundUngroupedPrimitiveAggregateUpdate {
	bool ready = false;
	idx_t op_idx = DConstants::INVALID_INDEX;
	optional_ptr<SljitExecutableRegionOp> op;
	optional_ptr<const vector<ExecutionRegionAggregateInput>> aggregates;
	optional_ptr<const vector<const ExecutionPrimitiveAggregateUpdateLane *>> payload_lanes;
	optional_ptr<SljitAggregatePayloadAdapterScratch> payload_scratch;
	SljitBoundSingleFusedPrimitiveAggregatePayloadUpdate single_fused_payload_update;
};

static void SljitBindUngroupedPrimitiveAggregateUpdate(ExecutionOperatorRuntime &native_runtime,
                                                       SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                       SljitExecutableRegionOp &op, DataChunk &input,
                                                       SljitBoundUngroupedPrimitiveAggregateUpdate &bound) {
	if (bound.ready) {
		return;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.use_primitive_payloads || op.aggregate_update.plan.use_grouped_state_addresses) {
		throw InternalException("SLJIT ungrouped primitive aggregate update received a non-ungrouped aggregate");
	}
	auto &binding =
	    SljitBindNativeSink(native_runtime, scratch, op_idx, input, sink_info,
	                        "aggregate-update-runtime-binding-failed", "SLJIT ungrouped aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT ungrouped aggregate update sink binding did not return a ready aggregate state");
	}
	auto &primitive = binding.aggregate_update.primitive;
	if (!primitive.ready) {
		auto blocker = primitive.blocker.empty() ? "aggregate-primitive-binding-missing" : primitive.blocker;
		throw InternalException("SLJIT ungrouped aggregate primitive update binding failed: %s", blocker.c_str());
	}
	auto &aggregates = sink_info.aggregates;
	if (aggregates.size() != op.aggregate_update.payloads.size()) {
		throw InternalException("SLJIT ungrouped aggregate primitive payload count mismatch");
	}
	if (!op.aggregate_update.fused_payload_update_function &&
	    aggregates.size() != op.aggregate_update.payload_update_functions.size()) {
		throw InternalException("SLJIT ungrouped aggregate primitive payload function count mismatch");
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, primitive);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	if (op.aggregate_update.fused_payload_update_function) {
		SljitBindSingleFusedPrimitiveAggregatePayloadUpdate(
		    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates, payload_lanes,
		    bound.single_fused_payload_update);
	}
	bound.ready = true;
	bound.op_idx = op_idx;
	bound.op = &op;
	bound.aggregates = &aggregates;
	bound.payload_lanes = &payload_lanes;
	bound.payload_scratch = &payload_scratch;
}

static SinkResultType
SljitExecuteBoundUngroupedPrimitiveAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                   SljitRegionExecutionScratch &scratch,
                                                   SljitBoundUngroupedPrimitiveAggregateUpdate &bound, DataChunk &input,
                                                   const SelectionVector *execute_sel, idx_t count) {
	if (!bound.ready || !bound.op || !bound.aggregates || !bound.payload_lanes || !bound.payload_scratch) {
		throw InternalException("SLJIT ungrouped primitive aggregate update executed before binding");
	}
	auto &op = *bound.op;
	auto &aggregates = *bound.aggregates;
	auto &payload_lanes = *bound.payload_lanes;
	auto &payload_scratch = *bound.payload_scratch;
	const auto trace_runtime = runtime.TraceRuntime();
	if (op.aggregate_update.fused_payload_update_function) {
		auto payload_stage_start =
		    trace_runtime ? SljitRegionStageStart(runtime) : std::chrono::steady_clock::time_point();
		if (!SljitExecuteBoundSingleFusedPrimitiveAggregatePayloadUpdate(bound.single_fused_payload_update, input,
		                                                                 execute_sel, count, payload_scratch)) {
			SljitExecuteFusedPrimitiveAggregatePayloadUpdate(
			    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
			    payload_lanes, input, execute_sel, count, payload_scratch);
		}
		if (trace_runtime) {
			RecordSljitRegionRuntimePath(runtime, op.kind, "fused_payload_update");
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", count);
			RecordSljitRegionStageRuntime(runtime, bound.op_idx, op.kind, "primitive_payload_update_fused",
			                              payload_stage_start);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto lane = payload_lanes[payload_idx];
		if (!lane) {
			throw InternalException("SLJIT ungrouped aggregate primitive lane missing for aggregate %llu",
			                        static_cast<unsigned long long>(aggregate.aggregate_index));
		}
		auto payload_stage_start =
		    trace_runtime ? SljitRegionStageStart(runtime) : std::chrono::steady_clock::time_point();
		SljitExecutePrimitiveAggregatePayloadUpdate(
		    op.aggregate_update.payloads[payload_idx], op.aggregate_update.payload_update_functions[payload_idx], *lane,
		    input, execute_sel, count, scratch.ExpressionAdapterScratch(bound.op_idx, payload_idx));
		if (trace_runtime) {
			RecordSljitRegionStageRuntime(runtime, bound.op_idx, op.kind, "primitive_payload_update",
			                              payload_stage_start);
		}
	}
	if (trace_runtime) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "primitive_payload_update", aggregates.size());
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", count);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

struct SljitUngroupedAggregateUpdateRuntimeState {
	bool Prepare(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitUngroupedAggregateUpdatePrimitive &primitive) {
		(void)runtime;
		(void)scratch;
		return SljitCanBindUngroupedAggregateUpdatePrimitive(ops, primitive);
	}

	bool Execute(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, vector<SljitExecutableRegionOp> &ops,
	             SljitRegionExecutionScratch &scratch, const SljitUngroupedAggregateUpdatePrimitive &primitive,
	             const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		if (input.count == 0) {
			return false;
		}
		if (primitive.strategy == SljitUngroupedAggregateUpdateStrategyKind::FILTERED_PRIMITIVE_PAYLOAD_UPDATE) {
			return ExecuteFilteredPrimitivePayloadUpdate(runtime, result, ops, scratch, primitive, input,
			                                             processed_batches);
		}
		auto &input_chunk = SljitBindRuntimeBatchInput(input, "SLJIT ungrouped aggregate update");
		auto &aggregate_op = ops[primitive.aggregate_idx];
		SljitBindUngroupedPrimitiveAggregateUpdate(runtime.ExecutionOperators(), scratch, primitive.aggregate_idx,
		                                           aggregate_op, input_chunk, bound_update);
		auto sink_result = SljitExecuteBoundUngroupedPrimitiveAggregateUpdate(
		    runtime, scratch, bound_update, input_chunk, input.selection, input.count);
		sink_result = runtime.ExecutionOperators().RecordSinkResult(input.count, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	}

	bool Flush(ExecutionRegionRuntime &runtime, vector<SljitExecutableRegionOp> &ops,
	           SljitRegionExecutionScratch &scratch, const SljitUngroupedAggregateUpdatePrimitive &primitive) {
		(void)runtime;
		(void)ops;
		(void)scratch;
		(void)primitive;
		return false;
	}

private:
	bool ExecuteFilteredPrimitivePayloadUpdate(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result,
	                                           vector<SljitExecutableRegionOp> &ops,
	                                           SljitRegionExecutionScratch &scratch,
	                                           const SljitUngroupedAggregateUpdatePrimitive &primitive,
	                                           const SljitRuntimeBatchView &input, idx_t &processed_batches) {
		auto &input_chunk = SljitBindMaterializedRuntimeBatchInput(input, "SLJIT filtered ungrouped aggregate update");
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto sink_result = SljitExecuteNativeFilteredAggregateUpdate(
		    runtime, runtime.ExecutionOperators(), scratch, primitive.aggregate_idx, aggregate_op, input_chunk);
		sink_result = runtime.ExecutionOperators().RecordSinkResult(input_chunk, sink_result);
		if (SljitNativeSinkResultStopsExecution(runtime, sink_result, result)) {
			return true;
		}
		processed_batches++;
		return false;
	}

private:
	SljitBoundUngroupedPrimitiveAggregateUpdate bound_update;
};

} // namespace duckdb
