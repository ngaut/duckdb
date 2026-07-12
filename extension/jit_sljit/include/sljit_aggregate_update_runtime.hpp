//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_primitive_aggregate_update_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_ungrouped_aggregate_update_primitive.hpp"

namespace duckdb {

static SinkResultType
SljitExecutePrimitiveAggregateUpdate(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
                                     SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
                                     DataChunk &input, const SelectionVector *execute_sel = nullptr,
                                     idx_t count = DConstants::INVALID_INDEX, bool defer_grouped_finish = false,
                                     optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (SljitGroupedPrimitiveAggregateSinkKind(sink_info.kind)) {
		auto &bound = scratch.AggregateBoundGroupedUpdate(op_idx);
		SljitBindRecordedGroupedPrimitiveAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, bound);
		return SljitExecuteBoundGroupedPrimitiveAggregateUpdate(runtime, scratch, bound, input, execute_sel, count,
		                                                        defer_grouped_finish, deferred_grouped_finish);
	}
	SljitBoundUngroupedPrimitiveAggregateUpdate bound;
	SljitBindRecordedUngroupedPrimitiveAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, bound);
	return SljitExecuteBoundUngroupedPrimitiveAggregateUpdate(runtime, scratch, bound, input, execute_sel, count);
}

static SinkResultType SljitExecuteDuckDBAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                        ExecutionOperatorRuntime &native_runtime,
                                                        SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                        SljitExecutableRegionOp &op, DataChunk &input,
                                                        const SelectionVector *execute_sel = nullptr,
                                                        idx_t count = DConstants::INVALID_INDEX) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
	}
	if (execute_sel != nullptr || count != input.size()) {
		throw InternalException("SLJIT native aggregate sink update requires a materialized input chunk");
	}
	if (!binding.aggregate_update.state) {
		throw InternalException("SLJIT aggregate update sink binding is missing native sink state");
	}
	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	auto sink_result = binding.aggregate_update.state->Sink(input);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "native_sink_update", aggregate_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "native_sink_update", input.size());
	RecordSljitRegionRuntimeDelegation(runtime, op.kind, "native_sink_update", input.size());
	return sink_result;
}

static SinkResultType SljitExecuteNativePipelineAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input, const SelectionVector *execute_sel = nullptr,
    idx_t count = DConstants::INVALID_INDEX, bool defer_grouped_finish = false,
    optional_ptr<bool> deferred_grouped_finish = nullptr) {
	if (count == DConstants::INVALID_INDEX) {
		count = input.size();
	}
	if (op.aggregate_update.plan.use_primitive_payloads) {
		return SljitExecutePrimitiveAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, execute_sel,
		                                            count, defer_grouped_finish, deferred_grouped_finish);
	}
	return SljitExecuteDuckDBAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, input, execute_sel, count);
}

} // namespace duckdb
