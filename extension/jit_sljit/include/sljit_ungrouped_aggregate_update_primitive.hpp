//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_ungrouped_aggregate_update_primitive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_runtime.hpp"
#include "sljit_grouped_aggregate_update_runtime.hpp"
#include "sljit_runtime_batch_view.hpp"

namespace duckdb {

struct SljitUngroupedAggregateUpdatePrimitive {
	idx_t aggregate_idx = DConstants::INVALID_INDEX;
};

static bool SljitCanBindUngroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops,
                                                          idx_t aggregate_idx) {
	return aggregate_idx < ops.size() && ops[aggregate_idx].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	       ops[aggregate_idx].aggregate_update.plan.sink_info.kind ==
	           ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE &&
	       ops[aggregate_idx].aggregate_update.plan.use_primitive_payloads;
}

static SljitUngroupedAggregateUpdatePrimitive
SljitBindUngroupedAggregateUpdatePrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t aggregate_idx) {
	if (!SljitCanBindUngroupedAggregateUpdatePrimitive(ops, aggregate_idx)) {
		throw InternalException("SLJIT ungrouped aggregate update primitive cannot bind requested operator");
	}
	SljitUngroupedAggregateUpdatePrimitive primitive;
	primitive.aggregate_idx = aggregate_idx;
	return primitive;
}

static bool SljitCanBindUngroupedAggregateUpdatePrimitive(
    const vector<SljitExecutableRegionOp> &ops, const SljitUngroupedAggregateUpdatePrimitive &primitive) {
	return SljitCanBindUngroupedAggregateUpdatePrimitive(ops, primitive.aggregate_idx);
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
		auto &input_chunk = SljitBindRuntimeBatchInput(input, "SLJIT ungrouped aggregate update");
		auto &aggregate_op = ops[primitive.aggregate_idx];
		auto sink_result =
		    SljitExecutePrimitiveAggregateUpdate(runtime, runtime.ExecutionOperators(), scratch, primitive.aggregate_idx,
		                                         aggregate_op, input_chunk, input.selection, input.count);
		sink_result = runtime.ExecutionOperators().RecordSinkResult(input_chunk, sink_result);
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
};

} // namespace duckdb
