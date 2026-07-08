//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_distinct_key_aggregate_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static SinkResultType SljitExecuteDistinctKeyAggregateUpdate(ExecutionRegionRuntime &runtime,
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
	    "aggregate-update-runtime-binding-failed", "SLJIT distinct aggregate key sink");
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.state) {
		throw InternalException("SLJIT distinct aggregate key sink did not bind a ready aggregate state");
	}
	if (!binding.aggregate_update.state->SupportsDistinctSink()) {
		throw InternalException("SLJIT distinct aggregate key sink binding does not support distinct ingestion");
	}
	if (execute_sel != nullptr || count != input.size()) {
		if (execute_sel == nullptr || !binding.aggregate_update.state->SupportsDistinctSelectedSink()) {
			throw InternalException("SLJIT distinct aggregate key sink requires a materialized input chunk");
		}
		auto aggregate_stage_start = SljitRegionStageStart(runtime);
		auto sink_result = binding.aggregate_update.state->SinkDistinctSelected(input, *execute_sel, count);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "distinct_key_selected_sink", aggregate_stage_start);
		RecordSljitRegionRuntimePath(runtime, op.kind, "distinct_key_selected_sink", count);
		RecordSljitRegionRuntimeDelegation(runtime, op.kind, "distinct_key_selected_sink", count);
		return sink_result;
	}
	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	auto sink_result = binding.aggregate_update.state->SinkDistinct(input);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "distinct_key_sink", aggregate_stage_start);
	RecordSljitRegionRuntimePath(runtime, op.kind, "distinct_key_sink", input.size());
	RecordSljitRegionRuntimeDelegation(runtime, op.kind, "distinct_key_sink", input.size());
	return sink_result;
}

} // namespace duckdb
