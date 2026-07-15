//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_ungrouped_aggregate_payload_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static SinkResultType SljitExecuteNativeUngroupedAggregateUpdateWithPayloads(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input,
    vector<SljitExecutableRegionExpression> &payloads, SljitAggregatePayloadSourceLayout payload_source_layout,
    const vector<idx_t> &combined_payload_source_indices, const vector<bool> &combined_payload_source_not_null,
    const char *stage_name, const char *runtime_path) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.UsesPrimitivePayloads() || op.aggregate_update.plan.use_grouped_state_addresses) {
		throw InternalException("SLJIT ungrouped aggregate direct update received a non-ungrouped aggregate");
	}
	auto &binding =
	    SljitBindRecordedNativeSink(runtime, native_runtime, scratch, op_idx, op.kind, payload_input, sink_info,
	                                "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready) {
		throw InternalException("SLJIT ungrouped aggregate direct update sink binding is incomplete");
	}
	auto &aggregates = sink_info.aggregates;
	if (aggregates.size() != payloads.size()) {
		throw InternalException("SLJIT ungrouped aggregate direct update payload count mismatch");
	}
	if (!op.aggregate_update.fused_payload_update.Function() &&
	    aggregates.size() != op.aggregate_update.payload_updates.size()) {
		throw InternalException("SLJIT ungrouped aggregate direct update payload function count mismatch");
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, op.aggregate_update.payload_descriptors,
	                                                    binding.aggregate_update.primitive);
	if (payload_lanes.size() != aggregates.size()) {
		throw InternalException("SLJIT ungrouped aggregate direct update primitive lane count mismatch");
	}
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	if (op.aggregate_update.fused_payload_update.Function()) {
		auto payload_stage_start = SljitRegionStageStart(runtime);
		SljitExecuteFusedPrimitiveAggregatePayloadUpdate(
		    payloads, op.aggregate_update.fused_payload_update.Function(), op.aggregate_update.payload_descriptors,
		    payload_source_layout, combined_payload_source_indices, combined_payload_source_not_null, payload_lanes,
		    payload_input, nullptr, payload_input.size(), payload_scratch);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_name, payload_stage_start);
	} else {
		for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			auto lane = payload_lanes[payload_idx];
			if (!lane) {
				throw InternalException("SLJIT ungrouped aggregate direct update primitive lane is missing");
			}
			auto payload_stage_start = SljitRegionStageStart(runtime);
			SljitExecutePrimitiveAggregatePayloadUpdate(
			    payloads[payload_idx], op.aggregate_update.payload_updates[payload_idx].Function(), *lane,
			    op.aggregate_update.payload_descriptors[payload_idx], payload_input, nullptr, payload_input.size(),
			    scratch.ExpressionAdapterScratch(op_idx, payload_idx));
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_name, payload_stage_start);
		}
	}
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, runtime_path, payload_input.size());
	return native_runtime.RecordSinkResult(payload_input, SinkResultType::NEED_MORE_INPUT);
}

} // namespace duckdb
