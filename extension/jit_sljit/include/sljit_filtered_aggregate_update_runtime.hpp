//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_filtered_aggregate_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static SinkResultType SljitExecuteNativeFilteredAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                                ExecutionOperatorRuntime &native_runtime,
                                                                SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                                SljitExecutableRegionOp &op, DataChunk &input) {
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready) {
		throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
	}
	auto &primitive = binding.aggregate_update.primitive;
	if (!primitive.ready) {
		auto blocker = primitive.blocker.empty() ? "aggregate-primitive-binding-missing" : primitive.blocker;
		throw InternalException("SLJIT aggregate primitive update binding failed: %s", blocker.c_str());
	}
	auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
	if (aggregates.empty() || aggregates.size() != op.aggregate_update.payloads.size()) {
		throw InternalException("SLJIT filtered aggregate update requires matching primitive aggregate lanes");
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, primitive);
	if (payload_lanes.size() != aggregates.size()) {
		throw InternalException("SLJIT filtered aggregate primitive lane count mismatch");
	}

	auto aggregate_stage_start = SljitRegionStageStart(runtime);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	if (op.aggregate_update.filtered_update.owns_perfect_hash_group_lookup) {
		auto &grouped_state = binding.aggregate_update.grouped_state;
		SljitExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
		    op.aggregate_update.filtered_update.payloads, op.aggregate_update.filtered_update.function, aggregates,
		    op.aggregate_update.plan.sink_info.groups, op.aggregate_update.plan.group_expressions,
		    op.aggregate_update.plan.sink_info.aggregate_contract, payload_lanes, grouped_state.perfect_hash_layout,
		    input, nullptr, input.size(), payload_scratch);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "filtered_perfect_hash_update",
		                                  aggregate_stage_start);
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, "filtered_perfect_hash_update", input.size());
	} else {
		SljitExecuteFilteredPrimitiveAggregateUpdate(op.aggregate_update.filtered_update, aggregates, payload_lanes,
		                                             input, input.size(), payload_scratch);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "filtered_primitive_update", aggregate_stage_start);
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, "filtered_primitive_update", input.size());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
