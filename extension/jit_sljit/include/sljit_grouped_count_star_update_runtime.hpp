//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_count_star_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_state.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

struct SljitCountStarGroupedAggregateUpdateDescriptor {
	optional_ptr<ExecutionGroupedAggregateStateAddressBinding> grouped_state;
	optional_ptr<const ExecutionPrimitiveAggregateUpdateLane> lane;

	bool Ready() const {
		return grouped_state && lane;
	}
};

static bool TryBuildCountStarGroupedAggregateUpdateDescriptor(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &bind_groups,
    ExecutionSinkBinding &binding, SljitCountStarGroupedAggregateUpdateDescriptor &descriptor) {
	descriptor = SljitCountStarGroupedAggregateUpdateDescriptor();
	if (op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.use_primitive_payloads || !op.aggregate_update.plan.use_grouped_state_addresses) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != bind_groups.ColumnCount() || sink_info.aggregates.size() != 1 ||
	    op.aggregate_update.payloads.size() != 1) {
		return false;
	}
	auto &aggregate = sink_info.aggregates[0];
	if (aggregate.child_count != 0 || aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return false;
	}
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready ||
	    !binding.aggregate_update.grouped_state.ready || !binding.aggregate_update.grouped_state.state) {
		return false;
	}
	auto &payload_lanes =
	    scratch.AggregatePayloadLanes(op_idx, sink_info.aggregates, binding.aggregate_update.primitive);
	if (payload_lanes.size() != 1 || !payload_lanes[0]) {
		return false;
	}
	auto lane = payload_lanes[0];
	if (!lane->ready || lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR ||
	    lane->aggregate_index != aggregate.aggregate_index ||
	    aggregate.aggregate_index >= sink_info.aggregate_contract.grouped_state_offsets.size() ||
	    lane->state_offset != sink_info.aggregate_contract.grouped_state_offsets[aggregate.aggregate_index] ||
	    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
	    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
		return false;
	}

	descriptor.grouped_state = &binding.aggregate_update.grouped_state;
	descriptor.lane = lane;
	return true;
}

static bool SljitTryPrepareCountStarGroupedAggregateUpdate(ExecutionRegionRuntime &runtime,
                                                           ExecutionOperatorRuntime &native_runtime,
                                                           SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                           SljitExecutableRegionOp &op, DataChunk &bind_groups,
                                                           SljitCountStarGroupedAggregateUpdateDescriptor &descriptor) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	auto &binding =
	    SljitBindRecordedNativeSink(runtime, native_runtime, scratch, op_idx, op.kind, bind_groups, sink_info,
	                                "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	return TryBuildCountStarGroupedAggregateUpdateDescriptor(scratch, op_idx, op, bind_groups, binding, descriptor);
}

static bool TryExecutePreaggregatedCountStarAddressAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &compact_groups, const ExecutionPrimitiveAggregateUpdateLane &lane,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, const vector<int64_t> &count_deltas,
    idx_t preaggregated_row_count, bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (compact_groups.size() == 0 || count_deltas.size() < compact_groups.size() || !lane.ready ||
	    lane.kind != AggregatePrimitiveUpdateKind::COUNT_STAR || lane.state_size == 0 || !grouped_state.ready ||
	    !grouped_state.state) {
		return false;
	}

	SljitPreaggregatedCountStarUpdateState update_state;
	update_state.lane = &lane;
	update_state.counts = count_deltas.data();
	auto &addresses = scratch.AggregateStateAddresses(op_idx);
	auto stage_start = SljitRegionStageStart(runtime);
	ExecuteSljitRegionRecordedOperation(runtime, op_idx, op.kind, "preaggregated_count_star_address_update",
	                                    stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		                                    grouped_state.state->ResolveStateAddresses(compact_groups, addresses,
		                                                                               recorder);
	                                    });
	addresses.Flatten();
	ExecuteSljitPreaggregatedCountStarUpdate(FlatVector::GetData<uintptr_t>(addresses), nullptr, nullptr,
	                                         compact_groups.size(), &update_state);
	if (!defer_grouped_finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "preaggregated_count_star_address_update", stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_count_star_groups",
	                                         compact_groups.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_address_vector_resolve",
	                                         compact_groups.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
	MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	return true;
}

static bool TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &compact_groups, const vector<int64_t> &count_deltas,
    SljitCountStarGroupedAggregateUpdateDescriptor &descriptor, idx_t preaggregated_row_count,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (compact_groups.size() == 0 || count_deltas.size() < compact_groups.size() || !descriptor.Ready()) {
		return false;
	}
	auto &grouped_state = *descriptor.grouped_state;
	auto lane = descriptor.lane.get();

	SljitPreaggregatedCountStarUpdateState update_state;
	update_state.lane = lane;
	update_state.counts = count_deltas.data();
	const bool finish = !defer_grouped_finish;
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_preaggregated_count_star_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
		        compact_groups, op.aggregate_update.plan.sink_info, ExecuteSljitPreaggregatedCountStarUpdate,
		        &update_state, recorder, finish);
	    });
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  updated ? "direct_preaggregated_count_star_update"
	                                          : "direct_preaggregated_count_star_update_miss",
	                                  stage_start);
	if (!updated) {
		return TryExecutePreaggregatedCountStarAddressAggregateUpdate(
		    runtime, scratch, op_idx, op, compact_groups, *lane, grouped_state, count_deltas, preaggregated_row_count,
		    defer_grouped_finish, deferred_grouped_finish);
	}
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_count_star_groups",
	                                         compact_groups.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
	MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	return true;
}

static bool SljitTryExecutePreaggregatedCountStarGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &compact_groups, const vector<int64_t> &count_deltas,
    idx_t preaggregated_row_count, bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (compact_groups.size() == 0 || count_deltas.size() < compact_groups.size()) {
		return false;
	}
	SljitCountStarGroupedAggregateUpdateDescriptor descriptor;
	if (!SljitTryPrepareCountStarGroupedAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, compact_groups,
	                                                    descriptor)) {
		return false;
	}
	return TryExecutePreparedPreaggregatedCountStarGroupedAggregateUpdate(
	    runtime, scratch, op_idx, op, compact_groups, count_deltas, descriptor, preaggregated_row_count,
	    defer_grouped_finish, deferred_grouped_finish);
}

} // namespace duckdb
