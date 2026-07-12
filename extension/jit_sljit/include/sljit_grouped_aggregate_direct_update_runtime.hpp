//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_direct_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool TryExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_append_new_grouped_primitive_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryAppendNewGroups(input, op.aggregate_update.plan.sink_info, payload_lanes,
		                                                   recorder, finish, dense_domain);
	    });
	scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  updated ? "direct_append_new_grouped_primitive_update"
	                                          : "direct_append_new_grouped_primitive_update_miss",
	                                  stage_start);
	if (updated) {
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, "direct_append_new_grouped_primitive_update",
		                                             input.size());
	}
	return updated;
}

static bool TryExecuteDirectNewGroupedPrimitiveAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_new_grouped_primitive_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateNewGroups(input, op.aggregate_update.plan.sink_info, payload_lanes,
		                                                   recorder, finish, dense_domain);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(
	    runtime, op_idx, op.kind,
	    updated ? "direct_new_grouped_primitive_update" : "direct_new_grouped_primitive_update_miss", stage_start);
	if (updated) {
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, "direct_new_grouped_primitive_update",
		                                             input.size());
	}
	return updated;
}

static bool TryExecuteDirectNewGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool defer_grouped_finish,
    optional_ptr<bool> deferred_grouped_finish) {
	optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
	if (op.aggregate_update.dense_group_domain.ready) {
		dense_domain = &op.aggregate_update.dense_group_domain;
	}
	if (SljitCanExecuteDirectNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, reduction_lanes,
	                                                            execute_sel, count) &&
	    TryExecuteDirectNewGroupedPrimitiveAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
	                                                       grouped_state, !defer_grouped_finish, dense_domain)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	return false;
}

static bool TryExecuteDirectGroupedStateAddressPayloadUpdatePath(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
	if (op.aggregate_update.dense_group_domain.ready) {
		dense_domain = &op.aggregate_update.dense_group_domain;
	}
	if (SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(scratch, op_idx, op, input, reduction_lanes, execute_sel,
	                                                          count) &&
	    TryExecuteDirectGroupedStateAddressPayloadUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
	                                                     grouped_state, payload_scratch, !defer_grouped_finish,
	                                                     dense_domain)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	return false;
}

static bool TryExecuteAdaptiveDirectGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (SljitCanExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, reduction_lanes,
	                                                                  execute_sel, count)) {
		optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
		if (op.aggregate_update.dense_group_domain.ready) {
			dense_domain = &op.aggregate_update.dense_group_domain;
		}
		if (TryExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
		                                                             grouped_state, !defer_grouped_finish,
		                                                             dense_domain)) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
	}
	if (TryExecuteDirectNewGroupedAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes, reduction_lanes,
	                                              execute_sel, count, grouped_state, defer_grouped_finish,
	                                              deferred_grouped_finish)) {
		return true;
	}
	if (op.aggregate_update.fused_payload_update.Function() &&
	    TryExecuteDirectGroupedStateAddressPayloadUpdatePath(
	        runtime, scratch, op_idx, op, input, payload_lanes, reduction_lanes, execute_sel, count, grouped_state,
	        payload_scratch, defer_grouped_finish, deferred_grouped_finish)) {
		return true;
	}
	return false;
}

static bool TryExecuteDirectGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	const auto direct_update_kind = op.aggregate_update.grouped_direct_update.kind;
	if (direct_update_kind != SljitGroupedAggregateDirectUpdatePlanKind::NONE &&
	    TryExecutePreaggregatedDirectGroupedAggregateUpdate(
	        runtime, scratch, op_idx, op, input, payload_lanes, reduction_lanes, execute_sel, count, grouped_state,
	        payload_scratch, defer_grouped_finish, deferred_grouped_finish)) {
		return true;
	}
	switch (direct_update_kind) {
	case SljitGroupedAggregateDirectUpdatePlanKind::NONE:
		return false;
	case SljitGroupedAggregateDirectUpdatePlanKind::ADAPTIVE_GROUPED_STATE_ADDRESS:
		return TryExecuteAdaptiveDirectGroupedAggregateUpdate(
		    runtime, scratch, op_idx, op, input, payload_lanes, reduction_lanes, execute_sel, count, grouped_state,
		    payload_scratch, defer_grouped_finish, deferred_grouped_finish);
	case SljitGroupedAggregateDirectUpdatePlanKind::DIRECT_STATE_ADDRESS_PAYLOAD_ONLY:
		return TryExecuteDirectGroupedStateAddressPayloadUpdatePath(
		    runtime, scratch, op_idx, op, input, payload_lanes, reduction_lanes, execute_sel, count, grouped_state,
		    payload_scratch, defer_grouped_finish, deferred_grouped_finish);
	default:
		throw InternalException("Unknown SLJIT direct grouped aggregate update plan");
	}
}

} // namespace duckdb
