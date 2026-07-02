//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_state_address_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

struct SljitGroupedStateAddressUpdateState {
	vector<SljitExecutableRegionExpression> *payloads = nullptr;
	SljitNativeAggregateUpdateFunction function = nullptr;
	const vector<ExecutionRegionAggregateInput> *aggregates = nullptr;
	const ExecutionRegionAggregateContract *contract = nullptr;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> *lanes = nullptr;
	DataChunk *input = nullptr;
	SljitAggregatePayloadAdapterScratch *adapter_scratch = nullptr;
	optional_ptr<const vector<idx_t>> input_source_indices_override;
};

static SljitGroupedStateAddressUpdateState
SljitBuildGroupedStateAddressUpdateState(SljitExecutableRegionOp &op, DataChunk &payload_input,
                                         const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                         SljitAggregatePayloadAdapterScratch &payload_scratch,
                                         optional_ptr<const vector<idx_t>> input_source_indices_override = nullptr) {
	auto &aggregate_update = op.aggregate_update;
	SljitGroupedStateAddressUpdateState update_state;
	update_state.payloads = &aggregate_update.payloads;
	update_state.function = aggregate_update.fused_payload_update_function;
	update_state.aggregates = &aggregate_update.plan.sink_info.aggregates;
	update_state.contract = &aggregate_update.plan.sink_info.aggregate_contract;
	update_state.lanes = &payload_lanes;
	update_state.input = &payload_input;
	update_state.adapter_scratch = &payload_scratch;
	update_state.input_source_indices_override = input_source_indices_override;
	return update_state;
}

static void SljitExecuteGroupedSelectedStateAddressUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                          const sel_t *execute_sel, idx_t count, void *state_p) {
	auto &state = *reinterpret_cast<SljitGroupedStateAddressUpdateState *>(state_p);
	if (!state.payloads || !state.function || !state.aggregates || !state.contract || !state.lanes || !state.input ||
	    !state.adapter_scratch) {
		throw InternalException("SLJIT grouped selected state-address callback is incomplete");
	}
	SelectionVector execute_selection(const_cast<sel_t *>(execute_sel), execute_sel ? count : 0);
	const bool state_addresses_by_loop_index = execute_sel && !address_sel;
	SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
	    *state.payloads, state.function, *state.aggregates, *state.contract, *state.lanes, *state.input, addresses,
	    address_sel, execute_sel ? &execute_selection : nullptr, state_addresses_by_loop_index, count,
	    *state.adapter_scratch, state.input_source_indices_override);
}

static void SljitExecuteGroupedStateTargetSpan(const ExecutionGroupedAggregateStateTargetSpan &span,
                                               SljitGroupedStateAddressUpdateState &state) {
	if (!span.HasTargets()) {
		return;
	}
	SljitExecuteGroupedSelectedStateAddressUpdate(span.addresses, span.address_sel, span.row_sel, span.count, &state);
}

static void SljitExecuteGroupedStateTargetBatch(const ExecutionGroupedAggregateStateTargetBatch &targets,
                                                SljitGroupedStateAddressUpdateState &state) {
	for (auto &span : targets.Spans()) {
		SljitExecuteGroupedStateTargetSpan(span, state);
	}
}

static bool TryResolveDirectNewGroupedStateAddresses(ExecutionRegionRuntime &runtime,
                                                     SljitRegionExecutionScratch &scratch, idx_t op_idx,
                                                     SljitExecutableRegionOp &op, DataChunk &input,
                                                     ExecutionGroupedAggregateStateAddressBinding &grouped_state,
                                                     Vector &addresses, bool finish = true) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto resolved = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_new_grouped_state_addresses", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryResolveNewGroups(input, op.aggregate_update.plan.sink_info, addresses,
		                                                    recorder, finish);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, resolved);
	RecordSljitRegionStageRuntimePath(
	    runtime, op_idx, op.kind,
	    resolved ? "direct_new_grouped_state_addresses" : "direct_new_grouped_state_addresses_miss", stage_start);
	if (resolved) {
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "address_vector_direct_new", input.size());
	}
	return resolved;
}

static bool
TryExecuteDirectGroupedFusedPayloadUpdate(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
                                          idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
                                          const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                          ExecutionGroupedAggregateStateAddressBinding &grouped_state,
                                          SljitAggregatePayloadAdapterScratch &payload_scratch, bool finish = true) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto update_state = SljitBuildGroupedStateAddressUpdateState(op, input, payload_lanes, payload_scratch);
	const char *stage_name = "direct_new_grouped_fused_payload_update";
	const char *miss_stage_name = "direct_new_grouped_fused_payload_update_miss";
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
		        input, op.aggregate_update.plan.sink_info, SljitExecuteGroupedSelectedStateAddressUpdate, &update_state,
		        recorder, finish);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, updated ? stage_name : miss_stage_name, stage_start);
	if (updated) {
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "state_address_selection_new_update", input.size());
	}
	return updated;
}

static bool TryExecuteDirectProjectedGroupedFusedPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &groups, DataChunk &payload_input, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish = true, optional_ptr<Vector> precomputed_hashes = nullptr) {
	if (groups.size() != payload_input.size()) {
		return false;
	}
	auto stage_start = SljitRegionStageStart(runtime);
	auto update_state = SljitBuildGroupedStateAddressUpdateState(op, payload_input, payload_lanes, payload_scratch,
	                                                             &payload_source_indices);
	const char *stage_name = "direct_projected_group_payload_update";
	const char *miss_stage_name = "direct_projected_group_payload_update_miss";
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
		        groups, op.aggregate_update.plan.sink_info, SljitExecuteGroupedSelectedStateAddressUpdate,
		        &update_state, recorder, finish, precomputed_hashes);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	if (updated) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_name, stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "projected_group_payload_update",
		                                         payload_input.size());
	} else {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, miss_stage_name, stage_start);
	}
	return updated;
}

} // namespace duckdb
