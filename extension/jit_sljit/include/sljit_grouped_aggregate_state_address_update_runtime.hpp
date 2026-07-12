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
	const ExecutionRegionAggregateContract *contract = nullptr;
	const vector<SljitAggregatePayloadDescriptor> *payload_descriptors = nullptr;
	const vector<const ExecutionPrimitiveAggregateUpdateLane *> *lanes = nullptr;
	const vector<SljitGroupedReductionLaneBinding> *reduction_lanes = nullptr;
	DataChunk *input = nullptr;
	SljitAggregatePayloadAdapterScratch *adapter_scratch = nullptr;
	optional_ptr<const vector<idx_t>> input_source_indices_override;
	optional_ptr<const vector<bool>> input_source_not_null_override;
};

static SljitGroupedStateAddressUpdateState
SljitBuildGroupedStateAddressUpdateState(SljitExecutableRegionOp &op, DataChunk &payload_input,
                                         const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                         const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
                                         SljitAggregatePayloadAdapterScratch &payload_scratch,
                                         optional_ptr<const vector<idx_t>> input_source_indices_override = nullptr,
                                         optional_ptr<const vector<bool>> input_source_not_null_override = nullptr) {
	auto &aggregate_update = op.aggregate_update;
	SljitGroupedStateAddressUpdateState update_state;
	update_state.payloads = &aggregate_update.payloads;
	update_state.function = aggregate_update.fused_payload_update.Function();
	update_state.contract = &aggregate_update.plan.sink_info.aggregate_contract;
	update_state.payload_descriptors = &aggregate_update.payload_descriptors;
	update_state.lanes = &payload_lanes;
	update_state.reduction_lanes = &reduction_lanes;
	update_state.input = &payload_input;
	update_state.adapter_scratch = &payload_scratch;
	update_state.input_source_indices_override = input_source_indices_override;
	update_state.input_source_not_null_override = input_source_not_null_override;
	return update_state;
}

static void SljitExecuteGroupedSelectedStateAddressUpdate(const uintptr_t *addresses, const sel_t *address_sel,
                                                          const sel_t *execute_sel, idx_t count, void *state_p) {
	auto &state = *reinterpret_cast<SljitGroupedStateAddressUpdateState *>(state_p);
	if (!state.payloads || !state.function || !state.contract || !state.payload_descriptors || !state.lanes ||
	    !state.reduction_lanes || !state.input || !state.adapter_scratch) {
		throw InternalException("SLJIT grouped selected state-address callback is incomplete");
	}
	SelectionVector execute_selection(const_cast<sel_t *>(execute_sel), execute_sel ? count : 0);
	const bool state_addresses_by_loop_index = execute_sel && !address_sel;
	SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
	    *state.payloads, state.function, *state.contract, *state.payload_descriptors, *state.lanes,
	    *state.reduction_lanes, *state.input, addresses, address_sel, execute_sel ? &execute_selection : nullptr,
	    state_addresses_by_loop_index, count, *state.adapter_scratch, state.input_source_indices_override,
	    state.input_source_not_null_override);
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

static idx_t SljitDenseGroupDomainReserveCount(const ExecutionDenseGroupDomain &domain) {
	if (!domain.ready || domain.distinct_count == 0 || domain.max_key < domain.min_key ||
	    domain.max_key - domain.min_key == NumericLimits<idx_t>::Maximum()) {
		return 0;
	}
	const auto domain_range = domain.max_key - domain.min_key + 1;
	if (domain_range > EXECUTION_DENSE_GROUP_DOMAIN_MAX_TARGET_ENTRIES) {
		return domain.distinct_count;
	}
	static constexpr idx_t DENSE_DOMAIN_RESERVE_DISTINCT_MULTIPLIER = 4;
	const auto max_distinct_reserve =
	    domain.distinct_count > NumericLimits<idx_t>::Maximum() / DENSE_DOMAIN_RESERVE_DISTINCT_MULTIPLIER
	        ? NumericLimits<idx_t>::Maximum()
	        : domain.distinct_count * DENSE_DOMAIN_RESERVE_DISTINCT_MULTIPLIER;
	return MinValue(domain_range, MaxValue(domain.distinct_count, max_distinct_reserve));
}

static bool SljitTryReserveGroupedAggregateGroups(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                                  SljitExecutableRegionOp &op,
                                                  ExecutionGroupedAggregateStateAddressBinding &grouped_state,
                                                  idx_t runtime_group_count = 0) {
	auto &reserve = op.aggregate_update.plan.group_reserve;
	if ((!reserve.CanReserve() && runtime_group_count == 0) || !grouped_state.ready || !grouped_state.state) {
		return false;
	}
	if (!runtime.TryMarkOnce(ExecutionRegionRuntimeOnceFlag::AGGREGATE_GROUP_RESERVE, op_idx)) {
		return false;
	}
	auto reserve_group_count = MaxValue<idx_t>(reserve.group_count, runtime_group_count);
	if (op.aggregate_update.dense_group_domain.ready) {
		reserve_group_count =
		    MaxValue(reserve_group_count, SljitDenseGroupDomainReserveCount(op.aggregate_update.dense_group_domain));
	}
	const auto parallelism = MaxValue<idx_t>(runtime.MaxThreads(), 1);
	reserve_group_count = reserve_group_count / parallelism + (reserve_group_count % parallelism != 0 ? 1 : 0);
	reserve_group_count = MaxValue<idx_t>(reserve_group_count, STANDARD_VECTOR_SIZE);
	RecordSljitRegionRuntimePath(runtime, op.kind, "grouped_aggregate_reserve_target", reserve_group_count);
	auto reserve_stage_start = SljitRegionStageStart(runtime);
	auto reserved = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "grouped_aggregate_reserve", reserve_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->ReserveGroups(reserve_group_count, recorder);
	    });
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  reserved ? "grouped_aggregate_reserve" : "grouped_aggregate_reserve_miss",
	                                  reserve_stage_start);
	return reserved;
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
	}
	return resolved;
}

static bool TryExecuteDirectGroupedStateAddressPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish = true, optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state);
	auto stage_start = SljitRegionStageStart(runtime);
	auto &reduction_lanes = scratch.GroupedReductionLanes(op_idx, op.aggregate_update.plan.sink_info.aggregate_contract,
	                                                      op.aggregate_update.payload_descriptors, payload_lanes);
	auto update_state =
	    SljitBuildGroupedStateAddressUpdateState(op, input, payload_lanes, reduction_lanes, payload_scratch);
	const char *stage_name = "direct_new_grouped_primitive_payload_update";
	const char *miss_stage_name = "direct_new_grouped_primitive_payload_update_miss";
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
		        input, op.aggregate_update.plan.sink_info, SljitExecuteGroupedSelectedStateAddressUpdate, &update_state,
		        recorder, finish, nullptr, dense_domain);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, updated ? stage_name : miss_stage_name, stage_start);
	if (updated) {
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, stage_name, input.size());
	}
	return updated;
}

static bool TryExecuteDirectProjectedGroupedStateAddressPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &groups, DataChunk &payload_input, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish = true, optional_ptr<Vector> precomputed_hashes = nullptr,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	if (groups.size() != payload_input.size()) {
		return false;
	}
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state);
	auto stage_start = SljitRegionStageStart(runtime);
	auto &reduction_lanes = scratch.GroupedReductionLanes(op_idx, op.aggregate_update.plan.sink_info.aggregate_contract,
	                                                      op.aggregate_update.payload_descriptors, payload_lanes);
	auto update_state = SljitBuildGroupedStateAddressUpdateState(op, payload_input, payload_lanes, reduction_lanes,
	                                                             payload_scratch, &payload_source_indices);
	const char *stage_name = "direct_projected_group_payload_update";
	const char *miss_stage_name = "direct_projected_group_payload_update_miss";
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateGroupKeysWithSelectedStateAddresses(
		        groups, op.aggregate_update.plan.sink_info, SljitExecuteGroupedSelectedStateAddressUpdate,
		        &update_state, recorder, finish, precomputed_hashes, dense_domain);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	if (updated) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_name, stage_start);
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, stage_name, payload_input.size());
	} else {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, miss_stage_name, stage_start);
	}
	return updated;
}

} // namespace duckdb
