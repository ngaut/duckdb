//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_direct_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_aggregate_preaggregation.hpp"
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool TryExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_append_new_grouped_primitive_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryAppendNewGroups(input, op.aggregate_update.plan.sink_info, payload_lanes,
		                                                   recorder, finish);
	    });
	scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  updated ? "direct_append_new_grouped_primitive_update"
	                                          : "direct_append_new_grouped_primitive_update_miss",
	                                  stage_start);
	if (updated) {
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", input.size());
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
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", input.size());
	}
	return updated;
}

static bool SljitCanApplyPreaggregatedGroupedPrimitiveAggregateUpdate(
    SljitExecutableRegionOp &op, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state) {
	if (compact_groups.size() == 0 ||
	    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
	    !op.aggregate_update.plan.use_primitive_payloads || !op.aggregate_update.plan.use_grouped_state_addresses ||
	    !grouped_state.ready || !grouped_state.state) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != compact_groups.ColumnCount() ||
	    sink_info.aggregates.size() != op.aggregate_update.payloads.size() ||
	    sink_info.aggregates.size() != payload_lanes.size() ||
	    sink_info.aggregates.size() != preaggregate_scratch.payloads.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		auto &aggregate = sink_info.aggregates[payload_idx];
		auto lane = payload_lanes[payload_idx];
		if (!lane || !lane->ready || lane->aggregate_index != aggregate.aggregate_index ||
		    aggregate.aggregate_index >= sink_info.aggregate_contract.grouped_state_offsets.size() ||
		    lane->state_offset != sink_info.aggregate_contract.grouped_state_offsets[aggregate.aggregate_index] ||
		    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
		    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset ||
		    lane->kind != aggregate.primitive_update_kind ||
		    lane->kind != preaggregate_scratch.payloads[payload_idx].kind) {
			return false;
		}
	}
	return true;
}

static bool TryExecutePreaggregatedGroupedPrimitiveAppendSuffixWithPrefixUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool defer_grouped_finish,
    optional_ptr<bool> deferred_grouped_finish);

static bool TryExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t preaggregated_row_count,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (!SljitCanApplyPreaggregatedGroupedPrimitiveAggregateUpdate(op, compact_groups, preaggregate_scratch,
	                                                               payload_lanes, grouped_state)) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;

	SljitPreaggregatedPrimitiveUpdateState update_state;
	update_state.lanes = &payload_lanes;
	update_state.payloads = &preaggregate_scratch.payloads;
	optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
	if (op.aggregate_update.dense_group_domain.ready) {
		dense_domain = &op.aggregate_update.dense_group_domain;
	}
	const bool finish = !defer_grouped_finish;
	if (!scratch.DirectAppendNewAggregateUpdateDisabled(op_idx)) {
		auto append_stage_start = SljitRegionStageStart(runtime);
		auto appended = ExecuteSljitRegionRecordedOperation(
		    runtime, op_idx, op.kind, "direct_append_preaggregated_grouped_primitive_update", append_stage_start,
		    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    return grouped_state.state->TryAppendNewGroupsWithStateAddresses(
			        compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &update_state, recorder,
			        finish);
		    });
		if (appended) {
			scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, true);
			RecordSljitRegionStageRuntimePath(
			    runtime, op_idx, op.kind, "direct_append_preaggregated_grouped_primitive_update", append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups",
			                                         compact_groups.size());
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		if (TryExecutePreaggregatedGroupedPrimitiveAppendSuffixWithPrefixUpdate(
		        runtime, scratch, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state,
		        defer_grouped_finish, deferred_grouped_finish)) {
			return true;
		}
		scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(
		    runtime, op_idx, op.kind, "direct_append_preaggregated_grouped_primitive_update_miss", append_stage_start);
	}

	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_preaggregated_grouped_primitive_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateGroupKeysWithSelectedStateAddresses(
		        compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveUpdate, &update_state, recorder, finish,
		        nullptr, dense_domain);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  updated ? "direct_preaggregated_grouped_primitive_update"
	                                          : "direct_preaggregated_grouped_primitive_update_miss",
	                                  stage_start);
	if (updated) {
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups",
		                                         compact_groups.size());
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}

	auto &addresses = scratch.AggregateStateAddresses(op_idx);
	auto fallback_stage_start = SljitRegionStageStart(runtime);
	ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "preaggregated_grouped_primitive_address_update", fallback_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    grouped_state.state->ResolveStateAddresses(compact_groups, addresses, recorder);
	    });
	addresses.Flatten();
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(FlatVector::GetData<uintptr_t>(addresses), nullptr,
	                                                compact_groups.size(), &update_state);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "preaggregated_grouped_primitive_address_update",
	                                  fallback_stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups", compact_groups.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_address_vector_resolve",
	                                         compact_groups.size());
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
	MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	return true;
}

static bool TryExecutePreaggregatedGroupedPrimitiveAppendSuffixWithPrefixUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool defer_grouped_finish,
    optional_ptr<bool> deferred_grouped_finish) {
	if (compact_groups.size() < 2 || !SljitCanApplyPreaggregatedGroupedPrimitiveAggregateUpdate(
	                                     op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state)) {
		return false;
	}
	const idx_t prefix_count = 1;
	const auto suffix_offset = prefix_count;
	const auto suffix_count = compact_groups.size() - suffix_offset;
	idx_t prefix_row_count;
	idx_t suffix_row_count;
	if (!CanSlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, 0, prefix_count) ||
	    !CanSlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, suffix_offset, suffix_count) ||
	    !PreaggregatedPrimitiveRepresentedRowCount(preaggregate_scratch, 0, prefix_count, prefix_row_count) ||
	    !PreaggregatedPrimitiveRepresentedRowCount(preaggregate_scratch, suffix_offset, suffix_count,
	                                               suffix_row_count)) {
		return false;
	}

	auto &sink_info = op.aggregate_update.plan.sink_info;
	auto &group_slice = scratch.AggregatePreaggregatedGroupSlice(op_idx);
	auto &preaggregate_scratch_slice = scratch.AggregatePreaggregateScratchSlice(op_idx);

	group_slice.Reset();
	group_slice.Slice(compact_groups, suffix_offset, compact_groups.size());
	if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, suffix_offset, suffix_count,
	                                        preaggregate_scratch_slice)) {
		return false;
	}
	SljitPreaggregatedPrimitiveUpdateState suffix_update_state;
	suffix_update_state.lanes = &payload_lanes;
	suffix_update_state.payloads = &preaggregate_scratch_slice.payloads;
	auto suffix_stage_start = SljitRegionStageStart(runtime);
	auto suffix_appended = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_append_preaggregated_grouped_primitive_update_suffix", suffix_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryAppendNewGroupsWithStateAddresses(
		        group_slice, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &suffix_update_state, recorder,
		        false);
	    });
	if (!suffix_appended) {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "direct_append_preaggregated_grouped_primitive_update_suffix_miss",
		                                  suffix_stage_start);
		return false;
	}
	RecordSljitRegionStageRuntimePath(
	    runtime, op_idx, op.kind, "direct_append_preaggregated_grouped_primitive_update_suffix", suffix_stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups", suffix_count);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", suffix_row_count);

	group_slice.Reset();
	group_slice.Slice(compact_groups, 0, prefix_count);
	if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, 0, prefix_count,
	                                        preaggregate_scratch_slice)) {
		throw InternalException("Validated SLJIT preaggregated prefix scratch slice failed");
	}
	SljitPreaggregatedPrimitiveUpdateState prefix_update_state;
	prefix_update_state.lanes = &payload_lanes;
	prefix_update_state.payloads = &preaggregate_scratch_slice.payloads;
	auto &addresses = scratch.AggregateStateAddresses(op_idx);
	auto prefix_stage_start = SljitRegionStageStart(runtime);
	ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "preaggregated_grouped_primitive_prefix_address_update", prefix_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    grouped_state.state->ResolveStateAddresses(group_slice, addresses, recorder);
	    });
	addresses.Flatten();
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(FlatVector::GetData<uintptr_t>(addresses), nullptr, prefix_count,
	                                                &prefix_update_state);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "preaggregated_grouped_primitive_prefix_address_update",
	                                  prefix_stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups", prefix_count);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_address_vector_resolve", prefix_count);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", prefix_row_count);
	if (!defer_grouped_finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionRuntimePath(runtime, op.kind, "preaggregated_suffix_append_prefix_update");
	MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	return true;
}

static bool TryExecutePreaggregatedGroupedPrimitiveAggregateUpdateBatches(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t preaggregated_row_count,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (!SljitCanApplyPreaggregatedGroupedPrimitiveAggregateUpdate(op, compact_groups, preaggregate_scratch,
	                                                               payload_lanes, grouped_state)) {
		return false;
	}
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state);
	if (compact_groups.size() <= STANDARD_VECTOR_SIZE) {
		return TryExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
		    runtime, scratch, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state,
		    preaggregated_row_count, defer_grouped_finish, deferred_grouped_finish);
	}

	idx_t total_represented_row_count = 0;
	for (idx_t offset = 0; offset < compact_groups.size(); offset += STANDARD_VECTOR_SIZE) {
		const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, compact_groups.size() - offset);
		idx_t represented_row_count;
		if (!CanSlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, offset, count) ||
		    !PreaggregatedPrimitiveRepresentedRowCount(preaggregate_scratch, offset, count, represented_row_count)) {
			return false;
		}
		total_represented_row_count += represented_row_count;
	}
	if (total_represented_row_count != preaggregated_row_count) {
		return false;
	}

	auto &compact_group_slice = scratch.AggregatePreaggregatedGroupSlice(op_idx);
	auto &preaggregate_scratch_slice = scratch.AggregatePreaggregateScratchSlice(op_idx);
	bool local_deferred_grouped_finish = false;
	const bool inner_defer_grouped_finish = true;
	auto inner_deferred_grouped_finish =
	    defer_grouped_finish ? deferred_grouped_finish : optional_ptr<bool>(&local_deferred_grouped_finish);
	RecordSljitRegionRuntimePath(runtime, op.kind, "preaggregated_primitive_group_batches", compact_groups.size());
	for (idx_t offset = 0; offset < compact_groups.size(); offset += STANDARD_VECTOR_SIZE) {
		const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, compact_groups.size() - offset);
		const auto end = offset + count;
		idx_t represented_row_count;
		const auto counted =
		    PreaggregatedPrimitiveRepresentedRowCount(preaggregate_scratch, offset, count, represented_row_count);
		D_ASSERT(counted);
		compact_group_slice.Reset();
		compact_group_slice.Slice(compact_groups, offset, end);
		if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, offset, count,
		                                        preaggregate_scratch_slice)) {
			throw InternalException("Validated SLJIT preaggregated primitive scratch slice failed");
		}
		if (!TryExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
		        runtime, scratch, op_idx, op, compact_group_slice, preaggregate_scratch_slice, payload_lanes,
		        grouped_state, represented_row_count, inner_defer_grouped_finish, inner_deferred_grouped_finish)) {
			throw InternalException("Validated SLJIT preaggregated primitive aggregate slice update failed");
		}
	}
	if (!defer_grouped_finish && local_deferred_grouped_finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	return true;
}

static bool TryExecutePreaggregatedDirectGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const SelectionVector *execute_sel, idx_t count, ExecutionGroupedAggregateStateAddressBinding &grouped_state,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (!SljitCanExecutePreaggregatedGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes,
	                                                                 execute_sel, count)) {
		return false;
	}
	auto &compact_groups = scratch.AggregatePreaggregatedGroups(op_idx);
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
	auto preaggregate_stage_start = SljitRegionStageStart(runtime);
	if (TryPreaggregateConsecutivePrimitiveGroups(op, input, payload_lanes, compact_groups, preaggregate_scratch)) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "local_preaggregate_primitive_groups",
		                              preaggregate_stage_start);
		if (TryExecutePreaggregatedGroupedPrimitiveAggregateUpdateBatches(
		        runtime, scratch, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state,
		        input.size(), defer_grouped_finish, deferred_grouped_finish)) {
			return true;
		}
	}
	return false;
}

static bool TryExecuteDirectNewGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const SelectionVector *execute_sel, idx_t count, ExecutionGroupedAggregateStateAddressBinding &grouped_state,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
	if (op.aggregate_update.dense_group_domain.ready) {
		dense_domain = &op.aggregate_update.dense_group_domain;
		RecordSljitRegionRuntimePath(runtime, op.kind, "direct_grouped_dense_group_domain", count);
	}
	if (SljitCanExecuteDirectNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel,
	                                                            count) &&
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
    const SelectionVector *execute_sel, idx_t count, ExecutionGroupedAggregateStateAddressBinding &grouped_state,
    SljitAggregatePayloadAdapterScratch &payload_scratch, bool defer_grouped_finish,
    optional_ptr<bool> deferred_grouped_finish) {
	optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
	if (op.aggregate_update.dense_group_domain.ready) {
		dense_domain = &op.aggregate_update.dense_group_domain;
		RecordSljitRegionRuntimePath(runtime, op.kind, "direct_grouped_dense_group_domain", count);
	}
	if (SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel,
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
    const SelectionVector *execute_sel, idx_t count, ExecutionGroupedAggregateStateAddressBinding &grouped_state,
    SljitAggregatePayloadAdapterScratch &payload_scratch, bool defer_grouped_finish,
    optional_ptr<bool> deferred_grouped_finish) {
	if (TryExecutePreaggregatedDirectGroupedAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
	                                                        execute_sel, count, grouped_state, defer_grouped_finish,
	                                                        deferred_grouped_finish)) {
		return true;
	}
	if (SljitCanExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes,
	                                                                  execute_sel, count) &&
	    TryExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
	                                                             grouped_state, !defer_grouped_finish)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	if (TryExecuteDirectNewGroupedAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes, execute_sel,
	                                              count, grouped_state, defer_grouped_finish,
	                                              deferred_grouped_finish)) {
		return true;
	}
	if (op.aggregate_update.fused_payload_update_function &&
	    TryExecuteDirectGroupedStateAddressPayloadUpdatePath(runtime, scratch, op_idx, op, input, payload_lanes,
	                                                         execute_sel, count, grouped_state, payload_scratch,
	                                                         defer_grouped_finish, deferred_grouped_finish)) {
		return true;
	}
	return false;
}

static bool TryExecuteDirectGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const SelectionVector *execute_sel, idx_t count, ExecutionGroupedAggregateStateAddressBinding &grouped_state,
    SljitAggregatePayloadAdapterScratch &payload_scratch, bool defer_grouped_finish,
    optional_ptr<bool> deferred_grouped_finish) {
	switch (op.aggregate_update.grouped_direct_update.kind) {
	case SljitGroupedAggregateDirectUpdatePlanKind::NONE:
		return false;
	case SljitGroupedAggregateDirectUpdatePlanKind::ADAPTIVE_GROUPED_STATE_ADDRESS:
		return TryExecuteAdaptiveDirectGroupedAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
		                                                      execute_sel, count, grouped_state, payload_scratch,
		                                                      defer_grouped_finish, deferred_grouped_finish);
	case SljitGroupedAggregateDirectUpdatePlanKind::DIRECT_STATE_ADDRESS_PAYLOAD_ONLY:
		return TryExecuteDirectGroupedStateAddressPayloadUpdatePath(runtime, scratch, op_idx, op, input, payload_lanes,
		                                                            execute_sel, count, grouped_state, payload_scratch,
		                                                            defer_grouped_finish, deferred_grouped_finish);
	default:
		throw InternalException("Unknown SLJIT direct grouped aggregate update plan");
	}
}

} // namespace duckdb
