//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_preaggregated_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_aggregate_preaggregation.hpp"
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_preaggregated_group_continuation_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

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
	    sink_info.aggregates.size() != op.aggregate_update.payload_descriptors.size() ||
	    sink_info.aggregates.size() != payload_lanes.size() ||
	    sink_info.aggregates.size() != preaggregate_scratch.payloads.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
		if (op.aggregate_update.payload_descriptors[payload_idx].primitive_kind !=
		    preaggregate_scratch.payloads[payload_idx].kind) {
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

static bool TryExecutePreaggregatedGroupedPrimitiveCarryoverOnlyUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t preaggregated_row_count,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (compact_groups.size() != 1 || !SljitCanApplyPreaggregatedGroupedPrimitiveAggregateUpdate(
	                                      op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state)) {
		return false;
	}
	auto &continuation = scratch.AggregatePreaggregatedGroupContinuation(op_idx);
	if (!SljitPreaggregatedGroupContinuationMatches(continuation, compact_groups, 0)) {
		return false;
	}
	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(payload_lanes, preaggregate_scratch.payloads);
	uintptr_t address = continuation.state_address;
	auto stage_start = SljitRegionStageStart(runtime);
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(&address, nullptr, compact_groups.size(), &update_state);
	grouped_state.state->RecordDirectStateAddressUpdates(preaggregated_row_count);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "preaggregated_grouped_primitive_carryover_only_update",
	                                  stage_start);
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "preaggregated_carryover_only_update",
	                                            compact_groups.size());
	if (!defer_grouped_finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
		continuation.Clear();
	}
	MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	return true;
}

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

	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(payload_lanes, preaggregate_scratch.payloads,
	                                                               compact_groups.size() - 1);
	optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
	if (op.aggregate_update.dense_group_domain.ready) {
		dense_domain = &op.aggregate_update.dense_group_domain;
	}
	const bool finish = !defer_grouped_finish;
	auto &continuation = scratch.AggregatePreaggregatedGroupContinuation(op_idx);
	const auto leading_continuation = SljitPreaggregatedGroupContinuationMatches(continuation, compact_groups, 0);
	if (leading_continuation) {
		if (TryExecutePreaggregatedGroupedPrimitiveCarryoverOnlyUpdate(
		        runtime, scratch, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state,
		        preaggregated_row_count, defer_grouped_finish, deferred_grouped_finish)) {
			return true;
		}
		if (TryExecutePreaggregatedGroupedPrimitiveAppendSuffixWithPrefixUpdate(
		        runtime, scratch, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state,
		        defer_grouped_finish, deferred_grouped_finish)) {
			return true;
		}
	}
	if (!leading_continuation && !scratch.DirectAppendNewAggregateUpdateDisabled(op_idx)) {
		auto append_stage_start = SljitRegionStageStart(runtime);
		auto appended = ExecuteSljitRegionRecordedOperation(
		    runtime, op_idx, op.kind, "preaggregated_grouped_primitive_append_update", append_stage_start,
		    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    return grouped_state.state->TryAppendNewGroupsWithStateAddresses(
			        compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &update_state, recorder,
			        finish, dense_domain);
		    });
		if (appended) {
			RecordPreaggregatedGroupedAggregateRepresentedRows(grouped_state, preaggregated_row_count,
			                                                   compact_groups.size());
			scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, true);
			auto &continuation = scratch.AggregatePreaggregatedGroupContinuation(op_idx);
			if (finish) {
				continuation.Clear();
			} else {
				SljitStorePreaggregatedGroupContinuation(continuation, compact_groups, compact_groups.size() - 1,
				                                         update_state.captured_address);
			}
			RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "preaggregated_grouped_primitive_append_update",
			                                  append_stage_start);
			RecordSljitRegionMaterializationElisionProof(
			    runtime, op.kind, "preaggregated_grouped_primitive_append_update", preaggregated_row_count);
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		if (TryExecutePreaggregatedGroupedPrimitiveAppendSuffixWithPrefixUpdate(
		        runtime, scratch, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state,
		        defer_grouped_finish, deferred_grouped_finish)) {
			return true;
		}
		scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "preaggregated_grouped_primitive_append_update_miss", append_stage_start);
	}

	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "preaggregated_grouped_primitive_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateGroupKeysWithSelectedStateAddresses(
		        compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveUpdate, &update_state, recorder, finish,
		        nullptr, dense_domain);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  updated ? "preaggregated_grouped_primitive_update"
	                                          : "preaggregated_grouped_primitive_update_miss",
	                                  stage_start);
	if (updated) {
		RecordPreaggregatedGroupedAggregateRepresentedRows(grouped_state, preaggregated_row_count,
		                                                   compact_groups.size());
		scratch.AggregatePreaggregatedGroupContinuation(op_idx).Clear();
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, "preaggregated_grouped_primitive_update",
		                                             preaggregated_row_count);
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	return false;
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
	auto &continuation = scratch.AggregatePreaggregatedGroupContinuation(op_idx);
	if (!SljitPreaggregatedGroupContinuationMatches(continuation, compact_groups, 0)) {
		return false;
	}

	group_slice.Reset();
	group_slice.Slice(compact_groups, suffix_offset, compact_groups.size());
	if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, suffix_offset, suffix_count,
	                                        preaggregate_scratch_slice)) {
		return false;
	}
	auto suffix_update_state = SljitMakePreaggregatedPrimitiveUpdateState(
	    payload_lanes, preaggregate_scratch_slice.payloads, suffix_count - 1);
	auto suffix_stage_start = SljitRegionStageStart(runtime);
	optional_ptr<const ExecutionDenseGroupDomain> dense_domain;
	if (op.aggregate_update.dense_group_domain.ready) {
		dense_domain = &op.aggregate_update.dense_group_domain;
	}
	auto suffix_appended = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "preaggregated_grouped_primitive_append_suffix_update", suffix_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryAppendNewGroupsWithStateAddresses(
		        group_slice, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &suffix_update_state, recorder,
		        false, dense_domain);
	    });
	if (!suffix_appended) {
		RecordSljitRegionStageRuntimePath(
		    runtime, op_idx, op.kind, "preaggregated_grouped_primitive_append_suffix_update_miss", suffix_stage_start);
		return false;
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "preaggregated_grouped_primitive_append_suffix_update",
	                                  suffix_stage_start);

	if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, 0, prefix_count,
	                                        preaggregate_scratch_slice)) {
		throw InternalException("Validated SLJIT preaggregated prefix scratch slice failed");
	}
	auto prefix_update_state =
	    SljitMakePreaggregatedPrimitiveUpdateState(payload_lanes, preaggregate_scratch_slice.payloads);
	auto prefix_stage_start = SljitRegionStageStart(runtime);
	uintptr_t prefix_address = continuation.state_address;
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(&prefix_address, nullptr, prefix_count, &prefix_update_state);
	grouped_state.state->RecordDirectStateAddressUpdates(prefix_count);
	RecordPreaggregatedGroupedAggregateRepresentedRows(grouped_state, prefix_row_count + suffix_row_count,
	                                                   compact_groups.size());
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "preaggregated_grouped_primitive_prefix_carryover_update", prefix_stage_start);
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "preaggregated_prefix_carryover_update",
	                                            prefix_count);
	if (!defer_grouped_finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
		continuation.Clear();
	} else {
		SljitStorePreaggregatedGroupContinuation(continuation, compact_groups, compact_groups.size() - 1,
		                                         suffix_update_state.captured_address);
	}
	scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "preaggregated_suffix_append_prefix_update",
	                                            compact_groups.size());
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
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state, compact_groups.size());
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
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "preaggregated_primitive_group_batches",
	                                            preaggregated_row_count);
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
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes, const SelectionVector *execute_sel, idx_t count,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	if (scratch.DirectNewAggregateUpdateDisabled(op_idx) || execute_sel != nullptr || count != input.size()) {
		return false;
	}
	auto &compact_groups = scratch.AggregatePreaggregatedGroups(op_idx);
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
	auto preaggregate_stage_start = SljitRegionStageStart(runtime);
	bool preaggregated = false;
	if (SljitCanExecutePreaggregatedGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, reduction_lanes,
	                                                                execute_sel, count)) {
		preaggregated =
		    TryPreaggregateConsecutivePrimitiveGroups(op, input, payload_lanes, compact_groups, preaggregate_scratch);
	}
	if (!preaggregated) {
		preaggregated = TryPreaggregateDenseFusedPrimitiveGroups(
		    op, input, payload_lanes, compact_groups, reduction_lanes, preaggregate_scratch, payload_scratch);
	}
	if (preaggregated) {
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

} // namespace duckdb
