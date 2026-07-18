//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_input_vector_run_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_run_preaggregation_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_preaggregated_group_continuation_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static idx_t SljitEstimateRunPreaggregatedGroupReserve(SljitExecutableRegionOp &op, idx_t input_count,
                                                       idx_t run_count) {
	const auto estimated_input_count = op.aggregate_update.plan.estimated_input_count;
	if (input_count == 0 || run_count == 0 || estimated_input_count <= input_count) {
		return 0;
	}
	if (estimated_input_count > NumericLimits<idx_t>::Maximum() / run_count) {
		return estimated_input_count;
	}
	auto estimated_groups = (estimated_input_count * run_count + input_count - 1) / input_count;
	return MinValue(estimated_groups, estimated_input_count);
}

static bool SljitCanApplyRunPreaggregatedInputVectorAggregateUpdate(
    SljitExecutableRegionOp &op, idx_t run_count, SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state) {
	return run_count != 0 && preaggregate_scratch.group_row_counts.size() == run_count &&
	       SljitPreaggregatedGroupedPrimitiveUpdateContractApplies(op, group_sources.size(), preaggregate_scratch,
	                                                               payload_lanes, grouped_state);
}

static void SljitRecordInputVectorPreaggregatedUpdateBoundaries(ExecutionRegionRuntime &runtime,
                                                                SljitNativeRegionOpKind op_kind, idx_t group_count,
                                                                idx_t represented_row_count) {
}

static bool TryExecuteRunPreaggregatedInputVectorCarryoverOnlyUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &run_group_keys, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t represented_row_count, bool finish) {
	if (run_group_keys.size() != 1 ||
	    !SljitCanApplyRunPreaggregatedInputVectorAggregateUpdate(op, run_group_keys.size(), preaggregate_scratch,
	                                                             group_sources, payload_lanes, grouped_state)) {
		return false;
	}
	auto &continuation = scratch.AggregatePreaggregatedGroupContinuation(op_idx);
	if (!SljitPreaggregatedGroupContinuationMatches(continuation, run_group_keys, 0)) {
		return false;
	}
	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(payload_lanes, preaggregate_scratch.payloads);
	uintptr_t address = continuation.state_address;
	auto stage_start = SljitRegionStageStart(runtime);
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(&address, nullptr, run_group_keys.size(),
	                                                ExecutionGroupedAggregateStateAddressUpdateMode::UPDATE_INITIALIZED,
	                                                &update_state);
	grouped_state.state->RecordDirectStateAddressUpdates(represented_row_count);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "direct_input_vector_run_preaggregated_carryover_update", stage_start);
	RecordSljitRegionMaterializationElisionPath(
	    runtime, op.kind, "direct_input_vector_run_preaggregated_carryover_update", represented_row_count);
	SljitRecordInputVectorPreaggregatedUpdateBoundaries(runtime, op.kind, run_group_keys.size(), represented_row_count);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
		continuation.Clear();
	}
	return true;
}

static bool TryExecuteRunPreaggregatedInputVectorAppendSuffixWithPrefixUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &run_group_keys, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t represented_row_count, bool finish,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (run_group_keys.size() == 1) {
		return TryExecuteRunPreaggregatedInputVectorCarryoverOnlyUpdate(
		    runtime, scratch, op_idx, op, run_group_keys, group_sources, preaggregate_scratch, payload_lanes,
		    grouped_state, represented_row_count, finish);
	}
	if (run_group_keys.size() < 2 ||
	    !SljitCanApplyRunPreaggregatedInputVectorAggregateUpdate(op, run_group_keys.size(), preaggregate_scratch,
	                                                             group_sources, payload_lanes, grouped_state)) {
		return false;
	}
	auto &continuation = scratch.AggregatePreaggregatedGroupContinuation(op_idx);
	if (!SljitPreaggregatedGroupContinuationMatches(continuation, run_group_keys, 0)) {
		return false;
	}

	const idx_t prefix_count = 1;
	const auto suffix_offset = prefix_count;
	const auto suffix_count = run_group_keys.size() - suffix_offset;
	idx_t prefix_row_count;
	idx_t suffix_row_count;
	if (!CanSlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, 0, prefix_count) ||
	    !CanSlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, suffix_offset, suffix_count) ||
	    !PreaggregatedPrimitiveRepresentedRowCount(preaggregate_scratch, 0, prefix_count, prefix_row_count) ||
	    !PreaggregatedPrimitiveRepresentedRowCount(preaggregate_scratch, suffix_offset, suffix_count,
	                                               suffix_row_count)) {
		return false;
	}
	D_ASSERT(prefix_row_count + suffix_row_count == represented_row_count);

	DataChunk run_group_suffix;
	run_group_suffix.InitializeEmpty(run_group_keys.GetTypes());
	run_group_suffix.Slice(run_group_keys, suffix_offset, run_group_keys.size());

	auto &sink_info = op.aggregate_update.plan.sink_info;
	auto &preaggregate_scratch_slice = scratch.AggregatePreaggregateScratchSlice(op_idx);
	if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, suffix_offset, suffix_count,
	                                        preaggregate_scratch_slice)) {
		return false;
	}

	auto suffix_update_state = SljitMakePreaggregatedPrimitiveUpdateState(
	    payload_lanes, preaggregate_scratch_slice.payloads, suffix_count - 1);
	auto suffix_stage_start = SljitRegionStageStart(runtime);
	auto suffix_appended = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_append_input_vector_run_preaggregated_suffix_update", suffix_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryAppendNewGroupKeysWithStateAddresses(
		        run_group_suffix, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &suffix_update_state,
		        recorder, false, dense_domain);
	    });
	if (!suffix_appended) {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "direct_append_input_vector_run_preaggregated_suffix_update_miss",
		                                  suffix_stage_start);
		return false;
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "direct_append_input_vector_run_preaggregated_suffix_update", suffix_stage_start);
	SljitRecordInputVectorPreaggregatedUpdateBoundaries(runtime, op.kind, suffix_count, suffix_row_count);

	if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, 0, prefix_count,
	                                        preaggregate_scratch_slice)) {
		throw InternalException("Validated SLJIT input-vector preaggregated prefix scratch slice failed");
	}
	auto prefix_update_state =
	    SljitMakePreaggregatedPrimitiveUpdateState(payload_lanes, preaggregate_scratch_slice.payloads);
	uintptr_t prefix_address = continuation.state_address;
	auto prefix_stage_start = SljitRegionStageStart(runtime);
	ExecuteSljitPreaggregatedPrimitiveAddressUpdate(&prefix_address, nullptr, prefix_count,
	                                                ExecutionGroupedAggregateStateAddressUpdateMode::UPDATE_INITIALIZED,
	                                                &prefix_update_state);
	grouped_state.state->RecordDirectStateAddressUpdates(prefix_count);
	RecordPreaggregatedGroupedAggregateRepresentedRows(grouped_state, represented_row_count, run_group_keys.size());
	RecordSljitRegionStageRuntimePath(
	    runtime, op_idx, op.kind, "direct_input_vector_run_preaggregated_prefix_carryover_update", prefix_stage_start);
	RecordSljitRegionMaterializationElisionPath(
	    runtime, op.kind, "direct_input_vector_run_preaggregated_prefix_carryover_update", prefix_row_count);
	SljitRecordInputVectorPreaggregatedUpdateBoundaries(runtime, op.kind, prefix_count, prefix_row_count);

	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
		continuation.Clear();
	} else {
		SljitStorePreaggregatedGroupContinuation(continuation, run_group_keys, run_group_keys.size() - 1,
		                                         suffix_update_state.captured_address);
	}
	scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionMaterializationElisionPath(
	    runtime, op.kind, "direct_input_vector_run_preaggregated_suffix_append_prefix_update", represented_row_count);
	return true;
}

static bool TryExecuteRunPreaggregatedInputVectorAppendNewUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &run_group_keys, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t represented_row_count, bool finish,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain) {
	if (!SljitCanApplyRunPreaggregatedInputVectorAggregateUpdate(op, run_group_keys.size(), preaggregate_scratch,
	                                                             group_sources, payload_lanes, grouped_state)) {
		return false;
	}
	if (SljitPreaggregatedGroupContinuationMatches(scratch.AggregatePreaggregatedGroupContinuation(op_idx),
	                                               run_group_keys, 0)) {
		return TryExecuteRunPreaggregatedInputVectorAppendSuffixWithPrefixUpdate(
		    runtime, scratch, op_idx, op, run_group_keys, group_sources, preaggregate_scratch, payload_lanes,
		    grouped_state, represented_row_count, finish, dense_domain);
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(payload_lanes, preaggregate_scratch.payloads,
	                                                               run_group_keys.size() - 1);
	auto append_stage_start = SljitRegionStageStart(runtime);
	auto appended = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_append_input_vector_run_preaggregated_grouped_update", append_stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryAppendNewGroupKeysWithStateAddresses(
		        run_group_keys, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &update_state, recorder,
		        finish, dense_domain);
	    });
	if (appended) {
		RecordPreaggregatedGroupedAggregateRepresentedRows(grouped_state, represented_row_count, run_group_keys.size());
		scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, true);
		auto &continuation = scratch.AggregatePreaggregatedGroupContinuation(op_idx);
		if (finish) {
			continuation.Clear();
		} else {
			SljitStorePreaggregatedGroupContinuation(continuation, run_group_keys, run_group_keys.size() - 1,
			                                         update_state.captured_address);
		}
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "direct_append_input_vector_run_preaggregated_grouped_update",
		                                  append_stage_start);
		RecordSljitRegionMaterializationElisionPath(
		    runtime, op.kind, "direct_input_vector_run_preaggregated_append_new_grouped_update", represented_row_count);
		SljitRecordInputVectorPreaggregatedUpdateBoundaries(runtime, op.kind, run_group_keys.size(),
		                                                    represented_row_count);
		return true;
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "direct_append_input_vector_run_preaggregated_grouped_update_miss",
	                                  append_stage_start);
	if (TryExecuteRunPreaggregatedInputVectorAppendSuffixWithPrefixUpdate(
	        runtime, scratch, op_idx, op, run_group_keys, group_sources, preaggregate_scratch, payload_lanes,
	        grouped_state, represented_row_count, finish, dense_domain)) {
		return true;
	}
	return false;
}

static bool TryExecuteRunPreaggregatedInputVectorGroupedTargetPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices, SljitAggregatePayloadSourceLayout payload_source_layout,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish, optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
	auto &run_group_keys = scratch.AggregatePreaggregatedGroups(op_idx);
	idx_t run_count = 0;
	bool fused_run_payloads = false;
	auto preaggregate_stage_start = SljitRegionStageStart(runtime);
	if (!TryPreaggregateInputVectorPrimitiveGroupRunsBest(
	        op, payload_input, group_sources, payload_source_indices, payload_source_layout, payload_lanes,
	        reduction_lanes, preaggregate_scratch, payload_scratch, optional_ptr<DataChunk>(&run_group_keys), run_count,
	        fused_run_payloads)) {
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
	                              fused_run_payloads ? "local_preaggregate_input_vector_fused_primitive_group_runs"
	                                                 : "local_preaggregate_input_vector_primitive_group_runs",
	                              preaggregate_stage_start);
	if (run_count == 0 || run_count > payload_input.size()) {
		return false;
	}

	const auto reserve_group_count = SljitEstimateRunPreaggregatedGroupReserve(op, payload_input.size(), run_count);
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state, reserve_group_count);

	DataChunk run_lookup_input;
	if (TryExecuteRunPreaggregatedInputVectorAppendNewUpdate(
	        runtime, scratch, op_idx, op, run_group_keys, group_sources, preaggregate_scratch, payload_lanes,
	        grouped_state, payload_input.size(), finish, dense_domain)) {
		return true;
	}
	run_lookup_input.InitializeEmpty(payload_input.GetTypes());
	SelectionVector run_group_rows(preaggregate_scratch.group_rows.data(), run_count);
	run_lookup_input.Slice(payload_input, run_group_rows, run_count, 0);
	ExecutionGroupedAggregateStateTargetBatch targets;
	auto lookup_start = SljitRegionStageStart(runtime);
	const char *lookup_stage_name = "direct_input_vector_run_preaggregated_group_lookup";
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, lookup_stage_name, lookup_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateInputVectorGroupStateTargets(
		        run_lookup_input, run_count, group_sources, op.aggregate_update.plan.sink_info, targets, recorder,
		        dense_domain);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	if (!updated) {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "direct_input_vector_run_preaggregated_group_lookup_miss", lookup_start);
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, lookup_stage_name, lookup_start);

	auto update_start = SljitRegionStageStart(runtime);
	auto update_state = SljitMakePreaggregatedPrimitiveUpdateState(payload_lanes, preaggregate_scratch.payloads);
	ExecuteSljitPreaggregatedPrimitiveTargetBatch(targets, update_state);
	RecordPreaggregatedGroupedAggregateRepresentedRows(grouped_state, payload_input.size(), run_count);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_input_vector_run_preaggregated_payload_update",
	                              update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	if (fused_run_payloads) {
		RecordSljitRegionMaterializationElisionPath(
		    runtime, op.kind, "direct_input_vector_run_fused_preaggregated_grouped_update", payload_input.size());
	}
	RecordSljitRegionMaterializationElisionPath(
	    runtime, op.kind, "direct_input_vector_run_preaggregated_grouped_update", payload_input.size());
	SljitRecordInputVectorPreaggregatedUpdateBoundaries(runtime, op.kind, run_count, payload_input.size());
	return true;
}

} // namespace duckdb
