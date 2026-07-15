//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_row_pointer_grouped_aggregate_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_aggregate_row_pointer_preaggregation.hpp"
#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_input_vector_update_runtime.hpp"
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_string_set_complementary_sum_runtime.hpp"

namespace duckdb {

static bool
SljitCanAttemptRowPointerCountOneTargetLookup(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	return ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources);
}

static bool TryExecuteDirectRowPointerGroupedTargetPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish = true) {
	auto stage_start = SljitRegionStageStart(runtime);
	const char *stage_name = "row_pointer_grouped_lookup_update";
	const char *miss_stage_name = "row_pointer_grouped_lookup_update_miss";
	ExecutionGroupedAggregateStateTargetBatch targets;
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        payload_input, row_pointers, count, group_sources, op.aggregate_update.plan.sink_info, targets,
		        recorder);
	    });
	if (updated) {
		auto target_payload_start = SljitRegionStageStart(runtime);
		auto &reduction_lanes =
		    scratch.GroupedReductionLanes(op_idx, op.aggregate_update.plan.sink_info.aggregate_contract,
		                                  op.aggregate_update.payload_descriptors, payload_lanes);
		auto update_state =
		    SljitBuildGroupedStateAddressUpdateState(op, payload_input, payload_lanes, reduction_lanes, payload_scratch,
		                                             optional_ptr<const vector<idx_t>>(&payload_source_indices));
		SljitExecuteGroupedStateTargetBatch(targets, update_state);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_row_pointer_grouped_target_payload_update",
		                              target_payload_start);
		if (finish) {
			FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
		}
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, updated ? stage_name : miss_stage_name, stage_start);
	if (updated) {
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, stage_name, count);
	}
	return updated;
}

static bool TryExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool uses_generated_payload_preaggregation, bool finish = true) {
	auto &compact_row_pointers = scratch.AggregatePreaggregatedRowPointers(op_idx);
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
	idx_t compact_count = 0;
	const char *failure_reason = "unknown";
	auto preaggregate_stage_start = SljitRegionStageStart(runtime);
	auto &reduction_lanes = scratch.GroupedReductionLanes(op_idx, op.aggregate_update.plan.sink_info.aggregate_contract,
	                                                      op.aggregate_update.payload_descriptors, payload_lanes);
	const bool preaggregated =
	    uses_generated_payload_preaggregation
	        ? SljitTryPreaggregateRowPointerFusedPrimitiveGroups(op, payload_input, row_pointers, group_sources,
	                                                             payload_source_indices, payload_lanes, reduction_lanes,
	                                                             compact_row_pointers, preaggregate_scratch,
	                                                             payload_scratch, compact_count, failure_reason)
	        : SljitTryPreaggregateRowPointerPrimitiveGroups(payload_input, row_pointers, group_sources,
	                                                        payload_source_indices, payload_lanes, compact_row_pointers,
	                                                        preaggregate_scratch, compact_count, failure_reason);
	if (!preaggregated) {
		scratch.RecordRowPointerPreaggregateResult(op_idx, false);
		RecordSljitRegionRuntimePath(runtime, op.kind, "direct_row_pointer_preaggregated_groups_miss", count);
		auto reason_path = string("direct_row_pointer_preaggregated_groups_miss.") + failure_reason;
		RecordSljitRegionRuntimePath(runtime, op.kind, reason_path.c_str(), count);
		return false;
	}
	scratch.RecordRowPointerPreaggregateResult(op_idx, true);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "local_preaggregate_row_pointer_primitive_groups",
	                              preaggregate_stage_start);

	DataChunk compact_lookup_input;
	if (SljitRowPointerPreaggregationUsesInputVectorGroups(group_sources)) {
		compact_lookup_input.InitializeEmpty(payload_input.GetTypes());
		SelectionVector compact_group_rows(preaggregate_scratch.group_rows.data(), compact_count);
		compact_lookup_input.Slice(payload_input, compact_group_rows, compact_count, 0);
	} else {
		vector<LogicalType> empty_types;
		compact_lookup_input.InitializeEmpty(empty_types);
		compact_lookup_input.SetChildCardinality(compact_count);
	}

	ExecutionGroupedAggregateStateTargetBatch targets;
	auto stage_start = SljitRegionStageStart(runtime);
	const char *stage_name = "row_pointer_preaggregated_grouped_primitive_update";
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        compact_lookup_input, compact_row_pointers, compact_count, group_sources,
		        op.aggregate_update.plan.sink_info, targets, recorder);
	    });
	if (!updated) {
		scratch.RecordRowPointerPreaggregateResult(op_idx, false);
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "row_pointer_preaggregated_grouped_primitive_update_miss", stage_start);
		return false;
	}

	auto update_start = SljitRegionStageStart(runtime);
	SljitPreaggregatedPrimitiveUpdateState update_state;
	update_state.lanes = &payload_lanes;
	update_state.payloads = &preaggregate_scratch.payloads;
	ExecuteSljitPreaggregatedPrimitiveTargetBatch(targets, update_state);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "row_pointer_preaggregated_primitive_payload_update",
	                              update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "row_pointer_preaggregated_grouped_primitive_update",
	                                  stage_start);
	RecordSljitRegionMaterializationElisionProof(runtime, op.kind, "row_pointer_preaggregated_grouped_primitive_update",
	                                             count);
	return true;
}

static bool SljitTryExecuteRowPointerCountOneGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	SljitPrimitiveCountOneUpdateState count_one_update;
	if (!SljitTryBindPrimitiveCountOneUpdateState(
	        op.aggregate_update.payload_descriptors, payload_lanes,
	        SljitInputVectorCountPayloadIsCountOne(payload_input, payload_source_indices), count_one_update)) {
		return false;
	}

	ExecutionGroupedAggregateStateTargetBatch targets;
	auto lookup_start = SljitRegionStageStart(runtime);
	auto found_targets = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_row_pointer_group_count_one_lookup", lookup_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        payload_input, row_pointers, count, group_sources, sink_info, targets, recorder);
	    });
	if (!found_targets) {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "direct_row_pointer_group_count_one_lookup_miss",
		                                  lookup_start);
		return false;
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "direct_row_pointer_group_count_one_lookup",
	                                  lookup_start);

	auto update_start = SljitRegionStageStart(runtime);
	ExecuteSljitPrimitiveCountOneTargetBatch(targets, count_one_update);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_row_pointer_group_count_one_update", update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind, "direct_row_pointer_group_count_one_update", count);
	return true;
}

struct SljitRowPointerGroupedAggregateUpdateDecision {
	bool try_preaggregated_primitive_groups = false;
	bool try_count_one_targets = false;
	bool try_input_vector_groups = false;
	bool use_target_payload_update = false;
	bool uses_generated_payload_preaggregation = false;
	bool prefer_sparse_row_pointer_target_update = false;
};

static SljitRowPointerGroupedAggregateUpdateDecision SljitPlanRowPointerGroupedAggregateUpdate(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input,
    Vector &row_pointers, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices, SljitAggregatePayloadSourceLayout payload_source_layout,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes) {
	SljitRowPointerGroupedAggregateUpdateDecision decision;
	const bool can_execute_direct_grouped_state_address_payload_update =
	    SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(scratch, op_idx, op, payload_input, reduction_lanes,
	                                                          nullptr, count);
	decision.prefer_sparse_row_pointer_target_update =
	    can_execute_direct_grouped_state_address_payload_update &&
	    SljitShouldPreferDirectSparseRowPointerTargetUpdate(row_pointers, count, group_sources);

	if (!decision.prefer_sparse_row_pointer_target_update && !scratch.RowPointerPreaggregateDisabled(op_idx) &&
	    SljitCanExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
	        scratch, op_idx, op, payload_input, row_pointers, group_sources, payload_source_indices,
	        payload_source_layout, reduction_lanes, count, decision.uses_generated_payload_preaggregation)) {
		decision.try_preaggregated_primitive_groups = true;
	}
	decision.try_count_one_targets = payload_source_layout == SljitAggregatePayloadSourceLayout::DIRECT_PER_LANE &&
	                                 SljitCanAttemptRowPointerCountOneTargetLookup(group_sources);
	decision.try_input_vector_groups = SljitGroupSourcesCanMaterializeFromInputVectors(payload_input, group_sources);
	decision.use_target_payload_update = can_execute_direct_grouped_state_address_payload_update;
	return decision;
}

static bool SljitTryExecuteRowPointerGroupedTargetPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    SljitAggregatePayloadSourceLayout payload_source_layout,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish) {
	if (payload_source_layout == SljitAggregatePayloadSourceLayout::FUSED_COMBINED) {
		if (TryExecuteDirectRowPointerPreclassifiedStringSetComplementarySumUpdate(
		        runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_source_indices,
		        payload_lanes, grouped_state, finish)) {
			return true;
		}
		if (TryExecuteDirectRowPointerStringSetComplementarySumUpdate(
		        runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_source_indices,
		        payload_lanes, grouped_state, finish)) {
			return true;
		}
	}
	return TryExecuteDirectRowPointerGroupedTargetPayloadUpdate(
	    runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_source_indices,
	    payload_lanes, grouped_state, payload_scratch, finish);
}

static bool SljitTryExecuteRowPointerGroupedSplitPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "row_pointer_grouped_lookup_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateRowPointerGroupPayloads(
		        payload_input, row_pointers, count, group_sources, payload_source_indices,
		        op.aggregate_update.plan.sink_info, payload_lanes, recorder, finish);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(
	    runtime, op_idx, op.kind,
	    updated ? "row_pointer_grouped_lookup_update" : "row_pointer_grouped_lookup_update_miss", stage_start);
	if (updated) {
		RecordSljitRegionMaterializationElisionProof(runtime, op.kind, "row_pointer_grouped_lookup_update", count);
	}
	return updated;
}

static bool SljitTryExecuteNativeRowPointerGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input, Vector &row_pointers,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    SljitAggregatePayloadSourceLayout payload_source_layout, bool defer_grouped_finish,
    optional_ptr<bool> deferred_grouped_finish, bool source_key0_int64_to_int32_unchecked = false) {
	auto record_unsupported = [&](const char *reason) {
		auto path = string("row_pointer_grouped_lookup_update_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str());
	};
	const auto count = payload_input.size();
	if (count == 0) {
		return false;
	}
	if (row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		record_unsupported("non_flat_row_pointers");
		return false;
	}
	if (payload_source_indices.empty()) {
		record_unsupported("payload_sources");
		return false;
	}
	auto proven_group_sources = group_sources;
	SljitApplyExecutableIntegralGroupKeyRangeProofs(op.aggregate_update.integral_group_key_ranges,
	                                                proven_group_sources);
	SljitApplyInputVectorGroupBatchCastProofs(payload_input, proven_group_sources, count);
	SljitApplyRowPointerGroupBatchCastProofs(row_pointers, proven_group_sources, count);
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, payload_input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready || !op.aggregate_update.plan.UsesPrimitivePayloads()) {
		record_unsupported("sink_binding");
		return false;
	}
	auto &primitive = binding.aggregate_update.primitive;
	if (!primitive.ready) {
		record_unsupported("primitive_binding");
		return false;
	}
	if (!NeedsGroupedAggregateStateAddressPlan(op.aggregate_update)) {
		record_unsupported("grouped_state_plan");
		return false;
	}
	auto &grouped_state = binding.aggregate_update.grouped_state;
	if (!grouped_state.ready || !grouped_state.state) {
		record_unsupported("grouped_state_binding");
		return false;
	}
	auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
	if (aggregates.size() != op.aggregate_update.payloads.size()) {
		record_unsupported("aggregate_payload_count");
		return false;
	}

	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, op.aggregate_update.payload_descriptors, primitive);
	auto &reduction_lanes = scratch.GroupedReductionLanes(op_idx, op.aggregate_update.plan.sink_info.aggregate_contract,
	                                                      op.aggregate_update.payload_descriptors, payload_lanes);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	const bool finish = !defer_grouped_finish;
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state);
	auto decision = SljitPlanRowPointerGroupedAggregateUpdate(scratch, op_idx, op, payload_input, row_pointers, count,
	                                                          proven_group_sources, payload_source_indices,
	                                                          payload_source_layout, reduction_lanes);
	if (decision.prefer_sparse_row_pointer_target_update) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "direct_row_pointer_sparse_target_update_preferred", count);
	}

	if (decision.try_preaggregated_primitive_groups &&
	    TryExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
	        runtime, scratch, op_idx, op, payload_input, row_pointers, count, proven_group_sources,
	        payload_source_indices, payload_lanes, grouped_state, payload_scratch,
	        decision.uses_generated_payload_preaggregation, finish)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	if (decision.try_count_one_targets &&
	    SljitTryExecuteRowPointerCountOneGroupedAggregateUpdate(
	        runtime, scratch, op_idx, op, payload_input, row_pointers, count, proven_group_sources,
	        payload_source_indices, payload_lanes, grouped_state, finish)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	if (decision.try_input_vector_groups &&
	    SljitTryExecuteInputVectorGroupedAggregateUpdate(
	        runtime, scratch, op_idx, op, payload_input, proven_group_sources, payload_source_indices, nullptr,
	        payload_source_layout, payload_lanes, grouped_state, payload_scratch, finish,
	        source_key0_int64_to_int32_unchecked)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	if (decision.use_target_payload_update) {
		if (SljitTryExecuteRowPointerGroupedTargetPayloadUpdate(
		        runtime, scratch, op_idx, op, payload_input, row_pointers, count, proven_group_sources,
		        payload_source_indices, payload_source_layout, payload_lanes, grouped_state, payload_scratch, finish)) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		record_unsupported("grouped_lookup_update");
		return false;
	}
	if (payload_source_layout == SljitAggregatePayloadSourceLayout::DIRECT_PER_LANE &&
	    SljitTryExecuteRowPointerGroupedSplitPayloadUpdate(runtime, scratch, op_idx, op, payload_input, row_pointers,
	                                                       count, proven_group_sources, payload_source_indices,
	                                                       payload_lanes, grouped_state, finish)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	record_unsupported("row_pointer_payload_update");
	return false;
}

} // namespace duckdb
