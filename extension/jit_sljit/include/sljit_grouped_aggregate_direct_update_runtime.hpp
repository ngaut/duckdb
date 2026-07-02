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
#include "sljit_aggregate_row_pointer_preaggregation.hpp"
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool TryExecuteDirectRowPointerGroupedFusedPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish = true) {
	auto stage_start = SljitRegionStageStart(runtime);
	const char *stage_name = "direct_row_pointer_grouped_lookup_update";
	const char *miss_stage_name = "direct_row_pointer_grouped_lookup_update_miss";
	ExecutionGroupedAggregateStateTargetBatch targets;
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        payload_input, row_pointers, count, group_sources, op.aggregate_update.plan.sink_info, targets,
		        recorder);
	    });
	if (updated) {
		auto target_payload_start = SljitRegionStageStart(runtime);
		auto update_state =
		    SljitBuildGroupedStateAddressUpdateState(op, payload_input, payload_lanes, payload_scratch,
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
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_grouped_lookup_update", count);
	}
	return updated;
}

static bool TryExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool payload_sources_are_fused_override, bool finish = true) {
	auto &compact_row_pointers = scratch.AggregatePreaggregatedRowPointers(op_idx);
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
	idx_t compact_count = 0;
	auto preaggregate_stage_start = SljitRegionStageStart(runtime);
	const bool preaggregated =
	    payload_sources_are_fused_override
	        ? SljitTryPreaggregateConsecutiveRowPointerFusedPrimitiveGroups(
	              op, payload_input, row_pointers, group_sources, payload_source_indices, payload_lanes,
	              compact_row_pointers, preaggregate_scratch, payload_scratch, compact_count)
	        : TryPreaggregateConsecutiveRowPointerPrimitiveGroups(
	              payload_input, row_pointers, group_sources, payload_source_indices, payload_lanes,
	              compact_row_pointers, preaggregate_scratch, compact_count);
	if (!preaggregated) {
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "local_preaggregate_row_pointer_primitive_groups",
	                              preaggregate_stage_start);

	DataChunk compact_lookup_input;
	vector<LogicalType> empty_types;
	compact_lookup_input.InitializeEmpty(empty_types);
	compact_lookup_input.SetChildCardinality(compact_count);

	ExecutionGroupedAggregateStateTargetBatch targets;
	auto stage_start = SljitRegionStageStart(runtime);
	const char *stage_name = "direct_row_pointer_preaggregated_grouped_primitive_update";
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        compact_lookup_input, compact_row_pointers, compact_count, group_sources,
		        op.aggregate_update.plan.sink_info, targets, recorder);
	    });
	if (!updated) {
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(
		    runtime, op_idx, op.kind, "direct_row_pointer_preaggregated_grouped_primitive_update_miss", stage_start);
		return false;
	}

	auto update_start = SljitRegionStageStart(runtime);
	SljitPreaggregatedPrimitiveUpdateState update_state;
	update_state.lanes = &payload_lanes;
	update_state.payloads = &preaggregate_scratch.payloads;
	ExecuteSljitPreaggregatedPrimitiveTargetBatch(targets, update_state);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_row_pointer_preaggregated_primitive_payload_update",
	                              update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "direct_row_pointer_preaggregated_grouped_primitive_update", stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_row_pointer_primitive_groups",
	                                         compact_count);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_grouped_lookup_update", compact_count);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", count);
	return true;
}

static bool SljitTryExecuteNativeRowPointerGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input, Vector &row_pointers,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
	auto record_unsupported = [&](const char *reason) {
		auto path = string("direct_row_pointer_grouped_lookup_update_unsupported.") + reason;
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
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, payload_input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready || !op.aggregate_update.plan.use_primitive_payloads) {
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

	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, primitive);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	const bool finish = !defer_grouped_finish;
	bool payload_sources_are_fused_override = false;
	if (SljitCanExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
	        scratch, op_idx, op, payload_input, row_pointers, group_sources, payload_source_indices, payload_lanes,
	        count, payload_sources_are_fused_override) &&
	    TryExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
	        runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_source_indices,
	        payload_lanes, grouped_state, payload_scratch, payload_sources_are_fused_override, finish)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	if (SljitCanExecuteDirectGroupedFusedPayloadUpdate(scratch, op_idx, op, payload_input, payload_lanes, nullptr,
	                                                   count)) {
		if (!TryExecuteDirectRowPointerGroupedFusedPayloadUpdate(
		        runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_source_indices,
		        payload_lanes, grouped_state, payload_scratch, finish)) {
			record_unsupported("grouped_lookup_update");
			return false;
		}
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}

	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_row_pointer_grouped_lookup_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateRowPointerGroupPayloads(
		        payload_input, row_pointers, count, group_sources, payload_source_indices,
		        op.aggregate_update.plan.sink_info, payload_lanes, recorder, finish);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  updated ? "direct_row_pointer_grouped_lookup_update"
	                                          : "direct_row_pointer_grouped_lookup_update_miss",
	                                  stage_start);
	if (!updated) {
		record_unsupported("row_pointer_payload_update");
		return false;
	}
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_grouped_lookup_update", count);
	MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	return true;
}

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
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_new_grouped_primitive_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateNewGroups(input, op.aggregate_update.plan.sink_info, payload_lanes,
		                                                   recorder, finish);
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

static bool TryExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t preaggregated_row_count,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
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

	SljitPreaggregatedPrimitiveUpdateState update_state;
	update_state.lanes = &payload_lanes;
	update_state.payloads = &preaggregate_scratch.payloads;
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
		scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(
		    runtime, op_idx, op.kind, "direct_append_preaggregated_grouped_primitive_update_miss", append_stage_start);
	}

	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_preaggregated_grouped_primitive_update", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
		        compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveUpdate, &update_state, recorder, finish);
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

static bool TryExecuteDirectGroupedAggregateUpdateRoute(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &input, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const SelectionVector *execute_sel, idx_t count, ExecutionGroupedAggregateStateAddressBinding &grouped_state,
    SljitAggregatePayloadAdapterScratch &payload_scratch, bool defer_grouped_finish,
    optional_ptr<bool> deferred_grouped_finish) {
	const bool finish = !defer_grouped_finish;
	if (SljitCanExecutePreaggregatedGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes,
	                                                                execute_sel, count)) {
		auto &compact_groups = scratch.AggregatePreaggregatedGroups(op_idx);
		auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
		auto preaggregate_stage_start = SljitRegionStageStart(runtime);
		if (TryPreaggregateConsecutivePrimitiveGroups(op, input, payload_lanes, compact_groups, preaggregate_scratch)) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "local_preaggregate_primitive_groups",
			                              preaggregate_stage_start);
			if (TryExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
			        runtime, scratch, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state,
			        input.size(), defer_grouped_finish, deferred_grouped_finish)) {
				return true;
			}
		}
	}
	if (SljitCanExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes,
	                                                                  execute_sel, count) &&
	    TryExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
	                                                             grouped_state, finish)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	if (SljitCanExecuteDirectNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel,
	                                                            count) &&
	    TryExecuteDirectNewGroupedPrimitiveAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
	                                                       grouped_state, finish)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	if (SljitCanExecuteDirectGroupedFusedPayloadUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel, count) &&
	    TryExecuteDirectGroupedFusedPayloadUpdate(runtime, scratch, op_idx, op, input, payload_lanes, grouped_state,
	                                              payload_scratch, finish)) {
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}
	return false;
}

} // namespace duckdb
