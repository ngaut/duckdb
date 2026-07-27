//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_input_vector_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_grouped_aggregate_direct_update_runtime.hpp"
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_input_vector_run_update_runtime.hpp"
#include "sljit_grouped_aggregate_pending_preaggregation_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_runtime_batch_runtime.hpp"

namespace duckdb {

static bool TryExecuteDirectInputVectorGroupedTargetPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices, optional_ptr<const vector<bool>> payload_source_not_null,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const vector<SljitGroupedReductionLaneBinding> &reduction_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish, bool reserve_from_input_count = true,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	if (reserve_from_input_count) {
		SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state, payload_input.size());
	}

	auto lookup_start = SljitRegionStageStart(runtime);
	const char *lookup_stage_name = "direct_input_vector_group_payload_update";
	const char *lookup_miss_stage_name = "direct_input_vector_group_payload_update_miss";
	ExecutionGroupedAggregateStateTargetBatch targets;
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, lookup_stage_name, lookup_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateInputVectorGroupStateTargets(
		        payload_input, payload_input.size(), group_sources, op.aggregate_update.plan.sink_info, targets,
		        recorder, dense_domain);
	    });
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	if (!updated) {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, lookup_miss_stage_name, lookup_start);
		return false;
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, lookup_stage_name, lookup_start);

	auto update_start = SljitRegionStageStart(runtime);
	auto update_state = SljitBuildGroupedStateAddressUpdateState(
	    op, payload_input, payload_lanes, reduction_lanes, payload_scratch,
	    optional_ptr<const vector<idx_t>>(&payload_source_indices), payload_source_not_null);
	SljitExecuteGroupedStateTargetBatch(targets, update_state);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_input_vector_group_payload_target_update",
	                              update_start);
	return true;
}

static bool SljitTryExecuteInputVectorGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices, optional_ptr<const vector<bool>> payload_source_not_null,
    SljitAggregatePayloadSourceLayout payload_source_layout,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish, bool source_key0_int64_to_int32_unchecked,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr, optional_ptr<string> failure_reason = nullptr,
    optional_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch> pending_preaggregated_groups = nullptr) {
	auto record_unsupported = [&](const char *reason) {
		if (failure_reason) {
			*failure_reason = reason;
		}
		auto path = string("direct_input_vector_grouped_update_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), payload_input.size());
	};
	auto &sink_info = op.aggregate_update.plan.sink_info;
	auto &reduction_lanes = scratch.GroupedReductionLanes(op_idx, sink_info.aggregate_contract,
	                                                      op.aggregate_update.payload_descriptors, payload_lanes);
	if (group_sources.empty() || group_sources.size() != sink_info.groups.size() || !grouped_state.ready ||
	    !grouped_state.state) {
		record_unsupported("shape");
		return false;
	}
	bool has_group_output_transform = false;
	for (auto &source : group_sources) {
		has_group_output_transform = has_group_output_transform || source.HasOutputTransform();
	}
	if (pending_preaggregated_groups && group_sources.size() == 1 &&
	    !pending_preaggregated_groups->ConfigureGroupOutputTransform(group_sources[0])) {
		record_unsupported("group_output_transform");
		return false;
	}
	if (has_group_output_transform && !pending_preaggregated_groups) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "group_output_transform.materialized_fallback",
		                             payload_input.size());
	}

	const bool direct_payload_sources = payload_source_layout == SljitAggregatePayloadSourceLayout::DIRECT_PER_LANE;
	const bool count_one_payload =
	    direct_payload_sources && SljitInputVectorCountPayloadIsCountOne(payload_input, payload_source_indices);
	const bool descriptor_count_one_payload = direct_payload_sources && payload_source_indices.size() == 1 &&
	                                          payload_source_indices[0] == DConstants::INVALID_INDEX;
	SljitPrimitiveCountOneUpdateState count_one_update;
	const bool count_one_update_ready = SljitTryBindPrimitiveCountOneUpdateState(
	    op.aggregate_update.payload_descriptors, payload_lanes, count_one_payload, count_one_update);
	if (!has_group_output_transform && pending_preaggregated_groups && dense_domain && dense_domain->ready &&
	    count_one_update_ready) {
		auto dense_pending_start = SljitRegionStageStart(runtime);
		if (SljitTryAccumulatePendingDenseSingleLaneGroups(op, payload_input, group_sources, payload_lanes,
		                                                   *dense_domain, *pending_preaggregated_groups)) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "pending_dense_single_lane_accumulate",
			                              dense_pending_start);
			RecordSljitRegionMaterializationElision(runtime, op.kind, "pending_dense_single_lane_grouped_update",
			                                            payload_input.size());
			if (finish && !SljitFlushPendingPreaggregatedPrimitiveGroups(
			                  runtime, scratch, op_idx, op, *pending_preaggregated_groups, grouped_state)) {
				record_unsupported("pending_dense_single_lane_flush");
				return false;
			}
			return true;
		}
		auto blocker = pending_preaggregated_groups->dense_single_lane_blocker.empty()
		                   ? string("unknown")
		                   : pending_preaggregated_groups->dense_single_lane_blocker;
		auto path = string("pending_dense_single_lane_grouped_update_miss.") + blocker;
		RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), payload_input.size());
	}
	if (pending_preaggregated_groups && payload_source_layout == SljitAggregatePayloadSourceLayout::FUSED_COMBINED) {
		auto pending_preaggregate_start = SljitRegionStageStart(runtime);
		if (TryPreaggregateInputVectorFusedAffinePrimitiveGroupsIntoPending(
		        runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices, payload_lanes,
		        grouped_state, *pending_preaggregated_groups, finish)) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
			                              "local_preaggregate_input_vector_pending_fused_affine_primitive_group_runs",
			                              pending_preaggregate_start);
			RecordSljitRegionMaterializationElision(
			    runtime, op.kind, "direct_input_vector_pending_fused_affine_preaggregated_grouped_update",
			    payload_input.size());
			return true;
		}
	}
	if (pending_preaggregated_groups && direct_payload_sources) {
		auto pending_preaggregate_start = SljitRegionStageStart(runtime);
		if (TryPreaggregateInputVectorPrimitiveGroupsIntoPending(
		        runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices, payload_lanes,
		        grouped_state, *pending_preaggregated_groups, finish)) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
			                              "local_preaggregate_input_vector_pending_primitive_group_runs",
			                              pending_preaggregate_start);
			RecordSljitRegionMaterializationElision(
			    runtime, op.kind, "direct_input_vector_pending_preaggregated_grouped_update", payload_input.size());
			return true;
		}
	}
	auto &preaggregated_groups = scratch.AggregatePreaggregatedGroups(op_idx);
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
	auto preaggregate_stage_start = SljitRegionStageStart(runtime);
	if (pending_preaggregated_groups && !has_group_output_transform) {
		idx_t preaggregated_group_count = 0;
		bool fused_run_payloads = false;
		if (TryPreaggregateInputVectorPrimitiveGroupRunsBest(
		        op, payload_input, group_sources, payload_source_indices, payload_source_layout, payload_lanes,
		        reduction_lanes, preaggregate_scratch, payload_scratch, optional_ptr<DataChunk>(&preaggregated_groups),
		        preaggregated_group_count, fused_run_payloads)) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
			                              fused_run_payloads
			                                  ? "local_preaggregate_input_vector_fused_primitive_group_runs"
			                                  : "local_preaggregate_input_vector_primitive_group_runs",
			                              preaggregate_stage_start);
			if (SljitBufferPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, preaggregated_groups,
			                                            preaggregate_scratch, payload_lanes, grouped_state,
			                                            payload_input.size(), *pending_preaggregated_groups, finish)) {
				RecordSljitRegionMaterializationElision(
				    runtime, op.kind, "direct_input_vector_preaggregated_grouped_update", payload_input.size());
				return true;
			}
		}
	}

	if (!has_group_output_transform && !descriptor_count_one_payload && dense_domain && dense_domain->ready &&
	    op.aggregate_update.fused_payload_update.Function()) {
		if (pending_preaggregated_groups && pending_preaggregated_groups->HasPending() &&
		    !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, *pending_preaggregated_groups,
		                                                   grouped_state)) {
			record_unsupported("pending_preaggregated_flush");
			return false;
		}
		if (TryExecuteRunPreaggregatedInputVectorGroupedTargetPayloadUpdate(
		        runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices,
		        payload_source_layout, payload_lanes, reduction_lanes, grouped_state, payload_scratch, finish,
		        dense_domain)) {
			return true;
		}
		if (TryExecuteDirectInputVectorGroupedTargetPayloadUpdate(
		        runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices,
		        payload_source_not_null, payload_lanes, reduction_lanes, grouped_state, payload_scratch, finish,
		        !pending_preaggregated_groups, dense_domain)) {
			RecordSljitRegionMaterializationElision(
			    runtime, op.kind, "direct_input_vector_dense_target_grouped_update", payload_input.size());
			return true;
		}
	}

	if (pending_preaggregated_groups && pending_preaggregated_groups->HasPending() &&
	    !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, *pending_preaggregated_groups,
	                                                   grouped_state)) {
		record_unsupported("pending_preaggregated_flush");
		return false;
	}
	preaggregate_stage_start = SljitRegionStageStart(runtime);
	idx_t preaggregated_group_count = 0;
	if (!has_group_output_transform && direct_payload_sources &&
	    TryPreaggregateInputVectorPrimitiveGroupRuns(
	        op, payload_input, group_sources, payload_source_indices, payload_lanes, preaggregate_scratch,
	        optional_ptr<DataChunk>(&preaggregated_groups), preaggregated_group_count)) {
		D_ASSERT(preaggregated_group_count == preaggregated_groups.size());
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "local_preaggregate_input_vector_primitive_groups",
		                              preaggregate_stage_start);
		if (pending_preaggregated_groups &&
		    SljitBufferPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, preaggregated_groups,
		                                            preaggregate_scratch, payload_lanes, grouped_state,
		                                            payload_input.size(), *pending_preaggregated_groups, finish)) {
			RecordSljitRegionMaterializationElision(
			    runtime, op.kind, "direct_input_vector_preaggregated_grouped_update", payload_input.size());
			return true;
		}
		if (!has_group_output_transform && TryExecutePreaggregatedGroupedPrimitiveAggregateUpdateBatches(
		                                       runtime, scratch, op_idx, op, preaggregated_groups, preaggregate_scratch,
		                                       payload_lanes, grouped_state, payload_input.size(), true, !finish)) {
			RecordSljitRegionMaterializationElision(
			    runtime, op.kind, "direct_input_vector_preaggregated_grouped_update", payload_input.size());
			return true;
		}
	}
	if (pending_preaggregated_groups &&
	    !SljitFlushPendingPreaggregatedPrimitiveGroups(runtime, scratch, op_idx, op, *pending_preaggregated_groups,
	                                                   grouped_state)) {
		record_unsupported("pending_preaggregated_flush");
		return false;
	}
	if (pending_preaggregated_groups) {
		SljitInvalidateProvenUniqueAppendContract(runtime, op, *pending_preaggregated_groups, grouped_state);
	}

	bool updated = false;
	if (!has_group_output_transform && count_one_update_ready) {
		if (dense_domain && dense_domain->ready) {
			auto fused_start = SljitRegionStageStart(runtime);
			updated = ExecuteSljitRegionRecordedOperation(
			    runtime, op_idx, op.kind, "input_vector_group_count_one.fused_lookup_update", fused_start,
			    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
				    return grouped_state.state->TryUpdateInputVectorGroupCountOne(
				        payload_input, payload_input.size(), group_sources, sink_info, *count_one_update.lane, recorder,
				        dense_domain);
			    });
			scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
			RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
			                                  updated ? "input_vector_group_count_one.fused_lookup_update"
			                                          : "input_vector_group_count_one.fused_lookup_update_miss",
			                                  fused_start);
			if (updated) {
				RecordSljitRegionMaterializationElision(
				    runtime, op.kind, "input_vector_group_count_one.fused_lookup_update", payload_input.size());
				return true;
			}
		}
		auto stage_start = SljitRegionStageStart(runtime);
		ExecutionGroupedAggregateStateTargetBatch targets;
		updated = ExecuteSljitRegionRecordedOperation(
		    runtime, op_idx, op.kind, "input_vector_group_targets.count_one_lookup", stage_start,
		    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    return grouped_state.state->TryFindOrCreateInputVectorGroupStateTargets(
			        payload_input, payload_input.size(), group_sources, sink_info, targets, recorder, dense_domain);
		    });
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  updated ? "input_vector_group_targets.count_one_lookup"
		                                          : "input_vector_group_targets.count_one_lookup_miss",
		                                  stage_start);
		if (updated) {
			auto update_start = SljitRegionStageStart(runtime);
			ExecuteSljitPrimitiveCountOneTargetBatch(targets, count_one_update);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "input_vector_group_targets.count_one_update",
			                              update_start);
			RecordSljitRegionMaterializationElision(
			    runtime, op.kind, "input_vector_group_targets.count_one_update", payload_input.size());
			return true;
		}
		if (descriptor_count_one_payload) {
			if (failure_reason) {
				*failure_reason = "count_one_update";
			}
			return false;
		}
	} else if (!has_group_output_transform && descriptor_count_one_payload) {
		record_unsupported("count_one_shape");
		return false;
	}

	if (!has_group_output_transform && op.aggregate_update.fused_payload_update.Function()) {
		if (!SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(scratch, op_idx, op, payload_input, reduction_lanes,
		                                                           nullptr, payload_input.size())) {
			record_unsupported("state_address_payload_shape");
		}
		updated = TryExecuteDirectInputVectorGroupedTargetPayloadUpdate(
		    runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices, payload_source_not_null,
		    payload_lanes, reduction_lanes, grouped_state, payload_scratch, finish, !pending_preaggregated_groups,
		    dense_domain);
		if (updated) {
			return true;
		}
		if (failure_reason) {
			*failure_reason = "state_address_target_payload_update";
		}
	}
	DataChunk *groups = nullptr;
	if (!SljitTryBuildInputVectorGroups(runtime, payload_scratch, payload_input, group_sources, groups,
	                                    source_key0_int64_to_int32_unchecked)) {
		record_unsupported("group_source");
		return false;
	}
	if (!groups || groups->size() != payload_input.size()) {
		record_unsupported("groups");
		return false;
	}

	if (op.aggregate_update.fused_payload_update.Function() &&
	    SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(scratch, op_idx, op, payload_input, reduction_lanes,
	                                                          nullptr, payload_input.size())) {
		updated = TryExecuteDirectProjectedGroupedStateAddressPayloadUpdate(
		    runtime, scratch, op_idx, op, *groups, payload_input, payload_source_indices, payload_lanes, grouped_state,
		    payload_scratch, finish, nullptr, dense_domain);
		if (updated) {
			return true;
		}
		if (failure_reason) {
			*failure_reason = "state_address_payload_update";
		}
	}

	if (direct_payload_sources) {
		auto stage_start = SljitRegionStageStart(runtime);
		updated = ExecuteSljitRegionRecordedOperation(
		    runtime, op_idx, op.kind, "direct_projected_group_payload_input_update", stage_start,
		    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    return grouped_state.state->TryUpdateNewGroupsWithPayloadInput(
			        *groups, payload_input, payload_source_indices, sink_info, payload_lanes, recorder, nullptr,
			        dense_domain);
		    });
		if (updated) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_projected_group_payload_input_update",
			                              stage_start);
			RecordSljitRegionMaterializationElision(
			    runtime, op.kind, "direct_projected_group_payload_input_update", payload_input.size());
			return true;
		}
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "direct_projected_group_payload_input_update_miss",
		                                  stage_start);
		if (failure_reason) {
			*failure_reason = "payload_update";
		}
	}

	scratch.RecordDirectNewAggregateUpdateResult(op_idx, false);
	return false;
}

static bool SljitTryExecuteNativeInputVectorGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    SljitAggregatePayloadSourceLayout payload_source_layout, bool defer_grouped_finish,
    bool source_key0_int64_to_int32_unchecked = false,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr, optional_ptr<string> failure_reason = nullptr,
    optional_ptr<SljitPendingPreaggregatedPrimitiveGroupBatch> pending_preaggregated_groups = nullptr,
    optional_ptr<const vector<bool>> payload_source_not_null = nullptr) {
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, payload_input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready ||
	    !binding.aggregate_update.grouped_state.ready || !binding.aggregate_update.grouped_state.state ||
	    !op.aggregate_update.plan.UsesPrimitivePayloads()) {
		if (failure_reason) {
			*failure_reason = "binding";
		}
		return false;
	}
	auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
	if (aggregates.size() != op.aggregate_update.payloads.size()) {
		if (failure_reason) {
			*failure_reason = "payload_count";
		}
		return false;
	}
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, op.aggregate_update.payload_descriptors,
	                                                    binding.aggregate_update.primitive);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	if (!pending_preaggregated_groups) {
		SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, binding.aggregate_update.grouped_state,
		                                      payload_input.size());
	}
	if (!SljitTryExecuteInputVectorGroupedAggregateUpdate(
	        runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices, payload_source_not_null,
	        payload_source_layout, payload_lanes, binding.aggregate_update.grouped_state, payload_scratch,
	        !defer_grouped_finish, source_key0_int64_to_int32_unchecked, dense_domain, failure_reason,
	        pending_preaggregated_groups)) {
		return false;
	}
	return true;
}

} // namespace duckdb
