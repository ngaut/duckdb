//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_input_vector_update_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_input_vector_groups.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static bool TryExecuteDirectInputVectorGroupedTargetPayloadUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish, optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr) {
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, grouped_state);

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
	    op, payload_input, payload_lanes, payload_scratch, optional_ptr<const vector<idx_t>>(&payload_source_indices));
	SljitExecuteGroupedStateTargetBatch(targets, update_state);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_input_vector_group_payload_target_update",
	                              update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "input_vector_group_payload_update",
	                                         payload_input.size());
	return true;
}

static bool SljitTryExecuteInputVectorGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish, bool source_key0_int64_to_int32_unchecked,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr,
    optional_ptr<string> failure_reason = nullptr) {
	auto record_unsupported = [&](const char *reason) {
		if (failure_reason) {
			*failure_reason = reason;
		}
		auto path = string("direct_input_vector_grouped_update_unsupported.") + reason;
		RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str(), payload_input.size());
	};
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (group_sources.empty() || group_sources.size() != sink_info.groups.size() || !grouped_state.ready ||
	    !grouped_state.state) {
		record_unsupported("shape");
		return false;
	}

	bool updated = false;
	const bool count_one_payload = SljitInputVectorCountPayloadIsCountOne(payload_input, payload_source_indices);
	const bool descriptor_count_one_payload =
	    payload_source_indices.size() == 1 && payload_source_indices[0] == DConstants::INVALID_INDEX;
	SljitPrimitiveCountOneUpdateState count_one_update;
	if (SljitTryBindPrimitiveCountOneUpdateState(sink_info, payload_lanes, count_one_payload, count_one_update)) {
		auto stage_start = SljitRegionStageStart(runtime);
		ExecutionGroupedAggregateStateTargetBatch targets;
		updated = ExecuteSljitRegionRecordedOperation(
		    runtime, op_idx, op.kind, "direct_input_vector_group_count_one_lookup", stage_start,
		    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
			    return grouped_state.state->TryFindOrCreateInputVectorGroupStateTargets(
			        payload_input, payload_input.size(), group_sources, sink_info, targets, recorder, dense_domain);
		    });
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  updated ? "direct_input_vector_group_count_one_lookup"
		                                          : "direct_input_vector_group_count_one_lookup_miss",
		                                  stage_start);
		if (updated) {
			auto update_start = SljitRegionStageStart(runtime);
			ExecuteSljitPrimitiveCountOneTargetBatch(targets, count_one_update);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_input_vector_group_count_one_update",
			                              update_start);
			if (finish) {
				FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
			}
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "input_vector_group_count_one_update",
			                                         payload_input.size());
			return true;
		}
		if (descriptor_count_one_payload) {
			if (failure_reason) {
				*failure_reason = "count_one_update";
			}
			return false;
		}
	} else if (descriptor_count_one_payload) {
		record_unsupported("count_one_shape");
		return false;
	}

	if (op.aggregate_update.fused_payload_update_function) {
		if (!SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(scratch, op_idx, op, payload_input, payload_lanes,
		                                                           nullptr, payload_input.size())) {
			record_unsupported("state_address_payload_shape");
		} else {
			updated = TryExecuteDirectInputVectorGroupedTargetPayloadUpdate(
			    runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices, payload_lanes,
			    grouped_state, payload_scratch, finish, dense_domain);
			if (updated) {
				return true;
			}
			if (failure_reason) {
				*failure_reason = "state_address_target_payload_update";
			}
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

	auto stage_start = SljitRegionStageStart(runtime);
	updated =
	    ExecuteSljitRegionRecordedOperation(runtime, op_idx, op.kind, "direct_projected_group_payload_update",
	                                        stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		                                        return grouped_state.state->TryUpdateNewGroupsWithPayloadInput(
		                                            *groups, payload_input, payload_source_indices, sink_info,
		                                            payload_lanes, recorder, finish, nullptr, dense_domain);
	                                        });
	if (updated) {
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_projected_group_payload_update", stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "projected_group_payload_update",
		                                         payload_input.size());
	} else {
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "direct_projected_group_payload_update_miss",
		                                  stage_start);
		if (failure_reason) {
			*failure_reason = "payload_update";
		}
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
	return updated;
}

static bool SljitTryExecuteNativeInputVectorGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish,
    bool source_key0_int64_to_int32_unchecked = false,
    optional_ptr<const ExecutionDenseGroupDomain> dense_domain = nullptr,
    optional_ptr<string> failure_reason = nullptr) {
	auto &binding = SljitBindRecordedNativeSink(
	    runtime, native_runtime, scratch, op_idx, op.kind, payload_input, op.aggregate_update.plan.sink_info,
	    "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink");
	if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready ||
	    !binding.aggregate_update.grouped_state.ready || !binding.aggregate_update.grouped_state.state ||
	    !op.aggregate_update.plan.use_primitive_payloads) {
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
	auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, binding.aggregate_update.primitive);
	auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
	SljitTryReserveGroupedAggregateGroups(runtime, op_idx, op, binding.aggregate_update.grouped_state);
	if (!SljitTryExecuteInputVectorGroupedAggregateUpdate(
	        runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices, payload_lanes,
	        binding.aggregate_update.grouped_state, payload_scratch, !defer_grouped_finish,
	        source_key0_int64_to_int32_unchecked, dense_domain, failure_reason)) {
		return false;
	}
	MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	return true;
}

} // namespace duckdb
