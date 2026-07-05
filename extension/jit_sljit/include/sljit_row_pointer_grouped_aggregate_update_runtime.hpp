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
#include "sljit_grouped_aggregate_direct_update_capability_runtime.hpp"
#include "sljit_grouped_aggregate_state_address_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

#include <array>

namespace duckdb {

static bool
SljitCanAttemptRowPointerCountOneTargetLookup(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	return ExecutionRowPointerGroupKeySourcesAreRowPointerFields(group_sources);
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
	if (sink_info.aggregates.size() == 1 && count_one_payload && payload_lanes.size() == 1 && payload_lanes[0] &&
	    payload_lanes[0]->ready &&
	    (payload_lanes[0]->kind == AggregatePrimitiveUpdateKind::COUNT ||
	     payload_lanes[0]->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) &&
	    payload_lanes[0]->kind == sink_info.aggregates[0].primitive_update_kind) {
		SljitPrimitiveCountOneUpdateState update_state;
		update_state.lane = payload_lanes[0];
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
			ExecuteSljitPrimitiveCountOneTargetBatch(targets, update_state);
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

	if (op.aggregate_update.fused_payload_update_function) {
		if (!SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(scratch, op_idx, op, payload_input, payload_lanes,
		                                                           nullptr, payload_input.size())) {
			record_unsupported("state_address_payload_shape");
		} else {
			updated = TryExecuteDirectProjectedGroupedStateAddressPayloadUpdate(
			    runtime, scratch, op_idx, op, *groups, payload_input, payload_source_indices, payload_lanes,
			    grouped_state, payload_scratch, finish, nullptr, dense_domain);
			if (updated) {
				return true;
			}
			if (failure_reason) {
				*failure_reason = "state_address_payload_update";
			}
		}
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
	if (!SljitTryExecuteInputVectorGroupedAggregateUpdate(
	        runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices, payload_lanes,
	        binding.aggregate_update.grouped_state, payload_scratch, !defer_grouped_finish,
	        source_key0_int64_to_int32_unchecked, dense_domain, failure_reason)) {
		return false;
	}
	MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
	return true;
}

static bool TryExecuteDirectRowPointerGroupedTargetPayloadUpdate(
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

struct SljitStringSetComplementarySumUpdateState {
	const ExecutionPrimitiveAggregateUpdateLane *matching_lane = nullptr;
	const ExecutionPrimitiveAggregateUpdateLane *non_matching_lane = nullptr;
	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> constants;
	DataChunk *payload_input = nullptr;
};

struct SljitStringSetComplementarySumDescriptor {
	idx_t matching_payload_idx = DConstants::INVALID_INDEX;
	idx_t non_matching_payload_idx = DConstants::INVALID_INDEX;
	idx_t predicate_source_position = DConstants::INVALID_INDEX;
	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> constants;
};

static bool SljitStringSetComplementarySumLaneSupported(
    const ExecutionRegionAggregateInput &aggregate, const ExecutionRegionAggregateContract &contract,
    const ExecutionPrimitiveAggregateUpdateLane *lane, const SljitExecutableRegionExpression &payload) {
	if (!lane || !lane->ready || lane->state_size == 0 || lane->aggregate_index != aggregate.aggregate_index ||
	    aggregate.aggregate_index >= contract.grouped_state_offsets.size() ||
	    lane->state_offset != contract.grouped_state_offsets[aggregate.aggregate_index] ||
	    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
	    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset ||
	    lane->kind != aggregate.primitive_update_kind ||
	    (lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     lane->kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) ||
	    lane->payload_type != PhysicalType::INT32 || payload.plan.return_type.id() != LogicalTypeId::INTEGER ||
	    aggregate.child_count != 1 || aggregate.child_types.size() != 1 ||
	    aggregate.child_types[0].InternalType() != PhysicalType::INT32) {
		return false;
	}
	return true;
}

static bool SljitTryBindStringSetComplementarySumDescriptor(
    SljitExecutableRegionOp &op, const vector<idx_t> &payload_source_indices,
    SljitStringSetComplementarySumDescriptor &descriptor) {
	descriptor = SljitStringSetComplementarySumDescriptor();
	auto &aggregate_update = op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (sink_info.aggregates.size() != 2 || aggregate_update.payloads.size() != 2 || payload_source_indices.empty()) {
		return false;
	}

	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> matching_constants;
	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> non_matching_constants;
	for (idx_t source_position = 0; source_position < payload_source_indices.size(); source_position++) {
		if (SljitTryReadStringSetCaseExpression(aggregate_update.payloads[0], source_position, true,
		                                        matching_constants) &&
		    SljitTryReadStringSetCaseExpression(aggregate_update.payloads[1], source_position, false,
		                                        non_matching_constants)) {
			descriptor.matching_payload_idx = 0;
			descriptor.non_matching_payload_idx = 1;
			descriptor.predicate_source_position = source_position;
			break;
		}
		if (SljitTryReadStringSetCaseExpression(aggregate_update.payloads[1], source_position, true,
		                                        matching_constants) &&
		    SljitTryReadStringSetCaseExpression(aggregate_update.payloads[0], source_position, false,
		                                        non_matching_constants)) {
			descriptor.matching_payload_idx = 1;
			descriptor.non_matching_payload_idx = 0;
			descriptor.predicate_source_position = source_position;
			break;
		}
	}
	if (descriptor.predicate_source_position == DConstants::INVALID_INDEX ||
	    !SljitSameStringConstantSet(matching_constants, non_matching_constants)) {
		return false;
	}

	descriptor.predicate_source_idx = payload_source_indices[descriptor.predicate_source_position];
	descriptor.constants = matching_constants;
	return true;
}

static bool SljitStringSetComplementarySumInputIsVarchar(
    DataChunk &payload_input, const SljitStringSetComplementarySumDescriptor &descriptor) {
	return descriptor.predicate_source_idx < payload_input.ColumnCount() &&
	       payload_input.data[descriptor.predicate_source_idx].GetType().id() == LogicalTypeId::VARCHAR;
}

static bool SljitTryBindStringSetComplementarySumUpdate(
    SljitExecutableRegionOp &op, DataChunk &payload_input, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    SljitStringSetComplementarySumUpdateState &state) {
	state = SljitStringSetComplementarySumUpdateState();
	if (payload_lanes.size() != 2) {
		return false;
	}
	SljitStringSetComplementarySumDescriptor descriptor;
	if (!SljitTryBindStringSetComplementarySumDescriptor(op, payload_source_indices, descriptor) ||
	    !SljitStringSetComplementarySumInputIsVarchar(payload_input, descriptor)) {
		return false;
	}
	auto &aggregate_update = op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (!SljitStringSetComplementarySumLaneSupported(
	        sink_info.aggregates[descriptor.matching_payload_idx], sink_info.aggregate_contract,
	        payload_lanes[descriptor.matching_payload_idx], aggregate_update.payloads[descriptor.matching_payload_idx]) ||
	    !SljitStringSetComplementarySumLaneSupported(
	        sink_info.aggregates[descriptor.non_matching_payload_idx], sink_info.aggregate_contract,
	        payload_lanes[descriptor.non_matching_payload_idx],
	        aggregate_update.payloads[descriptor.non_matching_payload_idx])) {
		return false;
	}

	state.matching_lane = payload_lanes[descriptor.matching_payload_idx];
	state.non_matching_lane = payload_lanes[descriptor.non_matching_payload_idx];
	state.predicate_source_idx = descriptor.predicate_source_idx;
	state.constants = descriptor.constants;
	state.payload_input = &payload_input;
	return true;
}

static void SljitApplyStringSetComplementarySumLane(data_ptr_t state_address,
                                                    const ExecutionPrimitiveAggregateUpdateLane &lane,
                                                    bool increment) {
	auto state_base = state_address + lane.state_offset;
	if (lane.kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
		auto sum = reinterpret_cast<int64_t *>(state_base + lane.state_value_offset);
		if (increment) {
			*sum += 1;
		}
	} else if (lane.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		auto sum = reinterpret_cast<hugeint_t *>(state_base + lane.state_value_offset);
		if (increment) {
			*sum += 1;
		}
	} else {
		throw InternalException("SLJIT string-set complementary sum lane has unsupported state kind");
	}
	if (AggregatePrimitiveUpdateHasStateIsSet(lane.kind)) {
		auto state_is_set = reinterpret_cast<bool *>(state_base + lane.state_is_set_offset);
		*state_is_set = true;
	}
}

static void SljitExecuteStringSetComplementarySumSpan(const ExecutionGroupedAggregateStateTargetSpan &span,
                                                      SljitStringSetComplementarySumUpdateState &state,
                                                      UnifiedVectorFormat &predicate_format) {
	if (!span.HasTargets()) {
		return;
	}
	auto predicate_data = UnifiedVectorFormat::GetData<string_t>(predicate_format);
	auto predicate_sel = predicate_format.sel;
	auto &predicate_validity = predicate_format.validity;
	for (idx_t idx = 0; idx < span.count; idx++) {
		const auto row_idx = span.row_sel ? span.row_sel[idx] : idx;
		const auto address_idx = SljitSelectedGroupedStateAddressIndex(span.address_sel, span.row_sel, idx, row_idx);
		auto state_address = reinterpret_cast<data_ptr_t>(span.addresses[address_idx]);
		bool matches = false;
		const auto predicate_idx = predicate_sel->get_index(row_idx);
		if (predicate_validity.RowIsValid(predicate_idx)) {
			auto predicate = predicate_data[predicate_idx];
			matches = SljitStringEqualsConstant(predicate, state.constants[0]) ||
			          SljitStringEqualsConstant(predicate, state.constants[1]);
		}
		SljitApplyStringSetComplementarySumLane(state_address, *state.matching_lane, matches);
		SljitApplyStringSetComplementarySumLane(state_address, *state.non_matching_lane,
		                                        predicate_validity.RowIsValid(predicate_idx) && !matches);
	}
}

static void SljitExecutePreclassifiedStringSetComplementarySumSpan(
    const ExecutionGroupedAggregateStateTargetSpan &span, SljitStringSetComplementarySumUpdateState &state,
    UnifiedVectorFormat &predicate_format) {
	D_ASSERT(state.matching_lane);
	D_ASSERT(state.non_matching_lane);
	auto predicate_data = UnifiedVectorFormat::GetData<uint8_t>(predicate_format);
	auto predicate_sel = predicate_format.sel;
	auto &predicate_validity = predicate_format.validity;
	for (idx_t idx = 0; idx < span.count; idx++) {
		const auto row_idx = span.row_sel ? span.row_sel[idx] : idx;
		const auto address_idx = SljitSelectedGroupedStateAddressIndex(span.address_sel, span.row_sel, idx, row_idx);
		const auto predicate_idx = predicate_sel->get_index(row_idx);
		if (!predicate_validity.RowIsValid(predicate_idx)) {
			continue;
		}
		auto state_address = reinterpret_cast<data_ptr_t>(span.addresses[address_idx]);
		const bool matches = predicate_data[predicate_idx] != 0;
		SljitApplyStringSetComplementarySumLane(state_address, *state.matching_lane, matches);
		SljitApplyStringSetComplementarySumLane(state_address, *state.non_matching_lane, !matches);
	}
}

static bool TryExecuteDirectRowPointerStringSetComplementarySumUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
	SljitStringSetComplementarySumUpdateState state;
	if (!SljitTryBindStringSetComplementarySumUpdate(op, payload_input, payload_source_indices, payload_lanes, state)) {
		return false;
	}
	ExecutionGroupedAggregateStateTargetBatch targets;
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_row_pointer_string_set_complementary_sum_lookup", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        payload_input, row_pointers, count, group_sources, op.aggregate_update.plan.sink_info, targets,
		        recorder);
	    });
	if (!updated) {
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "direct_row_pointer_string_set_complementary_sum_lookup_miss",
		                                  stage_start);
		return false;
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "direct_row_pointer_string_set_complementary_sum_lookup", stage_start);
	auto update_start = SljitRegionStageStart(runtime);
	UnifiedVectorFormat predicate_format;
	payload_input.data[state.predicate_source_idx].ToUnifiedFormat(predicate_format);
	for (auto &span : targets.Spans()) {
		SljitExecuteStringSetComplementarySumSpan(span, state, predicate_format);
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_row_pointer_string_set_complementary_sum_update",
	                              update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionRuntimePath(runtime, op.kind, "direct_row_pointer_string_set_complementary_sum_update", count);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_string_set_complementary_sum_update", count);
	return true;
}

static bool TryExecuteDirectRowPointerPreclassifiedStringSetComplementarySumUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
	SljitStringSetComplementarySumDescriptor descriptor;
	SljitStringSetComplementarySumUpdateState state;
	state = SljitStringSetComplementarySumUpdateState();
	if (payload_lanes.size() != 2 || payload_input.ColumnCount() != 1 ||
	    payload_input.data[0].GetType().id() != LogicalTypeId::UTINYINT ||
	    !SljitTryBindStringSetComplementarySumDescriptor(op, payload_source_indices, descriptor)) {
		return false;
	}
	auto &aggregate_update = op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (!SljitStringSetComplementarySumLaneSupported(
	        sink_info.aggregates[descriptor.matching_payload_idx], sink_info.aggregate_contract,
	        payload_lanes[descriptor.matching_payload_idx], aggregate_update.payloads[descriptor.matching_payload_idx]) ||
	    !SljitStringSetComplementarySumLaneSupported(
	        sink_info.aggregates[descriptor.non_matching_payload_idx], sink_info.aggregate_contract,
	        payload_lanes[descriptor.non_matching_payload_idx],
	        aggregate_update.payloads[descriptor.non_matching_payload_idx])) {
		return false;
	}
	state.matching_lane = payload_lanes[descriptor.matching_payload_idx];
	state.non_matching_lane = payload_lanes[descriptor.non_matching_payload_idx];
	state.predicate_source_idx = 0;
	ExecutionGroupedAggregateStateTargetBatch targets;
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "direct_row_pointer_preclassified_string_set_complementary_sum_lookup", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        payload_input, row_pointers, count, group_sources, op.aggregate_update.plan.sink_info, targets,
		        recorder);
	    });
	if (!updated) {
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(
		    runtime, op_idx, op.kind, "direct_row_pointer_preclassified_string_set_complementary_sum_lookup_miss",
		    stage_start);
		return false;
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "direct_row_pointer_preclassified_string_set_complementary_sum_lookup",
	                                  stage_start);
	auto update_start = SljitRegionStageStart(runtime);
	UnifiedVectorFormat predicate_format;
	payload_input.data[0].ToUnifiedFormat(predicate_format);
	for (auto &span : targets.Spans()) {
		SljitExecutePreclassifiedStringSetComplementarySumSpan(span, state, predicate_format);
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
	                              "direct_row_pointer_preclassified_string_set_complementary_sum_update",
	                              update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionRuntimePath(runtime, op.kind,
	                              "direct_row_pointer_preclassified_string_set_complementary_sum_update", count);
	RecordSljitRegionMaterializationBoundary(
	    runtime, op.kind, "row_pointer_preclassified_string_set_complementary_sum_update", count);
	return true;
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
	auto preaggregate_stage_start = SljitRegionStageStart(runtime);
	const bool preaggregated =
	    uses_generated_payload_preaggregation
	        ? SljitTryPreaggregateRowPointerFusedPrimitiveGroups(
	              op, payload_input, row_pointers, group_sources, payload_source_indices, payload_lanes,
	              compact_row_pointers, preaggregate_scratch, payload_scratch, compact_count)
	        : SljitTryPreaggregateRowPointerPrimitiveGroups(payload_input, row_pointers, group_sources,
	                                                        payload_source_indices, payload_lanes, compact_row_pointers,
	                                                        preaggregate_scratch, compact_count);
	if (!preaggregated) {
		scratch.RecordRowPointerPreaggregateResult(op_idx, false);
		RecordSljitRegionRuntimePath(runtime, op.kind, "direct_row_pointer_preaggregated_groups_miss", count);
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
	const char *stage_name = "direct_row_pointer_preaggregated_grouped_primitive_update";
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, stage_name, stage_start, [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        compact_lookup_input, compact_row_pointers, compact_count, group_sources,
		        op.aggregate_update.plan.sink_info, targets, recorder);
	    });
	if (!updated) {
		scratch.RecordRowPointerPreaggregateResult(op_idx, false);
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

static bool SljitTryExecuteRowPointerCountOneGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
	auto &sink_info = op.aggregate_update.plan.sink_info;
	if (sink_info.aggregates.size() != 1 ||
	    !SljitInputVectorCountPayloadIsCountOne(payload_input, payload_source_indices) || payload_lanes.size() != 1 ||
	    !payload_lanes[0] || !payload_lanes[0]->ready ||
	    (payload_lanes[0]->kind != AggregatePrimitiveUpdateKind::COUNT &&
	     payload_lanes[0]->kind != AggregatePrimitiveUpdateKind::COUNT_STAR) ||
	    payload_lanes[0]->kind != sink_info.aggregates[0].primitive_update_kind) {
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

	SljitPrimitiveCountOneUpdateState update_state;
	update_state.lane = payload_lanes[0];
	auto update_start = SljitRegionStageStart(runtime);
	ExecuteSljitPrimitiveCountOneTargetBatch(targets, update_state);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_row_pointer_group_count_one_update", update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionRuntimePath(runtime, op.kind, "direct_row_pointer_group_count_one_update", count);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_group_count_one_update", count);
	return true;
}

enum class SljitRowPointerGroupedAggregateUpdateStrategy : uint8_t {
	PREAGGREGATED_PRIMITIVE_GROUPS,
	COUNT_ONE_TARGETS,
	INPUT_VECTOR_GROUPS,
	TARGET_PAYLOAD_UPDATE,
	SPLIT_PAYLOAD_UPDATE
};

static constexpr idx_t SLJIT_ROW_POINTER_GROUPED_AGGREGATE_UPDATE_STRATEGY_COUNT = 5;

struct SljitRowPointerGroupedAggregateUpdateDecision {
	std::array<SljitRowPointerGroupedAggregateUpdateStrategy, SLJIT_ROW_POINTER_GROUPED_AGGREGATE_UPDATE_STRATEGY_COUNT>
	    strategies;
	idx_t strategy_count = 0;
	bool uses_generated_payload_preaggregation = false;
	bool prefer_sparse_row_pointer_target_update = false;

	void Add(SljitRowPointerGroupedAggregateUpdateStrategy strategy) {
		if (strategy_count >= strategies.size()) {
			throw InternalException("SLJIT row-pointer grouped aggregate update strategy list overflow");
		}
		strategies[strategy_count++] = strategy;
	}
};

static SljitRowPointerGroupedAggregateUpdateDecision SljitChooseRowPointerGroupedAggregateUpdateStrategies(
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input,
    Vector &row_pointers, idx_t count, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
	SljitRowPointerGroupedAggregateUpdateDecision decision;
	const bool can_execute_direct_grouped_state_address_payload_update =
	    SljitCanExecuteDirectGroupedStateAddressPayloadUpdate(scratch, op_idx, op, payload_input, payload_lanes,
	                                                          nullptr, count);
	decision.prefer_sparse_row_pointer_target_update =
	    can_execute_direct_grouped_state_address_payload_update &&
	    SljitShouldPreferDirectSparseRowPointerTargetUpdate(row_pointers, count, group_sources);

	if (!decision.prefer_sparse_row_pointer_target_update && !scratch.RowPointerPreaggregateDisabled(op_idx) &&
	    SljitCanExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
	        scratch, op_idx, op, payload_input, row_pointers, group_sources, payload_source_indices, payload_lanes,
	        count, decision.uses_generated_payload_preaggregation)) {
		decision.Add(SljitRowPointerGroupedAggregateUpdateStrategy::PREAGGREGATED_PRIMITIVE_GROUPS);
	}
	if (SljitCanAttemptRowPointerCountOneTargetLookup(group_sources)) {
		decision.Add(SljitRowPointerGroupedAggregateUpdateStrategy::COUNT_ONE_TARGETS);
	}
	if (SljitGroupSourcesCanMaterializeFromInputVectors(payload_input, group_sources)) {
		decision.Add(SljitRowPointerGroupedAggregateUpdateStrategy::INPUT_VECTOR_GROUPS);
	}
	decision.Add(can_execute_direct_grouped_state_address_payload_update
	                 ? SljitRowPointerGroupedAggregateUpdateStrategy::TARGET_PAYLOAD_UPDATE
	                 : SljitRowPointerGroupedAggregateUpdateStrategy::SPLIT_PAYLOAD_UPDATE);
	return decision;
}

static bool SljitTryExecuteRowPointerGroupedAggregateUpdateStrategy(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, SljitAggregatePayloadAdapterScratch &payload_scratch,
    bool finish, bool source_key0_int64_to_int32_unchecked,
    const SljitRowPointerGroupedAggregateUpdateDecision &decision,
    SljitRowPointerGroupedAggregateUpdateStrategy strategy) {
	switch (strategy) {
	case SljitRowPointerGroupedAggregateUpdateStrategy::PREAGGREGATED_PRIMITIVE_GROUPS:
		return TryExecuteDirectRowPointerPreaggregatedPrimitiveUpdate(
		    runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_source_indices,
		    payload_lanes, grouped_state, payload_scratch, decision.uses_generated_payload_preaggregation, finish);
	case SljitRowPointerGroupedAggregateUpdateStrategy::COUNT_ONE_TARGETS:
		return SljitTryExecuteRowPointerCountOneGroupedAggregateUpdate(
		    runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_source_indices,
		    payload_lanes, grouped_state, finish);
	case SljitRowPointerGroupedAggregateUpdateStrategy::INPUT_VECTOR_GROUPS:
		return SljitTryExecuteInputVectorGroupedAggregateUpdate(
		    runtime, scratch, op_idx, op, payload_input, group_sources, payload_source_indices, payload_lanes,
		    grouped_state, payload_scratch, finish, source_key0_int64_to_int32_unchecked);
	case SljitRowPointerGroupedAggregateUpdateStrategy::TARGET_PAYLOAD_UPDATE:
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
		return TryExecuteDirectRowPointerGroupedTargetPayloadUpdate(
		    runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_source_indices,
		    payload_lanes, grouped_state, payload_scratch, finish);
	case SljitRowPointerGroupedAggregateUpdateStrategy::SPLIT_PAYLOAD_UPDATE: {
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
		if (updated) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_grouped_lookup_update", count);
		}
		return updated;
	}
	default:
		throw InternalException("Unknown SLJIT row-pointer grouped aggregate update strategy");
	}
}

static bool SljitTryExecuteNativeRowPointerGroupedAggregateUpdate(
    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input, Vector &row_pointers,
	const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
	bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish,
	bool source_key0_int64_to_int32_unchecked = false) {
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
	auto proven_group_sources = group_sources;
	SljitApplyInputVectorGroupBatchCastProofs(payload_input, proven_group_sources, count);
	SljitApplyRowPointerGroupBatchCastProofs(row_pointers, proven_group_sources, count);
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
	auto decision = SljitChooseRowPointerGroupedAggregateUpdateStrategies(scratch, op_idx, op, payload_input,
	                                                                      row_pointers, count, proven_group_sources,
	                                                                      payload_source_indices, payload_lanes);
	if (decision.prefer_sparse_row_pointer_target_update) {
		RecordSljitRegionRuntimePath(runtime, op.kind, "direct_row_pointer_sparse_target_update_preferred", count);
	}

	for (idx_t strategy_idx = 0; strategy_idx < decision.strategy_count; strategy_idx++) {
		auto strategy = decision.strategies[strategy_idx];
		const bool updated = SljitTryExecuteRowPointerGroupedAggregateUpdateStrategy(
		    runtime, scratch, op_idx, op, payload_input, row_pointers, count, proven_group_sources,
		    payload_source_indices, payload_lanes, grouped_state, payload_scratch, finish,
		    source_key0_int64_to_int32_unchecked, decision, strategy);
		if (updated) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		if (strategy == SljitRowPointerGroupedAggregateUpdateStrategy::TARGET_PAYLOAD_UPDATE) {
			record_unsupported("grouped_lookup_update");
			return false;
		}
	}
	record_unsupported("row_pointer_payload_update");
	return false;
}

} // namespace duckdb
