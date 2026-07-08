//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_string_set_complementary_sum_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

#include <array>

namespace duckdb {

struct SljitStringSetComplementarySumUpdateState {
	const ExecutionPrimitiveAggregateUpdateLane *matching_lane = nullptr;
	const ExecutionPrimitiveAggregateUpdateLane *non_matching_lane = nullptr;
	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> constants;
};

struct SljitStringSetComplementarySumDescriptor {
	idx_t matching_payload_idx = DConstants::INVALID_INDEX;
	idx_t non_matching_payload_idx = DConstants::INVALID_INDEX;
	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> constants;
};

static bool SljitStringSetComplementarySumLaneSupported(const ExecutionRegionAggregateInput &aggregate,
                                                        const ExecutionRegionAggregateContract &contract,
                                                        const ExecutionPrimitiveAggregateUpdateLane *lane,
                                                        const SljitExecutableRegionExpression &payload) {
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

static bool SljitTryBindStringSetComplementarySumDescriptor(SljitExecutableRegionOp &op,
                                                            const vector<idx_t> &payload_source_indices,
                                                            SljitStringSetComplementarySumDescriptor &descriptor) {
	descriptor = SljitStringSetComplementarySumDescriptor();
	auto &aggregate_update = op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (sink_info.aggregates.size() != 2 || aggregate_update.payloads.size() != 2 || payload_source_indices.empty()) {
		return false;
	}

	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> matching_constants;
	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> non_matching_constants;
	idx_t predicate_source_position = DConstants::INVALID_INDEX;
	for (idx_t source_position = 0; source_position < payload_source_indices.size(); source_position++) {
		if (SljitTryReadStringSetCaseExpression(aggregate_update.payloads[0], source_position, true,
		                                        matching_constants) &&
		    SljitTryReadStringSetCaseExpression(aggregate_update.payloads[1], source_position, false,
		                                        non_matching_constants)) {
			descriptor.matching_payload_idx = 0;
			descriptor.non_matching_payload_idx = 1;
			predicate_source_position = source_position;
			break;
		}
		if (SljitTryReadStringSetCaseExpression(aggregate_update.payloads[1], source_position, true,
		                                        matching_constants) &&
		    SljitTryReadStringSetCaseExpression(aggregate_update.payloads[0], source_position, false,
		                                        non_matching_constants)) {
			descriptor.matching_payload_idx = 1;
			descriptor.non_matching_payload_idx = 0;
			predicate_source_position = source_position;
			break;
		}
	}
	if (predicate_source_position == DConstants::INVALID_INDEX ||
	    !SljitSameStringConstantSet(matching_constants, non_matching_constants)) {
		return false;
	}

	descriptor.predicate_source_idx = payload_source_indices[predicate_source_position];
	descriptor.constants = matching_constants;
	return true;
}

static bool SljitStringSetComplementarySumInputIsVarchar(DataChunk &payload_input,
                                                         const SljitStringSetComplementarySumDescriptor &descriptor) {
	return descriptor.predicate_source_idx < payload_input.ColumnCount() &&
	       payload_input.data[descriptor.predicate_source_idx].GetType().id() == LogicalTypeId::VARCHAR;
}

static bool SljitTryBindStringSetComplementarySumLanes(
    SljitExecutableRegionOp &op, const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const SljitStringSetComplementarySumDescriptor &descriptor, SljitStringSetComplementarySumUpdateState &state) {
	if (payload_lanes.size() != 2) {
		return false;
	}
	auto &aggregate_update = op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (!SljitStringSetComplementarySumLaneSupported(sink_info.aggregates[descriptor.matching_payload_idx],
	                                                 sink_info.aggregate_contract,
	                                                 payload_lanes[descriptor.matching_payload_idx],
	                                                 aggregate_update.payloads[descriptor.matching_payload_idx]) ||
	    !SljitStringSetComplementarySumLaneSupported(sink_info.aggregates[descriptor.non_matching_payload_idx],
	                                                 sink_info.aggregate_contract,
	                                                 payload_lanes[descriptor.non_matching_payload_idx],
	                                                 aggregate_update.payloads[descriptor.non_matching_payload_idx])) {
		return false;
	}

	state.matching_lane = payload_lanes[descriptor.matching_payload_idx];
	state.non_matching_lane = payload_lanes[descriptor.non_matching_payload_idx];
	return true;
}

static bool
SljitTryBindStringSetComplementarySumUpdate(SljitExecutableRegionOp &op, DataChunk &payload_input,
                                            const vector<idx_t> &payload_source_indices,
                                            const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
                                            SljitStringSetComplementarySumUpdateState &state) {
	state = SljitStringSetComplementarySumUpdateState();
	SljitStringSetComplementarySumDescriptor descriptor;
	if (!SljitTryBindStringSetComplementarySumDescriptor(op, payload_source_indices, descriptor) ||
	    !SljitStringSetComplementarySumInputIsVarchar(payload_input, descriptor) ||
	    !SljitTryBindStringSetComplementarySumLanes(op, payload_lanes, descriptor, state)) {
		return false;
	}
	state.predicate_source_idx = descriptor.predicate_source_idx;
	state.constants = descriptor.constants;
	return true;
}

static void SljitApplyStringSetComplementarySumLane(data_ptr_t state_address,
                                                    const ExecutionPrimitiveAggregateUpdateLane &lane, bool increment) {
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

static void SljitExecutePreclassifiedStringSetComplementarySumSpan(const ExecutionGroupedAggregateStateTargetSpan &span,
                                                                   SljitStringSetComplementarySumUpdateState &state,
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
		                                  "direct_row_pointer_string_set_complementary_sum_lookup_miss", stage_start);
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
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind,
	                                            "direct_row_pointer_string_set_complementary_sum_update", count);
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
	if (payload_input.ColumnCount() != 1 || payload_input.data[0].GetType().id() != LogicalTypeId::UTINYINT ||
	    !SljitTryBindStringSetComplementarySumDescriptor(op, payload_source_indices, descriptor)) {
		return false;
	}
	if (!SljitTryBindStringSetComplementarySumLanes(op, payload_lanes, descriptor, state)) {
		return false;
	}
	state.predicate_source_idx = 0;
	ExecutionGroupedAggregateStateTargetBatch targets;
	auto stage_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "row_pointer_preclassified_string_set_complementary_sum_lookup", stage_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        payload_input, row_pointers, count, group_sources, op.aggregate_update.plan.sink_info, targets,
		        recorder);
	    });
	if (!updated) {
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "row_pointer_preclassified_string_set_complementary_sum_lookup_miss",
		                                  stage_start);
		return false;
	}
	RecordSljitRegionStageRuntimePath(
	    runtime, op_idx, op.kind, "row_pointer_preclassified_string_set_complementary_sum_lookup", stage_start);
	auto update_start = SljitRegionStageStart(runtime);
	UnifiedVectorFormat predicate_format;
	payload_input.data[0].ToUnifiedFormat(predicate_format);
	for (auto &span : targets.Spans()) {
		SljitExecutePreclassifiedStringSetComplementarySumSpan(span, state, predicate_format);
	}
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
	                              "row_pointer_preclassified_string_set_complementary_sum_update", update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionMaterializationElisionPath(
	    runtime, op.kind, "row_pointer_preclassified_string_set_complementary_sum_update", count);
	return true;
}

} // namespace duckdb
