//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_string_set_complementary_sum_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregated_update_runtime.hpp"
#include "sljit_aggregate_row_pointer_preaggregation.hpp"
#include "sljit_grouped_aggregate_state_runtime.hpp"
#include "sljit_grouped_reduction_lane.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

#include "duckdb/function/scalar/string_common.hpp"

#include <array>

namespace duckdb {

struct SljitStringSetComplementarySumUpdateState {
	const ExecutionPrimitiveAggregateUpdateLane *matching_lane = nullptr;
	const ExecutionPrimitiveAggregateUpdateLane *non_matching_lane = nullptr;
	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> constants;
	std::array<SljitStringConstantSignature, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> signatures;
};

struct SljitStringSetComplementarySumDescriptor {
	idx_t matching_payload_idx = DConstants::INVALID_INDEX;
	idx_t non_matching_payload_idx = DConstants::INVALID_INDEX;
	idx_t predicate_source_idx = DConstants::INVALID_INDEX;
	std::array<string, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> constants;
	std::array<SljitStringConstantSignature, SljitStringSetCaseGroupedPayloadProjection::CONSTANT_COUNT> signatures;
};

static bool SljitStringSetComplementarySumLaneSupported(const SljitAggregatePayloadDescriptor &descriptor,
                                                        const ExecutionRegionAggregateContract &contract,
                                                        const ExecutionPrimitiveAggregateUpdateLane *lane,
                                                        const SljitExecutableRegionExpression &payload) {
	SljitGroupedReductionLaneBinding reduction_lane;
	if (!SljitTryBindGroupedReductionLane(contract, descriptor, lane, reduction_lane) ||
	    (descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) ||
	    descriptor.input_type != PhysicalType::INT32 || payload.plan.return_type.id() != LogicalTypeId::INTEGER) {
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
	for (idx_t constant_idx = 0; constant_idx < descriptor.signatures.size(); constant_idx++) {
		descriptor.signatures[constant_idx] = SljitPrepareStringConstantSignature(descriptor.constants[constant_idx]);
	}
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
	if (payload_lanes.size() != 2 || op.aggregate_update.payload_descriptors.size() != 2) {
		return false;
	}
	auto &aggregate_update = op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	if (!SljitStringSetComplementarySumLaneSupported(
	        aggregate_update.payload_descriptors[descriptor.matching_payload_idx], sink_info.aggregate_contract,
	        payload_lanes[descriptor.matching_payload_idx],
	        aggregate_update.payloads[descriptor.matching_payload_idx]) ||
	    !SljitStringSetComplementarySumLaneSupported(
	        aggregate_update.payload_descriptors[descriptor.non_matching_payload_idx], sink_info.aggregate_contract,
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
	state.signatures = descriptor.signatures;
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
			matches = SljitStringEqualsConstant(predicate, state.constants[0], state.signatures[0]) ||
			          SljitStringEqualsConstant(predicate, state.constants[1], state.signatures[1]);
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
		auto state_address = reinterpret_cast<data_ptr_t>(span.addresses[address_idx]);
		const bool predicate_is_valid = predicate_validity.RowIsValid(predicate_idx);
		const bool matches = predicate_is_valid && predicate_data[predicate_idx] != 0;
		SljitApplyStringSetComplementarySumLane(state_address, *state.matching_lane, matches);
		SljitApplyStringSetComplementarySumLane(state_address, *state.non_matching_lane,
		                                        predicate_is_valid && !matches);
	}
}

template <class T>
struct SljitTypedRowPointerPreaggregationEntry {
	bool occupied = false;
	bool valid = false;
	T key {};
	idx_t group_idx = DConstants::INVALID_INDEX;
};

template <class T>
static bool SljitTryPreaggregateTypedRowPointerComplementarySums(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const SljitStringSetComplementarySumDescriptor &descriptor, Vector &compact_row_pointers,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count) {
	compact_count = 0;
	if (count < 2 || group_sources.size() != 1 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	auto &source = group_sources[0];
	if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD ||
	    source.row_layout_offset == DConstants::INVALID_INDEX || source.target_physical_type != GetTypeId<T>()) {
		return false;
	}
	const bool load_fixed_key = source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE &&
	                            source.source_physical_type == source.target_physical_type;
	const bool compress_string_key = source.cast_kind == ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS &&
	                                 source.source_physical_type == PhysicalType::VARCHAR;
	if (!load_fixed_key && !compress_string_key) {
		return false;
	}

	preaggregate_scratch.Prepare(payload_lanes, count);
	compact_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(compact_row_pointers, count_t(count));
	auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(compact_row_pointers);
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);

	UnifiedVectorFormat predicate_format;
	payload_input.data[0].ToUnifiedFormat(predicate_format);
	auto predicate_data = UnifiedVectorFormat::GetData<uint8_t>(predicate_format);
	auto predicate_sel = predicate_format.sel;
	auto &predicate_validity = predicate_format.validity;

	static constexpr idx_t CACHE_CAPACITY = SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS * 2;
	static constexpr idx_t HOT_KEY_CAPACITY = 8;
	std::array<SljitTypedRowPointerPreaggregationEntry<T>, CACHE_CAPACITY> entries {};
	std::array<T, SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS> group_keys {};
	std::array<bool, SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS> group_validity {};
	std::array<int64_t, SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS> matching_counts {};
	std::array<int64_t, SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS> valid_counts {};
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		bool source_is_valid;
		if (!SljitRowPointerGroupKeySourceValueIsValid(row_pointer, source, source_is_valid)) {
			return false;
		}
		T key {};
		if (source_is_valid) {
			if (load_fixed_key) {
				key = Load<T>(row_pointer + source.row_layout_offset);
			} else if (!TryStringCompressValue(Load<string_t>(row_pointer + source.row_layout_offset), key)) {
				return false;
			}
		}
		idx_t group_idx = DConstants::INVALID_INDEX;
		const auto hot_key_count = MinValue<idx_t>(compact_count, HOT_KEY_CAPACITY);
		for (idx_t hot_idx = 0; hot_idx < hot_key_count; hot_idx++) {
			if (group_validity[hot_idx] == source_is_valid && (!source_is_valid || group_keys[hot_idx] == key)) {
				group_idx = hot_idx;
				break;
			}
		}
		if (group_idx != DConstants::INVALID_INDEX) {
			const auto predicate_idx = predicate_sel->get_index(row_idx);
			if (predicate_validity.RowIsValid(predicate_idx)) {
				matching_counts[group_idx] += predicate_data[predicate_idx] != 0 ? 1 : 0;
				valid_counts[group_idx]++;
			}
			continue;
		}

		const auto hash = source_is_valid ? Hash(key) : hash_t(0);
		auto entry_idx = static_cast<idx_t>(hash) & (CACHE_CAPACITY - 1);
		for (idx_t probe_idx = 0; probe_idx < CACHE_CAPACITY; probe_idx++) {
			auto &entry = entries[entry_idx];
			if (!entry.occupied) {
				if (compact_count == SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS) {
					return false;
				}
				entry.occupied = true;
				entry.valid = source_is_valid;
				entry.key = key;
				entry.group_idx = compact_count;
				group_idx = compact_count++;
				group_keys[group_idx] = key;
				group_validity[group_idx] = source_is_valid;
				compact_row_pointer_data[group_idx] = row_pointer;
				preaggregate_scratch.group_rows.push_back(static_cast<sel_t>(row_idx));
				if (!SljitStartPreaggregatedPrimitivePayloadGroup(preaggregate_scratch, payload_lanes)) {
					return false;
				}
				break;
			}
			if (entry.valid == source_is_valid && (!source_is_valid || entry.key == key)) {
				group_idx = entry.group_idx;
				break;
			}
			entry_idx = (entry_idx + 1) & (CACHE_CAPACITY - 1);
		}
		if (group_idx == DConstants::INVALID_INDEX) {
			return false;
		}
		const auto predicate_idx = predicate_sel->get_index(row_idx);
		if (predicate_validity.RowIsValid(predicate_idx)) {
			matching_counts[group_idx] += predicate_data[predicate_idx] != 0 ? 1 : 0;
			valid_counts[group_idx]++;
		}
	}
	if (compact_count == count) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < preaggregate_scratch.payloads.size(); payload_idx++) {
		auto &payload = preaggregate_scratch.payloads[payload_idx];
		const bool matching_payload = payload_idx == descriptor.matching_payload_idx;
		for (idx_t group_idx = 0; group_idx < compact_count; group_idx++) {
			const auto delta =
			    matching_payload ? matching_counts[group_idx] : valid_counts[group_idx] - matching_counts[group_idx];
			switch (payload.kind) {
			case AggregatePrimitiveUpdateKind::SUM_INT64:
				payload.int64_values[group_idx] = delta;
				break;
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
				payload.hugeint_values[group_idx] = hugeint_t(delta);
				break;
			default:
				return false;
			}
			// CASE ... ELSE 0 contributes a non-null zero for null predicates.
			payload.value_is_set[group_idx] = 1;
		}
	}
	FlatVector::SetSize(compact_row_pointers, count_t(compact_count));
	return true;
}

static bool SljitTryPreaggregateTypedRowPointerComplementarySums(
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const SljitStringSetComplementarySumDescriptor &descriptor, Vector &compact_row_pointers,
    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch, idx_t &compact_count) {
	if (group_sources.size() != 1) {
		return false;
	}
	switch (group_sources[0].target_physical_type) {
	case PhysicalType::UINT8:
		return SljitTryPreaggregateTypedRowPointerComplementarySums<uint8_t>(
		    payload_input, row_pointers, count, group_sources, payload_lanes, descriptor, compact_row_pointers,
		    preaggregate_scratch, compact_count);
	case PhysicalType::UINT16:
		return SljitTryPreaggregateTypedRowPointerComplementarySums<uint16_t>(
		    payload_input, row_pointers, count, group_sources, payload_lanes, descriptor, compact_row_pointers,
		    preaggregate_scratch, compact_count);
	case PhysicalType::UINT32:
		return SljitTryPreaggregateTypedRowPointerComplementarySums<uint32_t>(
		    payload_input, row_pointers, count, group_sources, payload_lanes, descriptor, compact_row_pointers,
		    preaggregate_scratch, compact_count);
	case PhysicalType::UINT64:
		return SljitTryPreaggregateTypedRowPointerComplementarySums<uint64_t>(
		    payload_input, row_pointers, count, group_sources, payload_lanes, descriptor, compact_row_pointers,
		    preaggregate_scratch, compact_count);
	default:
		return false;
	}
}

static bool TryExecutePreaggregatedRowPointerPreclassifiedStringSetComplementarySumUpdate(
    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
    DataChunk &payload_input, Vector &row_pointers, idx_t count,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
    const SljitStringSetComplementarySumDescriptor &descriptor,
    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish) {
	if (scratch.RowPointerPreaggregateDisabled(op_idx)) {
		return false;
	}
	auto &compact_row_pointers = scratch.AggregatePreaggregatedRowPointers(op_idx);
	auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
	idx_t compact_count = 0;
	auto preaggregate_start = SljitRegionStageStart(runtime);
	if (!SljitTryPreaggregateTypedRowPointerComplementarySums(payload_input, row_pointers, count, group_sources,
	                                                          payload_lanes, descriptor, compact_row_pointers,
	                                                          preaggregate_scratch, compact_count)) {
		scratch.RecordRowPointerPreaggregateResult(op_idx, false);
		return false;
	}
	scratch.RecordRowPointerPreaggregateResult(op_idx, true);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
	                              "local_preaggregate_preclassified_string_set_complementary_sum", preaggregate_start);

	DataChunk compact_lookup_input;
	vector<LogicalType> empty_types;
	compact_lookup_input.InitializeEmpty(empty_types);
	compact_lookup_input.SetChildCardinality(compact_count);

	ExecutionGroupedAggregateStateTargetBatch targets;
	auto lookup_start = SljitRegionStageStart(runtime);
	auto updated = ExecuteSljitRegionRecordedOperation(
	    runtime, op_idx, op.kind, "row_pointer_preaggregated_string_set_complementary_sum_lookup", lookup_start,
	    [&](optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		    return grouped_state.state->TryFindOrCreateRowPointerGroupStateTargets(
		        compact_lookup_input, compact_row_pointers, compact_count, group_sources,
		        op.aggregate_update.plan.sink_info, targets, recorder);
	    });
	if (!updated) {
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, false);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  "row_pointer_preaggregated_string_set_complementary_sum_lookup_miss",
		                                  lookup_start);
		return false;
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "row_pointer_preaggregated_string_set_complementary_sum_lookup", lookup_start);

	auto update_start = SljitRegionStageStart(runtime);
	SljitPreaggregatedPrimitiveUpdateState update_state;
	update_state.lanes = &payload_lanes;
	update_state.payloads = &preaggregate_scratch.payloads;
	ExecuteSljitPreaggregatedPrimitiveTargetBatch(targets, update_state);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
	                              "row_pointer_preaggregated_string_set_complementary_sum_update", update_start);
	if (finish) {
		FinishGroupedAggregateStateUpdates(runtime, op_idx, grouped_state, "finish_grouped_state_updates");
	}
	scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind,
	                                            "row_pointer_preaggregated_string_set_complementary_sum_update", count);
	return true;
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
	if (TryExecutePreaggregatedRowPointerPreclassifiedStringSetComplementarySumUpdate(
	        runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources, payload_lanes, descriptor,
	        grouped_state, finish)) {
		return true;
	}
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
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
	                                  "row_pointer_preclassified_string_set_complementary_sum_lookup", stage_start);
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
	RecordSljitRegionMaterializationElisionPath(runtime, op.kind,
	                                            "row_pointer_preclassified_string_set_complementary_sum_update", count);
	return true;
}

} // namespace duckdb
