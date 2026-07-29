//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_primitive_preaggregation_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

namespace duckdb {

struct SljitPreaggregatedPrimitivePayloadSource {
	PhysicalType type = PhysicalType::INVALID;
	UnifiedVectorFormat format;
	bool rows_all_valid = false;
};

static bool SljitPreaggregatedFormatRowsAllValid(const UnifiedVectorFormat &format, idx_t count) {
	if (format.validity.CannotHaveNull()) {
		return true;
	}
	if (!format.sel->IsSet()) {
		return format.validity.CheckAllValid(count);
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!format.validity.RowIsValid(format.sel->get_index(row_idx))) {
			return false;
		}
	}
	return true;
}

static bool SljitPreaggregatedPrimitiveIntegerTypeSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		return true;
	default:
		return false;
	}
}

static bool SljitPreaggregatedPrimitivePayloadSupported(AggregatePrimitiveUpdateKind kind, PhysicalType type) {
	switch (kind) {
	case AggregatePrimitiveUpdateKind::COUNT_STAR:
	case AggregatePrimitiveUpdateKind::COUNT:
		return true;
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		return SljitPreaggregatedPrimitiveIntegerTypeSupported(type);
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		return SljitPreaggregatedPrimitiveIntegerTypeSupported(type) || type == PhysicalType::INT128;
	default:
		return false;
	}
}

static bool SljitPreaggregationComparableInputVectorType(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::VARCHAR:
		return true;
	default:
		return false;
	}
}

static bool SljitPreaggregationCompressedUnsignedTargetType(PhysicalType type) {
	switch (type) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
		return true;
	default:
		return false;
	}
}

static bool SljitPreaggregationIntegralCompressionSourceType(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		return true;
	default:
		return false;
	}
}

static bool SljitPreaggregationInputVectorGroupCastSupported(const ExecutionRowPointerGroupKeySource &source) {
	if (source.HasOutputTransform()) {
		return source.output_transform_kind == ExecutionGroupKeyOutputTransformKind::ADD_CONSTANT &&
		       source.cast_kind == ExecutionRowPointerGroupKeyCastKind::NONE &&
		       SljitPreaggregationComparableInputVectorType(source.source_physical_type) &&
		       SljitSignedAffineGroupPhysicalType(source.source_physical_type) &&
		       SljitSignedAffineGroupPhysicalType(source.target_physical_type);
	}
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		return source.source_physical_type == source.target_physical_type &&
		       SljitPreaggregationComparableInputVectorType(source.source_physical_type);
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		return source.source_physical_type == PhysicalType::INT64 && source.target_physical_type == PhysicalType::INT32;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		return source.source_physical_type == PhysicalType::INT64 && source.target_physical_type == PhysicalType::INT16;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		return source.source_physical_type == PhysicalType::INT32 && source.target_physical_type == PhysicalType::INT8;
	case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
		return SljitPreaggregationIntegralCompressionSourceType(source.source_physical_type) &&
		       SljitPreaggregationCompressedUnsignedTargetType(source.target_physical_type);
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		return source.source_physical_type == PhysicalType::INT32 && source.source_type.id() == LogicalTypeId::DATE &&
		       SljitPreaggregationCompressedUnsignedTargetType(source.target_physical_type);
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		return source.source_physical_type == PhysicalType::VARCHAR &&
		       SljitPreaggregationCompressedUnsignedTargetType(source.target_physical_type);
	default:
		return false;
	}
}

static bool SljitPreaggregationInputVectorGroupSourceSupported(DataChunk &payload_input,
                                                               const ExecutionRowPointerGroupKeySource &source) {
	return source.ready && source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR &&
	       source.input_vector_index < payload_input.ColumnCount() &&
	       payload_input.data[source.input_vector_index].GetType().InternalType() == source.source_physical_type &&
	       SljitPreaggregationInputVectorGroupCastSupported(source);
}

static bool SljitLoadPreaggregatedInt64Payload(SljitPreaggregatedPrimitivePayloadSource &source, idx_t row_idx,
                                               int64_t &result) {
	auto source_idx = source.format.sel->get_index(row_idx);
	if (!source.rows_all_valid && !source.format.validity.RowIsValid(source_idx)) {
		return false;
	}
	switch (source.type) {
	case PhysicalType::INT8:
		result = UnifiedVectorFormat::GetData<int8_t>(source.format)[source_idx];
		return true;
	case PhysicalType::INT16:
		result = UnifiedVectorFormat::GetData<int16_t>(source.format)[source_idx];
		return true;
	case PhysicalType::INT32:
		result = UnifiedVectorFormat::GetData<int32_t>(source.format)[source_idx];
		return true;
	case PhysicalType::INT64:
		result = UnifiedVectorFormat::GetData<int64_t>(source.format)[source_idx];
		return true;
	case PhysicalType::UINT8:
		result = NumericCast<int64_t>(UnifiedVectorFormat::GetData<uint8_t>(source.format)[source_idx]);
		return true;
	case PhysicalType::UINT16:
		result = NumericCast<int64_t>(UnifiedVectorFormat::GetData<uint16_t>(source.format)[source_idx]);
		return true;
	case PhysicalType::UINT32:
		result = NumericCast<int64_t>(UnifiedVectorFormat::GetData<uint32_t>(source.format)[source_idx]);
		return true;
	default:
		return false;
	}
}

static bool SljitLoadPreaggregatedHugeintPayload(SljitPreaggregatedPrimitivePayloadSource &source, idx_t row_idx,
                                                 hugeint_t &result) {
	auto source_idx = source.format.sel->get_index(row_idx);
	if (!source.rows_all_valid && !source.format.validity.RowIsValid(source_idx)) {
		return false;
	}
	if (source.type == PhysicalType::INT128) {
		result = UnifiedVectorFormat::GetData<hugeint_t>(source.format)[source_idx];
		return true;
	}
	int64_t value;
	if (!SljitLoadPreaggregatedInt64Payload(source, row_idx, value)) {
		return false;
	}
	result = hugeint_t(value);
	return true;
}

static bool PrepareSljitPreaggregatedPrimitivePayloadSource(DataChunk &input,
                                                            const ExecutionPrimitiveAggregateUpdateLane *lane,
                                                            idx_t source_idx, bool require_lane_payload_type,
                                                            SljitPreaggregatedPrimitivePayloadSource &source) {
	source = SljitPreaggregatedPrimitivePayloadSource();
	if (!lane) {
		return false;
	}
	if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		if (source_idx != DConstants::INVALID_INDEX) {
			return false;
		}
		source.type = PhysicalType::INVALID;
		source.rows_all_valid = true;
		return true;
	}
	if (lane->kind == AggregatePrimitiveUpdateKind::COUNT && source_idx == DConstants::INVALID_INDEX) {
		source.type = PhysicalType::INVALID;
		source.rows_all_valid = true;
		return true;
	}
	if (source_idx >= input.ColumnCount()) {
		return false;
	}
	auto &source_vector = input.data[source_idx];
	source.type = source_vector.GetType().InternalType();
	if ((require_lane_payload_type && source.type != lane->payload_type) ||
	    !SljitPreaggregatedPrimitivePayloadSupported(lane->kind, source.type)) {
		return false;
	}
	source_vector.ToUnifiedFormat(source.format);
	source.rows_all_valid = SljitPreaggregatedFormatRowsAllValid(source.format, input.size());
	return true;
}

static bool SljitPreparedPrimitivePayloadSourceSupportsLane(DataChunk &input,
                                                            const ExecutionPrimitiveAggregateUpdateLane *lane,
                                                            idx_t source_idx, bool require_lane_payload_type,
                                                            const SljitPreaggregatedPrimitivePayloadSource &source) {
	if (!lane) {
		return false;
	}
	if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return source_idx == DConstants::INVALID_INDEX && source.type == PhysicalType::INVALID;
	}
	if (lane->kind == AggregatePrimitiveUpdateKind::COUNT && source_idx == DConstants::INVALID_INDEX) {
		return source.type == PhysicalType::INVALID;
	}
	if (source_idx >= input.ColumnCount()) {
		return false;
	}
	const auto input_type = input.data[source_idx].GetType().InternalType();
	return source.type == input_type && (!require_lane_payload_type || input_type == lane->payload_type) &&
	       SljitPreaggregatedPrimitivePayloadSupported(lane->kind, input_type);
}

struct SljitPreaggregatedPrimitivePayloadSources {
	bool Prepare(SljitExecutableRegionOp &op, DataChunk &input,
	             const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		if (payload_lanes.size() != aggregates.size()) {
			return false;
		}
		Reset(payload_lanes.size());
		for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			auto lane = payload_lanes[payload_idx];
			idx_t source_idx = DConstants::INVALID_INDEX;
			if (lane && lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
				auto &aggregate = aggregates[payload_idx];
				if (aggregate.child_indices.size() != 1) {
					return false;
				}
				source_idx = aggregate.child_indices[0];
			}
			if (!Bind(input, lane, source_idx, false)) {
				return false;
			}
		}
		return true;
	}

	bool Prepare(DataChunk &input, const vector<idx_t> &payload_source_indices,
	             const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		if (payload_source_indices.size() != payload_lanes.size()) {
			return false;
		}
		Reset(payload_lanes.size());
		for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
			if (!Bind(input, payload_lanes[payload_idx], payload_source_indices[payload_idx], true)) {
				return false;
			}
		}
		return true;
	}

	bool RowIsValid(idx_t payload_idx, idx_t row_idx) {
		auto source = GetMutableSource(payload_idx);
		if (!source) {
			return false;
		}
		if (source->type == PhysicalType::INVALID) {
			return true;
		}
		auto source_idx = source->format.sel->get_index(row_idx);
		return source->rows_all_valid || source->format.validity.RowIsValid(source_idx);
	}

	bool LoadInt64(idx_t payload_idx, idx_t row_idx, int64_t &result) {
		auto source = GetMutableSource(payload_idx);
		return source && SljitLoadPreaggregatedInt64Payload(*source, row_idx, result);
	}

	bool LoadHugeint(idx_t payload_idx, idx_t row_idx, hugeint_t &result) {
		auto source = GetMutableSource(payload_idx);
		return source && SljitLoadPreaggregatedHugeintPayload(*source, row_idx, result);
	}

	bool SourceCanHaveNull(idx_t payload_idx) const {
		auto source = GetSourceInternal(payload_idx);
		if (!source) {
			return true;
		}
		return source->type != PhysicalType::INVALID && source->format.validity.CanHaveNull();
	}

	optional_ptr<const SljitPreaggregatedPrimitivePayloadSource> GetSource(idx_t payload_idx) const {
		return optional_ptr<const SljitPreaggregatedPrimitivePayloadSource>(GetSourceInternal(payload_idx));
	}

private:
	void Reset(idx_t lane_count) {
		sources.clear();
		input_source_indices.clear();
		lane_source_bindings.clear();
		sources.reserve(lane_count);
		input_source_indices.reserve(lane_count);
		lane_source_bindings.reserve(lane_count);
	}

	bool Bind(DataChunk &input, const ExecutionPrimitiveAggregateUpdateLane *lane, idx_t source_idx,
	          bool require_lane_payload_type) {
		for (idx_t unique_idx = 0; unique_idx < input_source_indices.size(); unique_idx++) {
			if (input_source_indices[unique_idx] != source_idx) {
				continue;
			}
			if (!SljitPreparedPrimitivePayloadSourceSupportsLane(input, lane, source_idx, require_lane_payload_type,
			                                                     sources[unique_idx])) {
				return false;
			}
			lane_source_bindings.push_back(unique_idx);
			return true;
		}
		sources.emplace_back();
		if (!PrepareSljitPreaggregatedPrimitivePayloadSource(input, lane, source_idx, require_lane_payload_type,
		                                                     sources.back())) {
			sources.pop_back();
			return false;
		}
		input_source_indices.push_back(source_idx);
		lane_source_bindings.push_back(sources.size() - 1);
		return true;
	}

	SljitPreaggregatedPrimitivePayloadSource *GetMutableSource(idx_t payload_idx) {
		if (payload_idx >= lane_source_bindings.size() || lane_source_bindings[payload_idx] >= sources.size()) {
			return nullptr;
		}
		return &sources[lane_source_bindings[payload_idx]];
	}

	const SljitPreaggregatedPrimitivePayloadSource *GetSourceInternal(idx_t payload_idx) const {
		if (payload_idx >= lane_source_bindings.size() || lane_source_bindings[payload_idx] >= sources.size()) {
			return nullptr;
		}
		return &sources[lane_source_bindings[payload_idx]];
	}

	vector<SljitPreaggregatedPrimitivePayloadSource> sources;
	vector<idx_t> input_source_indices;
	vector<idx_t> lane_source_bindings;
};

static bool SljitStartPreaggregatedPrimitivePayloadGroup(
    SljitPreaggregatedPrimitiveAggregateScratch &scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
	scratch.group_row_counts.push_back(0);
	for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
		auto &payload = scratch.payloads[payload_idx];
		switch (payload.kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::COUNT:
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			payload.int64_values.push_back(0);
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			payload.hugeint_values.emplace_back(0);
			break;
		default:
			return false;
		}
		if (payload.kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
		    payload.kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			payload.value_is_set.push_back(0);
		}
	}
	return true;
}

static bool SljitAccumulatePreaggregatedPrimitivePayloadGroup(
    SljitPreaggregatedPrimitivePayloadSources &payload_sources, SljitPreaggregatedPrimitiveAggregateScratch &scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, idx_t row_idx, idx_t group_idx) {
	if (group_idx >= scratch.group_row_counts.size()) {
		return false;
	}
	for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
		auto lane = payload_lanes[payload_idx];
		auto &payload = scratch.payloads[payload_idx];
		switch (lane->kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
			payload.int64_values[group_idx]++;
			break;
		case AggregatePrimitiveUpdateKind::COUNT: {
			if (payload_sources.RowIsValid(payload_idx, row_idx)) {
				payload.int64_values[group_idx]++;
			}
			break;
		}
		case AggregatePrimitiveUpdateKind::SUM_INT64: {
			if (!payload_sources.RowIsValid(payload_idx, row_idx)) {
				break;
			}
			int64_t value;
			if (!payload_sources.LoadInt64(payload_idx, row_idx, value)) {
				return false;
			}
			payload.int64_values[group_idx] += value;
			payload.value_is_set[group_idx] = 1;
			break;
		}
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
			if (!payload_sources.RowIsValid(payload_idx, row_idx)) {
				break;
			}
			hugeint_t value;
			if (!payload_sources.LoadHugeint(payload_idx, row_idx, value)) {
				return false;
			}
			payload.hugeint_values[group_idx] += value;
			payload.value_is_set[group_idx] = 1;
			break;
		}
		default:
			return false;
		}
	}
	scratch.group_row_counts[group_idx]++;
	return true;
}

} // namespace duckdb
