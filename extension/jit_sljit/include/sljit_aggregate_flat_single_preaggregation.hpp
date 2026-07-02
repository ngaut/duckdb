//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_flat_single_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregation_common.hpp"
#include "sljit_aggregate_primitive_preaggregation_runtime.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

#include <type_traits>

namespace duckdb {

static bool SljitFlatVectorAllValid(Vector &vector, idx_t count) {
	if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	auto &validity = FlatVector::Validity(vector);
	return validity.CannotHaveNull() || validity.CheckAllValid(count);
}

template <class T>
static int64_t SljitPreaggregateToInt64(T value) {
	return NumericCast<int64_t>(value);
}

template <class T>
static hugeint_t SljitPreaggregateToHugeint(T value) {
	return hugeint_t(SljitPreaggregateToInt64(value));
}

static hugeint_t SljitPreaggregateToHugeint(hugeint_t value) {
	return value;
}

static vector<int64_t> &SljitPreaggregatedSingleSumValues(SljitPreaggregatedPrimitivePayloadDeltas &payload,
                                                          int64_t *) {
	return payload.int64_values;
}

static vector<hugeint_t> &SljitPreaggregatedSingleSumValues(SljitPreaggregatedPrimitivePayloadDeltas &payload,
                                                            hugeint_t *) {
	return payload.hugeint_values;
}

static void SljitPreaggregatedSingleSumAppendZero(vector<int64_t> &values) {
	values.push_back(0);
}

static void SljitPreaggregatedSingleSumAppendZero(vector<hugeint_t> &values) {
	values.emplace_back(0);
}

template <class SOURCE_TYPE>
static int64_t SljitPreaggregatedSingleSumConvert(SOURCE_TYPE value, int64_t *) {
	return SljitPreaggregateToInt64(value);
}

template <class SOURCE_TYPE>
static hugeint_t SljitPreaggregatedSingleSumConvert(SOURCE_TYPE value, hugeint_t *) {
	return SljitPreaggregateToHugeint(value);
}

template <class T>
static bool SljitFlatKeysHaveConsecutiveRepeat(const T *keys, idx_t count) {
	const auto sample_count = MinValue<idx_t>(count, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		if (keys[row_idx] == keys[row_idx - 1]) {
			return true;
		}
	}
	return false;
}

template <class GROUP_TYPE>
static bool TryPreaggregateFlatAllValidSingleCountStarGroup(
    DataChunk &input, idx_t group_source_index, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
	const auto count = input.size();
	auto &group_vector = input.data[group_source_index];
	if (!SljitFlatVectorAllValid(group_vector, count)) {
		return false;
	}
	auto group_data = FlatVector::GetData<GROUP_TYPE>(group_vector);
	if (!SljitFlatKeysHaveConsecutiveRepeat(group_data, count)) {
		return false;
	}
	scratch.Prepare(payload_lanes, count);
	auto target_data = PrepareFlatPreaggregatedGroupTarget<GROUP_TYPE>(compact_groups);
	auto &counts = scratch.payloads[0].int64_values;
	idx_t group_count = 0;
	GROUP_TYPE active_key {};
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto key = group_data[row_idx];
		if (group_count == 0 || !(key == active_key)) {
			active_key = key;
			target_data[group_count] = key;
			counts.push_back(0);
			group_count++;
		}
		counts[group_count - 1]++;
	}
	if (group_count == count) {
		return false;
	}
	FinishFlatPreaggregatedGroupTarget(compact_groups, group_count);
	return true;
}

template <class GROUP_TYPE, class SOURCE_TYPE, class SUM_TYPE>
static bool
TryPreaggregateFlatAllValidSingleSumGroup(DataChunk &input, idx_t group_source_index, idx_t payload_source_index,
                                          DataChunk &compact_groups,
                                          SljitPreaggregatedPrimitiveAggregateScratch &scratch,
                                          const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
	const auto count = input.size();
	auto &group_vector = input.data[group_source_index];
	auto &payload_vector = input.data[payload_source_index];
	if (!SljitFlatVectorAllValid(group_vector, count) || !SljitFlatVectorAllValid(payload_vector, count)) {
		return false;
	}
	auto group_data = FlatVector::GetData<GROUP_TYPE>(group_vector);
	if (!SljitFlatKeysHaveConsecutiveRepeat(group_data, count)) {
		return false;
	}
	auto payload_data = FlatVector::GetData<SOURCE_TYPE>(payload_vector);
	scratch.Prepare(payload_lanes, count);
	auto target_data = PrepareFlatPreaggregatedGroupTarget<GROUP_TYPE>(compact_groups);
	SUM_TYPE *sum_type = nullptr;
	auto &sums = SljitPreaggregatedSingleSumValues(scratch.payloads[0], sum_type);
	auto &value_is_set = scratch.payloads[0].value_is_set;
	idx_t group_count = 0;
	GROUP_TYPE active_key {};
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto key = group_data[row_idx];
		if (group_count == 0 || !(key == active_key)) {
			active_key = key;
			target_data[group_count] = key;
			SljitPreaggregatedSingleSumAppendZero(sums);
			value_is_set.push_back(0);
			group_count++;
		}
		sums[group_count - 1] += SljitPreaggregatedSingleSumConvert(payload_data[row_idx], sum_type);
		value_is_set[group_count - 1] = 1;
	}
	if (group_count == count) {
		return false;
	}
	FinishFlatPreaggregatedGroupTarget(compact_groups, group_count);
	return true;
}

template <class GROUP_TYPE, class SUM_TYPE>
static bool TryPreaggregateFlatAllValidSingleSumGroupBySource(
    PhysicalType source_type, DataChunk &input, idx_t group_source_index, idx_t payload_source_index,
    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &scratch,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
	switch (source_type) {
	case PhysicalType::INT8:
		return TryPreaggregateFlatAllValidSingleSumGroup<GROUP_TYPE, int8_t, SUM_TYPE>(
		    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	case PhysicalType::INT16:
		return TryPreaggregateFlatAllValidSingleSumGroup<GROUP_TYPE, int16_t, SUM_TYPE>(
		    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	case PhysicalType::INT32:
		return TryPreaggregateFlatAllValidSingleSumGroup<GROUP_TYPE, int32_t, SUM_TYPE>(
		    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	case PhysicalType::INT64:
		return TryPreaggregateFlatAllValidSingleSumGroup<GROUP_TYPE, int64_t, SUM_TYPE>(
		    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	case PhysicalType::UINT8:
		return TryPreaggregateFlatAllValidSingleSumGroup<GROUP_TYPE, uint8_t, SUM_TYPE>(
		    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	case PhysicalType::UINT16:
		return TryPreaggregateFlatAllValidSingleSumGroup<GROUP_TYPE, uint16_t, SUM_TYPE>(
		    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	case PhysicalType::UINT32:
		return TryPreaggregateFlatAllValidSingleSumGroup<GROUP_TYPE, uint32_t, SUM_TYPE>(
		    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	case PhysicalType::INT128:
		if constexpr (std::is_same<SUM_TYPE, hugeint_t>::value) {
			return TryPreaggregateFlatAllValidSingleSumGroup<GROUP_TYPE, hugeint_t, SUM_TYPE>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		}
		return false;
	default:
		return false;
	}
}

template <class GROUP_TYPE>
static bool TryPreaggregateFlatAllValidSinglePrimitiveGroup(
    SljitExecutableRegionOp &op, DataChunk &input,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
	if (payload_lanes.size() != 1) {
		return false;
	}
	auto &sink_info = op.aggregate_update.plan.sink_info;
	auto &aggregate = sink_info.aggregates[0];
	auto lane = payload_lanes[0];
	if (!lane) {
		return false;
	}
	const auto group_source_index = sink_info.groups[0].input_index;
	if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return TryPreaggregateFlatAllValidSingleCountStarGroup<GROUP_TYPE>(input, group_source_index, compact_groups,
		                                                                   scratch, payload_lanes);
	}
	if (aggregate.child_indices.size() != 1 || aggregate.child_indices[0] >= input.ColumnCount()) {
		return false;
	}
	const auto payload_source_index = aggregate.child_indices[0];
	const auto source_type = input.data[payload_source_index].GetType().InternalType();
	if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
		return TryPreaggregateFlatAllValidSingleSumGroupBySource<GROUP_TYPE, int64_t>(
		    source_type, input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	}
	if (lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		return TryPreaggregateFlatAllValidSingleSumGroupBySource<GROUP_TYPE, hugeint_t>(
		    source_type, input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
	}
	return false;
}

} // namespace duckdb
