//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_count_star_string_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregation_common.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <array>
#include <cstring>

namespace duckdb {

static void SljitReverseMemCpy(data_ptr_t dest, const_data_ptr_t src, idx_t length) {
	for (idx_t byte_idx = 0; byte_idx < length; byte_idx++) {
		dest[byte_idx] = src[length - 1 - byte_idx];
	}
}

template <class RESULT_TYPE>
static bool TrySljitStringCompressWide(const string_t &input, RESULT_TYPE &result) {
	if (input.GetSize() >= sizeof(RESULT_TYPE)) {
		return false;
	}
	auto result_ptr = data_ptr_cast(&result);
	if (sizeof(RESULT_TYPE) <= string_t::INLINE_LENGTH) {
		SljitReverseMemCpy(result_ptr, const_data_ptr_cast(input.GetPrefix()), sizeof(RESULT_TYPE));
	} else if (input.IsInlined()) {
		static constexpr auto REMAINDER = sizeof(RESULT_TYPE) - string_t::INLINE_LENGTH;
		SljitReverseMemCpy(result_ptr + REMAINDER, const_data_ptr_cast(input.GetPrefix()), string_t::INLINE_LENGTH);
		memset(result_ptr, '\0', REMAINDER);
	} else {
		const auto size = MinValue<idx_t>(sizeof(RESULT_TYPE), input.GetSize());
		const auto remainder = sizeof(RESULT_TYPE) - size;
		SljitReverseMemCpy(result_ptr + remainder, data_ptr_cast(input.GetPointer()), size);
		memset(result_ptr, '\0', remainder);
	}
	result_ptr[0] = UnsafeNumericCast<data_t>(input.GetSize());
	result = BSwapIfBE(result);
	return true;
}

static bool TrySljitStringCompressUInt8(const string_t &input, uint8_t &result) {
	if (input.GetSize() > sizeof(uint8_t)) {
		return false;
	}
	result = input.GetSize() == 0
	             ? 0
	             : UnsafeNumericCast<uint8_t>(input.GetSize() + *const_data_ptr_cast(input.GetPrefix()));
	result = BSwapIfBE(result);
	return true;
}

static bool SljitStringEquals(const string_t &left, const string_t &right) {
	return left == right;
}

static bool AccumulatePreaggregatedStringCountStarKey(
    const string_t &key, std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
    std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts, idx_t &group_count) {
	idx_t group_idx = 0;
	for (; group_idx < group_count; group_idx++) {
		if (SljitStringEquals(keys[group_idx], key)) {
			break;
		}
	}
	if (group_idx == group_count) {
		if (group_count == SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT) {
			return false;
		}
		keys[group_count] = key;
		counts[group_count] = 0;
		group_count++;
	}
	counts[group_idx]++;
	return true;
}

template <class T>
static bool TrySljitStringCompressCountStarKey(const string_t &input, T &result) {
	return TrySljitStringCompressWide(input, result);
}

static bool TrySljitStringCompressCountStarKey(const string_t &input, uint8_t &result) {
	return TrySljitStringCompressUInt8(input, result);
}

template <class T>
static bool
MaterializePreaggregatedStringCountStarGroups(const std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
                                              const std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts,
                                              idx_t group_count, DataChunk &compact_groups,
                                              vector<int64_t> &count_deltas) {
	auto target_data = PrepareFlatPreaggregatedGroupTarget<T>(compact_groups);
	count_deltas.resize(group_count);
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		T compressed_key;
		if (!TrySljitStringCompressCountStarKey(keys[group_idx], compressed_key)) {
			return false;
		}
		target_data[group_idx] = compressed_key;
		count_deltas[group_idx] = counts[group_idx];
	}
	FinishFlatPreaggregatedGroupTarget(compact_groups, group_count);
	return true;
}

template <class T>
static bool TryPreaggregateStringCompressedCountStarGroupsTemplated(Vector &source, const SelectionVector *execute_sel,
                                                                    idx_t count, DataChunk &compact_groups,
                                                                    vector<int64_t> &count_deltas) {
	UnifiedVectorFormat format;
	source.ToUnifiedFormat(format);
	auto source_data = UnifiedVectorFormat::GetData<string_t>(format);
	auto source_sel = format.sel;
	auto &source_validity = format.validity;
	const bool can_have_null = source_validity.CanHaveNull();
	std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
	std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
	idx_t group_count = 0;

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
		const auto source_idx = source_sel->get_index(input_idx);
		if (can_have_null && !source_validity.RowIsValid(source_idx)) {
			return false;
		}
		if (!AccumulatePreaggregatedStringCountStarKey(source_data[source_idx], keys, counts, group_count)) {
			return false;
		}
	}

	return MaterializePreaggregatedStringCountStarGroups<T>(keys, counts, group_count, compact_groups, count_deltas);
}

template <class T>
static bool
TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated(Vector &source, const SelectionVector &mark_flags,
                                                              idx_t count, DataChunk &compact_groups,
                                                              vector<int64_t> &count_deltas, idx_t &selected_count) {
	if (source.GetVectorType() == VectorType::FLAT_VECTOR) {
		auto &validity = FlatVector::Validity(source);
		if (validity.CannotHaveNull() || validity.CheckAllValid(count)) {
			auto source_data = FlatVector::GetData<string_t>(source);
			std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
			std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
			idx_t group_count = 0;
			selected_count = 0;
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				if (mark_flags.get_index(row_idx) == 0) {
					continue;
				}
				selected_count++;
				if (!AccumulatePreaggregatedStringCountStarKey(source_data[row_idx], keys, counts, group_count)) {
					return false;
				}
			}
			if (selected_count == 0) {
				compact_groups.Reset();
				return true;
			}
			return MaterializePreaggregatedStringCountStarGroups<T>(keys, counts, group_count, compact_groups,
			                                                        count_deltas);
		}
	}

	UnifiedVectorFormat format;
	source.ToUnifiedFormat(format);
	auto source_data = UnifiedVectorFormat::GetData<string_t>(format);
	auto source_sel = format.sel;
	auto &source_validity = format.validity;
	const bool can_have_null = source_validity.CanHaveNull();
	std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
	std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
	idx_t group_count = 0;
	selected_count = 0;

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (mark_flags.get_index(row_idx) == 0) {
			continue;
		}
		selected_count++;
		const auto source_idx = source_sel->get_index(row_idx);
		if (can_have_null && !source_validity.RowIsValid(source_idx)) {
			return false;
		}
		if (!AccumulatePreaggregatedStringCountStarKey(source_data[source_idx], keys, counts, group_count)) {
			return false;
		}
	}
	if (selected_count == 0) {
		compact_groups.Reset();
		return true;
	}

	return MaterializePreaggregatedStringCountStarGroups<T>(keys, counts, group_count, compact_groups, count_deltas);
}

static bool TryGetStringCompressedCountStarProjectionSource(const SljitExecutableRegionOp &projection_op,
                                                            DataChunk &input, DataChunk &compact_groups, idx_t count,
                                                            idx_t &source_index, PhysicalType &compressed_type) {
	if (count == 0 || projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    projection_op.projections.size() != 1 || projection_op.output_types.size() != 1 ||
	    compact_groups.ColumnCount() != 1 || compact_groups.data[0].GetType() != projection_op.output_types[0]) {
		return false;
	}
	auto &plan = projection_op.projections[0].plan;
	if (plan.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
	    plan.return_type != projection_op.output_types[0] || plan.source_index >= input.ColumnCount() ||
	    input.data[plan.source_index].GetType().id() != LogicalTypeId::VARCHAR) {
		return false;
	}
	compressed_type = projection_op.output_types[0].InternalType();
	if (plan.string_compress_target_size != GetTypeIdSize(compressed_type)) {
		return false;
	}
	source_index = plan.source_index;
	return true;
}

static bool TryPreaggregateProjectedCountStarGroups(const SljitExecutableRegionOp &projection_op, DataChunk &input,
                                                    const SelectionVector *execute_sel, idx_t count,
                                                    DataChunk &compact_groups, vector<int64_t> &count_deltas) {
	idx_t source_index;
	PhysicalType compressed_type;
	if (!TryGetStringCompressedCountStarProjectionSource(projection_op, input, compact_groups, count, source_index,
	                                                     compressed_type)) {
		return false;
	}
	switch (compressed_type) {
	case PhysicalType::UINT8:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint8_t>(input.data[source_index], execute_sel,
		                                                                        count, compact_groups, count_deltas);
	case PhysicalType::UINT16:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint16_t>(input.data[source_index], execute_sel,
		                                                                         count, compact_groups, count_deltas);
	case PhysicalType::UINT32:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint32_t>(input.data[source_index], execute_sel,
		                                                                         count, compact_groups, count_deltas);
	case PhysicalType::UINT64:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint64_t>(input.data[source_index], execute_sel,
		                                                                         count, compact_groups, count_deltas);
	case PhysicalType::UINT128:
		return TryPreaggregateStringCompressedCountStarGroupsTemplated<uhugeint_t>(
		    input.data[source_index], execute_sel, count, compact_groups, count_deltas);
	default:
		return false;
	}
}

static bool TryPreaggregateProjectedMarkedCountStarGroups(const SljitExecutableRegionOp &projection_op,
                                                          DataChunk &input, const SelectionVector &mark_flags,
                                                          idx_t count, DataChunk &compact_groups,
                                                          vector<int64_t> &count_deltas, idx_t &selected_count) {
	idx_t source_index;
	PhysicalType compressed_type;
	if (!TryGetStringCompressedCountStarProjectionSource(projection_op, input, compact_groups, count, source_index,
	                                                     compressed_type)) {
		return false;
	}
	switch (compressed_type) {
	case PhysicalType::UINT8:
		return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uint8_t>(
		    input.data[source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
	case PhysicalType::UINT16:
		return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uint16_t>(
		    input.data[source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
	case PhysicalType::UINT32:
		return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uint32_t>(
		    input.data[source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
	case PhysicalType::UINT64:
		return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uint64_t>(
		    input.data[source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
	case PhysicalType::UINT128:
		return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uhugeint_t>(
		    input.data[source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
	default:
		return false;
	}
}

} // namespace duckdb
