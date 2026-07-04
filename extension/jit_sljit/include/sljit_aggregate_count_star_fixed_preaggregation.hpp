//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_count_star_fixed_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_preaggregation_common.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <array>
#include <type_traits>

namespace duckdb {

template <class T>
static bool AccumulatePreaggregatedCountStarDeltaKey(const T &key, int64_t delta,
                                                     std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
                                                     std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts,
                                                     idx_t &group_count) {
	idx_t group_idx = 0;
	for (; group_idx < group_count; group_idx++) {
		if (keys[group_idx] == key) {
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
	counts[group_idx] += delta;
	return true;
}

template <class T>
static bool AccumulatePreaggregatedCountStarKey(const T &key,
                                                std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
                                                std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts,
                                                idx_t &group_count) {
	return AccumulatePreaggregatedCountStarDeltaKey(key, 1, keys, counts, group_count);
}

template <class T>
static void
MaterializePreaggregatedCountStarGroups(const std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
                                        const std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts,
                                        idx_t group_count, DataChunk &compact_groups, vector<int64_t> &count_deltas) {
	auto target_data = PrepareFlatPreaggregatedGroupTarget<T>(compact_groups);
	count_deltas.resize(group_count);
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		target_data[group_idx] = keys[group_idx];
		count_deltas[group_idx] = counts[group_idx];
	}
	FinishFlatPreaggregatedGroupTarget(compact_groups, group_count);
}

template <class T>
static bool SljitDenseCountStarRange(T min_key, T max_key, idx_t &range) {
	if constexpr (std::is_same<T, hugeint_t>::value || std::is_same<T, uhugeint_t>::value) {
		return false;
	} else {
		if (max_key < min_key) {
			return false;
		}
		auto key = min_key;
		range = 1;
		while (key < max_key) {
			if (range == SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT) {
				return false;
			}
			key = static_cast<T>(key + static_cast<T>(1));
			range++;
		}
		return true;
	}
}

template <class T>
static idx_t SljitDenseCountStarIndex(T key, T min_key) {
	D_ASSERT(key >= min_key);
	return static_cast<idx_t>(key - min_key);
}

template <class T>
static T SljitDenseCountStarKey(T min_key, idx_t offset) {
	return static_cast<T>(min_key + static_cast<T>(offset));
}

template <class T>
static bool TryPreaggregateDenseFixedWidthCountStarGroupsTemplated(UnifiedVectorFormat &format,
                                                                   const SelectionVector *execute_sel, idx_t count,
                                                                   DataChunk &compact_groups,
                                                                   vector<int64_t> &count_deltas);

template <class T>
static bool TryPreaggregateDenseFixedWidthCountStarGroupsTemplated(UnifiedVectorFormat &format, idx_t count,
                                                                   DataChunk &compact_groups,
                                                                   vector<int64_t> &count_deltas) {
	return TryPreaggregateDenseFixedWidthCountStarGroupsTemplated<T>(format, nullptr, count, compact_groups,
	                                                                 count_deltas);
}

template <class T>
static bool TryPreaggregateDenseFixedWidthCountStarGroupsTemplated(UnifiedVectorFormat &format,
                                                                   const SelectionVector *execute_sel, idx_t count,
                                                                   DataChunk &compact_groups,
                                                                   vector<int64_t> &count_deltas) {
	if constexpr (std::is_same<T, hugeint_t>::value || std::is_same<T, uhugeint_t>::value) {
		return false;
	} else {
		auto source_data = UnifiedVectorFormat::GetData<T>(format);
		auto source_sel = format.sel;
		auto &source_validity = format.validity;
		const bool can_have_null = source_validity.CanHaveNull();
		std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
		counts.fill(0);
		T min_key {};
		T max_key {};
		idx_t range = 0;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
			const auto source_idx = source_sel->get_index(input_idx);
			if (can_have_null && !source_validity.RowIsValid(source_idx)) {
				return false;
			}
			auto key = source_data[source_idx];
			if (range == 0) {
				min_key = key;
				max_key = key;
				range = 1;
				counts[0] = 1;
				continue;
			}
			if (key < min_key) {
				idx_t new_range;
				if (!SljitDenseCountStarRange(key, max_key, new_range)) {
					return false;
				}
				const auto shift = SljitDenseCountStarIndex(min_key, key);
				for (idx_t group_idx = range; group_idx > 0; group_idx--) {
					counts[group_idx + shift - 1] = counts[group_idx - 1];
				}
				for (idx_t group_idx = 0; group_idx < shift; group_idx++) {
					counts[group_idx] = 0;
				}
				min_key = key;
				range = new_range;
			} else if (key > max_key) {
				idx_t new_range;
				if (!SljitDenseCountStarRange(min_key, key, new_range)) {
					return false;
				}
				for (idx_t group_idx = range; group_idx < new_range; group_idx++) {
					counts[group_idx] = 0;
				}
				max_key = key;
				range = new_range;
			}
			counts[SljitDenseCountStarIndex(key, min_key)]++;
		}
		if (range == 0) {
			return false;
		}

		auto target_data = PrepareFlatPreaggregatedGroupTarget<T>(compact_groups);
		count_deltas.clear();
		idx_t group_count = 0;
		for (idx_t offset = 0; offset < range; offset++) {
			if (counts[offset] == 0) {
				continue;
			}
			target_data[group_count] = SljitDenseCountStarKey(min_key, offset);
			count_deltas.push_back(counts[offset]);
			group_count++;
		}
		FinishFlatPreaggregatedGroupTarget(compact_groups, group_count);
		return true;
	}
}

template <class T>
static bool TryPreaggregateFixedWidthCountStarVectorTemplated(Vector &input_group, const SelectionVector *execute_sel,
                                                              idx_t count, DataChunk &compact_groups,
                                                              vector<int64_t> &count_deltas);

template <class T>
static bool TryPreaggregateFixedWidthCountStarVectorTemplated(Vector &input_group, idx_t count,
                                                              DataChunk &compact_groups,
                                                              vector<int64_t> &count_deltas) {
	return TryPreaggregateFixedWidthCountStarVectorTemplated<T>(input_group, nullptr, count, compact_groups,
	                                                            count_deltas);
}

template <class T>
static bool TryPreaggregateFixedWidthCountStarVectorTemplated(Vector &input_group, const SelectionVector *execute_sel,
                                                              idx_t count, DataChunk &compact_groups,
                                                              vector<int64_t> &count_deltas) {
	if (count == 0 || compact_groups.ColumnCount() != 1 || input_group.GetType() != compact_groups.data[0].GetType()) {
		return false;
	}

	UnifiedVectorFormat format;
	input_group.ToUnifiedFormat(format);
	if (TryPreaggregateDenseFixedWidthCountStarGroupsTemplated<T>(format, execute_sel, count, compact_groups,
	                                                              count_deltas)) {
		return true;
	}
	auto source_data = UnifiedVectorFormat::GetData<T>(format);
	auto source_sel = format.sel;
	auto &source_validity = format.validity;
	std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
	std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
	idx_t group_count = 0;
	const bool can_have_null = source_validity.CanHaveNull();

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
		const auto source_idx = source_sel->get_index(input_idx);
		if (can_have_null && !source_validity.RowIsValid(source_idx)) {
			return false;
		}
		auto key = source_data[source_idx];
		if (!AccumulatePreaggregatedCountStarKey(key, keys, counts, group_count)) {
			return false;
		}
	}
	MaterializePreaggregatedCountStarGroups(keys, counts, group_count, compact_groups, count_deltas);
	return true;
}

static bool TryPreaggregateFixedWidthCountStarVector(Vector &input_group, const SelectionVector *execute_sel,
                                                     idx_t count, DataChunk &compact_groups,
                                                     vector<int64_t> &count_deltas);

static bool TryPreaggregateFixedWidthCountStarVector(Vector &input_group, idx_t count, DataChunk &compact_groups,
                                                     vector<int64_t> &count_deltas) {
	return TryPreaggregateFixedWidthCountStarVector(input_group, nullptr, count, compact_groups, count_deltas);
}

static bool TryPreaggregateFixedWidthCountStarVector(Vector &input_group, const SelectionVector *execute_sel,
                                                     idx_t count, DataChunk &compact_groups,
                                                     vector<int64_t> &count_deltas) {
	switch (input_group.GetType().InternalType()) {
	case PhysicalType::INT8:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<int8_t>(input_group, execute_sel, count,
		                                                                 compact_groups, count_deltas);
	case PhysicalType::INT16:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<int16_t>(input_group, execute_sel, count,
		                                                                  compact_groups, count_deltas);
	case PhysicalType::INT32:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<int32_t>(input_group, execute_sel, count,
		                                                                  compact_groups, count_deltas);
	case PhysicalType::INT64:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<int64_t>(input_group, execute_sel, count,
		                                                                  compact_groups, count_deltas);
	case PhysicalType::INT128:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<hugeint_t>(input_group, execute_sel, count,
		                                                                    compact_groups, count_deltas);
	case PhysicalType::UINT8:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<uint8_t>(input_group, execute_sel, count,
		                                                                  compact_groups, count_deltas);
	case PhysicalType::UINT16:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<uint16_t>(input_group, execute_sel, count,
		                                                                   compact_groups, count_deltas);
	case PhysicalType::UINT32:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<uint32_t>(input_group, execute_sel, count,
		                                                                   compact_groups, count_deltas);
	case PhysicalType::UINT64:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<uint64_t>(input_group, execute_sel, count,
		                                                                   compact_groups, count_deltas);
	case PhysicalType::UINT128:
		return TryPreaggregateFixedWidthCountStarVectorTemplated<uhugeint_t>(input_group, execute_sel, count,
		                                                                     compact_groups, count_deltas);
	default:
		return false;
	}
}

static bool TryPreaggregateFixedWidthCountStarGroups(DataChunk &input_groups, DataChunk &compact_groups,
                                                     vector<int64_t> &count_deltas) {
	if (input_groups.ColumnCount() != 1) {
		return false;
	}
	return TryPreaggregateFixedWidthCountStarVector(input_groups.data[0], input_groups.size(), compact_groups,
	                                                count_deltas);
}

static bool TryReadProjectionSourceReferenceIndex(const SljitNativeRegionExpressionPlan &projection,
                                                  idx_t &source_index) {
	if (projection.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		source_index = projection.source_index;
		return true;
	}
	if (projection.kind != SljitNativeRegionExpressionKind::EXPRESSION_TREE &&
	    projection.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
		return false;
	}
	if (!projection.expression_tree || projection.expression_tree->kind != ExecutionExpressionIRKind::REFERENCE ||
	    projection.expression_tree->ref_index >= projection.expression_tree_source_indices.size()) {
		return false;
	}
	source_index = projection.expression_tree_source_indices[projection.expression_tree->ref_index];
	return true;
}

static bool TryPreaggregateProjectedFixedWidthCountStarGroups(const SljitExecutableRegionOp &projection_op,
                                                              DataChunk &input, const SelectionVector *execute_sel,
                                                              idx_t count, DataChunk &compact_groups,
                                                              vector<int64_t> &count_deltas);

static bool TryPreaggregateProjectedFixedWidthCountStarGroups(const SljitExecutableRegionOp &projection_op,
                                                              DataChunk &input, DataChunk &compact_groups,
                                                              vector<int64_t> &count_deltas) {
	return TryPreaggregateProjectedFixedWidthCountStarGroups(projection_op, input, nullptr, input.size(),
	                                                         compact_groups, count_deltas);
}

static bool TryPreaggregateProjectedFixedWidthCountStarGroups(const SljitExecutableRegionOp &projection_op,
                                                              DataChunk &input, const SelectionVector *execute_sel,
                                                              idx_t count, DataChunk &compact_groups,
                                                              vector<int64_t> &count_deltas) {
	if (input.size() == 0 || projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
	    projection_op.projections.size() != 1 || projection_op.output_types.size() != 1 ||
	    compact_groups.ColumnCount() != 1 || compact_groups.data[0].GetType() != projection_op.output_types[0]) {
		return false;
	}
	auto &projection = projection_op.projections[0].plan;
	idx_t source_index;
	if (!TryReadProjectionSourceReferenceIndex(projection, source_index) || source_index >= input.ColumnCount() ||
	    projection.return_type != projection_op.output_types[0] ||
	    input.data[source_index].GetType() != projection_op.output_types[0]) {
		return false;
	}
	return TryPreaggregateFixedWidthCountStarVector(input.data[source_index], execute_sel, count, compact_groups,
	                                                count_deltas);
}

template <class T>
static bool
MergePreaggregatedFixedWidthCountStarGroupsTemplated(DataChunk &compact_groups, const vector<int64_t> &count_deltas,
                                                     std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
                                                     std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts,
                                                     idx_t &group_count) {
	if (compact_groups.ColumnCount() != 1 || count_deltas.size() < compact_groups.size()) {
		return false;
	}
	UnifiedVectorFormat format;
	compact_groups.data[0].ToUnifiedFormat(format);
	auto source_data = UnifiedVectorFormat::GetData<T>(format);
	auto source_sel = format.sel;
	auto &source_validity = format.validity;
	const bool can_have_null = source_validity.CanHaveNull();
	for (idx_t row_idx = 0; row_idx < compact_groups.size(); row_idx++) {
		const auto source_idx = source_sel->get_index(row_idx);
		if (can_have_null && !source_validity.RowIsValid(source_idx)) {
			return false;
		}
		if (!AccumulatePreaggregatedCountStarDeltaKey(source_data[source_idx], count_deltas[row_idx], keys, counts,
		                                              group_count)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
