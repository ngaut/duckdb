//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_row_pointer_preaggregation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_runtime.hpp"
#include "sljit_aggregate_primitive_preaggregation_runtime.hpp"
#include "sljit_date_year_runtime.hpp"

#include <array>

namespace duckdb {

enum class SljitRowPointerPreaggregationStrategy : uint8_t { DISABLED, CONSECUTIVE_GROUPS, LOCAL_DISTINCT_GROUPS };

static constexpr idx_t SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT = 64;
static constexpr idx_t SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS = 128;

struct SljitRowPointerPreaggregationSampleStats {
	idx_t sample_count = 0;
	idx_t sample_group_count = 0;
	idx_t sample_unique_group_count = 0;

	bool HasConsecutiveRepeat() const {
		return sample_group_count < sample_count;
	}

	bool HasInterleavedRepeat() const {
		return sample_unique_group_count < sample_group_count;
	}

	bool LocalDistinctBeatsConsecutive(idx_t min_reduction = 2) const {
		return HasInterleavedRepeat() && sample_unique_group_count * min_reduction <= sample_group_count;
	}
};

struct SljitRowPointerPreaggregationDecision {
	SljitRowPointerPreaggregationStrategy strategy = SljitRowPointerPreaggregationStrategy::DISABLED;
	SljitRowPointerPreaggregationSampleStats sample;
	bool has_variable_width_groups = false;

	bool UseConsecutiveGroups() const {
		return strategy == SljitRowPointerPreaggregationStrategy::CONSECUTIVE_GROUPS;
	}

	bool UseLocalDistinctGroups() const {
		return strategy == SljitRowPointerPreaggregationStrategy::LOCAL_DISTINCT_GROUPS;
	}
};

static bool SljitSameRowPointerIsEqual(const vector<ExecutionRowPointerGroupKeySource> &group_sources);
static bool SljitRowPointerPreaggregationSourcesRepeatWithRowPointer(
    const vector<ExecutionRowPointerGroupKeySource> &group_sources);
static bool SljitRowPointerGroupKeysEqual(data_ptr_t left, data_ptr_t right, bool same_row_pointer_is_equal,
                                          const vector<ExecutionRowPointerGroupKeySource> &group_sources);
static bool
SljitRowPointerPreaggregationUsesInputVectorGroups(const vector<ExecutionRowPointerGroupKeySource> &group_sources);
static bool
SljitRowPointerPreaggregationHasVariableWidthGroups(const vector<ExecutionRowPointerGroupKeySource> &group_sources);
static bool SljitTryPrepareInputVectorGroupFormats(DataChunk &payload_input,
                                                   const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                   vector<UnifiedVectorFormat> &input_group_formats);
static bool SljitTryPrepareRowPointerGroupSources(const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                  vector<ExecutionRowPointerGroupKeySource> &row_pointer_group_sources);
static bool SljitInputVectorGroupRowsEqual(const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                           const vector<UnifiedVectorFormat> &input_group_formats, idx_t left_row,
                                           idx_t right_row);
static bool
SljitRowPointerPreaggregationRowsEqual(data_ptr_t left_row_pointer, idx_t left_row, data_ptr_t right_row_pointer,
                                       idx_t right_row, bool uses_input_vector_groups, bool same_row_pointer_is_equal,
                                       const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                       const vector<ExecutionRowPointerGroupKeySource> &row_pointer_group_sources,
                                       const vector<UnifiedVectorFormat> &input_group_formats);
static bool
SljitTryCollectRowPointerPreaggregationSampleStats(DataChunk &payload_input, Vector &row_pointers, idx_t count,
                                                   const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                   vector<UnifiedVectorFormat> &input_group_formats,
                                                   SljitRowPointerPreaggregationSampleStats &stats);
static bool SljitTryCollectRowPointerIdentityPreaggregationSampleStats(Vector &row_pointers, idx_t count,
                                                                       SljitRowPointerPreaggregationSampleStats &stats);
static SljitRowPointerPreaggregationDecision
SljitChooseRowPointerPreaggregationStrategy(DataChunk &payload_input, Vector &row_pointers, idx_t count,
                                            const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                            vector<UnifiedVectorFormat> &input_group_formats);
static bool SljitSparseRowPointerTargetCacheTypeSupported(PhysicalType target_type);

static bool SljitRowPointerGroupSourceIsVariableWidth(const ExecutionRowPointerGroupKeySource &source) {
	const auto physical_type = source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR
	                               ? source.source_physical_type
	                               : source.target_physical_type;
	return physical_type == PhysicalType::VARCHAR;
}

static bool
SljitRowPointerPreaggregationHasVariableWidthGroups(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	for (auto &source : group_sources) {
		if (SljitRowPointerGroupSourceIsVariableWidth(source)) {
			return true;
		}
	}
	return false;
}

static bool
SljitTryCollectRowPointerPreaggregationSampleStats(DataChunk &payload_input, Vector &row_pointers, idx_t count,
                                                   const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                   vector<UnifiedVectorFormat> &input_group_formats,
                                                   SljitRowPointerPreaggregationSampleStats &stats) {
	stats = SljitRowPointerPreaggregationSampleStats();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	vector<ExecutionRowPointerGroupKeySource> row_pointer_group_sources;
	if (!SljitTryPrepareRowPointerGroupSources(group_sources, row_pointer_group_sources)) {
		return false;
	}
	if (SljitRowPointerPreaggregationSourcesRepeatWithRowPointer(group_sources) &&
	    SljitSameRowPointerIsEqual(row_pointer_group_sources)) {
		return SljitTryCollectRowPointerIdentityPreaggregationSampleStats(row_pointers, count, stats);
	}
	const bool uses_input_vector_groups = SljitRowPointerPreaggregationUsesInputVectorGroups(group_sources);
	if (uses_input_vector_groups &&
	    !SljitTryPrepareInputVectorGroupFormats(payload_input, group_sources, input_group_formats)) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	stats.sample_count = MinValue<idx_t>(count, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT);
	const bool same_row_pointer_is_equal = SljitSameRowPointerIsEqual(row_pointer_group_sources);
	data_ptr_t active_row_pointer = nullptr;
	idx_t active_row_idx = DConstants::INVALID_INDEX;
	bool has_active_row_pointer = false;
	std::array<data_ptr_t, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT> unique_row_pointers;
	std::array<idx_t, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT> unique_row_indices;
	for (idx_t row_idx = 0; row_idx < stats.sample_count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			return false;
		}
		bool same_group = false;
		if (has_active_row_pointer) {
			same_group = SljitRowPointerPreaggregationRowsEqual(
			    active_row_pointer, active_row_idx, row_pointer, row_idx, uses_input_vector_groups,
			    same_row_pointer_is_equal, group_sources, row_pointer_group_sources, input_group_formats);
		}
		if (!same_group) {
			active_row_pointer = row_pointer;
			active_row_idx = row_idx;
			has_active_row_pointer = true;
			stats.sample_group_count++;
		}
		bool found_unique_group = false;
		for (idx_t unique_idx = 0; unique_idx < stats.sample_unique_group_count; unique_idx++) {
			if (SljitRowPointerPreaggregationRowsEqual(unique_row_pointers[unique_idx], unique_row_indices[unique_idx],
			                                           row_pointer, row_idx, uses_input_vector_groups,
			                                           same_row_pointer_is_equal, group_sources,
			                                           row_pointer_group_sources, input_group_formats)) {
				found_unique_group = true;
				break;
			}
		}
		if (!found_unique_group) {
			unique_row_pointers[stats.sample_unique_group_count] = row_pointer;
			unique_row_indices[stats.sample_unique_group_count] = row_idx;
			stats.sample_unique_group_count++;
		}
	}
	return stats.HasConsecutiveRepeat() || stats.HasInterleavedRepeat();
}

static bool
SljitTryCollectRowPointerIdentityPreaggregationSampleStats(Vector &row_pointers, idx_t count,
                                                           SljitRowPointerPreaggregationSampleStats &stats) {
	stats = SljitRowPointerPreaggregationSampleStats();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	stats.sample_count = MinValue<idx_t>(count, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT);
	data_ptr_t active_row_pointer = nullptr;
	bool has_active_row_pointer = false;
	std::array<data_ptr_t, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT> unique_row_pointers;
	for (idx_t row_idx = 0; row_idx < stats.sample_count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			return false;
		}
		if (!has_active_row_pointer || active_row_pointer != row_pointer) {
			active_row_pointer = row_pointer;
			has_active_row_pointer = true;
			stats.sample_group_count++;
		}
		bool found_unique_group = false;
		for (idx_t unique_idx = 0; unique_idx < stats.sample_unique_group_count; unique_idx++) {
			if (unique_row_pointers[unique_idx] == row_pointer) {
				found_unique_group = true;
				break;
			}
		}
		if (!found_unique_group) {
			unique_row_pointers[stats.sample_unique_group_count] = row_pointer;
			stats.sample_unique_group_count++;
		}
	}
	return stats.HasConsecutiveRepeat() || stats.HasInterleavedRepeat();
}

static SljitRowPointerPreaggregationDecision
SljitChooseRowPointerPreaggregationStrategy(DataChunk &payload_input, Vector &row_pointers, idx_t count,
                                            const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                            vector<UnifiedVectorFormat> &input_group_formats) {
	SljitRowPointerPreaggregationDecision decision;
	decision.has_variable_width_groups = SljitRowPointerPreaggregationHasVariableWidthGroups(group_sources);
	if (!SljitTryCollectRowPointerPreaggregationSampleStats(payload_input, row_pointers, count, group_sources,
	                                                        input_group_formats, decision.sample)) {
		return decision;
	}
	if (!decision.has_variable_width_groups) {
		if (decision.sample.LocalDistinctBeatsConsecutive()) {
			decision.strategy = SljitRowPointerPreaggregationStrategy::LOCAL_DISTINCT_GROUPS;
		} else if (decision.sample.HasConsecutiveRepeat()) {
			decision.strategy = SljitRowPointerPreaggregationStrategy::CONSECUTIVE_GROUPS;
		}
		return decision;
	}
	constexpr idx_t VARIABLE_WIDTH_PREAGGREGATION_MIN_REDUCTION = 4;
	if (decision.sample.LocalDistinctBeatsConsecutive(VARIABLE_WIDTH_PREAGGREGATION_MIN_REDUCTION)) {
		decision.strategy = SljitRowPointerPreaggregationStrategy::LOCAL_DISTINCT_GROUPS;
	} else if (decision.sample.HasConsecutiveRepeat() &&
	           decision.sample.sample_group_count * VARIABLE_WIDTH_PREAGGREGATION_MIN_REDUCTION <=
	               decision.sample.sample_count) {
		decision.strategy = SljitRowPointerPreaggregationStrategy::CONSECUTIVE_GROUPS;
	}
	return decision;
}

static bool SljitSparseRowPointerTargetCacheTypeSupported(PhysicalType target_type) {
	switch (target_type) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
	case PhysicalType::UINT16:
	case PhysicalType::INT16:
	case PhysicalType::UINT32:
	case PhysicalType::INT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT64:
		return true;
	default:
		return false;
	}
}

static bool
SljitShouldPreferDirectSparseRowPointerTargetUpdate(Vector &row_pointers, idx_t count,
                                                    const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR || group_sources.size() != 1) {
		return false;
	}
	auto &source = group_sources[0];
	if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD || !source.ready ||
	    !SljitSparseRowPointerTargetCacheTypeSupported(source.target_physical_type)) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	const auto sample_count = MinValue<idx_t>(count, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		for (idx_t prev_idx = 0; prev_idx < row_idx; prev_idx++) {
			if (ExecutionRowPointerGroupKeysEqual(row_pointer_data[prev_idx], row_pointer_data[row_idx],
			                                      group_sources)) {
				return true;
			}
		}
	}
	return false;
}

static bool SljitSameRowPointerIsEqual(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	bool same_row_pointer_is_equal = true;
	for (auto &source : group_sources) {
		same_row_pointer_is_equal = same_row_pointer_is_equal && source.all_valid;
	}
	return same_row_pointer_is_equal;
}

static bool SljitRowPointerPreaggregationSourcesRepeatWithRowPointer(
    const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	for (auto &source : group_sources) {
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			continue;
		}
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR &&
		    source.input_vector_repeats_with_row_pointer) {
			continue;
		}
		return false;
	}
	return !group_sources.empty();
}

static bool SljitRowPointerGroupKeysEqual(data_ptr_t left, data_ptr_t right, bool same_row_pointer_is_equal,
                                          const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	return (same_row_pointer_is_equal && left == right) ||
	       ExecutionRowPointerGroupKeysEqual(left, right, group_sources);
}

static bool
SljitRowPointerPreaggregationUsesInputVectorGroups(const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	bool uses_input_vector_group = false;
	for (auto &source : group_sources) {
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			uses_input_vector_group = true;
			continue;
		}
		if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			return false;
		}
	}
	return uses_input_vector_group;
}

static bool SljitTryPrepareInputVectorGroupFormats(DataChunk &payload_input,
                                                   const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                   vector<UnifiedVectorFormat> &input_group_formats) {
	input_group_formats.resize(group_sources.size());
	bool has_input_vector_group = false;
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			continue;
		}
		if (!SljitPreaggregationInputVectorGroupSourceSupported(payload_input, source)) {
			return false;
		}
		payload_input.data[source.input_vector_index].ToUnifiedFormat(input_group_formats[group_idx]);
		has_input_vector_group = true;
	}
	return has_input_vector_group;
}

static bool
SljitTryPrepareRowPointerGroupSources(const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                      vector<ExecutionRowPointerGroupKeySource> &row_pointer_group_sources) {
	row_pointer_group_sources.clear();
	for (auto &source : group_sources) {
		switch (source.source_kind) {
		case ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD:
			if (!source.ready) {
				return false;
			}
			row_pointer_group_sources.push_back(source);
			break;
		case ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR:
			break;
		default:
			return false;
		}
	}
	return !group_sources.empty();
}

template <class T>
static bool SljitInputVectorGroupTypedRowsEqual(const UnifiedVectorFormat &format, idx_t left_row, idx_t right_row) {
	const auto left_idx = format.sel->get_index(left_row);
	const auto right_idx = format.sel->get_index(right_row);
	const bool left_valid = format.validity.RowIsValid(left_idx);
	const bool right_valid = format.validity.RowIsValid(right_idx);
	if (!left_valid || !right_valid) {
		return left_valid == right_valid;
	}
	auto data = UnifiedVectorFormat::GetData<T>(format);
	return data[left_idx] == data[right_idx];
}

template <class SRC, class DST>
static bool SljitInputVectorGroupTypedCastRowsEqual(const UnifiedVectorFormat &format, idx_t left_row, idx_t right_row,
                                                    bool unchecked) {
	const auto left_idx = format.sel->get_index(left_row);
	const auto right_idx = format.sel->get_index(right_row);
	const bool left_valid = format.validity.RowIsValid(left_idx);
	const bool right_valid = format.validity.RowIsValid(right_idx);
	if (!left_valid || !right_valid) {
		return left_valid == right_valid;
	}
	auto data = UnifiedVectorFormat::GetData<SRC>(format);
	if (unchecked) {
		return static_cast<DST>(data[left_idx]) == static_cast<DST>(data[right_idx]);
	}
	DST left_value;
	DST right_value;
	return TryCast::Operation<SRC, DST>(data[left_idx], left_value, false) &&
	       TryCast::Operation<SRC, DST>(data[right_idx], right_value, false) && left_value == right_value;
}

static bool SljitInputVectorGroupPhysicalRowsEqual(PhysicalType type, const UnifiedVectorFormat &format, idx_t left_row,
                                                   idx_t right_row) {
	switch (type) {
	case PhysicalType::BOOL:
		return SljitInputVectorGroupTypedRowsEqual<bool>(format, left_row, right_row);
	case PhysicalType::INT8:
		return SljitInputVectorGroupTypedRowsEqual<int8_t>(format, left_row, right_row);
	case PhysicalType::INT16:
		return SljitInputVectorGroupTypedRowsEqual<int16_t>(format, left_row, right_row);
	case PhysicalType::INT32:
		return SljitInputVectorGroupTypedRowsEqual<int32_t>(format, left_row, right_row);
	case PhysicalType::INT64:
		return SljitInputVectorGroupTypedRowsEqual<int64_t>(format, left_row, right_row);
	case PhysicalType::INT128:
		return SljitInputVectorGroupTypedRowsEqual<hugeint_t>(format, left_row, right_row);
	case PhysicalType::UINT8:
		return SljitInputVectorGroupTypedRowsEqual<uint8_t>(format, left_row, right_row);
	case PhysicalType::UINT16:
		return SljitInputVectorGroupTypedRowsEqual<uint16_t>(format, left_row, right_row);
	case PhysicalType::UINT32:
		return SljitInputVectorGroupTypedRowsEqual<uint32_t>(format, left_row, right_row);
	case PhysicalType::UINT64:
		return SljitInputVectorGroupTypedRowsEqual<uint64_t>(format, left_row, right_row);
	case PhysicalType::UINT128:
		return SljitInputVectorGroupTypedRowsEqual<uhugeint_t>(format, left_row, right_row);
	case PhysicalType::VARCHAR:
		return SljitInputVectorGroupTypedRowsEqual<string_t>(format, left_row, right_row);
	default:
		return false;
	}
}

template <class DST>
static bool SljitInputVectorGroupDateYearCompressRowsEqual(const UnifiedVectorFormat &format, idx_t left_row,
                                                           idx_t right_row,
                                                           const ExecutionRowPointerGroupKeySource &source) {
	const auto left_idx = format.sel->get_index(left_row);
	const auto right_idx = format.sel->get_index(right_row);
	const bool left_valid = format.validity.RowIsValid(left_idx);
	const bool right_valid = format.validity.RowIsValid(right_idx);
	if (!left_valid || !right_valid) {
		return left_valid == right_valid;
	}
	auto data = UnifiedVectorFormat::GetData<int32_t>(format);
	auto load_key = [&](idx_t source_idx, DST &result) {
		return SljitTryDateYearCompressedGroupKey<DST>(data[source_idx], source.cast_constant, result);
	};
	DST left_value;
	DST right_value;
	return load_key(left_idx, left_value) && load_key(right_idx, right_value) && left_value == right_value;
}

static bool SljitInputVectorGroupDateYearCompressRowsEqual(const UnifiedVectorFormat &format, idx_t left_row,
                                                           idx_t right_row,
                                                           const ExecutionRowPointerGroupKeySource &source) {
	if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE) {
		return false;
	}
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return SljitInputVectorGroupDateYearCompressRowsEqual<uint8_t>(format, left_row, right_row, source);
	case PhysicalType::UINT16:
		return SljitInputVectorGroupDateYearCompressRowsEqual<uint16_t>(format, left_row, right_row, source);
	case PhysicalType::UINT32:
		return SljitInputVectorGroupDateYearCompressRowsEqual<uint32_t>(format, left_row, right_row, source);
	case PhysicalType::UINT64:
		return SljitInputVectorGroupDateYearCompressRowsEqual<uint64_t>(format, left_row, right_row, source);
	default:
		return false;
	}
}

static bool SljitInputVectorGroupRowsEqual(const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                           const vector<UnifiedVectorFormat> &input_group_formats, idx_t left_row,
                                           idx_t right_row) {
	if (group_sources.size() != input_group_formats.size()) {
		return false;
	}
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		auto &source = group_sources[group_idx];
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			continue;
		}
		if (!SljitPreaggregationInputVectorGroupCastSupported(source)) {
			return false;
		}
		auto &format = input_group_formats[group_idx];
		switch (source.cast_kind) {
		case ExecutionRowPointerGroupKeyCastKind::NONE:
			if (!SljitInputVectorGroupPhysicalRowsEqual(source.source_physical_type, format, left_row, right_row)) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
			if (!SljitInputVectorGroupTypedCastRowsEqual<int64_t, int32_t>(format, left_row, right_row,
			                                                               source.unchecked_integral_cast)) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
			if (!SljitInputVectorGroupTypedCastRowsEqual<int64_t, int16_t>(format, left_row, right_row,
			                                                               source.unchecked_integral_cast)) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
			if (!SljitInputVectorGroupTypedCastRowsEqual<int32_t, int8_t>(format, left_row, right_row,
			                                                              source.unchecked_integral_cast)) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS:
		case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
			if (!SljitInputVectorGroupPhysicalRowsEqual(source.source_physical_type, format, left_row, right_row)) {
				return false;
			}
			break;
		case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
			if (!SljitInputVectorGroupDateYearCompressRowsEqual(format, left_row, right_row, source)) {
				return false;
			}
			break;
		default:
			return false;
		}
	}
	return true;
}

static bool
SljitRowPointerPreaggregationRowsEqual(data_ptr_t left_row_pointer, idx_t left_row, data_ptr_t right_row_pointer,
                                       idx_t right_row, bool uses_input_vector_groups, bool same_row_pointer_is_equal,
                                       const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                       const vector<ExecutionRowPointerGroupKeySource> &row_pointer_group_sources,
                                       const vector<UnifiedVectorFormat> &input_group_formats) {
	const bool input_vector_groups_match =
	    !uses_input_vector_groups ||
	    SljitInputVectorGroupRowsEqual(group_sources, input_group_formats, left_row, right_row);
	const bool row_pointer_groups_match =
	    row_pointer_group_sources.empty() ||
	    SljitRowPointerGroupKeysEqual(left_row_pointer, right_row_pointer, same_row_pointer_is_equal,
	                                  row_pointer_group_sources);
	return input_vector_groups_match && row_pointer_groups_match;
}

template <class START_GROUP, class VISIT_ROW>
static bool SljitForEachPreaggregatedRowPointerGroup(DataChunk &payload_input, Vector &row_pointers,
                                                     const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                     vector<UnifiedVectorFormat> &input_group_formats,
                                                     START_GROUP &&start_group, VISIT_ROW &&visit_row,
                                                     idx_t &group_count) {
	group_count = 0;
	const auto count = payload_input.size();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	auto preaggregation_decision = SljitChooseRowPointerPreaggregationStrategy(payload_input, row_pointers, count,
	                                                                           group_sources, input_group_formats);
	if (!preaggregation_decision.UseConsecutiveGroups() && !preaggregation_decision.UseLocalDistinctGroups()) {
		return false;
	}

	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	const bool uses_input_vector_groups = SljitRowPointerPreaggregationUsesInputVectorGroups(group_sources);
	vector<ExecutionRowPointerGroupKeySource> row_pointer_group_sources;
	if (!SljitTryPrepareRowPointerGroupSources(group_sources, row_pointer_group_sources)) {
		return false;
	}
	const bool same_row_pointer_is_equal = SljitSameRowPointerIsEqual(row_pointer_group_sources);
	data_ptr_t active_row_pointer = nullptr;
	idx_t active_row_idx = DConstants::INVALID_INDEX;
	bool has_active_row_pointer = false;
	std::array<data_ptr_t, SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS> local_group_row_pointers;
	std::array<idx_t, SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS> local_group_row_indices;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			return false;
		}
		idx_t group_idx = group_count;
		if (preaggregation_decision.UseConsecutiveGroups()) {
			bool same_group = false;
			if (has_active_row_pointer) {
				same_group = SljitRowPointerPreaggregationRowsEqual(
				    active_row_pointer, active_row_idx, row_pointer, row_idx, uses_input_vector_groups,
				    same_row_pointer_is_equal, group_sources, row_pointer_group_sources, input_group_formats);
			}
			if (!same_group) {
				active_row_pointer = row_pointer;
				active_row_idx = row_idx;
				has_active_row_pointer = true;
				if (!start_group(row_idx, row_pointer, group_count)) {
					return false;
				}
				group_count++;
			}
			group_idx = group_count - 1;
		} else {
			bool found_group = false;
			for (idx_t candidate_idx = 0; candidate_idx < group_count; candidate_idx++) {
				if (SljitRowPointerPreaggregationRowsEqual(
				        local_group_row_pointers[candidate_idx], local_group_row_indices[candidate_idx], row_pointer,
				        row_idx, uses_input_vector_groups, same_row_pointer_is_equal, group_sources,
				        row_pointer_group_sources, input_group_formats)) {
					group_idx = candidate_idx;
					found_group = true;
					break;
				}
			}
			if (!found_group) {
				if (group_count >= SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS) {
					return false;
				}
				if (!start_group(row_idx, row_pointer, group_count)) {
					return false;
				}
				local_group_row_pointers[group_count] = row_pointer;
				local_group_row_indices[group_count] = row_idx;
				group_idx = group_count;
				group_count++;
			}
		}
		if (!visit_row(row_idx, group_idx)) {
			return false;
		}
	}
	return group_count != count;
}

static bool SljitTryPreaggregateRowPointerPrimitiveGroups(
    DataChunk &payload_input, Vector &row_pointers, const vector<ExecutionRowPointerGroupKeySource> &group_sources,
    const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, Vector &compact_row_pointers,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, idx_t &compact_count) {
	compact_count = 0;
	const auto count = payload_input.size();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}

	SljitPreaggregatedPrimitivePayloadSources payload_sources;
	if (!payload_sources.Prepare(payload_input, payload_source_indices, payload_lanes)) {
		return false;
	}
	scratch.Prepare(payload_lanes, count);
	compact_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(compact_row_pointers, count_t(count));
	auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(compact_row_pointers);
	auto start_group = [&](idx_t row_idx, data_ptr_t row_pointer, idx_t group_idx) {
		compact_row_pointer_data[group_idx] = row_pointer;
		scratch.group_rows.push_back(static_cast<sel_t>(row_idx));
		return SljitStartPreaggregatedPrimitivePayloadGroup(scratch, payload_lanes);
	};
	auto visit_row = [&](idx_t row_idx, idx_t group_idx) {
		return SljitAccumulatePreaggregatedPrimitivePayloadGroup(payload_sources, scratch, payload_lanes, row_idx,
		                                                         group_idx);
	};
	idx_t group_count;
	if (!SljitForEachPreaggregatedRowPointerGroup(payload_input, row_pointers, group_sources,
	                                              scratch.input_group_formats, start_group, visit_row, group_count)) {
		return false;
	}
	FlatVector::SetSize(compact_row_pointers, count_t(group_count));
	compact_count = group_count;
	return true;
}

static bool SljitTryPreaggregateRowPointerFusedPrimitiveGroups(
    SljitExecutableRegionOp &op, DataChunk &payload_input, Vector &row_pointers,
    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, Vector &compact_row_pointers,
    SljitPreaggregatedPrimitiveAggregateScratch &scratch, SljitAggregatePayloadAdapterScratch &payload_scratch,
    idx_t &compact_count) {
	compact_count = 0;
	const auto count = payload_input.size();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    !op.aggregate_update.fused_payload_update_function) {
		return false;
	}

	idx_t state_stride = 0;
	for (auto lane : payload_lanes) {
		if (!lane || !lane->ready || lane->state_size == 0) {
			return false;
		}
		state_stride = MaxValue<idx_t>(state_stride, lane->state_offset + lane->state_size);
	}
	if (state_stride == 0) {
		return false;
	}
	state_stride = AlignValue<idx_t>(state_stride);
	scratch.Prepare(payload_lanes, count);
	scratch.fused_state_stride = state_stride;
	scratch.fused_state_storage.assign(count * state_stride, 0);
	scratch.fused_row_state_addresses.resize(count);

	compact_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(compact_row_pointers, count_t(count));
	auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(compact_row_pointers);
	auto state_base = scratch.fused_state_storage.data();
	auto start_group = [&](idx_t row_idx, data_ptr_t row_pointer, idx_t group_idx) {
		compact_row_pointer_data[group_idx] = row_pointer;
		scratch.group_rows.push_back(static_cast<sel_t>(row_idx));
		scratch.group_row_counts.push_back(0);
		return true;
	};
	auto visit_row = [&](idx_t row_idx, idx_t group_idx) {
		D_ASSERT(group_idx < scratch.group_row_counts.size());
		scratch.group_row_counts[group_idx]++;
		scratch.fused_row_state_addresses[row_idx] = reinterpret_cast<uintptr_t>(state_base + group_idx * state_stride);
		return true;
	};
	idx_t group_count;
	if (!SljitForEachPreaggregatedRowPointerGroup(payload_input, row_pointers, group_sources,
	                                              scratch.input_group_formats, start_group, visit_row, group_count)) {
		return false;
	}

	SljitExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
	    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function,
	    op.aggregate_update.plan.sink_info.aggregates, op.aggregate_update.plan.sink_info.aggregate_contract,
	    payload_lanes, payload_input, scratch.fused_row_state_addresses.data(), nullptr, nullptr, false, count,
	    payload_scratch, optional_ptr<const vector<idx_t>>(&payload_source_indices));

	for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
		auto lane = payload_lanes[payload_idx];
		auto &payload = scratch.payloads[payload_idx];
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			auto group_state = state_base + group_idx * state_stride + lane->state_offset;
			auto value_ptr = group_state + lane->state_value_offset;
			switch (lane->kind) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR:
				payload.int64_values.push_back(*reinterpret_cast<int64_t *>(value_ptr));
				break;
			case AggregatePrimitiveUpdateKind::SUM_INT64: {
				auto state_is_set = *reinterpret_cast<bool *>(group_state + lane->state_is_set_offset);
				payload.int64_values.push_back(state_is_set ? *reinterpret_cast<int64_t *>(value_ptr) : 0);
				payload.value_is_set.push_back(state_is_set ? 1 : 0);
				break;
			}
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
				auto state_is_set = *reinterpret_cast<bool *>(group_state + lane->state_is_set_offset);
				payload.hugeint_values.push_back(state_is_set ? *reinterpret_cast<hugeint_t *>(value_ptr)
				                                              : hugeint_t(0));
				payload.value_is_set.push_back(state_is_set ? 1 : 0);
				break;
			}
			default:
				return false;
			}
		}
	}

	FlatVector::SetSize(compact_row_pointers, count_t(group_count));
	compact_count = group_count;
	return true;
}
} // namespace duckdb
