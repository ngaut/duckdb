//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_row_pointer_preaggregation_groups.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_primitive_preaggregation_runtime.hpp"
#include "sljit_date_year_runtime.hpp"

namespace duckdb {

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

} // namespace duckdb
