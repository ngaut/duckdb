//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_input_vector_groups.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_date_year_runtime.hpp"
#include "sljit_grouped_aggregate_group_key_source.hpp"
#include "sljit_region_adapter_scratch.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"

namespace duckdb {

static bool SljitGroupSourceCanMaterializeFromInputVector(DataChunk &payload_input,
                                                          const ExecutionRowPointerGroupKeySource &source) {
	if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    source.input_vector_index >= payload_input.ColumnCount() ||
	    payload_input.data[source.input_vector_index].GetType() != source.source_type) {
		return false;
	}
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		return source.source_physical_type == source.target_physical_type;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		return source.source_physical_type == PhysicalType::INT64 && source.target_physical_type == PhysicalType::INT32;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		return source.source_physical_type == PhysicalType::INT64 && source.target_physical_type == PhysicalType::INT16;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		return source.source_physical_type == PhysicalType::INT32 && source.target_physical_type == PhysicalType::INT8;
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		return source.source_physical_type == PhysicalType::INT32 && source.source_type.id() == LogicalTypeId::DATE &&
		       source.target_physical_type == PhysicalType::UINT8;
	default:
		return false;
	}
}

static bool
SljitGroupSourcesCanMaterializeFromInputVectors(DataChunk &payload_input,
                                                const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (group_sources.empty()) {
		return false;
	}
	for (auto &source : group_sources) {
		if (!SljitGroupSourceCanMaterializeFromInputVector(payload_input, source)) {
			return false;
		}
	}
	return true;
}

template <class SRC, class DST>
static bool SljitInputVectorGroupBatchFitsCast(DataChunk &payload_input,
                                               const ExecutionRowPointerGroupKeySource &source, idx_t count) {
	if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    source.input_vector_index >= payload_input.ColumnCount() ||
	    payload_input.data[source.input_vector_index].GetType() != source.source_type) {
		return false;
	}
	UnifiedVectorFormat source_format;
	payload_input.data[source.input_vector_index].ToUnifiedFormat(source_format);
	auto source_data = UnifiedVectorFormat::GetData<SRC>(source_format);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = source_format.sel->get_index(row_idx);
		if (source_format.validity.RowIsValid(source_idx) &&
		    (source_data[source_idx] < NumericLimits<DST>::Minimum() ||
		     source_data[source_idx] > NumericLimits<DST>::Maximum())) {
			return false;
		}
	}
	return true;
}

static bool SljitInputVectorGroupBatchCastFits(DataChunk &payload_input,
                                               const ExecutionRowPointerGroupKeySource &source, idx_t count) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		if (source.source_physical_type != PhysicalType::INT64 || source.target_physical_type != PhysicalType::INT32) {
			return false;
		}
		return SljitInputVectorGroupBatchFitsCast<int64_t, int32_t>(payload_input, source, count);
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		if (source.source_physical_type != PhysicalType::INT64 || source.target_physical_type != PhysicalType::INT16) {
			return false;
		}
		return SljitInputVectorGroupBatchFitsCast<int64_t, int16_t>(payload_input, source, count);
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		if (source.source_physical_type != PhysicalType::INT32 || source.target_physical_type != PhysicalType::INT8) {
			return false;
		}
		return SljitInputVectorGroupBatchFitsCast<int32_t, int8_t>(payload_input, source, count);
	default:
		return false;
	}
}

static void SljitApplyInputVectorGroupBatchCastProofs(DataChunk &payload_input,
                                                      vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                      idx_t count) {
	for (auto &source : group_sources) {
		if (source.unchecked_integral_cast) {
			continue;
		}
		source.unchecked_integral_cast = SljitInputVectorGroupBatchCastFits(payload_input, source, count);
	}
}

static bool SljitRowPointerGroupKeySourceValueIsValid(data_ptr_t row_pointer,
                                                      const ExecutionRowPointerGroupKeySource &source,
                                                      bool &source_is_valid) {
	source_is_valid = false;
	if (!row_pointer) {
		return false;
	}
	if (source.all_valid) {
		source_is_valid = true;
		return true;
	}
	if (source.row_layout_column_idx == DConstants::INVALID_INDEX || source.row_layout_column_count == 0) {
		return false;
	}
	idx_t entry_idx;
	idx_t idx_in_entry;
	TupleDataLayout::ValidityBytes::GetEntryIndex(source.row_layout_column_idx, entry_idx, idx_in_entry);
	source_is_valid = TupleDataLayout::ValidityBytes::RowIsValid(
	    TupleDataLayout::ValidityBytes(row_pointer, source.row_layout_column_count).GetValidityEntryUnsafe(entry_idx),
	    idx_in_entry);
	return true;
}

template <class SRC, class DST>
static bool SljitRowPointerGroupBatchFitsCast(Vector &row_pointers, const ExecutionRowPointerGroupKeySource &source,
                                              idx_t count) {
	if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD ||
	    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    source.row_layout_offset == DConstants::INVALID_INDEX) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		bool source_is_valid;
		auto row_pointer = row_pointer_data[row_idx];
		if (!SljitRowPointerGroupKeySourceValueIsValid(row_pointer, source, source_is_valid)) {
			return false;
		}
		if (!source_is_valid) {
			continue;
		}
		const auto value = Load<SRC>(row_pointer + source.row_layout_offset);
		if (value < NumericLimits<DST>::Minimum() || value > NumericLimits<DST>::Maximum()) {
			return false;
		}
	}
	return true;
}

static bool SljitRowPointerGroupBatchCastFits(Vector &row_pointers, const ExecutionRowPointerGroupKeySource &source,
                                              idx_t count) {
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		if (source.source_physical_type != PhysicalType::INT64 || source.target_physical_type != PhysicalType::INT32) {
			return false;
		}
		return SljitRowPointerGroupBatchFitsCast<int64_t, int32_t>(row_pointers, source, count);
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		if (source.source_physical_type != PhysicalType::INT64 || source.target_physical_type != PhysicalType::INT16) {
			return false;
		}
		return SljitRowPointerGroupBatchFitsCast<int64_t, int16_t>(row_pointers, source, count);
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		if (source.source_physical_type != PhysicalType::INT32 || source.target_physical_type != PhysicalType::INT8) {
			return false;
		}
		return SljitRowPointerGroupBatchFitsCast<int32_t, int8_t>(row_pointers, source, count);
	default:
		return false;
	}
}

static void SljitApplyRowPointerGroupBatchCastProofs(Vector &row_pointers,
                                                     vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                     idx_t count) {
	for (auto &source : group_sources) {
		if (source.unchecked_integral_cast) {
			continue;
		}
		source.unchecked_integral_cast = SljitRowPointerGroupBatchCastFits(row_pointers, source, count);
	}
}

template <class SRC, class DST>
static void SljitMaterializeInputVectorGroupCast(Vector &source, Vector &target, idx_t count, bool unchecked) {
	target.SetVectorType(VectorType::FLAT_VECTOR);
	auto &target_validity = FlatVector::ValidityMutable(target);
	target_validity.Reset(count);
	target_validity.EnsureWritable();
	target_validity.SetAllValid(count);
	auto target_data = FlatVector::GetDataMutable<DST>(target);
	if (source.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::Validity(source).CheckAllValid(count)) {
		auto source_data = FlatVector::GetData<SRC>(source);
		if (unchecked) {
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				target_data[row_idx] = static_cast<DST>(source_data[row_idx]);
			}
		} else {
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				const auto value = source_data[row_idx];
				if (!TryCast::Operation<SRC, DST>(value, target_data[row_idx], false)) {
					throw InvalidInputException(CastExceptionText<SRC, DST>(value));
				}
			}
		}
		FlatVector::SetSize(target, count);
		return;
	}

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_format);
	auto source_data = UnifiedVectorFormat::GetData<SRC>(source_format);
	if (!source_format.validity.CanHaveNull()) {
		if (unchecked) {
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				target_data[row_idx] = static_cast<DST>(source_data[source_format.sel->get_index(row_idx)]);
			}
		} else {
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				const auto value = source_data[source_format.sel->get_index(row_idx)];
				if (!TryCast::Operation<SRC, DST>(value, target_data[row_idx], false)) {
					throw InvalidInputException(CastExceptionText<SRC, DST>(value));
				}
			}
		}
	} else {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_idx = source_format.sel->get_index(row_idx);
			if (!source_format.validity.RowIsValid(source_idx)) {
				target_validity.SetInvalid(row_idx);
				continue;
			}
			if (unchecked) {
				target_data[row_idx] = static_cast<DST>(source_data[source_idx]);
			} else if (!TryCast::Operation<SRC, DST>(source_data[source_idx], target_data[row_idx], false)) {
				throw InvalidInputException(CastExceptionText<SRC, DST>(source_data[source_idx]));
			}
		}
	}
	FlatVector::SetSize(target, count);
}

template <class DST>
static void SljitMaterializeDateYearCompressedInputVectorGroup(Vector &source, Vector &target, idx_t count,
                                                              int64_t minimum) {
	target.SetVectorType(VectorType::FLAT_VECTOR);
	auto &target_validity = FlatVector::ValidityMutable(target);
	target_validity.Reset(count);
	target_validity.EnsureWritable();
	target_validity.SetAllValid(count);
	auto target_data = FlatVector::GetDataMutable<DST>(target);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_format);
	auto source_data = UnifiedVectorFormat::GetData<int32_t>(source_format);
	if (!source_format.validity.CanHaveNull()) {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			target_data[row_idx] = SljitDateYearCompressedGroupKeyOrThrow<DST>(
			    source_data[source_format.sel->get_index(row_idx)], minimum);
		}
	} else {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_idx = source_format.sel->get_index(row_idx);
			if (!source_format.validity.RowIsValid(source_idx)) {
				target_validity.SetInvalid(row_idx);
				continue;
			}
			target_data[row_idx] = SljitDateYearCompressedGroupKeyOrThrow<DST>(source_data[source_idx], minimum);
		}
	}
	FlatVector::SetSize(target, count);
}

static bool SljitTryMaterializeInputVectorGroupSource(DataChunk &payload_input,
                                                      const ExecutionRowPointerGroupKeySource &source, Vector &target,
                                                      idx_t count, bool source_key0_int64_to_int32_unchecked) {
	if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR ||
	    source.input_vector_index >= payload_input.ColumnCount() ||
	    payload_input.data[source.input_vector_index].GetType() != source.source_type ||
	    target.GetType() != source.target_type) {
		return false;
	}
	if (!SljitGroupSourceCanMaterializeFromInputVector(payload_input, source)) {
		return false;
	}
	auto &input = payload_input.data[source.input_vector_index];
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		target.Reference(input);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
		if (source.source_physical_type != PhysicalType::INT64 || source.target_physical_type != PhysicalType::INT32) {
			return false;
		}
		SljitMaterializeInputVectorGroupCast<int64_t, int32_t>(
		    input, target, count,
		    source.unchecked_integral_cast ||
		        (source_key0_int64_to_int32_unchecked && source.hash_join_condition_idx == 0));
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
		if (source.source_physical_type != PhysicalType::INT64 || source.target_physical_type != PhysicalType::INT16) {
			return false;
		}
		SljitMaterializeInputVectorGroupCast<int64_t, int16_t>(input, target, count, source.unchecked_integral_cast);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		if (source.source_physical_type != PhysicalType::INT32 || source.target_physical_type != PhysicalType::INT8) {
			return false;
		}
		SljitMaterializeInputVectorGroupCast<int32_t, int8_t>(input, target, count, source.unchecked_integral_cast);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE ||
		    source.target_physical_type != PhysicalType::UINT8) {
			return false;
		}
		SljitMaterializeDateYearCompressedInputVectorGroup<uint8_t>(input, target, count, source.cast_constant);
		return true;
	default:
		return false;
	}
}

static bool SljitTryBuildInputVectorGroups(ExecutionRegionRuntime &runtime,
                                           SljitAggregatePayloadAdapterScratch &scratch, DataChunk &payload_input,
                                           const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                           DataChunk *&groups, bool source_key0_int64_to_int32_unchecked) {
	if (group_sources.empty()) {
		return false;
	}
	for (auto &source : group_sources) {
		if (!source.ready || source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			return false;
		}
	}
	auto &target_groups = scratch.PrepareInputVectorGroups(runtime.GetAllocator(), group_sources);
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		if (!SljitTryMaterializeInputVectorGroupSource(payload_input, group_sources[group_idx],
		                                               target_groups.data[group_idx], payload_input.size(),
		                                               source_key0_int64_to_int32_unchecked)) {
			return false;
		}
	}
	target_groups.SetChildCardinality(payload_input.size());
	groups = &target_groups;
	return true;
}

static bool SljitPayloadSourceAllValid(DataChunk &payload_input, idx_t payload_source_idx) {
	if (payload_source_idx >= payload_input.ColumnCount()) {
		return false;
	}
	UnifiedVectorFormat format;
	payload_input.data[payload_source_idx].ToUnifiedFormat(format);
	if (!format.validity.CanHaveNull()) {
		return true;
	}
	for (idx_t row_idx = 0; row_idx < payload_input.size(); row_idx++) {
		const auto source_idx = format.sel->get_index(row_idx);
		if (!format.validity.RowIsValid(source_idx)) {
			return false;
		}
	}
	return true;
}

static bool SljitInputVectorCountPayloadIsCountOne(DataChunk &payload_input,
                                                   const vector<idx_t> &payload_source_indices) {
	return payload_source_indices.size() == 1 && (payload_source_indices[0] == DConstants::INVALID_INDEX ||
	                                              SljitPayloadSourceAllValid(payload_input, payload_source_indices[0]));
}

} // namespace duckdb
