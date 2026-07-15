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

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/function/scalar/string_common.hpp"

namespace duckdb {

static bool SljitGroupSourceCanMaterializeFromInputVector(DataChunk &payload_input,
                                                          const ExecutionRowPointerGroupKeySource &source) {
	if (source.input_vector_index >= payload_input.ColumnCount() ||
	    payload_input.data[source.input_vector_index].GetType() != source.source_type) {
		return false;
	}
	return SljitInputVectorGroupKeySourceSupportsMaterialization(source);
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
	if (!source_format.sel->IsSet() &&
	    (!source_format.validity.CanHaveNull() || source_format.validity.CheckAllValid(count))) {
		uint8_t fits = 1;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto value = source_data[row_idx];
			fits &=
			    static_cast<uint8_t>(value >= NumericLimits<DST>::Minimum() && value <= NumericLimits<DST>::Maximum());
		}
		return fits != 0;
	}
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

struct SljitInputVectorGroupBatchCastFitDispatch {
	DataChunk &payload_input;
	const ExecutionRowPointerGroupKeySource &source;
	idx_t count;

	template <class SRC, class DST>
	bool Execute() {
		return SljitInputVectorGroupBatchFitsCast<SRC, DST>(payload_input, source, count);
	}
};

static bool SljitInputVectorGroupBatchCastFits(DataChunk &payload_input,
                                               const ExecutionRowPointerGroupKeySource &source, idx_t count) {
	SljitInputVectorGroupBatchCastFitDispatch dispatch {payload_input, source, count};
	return SljitDispatchGroupKeyNarrowingIntegralCast(source, dispatch);
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

static void SljitApplyExecutableIntegralGroupKeyRangeProofs(const vector<SljitExecutableIntegralGroupKeyRange> &ranges,
                                                            vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (ranges.size() != group_sources.size()) {
		return;
	}
	for (idx_t group_idx = 0; group_idx < group_sources.size(); group_idx++) {
		if (ranges[group_idx].ProvesNarrowingCast(group_sources[group_idx])) {
			group_sources[group_idx].unchecked_integral_cast = true;
		}
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

struct SljitRowPointerGroupBatchCastFitDispatch {
	Vector &row_pointers;
	const ExecutionRowPointerGroupKeySource &source;
	idx_t count;

	template <class SRC, class DST>
	bool Execute() {
		return SljitRowPointerGroupBatchFitsCast<SRC, DST>(row_pointers, source, count);
	}
};

static bool SljitRowPointerGroupBatchCastFits(Vector &row_pointers, const ExecutionRowPointerGroupKeySource &source,
                                              idx_t count) {
	SljitRowPointerGroupBatchCastFitDispatch dispatch {row_pointers, source, count};
	return SljitDispatchGroupKeyNarrowingIntegralCast(source, dispatch);
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

struct SljitInputVectorGroupCastMaterializeDispatch {
	Vector &input;
	Vector &target;
	idx_t count;
	bool unchecked;

	template <class SRC, class DST>
	bool Execute() {
		SljitMaterializeInputVectorGroupCast<SRC, DST>(input, target, count, unchecked);
		return true;
	}
};

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

template <class DST>
static bool SljitMaterializeStringCompressedInputVectorGroup(Vector &source, Vector &target, idx_t count) {
	target.SetVectorType(VectorType::FLAT_VECTOR);
	auto &target_validity = FlatVector::ValidityMutable(target);
	target_validity.Reset(count);
	target_validity.EnsureWritable();
	target_validity.SetAllValid(count);
	auto target_data = FlatVector::GetDataMutable<DST>(target);

	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_format);
	auto source_data = UnifiedVectorFormat::GetData<string_t>(source_format);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto source_idx = source_format.sel->get_index(row_idx);
		if (!source_format.validity.RowIsValid(source_idx)) {
			target_validity.SetInvalid(row_idx);
			continue;
		}
		if (!TryStringCompressValue(source_data[source_idx], target_data[row_idx])) {
			return false;
		}
	}
	FlatVector::SetSize(target, count);
	return true;
}

struct SljitInputVectorStringCompressMaterializeDispatch {
	Vector &input;
	Vector &target;
	idx_t count;

	template <class DST>
	bool Execute() {
		return SljitMaterializeStringCompressedInputVectorGroup<DST>(input, target, count);
	}
};

static bool SljitTryMaterializeStringCompressedInputVectorGroup(Vector &input, Vector &target, idx_t count,
                                                                const ExecutionRowPointerGroupKeySource &source) {
	SljitInputVectorStringCompressMaterializeDispatch dispatch {input, target, count};
	switch (source.target_physical_type) {
	case PhysicalType::UINT8:
		return dispatch.Execute<uint8_t>();
	case PhysicalType::UINT16:
		return dispatch.Execute<uint16_t>();
	case PhysicalType::UINT32:
		return dispatch.Execute<uint32_t>();
	case PhysicalType::UINT64:
		return dispatch.Execute<uint64_t>();
	case PhysicalType::UINT128:
		return dispatch.Execute<uhugeint_t>();
	default:
		return false;
	}
}

template <class SOURCE_TYPE, class TARGET_TYPE>
static bool SljitTryMaterializeAffineInputVectorGroupSource(DataChunk &payload_input,
                                                            const ExecutionRowPointerGroupKeySource &source,
                                                            Vector &target, idx_t count) {
	SOURCE_TYPE constant;
	if (!TryCast::Operation<int64_t, SOURCE_TYPE>(source.output_transform_constant, constant, false)) {
		return false;
	}
	auto &input = payload_input.data[source.input_vector_index];
	UnifiedVectorFormat input_format;
	input.ToUnifiedFormat(input_format);
	UnifiedVectorFormat guard_format;
	const bool has_guard = source.output_transform_validity_guard_index != DConstants::INVALID_INDEX;
	if (has_guard) {
		if (source.output_transform_validity_guard_index >= payload_input.ColumnCount()) {
			return false;
		}
		payload_input.data[source.output_transform_validity_guard_index].ToUnifiedFormat(guard_format);
	}

	target.SetVectorType(VectorType::FLAT_VECTOR);
	auto &target_validity = FlatVector::ValidityMutable(target);
	target_validity.Reset(count);
	target_validity.EnsureWritable();
	target_validity.SetAllValid(count);
	auto input_data = UnifiedVectorFormat::GetData<SOURCE_TYPE>(input_format);
	auto target_data = FlatVector::GetDataMutable<TARGET_TYPE>(target);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto input_idx = input_format.sel->get_index(row_idx);
		const auto guard_idx = has_guard ? guard_format.sel->get_index(row_idx) : 0;
		if (!input_format.validity.RowIsValid(input_idx) ||
		    (has_guard && !guard_format.validity.RowIsValid(guard_idx))) {
			target_validity.SetInvalid(row_idx);
			continue;
		}
		SOURCE_TYPE transformed;
		if (!TryAddOperator::Operation(input_data[input_idx], constant, transformed)) {
			throw OutOfRangeException("Overflow in SLJIT affine group-key publication");
		}
		if (!TryCast::Operation<SOURCE_TYPE, TARGET_TYPE>(transformed, target_data[row_idx], false)) {
			throw OutOfRangeException("Cast overflow in SLJIT affine group-key publication");
		}
	}
	FlatVector::SetSize(target, count);
	return true;
}

template <class SOURCE_TYPE>
struct SljitAffineInputVectorGroupTargetDispatch {
	DataChunk &payload_input;
	const ExecutionRowPointerGroupKeySource &source;
	Vector &target;
	idx_t count;

	template <class TARGET_TYPE>
	bool Execute() {
		return SljitTryMaterializeAffineInputVectorGroupSource<SOURCE_TYPE, TARGET_TYPE>(payload_input, source, target,
		                                                                                 count);
	}
};

template <class DISPATCH>
static bool SljitDispatchSignedAffineGroupPhysicalType(PhysicalType type, DISPATCH &dispatch) {
	switch (type) {
	case PhysicalType::INT8:
		return dispatch.template Execute<int8_t>();
	case PhysicalType::INT16:
		return dispatch.template Execute<int16_t>();
	case PhysicalType::INT32:
		return dispatch.template Execute<int32_t>();
	case PhysicalType::INT64:
		return dispatch.template Execute<int64_t>();
	default:
		return false;
	}
}

struct SljitAffineInputVectorGroupSourceDispatch {
	DataChunk &payload_input;
	const ExecutionRowPointerGroupKeySource &source;
	Vector &target;
	idx_t count;

	template <class SOURCE_TYPE>
	bool Execute() {
		SljitAffineInputVectorGroupTargetDispatch<SOURCE_TYPE> dispatch {payload_input, source, target, count};
		return SljitDispatchSignedAffineGroupPhysicalType(source.target_physical_type, dispatch);
	}
};

static bool SljitTryMaterializeAffineInputVectorGroupSource(DataChunk &payload_input,
                                                            const ExecutionRowPointerGroupKeySource &source,
                                                            Vector &target, idx_t count) {
	if (source.output_transform_kind != ExecutionGroupKeyOutputTransformKind::ADD_CONSTANT ||
	    source.cast_kind != ExecutionRowPointerGroupKeyCastKind::NONE ||
	    !SljitSignedAffineGroupPhysicalType(source.source_physical_type) ||
	    !SljitSignedAffineGroupPhysicalType(source.target_physical_type)) {
		return false;
	}
	SljitAffineInputVectorGroupSourceDispatch dispatch {payload_input, source, target, count};
	return SljitDispatchSignedAffineGroupPhysicalType(source.source_physical_type, dispatch);
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
	if (source.HasOutputTransform()) {
		return SljitTryMaterializeAffineInputVectorGroupSource(payload_input, source, target, count);
	}
	auto &input = payload_input.data[source.input_vector_index];
	if (ExecutionGroupKeyCastIsNarrowingIntegral(source.cast_kind)) {
		const auto unchecked = source.unchecked_integral_cast ||
		                       (source.cast_kind == ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32 &&
		                        source_key0_int64_to_int32_unchecked && source.hash_join_condition_idx == 0);
		SljitInputVectorGroupCastMaterializeDispatch dispatch {input, target, count, unchecked};
		return SljitDispatchGroupKeyNarrowingIntegralCast(source, dispatch);
	}
	switch (source.cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::NONE:
		if (source.source_physical_type != source.target_physical_type) {
			return false;
		}
		target.Reference(input);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::DATE_YEAR_COMPRESS:
		if (source.source_physical_type != PhysicalType::INT32 || source.source_type.id() != LogicalTypeId::DATE ||
		    source.target_physical_type != PhysicalType::UINT8) {
			return false;
		}
		SljitMaterializeDateYearCompressedInputVectorGroup<uint8_t>(input, target, count, source.cast_constant);
		return true;
	case ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS:
		if (source.source_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		return SljitTryMaterializeStringCompressedInputVectorGroup(input, target, count, source);
	case ExecutionRowPointerGroupKeyCastKind::STRING_SUBSTRING: {
		if (source.source_physical_type != PhysicalType::VARCHAR ||
		    source.target_physical_type != PhysicalType::VARCHAR) {
			return false;
		}
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(count);
		target_validity.EnsureWritable();
		target_validity.SetAllValid(count);
		StringVector::AddHeapReference(target, input);

		UnifiedVectorFormat input_format;
		input.ToUnifiedFormat(input_format);
		auto input_data = UnifiedVectorFormat::GetData<string_t>(input_format);
		auto target_data = FlatVector::GetDataMutable<string_t>(target);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto input_idx = input_format.sel->get_index(row_idx);
			if (!input_format.validity.RowIsValid(input_idx)) {
				target_validity.SetInvalid(row_idx);
				continue;
			}
			target_data[row_idx] = SubstringPrefixUnicode(input_data[input_idx], source.string_substring_length);
		}
		FlatVector::SetSize(target, count);
		return true;
	}
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
