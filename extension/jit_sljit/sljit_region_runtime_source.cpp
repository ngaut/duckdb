//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime_source.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_runtime_source.hpp"

#include "sljit_native_runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

const sel_t *SljitNormalizedSourceSelectionData(const UnifiedVectorFormat &format) {
	if (!format.sel || format.sel == FlatVector::IncrementalSelectionVector()) {
		return nullptr;
	}
	return format.sel->data();
}

bool SljitNormalizedSourceAllValid(const UnifiedVectorFormat &format, const sel_t *sel, idx_t count) {
	if (format.validity.CannotHaveNull()) {
		return true;
	}
	if (!sel) {
		return format.validity.CheckAllValid(count);
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!format.validity.RowIsValid(sel[row_idx])) {
			return false;
		}
	}
	return true;
}

bool SljitNormalizedSourceAllValid(const UnifiedVectorFormat &format, const sel_t *source_sel,
                                   const SelectionVector *execute_sel, idx_t count) {
	if (!execute_sel) {
		return SljitNormalizedSourceAllValid(format, source_sel, count);
	}
	if (format.validity.CannotHaveNull()) {
		return true;
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto logical_idx = execute_sel->get_index(row_idx);
		auto source_idx = source_sel ? source_sel[logical_idx] : logical_idx;
		if (!format.validity.RowIsValid(source_idx)) {
			return false;
		}
	}
	return true;
}

bool SljitSourceVectorRowsAllValid(Vector &source, idx_t count, const SelectionVector *sel) {
	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_format);
	auto source_sel = SljitNormalizedSourceSelectionData(source_format);
	if (sel) {
		return SljitNormalizedSourceAllValid(source_format, source_sel, sel, count);
	}
	return SljitNormalizedSourceAllValid(source_format, source_sel, count);
}

const validity_t *SljitNormalizedSourceValidityData(const UnifiedVectorFormat &format, const sel_t *sel, idx_t count) {
	return SljitNormalizedSourceAllValid(format, sel, count) ? nullptr : format.validity.GetData();
}

const validity_t *SljitNormalizedSourceValidityData(const UnifiedVectorFormat &format, const sel_t *source_sel,
                                                    const SelectionVector *execute_sel, idx_t count) {
	return SljitNormalizedSourceAllValid(format, source_sel, execute_sel, count) ? nullptr : format.validity.GetData();
}

static const validity_t *SljitDirectSourceValidityData(const ValidityMask &validity, const sel_t *source_sel,
                                                       const SelectionVector *execute_sel, idx_t count,
                                                       bool source_known_not_null) {
	if (source_known_not_null || validity.CannotHaveNull()) {
		return nullptr;
	}
	if (!execute_sel && !source_sel) {
		return validity.CheckAllValid(count) ? nullptr : validity.GetData();
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto logical_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
		const auto source_idx = source_sel ? source_sel[logical_idx] : logical_idx;
		if (!validity.RowIsValid(source_idx)) {
			return validity.GetData();
		}
	}
	return nullptr;
}

bool SljitUnifiedFormatHasIdentitySelection(const UnifiedVectorFormat &format) {
	return !format.sel || format.sel == FlatVector::IncrementalSelectionVector();
}

static bool SljitFindCommonSelection(const vector<const sel_t *> &selections, const sel_t *&common_sel) {
	for (auto sel : selections) {
		if (!sel) {
			return false;
		}
		if (!common_sel) {
			common_sel = sel;
		} else if (common_sel != sel) {
			return false;
		}
	}
	return true;
}

static void SljitClearSelections(vector<const sel_t *> &selections) {
	for (auto &sel : selections) {
		sel = nullptr;
	}
}

static const sel_t *SljitCommonSelectionOrNull(const vector<const sel_t *> &source_sel) {
	const sel_t *common_sel = nullptr;
	return SljitFindCommonSelection(source_sel, common_sel) ? common_sel : nullptr;
}

static const sel_t *SljitCanonicalizeCommonSelection(vector<const sel_t *> &source_sel,
                                                     vector<const sel_t *> &group_sel) {
	const sel_t *common_sel = nullptr;
	if (!SljitFindCommonSelection(group_sel, common_sel) || !SljitFindCommonSelection(source_sel, common_sel) ||
	    !common_sel) {
		return nullptr;
	}
	SljitClearSelections(group_sel);
	SljitClearSelections(source_sel);
	return common_sel;
}

static const sel_t *SljitCanonicalizeCommonSourceSelection(vector<const sel_t *> &source_sel) {
	auto common_sel = SljitCommonSelectionOrNull(source_sel);
	if (!common_sel) {
		return nullptr;
	}
	SljitClearSelections(source_sel);
	return common_sel;
}

static bool SljitAllSelectionsPresent(const vector<const sel_t *> &selections) {
	if (selections.empty()) {
		return false;
	}
	for (auto sel : selections) {
		if (!sel) {
			return false;
		}
	}
	return true;
}

template <class T>
static T **SljitPointerArrayOrNull(vector<T *> &values) {
	for (auto value : values) {
		if (value) {
			return values.data();
		}
	}
	return nullptr;
}

static SljitNativeIntegerKind SljitTypedExpressionTreeIntegerKind(const LogicalType &type) {
	if (type.id() == LogicalTypeId::BOOLEAN && type.InternalType() == PhysicalType::BOOL) {
		return SljitNativeIntegerKind::UINT8;
	}
	if (type.id() != LogicalTypeId::DECIMAL && type.InternalType() == PhysicalType::INT32) {
		return SljitNativeIntegerKind::INT32;
	}
	if (type.id() == LogicalTypeId::DECIMAL && type.InternalType() == PhysicalType::INT64) {
		return SljitNativeIntegerKind::DECIMAL64;
	}
	if (type.InternalType() == PhysicalType::INT64) {
		return SljitNativeIntegerKind::INT64;
	}
	throw InternalException("Unsupported SLJIT typed expression-tree physical type");
}

static const_data_ptr_t SljitTypedExpressionTreeSourceData(UnifiedVectorFormat &format, const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		return reinterpret_cast<const_data_ptr_t>(format.data);
	}
	return NativeIntegerSourceData(format, SljitTypedExpressionTreeIntegerKind(type));
}

SljitNativeIntegerKind SljitPerfectHashGroupIntegerKind(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return SljitNativeIntegerKind::INT8;
	case PhysicalType::UINT8:
		return SljitNativeIntegerKind::UINT8;
	case PhysicalType::INT32:
		return SljitNativeIntegerKind::INT32;
	case PhysicalType::INT64:
		return SljitNativeIntegerKind::INT64;
	default:
		throw InternalException("Unsupported SLJIT perfect-hash group physical type");
	}
}

void SljitSourceVectorScratch::Resize(idx_t source_count) {
	formats.resize(source_count);
	data.assign(source_count, nullptr);
	selections.assign(source_count, nullptr);
	validity.assign(source_count, nullptr);
}

idx_t SljitSourceVectorScratch::Count() const {
	return data.size();
}

UnifiedVectorFormat &SljitSourceVectorScratch::PrepareFormat(DataChunk &input, idx_t input_index, idx_t source_idx,
                                                             const char *out_of_range_error) {
	if (input_index >= input.ColumnCount()) {
		throw InternalException(out_of_range_error);
	}
	CheckSourceIndex(source_idx);
	input.data[input_index].ToUnifiedFormat(formats[source_idx]);
	return formats[source_idx];
}

bool SljitSourceVectorScratch::PrepareFlatSource(DataChunk &input, idx_t input_index, idx_t source_idx,
                                                 const_data_ptr_t source_data, idx_t count,
                                                 const char *out_of_range_error, bool source_known_not_null,
                                                 const SelectionVector *execute_sel) {
	if (input_index >= input.ColumnCount()) {
		throw InternalException(out_of_range_error);
	}
	CheckSourceIndex(source_idx);
	auto &source = input.data[input_index];
	if (source.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	data[source_idx] = source_data;
	selections[source_idx] = nullptr;
	auto &source_validity = FlatVector::Validity(source);
	validity[source_idx] = SljitDirectSourceValidityData(source_validity, nullptr, execute_sel, count,
	                                                    source_known_not_null);
	return true;
}

bool SljitSourceVectorScratch::PrepareDictionarySource(DataChunk &input, idx_t input_index, idx_t source_idx,
                                                       const_data_ptr_t source_data, idx_t count,
                                                       const char *out_of_range_error,
                                                       bool source_known_not_null,
                                                       const SelectionVector *execute_sel) {
	if (input_index >= input.ColumnCount()) {
		throw InternalException(out_of_range_error);
	}
	CheckSourceIndex(source_idx);
	auto &source = input.data[input_index];
	if (source.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return false;
	}
	auto &child = DictionaryVector::Child(source);
	if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	data[source_idx] = source_data ? source_data : FlatVector::GetData(child);
	selections[source_idx] = DictionaryVector::SelVector(source).data();
	auto &source_validity = FlatVector::Validity(child);
	validity[source_idx] = SljitDirectSourceValidityData(source_validity, selections[source_idx], execute_sel, count,
	                                                    source_known_not_null);
	return true;
}

bool SljitSourceVectorScratch::PrepareConstantSource(DataChunk &input, idx_t input_index, idx_t source_idx,
                                                     const_data_ptr_t source_data, idx_t count,
                                                     const char *out_of_range_error, bool source_known_not_null) {
	if (input_index >= input.ColumnCount()) {
		throw InternalException(out_of_range_error);
	}
	CheckSourceIndex(source_idx);
	auto &source = input.data[input_index];
	if (source.GetVectorType() != VectorType::CONSTANT_VECTOR) {
		return false;
	}
	if (count > STANDARD_VECTOR_SIZE) {
		return false;
	}
	data[source_idx] = source_data ? source_data : ConstantVector::GetData(source);
	selections[source_idx] = ConstantVector::ZeroSelectionVector()->data();
	auto &source_validity = ConstantVector::Validity(source);
	validity[source_idx] = source_known_not_null || source_validity.RowIsValid(0) ? nullptr : source_validity.GetData();
	return true;
}

void SljitSourceVectorScratch::SetData(idx_t source_idx, const_data_ptr_t source_data) {
	CheckSourceIndex(source_idx);
	data[source_idx] = source_data;
}

void SljitSourceVectorScratch::FinishSource(idx_t source_idx, const SelectionVector *execute_sel, idx_t count,
                                            bool source_known_not_null) {
	CheckSourceIndex(source_idx);
	selections[source_idx] = SljitNormalizedSourceSelectionData(formats[source_idx]);
	validity[source_idx] = source_known_not_null
	                           ? nullptr
	                           : SljitNormalizedSourceValidityData(formats[source_idx], selections[source_idx],
	                                                               execute_sel, count);
}

void SljitSourceVectorScratch::PrepareTypedExpressionSource(DataChunk &input, idx_t input_index, idx_t source_idx,
                                                            const SelectionVector *execute_sel, idx_t count,
                                                            const char *out_of_range_error,
                                                            bool source_known_not_null) {
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::FLAT_VECTOR &&
	    PrepareFlatSource(input, input_index, source_idx, FlatVector::GetData(input.data[input_index]), count,
	                      out_of_range_error, source_known_not_null, execute_sel)) {
		return;
	}
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::DICTIONARY_VECTOR &&
	    PrepareDictionarySource(input, input_index, source_idx, nullptr, count, out_of_range_error,
	                            source_known_not_null, execute_sel)) {
		return;
	}
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    PrepareConstantSource(input, input_index, source_idx, nullptr, count, out_of_range_error,
	                          source_known_not_null)) {
		return;
	}
	auto &format = PrepareFormat(input, input_index, source_idx, out_of_range_error);
	SetData(source_idx, SljitTypedExpressionTreeSourceData(format, input.data[input_index].GetType()));
	FinishSource(source_idx, execute_sel, count, source_known_not_null);
}

void SljitSourceVectorScratch::PrepareIntegerSource(DataChunk &input, idx_t input_index, idx_t source_idx,
                                                    SljitNativeIntegerKind integer_kind,
                                                    const SelectionVector *execute_sel, idx_t count,
                                                    const char *out_of_range_error,
                                                    bool source_known_not_null) {
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::FLAT_VECTOR &&
	    PrepareFlatSource(input, input_index, source_idx, FlatVector::GetData(input.data[input_index]), count,
	                      out_of_range_error, source_known_not_null, execute_sel)) {
		return;
	}
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::DICTIONARY_VECTOR &&
	    PrepareDictionarySource(input, input_index, source_idx, nullptr, count, out_of_range_error,
	                            source_known_not_null, execute_sel)) {
		return;
	}
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    PrepareConstantSource(input, input_index, source_idx, nullptr, count, out_of_range_error,
	                          source_known_not_null)) {
		return;
	}
	auto &format = PrepareFormat(input, input_index, source_idx, out_of_range_error);
	SetData(source_idx, NativeIntegerSourceData(format, integer_kind));
	FinishSource(source_idx, execute_sel, count, source_known_not_null);
}

void SljitSourceVectorScratch::PrepareValiditySource(DataChunk &input, idx_t input_index, idx_t source_idx,
                                                     const SelectionVector *execute_sel, idx_t count,
                                                     const char *out_of_range_error, bool source_known_not_null) {
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::FLAT_VECTOR &&
	    PrepareFlatSource(input, input_index, source_idx, nullptr, count, out_of_range_error, source_known_not_null,
	                      execute_sel)) {
		return;
	}
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::DICTIONARY_VECTOR &&
	    PrepareDictionarySource(input, input_index, source_idx, nullptr, count, out_of_range_error,
	                            source_known_not_null, execute_sel)) {
		return;
	}
	if (input_index < input.ColumnCount() && input.data[input_index].GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    PrepareConstantSource(input, input_index, source_idx, nullptr, count, out_of_range_error,
	                          source_known_not_null)) {
		return;
	}
	PrepareFormat(input, input_index, source_idx, out_of_range_error);
	SetData(source_idx, nullptr);
	FinishSource(source_idx, execute_sel, count, source_known_not_null);
}

const_data_ptr_t *SljitSourceVectorScratch::DataArray() {
	return data.data();
}

const sel_t **SljitSourceVectorScratch::SelectionArray() {
	return selections.data();
}

const sel_t **SljitSourceVectorScratch::SelectionArrayOrNull() {
	return SljitPointerArrayOrNull(selections);
}

const validity_t **SljitSourceVectorScratch::ValidityArray() {
	return validity.data();
}

const validity_t **SljitSourceVectorScratch::ValidityArrayOrNull() {
	return SljitPointerArrayOrNull(validity);
}

const sel_t *SljitSourceVectorScratch::CanonicalizeCommonSelection(SljitSourceVectorScratch &other) {
	return SljitCanonicalizeCommonSelection(selections, other.selections);
}

const sel_t *SljitSourceVectorScratch::CanonicalizeCommonSourceSelection() {
	return SljitCanonicalizeCommonSourceSelection(selections);
}

bool SljitSourceVectorScratch::HasCommonSelection() const {
	return SljitCommonSelectionOrNull(selections) != nullptr;
}

bool SljitSourceVectorScratch::AllSelectionsPresent() const {
	return SljitAllSelectionsPresent(selections);
}

bool SljitSourceVectorScratch::HasAnySelection() const {
	for (auto selection : selections) {
		if (selection) {
			return true;
		}
	}
	return false;
}

bool SljitSourceVectorScratch::AllValid() const {
	for (auto source_validity : validity) {
		if (source_validity) {
			return false;
		}
	}
	return true;
}

bool SljitSourceVectorScratch::SourceCanHaveNull() const {
	return !AllValid();
}

bool SljitSourceVectorScratch::FlatNoSelection(const SelectionVector *execute_sel) const {
	return execute_sel == nullptr && !HasAnySelection();
}

bool SljitSourceVectorScratch::FlatAllValid(const SelectionVector *execute_sel) const {
	return FlatNoSelection(execute_sel) && AllValid();
}

bool SljitSourceVectorScratch::FlatNoSelection(const sel_t *native_execute_sel, const sel_t *source_common_sel) const {
	return native_execute_sel == nullptr && source_common_sel == nullptr && !HasAnySelection();
}

bool SljitSourceVectorScratch::FlatAllValid(const sel_t *native_execute_sel, const sel_t *source_common_sel) const {
	return FlatNoSelection(native_execute_sel, source_common_sel) && AllValid();
}

void SljitSourceVectorScratch::CheckSourceIndex(idx_t source_idx) const {
	if (source_idx >= Count()) {
		throw InternalException("SLJIT source scratch index is out of range");
	}
}

} // namespace duckdb
