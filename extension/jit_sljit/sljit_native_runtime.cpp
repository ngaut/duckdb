#include "sljit_native_runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

namespace duckdb {

const_data_ptr_t NativeIntegerSourceData(UnifiedVectorFormat &format, SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int8_t>(format));
	case SljitNativeIntegerKind::UINT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint8_t>(format));
	case SljitNativeIntegerKind::UINT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint16_t>(format));
	case SljitNativeIntegerKind::UINT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint32_t>(format));
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	default:
		throw InternalException("Unknown SLJIT native integer kind");
	}
}

data_ptr_t NativeIntegerResultData(Vector &result, SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT8:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int8_t>(result));
	case SljitNativeIntegerKind::UINT8:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<uint8_t>(result));
	case SljitNativeIntegerKind::UINT16:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<uint16_t>(result));
	case SljitNativeIntegerKind::UINT32:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<uint32_t>(result));
	case SljitNativeIntegerKind::INT32:
	case SljitNativeIntegerKind::DATE:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int32_t>(result));
	case SljitNativeIntegerKind::INT64:
	case SljitNativeIntegerKind::DECIMAL64:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int64_t>(result));
	default:
		throw InternalException("Unknown SLJIT native integer kind");
	}
}

const_data_ptr_t NativeSignedIntegerSourceData(UnifiedVectorFormat &format, SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int8_t>(format));
	case SljitNativeSignedIntegerWidth::INT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int16_t>(format));
	case SljitNativeSignedIntegerWidth::INT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeSignedIntegerWidth::INT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

data_ptr_t NativeSignedIntegerResultData(Vector &result, SljitNativeSignedIntegerWidth width) {
	switch (width) {
	case SljitNativeSignedIntegerWidth::INT8:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int8_t>(result));
	case SljitNativeSignedIntegerWidth::INT16:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int16_t>(result));
	case SljitNativeSignedIntegerWidth::INT32:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int32_t>(result));
	case SljitNativeSignedIntegerWidth::INT64:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int64_t>(result));
	default:
		throw InternalException("Unknown SLJIT native signed integer width");
	}
}

const_data_ptr_t NativeUnsignedIntegerSourceData(UnifiedVectorFormat &format, SljitNativeUnsignedIntegerWidth width) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint8_t>(format));
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint16_t>(format));
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint32_t>(format));
	default:
		throw InternalException("Unknown SLJIT native unsigned integer width");
	}
}

data_ptr_t NativeUnsignedIntegerResultData(Vector &result, SljitNativeUnsignedIntegerWidth width) {
	switch (width) {
	case SljitNativeUnsignedIntegerWidth::UINT8:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<uint8_t>(result));
	case SljitNativeUnsignedIntegerWidth::UINT16:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<uint16_t>(result));
	case SljitNativeUnsignedIntegerWidth::UINT32:
		return reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<uint32_t>(result));
	default:
		throw InternalException("Unknown SLJIT native unsigned integer width");
	}
}

static const sel_t *NormalizedSljitSourceSelectionData(const UnifiedVectorFormat &format) {
	if (!format.sel || format.sel == FlatVector::IncrementalSelectionVector()) {
		return nullptr;
	}
	return format.sel->data();
}

static bool NormalizedSljitSourceAllValid(const UnifiedVectorFormat &format, const sel_t *sel, idx_t count) {
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

void SljitNativePredicateSourceAdapter::Reset() {
	formats.clear();
	source_data.clear();
	source_sel.clear();
	source_validity.clear();
	sources_all_valid = false;
	sources_flat = false;
}

void SljitNativePredicateSourceAdapter::Prepare(DataChunk *input, const vector<idx_t> &input_source_indices) {
	if (!input) {
		if (!input_source_indices.empty()) {
			throw InternalException("SLJIT native predicate kernel requires an input chunk");
		}
		Reset();
		return;
	}
	if (input_source_indices.empty()) {
		Reset();
		return;
	}
	auto source_count = input_source_indices.size();
	formats.resize(source_count);
	source_data.assign(source_count, nullptr);
	source_sel.assign(source_count, nullptr);
	source_validity.assign(source_count, nullptr);
	sources_all_valid = true;
	sources_flat = true;
	for (idx_t source_idx = 0; source_idx < source_count; source_idx++) {
		auto column_idx = input_source_indices[source_idx];
		if (column_idx >= input->ColumnCount()) {
			throw InternalException("SLJIT native predicate source index out of range");
		}
		input->data[column_idx].ToUnifiedFormat(formats[source_idx]);
		auto sel = NormalizedSljitSourceSelectionData(formats[source_idx]);
		source_data[source_idx] = formats[source_idx].data;
		source_sel[source_idx] = sel;
		source_validity[source_idx] = NormalizedSljitSourceAllValid(formats[source_idx], sel, input->size())
		                                  ? nullptr
		                                  : formats[source_idx].validity.GetData();
		sources_all_valid = sources_all_valid && source_validity[source_idx] == nullptr;
		sources_flat = sources_flat && !sel;
	}
}

const uint8_t *SljitNativePredicateSourceAdapter::ResidualDictionary(DataChunk &input,
                                                                     const vector<idx_t> &input_source_indices,
                                                                     idx_t source_slot,
                                                                     SljitNativePredicateFunction residual,
                                                                     idx_t count) {
	if (source_slot >= source_sel.size() || source_slot >= input_source_indices.size() || !source_sel[source_slot]) {
		return nullptr;
	}
	auto &source = input.data[input_source_indices[source_slot]];
	if (source.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return nullptr;
	}
	const auto dictionary_size = DictionaryVector::DictionarySize(source);
	auto &dictionary = DictionaryVector::Child(source);
	if (!dictionary_size.IsValid() || dictionary_size.GetIndex() * 8 > count ||
	    dictionary.GetVectorType() != VectorType::FLAT_VECTOR) {
		return nullptr;
	}
	UnifiedVectorFormat dictionary_format;
	dictionary.ToUnifiedFormat(dictionary_format);
	// the rows' selection indexes the data the row-by-row residual reads, which must be these entries
	if (dictionary_format.data != source_data[source_slot]) {
		return nullptr;
	}
	const auto entries = dictionary_size.GetIndex();
	residual_dictionary.resize(entries);
	residual_dictionary_validity.assign((entries + 63) / 64, ~validity_t(0));
	residual_dictionary_data.assign(source_data.size(), nullptr);
	residual_dictionary_sel.assign(source_data.size(), nullptr);
	residual_dictionary_source_validity.assign(source_data.size(), nullptr);
	residual_dictionary_data[source_slot] = dictionary_format.data;
	const bool entries_all_valid = dictionary_format.validity.CheckAllValid(entries);
	residual_dictionary_source_validity[source_slot] =
	    entries_all_valid ? nullptr : dictionary_format.validity.GetData();

	SljitNativePredicateInput dictionary_input;
	dictionary_input.source_data = residual_dictionary_data.data();
	dictionary_input.source_sel = residual_dictionary_sel.data();
	dictionary_input.source_validity = residual_dictionary_source_validity.data();
	dictionary_input.sources_all_valid = entries_all_valid;
	dictionary_input.rows_flat = true;
	dictionary_input.result_data = residual_dictionary.data();
	dictionary_input.result_validity = residual_dictionary_validity.data();
	dictionary_input.count = entries;
	SljitExecuteNativeFunction(residual, dictionary_input);
	return residual_dictionary.data();
}

const_data_ptr_t *SljitNativePredicateSourceAdapter::DataArray() {
	return source_data.data();
}

const sel_t **SljitNativePredicateSourceAdapter::SelectionArray() {
	return source_sel.data();
}

const validity_t **SljitNativePredicateSourceAdapter::ValidityArray() {
	return source_validity.data();
}

idx_t SljitNativePredicateSourceAdapter::SourceCount() const {
	return source_data.size();
}

bool SljitNativePredicateSourceAdapter::SourcesAllValid() const {
	return sources_all_valid;
}

bool SljitNativePredicateSourceAdapter::SourcesFlat() const {
	return sources_flat;
}

} // namespace duckdb
