#include "sljit_native_runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

namespace duckdb {

const_data_ptr_t NativeIntegerSourceData(UnifiedVectorFormat &format, SljitNativeIntegerKind kind) {
	switch (kind) {
	case SljitNativeIntegerKind::INT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int8_t>(format));
	case SljitNativeIntegerKind::UINT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint8_t>(format));
	case SljitNativeIntegerKind::INT32:
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
	case SljitNativeIntegerKind::INT32:
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
	}
}

} // namespace duckdb
