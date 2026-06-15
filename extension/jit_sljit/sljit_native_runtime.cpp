#include "sljit_native_runtime.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

namespace duckdb {

const_data_ptr_t NativeIntegerSourceData(UnifiedVectorFormat &format, SljitNativeIntegerKind kind) {
	switch (kind) {
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

const_data_ptr_t NativeSignedIntegerSourceData(UnifiedVectorFormat &format,
                                               SljitNativeSignedIntegerWidth width) {
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

const_data_ptr_t NativeUnsignedIntegerSourceData(UnifiedVectorFormat &format,
                                                 SljitNativeUnsignedIntegerWidth width) {
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

bool NativePredicateRequiresInput(const SljitNativePredicate &predicate) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return false;
	case SljitNativePredicateKind::NOT:
		return NativePredicateRequiresInput(*predicate.child);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (NativePredicateRequiresInput(*child)) {
				return true;
			}
		}
		return false;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		return !predicate.guard_source_indices.empty() || NativePredicateRequiresInput(*predicate.child);
	default:
		return true;
	}
}

void PrepareSljitPredicateSources(DataChunk *input, bool requires_input, vector<UnifiedVectorFormat> &formats,
                                  vector<const_data_ptr_t> &source_data, vector<const sel_t *> &source_sel,
                                  vector<const validity_t *> &source_validity) {
	if (!input) {
		if (requires_input) {
			throw InternalException("SLJIT native predicate kernel requires an input chunk");
		}
		source_data.clear();
		source_sel.clear();
		source_validity.clear();
		return;
	}
	formats.clear();
	source_data.clear();
	source_sel.clear();
	source_validity.clear();
	formats.reserve(input->ColumnCount());
	source_data.reserve(input->ColumnCount());
	source_sel.reserve(input->ColumnCount());
	source_validity.reserve(input->ColumnCount());
	for (idx_t column_idx = 0; column_idx < input->ColumnCount(); column_idx++) {
		formats.emplace_back();
		input->data[column_idx].ToUnifiedFormat(formats.back());
		auto sel = NormalizedSljitSourceSelectionData(formats.back());
		source_data.push_back(formats.back().data);
		source_sel.push_back(sel);
		source_validity.push_back(NormalizedSljitSourceAllValid(formats.back(), sel, input->size())
		                              ? nullptr
		                              : formats.back().validity.GetData());
	}
}

} // namespace duckdb
