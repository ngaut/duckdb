//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_inline_string_decompress_projection_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

#include "duckdb/common/bswap.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"

#include <cstring>

namespace duckdb {

static bool SljitProjectionUnifiedFormatHasIdentitySelection(const UnifiedVectorFormat &format) {
	return !format.sel || format.sel == FlatVector::IncrementalSelectionVector();
}

static bool SljitTryDecodeInlineCompressedString16Value(uhugeint_t compressed_value, string_t &result) {
	auto value = BSwapIfBE(compressed_value);
	data_t compressed[sizeof(uhugeint_t)];
	memcpy(compressed, const_data_ptr_cast(&value), sizeof(uhugeint_t));
	const auto length = UnsafeNumericCast<idx_t>(compressed[0]);
	if (length > string_t::INLINE_LENGTH || length >= sizeof(uhugeint_t)) {
		return false;
	}
	char decoded[string_t::INLINE_LENGTH];
	for (idx_t byte_idx = 0; byte_idx < length; byte_idx++) {
		decoded[byte_idx] = char(compressed[sizeof(uhugeint_t) - byte_idx - 1]);
	}
	result = string_t(decoded, UnsafeNumericCast<uint32_t>(length));
	return true;
}

static bool SljitTryFastDecodeInlineCompressedString16(Vector &source, idx_t count, Vector &result) {
	UnifiedVectorFormat source_format;
	source.ToUnifiedFormat(source_format);
	if (!SljitProjectionUnifiedFormatHasIdentitySelection(source_format) ||
	    (source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(count))) {
		return false;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::ValidityMutable(result);
	result_validity.Reset(count);
	result_validity.SetAllValid(count);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	auto source_data = source_format.data;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!SljitTryDecodeInlineCompressedString16Value(Load<uhugeint_t>(source_data + row_idx * sizeof(uhugeint_t)),
		                                                 result_data[row_idx])) {
			return false;
		}
	}
	FlatVector::SetSize(result, count_t(count));
	return true;
}

static bool SljitTryFastInlineStringDecompressProjection(SljitExecutableRegionOp &op, DataChunk &input,
                                                         DataChunk &output, idx_t count) {
	if (op.kind != SljitNativeRegionOpKind::PROJECTION || op.projections.size() != output.ColumnCount()) {
		return false;
	}
	bool has_fast_decompress = false;
	for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
		auto &plan = op.projections[projection_idx].plan;
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			if (plan.source_index >= input.ColumnCount() ||
			    plan.return_type != input.data[plan.source_index].GetType() ||
			    output.data[projection_idx].GetType() != input.data[plan.source_index].GetType()) {
				return false;
			}
			continue;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS &&
		    plan.string_decompress_source_size == sizeof(uhugeint_t) && plan.source_index < input.ColumnCount() &&
		    input.data[plan.source_index].GetType().InternalType() == PhysicalType::UINT128 &&
		    output.data[projection_idx].GetType().id() == LogicalTypeId::VARCHAR) {
			has_fast_decompress = true;
			continue;
		}
		return false;
	}
	if (!has_fast_decompress) {
		return false;
	}

	for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
		auto &plan = op.projections[projection_idx].plan;
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			output.data[projection_idx].Reference(input.data[plan.source_index]);
			continue;
		}
		if (!SljitTryFastDecodeInlineCompressedString16(input.data[plan.source_index], count,
		                                                output.data[projection_idx])) {
			return false;
		}
	}
	output.SetChildCardinality(count);
	return true;
}

} // namespace duckdb
