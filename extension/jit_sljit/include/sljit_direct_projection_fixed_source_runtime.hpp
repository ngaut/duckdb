//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_direct_projection_fixed_source_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_runtime.hpp"
#include "sljit_region_executable.hpp"
#include "sljit_region_runtime_source.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/table/direct_append_stats.hpp"

namespace duckdb {

static const_data_ptr_t OffsetFixedSizeData(const_data_ptr_t data, const LogicalType &type, idx_t offset) {
	return data + offset * GetTypeIdSize(type.InternalType());
}

struct SljitFixedDirectProjectionSourceCache {
	struct Source {
		idx_t index = DConstants::INVALID_INDEX;
		UnifiedVectorFormat format;
	};

	void Reset(idx_t column_count) {
		sources.clear();
		sources.reserve(column_count);
	}

	vector<Source> sources;
};

static bool PrepareFixedDirectProjectionSource(DataChunk &input, idx_t source_index, idx_t source_offset, idx_t count,
                                               UnifiedVectorFormat &source_format) {
	if (source_index >= input.ColumnCount()) {
		throw InternalException("SLJIT fixed direct projection source is out of range");
	}
	input.data[source_index].ToUnifiedFormat(source_format);
	if (!SljitUnifiedFormatHasIdentitySelection(source_format)) {
		return false;
	}
	if (source_format.validity.CanHaveNull() &&
	    !source_format.validity.CheckAllValid(source_offset + count, source_offset)) {
		return false;
	}
	return true;
}

static bool PrepareFixedDirectProjectionFullSource(DataChunk &input, idx_t source_index,
                                                   UnifiedVectorFormat &source_format) {
	if (source_index >= input.ColumnCount()) {
		throw InternalException("SLJIT fixed direct projection source is out of range");
	}
	input.data[source_index].ToUnifiedFormat(source_format);
	if (!SljitUnifiedFormatHasIdentitySelection(source_format)) {
		return false;
	}
	if (source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(input.size())) {
		return false;
	}
	return true;
}

static bool PrepareFixedDirectProjectionSource(DataChunk &input, idx_t source_index, idx_t source_offset, idx_t count,
                                               optional_ptr<SljitFixedDirectProjectionSourceCache> source_cache,
                                               UnifiedVectorFormat &local_source_format,
                                               UnifiedVectorFormat *&source_format) {
	source_format = nullptr;
	if (!source_cache) {
		if (!PrepareFixedDirectProjectionSource(input, source_index, source_offset, count, local_source_format)) {
			return false;
		}
		source_format = &local_source_format;
		return true;
	}
	for (auto &source : source_cache->sources) {
		if (source.index == source_index) {
			source_format = &source.format;
			return true;
		}
	}
	source_cache->sources.emplace_back();
	auto &prepared_source = source_cache->sources.back();
	prepared_source.index = source_index;
	if (!PrepareFixedDirectProjectionFullSource(input, source_index, prepared_source.format)) {
		source_cache->sources.pop_back();
		return false;
	}
	source_format = &prepared_source.format;
	return true;
}

} // namespace duckdb
