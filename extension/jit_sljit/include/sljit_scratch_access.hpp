//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_scratch_access.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

template <class T>
static T &SljitCheckedScratchPtr(vector<unique_ptr<T>> &scratch, idx_t op_idx, const char *message) {
	if (op_idx >= scratch.size() || !scratch[op_idx]) {
		throw InternalException(message);
	}
	return *scratch[op_idx];
}

template <class T>
static T &SljitCheckedScratchSlot(vector<T> &scratch, idx_t op_idx, const char *message) {
	if (op_idx >= scratch.size()) {
		throw InternalException(message);
	}
	return scratch[op_idx];
}

static void SljitInitializeScratchChunk(Allocator &allocator, const vector<LogicalType> &types,
                                        unique_ptr<DataChunk> &target) {
	auto chunk = make_uniq<DataChunk>();
	chunk->Initialize(allocator, types);
	target = std::move(chunk);
}

} // namespace duckdb
