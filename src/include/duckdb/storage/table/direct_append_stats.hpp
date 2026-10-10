//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/direct_append_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class Vector;

struct DirectAppendColumnSource {
	optional_ptr<const Vector> vector;
	idx_t offset = 0;

	bool IsSet() const {
		return vector;
	}
};

struct DirectAppendSlice {
	idx_t source_offset = 0;
	idx_t count = 0;
	vector<data_ptr_t> targets;
	vector<DirectAppendColumnSource> sources;
};

inline bool DirectAppendSupportsFixedSizeType(const LogicalType &type) {
	if (type.id() == LogicalTypeId::SQLNULL) {
		return false;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

inline bool DirectAppendSupportsSourceAppendType(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARCHAR;
}

} // namespace duckdb
