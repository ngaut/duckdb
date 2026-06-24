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

struct DirectAppendColumnStats {
	bool has_stats = false;
	PhysicalType physical_type = PhysicalType::INVALID;
	int64_t signed_min = 0;
	int64_t signed_max = 0;
	uint64_t unsigned_min = 0;
	uint64_t unsigned_max = 0;
	hugeint_t hugeint_min = {};
	hugeint_t hugeint_max = {};
	uhugeint_t uhugeint_min = {};
	uhugeint_t uhugeint_max = {};
	float float_min = 0;
	float float_max = 0;
	double double_min = 0;
	double double_max = 0;
	bool has_distinct_count = false;
	idx_t distinct_count = 0;
};

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
	vector<DirectAppendColumnStats> stats;
};

struct DirectAppendReservation {
	void Clear() {
		slices.clear();
	}

	bool Empty() const {
		return slices.empty();
	}

	idx_t Count() const {
		idx_t result = 0;
		for (auto &slice : slices) {
			result += slice.count;
		}
		return result;
	}

	vector<DirectAppendSlice> slices;
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

inline bool DirectAppendSupportsType(const LogicalType &type) {
	return DirectAppendSupportsFixedSizeType(type) || DirectAppendSupportsSourceAppendType(type);
}

inline bool DirectAppendSupportsTypes(const vector<LogicalType> &source_types, const vector<LogicalType> &target_types) {
	if (source_types.size() != target_types.size()) {
		return false;
	}
	for (idx_t type_idx = 0; type_idx < source_types.size(); type_idx++) {
		if (source_types[type_idx] != target_types[type_idx]) {
			return false;
		}
		if (!DirectAppendSupportsType(source_types[type_idx])) {
			return false;
		}
	}
	return true;
}

inline bool DirectAppendSupportsFixedSizeTypes(const vector<LogicalType> &source_types,
                                               const vector<LogicalType> &target_types) {
	if (source_types.size() != target_types.size()) {
		return false;
	}
	for (idx_t type_idx = 0; type_idx < source_types.size(); type_idx++) {
		if (source_types[type_idx] != target_types[type_idx]) {
			return false;
		}
		if (!DirectAppendSupportsFixedSizeType(source_types[type_idx])) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
