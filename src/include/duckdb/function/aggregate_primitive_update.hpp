//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_primitive_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class AggregatePrimitiveUpdateKind : uint8_t { NONE, SUM_INT64 };

struct AggregatePrimitiveUpdateABI {
	AggregatePrimitiveUpdateKind kind = AggregatePrimitiveUpdateKind::NONE;
	PhysicalType input_type = PhysicalType::INVALID;
	idx_t state_size = 0;
	idx_t state_value_offset = 0;
	idx_t state_is_set_offset = 0;

	bool IsReady() const {
		return kind != AggregatePrimitiveUpdateKind::NONE && input_type != PhysicalType::INVALID && state_size > 0;
	}
};

} // namespace duckdb
