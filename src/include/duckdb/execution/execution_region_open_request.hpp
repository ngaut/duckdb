//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_region_open_request.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/execution/execution_region_common.hpp"

namespace duckdb {

struct ExecutionRegionOpenRequest {
	bool present = false;
	ExecutionRegionSourceExecutionKind source_execution = ExecutionRegionSourceExecutionKind::NONE;
	ExecutionRegionSourceFilterOwnershipKind source_filter_ownership = ExecutionRegionSourceFilterOwnershipKind::NONE;
	vector<LogicalType> input_types;

	bool UsesSourceContract() const {
		return source_execution == ExecutionRegionSourceExecutionKind::SOURCE_CONTRACT;
	}

	bool OwnsFilters() const {
		return source_filter_ownership != ExecutionRegionSourceFilterOwnershipKind::NONE;
	}

	bool UsesGeneratedFilters() const {
		return source_filter_ownership == ExecutionRegionSourceFilterOwnershipKind::GENERATED;
	}

	bool UsesScanFilters() const {
		return source_filter_ownership == ExecutionRegionSourceFilterOwnershipKind::DUCKDB_SCAN;
	}
};

} // namespace duckdb
