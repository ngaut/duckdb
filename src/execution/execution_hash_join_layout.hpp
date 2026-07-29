//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution_hash_join_layout.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_layout.hpp"

namespace duckdb {

//! Canonical core-owned row layout for a regular hash join.
//!
//! Both the physical hash table and the pointer-free execution-region contract
//! must be derived from this object. Backends consume only the exported offsets
//! copied into their semantic plan and never include this core-private header.
struct ExecutionHashJoinRowLayout {
	shared_ptr<TupleDataLayout> layout;
	idx_t tuple_size = 0;
	idx_t pointer_offset = 0;
	idx_t entry_size = 0;
};

ExecutionHashJoinRowLayout BuildExecutionHashJoinRowLayout(const vector<LogicalType> &condition_types,
                                                           const vector<LogicalType> &payload_types,
                                                           bool found_match_column_present);

} // namespace duckdb
