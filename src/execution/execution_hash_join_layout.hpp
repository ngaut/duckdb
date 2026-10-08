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

//! Logical type of a dict-surviving payload slot that stores a dictionary index of index_width bytes (1, 2 or 4).
LogicalType ExecutionHashJoinDictIndexType(uint8_t index_width);

//! payload_dict_index_widths: per payload column, 0 keeps the native slot and 1/2/4 narrows it to a dictionary
//! index slot (dict-surviving join). Empty means every payload column keeps its native width.
ExecutionHashJoinRowLayout BuildExecutionHashJoinRowLayout(const vector<LogicalType> &condition_types,
                                                           const vector<LogicalType> &payload_types,
                                                           bool found_match_column_present,
                                                           const vector<uint8_t> &payload_dict_index_widths);

} // namespace duckdb
