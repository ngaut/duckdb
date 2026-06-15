//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/jit/join_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/ht_entry.hpp"

namespace duckdb {

class JoinHashTable;

struct JitNativeHashJoinTableLayout {
	bool ready = false;
	JoinType join_type = JoinType::INVALID;
	bool finalized = false;
	bool in_memory = false;
	bool needs_chain_matcher = false;
	bool chains_longer_than_one = false;
	bool residual_predicate = false;
	bool dictionary_emission = false;
	bool can_have_null = false;
	bool use_salt = false;
	bool single_match_probe = false;
	bool null_keys_are_filtered = false;
	idx_t condition_count = 0;
	vector<LogicalType> condition_types;
	idx_t payload_column_count = 0;
	vector<LogicalType> payload_types;
	idx_t layout_column_count = 0;
	vector<idx_t> layout_offsets;
	idx_t tuple_size = 0;
	idx_t entry_size = 0;
	idx_t pointer_offset = 0;
	idx_t hash_column_index = 0;
	bool found_match_column_present = false;
	idx_t found_match_column_index = 0;
	idx_t capacity = 0;
	uint64_t bitmask = 0;
	uint64_t pointer_mask = 0;
	uint64_t salt_mask = 0;
	const ht_entry_t *entries = nullptr;
	const data_ptr_t *aux_next_ptrs = nullptr;
	string blocker;
};

DUCKDB_API bool JitGetNativeHashJoinTableLayout(const JoinHashTable &hash_table,
                                                JitNativeHashJoinTableLayout &layout);

DUCKDB_API string DescribeJitNativeHashJoinTableLayout(const JitNativeHashJoinTableLayout &layout);

} // namespace duckdb
