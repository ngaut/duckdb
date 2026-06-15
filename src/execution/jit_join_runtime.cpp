//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/jit_join_runtime.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/jit/join_runtime.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/join_hashtable.hpp"

namespace duckdb {

static string JitNativeHashJoinBool(bool value) {
	return value ? "true" : "false";
}

static string JitNativeHashJoinTypeList(const vector<LogicalType> &types) {
	string result = "[";
	for (idx_t type_idx = 0; type_idx < types.size(); type_idx++) {
		if (type_idx > 0) {
			result += "|";
		}
		result += types[type_idx].ToString();
	}
	result += "]";
	return result;
}

static string JitNativeHashJoinIdxList(const vector<idx_t> &values) {
	string result = "[";
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		if (value_idx > 0) {
			result += "|";
		}
		result += std::to_string(values[value_idx]);
	}
	result += "]";
	return result;
}

bool JitGetNativeHashJoinTableLayout(const JoinHashTable &hash_table, JitNativeHashJoinTableLayout &layout) {
	return hash_table.GetJitNativeHashJoinTableLayout(layout);
}

string DescribeJitNativeHashJoinTableLayout(const JitNativeHashJoinTableLayout &layout) {
	string result = "native_hash_join_table_layout<ready=" + JitNativeHashJoinBool(layout.ready);
	result += ",join_type=" + StringUtil::Lower(JoinTypeToString(layout.join_type));
	result += ",finalized=" + JitNativeHashJoinBool(layout.finalized);
	result += ",in_memory=" + JitNativeHashJoinBool(layout.in_memory);
	result += ",needs_chain_matcher=" + JitNativeHashJoinBool(layout.needs_chain_matcher);
	result += ",chains_longer_than_one=" + JitNativeHashJoinBool(layout.chains_longer_than_one);
	result += ",residual_predicate=" + JitNativeHashJoinBool(layout.residual_predicate);
	result += ",dictionary_emission=" + JitNativeHashJoinBool(layout.dictionary_emission);
	result += ",can_have_null=" + JitNativeHashJoinBool(layout.can_have_null);
	result += ",use_salt=" + JitNativeHashJoinBool(layout.use_salt);
	result += ",null_keys_are_filtered=" + JitNativeHashJoinBool(layout.null_keys_are_filtered);
	result += ",condition_count=" + std::to_string(layout.condition_count);
	result += ",condition_types=" + JitNativeHashJoinTypeList(layout.condition_types);
	result += ",payload_columns=" + std::to_string(layout.payload_column_count);
	result += ",payload_types=" + JitNativeHashJoinTypeList(layout.payload_types);
	result += ",layout_columns=" + std::to_string(layout.layout_column_count);
	result += ",layout_offsets=" + JitNativeHashJoinIdxList(layout.layout_offsets);
	result += ",tuple_size=" + std::to_string(layout.tuple_size);
	result += ",entry_size=" + std::to_string(layout.entry_size);
	result += ",pointer_offset=" + std::to_string(layout.pointer_offset);
	result += ",hash_column_index=" + std::to_string(layout.hash_column_index);
	result += ",found_match_column_present=" + JitNativeHashJoinBool(layout.found_match_column_present);
	result += ",found_match_column_index=" + std::to_string(layout.found_match_column_index);
	result += ",capacity=" + std::to_string(layout.capacity);
	result += ",bitmask=" + std::to_string(layout.bitmask);
	result += ",pointer_mask=" + std::to_string(layout.pointer_mask);
	result += ",salt_mask=" + std::to_string(layout.salt_mask);
	result += ",entries=" + JitNativeHashJoinBool(layout.entries != nullptr);
	result += ",aux_next_ptrs=" + JitNativeHashJoinBool(layout.aux_next_ptrs != nullptr);
	result += ",blocker=" + layout.blocker;
	result += ">";
	return result;
}

} // namespace duckdb
