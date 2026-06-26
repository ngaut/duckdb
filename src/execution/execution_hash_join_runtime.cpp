//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution_hash_join_runtime.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/execution_hash_join_runtime.hpp"

#include "duckdb/execution/join_hashtable.hpp"

namespace duckdb {

static string ExecutionHashJoinBool(bool value) {
	return value ? "true" : "false";
}

static string ExecutionHashJoinTypeList(const vector<LogicalType> &types) {
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

static string ExecutionHashJoinIdxList(const vector<idx_t> &values) {
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

bool ExecutionGetHashJoinTableLayout(const JoinHashTable &hash_table, ExecutionHashJoinTableLayout &layout) {
	return hash_table.GetExecutionHashJoinTableLayout(layout);
}

string DescribeExecutionHashJoinTableLayout(const ExecutionHashJoinTableLayout &layout) {
	string result = "native_hash_join_table_layout<ready=" + ExecutionHashJoinBool(layout.ready);
	result += ",join_type=" + string(ExecutionRegionJoinTypeToString(layout.join_type));
	result += ",finalized=" + ExecutionHashJoinBool(layout.finalized);
	result += ",in_memory=" + ExecutionHashJoinBool(layout.in_memory);
	result += ",needs_chain_matcher=" + ExecutionHashJoinBool(layout.needs_chain_matcher);
	result += ",chains_longer_than_one=" + ExecutionHashJoinBool(layout.chains_longer_than_one);
	result += ",residual_predicate=" + ExecutionHashJoinBool(layout.residual_predicate);
	result += ",dictionary_emission=" + ExecutionHashJoinBool(layout.dictionary_emission);
	result += ",can_have_null=" + ExecutionHashJoinBool(layout.can_have_null);
	result += ",use_salt=" + ExecutionHashJoinBool(layout.use_salt);
	result += ",null_keys_are_filtered=" + ExecutionHashJoinBool(layout.null_keys_are_filtered);
	result += ",condition_count=" + std::to_string(layout.condition_count);
	result += ",condition_types=" + ExecutionHashJoinTypeList(layout.condition_types);
	result += ",payload_columns=" + std::to_string(layout.payload_column_count);
	result += ",payload_types=" + ExecutionHashJoinTypeList(layout.payload_types);
	result += ",layout_columns=" + std::to_string(layout.layout_column_count);
	result += ",layout_offsets=" + ExecutionHashJoinIdxList(layout.layout_offsets);
	result += ",tuple_size=" + std::to_string(layout.tuple_size);
	result += ",entry_size=" + std::to_string(layout.entry_size);
	result += ",pointer_offset=" + std::to_string(layout.pointer_offset);
	result += ",hash_column_index=" + std::to_string(layout.hash_column_index);
	result += ",found_match_column_present=" + ExecutionHashJoinBool(layout.found_match_column_present);
	result += ",found_match_column_index=" + std::to_string(layout.found_match_column_index);
	result += ",capacity=" + std::to_string(layout.capacity);
	result += ",bitmask=" + std::to_string(layout.bitmask);
	result += ",pointer_mask=" + std::to_string(layout.pointer_mask);
	result += ",salt_mask=" + std::to_string(layout.salt_mask);
	result += ",entries=" + ExecutionHashJoinBool(layout.entries != nullptr);
	result += ",aux_next_ptrs=" + ExecutionHashJoinBool(layout.aux_next_ptrs != nullptr);
	result += ",bloom_filter=" + ExecutionHashJoinBool(layout.bloom_filter != nullptr);
	result += ",blocker=" + layout.blocker;
	result += ">";
	return result;
}

string DescribeExecutionPerfectHashJoinTableLayout(const ExecutionPerfectHashJoinTableLayout &layout) {
	string result = "native_perfect_hash_join_table_layout<ready=" + ExecutionHashJoinBool(layout.ready);
	result += ",key_type=" + layout.key_type.ToString();
	result += ",key_physical_type=" + TypeIdToString(layout.key_physical_type);
	result += ",is_build_dense=" + ExecutionHashJoinBool(layout.is_build_dense);
	result += ",build_range=" + std::to_string(layout.build_range);
	result += ",build_capacity=" + std::to_string(layout.build_capacity);
	result += ",build_min=" + std::to_string(layout.build_min);
	result += ",build_max=" + std::to_string(layout.build_max);
	result += ",build_validity=" + ExecutionHashJoinBool(layout.build_validity != nullptr);
	result += ",rhs_output_columns=" + std::to_string(layout.rhs_output_column_count);
	result += ",rhs_output_types=" + ExecutionHashJoinTypeList(layout.rhs_output_types);
	result += ",rhs_dictionary_buffers=" + std::to_string(layout.rhs_dictionary_buffers.size());
	result += ",blocker=" + layout.blocker;
	result += ">";
	return result;
}

} // namespace duckdb
