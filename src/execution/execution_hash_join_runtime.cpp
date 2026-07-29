//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution_hash_join_runtime.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/execution_hash_join_runtime.hpp"

#include "duckdb/execution/join_hashtable.hpp"

#include "execution_hash_join_layout.hpp"

namespace duckdb {

ExecutionHashJoinRowLayout BuildExecutionHashJoinRowLayout(const vector<LogicalType> &condition_types,
                                                           const vector<LogicalType> &payload_types,
                                                           bool found_match_column_present) {
	ExecutionHashJoinRowLayout result;
	const auto condition_count = condition_types.size();
	const auto payload_column_count = payload_types.size();

	vector<LogicalType> layout_types(condition_types);
	layout_types.insert(layout_types.end(), payload_types.begin(), payload_types.end());
	if (found_match_column_present) {
		layout_types.emplace_back(LogicalType::BOOLEAN);
	}
	const auto hash_column_index = layout_types.size();
	layout_types.emplace_back(LogicalType::HASH);

	result.layout = make_shared_ptr<TupleDataLayout>();
	result.layout->Initialize(std::move(layout_types), TupleDataValidityType::CAN_HAVE_NULL_VALUES);
	const auto &offsets = result.layout->GetOffsets();
	D_ASSERT(offsets.size() > hash_column_index);
	result.tuple_size = offsets[condition_count + payload_column_count];
	result.pointer_offset = offsets.back();
	result.entry_size = result.layout->GetRowWidth();
	return result;
}

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
	string result = "native_hash_join_table_layout<status=";
	result += ExecutionHashJoinTableLayoutStatusToString(layout.status);
	result += ",needs_chain_matcher=" + ExecutionHashJoinBool(layout.needs_chain_matcher);
	result += ",chains_longer_than_one=" + ExecutionHashJoinBool(layout.chains_longer_than_one);
	result += ",dictionary_emission=" + ExecutionHashJoinBool(layout.dictionary_emission);
	result += ",use_salt=" + ExecutionHashJoinBool(layout.use_salt);
	result += ",null_keys_are_filtered=" + ExecutionHashJoinBool(layout.null_keys_are_filtered);
	result += ",stored_keys_have_null=" + ExecutionHashJoinBool(layout.stored_keys_have_null);
	result += ",condition_count=" + std::to_string(layout.condition_count);
	result += ",condition_types=" + ExecutionHashJoinTypeList(layout.condition_types);
	result += ",layout_columns=" + std::to_string(layout.layout_column_count);
	result += ",layout_offsets=" + ExecutionHashJoinIdxList(layout.layout_offsets);
	result += ",tuple_size=" + std::to_string(layout.tuple_size);
	result += ",pointer_offset=" + std::to_string(layout.pointer_offset);
	result += ",found_match_column_present=" + ExecutionHashJoinBool(layout.found_match_column_present);
	result += ",capacity=" + std::to_string(layout.entries.capacity);
	result += ",bitmask=" + std::to_string(layout.entries.bitmask);
	result += ",pointer_mask=" + std::to_string(layout.entries.pointer_mask);
	result += ",salt_mask=" + std::to_string(layout.entries.salt_mask);
	result += ",entries=" + ExecutionHashJoinBool(layout.entries.words != nullptr);
	result += ",aux_next_ptrs=" + ExecutionHashJoinBool(layout.entries.aux_next_ptrs != nullptr);
	result += ",bloom_filter=" + ExecutionHashJoinBool(layout.bloom_filter.words != nullptr);
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
	if (layout.key_physical_type == PhysicalType::INT128) {
		result += ",build_min=" + Hugeint::ToString(layout.build_min_128);
		result += ",build_max=" + Hugeint::ToString(layout.build_max_128);
	} else if (layout.key_physical_type == PhysicalType::UINT128) {
		result += ",build_min=" + Uhugeint::ToString(layout.build_min_u128);
		result += ",build_max=" + Uhugeint::ToString(layout.build_max_u128);
	} else {
		result += ",build_min=" + std::to_string(layout.build_min);
		result += ",build_max=" + std::to_string(layout.build_max);
	}
	result += ",build_validity=" + ExecutionHashJoinBool(layout.build_validity != nullptr);
	result +=
	    ",build_validity_non_empty_words=" + ExecutionHashJoinBool(layout.build_validity_non_empty_words != nullptr);
	result += ",build_validity_word_count=" + std::to_string(layout.build_validity_word_count);
	result += ",rhs_output_columns=" + std::to_string(layout.rhs_output_column_count);
	result += ",rhs_output_types=" + ExecutionHashJoinTypeList(layout.rhs_output_types);
	result += ",rhs_dictionary_buffers=" + std::to_string(layout.rhs_dictionary_buffers.size());
	result += ",blocker=" + layout.blocker;
	result += ">";
	return result;
}

} // namespace duckdb
