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

LogicalType ExecutionHashJoinDictIndexType(uint8_t index_width) {
	switch (index_width) {
	case sizeof(uint8_t):
		return LogicalType::UTINYINT;
	case sizeof(uint16_t):
		return LogicalType::USMALLINT;
	case sizeof(uint32_t):
		return LogicalType::UINTEGER;
	default:
		throw InternalException("Unsupported dict-surviving hash join index width %d", index_width);
	}
}

ExecutionHashJoinRowLayout BuildExecutionHashJoinRowLayout(const vector<LogicalType> &condition_types,
                                                           const vector<LogicalType> &payload_types,
                                                           bool found_match_column_present,
                                                           const vector<uint8_t> &payload_dict_index_widths) {
	D_ASSERT(payload_dict_index_widths.empty() || payload_dict_index_widths.size() == payload_types.size());
	ExecutionHashJoinRowLayout result;
	const auto condition_count = condition_types.size();
	const auto payload_column_count = payload_types.size();

	vector<LogicalType> layout_types(condition_types);
	for (idx_t payload_idx = 0; payload_idx < payload_column_count; payload_idx++) {
		const auto index_width = payload_dict_index_widths.empty() ? 0 : payload_dict_index_widths[payload_idx];
		layout_types.push_back(index_width == 0 ? payload_types[payload_idx]
		                                        : ExecutionHashJoinDictIndexType(index_width));
	}
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

bool ExecutionGetHashJoinTableLayout(const JoinHashTable &hash_table, ExecutionHashJoinTableLayout &layout) {
	return hash_table.GetExecutionHashJoinTableLayout(layout);
}

} // namespace duckdb
