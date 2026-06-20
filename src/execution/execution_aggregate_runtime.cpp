//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/execution_aggregate_runtime.cpp
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/execution_aggregate_runtime.hpp"

#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

namespace duckdb {

static string ExecutionAggregateBool(bool value) {
	return value ? "true" : "false";
}

static string ExecutionAggregateTypeList(const vector<LogicalType> &types) {
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

static string ExecutionAggregatePhysicalTypeList(const vector<PhysicalType> &types) {
	string result = "[";
	for (idx_t type_idx = 0; type_idx < types.size(); type_idx++) {
		if (type_idx > 0) {
			result += "|";
		}
		result += TypeIdToString(types[type_idx]);
	}
	result += "]";
	return result;
}

static string ExecutionAggregateIdxList(const vector<idx_t> &values) {
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

static bool ExecutionAggregatePhysicalTypeHasDirectRowEquality(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
	case PhysicalType::UINT16:
	case PhysicalType::INT16:
	case PhysicalType::UINT32:
	case PhysicalType::INT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT64:
	case PhysicalType::UINT128:
	case PhysicalType::INT128:
		return true;
	default:
		return false;
	}
}

bool ExecutionBuildHashAggregateLookupLayout(const TupleDataLayout &tuple_layout,
                                             ExecutionHashAggregateLookupLayout &layout) {
	layout = ExecutionHashAggregateLookupLayout();
	const auto layout_column_count = tuple_layout.ColumnCount();
	if (layout_column_count == 0) {
		layout.blocker = "hash-aggregate-layout-empty";
		return false;
	}

	layout.table_layout_ready = true;
	layout.can_have_null = tuple_layout.CanHaveNull();
	layout.group_count = layout_column_count - 1;
	auto &layout_types = tuple_layout.GetTypes();
	layout.group_types.reserve(layout.group_count);
	layout.group_physical_types.reserve(layout.group_count);
	layout.all_group_types_constant = true;
	for (idx_t group_idx = 0; group_idx < layout.group_count; group_idx++) {
		auto &group_type = layout_types[group_idx];
		auto physical_type = group_type.InternalType();
		layout.group_types.push_back(group_type);
		layout.group_physical_types.push_back(physical_type);
		if (!TypeIsConstantSize(physical_type)) {
			layout.all_group_types_constant = false;
			layout.variable_group_count++;
		}
	}
	layout.layout_column_count = layout_column_count;
	layout.layout_offsets = tuple_layout.GetOffsets();
	layout.row_validity_bytes = tuple_layout.GetDataOffset();
	layout.tuple_size = tuple_layout.GetRowWidth();
	layout.aggregate_state_offset = tuple_layout.GetAggrOffset();
	layout.hash_column_index = layout.group_count;
	layout.pointer_mask = ht_entry_t::POINTER_MASK;
	layout.salt_mask = ht_entry_t::SALT_MASK;
	layout.append_contract_ready = true;
	layout.append_contract_blocker.clear();
	layout.row_compare_contract_ready = layout.group_count > 0 && layout.all_group_types_constant;
	for (auto physical_type : layout.group_physical_types) {
		if (!ExecutionAggregatePhysicalTypeHasDirectRowEquality(physical_type)) {
			layout.row_compare_contract_ready = false;
			break;
		}
	}
	layout.row_compare_contract_blocker =
	    layout.row_compare_contract_ready ? "none" : "hash-aggregate-row-compare-direct-equality-contract-missing";
	layout.backend_lowering_ready = false;
	layout.backend_lowering_blocker = "hash-aggregate-generated-lookup-backend-lowering-missing";
	layout.blocker =
	    layout.row_compare_contract_ready ? layout.backend_lowering_blocker : layout.row_compare_contract_blocker;
	return true;
}

bool ExecutionGetHashAggregateLookupLayout(const GroupedAggregateHashTable &hash_table,
                                           ExecutionHashAggregateLookupLayout &layout) {
	return hash_table.GetExecutionHashAggregateLookupLayout(layout);
}

string DescribeExecutionHashAggregateLookupLayout(const ExecutionHashAggregateLookupLayout &layout) {
	string result = "native_hash_aggregate_lookup_layout<ready=" + ExecutionAggregateBool(layout.ready);
	result += ",table_layout_ready=" + ExecutionAggregateBool(layout.table_layout_ready);
	result += ",pointer_table_ready=" + ExecutionAggregateBool(layout.pointer_table_ready);
	result += ",in_memory=" + ExecutionAggregateBool(layout.in_memory);
	result += ",skip_lookups=" + ExecutionAggregateBool(layout.skip_lookups);
	result += ",can_have_null=" + ExecutionAggregateBool(layout.can_have_null);
	result += ",append_contract_ready=" + ExecutionAggregateBool(layout.append_contract_ready);
	result += ",row_compare_contract_ready=" + ExecutionAggregateBool(layout.row_compare_contract_ready);
	result += ",backend_lowering_ready=" + ExecutionAggregateBool(layout.backend_lowering_ready);
	result += ",group_count=" + std::to_string(layout.group_count);
	result += ",group_types=" + ExecutionAggregateTypeList(layout.group_types);
	result += ",group_physical_types=" + ExecutionAggregatePhysicalTypeList(layout.group_physical_types);
	result += ",layout_columns=" + std::to_string(layout.layout_column_count);
	result += ",layout_offsets=" + ExecutionAggregateIdxList(layout.layout_offsets);
	result += ",row_validity_bytes=" + std::to_string(layout.row_validity_bytes);
	result += ",tuple_size=" + std::to_string(layout.tuple_size);
	result += ",aggregate_state_offset=" + std::to_string(layout.aggregate_state_offset);
	result += ",hash_column_index=" + std::to_string(layout.hash_column_index);
	result += ",all_group_types_constant=" + ExecutionAggregateBool(layout.all_group_types_constant);
	result += ",variable_group_count=" + std::to_string(layout.variable_group_count);
	result += ",capacity=" + std::to_string(layout.capacity);
	result += ",bitmask=" + std::to_string(layout.bitmask);
	result += ",pointer_mask=" + std::to_string(layout.pointer_mask);
	result += ",salt_mask=" + std::to_string(layout.salt_mask);
	result += ",entries=" + ExecutionAggregateBool(layout.entries != nullptr);
	result += ",append_contract_blocker=" + layout.append_contract_blocker;
	result += ",row_compare_contract_blocker=" + layout.row_compare_contract_blocker;
	result += ",backend_lowering_blocker=" + layout.backend_lowering_blocker;
	result += ",blocker=" + layout.blocker;
	result += ">";
	return result;
}

} // namespace duckdb
