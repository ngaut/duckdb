//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_aggregate_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/ht_entry.hpp"

namespace duckdb {

class GroupedAggregateHashTable;
class TupleDataLayout;

struct ExecutionHashAggregateLookupLayout {
	bool ready = false;
	bool table_layout_ready = false;
	bool pointer_table_ready = false;
	bool in_memory = false;
	bool skip_lookups = false;
	bool can_have_null = false;
	bool append_contract_ready = false;
	bool row_compare_contract_ready = false;
	bool backend_lowering_ready = false;
	idx_t group_count = 0;
	vector<LogicalType> group_types;
	vector<PhysicalType> group_physical_types;
	idx_t layout_column_count = 0;
	vector<idx_t> layout_offsets;
	idx_t row_validity_bytes = 0;
	idx_t tuple_size = 0;
	idx_t aggregate_state_offset = 0;
	idx_t hash_column_index = 0;
	bool all_group_types_constant = false;
	idx_t variable_group_count = 0;
	idx_t capacity = 0;
	uint64_t bitmask = 0;
	uint64_t pointer_mask = 0;
	uint64_t salt_mask = 0;
	ht_entry_t *entries = nullptr;
	string append_contract_blocker;
	string row_compare_contract_blocker;
	string backend_lowering_blocker;
	string blocker;
};

enum class ExecutionRowPointerGroupKeyCastKind : uint8_t {
	NONE,
	INT64_TO_INT32,
	INT64_TO_INT16,
	INT32_TO_INT8,
	INTEGRAL_COMPRESS,
	STRING_COMPRESS
};

enum class ExecutionRowPointerGroupKeySourceKind : uint8_t { ROW_POINTER_FIELD, INPUT_VECTOR };

struct ExecutionRowPointerGroupKeySource {
	bool ready = false;
	ExecutionRowPointerGroupKeySourceKind source_kind = ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD;
	LogicalType target_type;
	PhysicalType source_physical_type = PhysicalType::INVALID;
	PhysicalType target_physical_type = PhysicalType::INVALID;
	idx_t input_vector_index = DConstants::INVALID_INDEX;
	idx_t row_layout_offset = DConstants::INVALID_INDEX;
	idx_t row_layout_column_idx = DConstants::INVALID_INDEX;
	idx_t row_layout_column_count = 0;
	ExecutionRowPointerGroupKeyCastKind cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
	int64_t cast_constant = 0;
	bool all_valid = false;
	string blocker;
};

typedef void (*ExecutionGroupedAggregateStateAddressUpdateFunction)(const uintptr_t *addresses,
                                                                    const sel_t *address_sel, idx_t count,
                                                                    void *state);
typedef void (*ExecutionGroupedAggregateStateSelectedAddressUpdateFunction)(const uintptr_t *addresses,
                                                                           const sel_t *address_sel,
                                                                           const sel_t *execute_sel, idx_t count,
                                                                           void *state);

DUCKDB_API bool ExecutionBuildHashAggregateLookupLayout(const TupleDataLayout &tuple_layout,
                                                        ExecutionHashAggregateLookupLayout &layout);

DUCKDB_API bool ExecutionGetHashAggregateLookupLayout(const GroupedAggregateHashTable &hash_table,
                                                      ExecutionHashAggregateLookupLayout &layout);

DUCKDB_API string DescribeExecutionHashAggregateLookupLayout(const ExecutionHashAggregateLookupLayout &layout);

} // namespace duckdb
