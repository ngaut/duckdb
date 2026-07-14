//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_aggregate_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

#include <array>

namespace duckdb {

static constexpr idx_t EXECUTION_DENSE_GROUP_DOMAIN_MAX_TARGET_ENTRIES = STANDARD_VECTOR_SIZE * 4096ULL;

struct ExecutionDenseGroupDomain {
	bool ready = false;
	PhysicalType physical_type = PhysicalType::INVALID;
	idx_t min_key = 0;
	idx_t max_key = 0;
	idx_t distinct_count = 0;
};

// Semantic evidence produced while materializing grouped keys. Backends may
// provide this proof, but the aggregate runtime owns its validation and use.
struct ExecutionGroupedAggregateAppendProof {
	bool groups_strictly_increasing = false;
};

enum class ExecutionRowPointerGroupKeyCastKind : uint8_t {
	NONE,
	INT64_TO_INT32,
	INT64_TO_INT16,
	INT32_TO_INT8,
	INTEGRAL_COMPRESS,
	DATE_YEAR_COMPRESS,
	STRING_COMPRESS,
	STRING_SUBSTRING
};

static inline bool ExecutionGroupKeyCastIsNarrowingIntegral(ExecutionRowPointerGroupKeyCastKind cast_kind) {
	switch (cast_kind) {
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32:
	case ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16:
	case ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8:
		return true;
	default:
		return false;
	}
}

enum class ExecutionRowPointerGroupKeySourceKind : uint8_t { ROW_POINTER_FIELD, INPUT_VECTOR };

struct ExecutionRowPointerGroupKeySource {
	bool ready = false;
	ExecutionRowPointerGroupKeySourceKind source_kind = ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD;
	LogicalType source_type;
	LogicalType target_type;
	PhysicalType source_physical_type = PhysicalType::INVALID;
	PhysicalType target_physical_type = PhysicalType::INVALID;
	idx_t input_vector_index = DConstants::INVALID_INDEX;
	bool input_vector_repeats_with_row_pointer = false;
	idx_t hash_join_condition_idx = DConstants::INVALID_INDEX;
	PhysicalType hash_join_build_key_physical_type = PhysicalType::INVALID;
	idx_t row_layout_offset = DConstants::INVALID_INDEX;
	idx_t row_layout_column_idx = DConstants::INVALID_INDEX;
	idx_t row_layout_column_count = 0;
	ExecutionRowPointerGroupKeyCastKind cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
	int64_t cast_constant = 0;
	idx_t string_substring_length = 0;
	bool unchecked_integral_cast = false;
	bool all_valid = false;
	string blocker;
};

struct ExecutionGroupedAggregateStateTargetSpan {
	const uintptr_t *addresses = nullptr;
	const sel_t *address_sel = nullptr;
	const sel_t *row_sel = nullptr;
	idx_t count = 0;

	bool HasTargets() const {
		return count > 0;
	}

	void Set(const uintptr_t *addresses_p, const sel_t *row_sel_p, idx_t count_p,
	         const sel_t *address_sel_p = nullptr) {
		D_ASSERT(addresses_p || count_p == 0);
		addresses = addresses_p;
		address_sel = address_sel_p;
		row_sel = row_sel_p;
		count = count_p;
	}
};

enum class ExecutionGroupedAggregateStateTargetKind : uint8_t {
	INPUT_ORDER = 0,
	EXISTING = 1,
	NEW_GROUPS = 2,
	DUPLICATE_GROUPS = 3
};

static constexpr idx_t EXECUTION_GROUPED_AGGREGATE_STATE_TARGET_COUNT = 4;

struct ExecutionGroupedAggregateStateTargetBatch {
	std::array<ExecutionGroupedAggregateStateTargetSpan, EXECUTION_GROUPED_AGGREGATE_STATE_TARGET_COUNT> spans;

	ExecutionGroupedAggregateStateTargetSpan &Span(ExecutionGroupedAggregateStateTargetKind kind) {
		return spans[static_cast<idx_t>(kind)];
	}

	const ExecutionGroupedAggregateStateTargetSpan &Span(ExecutionGroupedAggregateStateTargetKind kind) const {
		return spans[static_cast<idx_t>(kind)];
	}

	ExecutionGroupedAggregateStateTargetSpan &InputOrder() {
		return Span(ExecutionGroupedAggregateStateTargetKind::INPUT_ORDER);
	}

	ExecutionGroupedAggregateStateTargetSpan &Existing() {
		return Span(ExecutionGroupedAggregateStateTargetKind::EXISTING);
	}

	ExecutionGroupedAggregateStateTargetSpan &NewGroups() {
		return Span(ExecutionGroupedAggregateStateTargetKind::NEW_GROUPS);
	}

	ExecutionGroupedAggregateStateTargetSpan &DuplicateGroups() {
		return Span(ExecutionGroupedAggregateStateTargetKind::DUPLICATE_GROUPS);
	}

	const std::array<ExecutionGroupedAggregateStateTargetSpan, EXECUTION_GROUPED_AGGREGATE_STATE_TARGET_COUNT> &
	Spans() const {
		return spans;
	}

	void Reset() {
		for (auto &span : spans) {
			span = ExecutionGroupedAggregateStateTargetSpan();
		}
	}
};

enum class ExecutionGroupedAggregateStateAddressUpdateMode : uint8_t { UPDATE_INITIALIZED, INITIALIZE_AND_UPDATE };

// The fresh-state mode is issued only for trivially destructible aggregate layouts. The callback must fully
// initialize every aggregate state before applying its update.
typedef void (*ExecutionGroupedAggregateStateAddressUpdateFunction)(
    const uintptr_t *addresses, const sel_t *address_sel, idx_t count,
    ExecutionGroupedAggregateStateAddressUpdateMode mode, void *state);
typedef void (*ExecutionGroupedAggregateStateSelectedAddressUpdateFunction)(const uintptr_t *addresses,
                                                                            const sel_t *address_sel,
                                                                            const sel_t *execute_sel, idx_t count,
                                                                            void *state);

DUCKDB_API bool
ExecutionRowPointerGroupKeySourcesAreRowPointerFields(const vector<ExecutionRowPointerGroupKeySource> &group_sources);

DUCKDB_API bool ExecutionRowPointerGroupKeysEqual(data_ptr_t left_row_pointer, data_ptr_t right_row_pointer,
                                                  const vector<ExecutionRowPointerGroupKeySource> &group_sources);

} // namespace duckdb
