//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_hash_join_runtime.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/execution/execution_region_common.hpp"

namespace duckdb {

class BloomFilter;
class JoinHashTable;

enum class ExecutionHashJoinProbeLayoutKind : uint8_t { NONE, REGULAR_HASH_TABLE, PERFECT_HASH_TABLE };

struct ExecutionHashJoinTableLayout {
	bool ready = false;
	ExecutionRegionJoinType join_type = ExecutionRegionJoinType::INVALID;
	bool finalized = false;
	bool in_memory = false;
	bool needs_chain_matcher = false;
	bool chains_longer_than_one = false;
	bool residual_predicate = false;
	bool dictionary_emission = false;
	bool can_have_null = false;
	bool use_salt = false;
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
	const BloomFilter *bloom_filter = nullptr;
	bool exact_filter_build_keys_unique = false;
	shared_ptr<ExecutionRuntimeFilterIdentity> runtime_filter_identity;
	string blocker;
};

struct ExecutionPerfectHashJoinTableLayout {
	bool ready = false;
	LogicalType key_type;
	PhysicalType key_physical_type = PhysicalType::INVALID;
	bool is_build_dense = false;
	idx_t build_range = 0;
	idx_t build_capacity = 0;
	idx_t build_unique_count = 0;
	uint64_t build_min = 0;
	uint64_t build_max = 0;
	//! Wide integral bounds are kept as full values; the legacy words above are the <=64-bit ABI.
	hugeint_t build_min_128;
	hugeint_t build_max_128;
	uhugeint_t build_min_u128;
	uhugeint_t build_max_u128;
	const validity_t *build_validity = nullptr;
	shared_ptr<ExecutionRuntimeFilterIdentity> runtime_filter_identity;
	idx_t rhs_output_column_count = 0;
	vector<LogicalType> rhs_output_types;
	vector<buffer_ptr<DictionaryEntry>> rhs_dictionary_buffers;
	string blocker;
};

struct ExecutionHashJoinRHSFixedColumnSource {
	bool ready = false;
	LogicalType type;
	PhysicalType physical_type = PhysicalType::INVALID;
	idx_t rhs_output_idx = DConstants::INVALID_INDEX;
	idx_t layout_column_idx = DConstants::INVALID_INDEX;
	idx_t layout_column_count = 0;
	idx_t layout_offset = DConstants::INVALID_INDEX;
	bool all_valid = false;
	string blocker;
};

struct ExecutionHashJoinProbeOutputProof {
	bool source_key0_int64_to_int32 = false;
	bool match_selection_is_identity = false;
	//! A perfect-hash probe matched the complete input, so the consumer can
	//! derive its build row as the normalized key minus the table minimum.
	bool perfect_build_selection_is_key_offset = false;
	optional_ptr<const vector<idx_t>> exact_rhs_output_probe_input_indices;

	void Reset() {
		source_key0_int64_to_int32 = false;
		match_selection_is_identity = false;
		perfect_build_selection_is_key_offset = false;
		exact_rhs_output_probe_input_indices = nullptr;
	}

	void SetExactSourceFilterMatches(const vector<idx_t> &rhs_output_probe_input_indices) {
		match_selection_is_identity = true;
		perfect_build_selection_is_key_offset = false;
		exact_rhs_output_probe_input_indices = optional_ptr<const vector<idx_t>>(&rhs_output_probe_input_indices);
	}

	void SetExplicitMatchSelection() {
		match_selection_is_identity = false;
		perfect_build_selection_is_key_offset = false;
	}

	bool ExactSourceFilterMatches() const {
		return exact_rhs_output_probe_input_indices != nullptr;
	}

	const vector<idx_t> &ExactRHSOutputProbeInputIndices() const {
		D_ASSERT(ExactSourceFilterMatches());
		return *exact_rhs_output_probe_input_indices;
	}
};

DUCKDB_API bool ExecutionGetHashJoinTableLayout(const JoinHashTable &hash_table, ExecutionHashJoinTableLayout &layout);

DUCKDB_API string DescribeExecutionHashJoinTableLayout(const ExecutionHashJoinTableLayout &layout);
DUCKDB_API string DescribeExecutionPerfectHashJoinTableLayout(const ExecutionPerfectHashJoinTableLayout &layout);

} // namespace duckdb
