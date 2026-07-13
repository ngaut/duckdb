//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_drain_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct SljitHashJoinProbeDrainState {
	idx_t input_offset = 0;
	data_ptr_t resume_row_pointer = nullptr;
	vector<uint8_t> found_match;
	bool left_initialized = false;
	bool left_unmatched_emitted = false;
	bool source_key0_int64_to_int32_matches_are_proven = false;
	bool exact_source_filter_matches_are_proven = false;
	bool finished = false;
};

struct SljitNestedLoopJoinProbeDrainState {
	bool lhs_materialized = false;
	bool started = false;
	bool right_chunk_finished = false;
	bool finished = false;
	idx_t left_offset = 0;
	idx_t right_offset = 0;
};

} // namespace duckdb
