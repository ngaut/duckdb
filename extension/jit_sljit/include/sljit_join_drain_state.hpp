//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_join_drain_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/execution_hash_join_runtime.hpp"

namespace duckdb {

struct SljitHashJoinProbeDrainState {
	idx_t input_offset = 0;
	data_ptr_t resume_row_pointer = nullptr;
	//! Per input row: whether a key match survived the probe (LEFT unmatched rows, SEMI residual dedupe)
	vector<uint8_t> found_match;
	bool found_match_initialized = false;
	bool left_unmatched_emitted = false;
	ExecutionHashJoinProbeOutputProof output_proof;
	bool finished = false;
};

} // namespace duckdb
