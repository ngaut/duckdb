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
	vector<uint8_t> found_match;
	bool left_initialized = false;
	bool left_unmatched_emitted = false;
	ExecutionHashJoinProbeOutputProof output_proof;
	bool finished = false;
};

} // namespace duckdb
