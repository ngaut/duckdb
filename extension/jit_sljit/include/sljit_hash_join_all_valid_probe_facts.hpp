//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_facts.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

struct SljitAllValidHashJoinProbeFacts {
	bool can_use_chain_input;
	bool can_use_equality_chain_input;
};

} // namespace duckdb
