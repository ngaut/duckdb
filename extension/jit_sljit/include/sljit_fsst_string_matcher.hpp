//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_fsst_string_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

struct SljitNativePredicate;

//! A deterministic automaton over bytes that accepts the strings a predicate over one string column selects
struct SljitStringPatternAutomaton {
	//! transitions[state * 256 + byte]
	vector<uint8_t> transitions;
	vector<bool> accepting;

	idx_t StateCount() const {
		return accepting.size();
	}
};

//! Whether the automaton can express a predicate over one string column
bool SljitStringPatternAutomatonSupports(const SljitNativePredicate &predicate);
//! Builds the automaton of a predicate over one string column, or returns false for a predicate it cannot express
bool TryBuildSljitStringPatternAutomaton(const SljitNativePredicate &predicate,
                                         unique_ptr<SljitStringPatternAutomaton> &result);

//! Runs the automaton over FSST codes: every symbol advances it by the symbol's bytes at once
unique_ptr<CompressedStringMatcher> CreateSljitFSSTStringMatcher(const SljitStringPatternAutomaton &automaton,
                                                                 const uint8_t *symbol_lengths,
                                                                 const uint64_t *symbols);

} // namespace duckdb
