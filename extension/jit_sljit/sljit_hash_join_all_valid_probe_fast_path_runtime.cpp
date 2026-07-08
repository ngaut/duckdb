//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_fast_path_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_hash_join_all_valid_probe_fast_path_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_dispatch_runtime.hpp"

namespace duckdb {

static constexpr const char *SLJIT_GENERATED_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "regular_probe.generated.all_valid.flat";
static constexpr const char *SLJIT_GENERATED_MARK_MATCH_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "regular_probe.generated.all_valid.mark_match.flat";
static constexpr const char *SLJIT_GENERATED_MARK_NONMATCH_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "regular_probe.generated.all_valid.mark_nonmatch.flat";
static constexpr const char *SLJIT_GENERATED_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "regular_probe.generated.all_valid.selected";
static constexpr const char *SLJIT_GENERATED_MARK_MATCH_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "regular_probe.generated.all_valid.mark_match.selected";
static constexpr const char *SLJIT_GENERATED_MARK_NONMATCH_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "regular_probe.generated.all_valid.mark_nonmatch.selected";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.flat.int64_pair.no_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.flat.int64_pair.chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.selected.int64_pair.no_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.selected.int64_pair.chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.flat.single_key.notequal_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.flat.single_key.no_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.selected.single_key.no_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.flat.single_key.chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.selected.single_key.chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.selected.single_key.notequal_chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.flat.int64_pair.no_chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.flat.int64_pair.chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.selected.int64_pair.no_chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.selected.int64_pair.chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_SINGLE_KEY_COMPARISON_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.flat.single_key.comparison_chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_SINGLE_KEY_COMPARISON_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.flat.single_key.comparison_no_chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_SINGLE_KEY_COMPARISON_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.selected.single_key.comparison_no_chain";
static constexpr const char
    *SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_SINGLE_KEY_COMPARISON_CHAIN_HASH_JOIN_PROBE_STAGE =
        "regular_probe.all_valid.mark_match.selected.single_key.comparison_chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.flat.single_key.no_chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.selected.single_key.no_chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.flat.single_key.chain";
static constexpr const char *SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_match.selected.single_key.chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.flat.int64_pair.no_chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.flat.int64_pair.chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.selected.int64_pair.no_chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.selected.int64_pair.chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_SINGLE_KEY_COMPARISON_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.flat.single_key.comparison_chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_SINGLE_KEY_COMPARISON_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.flat.single_key.comparison_no_chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_SINGLE_KEY_COMPARISON_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.selected.single_key.comparison_no_chain";
static constexpr const char
    *SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_SINGLE_KEY_COMPARISON_CHAIN_HASH_JOIN_PROBE_STAGE =
        "regular_probe.all_valid.mark_nonmatch.selected.single_key.comparison_chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.flat.single_key.no_chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.selected.single_key.no_chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.flat.single_key.chain";
static constexpr const char *SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "regular_probe.all_valid.mark_nonmatch.selected.single_key.chain";

template <bool SELECTED, bool CHAIN>
static bool ExecuteAllValidUint64PairProbeFastPath(const SljitNativeHashJoinProbePlan &plan,
                                                   SljitNativeRegularHashJoinProbeInput &input,
                                                   const SljitAllValidHashJoinProbeFacts &facts) {
	if constexpr (CHAIN) {
		return TryExecuteAllValidUint64PairProbe<SELECTED, true>(plan, input, facts.can_use_equality_chain_input);
	}
	return TryExecuteAllValidUint64PairProbe<SELECTED, false>(plan, input);
}

template <bool SELECTED>
static bool ExecuteAllValidSingleKeyNotEqualPredicateChainProbeFastPath(const SljitNativeHashJoinProbePlan &plan,
                                                                        SljitNativeRegularHashJoinProbeInput &input,
                                                                        const SljitAllValidHashJoinProbeFacts &) {
	return TryExecuteAllValidSingleKeyNotEqualPredicateChainProbe<SELECTED>(plan, input);
}

template <bool SELECTED, bool CHAIN>
static bool ExecuteAllValidSingleKeyProbeFastPath(const SljitNativeHashJoinProbePlan &plan,
                                                  SljitNativeRegularHashJoinProbeInput &input,
                                                  const SljitAllValidHashJoinProbeFacts &facts) {
	if constexpr (CHAIN) {
		return TryExecuteAllValidSingleKeyProbe<SELECTED, true>(plan, input, facts.can_use_equality_chain_input);
	}
	return TryExecuteAllValidSingleKeyProbe<SELECTED, false>(plan, input);
}

template <bool SELECTED, bool CHAIN>
static bool ExecuteAllValidUint64PairMarkSelectionProbeFastPath(const SljitNativeHashJoinProbePlan &plan,
                                                                SljitNativeRegularHashJoinProbeInput &input,
                                                                const SljitAllValidHashJoinProbeFacts &facts,
                                                                SljitHashJoinMarkSelectionMode mark_selection_mode) {
	return TryExecuteAllValidUint64PairMarkSelectionProbe<SELECTED, CHAIN>(plan, input, facts, mark_selection_mode);
}

template <bool SELECTED, bool CHAIN>
static bool ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeFastPath(
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input,
    const SljitAllValidHashJoinProbeFacts &facts, SljitHashJoinMarkSelectionMode mark_selection_mode) {
	return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbe<SELECTED, CHAIN>(plan, input, facts,
	                                                                                         mark_selection_mode);
}

template <bool SELECTED, bool CHAIN>
static bool ExecuteAllValidSingleKeyMarkSelectionProbeFastPath(const SljitNativeHashJoinProbePlan &plan,
                                                               SljitNativeRegularHashJoinProbeInput &input,
                                                               const SljitAllValidHashJoinProbeFacts &facts,
                                                               SljitHashJoinMarkSelectionMode mark_selection_mode) {
	return TryExecuteAllValidSingleKeyMarkSelectionProbe<SELECTED, CHAIN>(plan, input, facts, mark_selection_mode);
}

template <bool SELECTED>
static const std::array<SljitAllValidHashJoinProbeFastPath, 5> &SljitAllValidHashJoinProbeFastPaths() {
	static const std::array<SljitAllValidHashJoinProbeFastPath, 5> fast_paths {{
	    {SLJIT_FAST_FLAT_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidUint64PairProbeFastPath<SELECTED, false>},
	    {SLJIT_FAST_FLAT_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidUint64PairProbeFastPath<SELECTED, true>},
	    {SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyNotEqualPredicateChainProbeFastPath<SELECTED>},
	    {SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyProbeFastPath<SELECTED, false>},
	    {SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyProbeFastPath<SELECTED, true>},
	}};
	return fast_paths;
}

template <bool SELECTED>
static const std::array<SljitAllValidHashJoinMarkSelectionProbeFastPath, 6> &
SljitAllValidHashJoinMarkSelectionProbeFastPaths() {
	static const std::array<SljitAllValidHashJoinMarkSelectionProbeFastPath, 6> fast_paths {{
	    {SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_UINT64_PAIR_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidUint64PairMarkSelectionProbeFastPath<SELECTED, false>},
	    {SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_UINT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidUint64PairMarkSelectionProbeFastPath<SELECTED, true>},
	    {SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_SINGLE_KEY_COMPARISON_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_SINGLE_KEY_COMPARISON_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_SINGLE_KEY_COMPARISON_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_SINGLE_KEY_COMPARISON_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeFastPath<SELECTED, false>},
	    {SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_SINGLE_KEY_COMPARISON_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_SINGLE_KEY_COMPARISON_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_SINGLE_KEY_COMPARISON_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_SINGLE_KEY_COMPARISON_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeFastPath<SELECTED, true>},
	    {SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyMarkSelectionProbeFastPath<SELECTED, false>},
	    {SLJIT_FAST_MARK_MATCH_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_MATCH_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
	     SLJIT_FAST_MARK_NONMATCH_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
	     ExecuteAllValidSingleKeyMarkSelectionProbeFastPath<SELECTED, true>},
	}};
	return fast_paths;
}

const std::array<SljitAllValidHashJoinProbeFastPath, 5> &SljitAllValidHashJoinProbeFastPaths(bool selected) {
	return selected ? SljitAllValidHashJoinProbeFastPaths<true>() : SljitAllValidHashJoinProbeFastPaths<false>();
}

const std::array<SljitAllValidHashJoinMarkSelectionProbeFastPath, 6> &
SljitAllValidHashJoinMarkSelectionProbeFastPaths(bool selected) {
	return selected ? SljitAllValidHashJoinMarkSelectionProbeFastPaths<true>()
	                : SljitAllValidHashJoinMarkSelectionProbeFastPaths<false>();
}

const char *SljitAllValidHashJoinProbeFastPathStage(const SljitAllValidHashJoinProbeFastPath &fast_path,
                                                    bool selected) {
	return selected ? fast_path.selected_stage : fast_path.flat_stage;
}

const char *
SljitAllValidHashJoinMarkSelectionProbeFastPathStage(const SljitAllValidHashJoinMarkSelectionProbeFastPath &fast_path,
                                                     bool selected,
                                                     SljitHashJoinMarkSelectionMode mark_selection_mode) {
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES) {
		return selected ? fast_path.selected_match_stage : fast_path.flat_match_stage;
	}
	return selected ? fast_path.selected_nonmatch_stage : fast_path.flat_nonmatch_stage;
}

const char *SljitGeneratedAllValidRegularHashJoinProbeStage(bool selected) {
	return selected ? SLJIT_GENERATED_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE
	                : SLJIT_GENERATED_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE;
}

const char *SljitGeneratedAllValidRegularHashJoinProbeStage(bool selected,
                                                            SljitHashJoinMarkSelectionMode mark_selection_mode) {
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::NONE) {
		return SljitGeneratedAllValidRegularHashJoinProbeStage(selected);
	}
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES) {
		return selected ? SLJIT_GENERATED_MARK_MATCH_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE
		                : SLJIT_GENERATED_MARK_MATCH_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE;
	}
	return selected ? SLJIT_GENERATED_MARK_NONMATCH_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE
	                : SLJIT_GENERATED_MARK_NONMATCH_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE;
}

} // namespace duckdb
