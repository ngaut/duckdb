//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_mark_selection_probe_dispatch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_probe_facts.hpp"
#include "sljit_hash_join_all_valid_mark_selection_probe_executor_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_matcher_runtime.hpp"

namespace duckdb {

static bool SljitHashJoinCanUseAllValidMarkSelectionProbe(const SljitNativeHashJoinProbePlan &plan,
                                                          const SljitNativeRegularHashJoinProbeInput &input,
                                                          bool selected,
                                                          SljitHashJoinMarkSelectionMode mark_selection_mode) {
	if (!SljitHashJoinEmitsMarkSelection(mark_selection_mode) ||
	    plan.output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE || plan.residual_predicate ||
	    input.source_validity || input.output_capacity < input.count || input.resume_row_pointer) {
		return false;
	}
	return selected ? input.source_sel && input.source_sel[0] : !input.source_sel;
}

static bool SljitHashJoinCanUseAllValidNoChainMarkSelectionProbe(const SljitNativeHashJoinProbePlan &plan,
                                                                 const SljitNativeRegularHashJoinProbeInput &input,
                                                                 bool selected,
                                                                 SljitHashJoinMarkSelectionMode mark_selection_mode) {
	return SljitHashJoinCanUseAllValidMarkSelectionProbe(plan, input, selected, mark_selection_mode) &&
	       !SljitHashJoinProbeLayoutChainsLongerThanOne(input.layout_kind);
}

static bool SljitHashJoinCanUseAllValidChainMarkSelectionProbe(const SljitNativeHashJoinProbePlan &plan,
                                                               const SljitNativeRegularHashJoinProbeInput &input,
                                                               bool selected, bool can_use_chain_input,
                                                               SljitHashJoinMarkSelectionMode mark_selection_mode) {
	return SljitHashJoinCanUseAllValidMarkSelectionProbe(plan, input, selected, mark_selection_mode) &&
	       can_use_chain_input;
}

template <class KEY0, class KEY1, bool SELECTED, bool CHAIN>
static void ExecuteAllValidPairMarkSelectionProbeForInput(const SljitNativeHashJoinProbePlan &plan,
                                                          SljitNativeRegularHashJoinProbeInput &input,
                                                          SljitHashJoinMarkSelectionMode mark_selection_mode) {
	SljitHashJoinPairEqualityMatcher<KEY0, KEY1> matcher(plan, input);
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES) {
		ExecuteAllValidHashJoinEqualityMarkSelectionProbeWithMatcher<SELECTED, CHAIN,
		                                                             SljitHashJoinMarkSelectionMode::MATCHES>(
		    plan, input, matcher);
	} else {
		ExecuteAllValidHashJoinEqualityMarkSelectionProbeWithMatcher<SELECTED, CHAIN,
		                                                             SljitHashJoinMarkSelectionMode::NON_MATCHES>(
		    plan, input, matcher);
	}
}

template <bool SELECTED, bool CHAIN>
struct SljitAllValidPairMarkSelectionProbeDispatch {
	const SljitNativeHashJoinProbePlan &plan;
	SljitNativeRegularHashJoinProbeInput &input;
	SljitHashJoinMarkSelectionMode mark_selection_mode;

	template <class KEY0, class KEY1>
	void Execute() {
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, KEY1, SELECTED, CHAIN>(plan, input, mark_selection_mode);
	}
};

template <bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidPairMarkSelectionProbe(const SljitNativeHashJoinProbePlan &plan,
                                                     SljitNativeRegularHashJoinProbeInput &input,
                                                     const SljitAllValidHashJoinProbeFacts &facts,
                                                     SljitHashJoinMarkSelectionMode mark_selection_mode) {
	if (plan.keys.size() != 2 || plan.equality_key_count != 2 || !SljitHashJoinHasAllValidEqualityKey(plan.keys[0]) ||
	    !SljitHashJoinHasAllValidEqualityKey(plan.keys[1])) {
		return false;
	}
	if constexpr (CHAIN) {
		if (!SljitHashJoinCanUseAllValidChainMarkSelectionProbe(plan, input, SELECTED, facts.can_use_chain_input,
		                                                        mark_selection_mode)) {
			return false;
		}
	} else {
		if (!SljitHashJoinCanUseAllValidNoChainMarkSelectionProbe(plan, input, SELECTED, mark_selection_mode)) {
			return false;
		}
	}
	SljitAllValidPairMarkSelectionProbeDispatch<SELECTED, CHAIN> dispatch {plan, input, mark_selection_mode};
	return SljitDispatchHashJoinPairKeyKinds(plan, dispatch);
}

template <bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidUint64PairMarkSelectionProbe(const SljitNativeHashJoinProbePlan &plan,
                                                           SljitNativeRegularHashJoinProbeInput &input,
                                                           const SljitAllValidHashJoinProbeFacts &facts,
                                                           SljitHashJoinMarkSelectionMode mark_selection_mode) {
	return TryExecuteAllValidPairMarkSelectionProbe<SELECTED, CHAIN>(plan, input, facts, mark_selection_mode);
}

template <class KEY_SOURCE, class KEY_LAYOUT, class PREDICATE, bool SELECTED, bool CHAIN>
static void ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput(
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input,
    SljitHashJoinMarkSelectionMode mark_selection_mode) {
	SljitHashJoinSingleKeyComparisonPredicateMatcher<KEY_SOURCE, KEY_LAYOUT, PREDICATE> matcher(plan, input);
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES) {
		ExecuteAllValidHashJoinMarkSelectionProbeWithMatcher<SELECTED, CHAIN, SljitHashJoinMarkSelectionMode::MATCHES>(
		    plan, input, matcher);
	} else {
		ExecuteAllValidHashJoinMarkSelectionProbeWithMatcher<SELECTED, CHAIN,
		                                                     SljitHashJoinMarkSelectionMode::NON_MATCHES>(plan, input,
		                                                                                                  matcher);
	}
}

template <bool SELECTED, bool CHAIN>
struct SljitAllValidComparisonPredicateMarkSelectionProbeDispatch {
	const SljitNativeHashJoinProbePlan &plan;
	SljitNativeRegularHashJoinProbeInput &input;
	SljitHashJoinMarkSelectionMode mark_selection_mode;

	template <class KEY_SOURCE, class KEY_LAYOUT, class PREDICATE>
	void Execute() {
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, PREDICATE,
		                                                                      SELECTED, CHAIN>(plan, input,
		                                                                                       mark_selection_mode);
	}
};

template <bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbe(
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input,
    const SljitAllValidHashJoinProbeFacts &facts, SljitHashJoinMarkSelectionMode mark_selection_mode) {
	if (plan.keys.size() != 2 || plan.equality_key_count != 1 || !SljitHashJoinHasAllValidEqualityKey(plan.keys[0]) ||
	    !SljitHashJoinHasPlainComparisonPredicateKey(plan.keys[1])) {
		return false;
	}
	if constexpr (CHAIN) {
		if (!SljitHashJoinCanUseAllValidChainMarkSelectionProbe(plan, input, SELECTED, facts.can_use_chain_input,
		                                                        mark_selection_mode)) {
			return false;
		}
	} else {
		if (!SljitHashJoinCanUseAllValidNoChainMarkSelectionProbe(plan, input, SELECTED, mark_selection_mode)) {
			return false;
		}
	}
	SljitAllValidComparisonPredicateMarkSelectionProbeDispatch<SELECTED, CHAIN> dispatch {plan, input,
	                                                                                      mark_selection_mode};
	return SljitDispatchHashJoinSingleComparisonPredicateKinds(plan, input, dispatch);
}

template <class KEY_READER, bool SELECTED, bool CHAIN>
static void ExecuteAllValidSingleKeyMarkSelectionProbeForInput(const SljitNativeHashJoinProbePlan &plan,
                                                               SljitNativeRegularHashJoinProbeInput &input,
                                                               SljitHashJoinMarkSelectionMode mark_selection_mode) {
	SljitHashJoinSingleKeyMatcher<KEY_READER> matcher(plan, input);
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES) {
		ExecuteAllValidHashJoinEqualityMarkSelectionProbeWithMatcher<SELECTED, CHAIN,
		                                                             SljitHashJoinMarkSelectionMode::MATCHES>(
		    plan, input, matcher);
	} else {
		ExecuteAllValidHashJoinEqualityMarkSelectionProbeWithMatcher<SELECTED, CHAIN,
		                                                             SljitHashJoinMarkSelectionMode::NON_MATCHES>(
		    plan, input, matcher);
	}
}

template <bool SELECTED, bool CHAIN>
struct SljitAllValidSingleKeyMarkSelectionProbeDispatch {
	const SljitNativeHashJoinProbePlan &plan;
	SljitNativeRegularHashJoinProbeInput &input;
	SljitHashJoinMarkSelectionMode mark_selection_mode;

	template <class KEY_READER>
	void Execute() {
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<KEY_READER, SELECTED, CHAIN>(plan, input,
		                                                                                mark_selection_mode);
	}
};

template <bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidSingleKeyMarkSelectionProbe(const SljitNativeHashJoinProbePlan &plan,
                                                          SljitNativeRegularHashJoinProbeInput &input,
                                                          const SljitAllValidHashJoinProbeFacts &facts,
                                                          SljitHashJoinMarkSelectionMode mark_selection_mode) {
	if constexpr (CHAIN) {
		if (!SljitHashJoinCanUseSingleKeyProbe(plan) ||
		    !SljitHashJoinCanUseAllValidChainMarkSelectionProbe(plan, input, SELECTED, facts.can_use_chain_input,
		                                                        mark_selection_mode)) {
			return false;
		}
	} else {
		if (!SljitHashJoinCanUseSingleKeyProbe(plan) ||
		    !SljitHashJoinCanUseAllValidNoChainMarkSelectionProbe(plan, input, SELECTED, mark_selection_mode)) {
			return false;
		}
	}
	SljitAllValidSingleKeyMarkSelectionProbeDispatch<SELECTED, CHAIN> dispatch {plan, input, mark_selection_mode};
	return SljitDispatchHashJoinSingleKeyReader(plan, input, dispatch);
}

} // namespace duckdb
