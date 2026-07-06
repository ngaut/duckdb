//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_dispatch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_mark_selection_probe_dispatch_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_executor_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_matcher_runtime.hpp"

namespace duckdb {

static bool SljitHashJoinHasUint64PairEqualityKeys(const SljitNativeHashJoinProbePlan &plan) {
	if (plan.keys.size() != 2 || plan.equality_key_count != 2) {
		return false;
	}
	for (auto &key : plan.keys) {
		if (!SljitHashJoinHasAllValidEqualityKey(key) || !SljitHashJoinHasUint64CompatibleKey(key)) {
			return false;
		}
	}
	return true;
}

static bool SljitHashJoinHasUint64SingleEqualityNotEqualPredicate(const SljitNativeHashJoinProbePlan &plan) {
	if (plan.keys.size() != 2 || plan.equality_key_count != 1) {
		return false;
	}
	auto &key = plan.keys[0];
	auto &predicate = plan.keys[1];
	return SljitHashJoinHasAllValidEqualityKey(key) && !predicate.equality_key && !predicate.null_equal &&
	       predicate.comparison_type == ExecutionRegionComparisonType::NOT_EQUAL &&
	       SljitHashJoinHasUint64CompatibleKey(key) && SljitHashJoinHasUint64CompatibleKey(predicate);
}

template <bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidUint64PairProbe(const SljitNativeHashJoinProbePlan &plan,
                                              SljitNativeRegularHashJoinProbeInput &input,
                                              bool can_use_equality_chain_input = false) {
	if (!SljitHashJoinHasUint64PairEqualityKeys(plan)) {
		return false;
	}
	if constexpr (CHAIN) {
		if (!SljitHashJoinCanUseAllValidEqualityChainProbe(plan, input, SELECTED, can_use_equality_chain_input)) {
			return false;
		}
	} else {
		if (!SljitHashJoinCanUseAllValidNoChainProbe(plan, input, SELECTED)) {
			return false;
		}
	}
	SljitHashJoinUint64PairMatcher matcher(plan, input);
	ExecuteAllValidHashJoinProbeWithMatcher<SELECTED, CHAIN>(plan, input, matcher);
	return true;
}

template <bool SELECTED>
static bool TryExecuteAllValidSingleKeyNotEqualPredicateChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                                   SljitNativeRegularHashJoinProbeInput &input) {
	if (!SljitHashJoinHasUint64SingleEqualityNotEqualPredicate(plan) || plan.mark_build_match ||
	    !SljitHashJoinCanUseAllValidMatchedProbe(plan, input, SELECTED) ||
	    !SljitHashJoinCanUseAllValidChainInput(input)) {
		return false;
	}

	SljitHashJoinSingleUint64NotEqualPredicateMatcher matcher(plan, input);
	ExecuteAllValidHashJoinProbeWithMatcher<SELECTED, true>(plan, input, matcher);
	return true;
}

template <class KEY_READER, bool SELECTED, bool CHAIN>
static void ExecuteAllValidSingleKeyProbeForInput(const SljitNativeHashJoinProbePlan &plan,
                                                  SljitNativeRegularHashJoinProbeInput &input) {
	SljitHashJoinSingleKeyMatcher<KEY_READER> matcher(plan, input);
	ExecuteAllValidHashJoinProbeWithMatcher<SELECTED, CHAIN>(plan, input, matcher);
}

template <bool SELECTED, bool CHAIN>
struct SljitAllValidSingleKeyProbeDispatch {
	const SljitNativeHashJoinProbePlan &plan;
	SljitNativeRegularHashJoinProbeInput &input;

	template <class KEY_READER>
	void Execute() {
		ExecuteAllValidSingleKeyProbeForInput<KEY_READER, SELECTED, CHAIN>(plan, input);
	}
};

template <bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidSingleKeyProbe(const SljitNativeHashJoinProbePlan &plan,
                                             SljitNativeRegularHashJoinProbeInput &input,
                                             bool can_use_equality_chain_input = false) {
	if constexpr (CHAIN) {
		if (!SljitHashJoinCanUseSingleKeyProbe(plan) || plan.mark_build_match ||
		    !SljitHashJoinCanUseAllValidEqualityChainProbe(plan, input, SELECTED, can_use_equality_chain_input)) {
			return false;
		}
	} else {
		if (!SljitHashJoinCanUseSingleKeyProbe(plan) ||
		    !SljitHashJoinCanUseAllValidNoChainProbe(plan, input, SELECTED)) {
			return false;
		}
	}
	SljitAllValidSingleKeyProbeDispatch<SELECTED, CHAIN> dispatch {plan, input};
	return SljitDispatchHashJoinSingleKeyReader(plan, input, dispatch);
}

} // namespace duckdb
