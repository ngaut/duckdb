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
static bool TryExecuteAllValidSingleKeyProbeForKind(const SljitNativeHashJoinProbePlan &plan,
                                                    SljitNativeRegularHashJoinProbeInput &input) {
	auto &key = plan.keys[0];
	if (input.source_key0_int64_to_int32) {
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT32) {
			return false;
		}
		if (input.source_key0_int64_to_int32_unchecked) {
			ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinInt64ToInt32KeyReader<true>, SELECTED, CHAIN>(plan,
			                                                                                                 input);
		} else {
			ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinInt64ToInt32KeyReader<false>, SELECTED, CHAIN>(plan,
			                                                                                                  input);
		}
		return true;
	}

	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinDirectKeyReader<int8_t>, SELECTED, CHAIN>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinDirectKeyReader<uint8_t>, SELECTED, CHAIN>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinDirectKeyReader<int16_t>, SELECTED, CHAIN>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinDirectKeyReader<uint16_t>, SELECTED, CHAIN>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinDirectKeyReader<int32_t>, SELECTED, CHAIN>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinDirectKeyReader<uint32_t>, SELECTED, CHAIN>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinDirectKeyReader<int64_t>, SELECTED, CHAIN>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		ExecuteAllValidSingleKeyProbeForInput<SljitHashJoinDirectKeyReader<uint64_t>, SELECTED, CHAIN>(plan, input);
		return true;
	default:
		return false;
	}
}

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
	return TryExecuteAllValidSingleKeyProbeForKind<SELECTED, CHAIN>(plan, input);
}

} // namespace duckdb
