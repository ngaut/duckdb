//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_mark_selection_probe_dispatch_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_probe_executor_runtime.hpp"
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

template <class KEY0, bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidPairMarkSelectionProbeForKey1Kind(const SljitNativeHashJoinProbePlan &plan,
                                                                SljitNativeRegularHashJoinProbeInput &input,
                                                                SljitHashJoinMarkSelectionMode mark_selection_mode) {
	auto &key1 = plan.keys[1];
	switch (key1.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, int8_t, SELECTED, CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, uint8_t, SELECTED, CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, int16_t, SELECTED, CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, uint16_t, SELECTED, CHAIN>(plan, input,
		                                                                               mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, int32_t, SELECTED, CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, uint32_t, SELECTED, CHAIN>(plan, input,
		                                                                               mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, int64_t, SELECTED, CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		ExecuteAllValidPairMarkSelectionProbeForInput<KEY0, uint64_t, SELECTED, CHAIN>(plan, input,
		                                                                               mark_selection_mode);
		return true;
	default:
		return false;
	}
}

template <bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidPairMarkSelectionProbeForKey0Kind(const SljitNativeHashJoinProbePlan &plan,
                                                                SljitNativeRegularHashJoinProbeInput &input,
                                                                SljitHashJoinMarkSelectionMode mark_selection_mode) {
	auto &key0 = plan.keys[0];
	switch (key0.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return TryExecuteAllValidPairMarkSelectionProbeForKey1Kind<int8_t, SELECTED, CHAIN>(plan, input,
		                                                                                    mark_selection_mode);
	case SljitNativeHashJoinKeyKind::UINT8:
		return TryExecuteAllValidPairMarkSelectionProbeForKey1Kind<uint8_t, SELECTED, CHAIN>(plan, input,
		                                                                                     mark_selection_mode);
	case SljitNativeHashJoinKeyKind::INT16:
		return TryExecuteAllValidPairMarkSelectionProbeForKey1Kind<int16_t, SELECTED, CHAIN>(plan, input,
		                                                                                     mark_selection_mode);
	case SljitNativeHashJoinKeyKind::UINT16:
		return TryExecuteAllValidPairMarkSelectionProbeForKey1Kind<uint16_t, SELECTED, CHAIN>(plan, input,
		                                                                                      mark_selection_mode);
	case SljitNativeHashJoinKeyKind::INT32:
		return TryExecuteAllValidPairMarkSelectionProbeForKey1Kind<int32_t, SELECTED, CHAIN>(plan, input,
		                                                                                     mark_selection_mode);
	case SljitNativeHashJoinKeyKind::UINT32:
		return TryExecuteAllValidPairMarkSelectionProbeForKey1Kind<uint32_t, SELECTED, CHAIN>(plan, input,
		                                                                                      mark_selection_mode);
	case SljitNativeHashJoinKeyKind::INT64:
		return TryExecuteAllValidPairMarkSelectionProbeForKey1Kind<int64_t, SELECTED, CHAIN>(plan, input,
		                                                                                     mark_selection_mode);
	case SljitNativeHashJoinKeyKind::UINT64:
		return TryExecuteAllValidPairMarkSelectionProbeForKey1Kind<uint64_t, SELECTED, CHAIN>(plan, input,
		                                                                                      mark_selection_mode);
	default:
		return false;
	}
}

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
	return TryExecuteAllValidPairMarkSelectionProbeForKey0Kind<SELECTED, CHAIN>(plan, input, mark_selection_mode);
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

template <class KEY_SOURCE, class KEY_LAYOUT, bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind(
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input,
    SljitHashJoinMarkSelectionMode mark_selection_mode) {
	auto &predicate = plan.keys[1];
	switch (predicate.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, int8_t, SELECTED,
		                                                                      CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, uint8_t, SELECTED,
		                                                                      CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, int16_t, SELECTED,
		                                                                      CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, uint16_t,
		                                                                      SELECTED, CHAIN>(plan, input,
		                                                                                       mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, int32_t, SELECTED,
		                                                                      CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, uint32_t,
		                                                                      SELECTED, CHAIN>(plan, input,
		                                                                                       mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, int64_t, SELECTED,
		                                                                      CHAIN>(plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		ExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForInput<KEY_SOURCE, KEY_LAYOUT, uint64_t,
		                                                                      SELECTED, CHAIN>(plan, input,
		                                                                                       mark_selection_mode);
		return true;
	default:
		return false;
	}
}

template <bool SELECTED, bool CHAIN>
static bool TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForKeyKind(
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input,
    SljitHashJoinMarkSelectionMode mark_selection_mode) {
	auto &key = plan.keys[0];
	if (input.source_key0_int64_to_int32) {
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT32) {
			return false;
		}
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<int64_t, int32_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	}
	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<int8_t, int8_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	case SljitNativeHashJoinKeyKind::UINT8:
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<uint8_t, uint8_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	case SljitNativeHashJoinKeyKind::INT16:
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<int16_t, int16_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	case SljitNativeHashJoinKeyKind::UINT16:
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<uint16_t, uint16_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	case SljitNativeHashJoinKeyKind::INT32:
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<int32_t, int32_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	case SljitNativeHashJoinKeyKind::UINT32:
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<uint32_t, uint32_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	case SljitNativeHashJoinKeyKind::INT64:
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<int64_t, int64_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	case SljitNativeHashJoinKeyKind::UINT64:
		return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForPredicateKind<uint64_t, uint64_t,
		                                                                                        SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
	default:
		return false;
	}
}

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
	return TryExecuteAllValidSingleKeyComparisonPredicateMarkSelectionProbeForKeyKind<SELECTED, CHAIN>(
	    plan, input, mark_selection_mode);
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
static bool TryExecuteAllValidSingleKeyMarkSelectionProbeForKind(const SljitNativeHashJoinProbePlan &plan,
                                                                 SljitNativeRegularHashJoinProbeInput &input,
                                                                 SljitHashJoinMarkSelectionMode mark_selection_mode) {
	auto &key = plan.keys[0];
	if (input.source_key0_int64_to_int32) {
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT32) {
			return false;
		}
		if (input.source_key0_int64_to_int32_unchecked) {
			ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinInt64ToInt32KeyReader<true>, SELECTED,
			                                                   CHAIN>(plan, input, mark_selection_mode);
		} else {
			ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinInt64ToInt32KeyReader<false>, SELECTED,
			                                                   CHAIN>(plan, input, mark_selection_mode);
		}
		return true;
	}

	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinDirectKeyReader<int8_t>, SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinDirectKeyReader<uint8_t>, SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinDirectKeyReader<int16_t>, SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinDirectKeyReader<uint16_t>, SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinDirectKeyReader<int32_t>, SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinDirectKeyReader<uint32_t>, SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinDirectKeyReader<int64_t>, SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		ExecuteAllValidSingleKeyMarkSelectionProbeForInput<SljitHashJoinDirectKeyReader<uint64_t>, SELECTED, CHAIN>(
		    plan, input, mark_selection_mode);
		return true;
	default:
		return false;
	}
}

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
	return TryExecuteAllValidSingleKeyMarkSelectionProbeForKind<SELECTED, CHAIN>(plan, input, mark_selection_mode);
}

} // namespace duckdb
