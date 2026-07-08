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

template <bool SELECTED>
static bool TryExecuteAllValidUint64PairNoChainProbeFastest(const SljitNativeHashJoinProbePlan &plan,
                                                            SljitNativeRegularHashJoinProbeInput &input) {
	if (!SljitHashJoinHasUint64PairEqualityKeys(plan) ||
	    !SljitHashJoinCanUseAllValidNoChainProbe(plan, input, SELECTED) ||
	    input.layout_kind != SljitHashJoinProbeLayoutKind::NO_CHAIN || input.bloom_filter_bits ||
	    plan.mark_build_match || plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
		return false;
	}

	const auto count = input.count;
	if (count > input.output_capacity) {
		throw InternalException("SLJIT no-chain hash join probe input exceeds output capacity");
	}

	const auto key0_data = reinterpret_cast<const uint64_t *__restrict>(input.source_data[0]);
	const auto key1_data = reinterpret_cast<const uint64_t *__restrict>(input.source_data[1]);
	const sel_t *__restrict key_sel = nullptr;
	if constexpr (SELECTED) {
		key_sel = input.source_sel[0];
	}
	const auto entries = reinterpret_cast<const ht_entry_t *__restrict>(input.entries);
	const auto bitmask = input.bitmask;
	const auto key0_offset = plan.keys[0].key_layout_offset;
	const auto key1_offset = plan.keys[1].key_layout_offset;
	data_ptr_t *__restrict row_pointers = input.row_pointers;
	sel_t *__restrict match_sel = input.match_sel;
	auto selected_count = input.selected_count;

	auto row_idx = input.input_offset;
	if (row_idx < count) {
		auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		auto key0 = key0_data[source_idx];
		auto key1 = key1_data[source_idx];
		auto ht_offset =
		    UnsafeNumericCast<idx_t>(SljitHashJoinCombineHashScalar(Hash<uint64_t>(key0), Hash<uint64_t>(key1)) &
		                             bitmask);
		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < count;
			uint64_t next_key0 = 0;
			uint64_t next_key1 = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				const auto next_source_idx = SELECTED ? key_sel[next_row_idx] : UnsafeNumericCast<sel_t>(next_row_idx);
				next_key0 = key0_data[next_source_idx];
				next_key1 = key1_data[next_source_idx];
				next_ht_offset = UnsafeNumericCast<idx_t>(
				    SljitHashJoinCombineHashScalar(Hash<uint64_t>(next_key0), Hash<uint64_t>(next_key1)) & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			while (true) {
				const auto entry_value = entries[ht_offset].GetValue();
				if (!entry_value) {
					break;
				}
				auto row_location = SljitHashJoinEntryPointer(entry_value);
				if (SljitHashJoinKeysEqual<uint64_t>(row_location, key0_offset, key0) &&
				    SljitHashJoinKeysEqual<uint64_t>(row_location, key1_offset, key1)) {
					row_pointers[selected_count] = row_location;
					match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
					selected_count++;
					break;
				}
				ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
			}
			if (!has_next) {
				break;
			}
			row_idx = next_row_idx;
			key0 = next_key0;
			key1 = next_key1;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
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
		if (TryExecuteAllValidUint64PairNoChainProbeFastest<SELECTED>(plan, input)) {
			return true;
		}
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
