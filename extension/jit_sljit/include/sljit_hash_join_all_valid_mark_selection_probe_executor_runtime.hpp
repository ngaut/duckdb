//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_mark_selection_probe_executor_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_probe_base_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_consumer_runtime.hpp"

namespace duckdb {

template <bool SELECTED, bool DICTIONARY_EMISSION, bool USE_SALT, bool HAS_BLOOM, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinChainMarkSelectionProbe(SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher,
                                                           CONSUMER &consumer) {
	using Row = typename MATCHER::Row;
	const auto key_sel = SELECTED ? input.source_sel[0] : nullptr;
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto bitmask = input.bitmask;
	const auto count = input.count;
	const auto prefetch_offset = matcher.PrefetchOffset();

	for (idx_t row_idx = input.input_offset; row_idx < count; row_idx++) {
		const auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		const Row key_row = matcher.Load(source_idx);
		const auto hash = matcher.BuildHash(key_row);
		const auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);
		auto row_location = SljitHashJoinFindFirstChainPointer<USE_SALT, HAS_BLOOM>(
		    input, entries, hash, ht_offset, prefetch_offset,
		    [&](data_ptr_t candidate) { return matcher.MatchesFirst(key_row, candidate); });
		bool matched = false;
		bool row_matches_first = row_location != nullptr;
		while (row_location) {
			const auto next_row_location = SljitHashJoinNextChainPointer<DICTIONARY_EMISSION>(input, row_location);
			if (next_row_location) {
				SljitPrefetchHashJoinRow(next_row_location, prefetch_offset);
			}
			matched = row_matches_first ? matcher.MatchesKnownFirst(key_row, row_location)
			                            : matcher.Matches(key_row, row_location);
			row_matches_first = false;
			if (matched) {
				consumer.EmitMatch(row_idx, row_location);
				break;
			}
			row_location = next_row_location;
		}
		if (!matched) {
			consumer.EmitNoMatch(row_idx);
		}
	}

	consumer.Finish();
}

template <bool SELECTED, bool DICTIONARY_EMISSION, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinChainMarkSelectionProbeForInput(SljitNativeRegularHashJoinProbeInput &input,
                                                                   MATCHER matcher, CONSUMER &consumer) {
	if (SljitHashJoinProbeLayoutUsesSalt(input.layout_kind)) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinChainMarkSelectionProbe<SELECTED, DICTIONARY_EMISSION, true, true>(input, matcher,
			                                                                                          consumer);
		} else {
			ExecuteAllValidHashJoinChainMarkSelectionProbe<SELECTED, DICTIONARY_EMISSION, true, false>(input, matcher,
			                                                                                           consumer);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinChainMarkSelectionProbe<SELECTED, DICTIONARY_EMISSION, false, true>(input, matcher,
			                                                                                           consumer);
		} else {
			ExecuteAllValidHashJoinChainMarkSelectionProbe<SELECTED, DICTIONARY_EMISSION, false, false>(input, matcher,
			                                                                                            consumer);
		}
	}
}

template <bool SELECTED, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinChainMarkSelectionProbeForInput(SljitNativeRegularHashJoinProbeInput &input,
                                                                   MATCHER matcher, CONSUMER &consumer) {
	if (SljitHashJoinProbeLayoutUsesDictionaryEmission(input.layout_kind)) {
		ExecuteAllValidHashJoinChainMarkSelectionProbeForInput<SELECTED, true>(input, matcher, consumer);
	} else {
		ExecuteAllValidHashJoinChainMarkSelectionProbeForInput<SELECTED, false>(input, matcher, consumer);
	}
}

template <bool SELECTED, bool USE_SALT, bool HAS_BLOOM, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinChainMarkSelectionExistenceProbe(SljitNativeRegularHashJoinProbeInput &input,
                                                                    MATCHER matcher, CONSUMER &consumer) {
	using Row = typename MATCHER::Row;
	const auto key_sel = SELECTED ? input.source_sel[0] : nullptr;
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto bitmask = input.bitmask;
	const auto count = input.count;
	const auto prefetch_offset = matcher.PrefetchOffset();

	for (idx_t row_idx = input.input_offset; row_idx < count; row_idx++) {
		const auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		const Row key_row = matcher.Load(source_idx);
		const auto hash = matcher.BuildHash(key_row);
		const auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);
		auto row_location = SljitHashJoinFindFirstChainPointer<USE_SALT, HAS_BLOOM>(
		    input, entries, hash, ht_offset, prefetch_offset,
		    [&](data_ptr_t candidate) { return matcher.MatchesFirst(key_row, candidate); });
		if (row_location) {
			consumer.EmitMatch(row_idx, row_location);
		} else {
			consumer.EmitNoMatch(row_idx);
		}
	}

	consumer.Finish();
}

template <bool SELECTED, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinChainMarkSelectionExistenceProbeForInput(SljitNativeRegularHashJoinProbeInput &input,
                                                                            MATCHER matcher, CONSUMER &consumer) {
	if (SljitHashJoinProbeLayoutUsesSalt(input.layout_kind)) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinChainMarkSelectionExistenceProbe<SELECTED, true, true>(input, matcher, consumer);
		} else {
			ExecuteAllValidHashJoinChainMarkSelectionExistenceProbe<SELECTED, true, false>(input, matcher, consumer);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinChainMarkSelectionExistenceProbe<SELECTED, false, true>(input, matcher, consumer);
		} else {
			ExecuteAllValidHashJoinChainMarkSelectionExistenceProbe<SELECTED, false, false>(input, matcher, consumer);
		}
	}
}

template <bool SELECTED, bool USE_SALT, bool HAS_BLOOM, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinNoChainMarkSelectionProbe(SljitNativeRegularHashJoinProbeInput &input,
                                                             MATCHER matcher, CONSUMER &consumer) {
	using Row = typename MATCHER::Row;
	const auto entries = reinterpret_cast<const ht_entry_t *__restrict>(input.entries);
	const sel_t *__restrict key_sel = SELECTED ? input.source_sel[0] : nullptr;
	const auto bitmask = input.bitmask;
	const auto count = input.count;

	for (idx_t row_idx = input.input_offset; row_idx < count; row_idx++) {
		const auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		const Row key_row = matcher.Load(source_idx);
		const auto hash = matcher.BuildHash(key_row);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);
		bool matched = false;
		if (SljitBloomFilterMayContainTemplated<HAS_BLOOM>(input, hash)) {
			hash_t salt = 0;
			if constexpr (USE_SALT) {
				salt = hash & ht_entry_t::SALT_MASK;
			}
			while (true) {
				const auto entry_value = entries[ht_offset].GetValue();
				if (!entry_value) {
					break;
				}
				if constexpr (USE_SALT) {
					if ((entry_value & ht_entry_t::SALT_MASK) != salt) {
						ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
						continue;
					}
				}
				auto row_location = SljitHashJoinEntryPointer(entry_value);
				if (matcher.Matches(key_row, row_location)) {
					consumer.EmitMatch(row_idx, row_location);
					matched = true;
					break;
				}
				ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
			}
		}
		if (!matched) {
			consumer.EmitNoMatch(row_idx);
		}
	}

	consumer.Finish();
}

template <bool SELECTED, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinNoChainMarkSelectionProbeForInput(SljitNativeRegularHashJoinProbeInput &input,
                                                                     MATCHER matcher, CONSUMER &consumer) {
	if (SljitHashJoinProbeLayoutUsesSalt(input.layout_kind)) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinNoChainMarkSelectionProbe<SELECTED, true, true>(input, matcher, consumer);
		} else {
			ExecuteAllValidHashJoinNoChainMarkSelectionProbe<SELECTED, true, false>(input, matcher, consumer);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinNoChainMarkSelectionProbe<SELECTED, false, true>(input, matcher, consumer);
		} else {
			ExecuteAllValidHashJoinNoChainMarkSelectionProbe<SELECTED, false, false>(input, matcher, consumer);
		}
	}
}

template <bool SELECTED, bool CHAIN, SljitHashJoinMarkSelectionMode MODE, bool MARK_BUILD_MATCH, class MATCHER>
static void ExecuteAllValidHashJoinMarkSelectionProbeWithMatcherMode(const SljitNativeHashJoinProbePlan &plan,
                                                                     SljitNativeRegularHashJoinProbeInput &input,
                                                                     MATCHER matcher) {
	SljitHashJoinMarkSelectionConsumer<MODE, MARK_BUILD_MATCH> consumer(input, plan);
	if constexpr (CHAIN) {
		ExecuteAllValidHashJoinChainMarkSelectionProbeForInput<SELECTED>(input, matcher, consumer);
	} else {
		ExecuteAllValidHashJoinNoChainMarkSelectionProbeForInput<SELECTED>(input, matcher, consumer);
	}
}

template <bool SELECTED, bool CHAIN, SljitHashJoinMarkSelectionMode MODE, class MATCHER>
static void ExecuteAllValidHashJoinMarkSelectionProbeWithMatcher(const SljitNativeHashJoinProbePlan &plan,
                                                                 SljitNativeRegularHashJoinProbeInput &input,
                                                                 MATCHER matcher) {
	if (plan.mark_build_match) {
		ExecuteAllValidHashJoinMarkSelectionProbeWithMatcherMode<SELECTED, CHAIN, MODE, true>(plan, input, matcher);
	} else {
		ExecuteAllValidHashJoinMarkSelectionProbeWithMatcherMode<SELECTED, CHAIN, MODE, false>(plan, input, matcher);
	}
}

template <bool SELECTED, bool CHAIN, SljitHashJoinMarkSelectionMode MODE, bool MARK_BUILD_MATCH, class MATCHER>
static void ExecuteAllValidHashJoinEqualityMarkSelectionProbeWithMatcherMode(
    const SljitNativeHashJoinProbePlan &plan, SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher) {
	SljitHashJoinMarkSelectionConsumer<MODE, MARK_BUILD_MATCH> consumer(input, plan);
	if constexpr (CHAIN) {
		ExecuteAllValidHashJoinChainMarkSelectionExistenceProbeForInput<SELECTED>(input, matcher, consumer);
	} else {
		ExecuteAllValidHashJoinNoChainMarkSelectionProbeForInput<SELECTED>(input, matcher, consumer);
	}
}

template <bool SELECTED, bool CHAIN, SljitHashJoinMarkSelectionMode MODE, class MATCHER>
static void ExecuteAllValidHashJoinEqualityMarkSelectionProbeWithMatcher(const SljitNativeHashJoinProbePlan &plan,
                                                                         SljitNativeRegularHashJoinProbeInput &input,
                                                                         MATCHER matcher) {
	if (plan.mark_build_match) {
		ExecuteAllValidHashJoinEqualityMarkSelectionProbeWithMatcherMode<SELECTED, CHAIN, MODE, true>(plan, input,
		                                                                                              matcher);
	} else {
		ExecuteAllValidHashJoinEqualityMarkSelectionProbeWithMatcherMode<SELECTED, CHAIN, MODE, false>(plan, input,
		                                                                                               matcher);
	}
}

} // namespace duckdb
