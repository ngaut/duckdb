//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_executor_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_mark_selection_probe_executor_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_base_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_consumer_runtime.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

template <bool SELECTED, bool DICTIONARY_EMISSION, bool USE_SALT, bool HAS_BLOOM, bool MARK_BUILD_MATCH,
          bool MATCHED_PROBE_ONLY, class MATCHER>
static void ExecuteAllValidHashJoinChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                              SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher) {
	using Row = typename MATCHER::Row;
	const auto key_sel = SELECTED ? input.source_sel[0] : nullptr;
	const auto entries = input.entries;
	const auto bitmask = input.bitmask;
	const auto count = input.count;
	const auto prefetch_offset = matcher.PrefetchOffset();
	const SljitHashJoinEntryDecoder entry_decoder(input);
	SljitHashJoinMatchedRowConsumer<MARK_BUILD_MATCH, MATCHED_PROBE_ONLY> consumer(input, plan);
	auto row_idx = input.input_offset;
	auto resume_row_pointer = input.resume_row_pointer;
	Row prefetched_key_row {};
	hash_t prefetched_hash = 0;
	idx_t prefetched_ht_offset = 0;
	bool has_prefetched_row = false;

	while (row_idx < count) {
		Row key_row {};
		hash_t hash = 0;
		idx_t ht_offset = 0;
		if (has_prefetched_row) {
			key_row = prefetched_key_row;
			hash = prefetched_hash;
			ht_offset = prefetched_ht_offset;
			has_prefetched_row = false;
		} else {
			const auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
			key_row = matcher.Load(source_idx);
			hash = matcher.BuildHash(key_row);
			ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);
		}
		const auto next_row_idx = row_idx + 1;
		if (next_row_idx < count) {
			const auto next_source_idx = SELECTED ? key_sel[next_row_idx] : UnsafeNumericCast<sel_t>(next_row_idx);
			prefetched_key_row = matcher.Load(next_source_idx);
			prefetched_hash = matcher.BuildHash(prefetched_key_row);
			prefetched_ht_offset = UnsafeNumericCast<idx_t>(prefetched_hash & bitmask);
			SljitPrefetchHashJoinEntry(entries, prefetched_ht_offset);
			has_prefetched_row = true;
		}
		data_ptr_t row_location = resume_row_pointer;
		bool row_matches_first = false;
		resume_row_pointer = nullptr;
		if (!row_location) {
			row_location = SljitHashJoinFindFirstChainPointer<USE_SALT, HAS_BLOOM>(
			    input, entry_decoder, entries, hash, ht_offset, prefetch_offset,
			    [&](data_ptr_t candidate) { return matcher.MatchesFirst(key_row, candidate); });
			row_matches_first = row_location != nullptr;
		}

		bool advanced_row = false;
		while (row_location) {
			const auto next_row_location = SljitHashJoinNextChainPointer<DICTIONARY_EMISSION>(input, row_location);
			if (next_row_location) {
				SljitPrefetchHashJoinRow(next_row_location, prefetch_offset);
			}
			const bool is_match = row_matches_first ? matcher.MatchesKnownFirst(key_row, row_location)
			                                        : matcher.Matches(key_row, row_location);
			row_matches_first = false;
			if (is_match) {
				auto result = consumer.EmitChainMatch(row_idx, row_location, next_row_location);
				if (result.output_full) {
					return;
				}
				if (result.row_finished) {
					advanced_row = true;
					break;
				}
			}
			row_location = next_row_location;
		}
		if (!advanced_row) {
			row_idx++;
		}
	}

	consumer.Finish();
}

template <bool SELECTED, bool DICTIONARY_EMISSION, bool MARK_BUILD_MATCH, bool MATCHED_PROBE_ONLY, class MATCHER>
static void ExecuteAllValidHashJoinChainProbeForInput(const SljitNativeHashJoinProbePlan &plan,
                                                      SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher) {
	if (SljitHashJoinProbeLayoutUsesSalt(input.layout_kind)) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinChainProbe<SELECTED, DICTIONARY_EMISSION, true, true, MARK_BUILD_MATCH,
			                                  MATCHED_PROBE_ONLY>(plan, input, matcher);
		} else {
			ExecuteAllValidHashJoinChainProbe<SELECTED, DICTIONARY_EMISSION, true, false, MARK_BUILD_MATCH,
			                                  MATCHED_PROBE_ONLY>(plan, input, matcher);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinChainProbe<SELECTED, DICTIONARY_EMISSION, false, true, MARK_BUILD_MATCH,
			                                  MATCHED_PROBE_ONLY>(plan, input, matcher);
		} else {
			ExecuteAllValidHashJoinChainProbe<SELECTED, DICTIONARY_EMISSION, false, false, MARK_BUILD_MATCH,
			                                  MATCHED_PROBE_ONLY>(plan, input, matcher);
		}
	}
}

template <bool SELECTED, bool MARK_BUILD_MATCH, bool MATCHED_PROBE_ONLY, class MATCHER>
static void ExecuteAllValidHashJoinChainProbeForInput(const SljitNativeHashJoinProbePlan &plan,
                                                      SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher) {
	if (SljitHashJoinProbeLayoutUsesDictionaryEmission(input.layout_kind)) {
		ExecuteAllValidHashJoinChainProbeForInput<SELECTED, true, MARK_BUILD_MATCH, MATCHED_PROBE_ONLY>(plan, input,
		                                                                                                matcher);
	} else {
		ExecuteAllValidHashJoinChainProbeForInput<SELECTED, false, MARK_BUILD_MATCH, MATCHED_PROBE_ONLY>(plan, input,
		                                                                                                 matcher);
	}
}

template <bool SELECTED, bool USE_SALT, bool HAS_BLOOM, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinNoChainProbe(SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher,
                                                CONSUMER &consumer) {
	using Row = typename MATCHER::Row;
	const uint64_t *__restrict entries = input.entries;
	const sel_t *__restrict key_sel = nullptr;
	if (SELECTED) {
		key_sel = input.source_sel[0];
	}
	const auto bitmask = input.bitmask;
	const auto count = input.count;
	const SljitHashJoinEntryDecoder entry_decoder(input);

	if (count > input.output_capacity) {
		throw InternalException("SLJIT no-chain hash join probe input exceeds output capacity");
	}

	auto row_idx = input.input_offset;
	if (row_idx < count) {
		auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		auto key_row = matcher.Load(source_idx);
		auto hash = matcher.BuildHash(key_row);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < count;
			Row next_key_row {};
			hash_t next_hash = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				const auto next_source_idx = SELECTED ? key_sel[next_row_idx] : UnsafeNumericCast<sel_t>(next_row_idx);
				next_key_row = matcher.Load(next_source_idx);
				next_hash = matcher.BuildHash(next_key_row);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			if (SljitBloomFilterMayContainTemplated<HAS_BLOOM>(input, hash)) {
				hash_t salt = 0;
				if constexpr (USE_SALT) {
					salt = entry_decoder.Salt(hash);
				}
				while (true) {
					const auto entry_value = entries[ht_offset];
					if (!entry_value) {
						break;
					}
					if constexpr (USE_SALT) {
						if (!entry_decoder.SaltMatches(entry_value, salt)) {
							ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
							continue;
						}
					}
					auto row_location = entry_decoder.Pointer(entry_value);
					if (matcher.Matches(key_row, row_location)) {
						consumer.EmitNoChainMatch(row_idx, row_location);
						break;
					}
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
				}
			}
			if (!has_next) {
				break;
			}
			row_idx = next_row_idx;
			key_row = next_key_row;
			hash = next_hash;
			ht_offset = next_ht_offset;
		}
	}

	consumer.Finish();
}

template <bool SELECTED, class MATCHER, class CONSUMER>
static void ExecuteAllValidHashJoinNoChainProbeForInput(SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher,
                                                        CONSUMER &consumer) {
	if (SljitHashJoinProbeLayoutUsesSalt(input.layout_kind)) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinNoChainProbe<SELECTED, true, true>(input, matcher, consumer);
		} else {
			ExecuteAllValidHashJoinNoChainProbe<SELECTED, true, false>(input, matcher, consumer);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidHashJoinNoChainProbe<SELECTED, false, true>(input, matcher, consumer);
		} else {
			ExecuteAllValidHashJoinNoChainProbe<SELECTED, false, false>(input, matcher, consumer);
		}
	}
}

template <bool SELECTED, bool CHAIN, bool MARK_BUILD_MATCH, bool MATCHED_PROBE_ONLY, class MATCHER>
static void ExecuteAllValidHashJoinProbeWithMatcherMode(const SljitNativeHashJoinProbePlan &plan,
                                                        SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher) {
	if constexpr (CHAIN) {
		ExecuteAllValidHashJoinChainProbeForInput<SELECTED, MARK_BUILD_MATCH, MATCHED_PROBE_ONLY>(plan, input, matcher);
	} else {
		SljitHashJoinMatchedRowConsumer<MARK_BUILD_MATCH, MATCHED_PROBE_ONLY> consumer(input, plan);
		ExecuteAllValidHashJoinNoChainProbeForInput<SELECTED>(input, matcher, consumer);
	}
}

template <bool SELECTED, bool CHAIN, class MATCHER>
static void ExecuteAllValidHashJoinProbeWithMatcher(const SljitNativeHashJoinProbePlan &plan,
                                                    SljitNativeRegularHashJoinProbeInput &input, MATCHER matcher) {
	if constexpr (CHAIN) {
		const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
		if (plan.mark_build_match) {
			if (matched_probe_only) {
				ExecuteAllValidHashJoinProbeWithMatcherMode<SELECTED, CHAIN, true, true>(plan, input, matcher);
			} else {
				ExecuteAllValidHashJoinProbeWithMatcherMode<SELECTED, CHAIN, true, false>(plan, input, matcher);
			}
		} else {
			if (matched_probe_only) {
				ExecuteAllValidHashJoinProbeWithMatcherMode<SELECTED, CHAIN, false, true>(plan, input, matcher);
			} else {
				ExecuteAllValidHashJoinProbeWithMatcherMode<SELECTED, CHAIN, false, false>(plan, input, matcher);
			}
		}
	} else if (plan.mark_build_match) {
		ExecuteAllValidHashJoinProbeWithMatcherMode<SELECTED, CHAIN, true, false>(plan, input, matcher);
	} else {
		ExecuteAllValidHashJoinProbeWithMatcherMode<SELECTED, CHAIN, false, false>(plan, input, matcher);
	}
}

} // namespace duckdb
