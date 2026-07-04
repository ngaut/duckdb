//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_base_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_runtime.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

static inline hash_t SljitHashJoinCombineHashScalar(hash_t left, hash_t right) {
	left ^= left >> 32;
	left *= 0xd6e8feb86659fd93U;
	return left ^ right;
}

template <class T>
static inline bool SljitHashJoinKeysEqual(const data_ptr_t row_location, idx_t layout_offset, T probe_key) {
	return Load<T>(row_location + layout_offset) == probe_key;
}

static inline void SljitPrefetchHashJoinEntry(const ht_entry_t *entries, idx_t ht_offset) {
#if defined(__GNUC__) || defined(__clang__)
	__builtin_prefetch(entries + ht_offset, 0, 3);
#else
	(void)entries;
	(void)ht_offset;
#endif
}

static inline void SljitPrefetchHashJoinRow(data_ptr_t row_location, idx_t layout_offset) {
#if defined(__GNUC__) || defined(__clang__)
	__builtin_prefetch(row_location + layout_offset, 0, 3);
#else
	(void)row_location;
	(void)layout_offset;
#endif
}

static inline data_ptr_t SljitHashJoinEntryPointer(hash_t entry_value) {
	return cast_uint64_to_pointer(entry_value & ht_entry_t::POINTER_MASK);
}

template <bool UNCHECKED>
static inline int32_t SljitCastHashJoinKeyInt64ToInt32(int64_t value) {
	if constexpr (UNCHECKED) {
		return UnsafeNumericCast<int32_t>(value);
	} else {
		return NumericCast<int32_t>(value);
	}
}

static inline uint64_t SljitBloomFilterMask(hash_t hash) {
	return BloomFilter::GetMask(hash);
}

static inline bool SljitBloomFilterMayContainKnownPresent(const uint64_t *bits, uint64_t bitmask, hash_t hash) {
	const auto slot = bits[hash & bitmask];
	const auto mask = SljitBloomFilterMask(hash);
	return (slot & mask) == mask;
}

template <bool HAS_BLOOM>
static inline bool SljitBloomFilterMayContainTemplated(const SljitNativeRegularHashJoinProbeInput &input, hash_t hash) {
	if constexpr (!HAS_BLOOM) {
		return true;
	} else {
		return SljitBloomFilterMayContainKnownPresent(input.bloom_filter_bits, input.bloom_filter_bitmask, hash);
	}
}

static bool SljitHashJoinCanUseAllValidMatchedProbe(const SljitNativeHashJoinProbePlan &plan,
                                                    const SljitNativeRegularHashJoinProbeInput &input, bool selected) {
	if (plan.residual_predicate ||
	    (plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	     plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) ||
	    input.source_validity || input.output_capacity == 0) {
		return false;
	}
	return selected ? input.source_sel && input.source_sel[0] : !input.source_sel;
}

static bool SljitHashJoinHasPlainEqualityKey(const SljitNativeHashJoinProbeKeyPlan &key) {
	return key.equality_key && !key.null_equal && key.comparison_type == ExecutionRegionComparisonType::EQUAL;
}

static bool SljitHashJoinHasPlainNotEqualPredicateKey(const SljitNativeHashJoinProbeKeyPlan &key) {
	return !key.equality_key && !key.null_equal && key.comparison_type == ExecutionRegionComparisonType::NOT_EQUAL;
}

static bool SljitHashJoinHasUint64CompatibleKey(const SljitNativeHashJoinProbeKeyPlan &key) {
	return key.key_kind == SljitNativeHashJoinKeyKind::INT64 || key.key_kind == SljitNativeHashJoinKeyKind::UINT64;
}

static bool SljitHashJoinCanUseSingleKeyProbe(const SljitNativeHashJoinProbePlan &plan) {
	if (plan.keys.size() != 1 || plan.equality_key_count != 1) {
		return false;
	}
	return SljitHashJoinHasPlainEqualityKey(plan.keys[0]);
}

static bool SljitHashJoinCanUseAllValidNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                    const SljitNativeRegularHashJoinProbeInput &input, bool selected) {
	return SljitHashJoinCanUseAllValidMatchedProbe(plan, input, selected) &&
	       !SljitHashJoinProbeLayoutChainsLongerThanOne(input.layout_kind) && !input.resume_row_pointer;
}

static bool SljitHashJoinCanUseAllValidEqualityChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                          const SljitNativeRegularHashJoinProbeInput &input,
                                                          bool selected, bool can_use_equality_chain_input) {
	return SljitHashJoinCanUseAllValidMatchedProbe(plan, input, selected) && can_use_equality_chain_input;
}

template <bool DICTIONARY_EMISSION>
static inline data_ptr_t SljitHashJoinNextChainPointer(const SljitNativeRegularHashJoinProbeInput &input,
                                                       data_ptr_t row_location) {
	if constexpr (DICTIONARY_EMISSION) {
		const auto dict_index = Load<uint32_t>(row_location + input.pointer_offset);
		return input.aux_next_ptrs[dict_index];
	}
	return cast_uint64_to_pointer(Load<uint64_t>(row_location + input.pointer_offset));
}

template <bool USE_SALT, bool HAS_BLOOM, class MATCH>
static inline data_ptr_t SljitHashJoinFindFirstChainPointer(const SljitNativeRegularHashJoinProbeInput &input,
                                                            const ht_entry_t *entries, hash_t hash, idx_t ht_offset,
                                                            idx_t prefetch_offset, MATCH match) {
	if (!SljitBloomFilterMayContainTemplated<HAS_BLOOM>(input, hash)) {
		return nullptr;
	}
	hash_t salt = 0;
	if constexpr (USE_SALT) {
		salt = hash & ht_entry_t::SALT_MASK;
	}
	while (true) {
		const auto &entry = entries[ht_offset];
		if (!entry.IsOccupied()) {
			return nullptr;
		}
		if constexpr (USE_SALT) {
			if (entry.GetSaltWithNulls() != salt) {
				ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
				continue;
			}
		}
		auto row_location = entry.GetPointer();
		SljitPrefetchHashJoinRow(row_location, prefetch_offset);
		if (match(row_location)) {
			return row_location;
		}
		ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
	}
}

} // namespace duckdb
