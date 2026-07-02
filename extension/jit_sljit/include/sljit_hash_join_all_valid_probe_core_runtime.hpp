//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_core_runtime.hpp
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

template <class T>
struct SljitHashJoinDirectKeyReader {
	using Key = T;

	explicit SljitHashJoinDirectKeyReader(const SljitNativeRegularHashJoinProbeInput &input)
	    : data(reinterpret_cast<const T *__restrict>(input.source_data[0])) {
	}

	inline T Load(const sel_t source_idx) const {
		return data[source_idx];
	}

	const T *__restrict data;
};

template <bool UNCHECKED>
struct SljitHashJoinInt64ToInt32KeyReader {
	using Key = int32_t;

	explicit SljitHashJoinInt64ToInt32KeyReader(const SljitNativeRegularHashJoinProbeInput &input)
	    : data(reinterpret_cast<const int64_t *__restrict>(input.source_data[0])) {
	}

	inline int32_t Load(const sel_t source_idx) const {
		return SljitCastHashJoinKeyInt64ToInt32<UNCHECKED>(data[source_idx]);
	}

	const int64_t *__restrict data;
};

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

struct SljitHashJoinChainMatchResult {
	bool output_full;
	bool row_finished;
};

template <bool MARK_BUILD_MATCH>
struct SljitHashJoinBuildMatchMarker {
	explicit SljitHashJoinBuildMatchMarker(const SljitNativeHashJoinProbePlan &plan)
	    : found_match_offset(plan.found_match_offset) {
	}

	inline void Mark(data_ptr_t row_location) const {
		row_location[found_match_offset] = 1;
	}

	idx_t found_match_offset;
};

template <>
struct SljitHashJoinBuildMatchMarker<false> {
	explicit SljitHashJoinBuildMatchMarker(const SljitNativeHashJoinProbePlan &) {
	}

	inline void Mark(data_ptr_t) const {
	}
};

template <bool MARK_BUILD_MATCH, bool MATCHED_PROBE_ONLY>
struct SljitHashJoinMatchedRowConsumer {
	SljitHashJoinMatchedRowConsumer(SljitNativeRegularHashJoinProbeInput &input,
	                                const SljitNativeHashJoinProbePlan &plan)
	    : input(input), row_pointers(input.row_pointers), match_sel(input.match_sel),
	      selected_count(input.selected_count), output_capacity(input.output_capacity), build_match_marker(plan) {
	}

	inline void EmitNoChainMatch(const idx_t row_idx, const data_ptr_t row_location) {
		Emit(row_idx, row_location);
	}

	inline SljitHashJoinChainMatchResult EmitChainMatch(idx_t &row_idx, const data_ptr_t row_location,
	                                                    const data_ptr_t next_row_location) {
		Emit(row_idx, row_location);
		if constexpr (MATCHED_PROBE_ONLY) {
			row_idx++;
			if (selected_count >= output_capacity) {
				Finish(row_idx, nullptr);
				return {true, true};
			}
			return {false, true};
		}
		if (selected_count < output_capacity) {
			return {false, false};
		}
		Finish(next_row_location ? row_idx : row_idx + 1, next_row_location);
		return {true, false};
	}

	void Finish() {
		Finish(input.count, nullptr);
	}

private:
	inline void Emit(const idx_t row_idx, const data_ptr_t row_location) {
		build_match_marker.Mark(row_location);
		row_pointers[selected_count] = row_location;
		match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
		selected_count++;
	}

	void Finish(const idx_t next_row_idx, const data_ptr_t next_row_location) {
		input.selected_count = selected_count;
		input.input_offset = next_row_idx;
		input.resume_row_pointer = next_row_location;
		input.finished = !next_row_location && input.input_offset >= input.count;
	}

	SljitNativeRegularHashJoinProbeInput &input;
	data_ptr_t *__restrict row_pointers;
	sel_t *__restrict match_sel;
	idx_t selected_count;
	idx_t output_capacity;
	SljitHashJoinBuildMatchMarker<MARK_BUILD_MATCH> build_match_marker;
};

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

struct SljitHashJoinUint64PairMatcher {
	struct Row {
		uint64_t key0;
		uint64_t key1;
	};

	SljitHashJoinUint64PairMatcher(const SljitNativeHashJoinProbePlan &plan,
	                               const SljitNativeRegularHashJoinProbeInput &input)
	    : key0_data(reinterpret_cast<const uint64_t *>(input.source_data[0])),
	      key1_data(reinterpret_cast<const uint64_t *>(input.source_data[1])),
	      key0_offset(plan.keys[0].key_layout_offset), key1_offset(plan.keys[1].key_layout_offset) {
	}

	inline Row Load(const sel_t source_idx) const {
		return {key0_data[source_idx], key1_data[source_idx]};
	}

	inline hash_t BuildHash(const Row &row) const {
		return SljitHashJoinCombineHashScalar(Hash<uint64_t>(row.key0), Hash<uint64_t>(row.key1));
	}

	inline idx_t PrefetchOffset() const {
		return key0_offset;
	}

	inline bool MatchesFirst(const Row &row, data_ptr_t row_location) const {
		return Matches(row, row_location);
	}

	inline bool MatchesKnownFirst(const Row &, data_ptr_t) const {
		return true;
	}

	inline bool Matches(const Row &row, data_ptr_t row_location) const {
		return SljitHashJoinKeysEqual<uint64_t>(row_location, key0_offset, row.key0) &&
		       SljitHashJoinKeysEqual<uint64_t>(row_location, key1_offset, row.key1);
	}

	const uint64_t *__restrict key0_data;
	const uint64_t *__restrict key1_data;
	idx_t key0_offset;
	idx_t key1_offset;
};

struct SljitHashJoinSingleUint64NotEqualPredicateMatcher {
	struct Row {
		uint64_t key_value;
		uint64_t predicate_value;
	};

	SljitHashJoinSingleUint64NotEqualPredicateMatcher(const SljitNativeHashJoinProbePlan &plan,
	                                                  const SljitNativeRegularHashJoinProbeInput &input)
	    : key_data(reinterpret_cast<const uint64_t *>(input.source_data[0])),
	      predicate_data(reinterpret_cast<const uint64_t *>(input.source_data[1])),
	      key_offset(plan.keys[0].key_layout_offset), predicate_offset(plan.keys[1].key_layout_offset) {
	}

	inline Row Load(const sel_t source_idx) const {
		return {key_data[source_idx], predicate_data[source_idx]};
	}

	inline hash_t BuildHash(const Row &row) const {
		return Hash<uint64_t>(row.key_value);
	}

	inline idx_t PrefetchOffset() const {
		return key_offset;
	}

	inline bool MatchesFirst(const Row &row, data_ptr_t row_location) const {
		return SljitHashJoinKeysEqual<uint64_t>(row_location, key_offset, row.key_value);
	}

	inline bool MatchesKnownFirst(const Row &row, data_ptr_t row_location) const {
		return !SljitHashJoinKeysEqual<uint64_t>(row_location, predicate_offset, row.predicate_value);
	}

	inline bool Matches(const Row &row, data_ptr_t row_location) const {
		return MatchesFirst(row, row_location) &&
		       !SljitHashJoinKeysEqual<uint64_t>(row_location, predicate_offset, row.predicate_value);
	}

	const uint64_t *__restrict key_data;
	const uint64_t *__restrict predicate_data;
	idx_t key_offset;
	idx_t predicate_offset;
};

template <class KEY_READER>
struct SljitHashJoinSingleKeyMatcher {
	using T = typename KEY_READER::Key;
	struct Row {
		T key;
	};

	SljitHashJoinSingleKeyMatcher(const SljitNativeHashJoinProbePlan &plan,
	                              const SljitNativeRegularHashJoinProbeInput &input)
	    : key_reader(input), key_offset(plan.keys[0].key_layout_offset) {
	}

	inline Row Load(const sel_t source_idx) const {
		return {key_reader.Load(source_idx)};
	}

	inline hash_t BuildHash(const Row &row) const {
		return Hash<T>(row.key);
	}

	inline idx_t PrefetchOffset() const {
		return key_offset;
	}

	inline bool MatchesFirst(const Row &row, data_ptr_t row_location) const {
		return Matches(row, row_location);
	}

	inline bool MatchesKnownFirst(const Row &, data_ptr_t) const {
		return true;
	}

	inline bool Matches(const Row &row, data_ptr_t row_location) const {
		return SljitHashJoinKeysEqual<T>(row_location, key_offset, row.key);
	}

	KEY_READER key_reader;
	idx_t key_offset;
};

} // namespace duckdb
