//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_all_valid_probe_matcher_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_probe_base_runtime.hpp"
#include "sljit_hash_join_single_key_reader_runtime.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include <type_traits>

namespace duckdb {

template <class KEY_SOURCE, class KEY_LAYOUT, class DISPATCH>
static bool SljitDispatchHashJoinComparisonPredicateKind(const SljitNativeHashJoinProbePlan &plan, DISPATCH &dispatch) {
	auto &predicate = plan.keys[1];
	switch (predicate.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		dispatch.template Execute<KEY_SOURCE, KEY_LAYOUT, int8_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		dispatch.template Execute<KEY_SOURCE, KEY_LAYOUT, uint8_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		dispatch.template Execute<KEY_SOURCE, KEY_LAYOUT, int16_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		dispatch.template Execute<KEY_SOURCE, KEY_LAYOUT, uint16_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		dispatch.template Execute<KEY_SOURCE, KEY_LAYOUT, int32_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		dispatch.template Execute<KEY_SOURCE, KEY_LAYOUT, uint32_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		dispatch.template Execute<KEY_SOURCE, KEY_LAYOUT, int64_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		dispatch.template Execute<KEY_SOURCE, KEY_LAYOUT, uint64_t>();
		return true;
	default:
		return false;
	}
}

template <class DISPATCH>
static bool SljitDispatchHashJoinSingleComparisonPredicateKinds(const SljitNativeHashJoinProbePlan &plan,
                                                                const SljitNativeRegularHashJoinProbeInput &input,
                                                                DISPATCH &dispatch) {
	auto &key = plan.keys[0];
	if (input.source_key0_int64_to_int32) {
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT32) {
			return false;
		}
		return SljitDispatchHashJoinComparisonPredicateKind<int64_t, int32_t>(plan, dispatch);
	}
	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return SljitDispatchHashJoinComparisonPredicateKind<int8_t, int8_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::UINT8:
		return SljitDispatchHashJoinComparisonPredicateKind<uint8_t, uint8_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::INT16:
		return SljitDispatchHashJoinComparisonPredicateKind<int16_t, int16_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::UINT16:
		return SljitDispatchHashJoinComparisonPredicateKind<uint16_t, uint16_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::INT32:
		return SljitDispatchHashJoinComparisonPredicateKind<int32_t, int32_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::UINT32:
		return SljitDispatchHashJoinComparisonPredicateKind<uint32_t, uint32_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::INT64:
		return SljitDispatchHashJoinComparisonPredicateKind<int64_t, int64_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::UINT64:
		return SljitDispatchHashJoinComparisonPredicateKind<uint64_t, uint64_t>(plan, dispatch);
	default:
		return false;
	}
}

template <class KEY0, class DISPATCH>
static bool SljitDispatchHashJoinPairKey1Kind(const SljitNativeHashJoinProbePlan &plan, DISPATCH &dispatch) {
	auto &key1 = plan.keys[1];
	switch (key1.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		dispatch.template Execute<KEY0, int8_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		dispatch.template Execute<KEY0, uint8_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		dispatch.template Execute<KEY0, int16_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		dispatch.template Execute<KEY0, uint16_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		dispatch.template Execute<KEY0, int32_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		dispatch.template Execute<KEY0, uint32_t>();
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		dispatch.template Execute<KEY0, int64_t>();
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		dispatch.template Execute<KEY0, uint64_t>();
		return true;
	default:
		return false;
	}
}

template <class DISPATCH>
static bool SljitDispatchHashJoinPairKeyKinds(const SljitNativeHashJoinProbePlan &plan, DISPATCH &dispatch) {
	auto &key0 = plan.keys[0];
	switch (key0.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return SljitDispatchHashJoinPairKey1Kind<int8_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::UINT8:
		return SljitDispatchHashJoinPairKey1Kind<uint8_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::INT16:
		return SljitDispatchHashJoinPairKey1Kind<int16_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::UINT16:
		return SljitDispatchHashJoinPairKey1Kind<uint16_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::INT32:
		return SljitDispatchHashJoinPairKey1Kind<int32_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::UINT32:
		return SljitDispatchHashJoinPairKey1Kind<uint32_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::INT64:
		return SljitDispatchHashJoinPairKey1Kind<int64_t>(plan, dispatch);
	case SljitNativeHashJoinKeyKind::UINT64:
		return SljitDispatchHashJoinPairKey1Kind<uint64_t>(plan, dispatch);
	default:
		return false;
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

template <class KEY0, class KEY1>
struct SljitHashJoinPairEqualityMatcher {
	struct Row {
		KEY0 key0;
		KEY1 key1;
	};

	SljitHashJoinPairEqualityMatcher(const SljitNativeHashJoinProbePlan &plan,
	                                 const SljitNativeRegularHashJoinProbeInput &input)
	    : key0_data(reinterpret_cast<const KEY0 *>(input.source_data[0])),
	      key1_data(reinterpret_cast<const KEY1 *>(input.source_data[1])), key0_offset(plan.keys[0].key_layout_offset),
	      key1_offset(plan.keys[1].key_layout_offset) {
	}

	inline Row Load(const sel_t source_idx) const {
		return {key0_data[source_idx], key1_data[source_idx]};
	}

	inline hash_t BuildHash(const Row &row) const {
		return SljitHashJoinCombineHashScalar(Hash<KEY0>(row.key0), Hash<KEY1>(row.key1));
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
		return SljitHashJoinKeysEqual<KEY0>(row_location, key0_offset, row.key0) &&
		       SljitHashJoinKeysEqual<KEY1>(row_location, key1_offset, row.key1);
	}

	const KEY0 *__restrict key0_data;
	const KEY1 *__restrict key1_data;
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

template <class T>
static inline bool SljitHashJoinComparePredicate(T left, T right, ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::NOT_EQUAL:
		return left != right;
	case ExecutionRegionComparisonType::LESS_THAN:
		return left < right;
	case ExecutionRegionComparisonType::GREATER_THAN:
		return left > right;
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return left <= right;
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return left >= right;
	default:
		return false;
	}
}

template <class KEY_SOURCE, class KEY_LAYOUT, class PREDICATE>
struct SljitHashJoinSingleKeyComparisonPredicateMatcher {
	struct Row {
		KEY_LAYOUT key_value;
		PREDICATE predicate_value;
	};

	SljitHashJoinSingleKeyComparisonPredicateMatcher(const SljitNativeHashJoinProbePlan &plan,
	                                                 const SljitNativeRegularHashJoinProbeInput &input)
	    : key_data(reinterpret_cast<const KEY_SOURCE *>(input.source_data[0])),
	      predicate_data(reinterpret_cast<const PREDICATE *>(input.source_data[1])),
	      key_offset(plan.keys[0].key_layout_offset), predicate_offset(plan.keys[1].key_layout_offset),
	      comparison_type(plan.keys[1].comparison_type) {
	}

	inline Row Load(const sel_t source_idx) const {
		return {CastKey(key_data[source_idx]), predicate_data[source_idx]};
	}

	inline hash_t BuildHash(const Row &row) const {
		return Hash<KEY_LAYOUT>(row.key_value);
	}

	inline idx_t PrefetchOffset() const {
		return key_offset;
	}

	inline bool MatchesFirst(const Row &row, data_ptr_t row_location) const {
		return SljitHashJoinKeysEqual<KEY_LAYOUT>(row_location, key_offset, row.key_value);
	}

	inline bool MatchesKnownFirst(const Row &row, data_ptr_t row_location) const {
		return SljitHashJoinComparePredicate<PREDICATE>(
		    row.predicate_value, duckdb::Load<PREDICATE>(row_location + predicate_offset), comparison_type);
	}

	inline bool Matches(const Row &row, data_ptr_t row_location) const {
		return MatchesFirst(row, row_location) && MatchesKnownFirst(row, row_location);
	}

private:
	static inline KEY_LAYOUT CastKey(KEY_SOURCE value) {
		if constexpr (std::is_same<KEY_SOURCE, KEY_LAYOUT>::value) {
			return value;
		}
		return NumericCast<KEY_LAYOUT>(value);
	}

	const KEY_SOURCE *__restrict key_data;
	const PREDICATE *__restrict predicate_data;
	idx_t key_offset;
	idx_t predicate_offset;
	ExecutionRegionComparisonType comparison_type;
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
