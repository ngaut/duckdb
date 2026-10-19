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
