//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/prefix_range_lookup.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/force_inline.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! The bitmap of an integral prefix-range filter, NULL-free: the membership test of a key. Keys are widened by
//! WidenIntegralBits, so a key's bucket offset is exact for every key width.
struct PrefixRangeLookupData {
	//! The smallest build key, widened
	uint64_t min = 0;
	//! The largest bucket offset of a build key
	uint64_t span = 0;
	//! Bucket offset bits per bitmap bit; 0 makes the bitmap exact (one bit per key)
	idx_t shift = 0;
	//! One bit per bucket; bits past the last bucket are zero, and one zero word follows the last bucket's word
	const uint64_t *bitmap = nullptr;

	//! Whether the key's bucket is set; EXACT asserts shift == 0
	template <bool EXACT, class T>
	DUCKDB_FORCE_INLINE bool Contains(T key) const {
		const uint64_t offset = WidenIntegralBits(key) - min;
		if (EXACT) {
			return offset <= span && ((bitmap[offset >> 6U] >> (offset & 63U)) & 1U);
		}
		const uint64_t bucket = offset >> shift;
		const uint64_t in_range = offset <= span;
		// out-of-range keys read word 0 and are masked out, so the test has no branch
		const uint64_t word = (bucket >> 6U) & (0ULL - in_range);
		return (bitmap[word] >> (bucket & 63U)) & in_range;
	}

	//! Answers a block of COUNT keys of an exact bitmap: bit 0 of match[k] is whether keys[k] is a member. Sorted keys
	//! cluster: when every key lies less than 64 keys above the first one, which is in range, one 64-bit window of the
	//! bitmap answers the whole block without range checks (bits past the last bucket and the word after it are zero).
	template <idx_t COUNT, class T>
	DUCKDB_FORCE_INLINE void ContainsExactBlock(const T *keys, uint64_t (&match)[COUNT]) const {
		const auto first = WidenIntegralBits(keys[0]);
		const uint64_t first_offset = first - min;
		uint64_t distances[COUNT];
		uint64_t spread = 0;
		for (idx_t k = 0; k < COUNT; k++) {
			distances[k] = WidenIntegralBits(keys[k]) - first;
			spread |= distances[k];
		}
		if (first_offset <= span && spread < 64) {
			const auto word = bitmap + (first_offset >> 6U);
			const auto bit = first_offset & 63U;
			const uint64_t window = (word[0] >> bit) | ((word[1] << 1U) << (63U - bit));
			for (idx_t k = 0; k < COUNT; k++) {
				match[k] = window >> distances[k];
			}
			return;
		}
		for (idx_t k = 0; k < COUNT; k++) {
			match[k] = Contains<true>(keys[k]);
		}
	}
};

} // namespace duckdb
