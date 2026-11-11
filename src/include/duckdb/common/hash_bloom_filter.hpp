//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/hash_bloom_filter.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/hash.hpp"

namespace duckdb {

struct HashBloomFilterConstants {
	//! The sector comes from the high hash half, which also drives radix partitioning, so the keys of one hash
	//! partition share sectors; the bits within the sector come from the low half
	static constexpr uint8_t SECTOR_SHIFT = 32;
	static constexpr uint8_t SHIFT_0 = 0;
	static constexpr uint8_t SHIFT_1 = 8;
	static constexpr uint8_t SHIFT_2 = 16;
	static constexpr uint8_t SHIFT_3 = 24;
};

//! Returns the sector of DuckDB's hash Bloom filters a hash falls in, given the filter's sector bitmask.
static inline uint64_t HashBloomFilterSector(hash_t hash, uint64_t bitmask) {
	return (hash >> HashBloomFilterConstants::SECTOR_SHIFT) & bitmask;
}

//! Returns the four-bit mask used by DuckDB's hash Bloom filters.
//!
//! This common primitive is shared by filter owners and execution backends so
//! a raw Bloom view never depends on a planner-private BloomFilter class.
static inline uint64_t HashBloomFilterMask(hash_t hash) {
	return (1ULL << ((hash >> HashBloomFilterConstants::SHIFT_0) & 0x3F)) |
	       (1ULL << ((hash >> HashBloomFilterConstants::SHIFT_1) & 0x3F)) |
	       (1ULL << ((hash >> HashBloomFilterConstants::SHIFT_2) & 0x3F)) |
	       (1ULL << ((hash >> HashBloomFilterConstants::SHIFT_3) & 0x3F));
}

} // namespace duckdb
