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
#if DUCKDB_IS_BIG_ENDIAN
	static constexpr uint8_t SHIFT_0 = 24;
	static constexpr uint8_t SHIFT_1 = 16;
	static constexpr uint8_t SHIFT_2 = 8;
	static constexpr uint8_t SHIFT_3 = 0;
#else
	static constexpr uint8_t SHIFT_0 = 32;
	static constexpr uint8_t SHIFT_1 = 40;
	static constexpr uint8_t SHIFT_2 = 48;
	static constexpr uint8_t SHIFT_3 = 56;
#endif
};

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
