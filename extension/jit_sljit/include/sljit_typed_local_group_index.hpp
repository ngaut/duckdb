//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_typed_local_group_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/hash.hpp"

#include <array>

namespace duckdb {

template <class T>
struct SljitLocalGroupKeyOperations {
	static hash_t HashKey(const T &key) {
		return Hash(key);
	}

	static bool Equals(const T &left, const T &right) {
		return left == right;
	}
};

template <>
struct SljitLocalGroupKeyOperations<hugeint_t> {
	static hash_t HashKey(const hugeint_t &key) {
		return MurmurHash64(key.lower) ^ MurmurHash64(static_cast<uint64_t>(key.upper));
	}

	static bool Equals(const hugeint_t &left, const hugeint_t &right) {
		return left.lower == right.lower && left.upper == right.upper;
	}
};

template <>
struct SljitLocalGroupKeyOperations<uhugeint_t> {
	static hash_t HashKey(const uhugeint_t &key) {
		return MurmurHash64(key.lower) ^ MurmurHash64(key.upper);
	}

	static bool Equals(const uhugeint_t &left, const uhugeint_t &right) {
		return left.lower == right.lower && left.upper == right.upper;
	}
};

template <class T, idx_t GROUP_LIMIT, idx_t HOT_KEY_CAPACITY = 8>
class SljitTypedLocalGroupIndex {
private:
	struct Entry {
		bool occupied = false;
		bool valid = false;
		T key {};
		idx_t group_idx = DConstants::INVALID_INDEX;
	};

	static constexpr idx_t CACHE_CAPACITY = GROUP_LIMIT * 2;
	static_assert(GROUP_LIMIT > 0 && (GROUP_LIMIT & (GROUP_LIMIT - 1)) == 0,
	              "SLJIT local group limit must be a power of two");
	static_assert(HOT_KEY_CAPACITY <= GROUP_LIMIT, "SLJIT local hot-key capacity must fit in the group index");

public:
	bool FindOrCreate(bool valid, const T &key, idx_t &group_idx, bool &created) {
		created = false;
		if (hot_tier_active) {
			for (idx_t hot_idx = 0; hot_idx < group_count; hot_idx++) {
				if (group_validity[hot_idx] == valid &&
				    (!valid || SljitLocalGroupKeyOperations<T>::Equals(group_keys[hot_idx], key))) {
					group_idx = hot_idx;
					return true;
				}
			}
		}

		const auto hash = valid ? SljitLocalGroupKeyOperations<T>::HashKey(key) : hash_t(0);
		auto entry_idx = static_cast<idx_t>(hash) & (CACHE_CAPACITY - 1);
		for (idx_t probe_idx = 0; probe_idx < CACHE_CAPACITY; probe_idx++) {
			auto &entry = entries[entry_idx];
			if (!entry.occupied) {
				if (group_count == GROUP_LIMIT) {
					return false;
				}
				entry.occupied = true;
				entry.valid = valid;
				entry.key = key;
				entry.group_idx = group_count;
				group_idx = group_count++;
				if (group_count > HOT_KEY_CAPACITY) {
					hot_tier_active = false;
				}
				group_validity[group_idx] = valid;
				group_keys[group_idx] = key;
				created = true;
				return true;
			}
			if (entry.valid == valid && (!valid || SljitLocalGroupKeyOperations<T>::Equals(entry.key, key))) {
				group_idx = entry.group_idx;
				return true;
			}
			entry_idx = (entry_idx + 1) & (CACHE_CAPACITY - 1);
		}
		return false;
	}

	idx_t Count() const {
		return group_count;
	}

	bool IsValid(idx_t group_idx) const {
		D_ASSERT(group_idx < group_count);
		return group_validity[group_idx];
	}

	const T &Key(idx_t group_idx) const {
		D_ASSERT(group_idx < group_count);
		return group_keys[group_idx];
	}

	void Reset() {
		group_count = 0;
		hot_tier_active = HOT_KEY_CAPACITY > 0;
		entries = {};
	}

private:
	bool hot_tier_active = HOT_KEY_CAPACITY > 0;
	idx_t group_count = 0;
	std::array<Entry, CACHE_CAPACITY> entries {};
	std::array<T, GROUP_LIMIT> group_keys {};
	std::array<bool, GROUP_LIMIT> group_validity {};
};

} // namespace duckdb
