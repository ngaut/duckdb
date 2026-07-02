//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/distinct_count_pointer_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

bool DistinctCountPointerPayloadTypeSupported(PhysicalType type);

struct DistinctCountPointerGroup {
	static constexpr idx_t INLINE_PAYLOAD_CAPACITY = 64;
	static constexpr idx_t INLINE_TABLE_CAPACITY = INLINE_PAYLOAD_CAPACITY * 2;

	DistinctCountPointerGroup();

	uintptr_t state_pointer = 0;
	idx_t payload_count = 0;
	bool uses_overflow = false;
	uint64_t payloads[INLINE_TABLE_CAPACITY];
	uint8_t payload_tags[INLINE_TABLE_CAPACITY];
};

class DistinctCountPairOverflowSet {
public:
	bool Add(uintptr_t state_pointer, uint64_t payload);

private:
	void Resize(idx_t capacity);
	void InsertKnownNew(uintptr_t state_pointer, uint64_t payload, hash_t hash);

private:
	vector<uintptr_t> state_entries;
	vector<uint64_t> payload_entries;
	vector<hash_t> hash_entries;
	idx_t entry_count = 0;
	hash_t bitmask = 0;
};

class DistinctCountPointerSet {
public:
	bool Add(Vector &state_pointers, Vector &payload, idx_t count, idx_t state_value_offset);

private:
	template <class T>
	bool AddTemplated(Vector &state_pointers, Vector &payload, idx_t count, idx_t state_value_offset);
	DistinctCountPointerGroup &FindOrCreateGroup(uintptr_t state_pointer);
	bool AddPayload(DistinctCountPointerGroup &group, uint64_t payload);
	bool PromoteToOverflow(DistinctCountPointerGroup &group);
	void ResizeGroups(idx_t capacity);
	void InsertKnownGroup(uintptr_t state_pointer, idx_t group_index, hash_t hash);

private:
	vector<uintptr_t> group_state_entries;
	vector<idx_t> group_index_entries;
	vector<hash_t> group_hash_entries;
	vector<DistinctCountPointerGroup> groups;
	DistinctCountPairOverflowSet overflow;
	idx_t group_count = 0;
	hash_t group_bitmask = 0;
};

} // namespace duckdb
