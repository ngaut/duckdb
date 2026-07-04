//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/distinct_count_pointer_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/execution_aggregate_runtime.hpp"

#include <array>

namespace duckdb {

bool DistinctCountPointerPayloadTypeSupported(PhysicalType type);

struct DistinctCountPointerGroup {
	static constexpr idx_t INLINE_PAYLOAD_CAPACITY = EXECUTION_DISTINCT_COUNT_POINTER_INLINE_PAYLOAD_CAPACITY;

	uintptr_t state_pointer = 0;
	idx_t group_index = DConstants::INVALID_INDEX;
	idx_t payload_count = 0;
	uint64_t inline_payload_occupied_mask = 0;
	bool uses_overflow = false;
	idx_t overflow_head = DConstants::INVALID_INDEX;
	uint64_t inline_payloads[INLINE_PAYLOAD_CAPACITY];
};

class DistinctCountPairOverflowSet {
public:
	bool Add(DistinctCountPointerGroup &group, uint64_t payload);
	void InsertKnownUnique(DistinctCountPointerGroup &group, uint64_t payload);
	void ReserveEntries(idx_t target_count);
	void Reset();
	idx_t EntryCount() const {
		return entry_count;
	}

	template <class FUNC>
	void VisitPayloads(const DistinctCountPointerGroup &group, FUNC &&func) const {
		auto entry_idx = group.overflow_head;
		while (entry_idx != DConstants::INVALID_INDEX) {
			D_ASSERT(entry_idx < payload_entries.size());
			func(payload_entries[entry_idx]);
			entry_idx = next_entries[entry_idx];
		}
	}

private:
	void ResizeTable(idx_t capacity);
	void InsertKnownNew(idx_t payload_entry_idx, hash_t hash);

private:
	vector<idx_t> group_entries;
	vector<uint64_t> payload_entries;
	vector<idx_t> next_entries;
	vector<idx_t> payload_index_entries;
	vector<hash_t> hash_entries;
	idx_t entry_count = 0;
	hash_t bitmask = 0;
};

class DistinctCountPointerSet {
public:
	DistinctCountPointerSet();

	bool Add(Vector &state_pointers, Vector &payload, idx_t count, idx_t state_value_offset);
	bool AddSelected(const uintptr_t *state_pointers, const sel_t *state_sel, const sel_t *payload_sel, Vector &payload,
	                 idx_t count, idx_t state_value_offset);
	bool UseGlobalPayloadSet(idx_t expected_payload_count);
	bool MergeStatePayloadsTo(DistinctCountPointerSet &target, uintptr_t source_state_pointer,
	                          uintptr_t target_state_pointer);
	void RemapStatePointers(const vector<pair<uintptr_t, uintptr_t>> &state_pointer_mappings);
	void Reset();

	idx_t GroupCount() const {
		return group_count;
	}

	idx_t StateValueOffset() const {
		return state_value_offset;
	}

	template <class FUNC>
	void VisitStatePointers(FUNC &&func) const {
		for (const auto &group : groups) {
			func(group.state_pointer);
		}
	}

private:
	template <class T>
	bool AddTemplated(Vector &state_pointers, Vector &payload, idx_t count, idx_t state_value_offset);
	template <class T>
	bool AddSelectedTemplated(const uintptr_t *state_pointers, const sel_t *state_sel, const sel_t *payload_sel,
	                          Vector &payload, idx_t count, idx_t state_value_offset);
	void ReserveGroupsForRows(idx_t count);
	void ReserveGroupEntries(idx_t target_count);
	void ReserveOverflowEntries(idx_t additional_count);
	void ReserveGlobalPayloadEntries(idx_t target_count);
	DistinctCountPointerGroup &FindOrCreateGroup(uintptr_t state_pointer);
	DistinctCountPointerGroup &FindOrCreateGroupCached(uintptr_t state_pointer);
	DistinctCountPointerGroup &InsertGroup(uintptr_t state_pointer, hash_t hash, idx_t ht_offset);
	DistinctCountPointerGroup &LookupGroup(idx_t group_index);
	const DistinctCountPointerGroup *FindGroup(uintptr_t state_pointer) const;
	bool AddPayload(DistinctCountPointerGroup &group, uint64_t payload);
	void CopyUniquePayloadsToEmptyGroup(const DistinctCountPointerGroup &source_group,
	                                    const DistinctCountPairOverflowSet &source_overflow,
	                                    DistinctCountPointerGroup &target_group);
	bool PromoteToOverflow(DistinctCountPointerGroup &group);
	bool EnsureStateValueOffset(idx_t state_value_offset);
	void ResizeGroups(idx_t capacity);
	void InsertKnownGroup(uintptr_t state_pointer, idx_t group_index, hash_t hash);

private:
	vector<uintptr_t> group_state_entries;
	vector<idx_t> group_index_entries;
	vector<hash_t> group_hash_entries;
	vector<DistinctCountPointerGroup> groups;
	DistinctCountPairOverflowSet overflow;
	static constexpr idx_t GROUP_LOOKUP_CACHE_SIZE = 65536;
	static_assert((GROUP_LOOKUP_CACHE_SIZE & (GROUP_LOOKUP_CACHE_SIZE - 1)) == 0,
	              "distinct count-pointer group lookup cache size must be a power of two");
	std::array<uintptr_t, GROUP_LOOKUP_CACHE_SIZE> group_lookup_cache_states;
	std::array<idx_t, GROUP_LOOKUP_CACHE_SIZE> group_lookup_cache_indices;
	idx_t group_count = 0;
	hash_t group_bitmask = 0;
	idx_t state_value_offset = DConstants::INVALID_INDEX;
	bool use_global_payload_set = false;
	idx_t global_payload_set_reserve_target = 0;
};

} // namespace duckdb
