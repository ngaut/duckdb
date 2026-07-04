//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/execution/operator/aggregate/distinct_count_pointer_set.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/execution/operator/aggregate/distinct_count_pointer_set.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

namespace duckdb {

DistinctCountPointerSet::DistinctCountPointerSet() {
	group_lookup_cache_states.fill(0);
	group_lookup_cache_indices.fill(DConstants::INVALID_INDEX);
}

bool DistinctCountPointerPayloadTypeSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
	case PhysicalType::UINT16:
	case PhysicalType::INT16:
	case PhysicalType::UINT32:
	case PhysicalType::INT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT64:
		return true;
	default:
		return false;
	}
}

static hash_t DistinctCountIntegerHash(uint64_t value) {
	auto key = value * 0x9e3779b97f4a7c15ULL;
	key ^= key >> 33;
	return key ? key : 1;
}

static hash_t DistinctCountPointerHash(uintptr_t state_pointer) {
	return DistinctCountIntegerHash(static_cast<uint64_t>(state_pointer >> 4));
}

static hash_t DistinctCountGroupIndexHash(idx_t group_index) {
	return DistinctCountIntegerHash(static_cast<uint64_t>(group_index));
}

static hash_t DistinctCountPairHash(idx_t group_index, uint64_t payload) {
	auto hash = DistinctCountGroupIndexHash(group_index);
	hash ^= DistinctCountIntegerHash(payload) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
	return hash ? hash : 1;
}

static uint64_t DistinctCountInlinePayloadSlotMask(idx_t slot) {
	return uint64_t(1) << slot;
}

static idx_t DistinctCountInlinePayloadSlot(uint64_t payload) {
	static_assert((DistinctCountPointerGroup::INLINE_PAYLOAD_CAPACITY &
	               (DistinctCountPointerGroup::INLINE_PAYLOAD_CAPACITY - 1)) == 0,
	              "distinct count-pointer inline payload capacity must be a power of two");
	return DistinctCountIntegerHash(payload) & (DistinctCountPointerGroup::INLINE_PAYLOAD_CAPACITY - 1);
}

template <class FUNC>
static void DistinctCountVisitInlinePayloads(const DistinctCountPointerGroup &group, FUNC &&func) {
	auto occupied = group.inline_payload_occupied_mask;
	for (idx_t slot = 0; slot < DistinctCountPointerGroup::INLINE_PAYLOAD_CAPACITY; slot++) {
		const auto slot_mask = DistinctCountInlinePayloadSlotMask(slot);
		if (occupied & slot_mask) {
			func(group.inline_payloads[slot]);
		}
	}
}

static bool DistinctCountTryAddInlinePayload(DistinctCountPointerGroup &group, uint64_t payload, bool &payload_added) {
	payload_added = false;
	auto slot = DistinctCountInlinePayloadSlot(payload);
	for (idx_t probe_count = 0; probe_count < DistinctCountPointerGroup::INLINE_PAYLOAD_CAPACITY; probe_count++) {
		const auto slot_mask = DistinctCountInlinePayloadSlotMask(slot);
		if (!(group.inline_payload_occupied_mask & slot_mask)) {
			group.inline_payload_occupied_mask |= slot_mask;
			group.inline_payloads[slot] = payload;
			group.payload_count++;
			payload_added = true;
			return true;
		}
		if (group.inline_payloads[slot] == payload) {
			return true;
		}
		slot = (slot + 1) & (DistinctCountPointerGroup::INLINE_PAYLOAD_CAPACITY - 1);
	}
	return false;
}

static idx_t DistinctCountPointerRequiredCapacity(idx_t min_capacity, idx_t target_count) {
	return NextPowerOfTwo(MaxValue<idx_t>(min_capacity, target_count * 2));
}

void DistinctCountPairOverflowSet::ReserveEntries(idx_t target_count) {
	const auto required_capacity = DistinctCountPointerRequiredCapacity(idx_t(1024), target_count);
	if (hash_entries.size() < required_capacity) {
		ResizeTable(required_capacity);
	}
	group_entries.reserve(target_count);
	payload_entries.reserve(target_count);
	next_entries.reserve(target_count);
}

void DistinctCountPairOverflowSet::Reset() {
	group_entries.clear();
	payload_entries.clear();
	next_entries.clear();
	payload_index_entries.clear();
	hash_entries.clear();
	entry_count = 0;
	bitmask = 0;
}

void DistinctCountPairOverflowSet::ResizeTable(idx_t capacity) {
	payload_index_entries.assign(capacity, DConstants::INVALID_INDEX);
	hash_entries.assign(capacity, 0);
	bitmask = capacity - 1;
	for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
		InsertKnownNew(entry_idx, DistinctCountPairHash(group_entries[entry_idx], payload_entries[entry_idx]));
	}
}

void DistinctCountPairOverflowSet::InsertKnownNew(idx_t payload_entry_idx, hash_t hash) {
	D_ASSERT(!hash_entries.empty());
	auto ht_offset = hash & bitmask;
	for (idx_t iteration_count = 0; iteration_count < hash_entries.size(); iteration_count++) {
		if (hash_entries[ht_offset] == 0) {
			payload_index_entries[ht_offset] = payload_entry_idx;
			hash_entries[ht_offset] = hash;
			return;
		}
		ht_offset = (ht_offset + 1) & bitmask;
	}
	throw InternalException("Maximum count-distinct pointer-set rehash iteration count reached");
}

bool DistinctCountPairOverflowSet::Add(DistinctCountPointerGroup &group, uint64_t payload) {
	if (hash_entries.empty() || entry_count * 2 >= hash_entries.size()) {
		ReserveEntries(entry_count + 1);
	}
	const auto group_index = group.group_index;
	const auto hash = DistinctCountPairHash(group_index, payload);
	auto ht_offset = hash & bitmask;
	auto payload_index_slots = payload_index_entries.data();
	auto hash_slots = hash_entries.data();
	for (idx_t iteration_count = 0; iteration_count < hash_entries.size(); iteration_count++) {
		const auto entry_hash = hash_slots[ht_offset];
		if (entry_hash == 0) {
			const auto payload_entry_idx = entry_count++;
			group_entries.push_back(group_index);
			payload_entries.push_back(payload);
			next_entries.push_back(group.overflow_head);
			group.overflow_head = payload_entry_idx;
			payload_index_slots[ht_offset] = payload_entry_idx;
			hash_slots[ht_offset] = hash;
			return true;
		}
		const auto payload_entry_idx = payload_index_slots[ht_offset];
		if (entry_hash == hash && group_entries[payload_entry_idx] == group_index &&
		    payload_entries[payload_entry_idx] == payload) {
			return false;
		}
		ht_offset = (ht_offset + 1) & bitmask;
	}
	throw InternalException("Maximum count-distinct payload-set iteration count reached");
}

void DistinctCountPairOverflowSet::InsertKnownUnique(DistinctCountPointerGroup &group, uint64_t payload) {
	if (hash_entries.empty() || entry_count * 2 >= hash_entries.size()) {
		ReserveEntries(entry_count + 1);
	}
	const auto payload_entry_idx = entry_count++;
	group_entries.push_back(group.group_index);
	payload_entries.push_back(payload);
	next_entries.push_back(group.overflow_head);
	group.overflow_head = payload_entry_idx;
	InsertKnownNew(payload_entry_idx, DistinctCountPairHash(group.group_index, payload));
}

void DistinctCountPointerSet::ResizeGroups(idx_t capacity) {
	auto old_group_state_entries = std::move(group_state_entries);
	auto old_group_index_entries = std::move(group_index_entries);
	auto old_group_hash_entries = std::move(group_hash_entries);
	group_state_entries.assign(capacity, 0);
	group_index_entries.assign(capacity, 0);
	group_hash_entries.assign(capacity, 0);
	group_bitmask = capacity - 1;
	group_count = 0;
	for (idx_t entry_idx = 0; entry_idx < old_group_hash_entries.size(); entry_idx++) {
		const auto hash = old_group_hash_entries[entry_idx];
		if (hash == 0) {
			continue;
		}
		InsertKnownGroup(old_group_state_entries[entry_idx], old_group_index_entries[entry_idx], hash);
	}
}

void DistinctCountPointerSet::InsertKnownGroup(uintptr_t state_pointer, idx_t group_index, hash_t hash) {
	D_ASSERT(!group_hash_entries.empty());
	auto ht_offset = hash & group_bitmask;
	for (idx_t iteration_count = 0; iteration_count < group_hash_entries.size(); iteration_count++) {
		if (group_hash_entries[ht_offset] == 0) {
			group_state_entries[ht_offset] = state_pointer;
			group_index_entries[ht_offset] = group_index;
			group_hash_entries[ht_offset] = hash;
			group_count++;
			return;
		}
		ht_offset = (ht_offset + 1) & group_bitmask;
	}
	throw InternalException("Maximum count-distinct group-set rehash iteration count reached");
}

DistinctCountPointerGroup &DistinctCountPointerSet::LookupGroup(idx_t group_index) {
	D_ASSERT(group_index < groups.size());
	return groups[group_index];
}

const DistinctCountPointerGroup *DistinctCountPointerSet::FindGroup(uintptr_t state_pointer) const {
	if (group_hash_entries.empty()) {
		return nullptr;
	}
	const auto hash = DistinctCountPointerHash(state_pointer);
	auto ht_offset = hash & group_bitmask;
	for (idx_t iteration_count = 0; iteration_count < group_hash_entries.size(); iteration_count++) {
		const auto entry_hash = group_hash_entries[ht_offset];
		if (entry_hash == 0) {
			return nullptr;
		}
		if (entry_hash == hash && group_state_entries[ht_offset] == state_pointer) {
			const auto group_index = group_index_entries[ht_offset];
			D_ASSERT(group_index < groups.size());
			return &groups[group_index];
		}
		ht_offset = (ht_offset + 1) & group_bitmask;
	}
	throw InternalException("Maximum count-distinct group-set iteration count reached");
}

DistinctCountPointerGroup &DistinctCountPointerSet::InsertGroup(uintptr_t state_pointer, hash_t hash, idx_t ht_offset) {
	const auto group_index = groups.size();
	groups.emplace_back();
	groups.back().state_pointer = state_pointer;
	groups.back().group_index = group_index;
	groups.back().uses_overflow = use_global_payload_set;
	group_state_entries[ht_offset] = state_pointer;
	group_index_entries[ht_offset] = group_index;
	group_hash_entries[ht_offset] = hash;
	group_count++;
	return groups[group_index];
}

void DistinctCountPointerSet::ReserveGroupEntries(idx_t target_count) {
	const auto required_capacity = DistinctCountPointerRequiredCapacity(idx_t(64), target_count);
	if (group_hash_entries.size() < required_capacity) {
		ResizeGroups(required_capacity);
		groups.reserve(target_count);
	}
}

void DistinctCountPointerSet::ReserveGroupsForRows(idx_t count) {
	if (count == 0) {
		return;
	}
	ReserveGroupEntries(group_count + count);
}

void DistinctCountPointerSet::ReserveOverflowEntries(idx_t additional_count) {
	if (additional_count == 0) {
		return;
	}
	if (use_global_payload_set) {
		ReserveGlobalPayloadEntries(overflow.EntryCount() + additional_count);
		return;
	}
	if (overflow.EntryCount() == 0) {
		return;
	}
	overflow.ReserveEntries(overflow.EntryCount() + additional_count);
}

void DistinctCountPointerSet::ReserveGlobalPayloadEntries(idx_t target_count) {
	if (target_count <= global_payload_set_reserve_target) {
		return;
	}
	idx_t reserve_target = target_count;
	if (global_payload_set_reserve_target > 0) {
		const auto max_count = NumericLimits<idx_t>::Maximum();
		const auto doubled_target =
		    global_payload_set_reserve_target > max_count / 2 ? max_count : global_payload_set_reserve_target * 2;
		reserve_target = MaxValue(target_count, doubled_target);
	}
	overflow.ReserveEntries(reserve_target);
	global_payload_set_reserve_target = reserve_target;
}

DistinctCountPointerGroup &DistinctCountPointerSet::FindOrCreateGroup(uintptr_t state_pointer) {
	const auto hash = DistinctCountPointerHash(state_pointer);
	ReserveGroupEntries(group_count + 1);
	auto ht_offset = hash & group_bitmask;
	for (idx_t iteration_count = 0; iteration_count < group_hash_entries.size(); iteration_count++) {
		const auto entry_hash = group_hash_entries[ht_offset];
		if (entry_hash == 0) {
			return InsertGroup(state_pointer, hash, ht_offset);
		}
		if (entry_hash == hash && group_state_entries[ht_offset] == state_pointer) {
			return LookupGroup(group_index_entries[ht_offset]);
		}
		ht_offset = (ht_offset + 1) & group_bitmask;
	}
	throw InternalException("Maximum count-distinct group-set iteration count reached");
}

DistinctCountPointerGroup &DistinctCountPointerSet::FindOrCreateGroupCached(uintptr_t state_pointer) {
	const auto cache_idx = DistinctCountPointerHash(state_pointer) & (GROUP_LOOKUP_CACHE_SIZE - 1);
	const auto cached_group_idx = group_lookup_cache_indices[cache_idx];
	if (cached_group_idx != DConstants::INVALID_INDEX && group_lookup_cache_states[cache_idx] == state_pointer) {
		return LookupGroup(cached_group_idx);
	}
	auto &group = FindOrCreateGroup(state_pointer);
	group_lookup_cache_states[cache_idx] = state_pointer;
	group_lookup_cache_indices[cache_idx] = UnsafeNumericCast<idx_t>(&group - groups.data());
	return group;
}

bool DistinctCountPointerSet::PromoteToOverflow(DistinctCountPointerGroup &group) {
	overflow.ReserveEntries(overflow.EntryCount() + group.payload_count + 1);
	DistinctCountVisitInlinePayloads(group, [&](uint64_t payload) { overflow.InsertKnownUnique(group, payload); });
	group.inline_payload_occupied_mask = 0;
	group.uses_overflow = true;
	return true;
}

bool DistinctCountPointerSet::AddPayload(DistinctCountPointerGroup &group, uint64_t payload) {
	if (use_global_payload_set && !group.uses_overflow) {
		if (group.payload_count == 0) {
			group.uses_overflow = true;
		} else {
			PromoteToOverflow(group);
		}
	}
	if (group.uses_overflow) {
		if (!overflow.Add(group, payload)) {
			return false;
		}
		group.payload_count++;
		return true;
	}
	bool payload_added;
	if (DistinctCountTryAddInlinePayload(group, payload, payload_added)) {
		return payload_added;
	}
	PromoteToOverflow(group);
	if (!overflow.Add(group, payload)) {
		return false;
	}
	group.payload_count++;
	return true;
}

bool DistinctCountPointerSet::UseGlobalPayloadSet(idx_t expected_payload_count) {
	if (expected_payload_count > 0) {
		ReserveGlobalPayloadEntries(MaxValue(expected_payload_count, overflow.EntryCount()));
	}
	if (use_global_payload_set) {
		return true;
	}
	use_global_payload_set = true;
	for (auto &group : groups) {
		if (group.uses_overflow) {
			continue;
		}
		if (group.payload_count == 0) {
			group.uses_overflow = true;
			continue;
		}
		PromoteToOverflow(group);
	}
	return true;
}

static int64_t *DistinctCountState(uintptr_t state_pointer, idx_t state_value_offset) {
	return reinterpret_cast<int64_t *>(reinterpret_cast<data_ptr_t>(state_pointer) + state_value_offset);
}

bool DistinctCountPointerSet::EnsureStateValueOffset(idx_t state_value_offset_p) {
	if (state_value_offset_p == DConstants::INVALID_INDEX) {
		return false;
	}
	if (state_value_offset == DConstants::INVALID_INDEX) {
		state_value_offset = state_value_offset_p;
		return true;
	}
	return state_value_offset == state_value_offset_p;
}

bool DistinctCountPointerSet::MergeStatePayloadsTo(DistinctCountPointerSet &target, uintptr_t source_state_pointer,
                                                   uintptr_t target_state_pointer) {
	if (state_value_offset == DConstants::INVALID_INDEX || !target.EnsureStateValueOffset(state_value_offset)) {
		return false;
	}
	auto source_group = FindGroup(source_state_pointer);
	if (!source_group) {
		return false;
	}
	auto &target_group = target.FindOrCreateGroupCached(target_state_pointer);
	auto target_count_state = DistinctCountState(target_state_pointer, state_value_offset);
	if (!target_group.uses_overflow && target_group.payload_count == 0) {
		target.CopyUniquePayloadsToEmptyGroup(*source_group, overflow, target_group);
		target_group.payload_count = source_group->payload_count;
		*target_count_state += UnsafeNumericCast<int64_t>(source_group->payload_count);
		*DistinctCountState(source_state_pointer, state_value_offset) = 0;
		return true;
	}
	auto merge_payload = [&](uint64_t payload) {
		if (target.AddPayload(target_group, payload)) {
			(*target_count_state)++;
		}
	};
	if (source_group->uses_overflow) {
		overflow.VisitPayloads(*source_group, merge_payload);
	} else {
		DistinctCountVisitInlinePayloads(*source_group, merge_payload);
	}
	*DistinctCountState(source_state_pointer, state_value_offset) = 0;
	return true;
}

void DistinctCountPointerSet::CopyUniquePayloadsToEmptyGroup(const DistinctCountPointerGroup &source_group,
                                                             const DistinctCountPairOverflowSet &source_overflow,
                                                             DistinctCountPointerGroup &target_group) {
	D_ASSERT(target_group.payload_count == 0);
	D_ASSERT(!target_group.uses_overflow);
	if (!source_group.uses_overflow) {
		target_group.inline_payload_occupied_mask = source_group.inline_payload_occupied_mask;
		for (idx_t slot = 0; slot < DistinctCountPointerGroup::INLINE_PAYLOAD_CAPACITY; slot++) {
			target_group.inline_payloads[slot] = source_group.inline_payloads[slot];
		}
		return;
	}
	target_group.uses_overflow = true;
	overflow.ReserveEntries(overflow.EntryCount() + source_group.payload_count);
	source_overflow.VisitPayloads(source_group,
	                              [&](uint64_t payload) { overflow.InsertKnownUnique(target_group, payload); });
}

void DistinctCountPointerSet::RemapStatePointers(const vector<pair<uintptr_t, uintptr_t>> &state_pointer_mappings) {
	if (state_pointer_mappings.empty() || group_count == 0) {
		return;
	}
	unordered_map<uintptr_t, uintptr_t> state_pointer_remap;
	state_pointer_remap.reserve(state_pointer_mappings.size());
	for (auto &mapping : state_pointer_mappings) {
		state_pointer_remap[mapping.first] = mapping.second;
	}
	bool changed = false;
	for (auto &group : groups) {
		auto remap_entry = state_pointer_remap.find(group.state_pointer);
		if (remap_entry == state_pointer_remap.end()) {
			continue;
		}
		group.state_pointer = remap_entry->second;
		changed = true;
	}
	if (!changed) {
		return;
	}
	group_state_entries.clear();
	group_index_entries.clear();
	group_hash_entries.clear();
	group_count = 0;
	group_bitmask = 0;
	ReserveGroupEntries(groups.size());
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		InsertKnownGroup(groups[group_idx].state_pointer, group_idx,
		                 DistinctCountPointerHash(groups[group_idx].state_pointer));
	}
	group_lookup_cache_states.fill(0);
	group_lookup_cache_indices.fill(DConstants::INVALID_INDEX);
}

void DistinctCountPointerSet::Reset() {
	group_state_entries.clear();
	group_index_entries.clear();
	group_hash_entries.clear();
	groups.clear();
	overflow.Reset();
	group_lookup_cache_states.fill(0);
	group_lookup_cache_indices.fill(DConstants::INVALID_INDEX);
	group_count = 0;
	group_bitmask = 0;
	state_value_offset = DConstants::INVALID_INDEX;
	use_global_payload_set = false;
	global_payload_set_reserve_target = 0;
}

template <class T>
bool DistinctCountPointerSet::AddTemplated(Vector &state_pointers, Vector &payload, idx_t count,
                                           idx_t state_value_offset) {
	if (count == 0) {
		return true;
	}
	if (state_pointers.GetVectorType() != VectorType::FLAT_VECTOR ||
	    !FlatVector::Validity(state_pointers).CheckAllValid(count)) {
		return false;
	}

	const auto state_data = FlatVector::GetData<uintptr_t>(state_pointers);
	ReserveGroupsForRows(count);
	ReserveOverflowEntries(count);
	DistinctCountPointerGroup *current_group = nullptr;
	int64_t *current_count_state = nullptr;
	uintptr_t current_state_pointer = 0;
	if (payload.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::Validity(payload).CheckAllValid(count)) {
		const auto payload_data = FlatVector::GetData<T>(payload);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto state_pointer = state_data[row_idx];
			if (!current_group || current_state_pointer != state_pointer) {
				current_state_pointer = state_pointer;
				current_group = &FindOrCreateGroupCached(state_pointer);
				current_count_state = DistinctCountState(state_pointer, state_value_offset);
			}
			const auto payload_bits = static_cast<uint64_t>(payload_data[row_idx]);
			if (AddPayload(*current_group, payload_bits)) {
				(*current_count_state)++;
			}
		}
		return true;
	}

	UnifiedVectorFormat payload_data;
	payload.ToUnifiedFormat(payload_data);
	const auto payload_entries = UnifiedVectorFormat::GetData<T>(payload_data);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto payload_idx = payload_data.sel->get_index(row_idx);
		if (!payload_data.validity.RowIsValid(payload_idx)) {
			continue;
		}
		const auto state_pointer = state_data[row_idx];
		if (!current_group || current_state_pointer != state_pointer) {
			current_state_pointer = state_pointer;
			current_group = &FindOrCreateGroupCached(state_pointer);
			current_count_state = DistinctCountState(state_pointer, state_value_offset);
		}
		const auto payload_bits = static_cast<uint64_t>(payload_entries[payload_idx]);
		if (AddPayload(*current_group, payload_bits)) {
			(*current_count_state)++;
		}
	}
	return true;
}

template <class T>
bool DistinctCountPointerSet::AddSelectedTemplated(const uintptr_t *state_pointers, const sel_t *state_sel,
                                                   const sel_t *payload_sel, Vector &payload, idx_t count,
                                                   idx_t state_value_offset) {
	if (count == 0) {
		return true;
	}
	if (!state_pointers) {
		return false;
	}

	ReserveGroupsForRows(count);
	ReserveOverflowEntries(count);
	DistinctCountPointerGroup *current_group = nullptr;
	int64_t *current_count_state = nullptr;
	uintptr_t current_state_pointer = 0;
	if (payload.GetVectorType() == VectorType::FLAT_VECTOR &&
	    (payload_sel ? FlatVector::Validity(payload).CannotHaveNull()
	                 : FlatVector::Validity(payload).CheckAllValid(count))) {
		const auto payload_data = FlatVector::GetData<T>(payload);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto payload_idx = payload_sel ? payload_sel[row_idx] : row_idx;
			const auto state_idx = state_sel ? state_sel[payload_idx] : row_idx;
			const auto state_pointer = state_pointers[state_idx];
			if (!current_group || current_state_pointer != state_pointer) {
				current_state_pointer = state_pointer;
				current_group = &FindOrCreateGroupCached(state_pointer);
				current_count_state = DistinctCountState(state_pointer, state_value_offset);
			}
			const auto payload_bits = static_cast<uint64_t>(payload_data[payload_idx]);
			if (AddPayload(*current_group, payload_bits)) {
				(*current_count_state)++;
			}
		}
		return true;
	}

	UnifiedVectorFormat payload_data;
	payload.ToUnifiedFormat(payload_data);
	const auto payload_entries = UnifiedVectorFormat::GetData<T>(payload_data);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		const auto payload_idx = payload_sel ? payload_sel[row_idx] : row_idx;
		const auto payload_entry_idx = payload_data.sel->get_index(payload_idx);
		if (!payload_data.validity.RowIsValid(payload_entry_idx)) {
			continue;
		}
		const auto state_idx = state_sel ? state_sel[payload_idx] : row_idx;
		const auto state_pointer = state_pointers[state_idx];
		if (!current_group || current_state_pointer != state_pointer) {
			current_state_pointer = state_pointer;
			current_group = &FindOrCreateGroupCached(state_pointer);
			current_count_state = DistinctCountState(state_pointer, state_value_offset);
		}
		const auto payload_bits = static_cast<uint64_t>(payload_entries[payload_entry_idx]);
		if (AddPayload(*current_group, payload_bits)) {
			(*current_count_state)++;
		}
	}
	return true;
}

bool DistinctCountPointerSet::Add(Vector &state_pointers, Vector &payload, idx_t count, idx_t state_value_offset) {
	if (!DistinctCountPointerPayloadTypeSupported(payload.GetType().InternalType())) {
		return false;
	}
	if (!EnsureStateValueOffset(state_value_offset)) {
		return false;
	}
	switch (payload.GetType().InternalType()) {
	case PhysicalType::BOOL:
		return AddTemplated<bool>(state_pointers, payload, count, state_value_offset);
	case PhysicalType::UINT8:
		return AddTemplated<uint8_t>(state_pointers, payload, count, state_value_offset);
	case PhysicalType::INT8:
		return AddTemplated<int8_t>(state_pointers, payload, count, state_value_offset);
	case PhysicalType::UINT16:
		return AddTemplated<uint16_t>(state_pointers, payload, count, state_value_offset);
	case PhysicalType::INT16:
		return AddTemplated<int16_t>(state_pointers, payload, count, state_value_offset);
	case PhysicalType::UINT32:
		return AddTemplated<uint32_t>(state_pointers, payload, count, state_value_offset);
	case PhysicalType::INT32:
		return AddTemplated<int32_t>(state_pointers, payload, count, state_value_offset);
	case PhysicalType::UINT64:
		return AddTemplated<uint64_t>(state_pointers, payload, count, state_value_offset);
	case PhysicalType::INT64:
		return AddTemplated<int64_t>(state_pointers, payload, count, state_value_offset);
	default:
		return false;
	}
}

bool DistinctCountPointerSet::AddSelected(const uintptr_t *state_pointers, const sel_t *state_sel,
                                          const sel_t *payload_sel, Vector &payload, idx_t count,
                                          idx_t state_value_offset) {
	if (!DistinctCountPointerPayloadTypeSupported(payload.GetType().InternalType())) {
		return false;
	}
	if (!EnsureStateValueOffset(state_value_offset)) {
		return false;
	}
	switch (payload.GetType().InternalType()) {
	case PhysicalType::BOOL:
		return AddSelectedTemplated<bool>(state_pointers, state_sel, payload_sel, payload, count, state_value_offset);
	case PhysicalType::UINT8:
		return AddSelectedTemplated<uint8_t>(state_pointers, state_sel, payload_sel, payload, count,
		                                     state_value_offset);
	case PhysicalType::INT8:
		return AddSelectedTemplated<int8_t>(state_pointers, state_sel, payload_sel, payload, count, state_value_offset);
	case PhysicalType::UINT16:
		return AddSelectedTemplated<uint16_t>(state_pointers, state_sel, payload_sel, payload, count,
		                                      state_value_offset);
	case PhysicalType::INT16:
		return AddSelectedTemplated<int16_t>(state_pointers, state_sel, payload_sel, payload, count,
		                                     state_value_offset);
	case PhysicalType::UINT32:
		return AddSelectedTemplated<uint32_t>(state_pointers, state_sel, payload_sel, payload, count,
		                                      state_value_offset);
	case PhysicalType::INT32:
		return AddSelectedTemplated<int32_t>(state_pointers, state_sel, payload_sel, payload, count,
		                                     state_value_offset);
	case PhysicalType::UINT64:
		return AddSelectedTemplated<uint64_t>(state_pointers, state_sel, payload_sel, payload, count,
		                                      state_value_offset);
	case PhysicalType::INT64:
		return AddSelectedTemplated<int64_t>(state_pointers, state_sel, payload_sel, payload, count,
		                                     state_value_offset);
	default:
		return false;
	}
}

DistinctCountPointerScratch::DistinctCountPointerScratch()
    : state_addresses(LogicalType::POINTER), distinct_set(make_uniq<DistinctCountPointerSet>()) {
}

DistinctCountPointerScratch::~DistinctCountPointerScratch() {
}

void DistinctCountPointerScratch::Reset() {
	state_addresses.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::ValidityMutable(state_addresses).SetAllValid(STANDARD_VECTOR_SIZE);
	FlatVector::SetSize(state_addresses, 0);
	if (distinct_set) {
		distinct_set->Reset();
	} else {
		distinct_set = make_uniq<DistinctCountPointerSet>();
	}
}

} // namespace duckdb
