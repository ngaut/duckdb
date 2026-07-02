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

#include <cstring>

namespace duckdb {

DistinctCountPointerGroup::DistinctCountPointerGroup() {
	std::memset(payload_tags, 0, sizeof(payload_tags));
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
	key ^= key >> 32;
	key *= 0xd6e8feb86659fd93ULL;
	key ^= key >> 32;
	return key ? key : 1;
}

static hash_t DistinctCountPointerHash(uintptr_t state_pointer) {
	return DistinctCountIntegerHash(static_cast<uint64_t>(state_pointer >> 4));
}

static hash_t DistinctCountPairHash(uintptr_t state_pointer, uint64_t payload) {
	auto hash = DistinctCountPointerHash(state_pointer);
	hash ^= DistinctCountIntegerHash(payload) + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
	return hash ? hash : 1;
}

static uint8_t DistinctCountPayloadTag(hash_t hash) {
	auto tag = static_cast<uint8_t>(hash >> 56);
	return tag ? tag : 1;
}

void DistinctCountPairOverflowSet::Resize(idx_t capacity) {
	auto old_state_entries = std::move(state_entries);
	auto old_payload_entries = std::move(payload_entries);
	auto old_hash_entries = std::move(hash_entries);
	state_entries.assign(capacity, 0);
	payload_entries.assign(capacity, 0);
	hash_entries.assign(capacity, 0);
	bitmask = capacity - 1;
	entry_count = 0;
	for (idx_t entry_idx = 0; entry_idx < old_hash_entries.size(); entry_idx++) {
		const auto hash = old_hash_entries[entry_idx];
		if (hash == 0) {
			continue;
		}
		InsertKnownNew(old_state_entries[entry_idx], old_payload_entries[entry_idx], hash);
	}
}

void DistinctCountPairOverflowSet::InsertKnownNew(uintptr_t state_pointer, uint64_t payload, hash_t hash) {
	D_ASSERT(!hash_entries.empty());
	auto ht_offset = hash & bitmask;
	for (idx_t iteration_count = 0; iteration_count < hash_entries.size(); iteration_count++) {
		if (hash_entries[ht_offset] == 0) {
			state_entries[ht_offset] = state_pointer;
			payload_entries[ht_offset] = payload;
			hash_entries[ht_offset] = hash;
			entry_count++;
			return;
		}
		ht_offset = (ht_offset + 1) & bitmask;
	}
	throw InternalException("Maximum count-distinct pointer-set rehash iteration count reached");
}

bool DistinctCountPairOverflowSet::Add(uintptr_t state_pointer, uint64_t payload) {
	const auto target_count = entry_count + 1;
	const auto required_capacity = NextPowerOfTwo(MaxValue<idx_t>(idx_t(1024), target_count * 2));
	if (hash_entries.size() < required_capacity) {
		Resize(required_capacity);
	}
	const auto hash = DistinctCountPairHash(state_pointer, payload);
	auto ht_offset = hash & bitmask;
	auto state_slots = state_entries.data();
	auto payload_slots = payload_entries.data();
	auto hash_slots = hash_entries.data();
	for (idx_t iteration_count = 0; iteration_count < hash_entries.size(); iteration_count++) {
		const auto entry_hash = hash_slots[ht_offset];
		if (entry_hash == 0) {
			state_slots[ht_offset] = state_pointer;
			payload_slots[ht_offset] = payload;
			hash_slots[ht_offset] = hash;
			entry_count++;
			return true;
		}
		if (entry_hash == hash && state_slots[ht_offset] == state_pointer && payload_slots[ht_offset] == payload) {
			return false;
		}
		ht_offset = (ht_offset + 1) & bitmask;
	}
	throw InternalException("Maximum count-distinct payload-set iteration count reached");
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

DistinctCountPointerGroup &DistinctCountPointerSet::FindOrCreateGroup(uintptr_t state_pointer) {
	const auto hash = DistinctCountPointerHash(state_pointer);
	const auto target_count = group_count + 1;
	const auto required_capacity = NextPowerOfTwo(MaxValue<idx_t>(idx_t(64), target_count * 2));
	if (group_hash_entries.size() < required_capacity) {
		ResizeGroups(required_capacity);
		groups.reserve(required_capacity);
	}
	auto ht_offset = hash & group_bitmask;
	for (idx_t iteration_count = 0; iteration_count < group_hash_entries.size(); iteration_count++) {
		const auto entry_hash = group_hash_entries[ht_offset];
		if (entry_hash == 0) {
			const auto group_index = groups.size();
			groups.emplace_back();
			groups.back().state_pointer = state_pointer;
			group_state_entries[ht_offset] = state_pointer;
			group_index_entries[ht_offset] = group_index;
			group_hash_entries[ht_offset] = hash;
			group_count++;
			return groups[group_index];
		}
		if (entry_hash == hash && group_state_entries[ht_offset] == state_pointer) {
			return groups[group_index_entries[ht_offset]];
		}
		ht_offset = (ht_offset + 1) & group_bitmask;
	}
	throw InternalException("Maximum count-distinct group-set iteration count reached");
}

bool DistinctCountPointerSet::PromoteToOverflow(DistinctCountPointerGroup &group) {
	for (idx_t entry_idx = 0; entry_idx < DistinctCountPointerGroup::INLINE_TABLE_CAPACITY; entry_idx++) {
		if (group.payload_tags[entry_idx] == 0) {
			continue;
		}
		const auto is_new = overflow.Add(group.state_pointer, group.payloads[entry_idx]);
		D_ASSERT(is_new);
		(void)is_new;
	}
	group.uses_overflow = true;
	return true;
}

bool DistinctCountPointerSet::AddPayload(DistinctCountPointerGroup &group, uint64_t payload) {
	if (group.uses_overflow) {
		return overflow.Add(group.state_pointer, payload);
	}
	const auto hash = DistinctCountIntegerHash(payload);
	const auto tag = DistinctCountPayloadTag(hash);
	auto ht_offset = hash & (DistinctCountPointerGroup::INLINE_TABLE_CAPACITY - 1);
	for (idx_t iteration_count = 0; iteration_count < DistinctCountPointerGroup::INLINE_TABLE_CAPACITY;
	     iteration_count++) {
		const auto entry_tag = group.payload_tags[ht_offset];
		if (entry_tag == 0) {
			if (group.payload_count >= DistinctCountPointerGroup::INLINE_PAYLOAD_CAPACITY) {
				PromoteToOverflow(group);
				return overflow.Add(group.state_pointer, payload);
			}
			group.payloads[ht_offset] = payload;
			group.payload_tags[ht_offset] = tag;
			group.payload_count++;
			return true;
		}
		if (entry_tag == tag && group.payloads[ht_offset] == payload) {
			return false;
		}
		ht_offset = (ht_offset + 1) & (DistinctCountPointerGroup::INLINE_TABLE_CAPACITY - 1);
	}
	PromoteToOverflow(group);
	return overflow.Add(group.state_pointer, payload);
}

static int64_t *DistinctCountState(uintptr_t state_pointer, idx_t state_value_offset) {
	return reinterpret_cast<int64_t *>(reinterpret_cast<data_ptr_t>(state_pointer) + state_value_offset);
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
	DistinctCountPointerGroup *current_group = nullptr;
	int64_t *current_count_state = nullptr;
	uintptr_t current_state_pointer = 0;
	if (payload.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::Validity(payload).CheckAllValid(count)) {
		const auto payload_data = FlatVector::GetData<T>(payload);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto state_pointer = state_data[row_idx];
			if (!current_group || current_state_pointer != state_pointer) {
				current_state_pointer = state_pointer;
				current_group = &FindOrCreateGroup(state_pointer);
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
			current_group = &FindOrCreateGroup(state_pointer);
			current_count_state = DistinctCountState(state_pointer, state_value_offset);
		}
		const auto payload_bits = static_cast<uint64_t>(payload_entries[payload_idx]);
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
	if (state_value_offset == DConstants::INVALID_INDEX) {
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

DistinctCountPointerScratch::DistinctCountPointerScratch()
    : state_addresses(LogicalType::POINTER), distinct_set(make_uniq<DistinctCountPointerSet>()) {
}

DistinctCountPointerScratch::~DistinctCountPointerScratch() {
}

} // namespace duckdb
