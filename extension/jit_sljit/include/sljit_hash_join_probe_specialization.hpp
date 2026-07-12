//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_specialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_types.hpp"

namespace duckdb {

enum class SljitHashJoinMarkSelectionMode : uint8_t { NONE, MATCHES, NON_MATCHES };

struct SljitHashJoinProbeAllValidSpecializationKey {
	static constexpr idx_t LAYOUT_KIND_COUNT = 6;
	static constexpr idx_t SPECIALIZATION_COUNT = 2 * LAYOUT_KIND_COUNT;

	bool selected = false;
	SljitHashJoinProbeLayoutKind layout_kind = SljitHashJoinProbeLayoutKind::NO_CHAIN;

	static SljitHashJoinProbeAllValidSpecializationKey FromLayoutKind(bool selected,
	                                                                  SljitHashJoinProbeLayoutKind layout_kind) {
		SljitHashJoinProbeAllValidSpecializationKey key;
		key.selected = selected;
		key.layout_kind = layout_kind;
		return key;
	}

	idx_t LayoutIndex() const {
		return static_cast<idx_t>(layout_kind);
	}

	idx_t CacheIndex() const {
		return (selected ? LAYOUT_KIND_COUNT : 0) + LayoutIndex();
	}
};

struct SljitHashJoinProbeSpecializationKey {
	static constexpr idx_t MARK_SELECTION_MODE_COUNT = 3;
	static constexpr idx_t GENERAL_SPECIALIZATION_COUNT = 2 * MARK_SELECTION_MODE_COUNT;
	static constexpr idx_t ALL_VALID_SPECIALIZATION_COUNT =
	    MARK_SELECTION_MODE_COUNT * SljitHashJoinProbeAllValidSpecializationKey::SPECIALIZATION_COUNT;
	static constexpr idx_t SPECIALIZATION_COUNT = GENERAL_SPECIALIZATION_COUNT + ALL_VALID_SPECIALIZATION_COUNT;

	bool all_valid = false;
	bool uses_bloom_filter = false;
	SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE;
	SljitHashJoinProbeAllValidSpecializationKey all_valid_key;

	static SljitHashJoinProbeSpecializationKey General(bool uses_bloom_filter,
	                                                   SljitHashJoinMarkSelectionMode mark_selection_mode) {
		SljitHashJoinProbeSpecializationKey key;
		key.uses_bloom_filter = uses_bloom_filter;
		key.mark_selection_mode = mark_selection_mode;
		return key;
	}

	static SljitHashJoinProbeSpecializationKey
	AllValid(const SljitHashJoinProbeAllValidSpecializationKey &all_valid_key,
	         SljitHashJoinMarkSelectionMode mark_selection_mode) {
		SljitHashJoinProbeSpecializationKey key;
		key.all_valid = true;
		key.mark_selection_mode = mark_selection_mode;
		key.all_valid_key = all_valid_key;
		return key;
	}

	idx_t CacheIndex() const {
		auto mark_index = static_cast<idx_t>(mark_selection_mode);
		if (!all_valid) {
			return mark_index * 2 + (uses_bloom_filter ? 1 : 0);
		}
		return GENERAL_SPECIALIZATION_COUNT +
		       mark_index * SljitHashJoinProbeAllValidSpecializationKey::SPECIALIZATION_COUNT +
		       all_valid_key.CacheIndex();
	}
};

} // namespace duckdb
