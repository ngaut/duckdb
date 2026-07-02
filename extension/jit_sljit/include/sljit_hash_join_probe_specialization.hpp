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

} // namespace duckdb
