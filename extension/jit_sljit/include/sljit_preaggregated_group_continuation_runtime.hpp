//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_preaggregated_group_continuation_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_runtime_state.hpp"

#include <cstring>

namespace duckdb {

static idx_t SljitPreaggregatedGroupContinuationKeySize(PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BOOL:
		return sizeof(bool);
	case PhysicalType::INT8:
		return sizeof(int8_t);
	case PhysicalType::INT16:
		return sizeof(int16_t);
	case PhysicalType::INT32:
		return sizeof(int32_t);
	case PhysicalType::INT64:
		return sizeof(int64_t);
	case PhysicalType::INT128:
		return sizeof(hugeint_t);
	case PhysicalType::UINT8:
		return sizeof(uint8_t);
	case PhysicalType::UINT16:
		return sizeof(uint16_t);
	case PhysicalType::UINT32:
		return sizeof(uint32_t);
	case PhysicalType::UINT64:
		return sizeof(uint64_t);
	case PhysicalType::UINT128:
		return sizeof(uhugeint_t);
	default:
		return 0;
	}
}

static bool SljitLoadPreaggregatedGroupContinuationKey(DataChunk &groups, idx_t row_idx, PhysicalType physical_type,
                                                       std::array<uint8_t, sizeof(hugeint_t)> &key) {
	if (groups.ColumnCount() != 1 || row_idx >= groups.size() ||
	    groups.data[0].GetType().InternalType() != physical_type) {
		return false;
	}
	const auto key_size = SljitPreaggregatedGroupContinuationKeySize(physical_type);
	if (key_size == 0 || key_size > key.size()) {
		return false;
	}
	UnifiedVectorFormat format;
	groups.data[0].ToUnifiedFormat(format);
	const auto source_idx = format.sel->get_index(row_idx);
	if (!format.validity.RowIsValid(source_idx)) {
		return false;
	}
	std::memcpy(key.data(), format.data + source_idx * key_size, key_size);
	return true;
}

static bool SljitPreaggregatedGroupContinuationMatches(SljitPreaggregatedGroupContinuationState &continuation,
                                                       DataChunk &groups, idx_t row_idx) {
	if (!continuation.ready || continuation.state_address == 0) {
		return false;
	}
	std::array<uint8_t, sizeof(hugeint_t)> key {};
	if (!SljitLoadPreaggregatedGroupContinuationKey(groups, row_idx, continuation.physical_type, key)) {
		return false;
	}
	const auto key_size = SljitPreaggregatedGroupContinuationKeySize(continuation.physical_type);
	return std::memcmp(continuation.key.data(), key.data(), key_size) == 0;
}

static void SljitStorePreaggregatedGroupContinuation(SljitPreaggregatedGroupContinuationState &continuation,
                                                     DataChunk &groups, idx_t row_idx, uintptr_t state_address) {
	continuation.Clear();
	if (state_address == 0 || groups.ColumnCount() != 1 || row_idx >= groups.size()) {
		return;
	}
	auto physical_type = groups.data[0].GetType().InternalType();
	if (!SljitLoadPreaggregatedGroupContinuationKey(groups, row_idx, physical_type, continuation.key)) {
		return;
	}
	continuation.ready = true;
	continuation.physical_type = physical_type;
	continuation.state_address = state_address;
}

} // namespace duckdb
