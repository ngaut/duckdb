//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_rhs_fixed_column_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"

namespace duckdb {

using SljitHashJoinRowValidityMask = TemplatedValidityMask<uint8_t>;

static bool SljitHashJoinRHSFixedColumnSourceCanLoad(const ExecutionHashJoinRHSFixedColumnSource &source) {
	if (!source.ready || source.physical_type == PhysicalType::INVALID) {
		return false;
	}
	switch (source.storage_kind) {
	case ExecutionHashJoinRHSFixedColumnStorageKind::ROW:
		return source.layout_offset != DConstants::INVALID_INDEX &&
		       (source.all_valid ||
		        (source.layout_column_idx != DConstants::INVALID_INDEX && source.layout_column_count != 0));
	case ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY:
		return source.dictionary_index_offset != DConstants::INVALID_INDEX && source.dictionary_data &&
		       source.dictionary_count != 0 && (source.all_valid || source.dictionary_validity);
	default:
		return false;
	}
}

static bool SljitTryGetHashJoinRHSDictionaryIndex(data_ptr_t row_pointer,
                                                  const ExecutionHashJoinRHSFixedColumnSource &source,
                                                  idx_t &dictionary_idx) {
	if (!row_pointer || source.storage_kind != ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY ||
	    source.dictionary_index_offset == DConstants::INVALID_INDEX) {
		return false;
	}
	dictionary_idx = Load<uint32_t>(row_pointer + source.dictionary_index_offset);
	return dictionary_idx < source.dictionary_count;
}

static bool SljitHashJoinRHSFixedColumnSourceIsValid(data_ptr_t row_pointer,
                                                     const ExecutionHashJoinRHSFixedColumnSource &source) {
	if (!row_pointer || !SljitHashJoinRHSFixedColumnSourceCanLoad(source)) {
		return false;
	}
	switch (source.storage_kind) {
	case ExecutionHashJoinRHSFixedColumnStorageKind::ROW: {
		if (source.all_valid) {
			return true;
		}
		idx_t entry_idx;
		idx_t idx_in_entry;
		SljitHashJoinRowValidityMask::GetEntryIndex(source.layout_column_idx, entry_idx, idx_in_entry);
		return SljitHashJoinRowValidityMask::RowIsValid(
		    SljitHashJoinRowValidityMask(row_pointer, source.layout_column_count).GetValidityEntryUnsafe(entry_idx),
		    idx_in_entry);
	}
	case ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY: {
		idx_t dictionary_idx;
		if (!SljitTryGetHashJoinRHSDictionaryIndex(row_pointer, source, dictionary_idx)) {
			return false;
		}
		if (source.all_valid) {
			return true;
		}
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityMask::GetEntryIndex(dictionary_idx, entry_idx, idx_in_entry);
		return ValidityMask::RowIsValid(source.dictionary_validity[entry_idx], idx_in_entry);
	}
	default:
		return false;
	}
}

} // namespace duckdb
