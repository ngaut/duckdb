//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_direct_ungrouped_aggregate_probe_consumer_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_payload_lane_runtime.hpp"
#include "sljit_hash_join_rhs_fixed_column_runtime.hpp"

#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

static inline void SljitAccumulateHugeintInt64Unchecked(hugeint_t &sum, int64_t value) {
	const auto previous_lower = sum.lower;
	sum.lower += static_cast<uint64_t>(value);
	const auto carry = static_cast<uint64_t>(sum.lower < previous_lower);
	const auto sign_extension = value < 0 ? ~uint64_t(0) : uint64_t(0);
	sum.upper = static_cast<int64_t>(static_cast<uint64_t>(sum.upper) + sign_extension + carry);
}

//! One concrete terminal type keeps key-reader dispatch bounded while folding
//! probe, dictionary lookup, null handling, and reduction into the match loop.
//! It deliberately does not publish match selections or row-pointer batches.
struct SljitHashJoinDirectUngroupedAggregateProbeConsumer {
	SljitHashJoinDirectUngroupedAggregateProbeConsumer(SljitNativeRegularHashJoinProbeInput &input_p,
	                                                   AggregatePrimitiveUpdateKind primitive_kind_p,
	                                                   const ExecutionHashJoinRHSFixedColumnSource &source)
	    : input(input_p), primitive_kind(primitive_kind_p), storage_kind(source.storage_kind),
	      layout_column_idx(source.layout_column_idx), layout_column_count(source.layout_column_count),
	      layout_offset(source.layout_offset), dictionary_index_offset(source.dictionary_index_offset),
	      dictionary_data(source.dictionary_data), dictionary_validity(source.dictionary_validity),
	      dictionary_count(source.dictionary_count), all_valid(source.all_valid), local_sum(0, 0) {
	}

	inline void EmitNoChainMatch(idx_t, data_ptr_t row_pointer) {
		matched_count++;
		switch (primitive_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
			aggregate_delta++;
			return;
		case AggregatePrimitiveUpdateKind::COUNT:
			aggregate_delta += SourceIsValid(row_pointer) ? 1 : 0;
			return;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
			int64_t value;
			if (TryLoadInt64(row_pointer, value)) {
				SljitAccumulateHugeintInt64Unchecked(local_sum, value);
				aggregate_delta++;
			}
			return;
		}
		default:
			throw InternalException("SLJIT direct hash-join aggregate consumer received an unsupported primitive");
		}
	}

	void Finish() {
		input.selected_count = matched_count;
		input.input_offset = input.count;
		input.resume_row_pointer = nullptr;
		input.finished = true;
	}

	idx_t MatchedCount() const {
		return matched_count;
	}

	idx_t AggregateDelta() const {
		return aggregate_delta;
	}

	const hugeint_t &LocalSum() const {
		return local_sum;
	}

private:
	inline idx_t DictionaryIndex(data_ptr_t row_pointer) const {
		if (!row_pointer || dictionary_index_offset == DConstants::INVALID_INDEX) {
			throw InternalException("SLJIT direct hash-join aggregate dictionary source is invalid");
		}
		const auto dictionary_idx = UnsafeNumericCast<idx_t>(Load<uint32_t>(row_pointer + dictionary_index_offset));
		if (dictionary_idx >= dictionary_count) {
			throw InternalException("SLJIT direct hash-join aggregate dictionary index is out of range");
		}
		return dictionary_idx;
	}

	inline bool DictionaryValueIsValid(idx_t dictionary_idx) const {
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityMask::GetEntryIndex(dictionary_idx, entry_idx, idx_in_entry);
		return ValidityMask::RowIsValid(dictionary_validity[entry_idx], idx_in_entry);
	}

	inline bool RowValueIsValid(data_ptr_t row_pointer) const {
		idx_t entry_idx;
		idx_t idx_in_entry;
		SljitHashJoinRowValidityMask::GetEntryIndex(layout_column_idx, entry_idx, idx_in_entry);
		return SljitHashJoinRowValidityMask::RowIsValid(
		    SljitHashJoinRowValidityMask(row_pointer, layout_column_count).GetValidityEntryUnsafe(entry_idx),
		    idx_in_entry);
	}

	inline bool SourceIsValid(data_ptr_t row_pointer) const {
		if (!row_pointer) {
			throw InternalException("SLJIT direct hash-join aggregate row source is invalid");
		}
		if (all_valid) {
			return true;
		}
		switch (storage_kind) {
		case ExecutionHashJoinRHSFixedColumnStorageKind::ROW:
			return RowValueIsValid(row_pointer);
		case ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY:
			return DictionaryValueIsValid(DictionaryIndex(row_pointer));
		default:
			throw InternalException("SLJIT direct hash-join aggregate storage source is invalid");
		}
	}

	inline bool TryLoadInt64(data_ptr_t row_pointer, int64_t &value) const {
		if (!row_pointer) {
			throw InternalException("SLJIT direct hash-join aggregate row source is invalid");
		}
		switch (storage_kind) {
		case ExecutionHashJoinRHSFixedColumnStorageKind::ROW:
			if (!all_valid && !RowValueIsValid(row_pointer)) {
				return false;
			}
			value = Load<int64_t>(row_pointer + layout_offset);
			return true;
		case ExecutionHashJoinRHSFixedColumnStorageKind::DICTIONARY: {
			const auto dictionary_idx = DictionaryIndex(row_pointer);
			if (!all_valid && !DictionaryValueIsValid(dictionary_idx)) {
				return false;
			}
			value = reinterpret_cast<const int64_t *>(dictionary_data)[dictionary_idx];
			return true;
		}
		default:
			throw InternalException("SLJIT direct hash-join aggregate storage source is invalid");
		}
	}

private:
	SljitNativeRegularHashJoinProbeInput &input;
	AggregatePrimitiveUpdateKind primitive_kind;
	ExecutionHashJoinRHSFixedColumnStorageKind storage_kind;
	idx_t layout_column_idx;
	idx_t layout_column_count;
	idx_t layout_offset;
	idx_t dictionary_index_offset;
	const_data_ptr_t dictionary_data;
	const validity_t *dictionary_validity;
	idx_t dictionary_count;
	bool all_valid;
	idx_t matched_count = 0;
	idx_t aggregate_delta = 0;
	hugeint_t local_sum;
};

} // namespace duckdb
