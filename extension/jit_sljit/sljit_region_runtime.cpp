//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_runtime.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_region_runtime.hpp"

#include "sljit_codegen_util.hpp"
#include "sljit_native_runtime.hpp"
#include "sljit_region_codegen.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_region_backend.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include <array>
#include <chrono>
#include <cstring>
#include <exception>
#include <limits>
#include <type_traits>

namespace duckdb {

static int64_t SljitRegionElapsedMicros(std::chrono::steady_clock::time_point start) {
	auto end = std::chrono::steady_clock::now();
	return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

struct SljitLazyCodegenTiming {
	int64_t codegen_time_us = 0;
	int64_t machine_codegen_time_us = 0;
};

template <class BUILD>
static SljitLazyCodegenTiming TimeSljitLazyCodegen(BUILD build) {
	ExecutionRegionCompileTimings timings;
	auto codegen_start = std::chrono::steady_clock::now();
	{
		SljitCodegenTimingScope codegen_timing_scope(&timings);
		build();
	}
	SljitLazyCodegenTiming result;
	result.codegen_time_us = SljitRegionElapsedMicros(codegen_start);
	result.machine_codegen_time_us = timings.machine_codegen_time_us;
	return result;
}

static const sel_t *SljitNormalizedSourceSelectionData(const UnifiedVectorFormat &format) {
	if (!format.sel || format.sel == FlatVector::IncrementalSelectionVector()) {
		return nullptr;
	}
	return format.sel->data();
}

static bool SljitNormalizedSourceAllValid(const UnifiedVectorFormat &format, const sel_t *sel, idx_t count) {
	if (format.validity.CannotHaveNull()) {
		return true;
	}
	if (!sel) {
		return format.validity.CheckAllValid(count);
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!format.validity.RowIsValid(sel[row_idx])) {
			return false;
		}
	}
	return true;
}

static bool SljitNormalizedSourceAllValid(const UnifiedVectorFormat &format, const sel_t *source_sel,
                                          const SelectionVector *execute_sel, idx_t count) {
	if (!execute_sel) {
		return SljitNormalizedSourceAllValid(format, source_sel, count);
	}
	if (format.validity.CannotHaveNull()) {
		return true;
	}
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto logical_idx = execute_sel->get_index(row_idx);
		auto source_idx = source_sel ? source_sel[logical_idx] : logical_idx;
		if (!format.validity.RowIsValid(source_idx)) {
			return false;
		}
	}
	return true;
}

static const validity_t *SljitNormalizedSourceValidityData(const UnifiedVectorFormat &format, const sel_t *sel,
                                                           idx_t count) {
	return SljitNormalizedSourceAllValid(format, sel, count) ? nullptr : format.validity.GetData();
}

static const validity_t *SljitNormalizedSourceValidityData(const UnifiedVectorFormat &format, const sel_t *source_sel,
                                                           const SelectionVector *execute_sel, idx_t count) {
	return SljitNormalizedSourceAllValid(format, source_sel, execute_sel, count) ? nullptr : format.validity.GetData();
}

static bool SljitTryFastAppendFixedFlatAllValid(DataChunk &target, DataChunk &source) {
	const auto append_count = source.size();
	if (append_count == 0) {
		return true;
	}
	if (target.ColumnCount() != source.ColumnCount()) {
		return false;
	}
	const auto target_count = target.size();
	const auto new_count = target_count + append_count;
	if (new_count > STANDARD_VECTOR_SIZE) {
		return false;
	}
	for (idx_t col_idx = 0; col_idx < target.ColumnCount(); col_idx++) {
		auto &target_vector = target.data[col_idx];
		auto &source_vector = source.data[col_idx];
		if (target_vector.GetType() != source_vector.GetType()) {
			return false;
		}
		if (!TypeIsConstantSize(target_vector.GetType().InternalType())) {
			return false;
		}
		if (target_vector.GetVectorType() != VectorType::FLAT_VECTOR ||
		    source_vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		if (FlatVector::GetCapacity(target_vector) < new_count) {
			return false;
		}
		if (FlatVector::Validity(target_vector).CanHaveNull() ||
		    !FlatVector::Validity(source_vector).CheckAllValid(append_count)) {
			return false;
		}
	}
	for (idx_t col_idx = 0; col_idx < target.ColumnCount(); col_idx++) {
		auto &target_vector = target.data[col_idx];
		auto &source_vector = source.data[col_idx];
		const auto type_size = GetTypeIdSize(target_vector.GetType().InternalType());
		auto target_data = FlatVector::GetDataMutable(target_vector) + target_count * type_size;
		auto source_data = FlatVector::GetData(source_vector);
		memcpy(target_data, source_data, append_count * type_size);
		FlatVector::SetSize(target_vector, new_count);
	}
	target.CheckCardinality(new_count);
	return true;
}

static idx_t SljitBatchedSourceContractFetchBudget(idx_t max_chunks) {
	constexpr idx_t SOURCE_FETCHES_PER_DOWNSTREAM_BATCH = 64;
	const auto max_idx = NumericLimits<idx_t>::Maximum();
	if (max_chunks >= max_idx / SOURCE_FETCHES_PER_DOWNSTREAM_BATCH) {
		return max_idx;
	}
	return max_chunks * SOURCE_FETCHES_PER_DOWNSTREAM_BATCH;
}

static void SljitChargeDownstreamRows(idx_t &processed_rows, idx_t rows) {
	const auto max_idx = NumericLimits<idx_t>::Maximum();
	if (processed_rows >= max_idx - rows) {
		processed_rows = max_idx;
		return;
	}
	processed_rows += rows;
}

static bool SljitDownstreamRowBudgetReached(idx_t processed_rows, idx_t max_chunks) {
	const auto max_idx = NumericLimits<idx_t>::Maximum();
	if (max_chunks >= max_idx / STANDARD_VECTOR_SIZE) {
		return false;
	}
	return processed_rows >= max_chunks * STANDARD_VECTOR_SIZE;
}

static const sel_t *SljitCanonicalizeCommonSelection(vector<const sel_t *> &source_sel,
                                                     vector<const sel_t *> &group_sel) {
	const sel_t *common_sel = nullptr;
	for (auto sel : group_sel) {
		if (!sel) {
			return nullptr;
		}
		if (!common_sel) {
			common_sel = sel;
		} else if (common_sel != sel) {
			return nullptr;
		}
	}
	for (auto sel : source_sel) {
		if (!sel) {
			return nullptr;
		}
		if (!common_sel) {
			common_sel = sel;
		} else if (common_sel != sel) {
			return nullptr;
		}
	}
	if (!common_sel) {
		return nullptr;
	}
	for (auto &sel : group_sel) {
		sel = nullptr;
	}
	for (auto &sel : source_sel) {
		sel = nullptr;
	}
	return common_sel;
}

static const sel_t *SljitCanonicalizeCommonSourceSelection(vector<const sel_t *> &source_sel) {
	const sel_t *common_sel = nullptr;
	for (auto sel : source_sel) {
		if (!sel) {
			return nullptr;
		}
		if (!common_sel) {
			common_sel = sel;
		} else if (common_sel != sel) {
			return nullptr;
		}
	}
	if (!common_sel) {
		return nullptr;
	}
	for (auto &sel : source_sel) {
		sel = nullptr;
	}
	return common_sel;
}

static const sel_t *SljitCommonSelectionOrNull(const vector<const sel_t *> &source_sel) {
	const sel_t *common_sel = nullptr;
	for (auto sel : source_sel) {
		if (!sel) {
			return nullptr;
		}
		if (!common_sel) {
			common_sel = sel;
		} else if (common_sel != sel) {
			return nullptr;
		}
	}
	return common_sel;
}

static bool SljitAllSelectionsPresent(const vector<const sel_t *> &selections) {
	if (selections.empty()) {
		return false;
	}
	for (auto sel : selections) {
		if (!sel) {
			return false;
		}
	}
	return true;
}

template <class T>
static T **SljitPointerArrayOrNull(vector<T *> &values) {
	for (auto value : values) {
		if (value) {
			return values.data();
		}
	}
	return nullptr;
}

static const_data_ptr_t NativeHashJoinKeySourceData(UnifiedVectorFormat &format, SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int8_t>(format));
	case SljitNativeHashJoinKeyKind::INT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int16_t>(format));
	case SljitNativeHashJoinKeyKind::INT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeHashJoinKeyKind::INT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	case SljitNativeHashJoinKeyKind::INT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<hugeint_t>(format));
	case SljitNativeHashJoinKeyKind::UINT8:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint8_t>(format));
	case SljitNativeHashJoinKeyKind::UINT16:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint16_t>(format));
	case SljitNativeHashJoinKeyKind::UINT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint32_t>(format));
	case SljitNativeHashJoinKeyKind::UINT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uint64_t>(format));
	case SljitNativeHashJoinKeyKind::UINT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<uhugeint_t>(format));
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
}

static inline hash_t SljitHashJoinCombineHashScalar(hash_t left, hash_t right) {
	left ^= left >> 32;
	left *= 0xd6e8feb86659fd93U;
	return left ^ right;
}

template <class T>
static inline bool SljitHashJoinKeysEqual(const data_ptr_t row_location, idx_t layout_offset, T probe_key) {
	return Load<T>(row_location + layout_offset) == probe_key;
}

static inline void SljitPrefetchHashJoinEntry(const ht_entry_t *entries, idx_t ht_offset) {
#if defined(__GNUC__) || defined(__clang__)
	__builtin_prefetch(entries + ht_offset, 0, 3);
#else
	(void)entries;
	(void)ht_offset;
#endif
}

static inline void SljitPrefetchHashJoinRow(data_ptr_t row_location, idx_t layout_offset) {
#if defined(__GNUC__) || defined(__clang__)
	__builtin_prefetch(row_location + layout_offset, 0, 3);
#else
	(void)row_location;
	(void)layout_offset;
#endif
}

static inline data_ptr_t SljitHashJoinEntryPointer(hash_t entry_value) {
	return cast_uint64_to_pointer(entry_value & ht_entry_t::POINTER_MASK);
}

static inline int32_t SljitCastHashJoinKeyInt64ToInt32(const SljitNativeHashJoinProbeInput &input, int64_t value) {
	return input.source_key0_int64_to_int32_unchecked ? UnsafeNumericCast<int32_t>(value) : NumericCast<int32_t>(value);
}

static inline uint64_t SljitBloomFilterMask(hash_t hash) {
	return BloomFilter::GetMask(hash);
}

static inline bool SljitBloomFilterMayContainKnownPresent(const uint64_t *bits, uint64_t bitmask, hash_t hash) {
	const auto slot = bits[hash & bitmask];
	const auto mask = SljitBloomFilterMask(hash);
	return (slot & mask) == mask;
}

static inline bool SljitBloomFilterMayContain(const SljitNativeHashJoinProbeInput &input, hash_t hash) {
	return !input.bloom_filter_bits ||
	       SljitBloomFilterMayContainKnownPresent(input.bloom_filter_bits, input.bloom_filter_bitmask, hash);
}

template <bool HAS_BLOOM>
static inline bool SljitBloomFilterMayContainTemplated(const SljitNativeHashJoinProbeInput &input, hash_t hash) {
	if constexpr (!HAS_BLOOM) {
		return true;
	} else {
		return SljitBloomFilterMayContainKnownPresent(input.bloom_filter_bits, input.bloom_filter_bitmask, hash);
	}
}

static bool SljitHashJoinMatchedProbeOutputMode(ExecutionHashJoinProbeOutputMode mode) {
	return mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	       mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
}

struct SljitHashJoinMatchedRowConsumer {
	explicit SljitHashJoinMatchedRowConsumer(SljitNativeHashJoinProbeInput &input)
	    : row_pointers(input.row_pointers), match_sel(input.match_sel), selected_count(input.selected_count) {
	}

	inline void EmitMatch(const idx_t row_idx, const data_ptr_t row_location) {
		row_pointers[selected_count] = row_location;
		match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
		selected_count++;
	}

	idx_t Count() const {
		return selected_count;
	}

private:
	data_ptr_t *__restrict row_pointers;
	sel_t *__restrict match_sel;
	idx_t selected_count;
};

static bool SljitHashJoinCanUseAllValidNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                    const ExecutionHashJoinTableLayout &layout,
                                                    const SljitNativeHashJoinProbeInput &input, bool selected) {
	if (plan.residual_predicate || plan.mark_build_match || !SljitHashJoinMatchedProbeOutputMode(plan.output_mode) ||
	    layout.chains_longer_than_one || input.source_validity || input.resume_row_pointer ||
	    input.count > input.output_capacity) {
		return false;
	}
	if (selected) {
		return input.source_sel && input.source_sel[0];
	}
	return !input.source_sel;
}

static bool SljitHashJoinCanUseAllValidSingleKeyChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                           const ExecutionHashJoinTableLayout &layout,
                                                           const SljitNativeHashJoinProbeInput &input) {
	return plan.keys.size() == 1 && plan.equality_key_count == 1 && !plan.residual_predicate &&
	       !plan.mark_build_match && SljitHashJoinMatchedProbeOutputMode(plan.output_mode) &&
	       layout.chains_longer_than_one && !layout.needs_chain_matcher && !input.source_validity &&
	       input.output_capacity > 0 && (!layout.dictionary_emission || layout.aux_next_ptrs);
}

static bool SljitHashJoinCanUseAllValidPairChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                      const ExecutionHashJoinTableLayout &layout,
                                                      const SljitNativeHashJoinProbeInput &input, bool selected) {
	if (plan.keys.size() != 2 || plan.equality_key_count != 2 || plan.residual_predicate ||
	    !SljitHashJoinMatchedProbeOutputMode(plan.output_mode) || !layout.chains_longer_than_one ||
	    layout.needs_chain_matcher || input.source_validity || input.output_capacity == 0 ||
	    (layout.dictionary_emission && !layout.aux_next_ptrs)) {
		return false;
	}
	if (selected) {
		return input.source_sel && input.source_sel[0];
	}
	return !input.source_sel;
}

static bool SljitHashJoinCanUseSingleKeyNotEqualPredicateChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                                    const ExecutionHashJoinTableLayout &layout,
                                                                    const SljitNativeHashJoinProbeInput &input,
                                                                    bool selected) {
	if (plan.keys.size() != 2 || plan.equality_key_count != 1 || plan.residual_predicate || plan.mark_build_match ||
	    !SljitHashJoinMatchedProbeOutputMode(plan.output_mode) || !layout.chains_longer_than_one ||
	    input.source_validity || input.output_capacity == 0 || (layout.dictionary_emission && !layout.aux_next_ptrs)) {
		return false;
	}
	if (selected) {
		return input.source_sel && input.source_sel[0];
	}
	return !input.source_sel;
}

static inline data_ptr_t SljitHashJoinNextChainPointer(const SljitNativeHashJoinProbeInput &input,
                                                       data_ptr_t row_location) {
	if (!input.chains_longer_than_one) {
		return nullptr;
	}
	if (input.dictionary_emission) {
		const auto dict_index = Load<uint32_t>(row_location + input.pointer_offset);
		return input.aux_next_ptrs[dict_index];
	}
	return cast_uint64_to_pointer(Load<uint64_t>(row_location + input.pointer_offset));
}

template <class MATCH>
static inline data_ptr_t SljitHashJoinFindFirstChainPointer(const SljitNativeHashJoinProbeInput &input,
                                                            const ht_entry_t *entries, hash_t hash, idx_t ht_offset,
                                                            idx_t prefetch_offset, MATCH match) {
	if (!SljitBloomFilterMayContain(input, hash)) {
		return nullptr;
	}
	hash_t salt = 0;
	if (input.use_salt) {
		salt = hash & ht_entry_t::SALT_MASK;
	}
	while (true) {
		const auto &entry = entries[ht_offset];
		if (!entry.IsOccupied()) {
			return nullptr;
		}
		if (!input.use_salt || entry.GetSaltWithNulls() == salt) {
			auto row_location = entry.GetPointer();
			SljitPrefetchHashJoinRow(row_location, prefetch_offset);
			if (match(row_location)) {
				return row_location;
			}
		}
		ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
	}
}

static bool SljitHashJoinCanUseInt64PairProbe(const SljitNativeHashJoinProbePlan &plan) {
	if (plan.keys.size() != 2 || plan.equality_key_count != 2 ||
	    !SljitHashJoinMatchedProbeOutputMode(plan.output_mode)) {
		return false;
	}
	for (auto &key : plan.keys) {
		if (!key.equality_key || key.null_equal || key.comparison_type != ExecutionRegionComparisonType::EQUAL) {
			return false;
		}
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT64 && key.key_kind != SljitNativeHashJoinKeyKind::UINT64) {
			return false;
		}
	}
	return true;
}

template <bool SELECTED, bool USE_SALT, bool HAS_BLOOM, class CONSUMER>
static void ExecuteAllValidInt64PairNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                 SljitNativeHashJoinProbeInput &input, CONSUMER &consumer) {
	const auto key0_data = reinterpret_cast<const uint64_t *__restrict>(input.source_data[0]);
	const auto key1_data = reinterpret_cast<const uint64_t *__restrict>(input.source_data[1]);
	const auto entries = reinterpret_cast<const ht_entry_t *__restrict>(input.entries);
	const sel_t *__restrict key_sel = nullptr;
	if (SELECTED) {
		key_sel = input.source_sel[0];
	}
	const auto key0_offset = plan.keys[0].key_layout_offset;
	const auto key1_offset = plan.keys[1].key_layout_offset;
	const auto bitmask = input.bitmask;

	auto row_idx = input.input_offset;
	if (row_idx < input.count) {
		auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		auto key0 = key0_data[source_idx];
		auto key1 = key1_data[source_idx];
		auto hash = SljitHashJoinCombineHashScalar(Hash<uint64_t>(key0), Hash<uint64_t>(key1));
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);
		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			uint64_t next_key0 = 0;
			uint64_t next_key1 = 0;
			hash_t next_hash = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				const auto next_source_idx = SELECTED ? key_sel[next_row_idx] : UnsafeNumericCast<sel_t>(next_row_idx);
				next_key0 = key0_data[next_source_idx];
				next_key1 = key1_data[next_source_idx];
				next_hash = SljitHashJoinCombineHashScalar(Hash<uint64_t>(next_key0), Hash<uint64_t>(next_key1));
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}
			if (SljitBloomFilterMayContainTemplated<HAS_BLOOM>(input, hash)) {
				hash_t salt = 0;
				if constexpr (USE_SALT) {
					salt = hash & ht_entry_t::SALT_MASK;
				}
				while (true) {
					const auto entry_value = entries[ht_offset].GetValue();
					if (!entry_value) {
						break;
					}
					if (!USE_SALT || (entry_value & ht_entry_t::SALT_MASK) == salt) {
						auto row_location = SljitHashJoinEntryPointer(entry_value);
						if (SljitHashJoinKeysEqual<uint64_t>(row_location, key0_offset, key0) &&
						    SljitHashJoinKeysEqual<uint64_t>(row_location, key1_offset, key1)) {
							consumer.EmitMatch(row_idx, row_location);
							break;
						}
					}
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
				}
			}
			if (!has_next) {
				break;
			}
			row_idx = next_row_idx;
			key0 = next_key0;
			key1 = next_key1;
			hash = next_hash;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = consumer.Count();
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

template <bool SELECTED>
static void ExecuteAllValidInt64PairNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                 SljitNativeHashJoinProbeInput &input) {
	SljitHashJoinMatchedRowConsumer consumer(input);
	if (input.use_salt) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidInt64PairNoChainProbe<SELECTED, true, true>(plan, input, consumer);
		} else {
			ExecuteAllValidInt64PairNoChainProbe<SELECTED, true, false>(plan, input, consumer);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidInt64PairNoChainProbe<SELECTED, false, true>(plan, input, consumer);
		} else {
			ExecuteAllValidInt64PairNoChainProbe<SELECTED, false, false>(plan, input, consumer);
		}
	}
}

template <bool SELECTED>
static bool TryExecuteAllValidInt64PairNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                    const ExecutionHashJoinTableLayout &layout,
                                                    SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseInt64PairProbe(plan) ||
	    !SljitHashJoinCanUseAllValidNoChainProbe(plan, layout, input, SELECTED)) {
		return false;
	}
	ExecuteAllValidInt64PairNoChainProbe<SELECTED>(plan, input);
	return true;
}

template <bool SELECTED>
static bool TryExecuteAllValidInt64PairChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                  const ExecutionHashJoinTableLayout &layout,
                                                  SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseAllValidPairChainProbe(plan, layout, input, SELECTED)) {
		return false;
	}
	for (auto &key : plan.keys) {
		if (!key.equality_key || key.null_equal || key.comparison_type != ExecutionRegionComparisonType::EQUAL) {
			return false;
		}
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT64 && key.key_kind != SljitNativeHashJoinKeyKind::UINT64) {
			return false;
		}
	}

	const auto key0_data = reinterpret_cast<const uint64_t *>(input.source_data[0]);
	const auto key1_data = reinterpret_cast<const uint64_t *>(input.source_data[1]);
	const auto key_sel = SELECTED ? input.source_sel[0] : nullptr;
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto key0_offset = plan.keys[0].key_layout_offset;
	const auto key1_offset = plan.keys[1].key_layout_offset;
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto selected_count = input.selected_count;
	auto row_idx = input.input_offset;
	auto resume_row_pointer = input.resume_row_pointer;

	while (row_idx < input.count) {
		const auto source_idx = SELECTED ? key_sel[row_idx] : row_idx;
		const auto key0 = key0_data[source_idx];
		const auto key1 = key1_data[source_idx];
		data_ptr_t row_location = resume_row_pointer;
		resume_row_pointer = nullptr;
		if (!row_location) {
			const auto hash = SljitHashJoinCombineHashScalar(Hash<uint64_t>(key0), Hash<uint64_t>(key1));
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			row_location = SljitHashJoinFindFirstChainPointer(
			    input, entries, hash, ht_offset, key0_offset, [&](data_ptr_t candidate) {
				    return SljitHashJoinKeysEqual<uint64_t>(candidate, key0_offset, key0) &&
				           SljitHashJoinKeysEqual<uint64_t>(candidate, key1_offset, key1);
			    });
		}

		bool advanced_row = false;
		while (row_location) {
			const auto next_row_location = SljitHashJoinNextChainPointer(input, row_location);
			if (next_row_location) {
				SljitPrefetchHashJoinRow(next_row_location, key0_offset);
			}
			if (SljitHashJoinKeysEqual<uint64_t>(row_location, key0_offset, key0) &&
			    SljitHashJoinKeysEqual<uint64_t>(row_location, key1_offset, key1)) {
				if (plan.mark_build_match) {
					row_location[plan.found_match_offset] = 1;
				}
				input.row_pointers[selected_count] = row_location;
				input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
				selected_count++;
				if (matched_probe_only) {
					row_idx++;
					advanced_row = true;
					if (selected_count >= input.output_capacity) {
						input.selected_count = selected_count;
						input.input_offset = row_idx;
						input.resume_row_pointer = nullptr;
						input.finished = row_idx >= input.count;
						return true;
					}
					break;
				}
				if (selected_count >= input.output_capacity) {
					input.selected_count = selected_count;
					if (next_row_location) {
						input.input_offset = row_idx;
						input.resume_row_pointer = next_row_location;
						input.finished = false;
					} else {
						input.input_offset = row_idx + 1;
						input.resume_row_pointer = nullptr;
						input.finished = input.input_offset >= input.count;
					}
					return true;
				}
			}
			row_location = next_row_location;
		}
		if (!advanced_row) {
			row_idx++;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
	return true;
}

template <bool SELECTED>
static bool TryExecuteAllValidSingleKeyNotEqualPredicateChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                                   const ExecutionHashJoinTableLayout &layout,
                                                                   SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseSingleKeyNotEqualPredicateChainProbe(plan, layout, input, SELECTED)) {
		return false;
	}
	auto &key = plan.keys[0];
	auto &predicate = plan.keys[1];
	if (!key.equality_key || key.null_equal || key.comparison_type != ExecutionRegionComparisonType::EQUAL ||
	    predicate.equality_key || predicate.null_equal ||
	    predicate.comparison_type != ExecutionRegionComparisonType::NOT_EQUAL) {
		return false;
	}
	if ((key.key_kind != SljitNativeHashJoinKeyKind::INT64 && key.key_kind != SljitNativeHashJoinKeyKind::UINT64) ||
	    (predicate.key_kind != SljitNativeHashJoinKeyKind::INT64 &&
	     predicate.key_kind != SljitNativeHashJoinKeyKind::UINT64)) {
		return false;
	}

	const auto key_data = reinterpret_cast<const uint64_t *>(input.source_data[0]);
	const auto predicate_data = reinterpret_cast<const uint64_t *>(input.source_data[1]);
	const auto key_sel = SELECTED ? input.source_sel[0] : nullptr;
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto key_offset = key.key_layout_offset;
	const auto predicate_offset = predicate.key_layout_offset;
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto selected_count = input.selected_count;
	auto row_idx = input.input_offset;
	auto resume_row_pointer = input.resume_row_pointer;

	while (row_idx < input.count) {
		const auto source_idx = SELECTED ? key_sel[row_idx] : row_idx;
		const auto key_value = key_data[source_idx];
		const auto predicate_value = predicate_data[source_idx];
		data_ptr_t row_location = resume_row_pointer;
		resume_row_pointer = nullptr;
		if (!row_location) {
			const auto hash = Hash<uint64_t>(key_value);
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			row_location = SljitHashJoinFindFirstChainPointer(
			    input, entries, hash, ht_offset, key_offset, [&](data_ptr_t candidate) {
				    return SljitHashJoinKeysEqual<uint64_t>(candidate, key_offset, key_value);
			    });
		}

		bool advanced_row = false;
		while (row_location) {
			const auto next_row_location = SljitHashJoinNextChainPointer(input, row_location);
			if (next_row_location) {
				SljitPrefetchHashJoinRow(next_row_location, key_offset);
			}
			if (SljitHashJoinKeysEqual<uint64_t>(row_location, key_offset, key_value) &&
			    !SljitHashJoinKeysEqual<uint64_t>(row_location, predicate_offset, predicate_value)) {
				input.row_pointers[selected_count] = row_location;
				input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
				selected_count++;
				if (matched_probe_only) {
					row_idx++;
					advanced_row = true;
					if (selected_count >= input.output_capacity) {
						input.selected_count = selected_count;
						input.input_offset = row_idx;
						input.resume_row_pointer = nullptr;
						input.finished = row_idx >= input.count;
						return true;
					}
					break;
				}
				if (selected_count >= input.output_capacity) {
					input.selected_count = selected_count;
					if (next_row_location) {
						input.input_offset = row_idx;
						input.resume_row_pointer = next_row_location;
						input.finished = false;
					} else {
						input.input_offset = row_idx + 1;
						input.resume_row_pointer = nullptr;
						input.finished = input.input_offset >= input.count;
					}
					return true;
				}
			}
			row_location = next_row_location;
		}
		if (!advanced_row) {
			row_idx++;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
	return true;
}

template <class T, bool SELECTED, bool USE_SALT, bool HAS_BLOOM, class CONSUMER>
static void ExecuteAllValidSingleKeyNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                 SljitNativeHashJoinProbeInput &input, CONSUMER &consumer) {
	const auto key_data = reinterpret_cast<const T *__restrict>(input.source_data[0]);
	const auto entries = reinterpret_cast<const ht_entry_t *__restrict>(input.entries);
	const sel_t *__restrict key_sel = nullptr;
	if (SELECTED) {
		key_sel = input.source_sel[0];
	}
	const auto key_offset = plan.keys[0].key_layout_offset;
	const auto bitmask = input.bitmask;

	auto row_idx = input.input_offset;
	if (row_idx < input.count) {
		auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		auto key = key_data[source_idx];
		auto hash = Hash<T>(key);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			T next_key = 0;
			hash_t next_hash = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				const auto next_source_idx = SELECTED ? key_sel[next_row_idx] : UnsafeNumericCast<sel_t>(next_row_idx);
				next_key = key_data[next_source_idx];
				next_hash = Hash<T>(next_key);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			if (SljitBloomFilterMayContainTemplated<HAS_BLOOM>(input, hash)) {
				hash_t salt = 0;
				if constexpr (USE_SALT) {
					salt = hash & ht_entry_t::SALT_MASK;
				}
				while (true) {
					const auto entry_value = entries[ht_offset].GetValue();
					if (!entry_value) {
						break;
					}
					if (!USE_SALT || (entry_value & ht_entry_t::SALT_MASK) == salt) {
						auto row_location = SljitHashJoinEntryPointer(entry_value);
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<T>(row_location, key_offset, key)) {
							consumer.EmitMatch(row_idx, row_location);
							break;
						}
					}
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
				}
			}
			if (!has_next) {
				break;
			}
			row_idx = next_row_idx;
			key = next_key;
			hash = next_hash;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = consumer.Count();
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

static void ExecuteFlatAllValidSingleKeyNoChainProbeInt64ToInt32(const SljitNativeHashJoinProbePlan &plan,
                                                                 SljitNativeHashJoinProbeInput &input) {
	const auto key_data = reinterpret_cast<const int64_t *__restrict>(input.source_data[0]);
	const auto entries = reinterpret_cast<const ht_entry_t *__restrict>(input.entries);
	SljitHashJoinMatchedRowConsumer consumer(input);
	const auto key_offset = plan.keys[0].key_layout_offset;
	const auto bitmask = input.bitmask;

	auto row_idx = input.input_offset;
	if (row_idx < input.count) {
		auto key = SljitCastHashJoinKeyInt64ToInt32(input, key_data[row_idx]);
		auto hash = Hash<int32_t>(key);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			int32_t next_key = 0;
			hash_t next_hash = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				next_key = SljitCastHashJoinKeyInt64ToInt32(input, key_data[next_row_idx]);
				next_hash = Hash<int32_t>(next_key);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			if (SljitBloomFilterMayContain(input, hash)) {
				hash_t salt = 0;
				if (input.use_salt) {
					salt = hash & ht_entry_t::SALT_MASK;
				}
				while (true) {
					const auto entry_value = entries[ht_offset].GetValue();
					if (!entry_value) {
						break;
					}
					if (!input.use_salt || (entry_value & ht_entry_t::SALT_MASK) == salt) {
						auto row_location = SljitHashJoinEntryPointer(entry_value);
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<int32_t>(row_location, key_offset, key)) {
							consumer.EmitMatch(row_idx, row_location);
							break;
						}
					}
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & bitmask);
				}
			}
			if (!has_next) {
				break;
			}
			row_idx = next_row_idx;
			key = next_key;
			hash = next_hash;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = consumer.Count();
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

template <class T, bool SELECTED>
static void ExecuteAllValidSingleKeyNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                 SljitNativeHashJoinProbeInput &input) {
	SljitHashJoinMatchedRowConsumer consumer(input);
	if (input.use_salt) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidSingleKeyNoChainProbe<T, SELECTED, true, true>(plan, input, consumer);
		} else {
			ExecuteAllValidSingleKeyNoChainProbe<T, SELECTED, true, false>(plan, input, consumer);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidSingleKeyNoChainProbe<T, SELECTED, false, true>(plan, input, consumer);
		} else {
			ExecuteAllValidSingleKeyNoChainProbe<T, SELECTED, false, false>(plan, input, consumer);
		}
	}
}

static bool SljitHashJoinCanUseSingleKeyProbe(const SljitNativeHashJoinProbePlan &plan) {
	if (plan.keys.size() != 1 || plan.equality_key_count != 1) {
		return false;
	}
	auto &key = plan.keys[0];
	return key.equality_key && !key.null_equal && key.comparison_type == ExecutionRegionComparisonType::EQUAL;
}

static bool TryExecuteSelectedAllValidSingleKeyNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                            const ExecutionHashJoinTableLayout &layout,
                                                            SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseSingleKeyProbe(plan) ||
	    !SljitHashJoinCanUseAllValidNoChainProbe(plan, layout, input, true)) {
		return false;
	}
	auto &key = plan.keys[0];

	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		ExecuteAllValidSingleKeyNoChainProbe<int8_t, true>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		ExecuteAllValidSingleKeyNoChainProbe<uint8_t, true>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		ExecuteAllValidSingleKeyNoChainProbe<int16_t, true>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		ExecuteAllValidSingleKeyNoChainProbe<uint16_t, true>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		ExecuteAllValidSingleKeyNoChainProbe<int32_t, true>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		ExecuteAllValidSingleKeyNoChainProbe<uint32_t, true>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		ExecuteAllValidSingleKeyNoChainProbe<int64_t, true>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		ExecuteAllValidSingleKeyNoChainProbe<uint64_t, true>(plan, input);
		return true;
	default:
		return false;
	}
}

static bool TryExecuteFlatAllValidSingleKeyNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                        const ExecutionHashJoinTableLayout &layout,
                                                        SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseSingleKeyProbe(plan) ||
	    !SljitHashJoinCanUseAllValidNoChainProbe(plan, layout, input, false)) {
		return false;
	}
	auto &key = plan.keys[0];
	if (input.source_key0_int64_to_int32) {
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT32) {
			return false;
		}
		ExecuteFlatAllValidSingleKeyNoChainProbeInt64ToInt32(plan, input);
		return true;
	}

	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		ExecuteAllValidSingleKeyNoChainProbe<int8_t, false>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		ExecuteAllValidSingleKeyNoChainProbe<uint8_t, false>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		ExecuteAllValidSingleKeyNoChainProbe<int16_t, false>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		ExecuteAllValidSingleKeyNoChainProbe<uint16_t, false>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		ExecuteAllValidSingleKeyNoChainProbe<int32_t, false>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		ExecuteAllValidSingleKeyNoChainProbe<uint32_t, false>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		ExecuteAllValidSingleKeyNoChainProbe<int64_t, false>(plan, input);
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		ExecuteAllValidSingleKeyNoChainProbe<uint64_t, false>(plan, input);
		return true;
	default:
		return false;
	}
}

template <class T, bool SELECTED>
static void ExecuteAllValidSingleKeyChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                               SljitNativeHashJoinProbeInput &input) {
	const auto key_data = reinterpret_cast<const T *>(input.source_data[0]);
	const auto key_sel = SELECTED ? input.source_sel[0] : nullptr;
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto key_offset = plan.keys[0].key_layout_offset;
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto selected_count = input.selected_count;
	auto row_idx = input.input_offset;
	auto resume_row_pointer = input.resume_row_pointer;

	while (row_idx < input.count) {
		const auto source_idx = SELECTED ? key_sel[row_idx] : row_idx;
		const auto key = key_data[source_idx];
		data_ptr_t row_location = resume_row_pointer;
		resume_row_pointer = nullptr;
		if (!row_location) {
			auto hash = Hash<T>(key);
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			row_location = SljitHashJoinFindFirstChainPointer(
			    input, entries, hash, ht_offset, key_offset,
			    [&](data_ptr_t candidate) { return SljitHashJoinKeysEqual<T>(candidate, key_offset, key); });
		}
		bool advanced_row = false;
		while (row_location) {
			const auto next_row_location = SljitHashJoinNextChainPointer(input, row_location);
			if (next_row_location) {
				SljitPrefetchHashJoinRow(next_row_location, key_offset);
			}
			if (SljitHashJoinKeysEqual<T>(row_location, key_offset, key)) {
				input.row_pointers[selected_count] = row_location;
				input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
				selected_count++;
				if (matched_probe_only) {
					row_idx++;
					advanced_row = true;
					if (selected_count >= input.output_capacity) {
						input.selected_count = selected_count;
						input.input_offset = row_idx;
						input.resume_row_pointer = nullptr;
						input.finished = row_idx >= input.count;
						return;
					}
					row_location = nullptr;
					break;
				}
				if (selected_count >= input.output_capacity) {
					if (next_row_location) {
						input.selected_count = selected_count;
						input.input_offset = row_idx;
						input.resume_row_pointer = next_row_location;
						input.finished = false;
						return;
					}
					row_idx++;
					input.selected_count = selected_count;
					input.input_offset = row_idx;
					input.resume_row_pointer = nullptr;
					input.finished = row_idx >= input.count;
					return;
				}
			}
			row_location = next_row_location;
		}
		if (!advanced_row) {
			row_idx++;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

template <bool SELECTED>
static void ExecuteAllValidSingleKeyChainProbeInt64ToInt32(const SljitNativeHashJoinProbePlan &plan,
                                                           SljitNativeHashJoinProbeInput &input) {
	const auto key_data = reinterpret_cast<const int64_t *>(input.source_data[0]);
	const auto key_sel = SELECTED ? input.source_sel[0] : nullptr;
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto key_offset = plan.keys[0].key_layout_offset;
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto selected_count = input.selected_count;
	auto row_idx = input.input_offset;
	auto resume_row_pointer = input.resume_row_pointer;

	while (row_idx < input.count) {
		const auto source_idx = SELECTED ? key_sel[row_idx] : row_idx;
		const auto key = SljitCastHashJoinKeyInt64ToInt32(input, key_data[source_idx]);
		data_ptr_t row_location = resume_row_pointer;
		resume_row_pointer = nullptr;
		if (!row_location) {
			auto hash = Hash<int32_t>(key);
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			row_location = SljitHashJoinFindFirstChainPointer(
			    input, entries, hash, ht_offset, key_offset,
			    [&](data_ptr_t candidate) { return SljitHashJoinKeysEqual<int32_t>(candidate, key_offset, key); });
		}
		bool advanced_row = false;
		while (row_location) {
			const auto next_row_location = SljitHashJoinNextChainPointer(input, row_location);
			if (next_row_location) {
				SljitPrefetchHashJoinRow(next_row_location, key_offset);
			}
			if (SljitHashJoinKeysEqual<int32_t>(row_location, key_offset, key)) {
				input.row_pointers[selected_count] = row_location;
				input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
				selected_count++;
				if (matched_probe_only) {
					row_idx++;
					advanced_row = true;
					if (selected_count >= input.output_capacity) {
						input.selected_count = selected_count;
						input.input_offset = row_idx;
						input.resume_row_pointer = nullptr;
						input.finished = row_idx >= input.count;
						return;
					}
					row_location = nullptr;
					break;
				}
				if (selected_count >= input.output_capacity) {
					if (next_row_location) {
						input.selected_count = selected_count;
						input.input_offset = row_idx;
						input.resume_row_pointer = next_row_location;
						input.finished = false;
						return;
					}
					row_idx++;
					input.selected_count = selected_count;
					input.input_offset = row_idx;
					input.resume_row_pointer = nullptr;
					input.finished = row_idx >= input.count;
					return;
				}
			}
			row_location = next_row_location;
		}
		if (!advanced_row) {
			row_idx++;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

template <class T>
static void ExecuteFlatAllValidSingleKeyChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                   SljitNativeHashJoinProbeInput &input) {
	const auto key_data = reinterpret_cast<const T *>(input.source_data[0]);
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto key_offset = plan.keys[0].key_layout_offset;
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto selected_count = input.selected_count;

	auto row_idx = input.input_offset;
	if (row_idx < input.count) {
		auto key = key_data[row_idx];
		auto hash = Hash<T>(key);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			T next_key = 0;
			hash_t next_hash = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				next_key = key_data[next_row_idx];
				next_hash = Hash<T>(next_key);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & input.bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			data_ptr_t row_location = nullptr;
			row_location = SljitHashJoinFindFirstChainPointer(
			    input, entries, hash, ht_offset, key_offset,
			    [&](data_ptr_t candidate) { return SljitHashJoinKeysEqual<T>(candidate, key_offset, key); });

			bool advanced_row = false;
			while (row_location) {
				const auto next_row_location = SljitHashJoinNextChainPointer(input, row_location);
				if (next_row_location) {
					SljitPrefetchHashJoinRow(next_row_location, key_offset);
				}
				if (SljitHashJoinKeysEqual<T>(row_location, key_offset, key)) {
					input.row_pointers[selected_count] = row_location;
					input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
					selected_count++;
					if (matched_probe_only) {
						row_idx++;
						advanced_row = true;
						if (selected_count >= input.output_capacity) {
							input.selected_count = selected_count;
							input.input_offset = row_idx;
							input.resume_row_pointer = nullptr;
							input.finished = row_idx >= input.count;
							return;
						}
						break;
					}
					if (selected_count >= input.output_capacity) {
						input.selected_count = selected_count;
						if (next_row_location) {
							input.input_offset = row_idx;
							input.resume_row_pointer = next_row_location;
							input.finished = false;
						} else {
							input.input_offset = row_idx + 1;
							input.resume_row_pointer = nullptr;
							input.finished = input.input_offset >= input.count;
						}
						return;
					}
				}
				row_location = next_row_location;
			}
			if (!advanced_row) {
				row_idx++;
			}
			if (!has_next) {
				break;
			}
			row_idx = next_row_idx;
			key = next_key;
			hash = next_hash;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

static void ExecuteFlatAllValidSingleKeyChainProbeInt64ToInt32(const SljitNativeHashJoinProbePlan &plan,
                                                               SljitNativeHashJoinProbeInput &input) {
	const auto key_data = reinterpret_cast<const int64_t *>(input.source_data[0]);
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto key_offset = plan.keys[0].key_layout_offset;
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto selected_count = input.selected_count;

	auto row_idx = input.input_offset;
	if (row_idx < input.count) {
		auto key = SljitCastHashJoinKeyInt64ToInt32(input, key_data[row_idx]);
		auto hash = Hash<int32_t>(key);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			int32_t next_key = 0;
			hash_t next_hash = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				next_key = SljitCastHashJoinKeyInt64ToInt32(input, key_data[next_row_idx]);
				next_hash = Hash<int32_t>(next_key);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & input.bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			data_ptr_t row_location = nullptr;
			row_location = SljitHashJoinFindFirstChainPointer(
			    input, entries, hash, ht_offset, key_offset,
			    [&](data_ptr_t candidate) { return SljitHashJoinKeysEqual<int32_t>(candidate, key_offset, key); });

			bool advanced_row = false;
			while (row_location) {
				const auto next_row_location = SljitHashJoinNextChainPointer(input, row_location);
				if (next_row_location) {
					SljitPrefetchHashJoinRow(next_row_location, key_offset);
				}
				if (SljitHashJoinKeysEqual<int32_t>(row_location, key_offset, key)) {
					input.row_pointers[selected_count] = row_location;
					input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
					selected_count++;
					if (matched_probe_only) {
						row_idx++;
						advanced_row = true;
						if (selected_count >= input.output_capacity) {
							input.selected_count = selected_count;
							input.input_offset = row_idx;
							input.resume_row_pointer = nullptr;
							input.finished = row_idx >= input.count;
							return;
						}
						break;
					}
					if (selected_count >= input.output_capacity) {
						input.selected_count = selected_count;
						if (next_row_location) {
							input.input_offset = row_idx;
							input.resume_row_pointer = next_row_location;
							input.finished = false;
						} else {
							input.input_offset = row_idx + 1;
							input.resume_row_pointer = nullptr;
							input.finished = input.input_offset >= input.count;
						}
						return;
					}
				}
				row_location = next_row_location;
			}
			if (!advanced_row) {
				row_idx++;
			}
			if (!has_next) {
				break;
			}
			row_idx = next_row_idx;
			key = next_key;
			hash = next_hash;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

template <bool SELECTED>
static bool TryExecuteAllValidSingleKeyChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                  const ExecutionHashJoinTableLayout &layout,
                                                  SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseAllValidSingleKeyChainProbe(plan, layout, input)) {
		return false;
	}
	if (SELECTED) {
		if (!input.source_sel || !input.source_sel[0]) {
			return false;
		}
	} else if (input.source_sel) {
		return false;
	}
	auto &key = plan.keys[0];
	if (!key.equality_key || key.null_equal || key.comparison_type != ExecutionRegionComparisonType::EQUAL) {
		return false;
	}

	if (input.source_key0_int64_to_int32) {
		if (key.key_kind != SljitNativeHashJoinKeyKind::INT32) {
			return false;
		}
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbeInt64ToInt32(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbeInt64ToInt32<SELECTED>(plan, input);
		}
		return true;
	}

	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbe<int8_t>(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbe<int8_t, SELECTED>(plan, input);
		}
		return true;
	case SljitNativeHashJoinKeyKind::UINT8:
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbe<uint8_t>(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbe<uint8_t, SELECTED>(plan, input);
		}
		return true;
	case SljitNativeHashJoinKeyKind::INT16:
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbe<int16_t>(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbe<int16_t, SELECTED>(plan, input);
		}
		return true;
	case SljitNativeHashJoinKeyKind::UINT16:
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbe<uint16_t>(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbe<uint16_t, SELECTED>(plan, input);
		}
		return true;
	case SljitNativeHashJoinKeyKind::INT32:
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbe<int32_t>(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbe<int32_t, SELECTED>(plan, input);
		}
		return true;
	case SljitNativeHashJoinKeyKind::UINT32:
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbe<uint32_t>(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbe<uint32_t, SELECTED>(plan, input);
		}
		return true;
	case SljitNativeHashJoinKeyKind::INT64:
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbe<int64_t>(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbe<int64_t, SELECTED>(plan, input);
		}
		return true;
	case SljitNativeHashJoinKeyKind::UINT64:
		if (!SELECTED && !input.resume_row_pointer) {
			ExecuteFlatAllValidSingleKeyChainProbe<uint64_t>(plan, input);
		} else {
			ExecuteAllValidSingleKeyChainProbe<uint64_t, SELECTED>(plan, input);
		}
		return true;
	default:
		return false;
	}
}

static const_data_ptr_t NativeNestedLoopJoinConditionSourceData(UnifiedVectorFormat &format,
                                                                SljitNativeNestedLoopJoinValueKind kind) {
	switch (kind) {
	case SljitNativeNestedLoopJoinValueKind::INT32:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int32_t>(format));
	case SljitNativeNestedLoopJoinValueKind::INT64:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(format));
	case SljitNativeNestedLoopJoinValueKind::INT128:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<hugeint_t>(format));
	case SljitNativeNestedLoopJoinValueKind::DOUBLE:
		return reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<double>(format));
	default:
		throw InternalException("Unknown SLJIT native nested loop join value kind");
	}
}

static const char *SljitRegionOpKindName(SljitNativeRegionOpKind kind) {
	switch (kind) {
	case SljitNativeRegionOpKind::FILTER:
		return "filter";
	case SljitNativeRegionOpKind::PROJECTION:
		return "projection";
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return "hash_join_probe";
	case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		return "hash_join_build";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
		return "nested_loop_join_probe";
	case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
		return "nested_loop_join_build";
	case SljitNativeRegionOpKind::ORDER_SINK:
		return "order_sink";
	case SljitNativeRegionOpKind::APPEND_SINK:
		return "append_sink";
	case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
		return "delim_join_sink";
	case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
		return "aggregate_update";
	default:
		return "unknown";
	}
}

static string SljitRegionStageName(idx_t op_idx, SljitNativeRegionOpKind kind) {
	return "op" + std::to_string(op_idx) + ":" + SljitRegionOpKindName(kind);
}

static string SljitRegionStageName(idx_t op_idx, SljitNativeRegionOpKind kind, const string &phase) {
	return SljitRegionStageName(op_idx, kind) + "." + phase;
}

static bool SljitUnifiedFormatHasIdentitySelection(const UnifiedVectorFormat &format) {
	return !format.sel || format.sel == FlatVector::IncrementalSelectionVector();
}

static constexpr const char *SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_STAGE = "generated_regular_probe_function";
static constexpr const char *SLJIT_GENERATED_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "generated_regular_probe_flat_all_valid_function";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_INT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_int64_pair_no_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_INT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_int64_pair_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_INT64_PAIR_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_int64_pair_no_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_INT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_int64_pair_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_single_key_notequal_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_single_key_no_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_single_key_no_chain";
static constexpr const char *SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_flat_all_valid_single_key_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_single_key_chain";
static constexpr const char *SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE =
    "fast_regular_probe_selected_all_valid_single_key_notequal_chain";
static constexpr const char *SLJIT_GENERATED_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE =
    "generated_regular_probe_selected_all_valid_function";
static constexpr const char *SLJIT_GENERATED_PERFECT_HASH_JOIN_PROBE_STAGE = "generated_perfect_probe_function";

static const char *SljitHashJoinProbeLayoutName(ExecutionHashJoinProbeLayoutKind kind) {
	switch (kind) {
	case ExecutionHashJoinProbeLayoutKind::NONE:
		return "none";
	case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
		return "regular_hash_table";
	case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
		return "perfect_hash_table";
	default:
		return "unknown";
	}
}

static SljitNativeIntegerKind SljitTypedExpressionTreeIntegerKind(const LogicalType &type) {
	if (type.id() == LogicalTypeId::BOOLEAN && type.InternalType() == PhysicalType::BOOL) {
		return SljitNativeIntegerKind::UINT8;
	}
	if (type.id() != LogicalTypeId::DECIMAL && type.InternalType() == PhysicalType::INT32) {
		return SljitNativeIntegerKind::INT32;
	}
	if (type.id() == LogicalTypeId::DECIMAL && type.InternalType() == PhysicalType::INT64) {
		return SljitNativeIntegerKind::DECIMAL64;
	}
	if (type.InternalType() == PhysicalType::INT64) {
		return SljitNativeIntegerKind::INT64;
	}
	throw InternalException("Unsupported SLJIT typed expression-tree physical type");
}

static const_data_ptr_t SljitTypedExpressionTreeSourceData(UnifiedVectorFormat &format, const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		return reinterpret_cast<const_data_ptr_t>(format.data);
	}
	return NativeIntegerSourceData(format, SljitTypedExpressionTreeIntegerKind(type));
}

static SljitNativeIntegerKind SljitPerfectHashGroupIntegerKind(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return SljitNativeIntegerKind::INT8;
	case PhysicalType::UINT8:
		return SljitNativeIntegerKind::UINT8;
	case PhysicalType::INT32:
		return SljitNativeIntegerKind::INT32;
	case PhysicalType::INT64:
		return SljitNativeIntegerKind::INT64;
	default:
		throw InternalException("Unsupported SLJIT perfect-hash group physical type");
	}
}

static std::chrono::steady_clock::time_point SljitRegionStageStart(ExecutionRegionRuntime &runtime) {
	return runtime.TraceRuntime() ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
}

static void RecordSljitRegionStageRuntime(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                          std::chrono::steady_clock::time_point start) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(SljitRegionStageName(op_idx, kind), SljitRegionElapsedMicros(start));
}

static void RecordSljitRegionStageRuntime(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                          const char *phase, std::chrono::steady_clock::time_point start) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(SljitRegionStageName(op_idx, kind, phase), SljitRegionElapsedMicros(start));
}

static void RecordSljitRegionRuntimePath(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind,
                                         const char *path, idx_t count = 1) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	auto runtime_path = string(SljitRegionOpKindName(kind)) + "." + path;
	runtime.RecordJitRuntimePath(runtime_path.c_str(), count);
}

static void RecordSljitRegionMaterializationBoundary(ExecutionRegionRuntime &runtime, SljitNativeRegionOpKind kind,
                                                     const char *boundary, idx_t row_count) {
	if (!runtime.TraceRuntime() || row_count == 0) {
		return;
	}
	auto materialization_boundary = string(SljitRegionOpKindName(kind)) + "." + boundary;
	runtime.RecordJitMaterializationBoundary(materialization_boundary.c_str(), row_count);
}

static bool SljitAppendSelectedProbeBatch(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                          DataChunk &source, const SelectionVector &source_selection,
                                          Vector &row_pointers, idx_t count, DataChunk &target,
                                          Vector &target_row_pointers) {
	if (target.ColumnCount() != source.ColumnCount()) {
		return false;
	}
	const auto append_offset = target.size();
	const auto new_count = append_offset + count;
	if (new_count > STANDARD_VECTOR_SIZE) {
		return false;
	}
	for (idx_t col_idx = 0; col_idx < source.ColumnCount(); col_idx++) {
		if (target.data[col_idx].GetType() != source.data[col_idx].GetType()) {
			return false;
		}
	}

	auto append_stage_start = SljitRegionStageStart(runtime);
	for (idx_t col_idx = 0; col_idx < source.ColumnCount(); col_idx++) {
		VectorOperations::Copy(source.data[col_idx], target.data[col_idx], source_selection, source.size(), 0,
		                       append_offset, count);
		FlatVector::SetSize(target.data[col_idx], count_t(new_count));
	}
	VectorOperations::Copy(row_pointers, target_row_pointers, count, 0, append_offset);
	FlatVector::SetSize(target_row_pointers, count_t(new_count));
	target.CheckCardinality(new_count);
	RecordSljitRegionStageRuntime(runtime, op_idx, kind, "pending_probe_batch_append", append_stage_start);
	RecordSljitRegionMaterializationBoundary(runtime, kind, "pending_probe_batch", count);
	return true;
}

static void RecordSljitRegionStageRuntimePath(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                              SljitNativeRegionOpKind kind, const char *phase,
                                              std::chrono::steady_clock::time_point start) {
	RecordSljitRegionStageRuntime(runtime, op_idx, kind, phase, start);
	RecordSljitRegionRuntimePath(runtime, kind, phase);
}

static void RecordSljitRegionStageRuntimeWithSuffix(ExecutionRegionRuntime &runtime, idx_t op_idx,
                                                    SljitNativeRegionOpKind kind, const char *suffix,
                                                    std::chrono::steady_clock::time_point start) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(SljitRegionStageName(op_idx, kind) + suffix, SljitRegionElapsedMicros(start));
}

static void RecordSljitDirectAppendProfileStage(ExecutionRegionRuntime &runtime, const string &stage_prefix,
                                                const char *stage_name, int64_t runtime_time_us, idx_t count = 1) {
	if (!runtime.TraceRuntime() || runtime_time_us <= 0 || count == 0) {
		return;
	}
	runtime.RecordGeneratedStageRuntime(stage_prefix + "." + stage_name, runtime_time_us, count);
}

static void RecordSljitDirectAppendProfile(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind kind,
                                           const DirectAppendProfile &profile) {
	if (!runtime.TraceRuntime()) {
		return;
	}
	auto prepare_prefix = SljitRegionStageName(op_idx, kind, "direct_append_prepare");
	RecordSljitDirectAppendProfileStage(runtime, prepare_prefix, "storage_finalize_row_group",
	                                    profile.prepare_finalize_row_group_time_us);
	RecordSljitDirectAppendProfileStage(runtime, prepare_prefix, "storage_new_row_group",
	                                    profile.prepare_new_row_group_time_us);
	RecordSljitDirectAppendProfileStage(runtime, prepare_prefix, "storage_fixed_column_prepare",
	                                    profile.prepare_fixed_column_time_us, profile.prepare_fixed_column_count);

	auto commit_prefix = SljitRegionStageName(op_idx, kind, "direct_append_commit");
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_source_format",
	                                    profile.commit_source_format_time_us,
	                                    profile.commit_source_append_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_source_append",
	                                    profile.commit_source_append_time_us,
	                                    profile.commit_source_append_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_fixed_column_commit",
	                                    profile.commit_fixed_column_time_us, profile.commit_fixed_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_distinct_lock",
	                                    profile.commit_distinct_lock_time_us);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_source_distinct_stats",
	                                    profile.commit_source_distinct_stats_time_us,
	                                    profile.commit_source_distinct_stats_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_target_distinct_stats",
	                                    profile.commit_target_distinct_stats_time_us,
	                                    profile.commit_target_distinct_stats_column_count);
	RecordSljitDirectAppendProfileStage(runtime, commit_prefix, "storage_provided_distinct_count",
	                                    profile.commit_provided_distinct_count_time_us,
	                                    profile.commit_provided_distinct_count_column_count);
}

class SljitRegionStageAccumulator {
public:
	SljitRegionStageAccumulator(ExecutionRegionRuntime &runtime_p, idx_t op_idx, SljitNativeRegionOpKind kind,
	                            const char *phase)
	    : runtime(runtime_p), enabled(runtime.TraceRuntime()) {
		if (enabled) {
			stage = SljitRegionStageName(op_idx, kind, phase);
		}
	}

	void Add(std::chrono::steady_clock::time_point start) {
		if (!enabled) {
			return;
		}
		runtime_time_us += SljitRegionElapsedMicros(start);
		count++;
	}

	void Flush() {
		if (!enabled || count == 0) {
			return;
		}
		runtime.RecordGeneratedStageRuntime(stage, runtime_time_us, count);
		runtime_time_us = 0;
		count = 0;
	}

private:
	ExecutionRegionRuntime &runtime;
	string stage;
	int64_t runtime_time_us = 0;
	idx_t count = 0;
	bool enabled;
};

class SljitRegionStageRecorder : public ExecutionOperatorStageRecorder {
public:
	SljitRegionStageRecorder(ExecutionRegionRuntime &runtime_p, string stage_prefix_p)
	    : runtime(runtime_p), stage_prefix(std::move(stage_prefix_p)) {
	}

	SljitRegionStageRecorder(ExecutionRegionRuntime &runtime_p, idx_t op_idx, SljitNativeRegionOpKind kind,
	                         const char *phase)
	    : runtime(runtime_p) {
		if (runtime.TraceRuntime()) {
			stage_prefix = SljitRegionStageName(op_idx, kind, phase);
		}
	}

	void RecordStageRuntime(ExecutionRegionStageId stage, int64_t runtime_time_us) override {
		if (!runtime.TraceRuntime()) {
			return;
		}
		recorded_runtime_time_us += runtime_time_us;
		runtime.RecordGeneratedStageRuntime(stage_prefix + "." + stage.name, runtime_time_us);
	}

	int64_t RecordedRuntimeTimeUs() const {
		return recorded_runtime_time_us;
	}

private:
	ExecutionRegionRuntime &runtime;
	string stage_prefix;
	int64_t recorded_runtime_time_us = 0;
};

class SljitNativeRegionKernel : public ExecutionRegionKernel {
private:
	enum class SljitRegularHashJoinProbeInputKind : uint8_t { GENERIC, FLAT_ALL_VALID, SELECTED_ALL_VALID };

	struct SljitRegularHashJoinProbeRuntimeTraits {
		SljitRegularHashJoinProbeInputKind input_kind = SljitRegularHashJoinProbeInputKind::GENERIC;

		bool UsesFlatAllValidProbe() const {
			return input_kind == SljitRegularHashJoinProbeInputKind::FLAT_ALL_VALID;
		}

		bool UsesSelectedAllValidProbe() const {
			return input_kind == SljitRegularHashJoinProbeInputKind::SELECTED_ALL_VALID;
		}
	};

	struct SljitGroupedAggregateRuntimeTraits {
		bool use_grouped_state_addresses = false;
		bool fused_payload_update_owns_group_lookup = false;

		bool NeedsGroupedStateAddressPlan() const {
			return use_grouped_state_addresses && !fused_payload_update_owns_group_lookup;
		}
	};

	struct SljitHashJoinProbeDrainState {
		idx_t input_offset = 0;
		data_ptr_t resume_row_pointer = nullptr;
		vector<uint8_t> found_match;
		bool left_initialized = false;
		bool left_unmatched_emitted = false;
		bool finished = false;
	};

	struct SljitNestedLoopJoinProbeDrainState {
		bool lhs_materialized = false;
		bool started = false;
		bool right_chunk_finished = false;
		bool finished = false;
		idx_t left_offset = 0;
		idx_t right_offset = 0;
	};

	struct SljitAggregatePayloadAdapterScratch {
		void PrepareUngrouped(idx_t payload_count) {
			source_formats.resize(payload_count);
			right_source_formats.resize(payload_count);
			source_data.assign(payload_count, nullptr);
			right_source_data.assign(payload_count, nullptr);
			source_sel.assign(payload_count, nullptr);
			right_source_sel.assign(payload_count, nullptr);
			source_validity.assign(payload_count, nullptr);
			right_source_validity.assign(payload_count, nullptr);
			aggregate_int64_values.assign(payload_count, nullptr);
			aggregate_hugeint_values.assign(payload_count, nullptr);
			aggregate_state_is_sets.assign(payload_count, nullptr);
			aggregate_row_counts.assign(payload_count, nullptr);
			constants.assign(payload_count, 0);
		}

		void PrepareExpressionSources(idx_t source_count) {
			source_formats.resize(source_count);
			source_data.assign(source_count, nullptr);
			source_sel.assign(source_count, nullptr);
			source_validity.assign(source_count, nullptr);
		}

		void PrepareFiltered(idx_t source_count, idx_t payload_count) {
			PrepareExpressionSources(source_count);
			aggregate_int64_values.assign(payload_count, nullptr);
			aggregate_hugeint_values.assign(payload_count, nullptr);
			aggregate_state_is_sets.assign(payload_count, nullptr);
			aggregate_row_counts.assign(payload_count, nullptr);
		}

		void PrepareGrouped(idx_t payload_count) {
			source_formats.resize(payload_count);
			source_data.assign(payload_count, nullptr);
			source_sel.assign(payload_count, nullptr);
			source_validity.assign(payload_count, nullptr);
		}

		void PreparePerfectHash(idx_t payload_count, idx_t group_count) {
			PrepareGrouped(payload_count);
			group_formats.resize(group_count);
			group_data.assign(group_count, nullptr);
			group_sel.assign(group_count, nullptr);
			group_validity.assign(group_count, nullptr);
		}

		vector<UnifiedVectorFormat> source_formats;
		vector<UnifiedVectorFormat> right_source_formats;
		vector<UnifiedVectorFormat> group_formats;
		vector<const_data_ptr_t> source_data;
		vector<const_data_ptr_t> right_source_data;
		vector<const_data_ptr_t> group_data;
		vector<const sel_t *> source_sel;
		vector<const sel_t *> right_source_sel;
		vector<const sel_t *> group_sel;
		vector<const validity_t *> source_validity;
		vector<const validity_t *> right_source_validity;
		vector<const validity_t *> group_validity;
		vector<int64_t *> aggregate_int64_values;
		vector<hugeint_t *> aggregate_hugeint_values;
		vector<bool *> aggregate_state_is_sets;
		vector<idx_t *> aggregate_row_counts;
		vector<int64_t> constants;
	};

	struct SljitProjectionAdapterScratch {
		void Prepare(idx_t projection_count, bool track_fused) {
			source_formats.resize(projection_count);
			right_source_formats.resize(projection_count);
			source_data.assign(projection_count, nullptr);
			right_source_data.assign(projection_count, nullptr);
			result_data.assign(projection_count, nullptr);
			overflow_messages.assign(projection_count, nullptr);
			integer_constants.assign(projection_count, 0);
			float_constants.assign(projection_count, 0);
			double_constants.assign(projection_count, 0);
			collect_floating_stats = false;
			if (track_fused) {
				fused.assign(projection_count, 0);
			} else {
				fused.clear();
			}
			prepared_input_indices.clear();
			prepared_input_data.clear();
		}

		void PrepareFloatingStats(idx_t projection_count, bool single_precision) {
			collect_floating_stats = true;
			if (single_precision) {
				float_stats_min.assign(projection_count, 0);
				float_stats_max.assign(projection_count, 0);
				double_stats_min.clear();
				double_stats_max.clear();
			} else {
				double_stats_min.assign(projection_count, 0);
				double_stats_max.assign(projection_count, 0);
				float_stats_min.clear();
				float_stats_max.clear();
			}
			direct_append_stats.assign(projection_count, DirectAppendColumnStats());
		}

		void FinishFloatingStats(const vector<SljitExecutableRegionExpression> &projections, bool single_precision) {
			if (!collect_floating_stats) {
				direct_append_stats.clear();
				return;
			}
			direct_append_stats.resize(projections.size());
			for (idx_t projection_idx = 0; projection_idx < projections.size(); projection_idx++) {
				auto &stats = direct_append_stats[projection_idx];
				stats.has_stats = true;
				if (single_precision) {
					stats.physical_type = PhysicalType::FLOAT;
					stats.float_min = float_stats_min[projection_idx];
					stats.float_max = float_stats_max[projection_idx];
				} else {
					stats.physical_type = PhysicalType::DOUBLE;
					stats.double_min = double_stats_min[projection_idx];
					stats.double_max = double_stats_max[projection_idx];
				}
			}
		}

		vector<UnifiedVectorFormat> source_formats;
		vector<UnifiedVectorFormat> right_source_formats;
		vector<const_data_ptr_t> source_data;
		vector<const_data_ptr_t> right_source_data;
		vector<data_ptr_t> result_data;
		vector<const char *> overflow_messages;
		vector<int64_t> integer_constants;
		vector<float> float_constants;
		vector<double> double_constants;
		vector<float> float_stats_min;
		vector<float> float_stats_max;
		vector<double> double_stats_min;
		vector<double> double_stats_max;
		vector<DirectAppendColumnStats> direct_append_stats;
		vector<uint8_t> fused;
		vector<idx_t> prepared_input_indices;
		vector<const_data_ptr_t> prepared_input_data;
		bool collect_floating_stats = false;
	};

	struct SljitExpressionAdapterScratch {
		void PrepareExpressionTree(DataChunk &input, const SljitExecutableRegionExpression &expr,
		                           SljitNativeVectorInput &native_input, const SelectionVector *execute_sel,
		                           idx_t count) {
			auto &plan = expr.plan;
			auto &source_indices =
			    expr.input_source_indices.empty() ? plan.expression_tree_source_indices : expr.input_source_indices;
			formats.resize(source_indices.size());
			source_data.resize(source_indices.size());
			source_sel.resize(source_indices.size());
			source_validity.resize(source_indices.size());
			source_can_have_null = false;
			flat_no_selection = execute_sel == nullptr;
			flat_all_valid = execute_sel == nullptr;
			all_valid = true;
			for (idx_t source_idx = 0; source_idx < source_indices.size(); source_idx++) {
				auto input_index = source_indices[source_idx];
				if (input_index >= input.ColumnCount()) {
					throw InternalException("SLJIT expression-tree source is out of range");
				}
				input.data[input_index].ToUnifiedFormat(formats[source_idx]);
				source_data[source_idx] =
				    plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE
				        ? NativeIntegerSourceData(formats[source_idx], SljitNativeIntegerKind::DECIMAL64)
				        : SljitTypedExpressionTreeSourceData(formats[source_idx], input.data[input_index].GetType());
				source_sel[source_idx] = SljitNormalizedSourceSelectionData(formats[source_idx]);
				source_validity[source_idx] =
				    SljitNormalizedSourceValidityData(formats[source_idx], source_sel[source_idx], execute_sel, count);
				source_can_have_null = source_can_have_null || source_validity[source_idx] != nullptr;
				flat_no_selection = flat_no_selection && source_sel[source_idx] == nullptr;
				flat_all_valid =
				    flat_all_valid && source_sel[source_idx] == nullptr && source_validity[source_idx] == nullptr;
				all_valid = all_valid && source_validity[source_idx] == nullptr;
			}
			native_input.source_data_array = source_data.data();
			native_input.source_sel_array = source_sel.data();
			native_input.source_validity_array = source_validity.data();
			native_input.expression_tree_flat_no_selection = flat_no_selection;
			native_input.expression_tree_flat_all_valid = flat_all_valid;
			native_input.expression_tree_all_valid = all_valid;
		}

		SljitNativePredicateSourceAdapter predicate_sources;
		vector<UnifiedVectorFormat> formats;
		vector<const_data_ptr_t> source_data;
		vector<const sel_t *> source_sel;
		vector<const validity_t *> source_validity;
		bool source_can_have_null = false;
		bool flat_no_selection = false;
		bool flat_all_valid = false;
		bool all_valid = false;
	};

	struct SljitPreaggregatedPrimitivePayloadDeltas {
		AggregatePrimitiveUpdateKind kind = AggregatePrimitiveUpdateKind::NONE;
		vector<int64_t> int64_values;
		vector<hugeint_t> hugeint_values;
	};

	struct SljitPreaggregatedPrimitiveAggregateScratch {
		vector<SljitPreaggregatedPrimitivePayloadDeltas> payloads;

		void Prepare(const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, idx_t capacity) {
			payloads.resize(lanes.size());
			for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
				auto &payload = payloads[payload_idx];
				auto lane = lanes[payload_idx];
				payload.kind = lane ? lane->kind : AggregatePrimitiveUpdateKind::NONE;
				payload.int64_values.clear();
				payload.hugeint_values.clear();
				switch (payload.kind) {
				case AggregatePrimitiveUpdateKind::COUNT_STAR:
				case AggregatePrimitiveUpdateKind::SUM_INT64:
					payload.int64_values.reserve(capacity);
					break;
				case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
					payload.hugeint_values.reserve(capacity);
					break;
				default:
					break;
				}
			}
		}
	};

	static bool SlicePreaggregatedPrimitiveScratch(const SljitPreaggregatedPrimitiveAggregateScratch &source,
	                                               const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                               idx_t offset, idx_t count,
	                                               SljitPreaggregatedPrimitiveAggregateScratch &target) {
		if (source.payloads.size() != lanes.size()) {
			return false;
		}
		target.Prepare(lanes, count);
		for (idx_t payload_idx = 0; payload_idx < source.payloads.size(); payload_idx++) {
			auto &source_payload = source.payloads[payload_idx];
			auto &target_payload = target.payloads[payload_idx];
			if (source_payload.kind != target_payload.kind) {
				return false;
			}
			switch (source_payload.kind) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR:
			case AggregatePrimitiveUpdateKind::SUM_INT64:
				if (source_payload.int64_values.size() < offset + count) {
					return false;
				}
				target_payload.int64_values.insert(
				    target_payload.int64_values.end(),
				    source_payload.int64_values.begin() + UnsafeNumericCast<int64_t>(offset),
				    source_payload.int64_values.begin() + UnsafeNumericCast<int64_t>(offset + count));
				break;
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
				if (source_payload.hugeint_values.size() < offset + count) {
					return false;
				}
				target_payload.hugeint_values.insert(
				    target_payload.hugeint_values.end(),
				    source_payload.hugeint_values.begin() + UnsafeNumericCast<int64_t>(offset),
				    source_payload.hugeint_values.begin() + UnsafeNumericCast<int64_t>(offset + count));
				break;
			default:
				return false;
			}
		}
		return true;
	}

	struct SljitRegionExecutionScratch {
		SljitRegionExecutionScratch(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops) {
			temporary_chunks.resize(ops.size());
			filter_selections.resize(ops.size());
			operator_bindings.resize(ops.size());
			operator_binding_ready.resize(ops.size());
			sink_bindings.resize(ops.size());
			sink_binding_ready.resize(ops.size());
			hash_join_build_selections.resize(ops.size());
			hash_join_row_pointers.resize(ops.size());
			hash_join_residual_chunks.resize(ops.size());
			hash_join_residual_selections.resize(ops.size());
			hash_join_residual_match_selections.resize(ops.size());
			hash_join_residual_row_pointers.resize(ops.size());
			hash_join_source_formats.resize(ops.size());
			hash_join_source_data.resize(ops.size());
			hash_join_source_sel.resize(ops.size());
			hash_join_source_validity.resize(ops.size());
			hash_join_build_source_chunks.resize(ops.size());
			hash_join_build_hash_values.resize(ops.size());
			nested_loop_left_condition_chunks.resize(ops.size());
			nested_loop_left_selections.resize(ops.size());
			nested_loop_right_selections.resize(ops.size());
			nested_loop_condition_chunks.resize(ops.size());
			order_key_chunks.resize(ops.size());
			order_payload_chunks.resize(ops.size());
			aggregate_state_addresses.resize(ops.size());
			aggregate_preaggregated_groups.resize(ops.size());
			aggregate_preaggregate_scratch.resize(ops.size());
			aggregate_payload_scratch.resize(ops.size());
			aggregate_payload_lanes.resize(ops.size());
			aggregate_payload_lanes_ready.resize(ops.size());
			aggregate_direct_existing_disabled.resize(ops.size());
			aggregate_direct_existing_misses.resize(ops.size());
			aggregate_direct_new_disabled.resize(ops.size());
			aggregate_direct_new_misses.resize(ops.size());
			aggregate_direct_append_new_disabled.resize(ops.size());
			aggregate_direct_append_new_misses.resize(ops.size());
			projection_adapter_scratch.resize(ops.size());
			expression_adapter_scratch.resize(ops.size());
			for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
				auto &op = ops[op_idx];
				if (op.kind == SljitNativeRegionOpKind::FILTER || op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
					filter_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
				}
				InitializeExpressionAdapterScratch(op_idx, op);
				if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
					hash_join_build_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
					hash_join_row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
					auto key_count = op.hash_join_probe.plan.keys.size();
					hash_join_source_formats[op_idx].resize(key_count);
					hash_join_source_data[op_idx].resize(key_count);
					hash_join_source_sel[op_idx].resize(key_count);
					hash_join_source_validity[op_idx].resize(key_count);
					if (op.hash_join_probe.plan.residual_predicate) {
						hash_join_residual_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
						hash_join_residual_match_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
						hash_join_residual_row_pointers[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
						auto residual_chunk = make_uniq<DataChunk>();
						residual_chunk->Initialize(allocator, op.hash_join_probe.plan.residual_source_types);
						hash_join_residual_chunks[op_idx] = std::move(residual_chunk);
					}
				}
				if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
					hash_join_build_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
					hash_join_build_source_chunks[op_idx] = make_uniq<DataChunk>();
					hash_join_build_hash_values[op_idx] = make_uniq<Vector>(LogicalType::HASH);
				}
				if (op.kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE) {
					nested_loop_left_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
					nested_loop_right_selections[op_idx] = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
					auto condition_chunk = make_uniq<DataChunk>();
					condition_chunk->Initialize(allocator, op.nested_loop_join_probe.plan.condition_types);
					nested_loop_left_condition_chunks[op_idx] = std::move(condition_chunk);
				}
				if (op.kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD) {
					auto condition_chunk = make_uniq<DataChunk>();
					condition_chunk->Initialize(allocator, op.nested_loop_join_build.plan.condition_types);
					nested_loop_condition_chunks[op_idx] = std::move(condition_chunk);
				}
				if (op.kind == SljitNativeRegionOpKind::ORDER_SINK) {
					auto order_key_chunk = make_uniq<DataChunk>();
					order_key_chunk->Initialize(allocator, op.order_sink.plan.key_types);
					order_key_chunks[op_idx] = std::move(order_key_chunk);
					auto order_payload_chunk = make_uniq<DataChunk>();
					order_payload_chunk->Initialize(allocator, op.order_sink.plan.input_types);
					order_payload_chunks[op_idx] = std::move(order_payload_chunk);
				}
				if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
				    op.aggregate_update.plan.use_grouped_state_addresses) {
					aggregate_state_addresses[op_idx] = make_uniq<Vector>(LogicalType::POINTER);
					auto &groups = op.aggregate_update.plan.sink_info.groups;
					if (!groups.empty()) {
						vector<LogicalType> group_types;
						group_types.reserve(groups.size());
						for (auto &group : groups) {
							group_types.push_back(group.type);
						}
						auto compact_groups = make_uniq<DataChunk>();
						compact_groups->Initialize(allocator, group_types);
						aggregate_preaggregated_groups[op_idx] = std::move(compact_groups);
					}
				}
				if (OpIsSink(op.kind)) {
					continue;
				}
				if (CanFuseFilterProjection(ops, op_idx)) {
					InitializeTemporaryChunk(allocator, ops, op_idx + 1);
					continue;
				}
				InitializeTemporaryChunk(allocator, ops, op_idx);
			}
		}

		DataChunk &TemporaryChunk(idx_t op_idx) {
			if (op_idx >= temporary_chunks.size() || !temporary_chunks[op_idx]) {
				throw InternalException("SLJIT full pipeline transform has no stage scratch chunk");
			}
			return *temporary_chunks[op_idx];
		}

		Vector &AggregateStateAddresses(idx_t op_idx) {
			if (op_idx >= aggregate_state_addresses.size() || !aggregate_state_addresses[op_idx]) {
				throw InternalException("SLJIT aggregate update has no grouped state-address scratch");
			}
			return *aggregate_state_addresses[op_idx];
		}

		SljitAggregatePayloadAdapterScratch &AggregatePayloadScratch(idx_t op_idx) {
			if (op_idx >= aggregate_payload_scratch.size()) {
				throw InternalException("SLJIT aggregate update has no payload-adapter scratch");
			}
			return aggregate_payload_scratch[op_idx];
		}

		DataChunk &AggregatePreaggregatedGroups(idx_t op_idx) {
			if (op_idx >= aggregate_preaggregated_groups.size() || !aggregate_preaggregated_groups[op_idx]) {
				throw InternalException("SLJIT aggregate update has no preaggregated group scratch");
			}
			return *aggregate_preaggregated_groups[op_idx];
		}

		SljitPreaggregatedPrimitiveAggregateScratch &AggregatePreaggregateScratch(idx_t op_idx) {
			if (op_idx >= aggregate_preaggregate_scratch.size()) {
				throw InternalException("SLJIT aggregate update has no preaggregate scratch");
			}
			return aggregate_preaggregate_scratch[op_idx];
		}

		const vector<const ExecutionPrimitiveAggregateUpdateLane *> &
		AggregatePayloadLanes(idx_t op_idx, const vector<ExecutionRegionAggregateInput> &aggregates,
		                      const ExecutionPrimitiveAggregateUpdateBinding &primitive) {
			if (op_idx >= aggregate_payload_lanes.size() || op_idx >= aggregate_payload_lanes_ready.size()) {
				throw InternalException("SLJIT aggregate update has no payload-lane scratch");
			}
			if (aggregate_payload_lanes_ready[op_idx]) {
				return aggregate_payload_lanes[op_idx];
			}
			auto &lanes = aggregate_payload_lanes[op_idx];
			lanes.assign(aggregates.size(), nullptr);
			for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
				auto &aggregate = aggregates[payload_idx];
				auto aggregate_index = aggregate.aggregate_index;
				auto lane = primitive.FindLane(aggregate_index);
				if (!lane) {
					throw InternalException("SLJIT aggregate primitive lane missing for aggregate %llu",
					                        static_cast<unsigned long long>(aggregate_index));
				}
				lanes[payload_idx] = lane;
			}
			aggregate_payload_lanes_ready[op_idx] = true;
			return lanes;
		}

		bool DirectExistingAggregateUpdateDisabled(idx_t op_idx) const {
			return op_idx >= aggregate_direct_existing_disabled.size() || aggregate_direct_existing_disabled[op_idx];
		}

		void RecordDirectExistingAggregateUpdateResult(idx_t op_idx, bool updated) {
			if (op_idx >= aggregate_direct_existing_disabled.size() ||
			    op_idx >= aggregate_direct_existing_misses.size()) {
				throw InternalException("SLJIT aggregate update has no direct-existing scratch");
			}
			if (updated) {
				aggregate_direct_existing_misses[op_idx] = 0;
				return;
			}
			auto misses = ++aggregate_direct_existing_misses[op_idx];
			if (misses >= 2) {
				aggregate_direct_existing_disabled[op_idx] = true;
			}
		}

		bool DirectNewAggregateUpdateDisabled(idx_t op_idx) const {
			return op_idx >= aggregate_direct_new_disabled.size() || aggregate_direct_new_disabled[op_idx];
		}

		void RecordDirectNewAggregateUpdateResult(idx_t op_idx, bool updated) {
			static constexpr idx_t DIRECT_NEW_AGGREGATE_UPDATE_MISS_LIMIT = 8;
			if (op_idx >= aggregate_direct_new_disabled.size() || op_idx >= aggregate_direct_new_misses.size()) {
				throw InternalException("SLJIT aggregate update has no direct-new scratch");
			}
			if (updated) {
				aggregate_direct_new_misses[op_idx] = 0;
				return;
			}
			auto misses = ++aggregate_direct_new_misses[op_idx];
			if (misses >= DIRECT_NEW_AGGREGATE_UPDATE_MISS_LIMIT) {
				aggregate_direct_new_disabled[op_idx] = true;
			}
		}

		bool DirectAppendNewAggregateUpdateDisabled(idx_t op_idx) const {
			return op_idx >= aggregate_direct_append_new_disabled.size() ||
			       aggregate_direct_append_new_disabled[op_idx];
		}

		void RecordDirectAppendNewAggregateUpdateResult(idx_t op_idx, bool updated) {
			static constexpr idx_t DIRECT_APPEND_NEW_AGGREGATE_UPDATE_MISS_LIMIT = 2;
			if (op_idx >= aggregate_direct_append_new_disabled.size() ||
			    op_idx >= aggregate_direct_append_new_misses.size()) {
				throw InternalException("SLJIT aggregate update has no direct-append-new scratch");
			}
			if (updated) {
				aggregate_direct_append_new_misses[op_idx] = 0;
				return;
			}
			auto misses = ++aggregate_direct_append_new_misses[op_idx];
			if (misses >= DIRECT_APPEND_NEW_AGGREGATE_UPDATE_MISS_LIMIT) {
				aggregate_direct_append_new_disabled[op_idx] = true;
			}
		}

		SljitExpressionAdapterScratch &ExpressionAdapterScratch(idx_t op_idx, idx_t expression_idx) {
			if (op_idx >= expression_adapter_scratch.size() ||
			    expression_idx >= expression_adapter_scratch[op_idx].size()) {
				throw InternalException("SLJIT expression has no adapter scratch");
			}
			return expression_adapter_scratch[op_idx][expression_idx];
		}

		SljitProjectionAdapterScratch &ProjectionScratch(idx_t op_idx) {
			if (op_idx >= projection_adapter_scratch.size()) {
				throw InternalException("SLJIT projection has no adapter scratch");
			}
			return projection_adapter_scratch[op_idx];
		}

		SelectionVector &FilterSelection(idx_t op_idx) {
			if (op_idx >= filter_selections.size() || !filter_selections[op_idx]) {
				throw InternalException("SLJIT full pipeline transform has no selection scratch");
			}
			return *filter_selections[op_idx];
		}

		bool HasSinkBinding(idx_t op_idx) const {
			return op_idx < sink_binding_ready.size() && sink_binding_ready[op_idx];
		}

		ExecutionSinkBinding &SinkBinding(idx_t op_idx) {
			if (op_idx >= sink_bindings.size()) {
				throw InternalException("SLJIT full pipeline sink has no binding scratch");
			}
			return sink_bindings[op_idx];
		}

		void MarkSinkBindingReady(idx_t op_idx) {
			if (op_idx >= sink_binding_ready.size()) {
				throw InternalException("SLJIT full pipeline sink has no binding-ready scratch");
			}
			sink_binding_ready[op_idx] = true;
		}

		bool HasOperatorBinding(idx_t op_idx) const {
			return op_idx < operator_binding_ready.size() && operator_binding_ready[op_idx];
		}

		ExecutionOperatorBinding &OperatorBinding(idx_t op_idx) {
			if (op_idx >= operator_bindings.size()) {
				throw InternalException("SLJIT full pipeline operator has no binding scratch");
			}
			return operator_bindings[op_idx];
		}

		void MarkOperatorBindingReady(idx_t op_idx) {
			if (op_idx >= operator_binding_ready.size()) {
				throw InternalException("SLJIT full pipeline operator has no binding-ready scratch");
			}
			operator_binding_ready[op_idx] = true;
		}

		Vector &HashJoinRowPointers(idx_t op_idx) {
			if (op_idx >= hash_join_row_pointers.size() || !hash_join_row_pointers[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no row-pointer scratch");
			}
			return *hash_join_row_pointers[op_idx];
		}

		SelectionVector &HashJoinBuildSelection(idx_t op_idx) {
			if (op_idx >= hash_join_build_selections.size() || !hash_join_build_selections[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no build-selection scratch");
			}
			return *hash_join_build_selections[op_idx];
		}

		DataChunk &HashJoinBuildSourceChunk(idx_t op_idx) {
			if (op_idx >= hash_join_build_source_chunks.size() || !hash_join_build_source_chunks[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join build has no source-chunk scratch");
			}
			return *hash_join_build_source_chunks[op_idx];
		}

		Vector &HashJoinBuildHashValues(idx_t op_idx) {
			if (op_idx >= hash_join_build_hash_values.size() || !hash_join_build_hash_values[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join build has no hash-value scratch");
			}
			return *hash_join_build_hash_values[op_idx];
		}

		DataChunk &HashJoinResidualChunk(idx_t op_idx) {
			if (op_idx >= hash_join_residual_chunks.size() || !hash_join_residual_chunks[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no residual chunk scratch");
			}
			return *hash_join_residual_chunks[op_idx];
		}

		SelectionVector &HashJoinResidualSelection(idx_t op_idx) {
			if (op_idx >= hash_join_residual_selections.size() || !hash_join_residual_selections[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no residual selection scratch");
			}
			return *hash_join_residual_selections[op_idx];
		}

		SelectionVector &HashJoinResidualMatchSelection(idx_t op_idx) {
			if (op_idx >= hash_join_residual_match_selections.size() || !hash_join_residual_match_selections[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no residual match selection scratch");
			}
			return *hash_join_residual_match_selections[op_idx];
		}

		Vector &HashJoinResidualRowPointers(idx_t op_idx) {
			if (op_idx >= hash_join_residual_row_pointers.size() || !hash_join_residual_row_pointers[op_idx]) {
				throw InternalException("SLJIT full pipeline hash join probe has no residual row-pointer scratch");
			}
			return *hash_join_residual_row_pointers[op_idx];
		}

		vector<UnifiedVectorFormat> &HashJoinSourceFormats(idx_t op_idx) {
			if (op_idx >= hash_join_source_formats.size()) {
				throw InternalException("SLJIT hash join probe has no source-format scratch slot");
			}
			return hash_join_source_formats[op_idx];
		}

		vector<const_data_ptr_t> &HashJoinSourceData(idx_t op_idx) {
			if (op_idx >= hash_join_source_data.size()) {
				throw InternalException("SLJIT hash join probe has no source-data scratch slot");
			}
			return hash_join_source_data[op_idx];
		}

		vector<const sel_t *> &HashJoinSourceSelections(idx_t op_idx) {
			if (op_idx >= hash_join_source_sel.size()) {
				throw InternalException("SLJIT hash join probe has no source-selection scratch slot");
			}
			return hash_join_source_sel[op_idx];
		}

		vector<const validity_t *> &HashJoinSourceValidity(idx_t op_idx) {
			if (op_idx >= hash_join_source_validity.size()) {
				throw InternalException("SLJIT hash join probe has no source-validity scratch slot");
			}
			return hash_join_source_validity[op_idx];
		}

		DataChunk &NestedLoopLeftConditionChunk(idx_t op_idx) {
			if (op_idx >= nested_loop_left_condition_chunks.size() || !nested_loop_left_condition_chunks[op_idx]) {
				throw InternalException("SLJIT nested loop join probe has no left condition scratch chunk");
			}
			return *nested_loop_left_condition_chunks[op_idx];
		}

		SelectionVector &NestedLoopLeftSelection(idx_t op_idx) {
			if (op_idx >= nested_loop_left_selections.size() || !nested_loop_left_selections[op_idx]) {
				throw InternalException("SLJIT nested loop join probe has no left selection scratch");
			}
			return *nested_loop_left_selections[op_idx];
		}

		SelectionVector &NestedLoopRightSelection(idx_t op_idx) {
			if (op_idx >= nested_loop_right_selections.size() || !nested_loop_right_selections[op_idx]) {
				throw InternalException("SLJIT nested loop join probe has no right selection scratch");
			}
			return *nested_loop_right_selections[op_idx];
		}

		DataChunk &NestedLoopConditionChunk(idx_t op_idx) {
			if (op_idx >= nested_loop_condition_chunks.size() || !nested_loop_condition_chunks[op_idx]) {
				throw InternalException("SLJIT nested loop join build has no condition scratch chunk");
			}
			return *nested_loop_condition_chunks[op_idx];
		}

		DataChunk &OrderKeyChunk(idx_t op_idx) {
			if (op_idx >= order_key_chunks.size() || !order_key_chunks[op_idx]) {
				throw InternalException("SLJIT ordered sink has no order-key scratch chunk");
			}
			return *order_key_chunks[op_idx];
		}

		DataChunk &OrderPayloadChunk(idx_t op_idx) {
			if (op_idx >= order_payload_chunks.size() || !order_payload_chunks[op_idx]) {
				throw InternalException("SLJIT ordered sink has no payload scratch chunk");
			}
			return *order_payload_chunks[op_idx];
		}

		vector<unique_ptr<DataChunk>> temporary_chunks;
		vector<unique_ptr<SelectionVector>> filter_selections;
		vector<ExecutionOperatorBinding> operator_bindings;
		vector<bool> operator_binding_ready;
		vector<ExecutionSinkBinding> sink_bindings;
		vector<bool> sink_binding_ready;
		vector<unique_ptr<SelectionVector>> hash_join_build_selections;
		vector<unique_ptr<Vector>> hash_join_row_pointers;
		vector<unique_ptr<DataChunk>> hash_join_build_source_chunks;
		vector<unique_ptr<Vector>> hash_join_build_hash_values;
		vector<unique_ptr<DataChunk>> hash_join_residual_chunks;
		vector<unique_ptr<SelectionVector>> hash_join_residual_selections;
		vector<unique_ptr<SelectionVector>> hash_join_residual_match_selections;
		vector<unique_ptr<Vector>> hash_join_residual_row_pointers;
		vector<vector<UnifiedVectorFormat>> hash_join_source_formats;
		vector<vector<const_data_ptr_t>> hash_join_source_data;
		vector<vector<const sel_t *>> hash_join_source_sel;
		vector<vector<const validity_t *>> hash_join_source_validity;
		vector<unique_ptr<DataChunk>> nested_loop_left_condition_chunks;
		vector<unique_ptr<SelectionVector>> nested_loop_left_selections;
		vector<unique_ptr<SelectionVector>> nested_loop_right_selections;
		vector<unique_ptr<DataChunk>> nested_loop_condition_chunks;
		vector<unique_ptr<DataChunk>> order_key_chunks;
		vector<unique_ptr<DataChunk>> order_payload_chunks;
		vector<unique_ptr<Vector>> aggregate_state_addresses;
		vector<unique_ptr<DataChunk>> aggregate_preaggregated_groups;
		vector<SljitPreaggregatedPrimitiveAggregateScratch> aggregate_preaggregate_scratch;
		vector<SljitAggregatePayloadAdapterScratch> aggregate_payload_scratch;
		vector<vector<const ExecutionPrimitiveAggregateUpdateLane *>> aggregate_payload_lanes;
		vector<bool> aggregate_payload_lanes_ready;
		vector<bool> aggregate_direct_existing_disabled;
		vector<idx_t> aggregate_direct_existing_misses;
		vector<bool> aggregate_direct_new_disabled;
		vector<idx_t> aggregate_direct_new_misses;
		vector<bool> aggregate_direct_append_new_disabled;
		vector<idx_t> aggregate_direct_append_new_misses;
		vector<SljitProjectionAdapterScratch> projection_adapter_scratch;
		vector<vector<SljitExpressionAdapterScratch>> expression_adapter_scratch;
		DirectAppendReservation direct_append_reservation;

	private:
		void InitializeExpressionAdapterScratch(idx_t op_idx, const SljitExecutableRegionOp &op) {
			idx_t expression_count = 0;
			switch (op.kind) {
			case SljitNativeRegionOpKind::FILTER:
				expression_count = 1;
				break;
			case SljitNativeRegionOpKind::PROJECTION:
				expression_count = op.projections.size();
				break;
			case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
				expression_count = op.hash_join_probe.plan.residual_predicate ? 1 : 0;
				break;
			case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE:
				expression_count = op.nested_loop_join_probe.lhs_conditions.size();
				break;
			case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
				expression_count = op.nested_loop_join_build.rhs_conditions.size();
				break;
			case SljitNativeRegionOpKind::ORDER_SINK:
				expression_count = op.order_sink.order_keys.size();
				break;
			case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
				expression_count = op.aggregate_update.payloads.size();
				break;
			default:
				break;
			}
			expression_adapter_scratch[op_idx].resize(expression_count);
		}

		static bool OpIsSink(SljitNativeRegionOpKind kind) {
			switch (kind) {
			case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD:
			case SljitNativeRegionOpKind::ORDER_SINK:
			case SljitNativeRegionOpKind::APPEND_SINK:
			case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
			case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
				return true;
			default:
				return false;
			}
		}

		static bool CanFuseFilterProjection(const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
			return op_idx + 1 < ops.size() && ops[op_idx].kind == SljitNativeRegionOpKind::FILTER &&
			       ops[op_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION;
		}

		void InitializeTemporaryChunk(Allocator &allocator, const vector<SljitExecutableRegionOp> &ops, idx_t op_idx) {
			if (op_idx >= ops.size() || temporary_chunks[op_idx]) {
				return;
			}
			auto chunk = make_uniq<DataChunk>();
			chunk->Initialize(allocator, ops[op_idx].output_types);
			temporary_chunks[op_idx] = std::move(chunk);
		}
	};

public:
	SljitNativeRegionKernel(string backend_name_p, vector<SljitExecutableRegionOp> ops_p,
	                        vector<idx_t> source_distinct_counts_p, vector<Value> source_min_values_p,
	                        vector<Value> source_max_values_p, ExecutionRegionABI abi_p)
	    : backend_name(std::move(backend_name_p)), ops(std::move(ops_p)),
	      source_distinct_counts(std::move(source_distinct_counts_p)),
	      source_min_values(std::move(source_min_values_p)), source_max_values(std::move(source_max_values_p)),
	      abi(abi_p) {
	}

	const string &BackendName() const override {
		return backend_name;
	}

	idx_t CodeSize() const override {
		idx_t result = 0;
		for (auto &op : ops) {
			result += op.CodeSize();
		}
		return result;
	}

	bool HasExecutableBody() const override {
		for (auto &op : ops) {
			if (op.HasExecutableBody()) {
				return true;
			}
		}
		return false;
	}

	bool CanExecuteFullPipeline() const override {
		return ExecutionRegionABIIsFullPipeline(abi);
	}

	void RefreshTraceCodeSize() {
		SetTraceInfo(TraceId(), ExecutionMode(), TraceCompileReason(), TraceCompileTime(), CodeSize());
	}

	void RecordLazyHashJoinProbeCodegen(ExecutionRegionRuntime &runtime, const SljitLazyCodegenTiming &timing,
	                                    idx_t code_size) {
		ExecutionRegionLazyCodegenMetrics metrics;
		metrics.codegen_time_us = timing.codegen_time_us;
		metrics.machine_codegen_time_us = timing.machine_codegen_time_us;
		metrics.code_size = code_size;
		runtime.RecordLazyCodegen(metrics);
		RefreshTraceCodeSize();
	}

	void EnsurePerfectHashJoinProbeCode(ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe) {
		if (probe.perfect_function) {
			return;
		}
		lock_guard<mutex> guard(codegen_lock);
		if (probe.perfect_function) {
			return;
		}
		if (probe.plan.keys.empty()) {
			throw InternalException("SLJIT native perfect hash join probe has no key plan");
		}
		string error;
		auto timing = TimeSljitLazyCodegen([&]() {
			probe.perfect_code = BuildSljitPerfectHashJoinProbe(probe.plan.keys[0], probe.plan.output_mode,
			                                                    probe.perfect_function, error);
		});
		if (!probe.perfect_code || !probe.perfect_function) {
			throw InternalException("SLJIT native perfect hash join probe lazy code generation failed: %s",
			                        error.empty() ? "unknown error" : error);
		}
		RecordLazyHashJoinProbeCodegen(runtime, timing, probe.perfect_code->CodeSize());
	}

	void EnsureRegularHashJoinProbeCode(ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe) {
		if (probe.function) {
			return;
		}
		lock_guard<mutex> guard(codegen_lock);
		if (probe.function) {
			return;
		}
		string error;
		auto timing = TimeSljitLazyCodegen([&]() {
			probe.code =
			    BuildSljitHashJoinProbe(probe.plan.keys, probe.plan.equality_key_count, probe.plan.mark_build_match,
			                            probe.plan.found_match_offset, probe.plan.pointer_offset,
			                            probe.plan.output_mode, probe.function, error);
		});
		if (!probe.code || !probe.function) {
			throw InternalException("SLJIT native hash join probe lazy code generation failed: %s",
			                        error.empty() ? "unknown error" : error);
		}
		RecordLazyHashJoinProbeCodegen(runtime, timing, probe.code->CodeSize());
	}

	SljitNativeHashJoinProbeFunction
	EnsureFlatAllValidRegularHashJoinProbeCode(ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &probe,
	                                           bool use_salt, bool chains_longer_than_one, bool dictionary_emission) {
		auto &variant = probe.FlatAllValidVariantFor(use_salt, chains_longer_than_one, dictionary_emission);
		if (variant.function) {
			return variant.function;
		}
		lock_guard<mutex> guard(codegen_lock);
		if (variant.function) {
			return variant.function;
		}
		string error;
		SljitLazyCodegenTiming timing;
		timing = TimeSljitLazyCodegen([&]() {
			variant.code = BuildSljitHashJoinProbe(
			    probe.plan.keys, probe.plan.equality_key_count, probe.plan.mark_build_match,
			    probe.plan.found_match_offset, probe.plan.pointer_offset, probe.plan.output_mode, variant.function,
			    error, true, true, use_salt, true, chains_longer_than_one, dictionary_emission);
		});
		if (!variant.code || !variant.function) {
			throw InternalException("SLJIT native flat all-valid hash join probe codegen failed: %s", error);
		}
		RecordLazyHashJoinProbeCodegen(runtime, timing, variant.code->CodeSize());
		return variant.function;
	}

	SljitNativeHashJoinProbeFunction EnsureSelectedAllValidRegularHashJoinProbeCode(ExecutionRegionRuntime &runtime,
	                                                                                SljitExecutableHashJoinProbe &probe,
	                                                                                bool use_salt,
	                                                                                bool chains_longer_than_one,
	                                                                                bool dictionary_emission) {
		auto &variant = probe.SelectedAllValidVariantFor(use_salt, chains_longer_than_one, dictionary_emission);
		if (variant.function) {
			return variant.function;
		}
		lock_guard<mutex> guard(codegen_lock);
		if (variant.function) {
			return variant.function;
		}
		string error;
		SljitLazyCodegenTiming timing;
		timing = TimeSljitLazyCodegen([&]() {
			variant.code = BuildSljitHashJoinProbe(
			    probe.plan.keys, probe.plan.equality_key_count, probe.plan.mark_build_match,
			    probe.plan.found_match_offset, probe.plan.pointer_offset, probe.plan.output_mode, variant.function,
			    error, false, true, use_salt, true, chains_longer_than_one, dictionary_emission, true);
		});
		if (!variant.code || !variant.function) {
			throw InternalException("SLJIT native selected all-valid hash join probe codegen failed: %s", error);
		}
		RecordLazyHashJoinProbeCodegen(runtime, timing, variant.code->CodeSize());
		return variant.function;
	}

	bool CanBatchFilteredSourceFullPipeline() const {
		return UsesScanFilters() && ops.size() == 2 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[1].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	}

	bool CanBatchGeneratedFilterProjectionFullPipeline() const {
		if (ops.size() < 3 || ops[0].kind != SljitNativeRegionOpKind::FILTER ||
		    ops[1].kind != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
		if (ops.size() == 3) {
			return ops[2].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
			       ops[2].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
			       ProjectionOutputsAreFixedWidth(ops[1]);
		}
		return ops.size() == 4 && ops[2].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[3].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[3].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	}

	bool CanBatchProjectionCountStarGroupedAggregateFullPipeline() const {
		if (ops.size() != 2 || ops[0].kind != SljitNativeRegionOpKind::PROJECTION ||
		    ops[1].kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE || ops[0].output_types.size() != 1 ||
		    !ProjectionOutputsAreFixedWidth(ops[0])) {
			return false;
		}
		auto &aggregate_update = ops[1].aggregate_update.plan;
		auto &sink_info = aggregate_update.sink_info;
		if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    !aggregate_update.use_primitive_payloads || !aggregate_update.use_grouped_state_addresses ||
		    aggregate_update.use_perfect_hash_group_lookup || sink_info.groups.size() != 1 ||
		    sink_info.groups[0].input_index != 0 || sink_info.groups[0].type != ops[0].output_types[0] ||
		    sink_info.aggregates.size() != 1 || ops[1].aggregate_update.payloads.size() != 1) {
			return false;
		}
		auto &aggregate = sink_info.aggregates[0];
		return aggregate.child_count == 0 &&
		       aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR;
	}

	bool CanBatchHashJoinProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 3 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[2].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[2].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
	}

	bool CanBatchHashJoinDelimJoinSinkFullPipeline() const {
		return ops.size() == 2 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::DELIM_JOIN_SINK;
	}

	bool CanBatchHashJoinBuildSinkFullPipeline() const {
		if (ops.size() < 2 || ops.back().kind != SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
			return false;
		}
		for (idx_t op_idx = 0; op_idx + 1 < ops.size(); op_idx++) {
			if (ops[op_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
				return true;
			}
		}
		return false;
	}

	bool CanBatchGeneratedFilterProjectionHashJoinBuildSinkFullPipeline() const {
		if (ops.size() != 4 && ops.size() != 5) {
			return false;
		}
		if (ops[0].kind != SljitNativeRegionOpKind::FILTER || ops[1].kind != SljitNativeRegionOpKind::PROJECTION ||
		    ops[2].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE ||
		    ops.back().kind != SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
			return false;
		}
		return ops.size() == 4 || ops[3].kind == SljitNativeRegionOpKind::PROJECTION;
	}

	bool CanBatchHashJoinProjectionProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 4 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[2].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[3].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[3].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		       ProjectionOutputsAreFixedWidth(ops[2]);
	}

	bool CanBatchMarkHashJoinFilterProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 4 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::FILTER && ops[2].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[3].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[0].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
		       ops[3].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		       ProjectionOutputsAreFixedWidth(ops[2]) && IsMarkProbeMarkerFilter(ops[0], ops[1]);
	}

	bool CanBatchHashJoinHashJoinProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 4 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[2].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[3].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[3].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		       ops[0].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		       ops[1].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		       ProjectionOutputsAreFixedWidth(ops[2]);
	}

	bool CanBatchProjectionHashJoinProjectionProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 5 && ops[0].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[1].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[2].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[3].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[4].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[4].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		       ProjectionOutputsAreFixedWidth(ops[3]);
	}

	bool CanBatchHashJoinProjectionHashJoinProjectionsGroupedAggregateFullPipeline() const {
		if (ops.size() < 6 || ops[0].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE ||
		    ops[1].kind != SljitNativeRegionOpKind::PROJECTION ||
		    ops[2].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE ||
		    ops.back().kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
		    ops.back().aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    ops[0].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
		    ops[2].hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
			return false;
		}
		for (idx_t op_idx = 3; op_idx + 1 < ops.size(); op_idx++) {
			if (ops[op_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
				return false;
			}
		}
		return ProjectionOutputsAreFixedWidth(ops[ops.size() - 2]);
	}

	bool OutputTypesAreFixedWidth(const vector<LogicalType> &types) const {
		for (auto &type : types) {
			if (!TypeIsConstantSize(type.InternalType())) {
				return false;
			}
		}
		return true;
	}

	bool CanBatchProjectionHashJoinProjectionHashJoinProjectionProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 7 && ops[0].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[1].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[2].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[3].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[4].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[5].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[6].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[6].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		       OutputTypesAreFixedWidth(ops[1].output_types) && ProjectionOutputsAreFixedWidth(ops[5]);
	}

	bool CanBypassInt64ToInt32PreJoinProjection(idx_t projection_idx, idx_t hash_join_idx) const {
		if (projection_idx >= ops.size() || hash_join_idx >= ops.size()) {
			return false;
		}
		auto &pre_join_projection = ops[projection_idx];
		if (pre_join_projection.kind != SljitNativeRegionOpKind::PROJECTION ||
		    pre_join_projection.projections.size() != 2 || pre_join_projection.output_types.size() != 2) {
			return false;
		}
		auto &cast_key = pre_join_projection.projections[0].plan;
		if (cast_key.kind != SljitNativeRegionExpressionKind::INTEGER_CAST || cast_key.source_index != 0 ||
		    cast_key.cast_source_width != SljitNativeSignedIntegerWidth::INT64 ||
		    cast_key.cast_target_width != SljitNativeSignedIntegerWidth::INT32 || cast_key.try_cast) {
			return false;
		}
		auto &payload = pre_join_projection.projections[1].plan;
		if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE || payload.source_index != 1) {
			return false;
		}
		auto &join_op = ops[hash_join_idx];
		if (join_op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			return false;
		}
		auto &join = join_op.hash_join_probe.plan;
		return join.keys.size() == 1 && join.keys[0].key_input_index == 0 &&
		       join.keys[0].key_kind == SljitNativeHashJoinKeyKind::INT32;
	}

	static bool TryReadSignedIntegerValue(const Value &value, int64_t &result) {
		if (value.IsNull()) {
			return false;
		}
		switch (value.type().InternalType()) {
		case PhysicalType::INT8:
			result = NumericCast<int64_t>(value.GetValueUnsafe<int8_t>());
			return true;
		case PhysicalType::INT16:
			result = NumericCast<int64_t>(value.GetValueUnsafe<int16_t>());
			return true;
		case PhysicalType::INT32:
			result = NumericCast<int64_t>(value.GetValueUnsafe<int32_t>());
			return true;
		case PhysicalType::INT64:
			result = value.GetValueUnsafe<int64_t>();
			return true;
		default:
			return false;
		}
	}

	bool SourceRangeFitsInt32(idx_t source_index) const {
		if (source_index >= source_min_values.size() || source_index >= source_max_values.size()) {
			return false;
		}
		int64_t min_value;
		int64_t max_value;
		if (!TryReadSignedIntegerValue(source_min_values[source_index], min_value) ||
		    !TryReadSignedIntegerValue(source_max_values[source_index], max_value)) {
			return false;
		}
		return min_value >= NumericLimits<int32_t>::Minimum() && max_value <= NumericLimits<int32_t>::Maximum();
	}

	bool CanUseUncheckedInt64ToInt32PreJoinProjection(idx_t projection_idx, idx_t hash_join_idx) const {
		if (!CanBypassInt64ToInt32PreJoinProjection(projection_idx, hash_join_idx)) {
			return false;
		}
		auto &cast_key = ops[projection_idx].projections[0].plan;
		return SourceRangeFitsInt32(cast_key.source_index);
	}

	bool CanBypassInt64ToInt32PreJoinProjection() const {
		if (!CanBatchProjectionHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return false;
		}
		return CanBypassInt64ToInt32PreJoinProjection(0, 1);
	}

	static bool IsQ12PriorityIntegerConstant(const ExecutionExpressionIR &node, int32_t expected) {
		return node.kind == ExecutionExpressionIRKind::CONSTANT && node.return_type.id() == LogicalTypeId::INTEGER &&
		       !node.constant.IsNull() && node.constant.GetValue<int32_t>() == expected;
	}

	static bool TryReadQ12PriorityCompareConstant(const ExecutionExpressionIR &node,
	                                              ExecutionExpressionBinaryOp expected_op, string &constant) {
		if (node.kind != ExecutionExpressionIRKind::BINARY || node.binary_op != expected_op || !node.left ||
		    !node.right) {
			return false;
		}
		auto try_read = [&](const ExecutionExpressionIR &reference, const ExecutionExpressionIR &constant_node) {
			if (reference.kind != ExecutionExpressionIRKind::REFERENCE || reference.ref_index != 0 ||
			    reference.return_type.id() != LogicalTypeId::VARCHAR ||
			    constant_node.kind != ExecutionExpressionIRKind::CONSTANT ||
			    constant_node.return_type.id() != LogicalTypeId::VARCHAR || constant_node.constant.IsNull()) {
				return false;
			}
			constant = StringValue::Get(constant_node.constant);
			return true;
		};
		return try_read(*node.left, *node.right) || try_read(*node.right, *node.left);
	}

	static bool IsQ12PriorityConstantSet(const string &left, const string &right) {
		return (left == "1-URGENT" && right == "2-HIGH") || (left == "2-HIGH" && right == "1-URGENT");
	}

	static bool IsQ12PriorityCaseExpression(const SljitExecutableRegionExpression &expr, bool high_priority) {
		auto &plan = expr.plan;
		if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !plan.expression_tree ||
		    plan.return_type.id() != LogicalTypeId::INTEGER) {
			return false;
		}
		auto &root = *plan.expression_tree;
		if (root.kind != ExecutionExpressionIRKind::CASE || root.children.size() != 2 || !root.children[0] ||
		    !root.children[1] || !root.else_node || !IsQ12PriorityIntegerConstant(*root.children[1], 1) ||
		    !IsQ12PriorityIntegerConstant(*root.else_node, 0)) {
			return false;
		}
		auto &predicate = *root.children[0];
		const auto expected_conjunction =
		    high_priority ? ExecutionExpressionConjunctionOp::OR : ExecutionExpressionConjunctionOp::AND;
		const auto expected_compare =
		    high_priority ? ExecutionExpressionBinaryOp::COMPARE_EQUAL : ExecutionExpressionBinaryOp::COMPARE_NOTEQUAL;
		if (predicate.kind != ExecutionExpressionIRKind::CONJUNCTION ||
		    predicate.conjunction_op != expected_conjunction || predicate.children.size() != 2 ||
		    !predicate.children[0] || !predicate.children[1]) {
			return false;
		}
		string left_constant;
		string right_constant;
		return TryReadQ12PriorityCompareConstant(*predicate.children[0], expected_compare, left_constant) &&
		       TryReadQ12PriorityCompareConstant(*predicate.children[1], expected_compare, right_constant) &&
		       IsQ12PriorityConstantSet(left_constant, right_constant);
	}

	bool CanFastProjectQ12PriorityGroupedPayload() const {
		if (!CanBypassInt64ToInt32PreJoinProjection()) {
			return false;
		}
		auto &first_projection = ops[2];
		if (first_projection.projections.size() != 2) {
			return false;
		}
		auto &priority_ref = first_projection.projections[0].plan;
		auto &shipmode_decompress = first_projection.projections[1].plan;
		if (priority_ref.kind != SljitNativeRegionExpressionKind::REFERENCE || priority_ref.source_index != 0 ||
		    priority_ref.return_type.id() != LogicalTypeId::VARCHAR ||
		    shipmode_decompress.kind != SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
		    shipmode_decompress.source_index != 1 || shipmode_decompress.return_type.id() != LogicalTypeId::VARCHAR) {
			return false;
		}

		auto &final_projection = ops[3];
		if (final_projection.projections.size() != 3 || final_projection.output_types.size() != 3 ||
		    final_projection.output_types[0].id() != LogicalTypeId::UBIGINT ||
		    final_projection.output_types[1].id() != LogicalTypeId::INTEGER ||
		    final_projection.output_types[2].id() != LogicalTypeId::INTEGER) {
			return false;
		}
		auto &shipmode_compress = final_projection.projections[0].plan;
		if (shipmode_compress.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		    shipmode_compress.source_index != 1 || shipmode_compress.return_type.id() != LogicalTypeId::UBIGINT) {
			return false;
		}
		return IsQ12PriorityCaseExpression(final_projection.projections[1], true) &&
		       IsQ12PriorityCaseExpression(final_projection.projections[2], false);
	}

	static bool Q12PriorityEquals(const string_t &value, const char *constant, idx_t constant_size) {
		return value.GetSize() == constant_size && memcmp(value.GetData(), constant, constant_size) == 0;
	}

	bool TryFastProjectQ12PriorityGroupedPayload(DataChunk &join_output, DataChunk &projected) const {
		if (join_output.ColumnCount() < 2 || projected.ColumnCount() != 3 ||
		    join_output.data[0].GetType().id() != LogicalTypeId::VARCHAR ||
		    join_output.data[1].GetType().id() != LogicalTypeId::UBIGINT ||
		    projected.data[0].GetType().id() != LogicalTypeId::UBIGINT ||
		    projected.data[1].GetType().id() != LogicalTypeId::INTEGER ||
		    projected.data[2].GetType().id() != LogicalTypeId::INTEGER) {
			return false;
		}
		const auto count = join_output.size();
		projected.Reset();
		projected.data[0].Reference(join_output.data[1]);
		projected.data[1].SetVectorType(VectorType::FLAT_VECTOR);
		projected.data[2].SetVectorType(VectorType::FLAT_VECTOR);
		auto high_data = FlatVector::GetDataMutable<int32_t>(projected.data[1]);
		auto low_data = FlatVector::GetDataMutable<int32_t>(projected.data[2]);
		FlatVector::ValidityMutable(projected.data[1]).SetAllValid(count);
		FlatVector::ValidityMutable(projected.data[2]).SetAllValid(count);

		UnifiedVectorFormat priority_format;
		join_output.data[0].ToUnifiedFormat(priority_format);
		auto priority_data = UnifiedVectorFormat::GetData<string_t>(priority_format);
		auto priority_sel = priority_format.sel;
		auto &priority_validity = priority_format.validity;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_idx = priority_sel->get_index(row_idx);
			bool high = false;
			bool low = false;
			if (priority_validity.RowIsValid(source_idx)) {
				auto priority = priority_data[source_idx];
				high = Q12PriorityEquals(priority, "1-URGENT", 8) || Q12PriorityEquals(priority, "2-HIGH", 6);
				low = !high;
			}
			high_data[row_idx] = high ? 1 : 0;
			low_data[row_idx] = low ? 1 : 0;
		}
		projected.SetChildCardinality(count);
		return true;
	}

	static bool TryDecodeInlineCompressedString16Value(uhugeint_t compressed_value, string_t &result) {
		auto value = BSwapIfBE(compressed_value);
		data_t compressed[sizeof(uhugeint_t)];
		memcpy(compressed, const_data_ptr_cast(&value), sizeof(uhugeint_t));
		const auto length = UnsafeNumericCast<idx_t>(compressed[0]);
		if (length > string_t::INLINE_LENGTH || length >= sizeof(uhugeint_t)) {
			return false;
		}
		char decoded[string_t::INLINE_LENGTH];
		for (idx_t byte_idx = 0; byte_idx < length; byte_idx++) {
			decoded[byte_idx] = char(compressed[sizeof(uhugeint_t) - byte_idx - 1]);
		}
		result = string_t(decoded, UnsafeNumericCast<uint32_t>(length));
		return true;
	}

	static bool TryFastDecodeInlineCompressedString16(Vector &source, idx_t count, Vector &result) {
		UnifiedVectorFormat source_format;
		source.ToUnifiedFormat(source_format);
		if (!SljitUnifiedFormatHasIdentitySelection(source_format) ||
		    (source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(count))) {
			return false;
		}

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		result_validity.SetAllValid(count);
		auto result_data = FlatVector::GetDataMutable<string_t>(result);
		auto source_data = source_format.data;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (!TryDecodeInlineCompressedString16Value(Load<uhugeint_t>(source_data + row_idx * sizeof(uhugeint_t)),
			                                            result_data[row_idx])) {
				return false;
			}
		}
		FlatVector::SetSize(result, count_t(count));
		return true;
	}

	bool TryFastInlineStringDecompressProjection(SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
	                                             idx_t count) {
		if (op.kind != SljitNativeRegionOpKind::PROJECTION || op.projections.size() != output.ColumnCount()) {
			return false;
		}
		bool has_fast_decompress = false;
		for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
			auto &plan = op.projections[projection_idx].plan;
			if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				if (plan.source_index >= input.ColumnCount() ||
				    plan.return_type != input.data[plan.source_index].GetType() ||
				    output.data[projection_idx].GetType() != input.data[plan.source_index].GetType()) {
					return false;
				}
				continue;
			}
			if (plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS &&
			    plan.string_decompress_source_size == sizeof(uhugeint_t) && plan.source_index < input.ColumnCount() &&
			    input.data[plan.source_index].GetType().InternalType() == PhysicalType::UINT128 &&
			    output.data[projection_idx].GetType().id() == LogicalTypeId::VARCHAR) {
				has_fast_decompress = true;
				continue;
			}
			return false;
		}
		if (!has_fast_decompress) {
			return false;
		}

		for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
			auto &plan = op.projections[projection_idx].plan;
			if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				output.data[projection_idx].Reference(input.data[plan.source_index]);
				continue;
			}
			if (!TryFastDecodeInlineCompressedString16(input.data[plan.source_index], count,
			                                           output.data[projection_idx])) {
				return false;
			}
		}
		output.SetChildCardinality(count);
		return true;
	}

	bool CanDirectBuildQ7SecondJoinInput() const {
		if (ops.size() != 7 || ops[1].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE ||
		    ops[2].kind != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
		auto &projection = ops[2];
		if (projection.projections.size() != 5 || projection.output_types.size() != 5 ||
		    projection.output_types[0].id() != LogicalTypeId::BIGINT ||
		    projection.output_types[1].id() != LogicalTypeId::VARCHAR ||
		    projection.output_types[2].id() != LogicalTypeId::DATE ||
		    projection.output_types[3].InternalType() != PhysicalType::INT64 ||
		    projection.output_types[4].InternalType() != PhysicalType::INT64) {
			return false;
		}
		auto &custkey = projection.projections[0].plan;
		auto &nation_name = projection.projections[1].plan;
		auto &shipdate = projection.projections[2].plan;
		auto &extended_price = projection.projections[3].plan;
		auto &discount = projection.projections[4].plan;
		return custkey.kind == SljitNativeRegionExpressionKind::REFERENCE && custkey.source_index == 0 &&
		       nation_name.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS &&
		       nation_name.source_index == 4 && nation_name.string_decompress_source_size == sizeof(uhugeint_t) &&
		       shipdate.kind == SljitNativeRegionExpressionKind::REFERENCE && shipdate.source_index == 1 &&
		       extended_price.kind == SljitNativeRegionExpressionKind::REFERENCE && extended_price.source_index == 2 &&
		       discount.kind == SljitNativeRegionExpressionKind::REFERENCE && discount.source_index == 3;
	}

	bool TryDirectBuildQ7SecondJoinInput(SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
	                                     SelectionVector &first_match_selection, Vector &first_row_pointers,
	                                     idx_t count, DataChunk &second_join_input, idx_t target_offset = 0) const {
		if (!CanDirectBuildQ7SecondJoinInput() || source_chunk.ColumnCount() < 2 ||
		    second_join_input.ColumnCount() != 5 || !scratch.HasOperatorBinding(1) ||
		    target_offset + count > STANDARD_VECTOR_SIZE) {
			return false;
		}
		auto &layout = scratch.OperatorBinding(1).hash_join_probe.table_layout;
		if (!layout.ready || layout.layout_offsets.size() < 5) {
			return false;
		}
		static constexpr idx_t FIRST_JOIN_SHIPDATE_PAYLOAD = 1;
		static constexpr idx_t FIRST_JOIN_EXTENDED_PRICE_PAYLOAD = 2;
		static constexpr idx_t FIRST_JOIN_DISCOUNT_PAYLOAD = 3;
		static constexpr idx_t FIRST_JOIN_NATION_NAME_PAYLOAD = 4;
		const auto shipdate_offset = layout.layout_offsets[FIRST_JOIN_SHIPDATE_PAYLOAD];
		const auto extended_price_offset = layout.layout_offsets[FIRST_JOIN_EXTENDED_PRICE_PAYLOAD];
		const auto discount_offset = layout.layout_offsets[FIRST_JOIN_DISCOUNT_PAYLOAD];
		const auto nation_name_offset = layout.layout_offsets[FIRST_JOIN_NATION_NAME_PAYLOAD];

		UnifiedVectorFormat custkey_format;
		source_chunk.data[1].ToUnifiedFormat(custkey_format);
		auto custkey_data = UnifiedVectorFormat::GetData<int64_t>(custkey_format);
		auto custkey_sel = custkey_format.sel;
		auto &custkey_validity = custkey_format.validity;
		auto row_pointer_data = FlatVector::GetData<data_ptr_t>(first_row_pointers);

		const auto result_count = target_offset + count;
		for (auto &column : second_join_input.data) {
			column.SetVectorType(VectorType::FLAT_VECTOR);
			auto &validity = FlatVector::ValidityMutable(column);
			validity.Reset(result_count);
			validity.SetAllValid(result_count);
			FlatVector::SetSize(column, count_t(result_count));
		}
		auto out_custkey = FlatVector::GetDataMutable<int64_t>(second_join_input.data[0]);
		auto out_nation_name = FlatVector::GetDataMutable<string_t>(second_join_input.data[1]);
		auto out_shipdate = FlatVector::GetDataMutable<int32_t>(second_join_input.data[2]);
		auto out_extended_price = FlatVector::GetDataMutable<int64_t>(second_join_input.data[3]);
		auto out_discount = FlatVector::GetDataMutable<int64_t>(second_join_input.data[4]);

		for (idx_t local_idx = 0; local_idx < count; local_idx++) {
			const auto out_idx = target_offset + local_idx;
			const auto probe_idx = first_match_selection.get_index(local_idx);
			const auto source_idx = custkey_sel->get_index(probe_idx);
			if (custkey_validity.RowIsValid(source_idx) == false) {
				return false;
			}
			auto row_location = row_pointer_data[local_idx];
			if (!row_location) {
				return false;
			}
			out_custkey[out_idx] = custkey_data[source_idx];
			out_shipdate[out_idx] = Load<int32_t>(row_location + shipdate_offset);
			out_extended_price[out_idx] = Load<int64_t>(row_location + extended_price_offset);
			out_discount[out_idx] = Load<int64_t>(row_location + discount_offset);
			if (!TryDecodeInlineCompressedString16Value(Load<uhugeint_t>(row_location + nation_name_offset),
			                                            out_nation_name[out_idx])) {
				return false;
			}
		}
		second_join_input.SetChildCardinality(result_count);
		return true;
	}

	bool CanBatchHashJoinHashJoinProjectionProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 5 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[2].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[3].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[4].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[4].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		       ProjectionOutputsAreFixedWidth(ops[3]);
	}

	bool CanBatchGeneratedFilterProjectionHashJoinProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 5 && ops[0].kind == SljitNativeRegionOpKind::FILTER &&
		       ops[1].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[2].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[3].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[4].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[4].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		       ProjectionOutputsAreFixedWidth(ops[3]);
	}

	bool ProjectionOutputsAreFixedWidth(const SljitExecutableRegionOp &projection_op) const {
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
		return OutputTypesAreFixedWidth(projection_op.output_types);
	}

	static bool IsBooleanType(const LogicalType &type) {
		return type.id() == LogicalTypeId::BOOLEAN && type.InternalType() == PhysicalType::BOOL;
	}

	static bool LocalSourceReferencesColumn(const vector<idx_t> &input_source_indices, idx_t local_source_index,
	                                        idx_t column_index) {
		if (input_source_indices.empty()) {
			return local_source_index == column_index;
		}
		return local_source_index < input_source_indices.size() &&
		       input_source_indices[local_source_index] == column_index;
	}

	static bool IsBooleanMarkerReference(const SljitExecutableRegionExpression &expression, idx_t marker_index) {
		auto &plan = expression.plan;
		if (!IsBooleanType(plan.return_type)) {
			return false;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			return LocalSourceReferencesColumn(expression.input_source_indices, plan.source_index, marker_index);
		}
		if (plan.kind == SljitNativeRegionExpressionKind::PREDICATE) {
			return plan.predicate && plan.predicate->kind == SljitNativePredicateKind::REFERENCE &&
			       IsBooleanType(plan.predicate->return_type) &&
			       LocalSourceReferencesColumn(expression.input_source_indices, plan.predicate->source_index,
			                                   marker_index);
		}
		if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !plan.expression_tree ||
		    plan.expression_tree->kind != ExecutionExpressionIRKind::REFERENCE ||
		    !IsBooleanType(plan.expression_tree->return_type)) {
			return false;
		}
		return LocalSourceReferencesColumn(plan.expression_tree_source_indices, plan.expression_tree->ref_index,
		                                   marker_index);
	}

	static bool IsMarkProbeMarkerFilter(const SljitExecutableRegionOp &hash_join_op,
	                                    const SljitExecutableRegionOp &filter_op) {
		if (hash_join_op.output_types.empty() || hash_join_op.output_types.back().id() != LogicalTypeId::BOOLEAN ||
		    filter_op.kind != SljitNativeRegionOpKind::FILTER) {
			return false;
		}
		return IsBooleanMarkerReference(filter_op.filter, hash_join_op.output_types.size() - 1);
	}

	static constexpr idx_t SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT = 64;

	template <class T>
	static bool AccumulatePreaggregatedCountStarDeltaKey(
	    const T &key, int64_t delta, std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
	    std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts, idx_t &group_count) {
		idx_t group_idx = 0;
		for (; group_idx < group_count; group_idx++) {
			if (keys[group_idx] == key) {
				break;
			}
		}
		if (group_idx == group_count) {
			if (group_count == SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT) {
				return false;
			}
			keys[group_count] = key;
			counts[group_count] = 0;
			group_count++;
		}
		counts[group_idx] += delta;
		return true;
	}

	template <class T>
	static bool AccumulatePreaggregatedCountStarKey(const T &key,
	                                                std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
	                                                std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts,
	                                                idx_t &group_count) {
		return AccumulatePreaggregatedCountStarDeltaKey(key, 1, keys, counts, group_count);
	}

	template <class T>
	static void
	MaterializePreaggregatedCountStarGroups(const std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
	                                        const std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts,
	                                        idx_t group_count, DataChunk &compact_groups,
	                                        vector<int64_t> &count_deltas) {
		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<T>(target);
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		count_deltas.resize(group_count);
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			target_data[group_idx] = keys[group_idx];
			count_deltas[group_idx] = counts[group_idx];
		}
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
	}

	template <class T>
	static bool TryPreaggregateFixedWidthCountStarGroupsTemplated(DataChunk &input_groups, DataChunk &compact_groups,
	                                                              vector<int64_t> &count_deltas) {
		const auto count = input_groups.size();
		if (count == 0 || input_groups.ColumnCount() != 1 || compact_groups.ColumnCount() != 1 ||
		    input_groups.data[0].GetType() != compact_groups.data[0].GetType()) {
			return false;
		}

		UnifiedVectorFormat format;
		input_groups.data[0].ToUnifiedFormat(format);
		auto source_data = UnifiedVectorFormat::GetData<T>(format);
		auto source_sel = format.sel;
		auto &source_validity = format.validity;
		std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
		std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
		idx_t group_count = 0;
		const bool can_have_null = source_validity.CanHaveNull();

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_idx = source_sel->get_index(row_idx);
			if (can_have_null && !source_validity.RowIsValid(source_idx)) {
				return false;
			}
			auto key = source_data[source_idx];
			if (!AccumulatePreaggregatedCountStarKey(key, keys, counts, group_count)) {
				return false;
			}
		}
		MaterializePreaggregatedCountStarGroups(keys, counts, group_count, compact_groups, count_deltas);
		return true;
	}

	static bool TryPreaggregateFixedWidthCountStarGroups(DataChunk &input_groups, DataChunk &compact_groups,
	                                                     vector<int64_t> &count_deltas) {
		if (input_groups.ColumnCount() != 1) {
			return false;
		}
		switch (input_groups.data[0].GetType().InternalType()) {
		case PhysicalType::INT8:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<int8_t>(input_groups, compact_groups,
			                                                                 count_deltas);
		case PhysicalType::INT16:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<int16_t>(input_groups, compact_groups,
			                                                                  count_deltas);
		case PhysicalType::INT32:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<int32_t>(input_groups, compact_groups,
			                                                                  count_deltas);
		case PhysicalType::INT64:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<int64_t>(input_groups, compact_groups,
			                                                                  count_deltas);
		case PhysicalType::INT128:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<hugeint_t>(input_groups, compact_groups,
			                                                                    count_deltas);
		case PhysicalType::UINT8:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<uint8_t>(input_groups, compact_groups,
			                                                                  count_deltas);
		case PhysicalType::UINT16:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<uint16_t>(input_groups, compact_groups,
			                                                                   count_deltas);
		case PhysicalType::UINT32:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<uint32_t>(input_groups, compact_groups,
			                                                                   count_deltas);
		case PhysicalType::UINT64:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<uint64_t>(input_groups, compact_groups,
			                                                                   count_deltas);
		case PhysicalType::UINT128:
			return TryPreaggregateFixedWidthCountStarGroupsTemplated<uhugeint_t>(input_groups, compact_groups,
			                                                                     count_deltas);
		default:
			return false;
		}
	}

	template <class T>
	static bool MergePreaggregatedFixedWidthCountStarGroupsTemplated(
	    DataChunk &compact_groups, const vector<int64_t> &count_deltas,
	    std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
	    std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts, idx_t &group_count) {
		if (compact_groups.ColumnCount() != 1 || count_deltas.size() < compact_groups.size()) {
			return false;
		}
		UnifiedVectorFormat format;
		compact_groups.data[0].ToUnifiedFormat(format);
		auto source_data = UnifiedVectorFormat::GetData<T>(format);
		auto source_sel = format.sel;
		auto &source_validity = format.validity;
		const bool can_have_null = source_validity.CanHaveNull();
		for (idx_t row_idx = 0; row_idx < compact_groups.size(); row_idx++) {
			const auto source_idx = source_sel->get_index(row_idx);
			if (can_have_null && !source_validity.RowIsValid(source_idx)) {
				return false;
			}
			if (!AccumulatePreaggregatedCountStarDeltaKey(source_data[source_idx], count_deltas[row_idx], keys, counts,
			                                              group_count)) {
				return false;
			}
		}
		return true;
	}

	static void SljitReverseMemCpy(data_ptr_t dest, const_data_ptr_t src, idx_t length) {
		for (idx_t byte_idx = 0; byte_idx < length; byte_idx++) {
			dest[byte_idx] = src[length - 1 - byte_idx];
		}
	}

	template <class RESULT_TYPE>
	static bool TrySljitStringCompressWide(const string_t &input, RESULT_TYPE &result) {
		if (input.GetSize() >= sizeof(RESULT_TYPE)) {
			return false;
		}
		auto result_ptr = data_ptr_cast(&result);
		if (sizeof(RESULT_TYPE) <= string_t::INLINE_LENGTH) {
			SljitReverseMemCpy(result_ptr, const_data_ptr_cast(input.GetPrefix()), sizeof(RESULT_TYPE));
		} else if (input.IsInlined()) {
			static constexpr auto REMAINDER = sizeof(RESULT_TYPE) - string_t::INLINE_LENGTH;
			SljitReverseMemCpy(result_ptr + REMAINDER, const_data_ptr_cast(input.GetPrefix()), string_t::INLINE_LENGTH);
			memset(result_ptr, '\0', REMAINDER);
		} else {
			const auto size = MinValue<idx_t>(sizeof(RESULT_TYPE), input.GetSize());
			const auto remainder = sizeof(RESULT_TYPE) - size;
			SljitReverseMemCpy(result_ptr + remainder, data_ptr_cast(input.GetPointer()), size);
			memset(result_ptr, '\0', remainder);
		}
		result_ptr[0] = UnsafeNumericCast<data_t>(input.GetSize());
		result = BSwapIfBE(result);
		return true;
	}

	static bool TrySljitStringCompressUInt8(const string_t &input, uint8_t &result) {
		if (input.GetSize() > sizeof(uint8_t)) {
			return false;
		}
		result = input.GetSize() == 0
		             ? 0
		             : UnsafeNumericCast<uint8_t>(input.GetSize() + *const_data_ptr_cast(input.GetPrefix()));
		result = BSwapIfBE(result);
		return true;
	}

	static bool SljitStringEquals(const string_t &left, const string_t &right) {
		return left == right;
	}

	static bool AccumulatePreaggregatedStringCountStarKey(
	    const string_t &key, std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &keys,
	    std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> &counts, idx_t &group_count) {
		idx_t group_idx = 0;
		for (; group_idx < group_count; group_idx++) {
			if (SljitStringEquals(keys[group_idx], key)) {
				break;
			}
		}
		if (group_idx == group_count) {
			if (group_count == SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT) {
				return false;
			}
			keys[group_count] = key;
			counts[group_count] = 0;
			group_count++;
		}
		counts[group_idx]++;
		return true;
	}

	template <class T>
	static bool TryPreaggregateStringCompressedCountStarGroupsTemplated(Vector &source,
	                                                                    const SelectionVector *execute_sel, idx_t count,
	                                                                    DataChunk &compact_groups,
	                                                                    vector<int64_t> &count_deltas) {
		UnifiedVectorFormat format;
		source.ToUnifiedFormat(format);
		auto source_data = UnifiedVectorFormat::GetData<string_t>(format);
		auto source_sel = format.sel;
		auto &source_validity = format.validity;
		const bool can_have_null = source_validity.CanHaveNull();
		std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
		std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
		idx_t group_count = 0;

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
			const auto source_idx = source_sel->get_index(input_idx);
			if (can_have_null && !source_validity.RowIsValid(source_idx)) {
				return false;
			}
			if (!AccumulatePreaggregatedStringCountStarKey(source_data[source_idx], keys, counts, group_count)) {
				return false;
			}
		}

		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<T>(target);
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		count_deltas.resize(group_count);
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			T compressed_key;
			if (!TrySljitStringCompressWide(keys[group_idx], compressed_key)) {
				return false;
			}
			target_data[group_idx] = compressed_key;
			count_deltas[group_idx] = counts[group_idx];
		}
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	template <class T>
	static bool TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated(Vector &source,
	                                                                          const SelectionVector &mark_flags,
	                                                                          idx_t count, DataChunk &compact_groups,
	                                                                          vector<int64_t> &count_deltas,
	                                                                          idx_t &selected_count) {
		if (source.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto &validity = FlatVector::Validity(source);
			if (validity.CannotHaveNull() || validity.CheckAllValid(count)) {
				auto source_data = FlatVector::GetData<string_t>(source);
				std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
				std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
				idx_t group_count = 0;
				selected_count = 0;
				for (idx_t row_idx = 0; row_idx < count; row_idx++) {
					if (mark_flags.get_index(row_idx) == 0) {
						continue;
					}
					selected_count++;
					if (!AccumulatePreaggregatedStringCountStarKey(source_data[row_idx], keys, counts, group_count)) {
						return false;
					}
				}
				if (selected_count == 0) {
					compact_groups.Reset();
					return true;
				}
				compact_groups.Reset();
				auto &target = compact_groups.data[0];
				target.SetVectorType(VectorType::FLAT_VECTOR);
				auto target_data = FlatVector::GetDataMutable<T>(target);
				auto &target_validity = FlatVector::ValidityMutable(target);
				target_validity.Reset(group_count);
				target_validity.SetAllValid(group_count);
				count_deltas.resize(group_count);
				for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
					T compressed_key;
					if (!TrySljitStringCompressWide(keys[group_idx], compressed_key)) {
						return false;
					}
					target_data[group_idx] = compressed_key;
					count_deltas[group_idx] = counts[group_idx];
				}
				FlatVector::SetSize(target, count_t(group_count));
				compact_groups.SetChildCardinality(group_count);
				return true;
			}
		}

		UnifiedVectorFormat format;
		source.ToUnifiedFormat(format);
		auto source_data = UnifiedVectorFormat::GetData<string_t>(format);
		auto source_sel = format.sel;
		auto &source_validity = format.validity;
		const bool can_have_null = source_validity.CanHaveNull();
		std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
		std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
		idx_t group_count = 0;
		selected_count = 0;

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (mark_flags.get_index(row_idx) == 0) {
				continue;
			}
			selected_count++;
			const auto source_idx = source_sel->get_index(row_idx);
			if (can_have_null && !source_validity.RowIsValid(source_idx)) {
				return false;
			}
			if (!AccumulatePreaggregatedStringCountStarKey(source_data[source_idx], keys, counts, group_count)) {
				return false;
			}
		}
		if (selected_count == 0) {
			compact_groups.Reset();
			return true;
		}

		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<T>(target);
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		count_deltas.resize(group_count);
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			T compressed_key;
			if (!TrySljitStringCompressWide(keys[group_idx], compressed_key)) {
				return false;
			}
			target_data[group_idx] = compressed_key;
			count_deltas[group_idx] = counts[group_idx];
		}
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	static bool TryPreaggregateStringCompressedUInt8CountStarGroups(Vector &source, const SelectionVector *execute_sel,
	                                                                idx_t count, DataChunk &compact_groups,
	                                                                vector<int64_t> &count_deltas) {
		UnifiedVectorFormat format;
		source.ToUnifiedFormat(format);
		auto source_data = UnifiedVectorFormat::GetData<string_t>(format);
		auto source_sel = format.sel;
		auto &source_validity = format.validity;
		const bool can_have_null = source_validity.CanHaveNull();
		std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
		std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
		idx_t group_count = 0;

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
			const auto source_idx = source_sel->get_index(input_idx);
			if (can_have_null && !source_validity.RowIsValid(source_idx)) {
				return false;
			}
			if (!AccumulatePreaggregatedStringCountStarKey(source_data[source_idx], keys, counts, group_count)) {
				return false;
			}
		}

		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<uint8_t>(target);
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		count_deltas.resize(group_count);
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			uint8_t compressed_key;
			if (!TrySljitStringCompressUInt8(keys[group_idx], compressed_key)) {
				return false;
			}
			target_data[group_idx] = compressed_key;
			count_deltas[group_idx] = counts[group_idx];
		}
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	static bool TryPreaggregateStringCompressedUInt8MarkedCountStarGroups(Vector &source,
	                                                                      const SelectionVector &mark_flags,
	                                                                      idx_t count, DataChunk &compact_groups,
	                                                                      vector<int64_t> &count_deltas,
	                                                                      idx_t &selected_count) {
		if (source.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto &validity = FlatVector::Validity(source);
			if (validity.CannotHaveNull() || validity.CheckAllValid(count)) {
				auto source_data = FlatVector::GetData<string_t>(source);
				std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
				std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
				idx_t group_count = 0;
				selected_count = 0;
				for (idx_t row_idx = 0; row_idx < count; row_idx++) {
					if (mark_flags.get_index(row_idx) == 0) {
						continue;
					}
					selected_count++;
					if (!AccumulatePreaggregatedStringCountStarKey(source_data[row_idx], keys, counts, group_count)) {
						return false;
					}
				}
				if (selected_count == 0) {
					compact_groups.Reset();
					return true;
				}
				compact_groups.Reset();
				auto &target = compact_groups.data[0];
				target.SetVectorType(VectorType::FLAT_VECTOR);
				auto target_data = FlatVector::GetDataMutable<uint8_t>(target);
				auto &target_validity = FlatVector::ValidityMutable(target);
				target_validity.Reset(group_count);
				target_validity.SetAllValid(group_count);
				count_deltas.resize(group_count);
				for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
					uint8_t compressed_key;
					if (!TrySljitStringCompressUInt8(keys[group_idx], compressed_key)) {
						return false;
					}
					target_data[group_idx] = compressed_key;
					count_deltas[group_idx] = counts[group_idx];
				}
				FlatVector::SetSize(target, count_t(group_count));
				compact_groups.SetChildCardinality(group_count);
				return true;
			}
		}

		UnifiedVectorFormat format;
		source.ToUnifiedFormat(format);
		auto source_data = UnifiedVectorFormat::GetData<string_t>(format);
		auto source_sel = format.sel;
		auto &source_validity = format.validity;
		const bool can_have_null = source_validity.CanHaveNull();
		std::array<string_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
		std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> counts;
		idx_t group_count = 0;
		selected_count = 0;

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (mark_flags.get_index(row_idx) == 0) {
				continue;
			}
			selected_count++;
			const auto source_idx = source_sel->get_index(row_idx);
			if (can_have_null && !source_validity.RowIsValid(source_idx)) {
				return false;
			}
			if (!AccumulatePreaggregatedStringCountStarKey(source_data[source_idx], keys, counts, group_count)) {
				return false;
			}
		}
		if (selected_count == 0) {
			compact_groups.Reset();
			return true;
		}

		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<uint8_t>(target);
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		count_deltas.resize(group_count);
		for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
			uint8_t compressed_key;
			if (!TrySljitStringCompressUInt8(keys[group_idx], compressed_key)) {
				return false;
			}
			target_data[group_idx] = compressed_key;
			count_deltas[group_idx] = counts[group_idx];
		}
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	static bool TryPreaggregateProjectedCountStarGroups(const SljitExecutableRegionOp &projection_op, DataChunk &input,
	                                                    const SelectionVector *execute_sel, idx_t count,
	                                                    DataChunk &compact_groups, vector<int64_t> &count_deltas) {
		if (count == 0 || projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    projection_op.projections.size() != 1 || projection_op.output_types.size() != 1 ||
		    compact_groups.ColumnCount() != 1 || compact_groups.data[0].GetType() != projection_op.output_types[0]) {
			return false;
		}
		auto &plan = projection_op.projections[0].plan;
		if (plan.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		    plan.return_type != projection_op.output_types[0] || plan.source_index >= input.ColumnCount() ||
		    input.data[plan.source_index].GetType().id() != LogicalTypeId::VARCHAR) {
			return false;
		}
		if (plan.string_compress_target_size != GetTypeIdSize(projection_op.output_types[0].InternalType())) {
			return false;
		}
		switch (projection_op.output_types[0].InternalType()) {
		case PhysicalType::UINT8:
			return TryPreaggregateStringCompressedUInt8CountStarGroups(input.data[plan.source_index], execute_sel,
			                                                           count, compact_groups, count_deltas);
		case PhysicalType::UINT16:
			return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint16_t>(
			    input.data[plan.source_index], execute_sel, count, compact_groups, count_deltas);
		case PhysicalType::UINT32:
			return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint32_t>(
			    input.data[plan.source_index], execute_sel, count, compact_groups, count_deltas);
		case PhysicalType::UINT64:
			return TryPreaggregateStringCompressedCountStarGroupsTemplated<uint64_t>(
			    input.data[plan.source_index], execute_sel, count, compact_groups, count_deltas);
		case PhysicalType::UINT128:
			return TryPreaggregateStringCompressedCountStarGroupsTemplated<uhugeint_t>(
			    input.data[plan.source_index], execute_sel, count, compact_groups, count_deltas);
		default:
			return false;
		}
	}

	static bool TryPreaggregateProjectedMarkedCountStarGroups(const SljitExecutableRegionOp &projection_op,
	                                                          DataChunk &input, const SelectionVector &mark_flags,
	                                                          idx_t count, DataChunk &compact_groups,
	                                                          vector<int64_t> &count_deltas, idx_t &selected_count) {
		if (count == 0 || projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    projection_op.projections.size() != 1 || projection_op.output_types.size() != 1 ||
		    compact_groups.ColumnCount() != 1 || compact_groups.data[0].GetType() != projection_op.output_types[0]) {
			return false;
		}
		auto &plan = projection_op.projections[0].plan;
		if (plan.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		    plan.return_type != projection_op.output_types[0] || plan.source_index >= input.ColumnCount() ||
		    input.data[plan.source_index].GetType().id() != LogicalTypeId::VARCHAR) {
			return false;
		}
		if (plan.string_compress_target_size != GetTypeIdSize(projection_op.output_types[0].InternalType())) {
			return false;
		}
		switch (projection_op.output_types[0].InternalType()) {
		case PhysicalType::UINT8:
			return TryPreaggregateStringCompressedUInt8MarkedCountStarGroups(
			    input.data[plan.source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
		case PhysicalType::UINT16:
			return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uint16_t>(
			    input.data[plan.source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
		case PhysicalType::UINT32:
			return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uint32_t>(
			    input.data[plan.source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
		case PhysicalType::UINT64:
			return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uint64_t>(
			    input.data[plan.source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
		case PhysicalType::UINT128:
			return TryPreaggregateStringCompressedMarkedCountStarGroupsTemplated<uhugeint_t>(
			    input.data[plan.source_index], mark_flags, count, compact_groups, count_deltas, selected_count);
		default:
			return false;
		}
	}

	struct SljitPreaggregatedPrimitivePayloadSource {
		AggregatePrimitiveUpdateKind kind = AggregatePrimitiveUpdateKind::NONE;
		idx_t source_index = DConstants::INVALID_INDEX;
		PhysicalType type = PhysicalType::INVALID;
		UnifiedVectorFormat format;
	};

	static bool SljitPreaggregatedPrimitiveIntegerTypeSupported(PhysicalType type) {
		switch (type) {
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
			return true;
		default:
			return false;
		}
	}

	static bool SljitPreaggregatedPrimitivePayloadSupported(AggregatePrimitiveUpdateKind kind, PhysicalType type) {
		switch (kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
			return true;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			return SljitPreaggregatedPrimitiveIntegerTypeSupported(type);
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			return SljitPreaggregatedPrimitiveIntegerTypeSupported(type) || type == PhysicalType::INT128;
		default:
			return false;
		}
	}

	static bool SljitLoadPreaggregatedInt64Payload(SljitPreaggregatedPrimitivePayloadSource &source, idx_t row_idx,
	                                               int64_t &result) {
		auto source_idx = source.format.sel->get_index(row_idx);
		if (source.format.validity.RowIsValid(source_idx) == false) {
			return false;
		}
		switch (source.type) {
		case PhysicalType::INT8:
			result = UnifiedVectorFormat::GetData<int8_t>(source.format)[source_idx];
			return true;
		case PhysicalType::INT16:
			result = UnifiedVectorFormat::GetData<int16_t>(source.format)[source_idx];
			return true;
		case PhysicalType::INT32:
			result = UnifiedVectorFormat::GetData<int32_t>(source.format)[source_idx];
			return true;
		case PhysicalType::INT64:
			result = UnifiedVectorFormat::GetData<int64_t>(source.format)[source_idx];
			return true;
		case PhysicalType::UINT8:
			result = NumericCast<int64_t>(UnifiedVectorFormat::GetData<uint8_t>(source.format)[source_idx]);
			return true;
		case PhysicalType::UINT16:
			result = NumericCast<int64_t>(UnifiedVectorFormat::GetData<uint16_t>(source.format)[source_idx]);
			return true;
		case PhysicalType::UINT32:
			result = NumericCast<int64_t>(UnifiedVectorFormat::GetData<uint32_t>(source.format)[source_idx]);
			return true;
		default:
			return false;
		}
	}

	static bool SljitLoadPreaggregatedHugeintPayload(SljitPreaggregatedPrimitivePayloadSource &source, idx_t row_idx,
	                                                 hugeint_t &result) {
		auto source_idx = source.format.sel->get_index(row_idx);
		if (source.format.validity.RowIsValid(source_idx) == false) {
			return false;
		}
		if (source.type == PhysicalType::INT128) {
			result = UnifiedVectorFormat::GetData<hugeint_t>(source.format)[source_idx];
			return true;
		}
		int64_t value;
		if (!SljitLoadPreaggregatedInt64Payload(source, row_idx, value)) {
			return false;
		}
		result = hugeint_t(value);
		return true;
	}

	static bool PrepareSljitPreaggregatedPrimitivePayloadSources(
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    vector<SljitPreaggregatedPrimitivePayloadSource> &payload_sources) {
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		payload_sources.resize(aggregates.size());
		for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			auto lane = payload_lanes[payload_idx];
			if (!lane) {
				return false;
			}
			auto &source = payload_sources[payload_idx];
			source.kind = lane->kind;
			if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				source.source_index = DConstants::INVALID_INDEX;
				source.type = PhysicalType::INVALID;
				continue;
			}
			auto &aggregate = aggregates[payload_idx];
			if (aggregate.child_indices.size() != 1 || aggregate.child_indices[0] >= input.ColumnCount()) {
				return false;
			}
			source.source_index = aggregate.child_indices[0];
			auto &source_vector = input.data[source.source_index];
			source.type = source_vector.GetType().InternalType();
			if (!SljitPreaggregatedPrimitivePayloadSupported(lane->kind, source.type)) {
				return false;
			}
			source_vector.ToUnifiedFormat(source.format);
		}
		return true;
	}

	static bool SljitFlatVectorAllValid(Vector &vector, idx_t count) {
		if (vector.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		auto &validity = FlatVector::Validity(vector);
		return validity.CannotHaveNull() || validity.CheckAllValid(count);
	}

	template <class T>
	static int64_t SljitPreaggregateToInt64(T value) {
		return NumericCast<int64_t>(value);
	}

	template <class T>
	static hugeint_t SljitPreaggregateToHugeint(T value) {
		return hugeint_t(SljitPreaggregateToInt64(value));
	}

	static hugeint_t SljitPreaggregateToHugeint(hugeint_t value) {
		return value;
	}

	template <class T>
	static bool SljitFlatKeysHaveConsecutiveRepeat(const T *keys, idx_t count) {
		const auto sample_count = MinValue<idx_t>(count, 64);
		for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
			if (keys[row_idx] == keys[row_idx - 1]) {
				return true;
			}
		}
		return false;
	}

	template <class GROUP_TYPE>
	static bool TryPreaggregateFlatAllValidSingleCountStarGroup(
	    DataChunk &input, idx_t group_source_index, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &scratch,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		const auto count = input.size();
		auto &group_vector = input.data[group_source_index];
		if (!SljitFlatVectorAllValid(group_vector, count)) {
			return false;
		}
		auto group_data = FlatVector::GetData<GROUP_TYPE>(group_vector);
		if (!SljitFlatKeysHaveConsecutiveRepeat(group_data, count)) {
			return false;
		}
		scratch.Prepare(payload_lanes, count);
		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<GROUP_TYPE>(target);
		auto &counts = scratch.payloads[0].int64_values;
		idx_t group_count = 0;
		GROUP_TYPE active_key {};
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto key = group_data[row_idx];
			if (group_count == 0 || !(key == active_key)) {
				active_key = key;
				target_data[group_count] = key;
				counts.push_back(0);
				group_count++;
			}
			counts[group_count - 1]++;
		}
		if (group_count == count) {
			return false;
		}
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	template <class GROUP_TYPE, class SOURCE_TYPE>
	static bool TryPreaggregateFlatAllValidSingleInt64SumGroup(
	    DataChunk &input, idx_t group_source_index, idx_t payload_source_index, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &scratch,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		const auto count = input.size();
		auto &group_vector = input.data[group_source_index];
		auto &payload_vector = input.data[payload_source_index];
		if (!SljitFlatVectorAllValid(group_vector, count) || !SljitFlatVectorAllValid(payload_vector, count)) {
			return false;
		}
		auto group_data = FlatVector::GetData<GROUP_TYPE>(group_vector);
		if (!SljitFlatKeysHaveConsecutiveRepeat(group_data, count)) {
			return false;
		}
		auto payload_data = FlatVector::GetData<SOURCE_TYPE>(payload_vector);
		scratch.Prepare(payload_lanes, count);
		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<GROUP_TYPE>(target);
		auto &sums = scratch.payloads[0].int64_values;
		idx_t group_count = 0;
		GROUP_TYPE active_key {};
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto key = group_data[row_idx];
			if (group_count == 0 || !(key == active_key)) {
				active_key = key;
				target_data[group_count] = key;
				sums.push_back(0);
				group_count++;
			}
			sums[group_count - 1] += SljitPreaggregateToInt64(payload_data[row_idx]);
		}
		if (group_count == count) {
			return false;
		}
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	template <class GROUP_TYPE, class SOURCE_TYPE>
	static bool TryPreaggregateFlatAllValidSingleHugeintSumGroup(
	    DataChunk &input, idx_t group_source_index, idx_t payload_source_index, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &scratch,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		const auto count = input.size();
		auto &group_vector = input.data[group_source_index];
		auto &payload_vector = input.data[payload_source_index];
		if (!SljitFlatVectorAllValid(group_vector, count) || !SljitFlatVectorAllValid(payload_vector, count)) {
			return false;
		}
		auto group_data = FlatVector::GetData<GROUP_TYPE>(group_vector);
		if (!SljitFlatKeysHaveConsecutiveRepeat(group_data, count)) {
			return false;
		}
		auto payload_data = FlatVector::GetData<SOURCE_TYPE>(payload_vector);
		scratch.Prepare(payload_lanes, count);
		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<GROUP_TYPE>(target);
		auto &sums = scratch.payloads[0].hugeint_values;
		idx_t group_count = 0;
		GROUP_TYPE active_key {};
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto key = group_data[row_idx];
			if (group_count == 0 || !(key == active_key)) {
				active_key = key;
				target_data[group_count] = key;
				sums.emplace_back(0);
				group_count++;
			}
			sums[group_count - 1] += SljitPreaggregateToHugeint(payload_data[row_idx]);
		}
		if (group_count == count) {
			return false;
		}
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	template <class GROUP_TYPE>
	static bool TryPreaggregateFlatAllValidSingleInt64SumGroupBySource(
	    PhysicalType source_type, DataChunk &input, idx_t group_source_index, idx_t payload_source_index,
	    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &scratch,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		switch (source_type) {
		case PhysicalType::INT8:
			return TryPreaggregateFlatAllValidSingleInt64SumGroup<GROUP_TYPE, int8_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::INT16:
			return TryPreaggregateFlatAllValidSingleInt64SumGroup<GROUP_TYPE, int16_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::INT32:
			return TryPreaggregateFlatAllValidSingleInt64SumGroup<GROUP_TYPE, int32_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::INT64:
			return TryPreaggregateFlatAllValidSingleInt64SumGroup<GROUP_TYPE, int64_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::UINT8:
			return TryPreaggregateFlatAllValidSingleInt64SumGroup<GROUP_TYPE, uint8_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::UINT16:
			return TryPreaggregateFlatAllValidSingleInt64SumGroup<GROUP_TYPE, uint16_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::UINT32:
			return TryPreaggregateFlatAllValidSingleInt64SumGroup<GROUP_TYPE, uint32_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		default:
			return false;
		}
	}

	template <class GROUP_TYPE>
	static bool TryPreaggregateFlatAllValidSingleHugeintSumGroupBySource(
	    PhysicalType source_type, DataChunk &input, idx_t group_source_index, idx_t payload_source_index,
	    DataChunk &compact_groups, SljitPreaggregatedPrimitiveAggregateScratch &scratch,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes) {
		switch (source_type) {
		case PhysicalType::INT8:
			return TryPreaggregateFlatAllValidSingleHugeintSumGroup<GROUP_TYPE, int8_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::INT16:
			return TryPreaggregateFlatAllValidSingleHugeintSumGroup<GROUP_TYPE, int16_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::INT32:
			return TryPreaggregateFlatAllValidSingleHugeintSumGroup<GROUP_TYPE, int32_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::INT64:
			return TryPreaggregateFlatAllValidSingleHugeintSumGroup<GROUP_TYPE, int64_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::INT128:
			return TryPreaggregateFlatAllValidSingleHugeintSumGroup<GROUP_TYPE, hugeint_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::UINT8:
			return TryPreaggregateFlatAllValidSingleHugeintSumGroup<GROUP_TYPE, uint8_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::UINT16:
			return TryPreaggregateFlatAllValidSingleHugeintSumGroup<GROUP_TYPE, uint16_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		case PhysicalType::UINT32:
			return TryPreaggregateFlatAllValidSingleHugeintSumGroup<GROUP_TYPE, uint32_t>(
			    input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		default:
			return false;
		}
	}

	template <class GROUP_TYPE>
	static bool TryPreaggregateFlatAllValidSinglePrimitiveGroup(
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
		if (payload_lanes.size() != 1) {
			return false;
		}
		auto &sink_info = op.aggregate_update.plan.sink_info;
		auto &aggregate = sink_info.aggregates[0];
		auto lane = payload_lanes[0];
		if (!lane) {
			return false;
		}
		const auto group_source_index = sink_info.groups[0].input_index;
		if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			return TryPreaggregateFlatAllValidSingleCountStarGroup<GROUP_TYPE>(input, group_source_index,
			                                                                   compact_groups, scratch, payload_lanes);
		}
		if (aggregate.child_indices.size() != 1 || aggregate.child_indices[0] >= input.ColumnCount()) {
			return false;
		}
		const auto payload_source_index = aggregate.child_indices[0];
		const auto source_type = input.data[payload_source_index].GetType().InternalType();
		if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			return TryPreaggregateFlatAllValidSingleInt64SumGroupBySource<GROUP_TYPE>(
			    source_type, input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		}
		if (lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			return TryPreaggregateFlatAllValidSingleHugeintSumGroupBySource<GROUP_TYPE>(
			    source_type, input, group_source_index, payload_source_index, compact_groups, scratch, payload_lanes);
		}
		return false;
	}

	template <class GROUP_TYPE>
	static bool TryPreaggregateBoundedPrimitiveGroupsTemplated(
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
		const auto count = input.size();
		auto &sink_info = op.aggregate_update.plan.sink_info;
		if (count < 2 || sink_info.groups.size() != 1 || compact_groups.ColumnCount() != 1) {
			return false;
		}
		const auto group_source_index = sink_info.groups[0].input_index;
		if (group_source_index >= input.ColumnCount()) {
			return false;
		}

		UnifiedVectorFormat group_format;
		input.data[group_source_index].ToUnifiedFormat(group_format);
		auto group_data = UnifiedVectorFormat::GetData<GROUP_TYPE>(group_format);
		auto group_sel = group_format.sel;
		auto &group_validity = group_format.validity;
		const bool can_have_null = group_validity.CanHaveNull();

		vector<SljitPreaggregatedPrimitivePayloadSource> payload_sources;
		if (!PrepareSljitPreaggregatedPrimitivePayloadSources(op, input, payload_lanes, payload_sources)) {
			return false;
		}
		scratch.Prepare(payload_lanes, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT);

		std::array<GROUP_TYPE, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> keys;
		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<GROUP_TYPE>(target);
		idx_t group_count = 0;

		auto append_group = [&](GROUP_TYPE key) {
			if (group_count == SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT) {
				return DConstants::INVALID_INDEX;
			}
			const auto group_idx = group_count++;
			keys[group_idx] = key;
			target_data[group_idx] = key;
			for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
				auto &payload = scratch.payloads[payload_idx];
				switch (payload.kind) {
				case AggregatePrimitiveUpdateKind::COUNT_STAR:
				case AggregatePrimitiveUpdateKind::SUM_INT64:
					payload.int64_values.push_back(0);
					break;
				case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
					payload.hugeint_values.emplace_back(0);
					break;
				default:
					break;
				}
			}
			return group_idx;
		};

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_idx = group_sel->get_index(row_idx);
			if (can_have_null && !group_validity.RowIsValid(source_idx)) {
				return false;
			}
			auto key = group_data[source_idx];
			idx_t group_idx = 0;
			for (; group_idx < group_count; group_idx++) {
				if (keys[group_idx] == key) {
					break;
				}
			}
			if (group_idx == group_count) {
				group_idx = append_group(key);
				if (group_idx == DConstants::INVALID_INDEX) {
					return false;
				}
			}
			for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
				auto lane = payload_lanes[payload_idx];
				auto &payload = scratch.payloads[payload_idx];
				switch (lane->kind) {
				case AggregatePrimitiveUpdateKind::COUNT_STAR:
					payload.int64_values[group_idx]++;
					break;
				case AggregatePrimitiveUpdateKind::SUM_INT64: {
					int64_t value;
					if (!SljitLoadPreaggregatedInt64Payload(payload_sources[payload_idx], row_idx, value)) {
						return false;
					}
					payload.int64_values[group_idx] += value;
					break;
				}
				case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
					hugeint_t value;
					if (!SljitLoadPreaggregatedHugeintPayload(payload_sources[payload_idx], row_idx, value)) {
						return false;
					}
					payload.hugeint_values[group_idx] += value;
					break;
				}
				default:
					return false;
				}
			}
		}
		if (group_count == count) {
			return false;
		}
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	template <class T>
	static bool TryPreaggregateConsecutivePrimitiveGroupsTemplated(
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
		const auto count = input.size();
		auto &sink_info = op.aggregate_update.plan.sink_info;
		if (count < 2 || sink_info.groups.size() != 1 || compact_groups.ColumnCount() != 1) {
			return false;
		}
		const auto group_source_index = sink_info.groups[0].input_index;
		if (group_source_index >= input.ColumnCount()) {
			return false;
		}

		UnifiedVectorFormat group_format;
		input.data[group_source_index].ToUnifiedFormat(group_format);
		auto group_data = UnifiedVectorFormat::GetData<T>(group_format);
		auto group_sel = group_format.sel;
		auto &group_validity = group_format.validity;
		const auto sample_count = MinValue<idx_t>(count, 64);
		bool has_consecutive_repeat = false;
		bool monotonic_nondecreasing = true;
		auto previous_key = group_data[group_sel->get_index(0)];
		if (!group_validity.RowIsValid(group_sel->get_index(0))) {
			return false;
		}
		for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
			const auto source_idx = group_sel->get_index(row_idx);
			if (!group_validity.RowIsValid(source_idx)) {
				return false;
			}
			auto key = group_data[source_idx];
			if (key == previous_key) {
				has_consecutive_repeat = true;
			}
			if (key < previous_key) {
				monotonic_nondecreasing = false;
			}
			previous_key = key;
		}
		const bool prefer_consecutive_runs = monotonic_nondecreasing && has_consecutive_repeat;
		if (!prefer_consecutive_runs &&
		    TryPreaggregateBoundedPrimitiveGroupsTemplated<T>(op, input, payload_lanes, compact_groups, scratch)) {
			return true;
		}
		if (TryPreaggregateFlatAllValidSinglePrimitiveGroup<T>(op, input, payload_lanes, compact_groups, scratch)) {
			return true;
		}
		if (!has_consecutive_repeat) {
			return false;
		}

		vector<SljitPreaggregatedPrimitivePayloadSource> payload_sources;
		if (!PrepareSljitPreaggregatedPrimitivePayloadSources(op, input, payload_lanes, payload_sources)) {
			return false;
		}
		scratch.Prepare(payload_lanes, count);

		compact_groups.Reset();
		auto &target = compact_groups.data[0];
		target.SetVectorType(VectorType::FLAT_VECTOR);
		auto target_data = FlatVector::GetDataMutable<T>(target);
		idx_t group_count = 0;
		T active_key {};
		bool has_active_key = false;

		auto start_group = [&](T key) {
			active_key = key;
			has_active_key = true;
			target_data[group_count] = key;
			for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
				auto &payload = scratch.payloads[payload_idx];
				switch (payload.kind) {
				case AggregatePrimitiveUpdateKind::COUNT_STAR:
				case AggregatePrimitiveUpdateKind::SUM_INT64:
					payload.int64_values.push_back(0);
					break;
				case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
					payload.hugeint_values.emplace_back(0);
					break;
				default:
					break;
				}
			}
			group_count++;
		};

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto source_idx = group_sel->get_index(row_idx);
			if (!group_validity.RowIsValid(source_idx)) {
				return false;
			}
			auto key = group_data[source_idx];
			if (!has_active_key || !(key == active_key)) {
				start_group(key);
			}
			const auto payload_group_idx = group_count - 1;
			for (idx_t payload_idx = 0; payload_idx < payload_lanes.size(); payload_idx++) {
				auto lane = payload_lanes[payload_idx];
				auto &payload = scratch.payloads[payload_idx];
				switch (lane->kind) {
				case AggregatePrimitiveUpdateKind::COUNT_STAR:
					payload.int64_values[payload_group_idx]++;
					break;
				case AggregatePrimitiveUpdateKind::SUM_INT64: {
					int64_t value;
					if (!SljitLoadPreaggregatedInt64Payload(payload_sources[payload_idx], row_idx, value)) {
						return false;
					}
					payload.int64_values[payload_group_idx] += value;
					break;
				}
				case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
					hugeint_t value;
					if (!SljitLoadPreaggregatedHugeintPayload(payload_sources[payload_idx], row_idx, value)) {
						return false;
					}
					payload.hugeint_values[payload_group_idx] += value;
					break;
				}
				default:
					return false;
				}
			}
		}
		if (group_count == count) {
			return false;
		}
		auto &target_validity = FlatVector::ValidityMutable(target);
		target_validity.Reset(group_count);
		target_validity.SetAllValid(group_count);
		FlatVector::SetSize(target, count_t(group_count));
		compact_groups.SetChildCardinality(group_count);
		return true;
	}

	static bool TryPreaggregateConsecutivePrimitiveGroups(
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &scratch) {
		auto &sink_info = op.aggregate_update.plan.sink_info;
		if (sink_info.groups.size() != 1 || compact_groups.ColumnCount() != 1) {
			return false;
		}
		const auto group_source_index = sink_info.groups[0].input_index;
		if (group_source_index >= input.ColumnCount() ||
		    input.data[group_source_index].GetType() != sink_info.groups[0].type ||
		    compact_groups.data[0].GetType() != sink_info.groups[0].type) {
			return false;
		}
		switch (input.data[group_source_index].GetType().InternalType()) {
		case PhysicalType::INT8:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<int8_t>(op, input, payload_lanes, compact_groups,
			                                                                  scratch);
		case PhysicalType::INT16:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<int16_t>(op, input, payload_lanes, compact_groups,
			                                                                   scratch);
		case PhysicalType::INT32:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<int32_t>(op, input, payload_lanes, compact_groups,
			                                                                   scratch);
		case PhysicalType::INT64:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<int64_t>(op, input, payload_lanes, compact_groups,
			                                                                   scratch);
		case PhysicalType::INT128:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<hugeint_t>(op, input, payload_lanes,
			                                                                     compact_groups, scratch);
		case PhysicalType::UINT8:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uint8_t>(op, input, payload_lanes, compact_groups,
			                                                                   scratch);
		case PhysicalType::UINT16:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uint16_t>(op, input, payload_lanes,
			                                                                    compact_groups, scratch);
		case PhysicalType::UINT32:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uint32_t>(op, input, payload_lanes,
			                                                                    compact_groups, scratch);
		case PhysicalType::UINT64:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uint64_t>(op, input, payload_lanes,
			                                                                    compact_groups, scratch);
		case PhysicalType::UINT128:
			return TryPreaggregateConsecutivePrimitiveGroupsTemplated<uhugeint_t>(op, input, payload_lanes,
			                                                                      compact_groups, scratch);
		default:
			return false;
		}
	}

	bool CanUseExtendedRegularHashAggregateSourceBudget() const {
		bool has_hash_join_probe = false;
		for (auto &op : ops) {
			if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
				has_hash_join_probe = true;
				continue;
			}
			if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
				return has_hash_join_probe &&
				       op.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
			}
		}
		return false;
	}

	bool CanUseExtendedNativeSinkSourceBudget() const {
		if (ops.empty()) {
			return false;
		}
		switch (ops.back().kind) {
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
			return true;
		default:
			return false;
		}
	}

	bool TryAppendReferenceProjectionToBatch(DataChunk &batch, DataChunk &input,
	                                         const SljitExecutableRegionOp &projection_op,
	                                         const SelectionVector &selection, idx_t count) {
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    projection_op.projections.size() != batch.ColumnCount()) {
			return false;
		}
		const auto current_size = batch.size();
		if (current_size + count > STANDARD_VECTOR_SIZE) {
			return false;
		}
		for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
			auto &projection = projection_op.projections[projection_idx].plan;
			if (projection.kind != SljitNativeRegionExpressionKind::REFERENCE ||
			    projection.source_index >= input.ColumnCount()) {
				return false;
			}
			auto &target = batch.data[projection_idx];
			auto &source = input.data[projection.source_index];
			if (target.size() != current_size || target.GetType() != source.GetType() ||
			    projection.return_type != source.GetType()) {
				return false;
			}
		}
		for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
			auto &projection = projection_op.projections[projection_idx].plan;
			batch.data[projection_idx].Append(input.data[projection.source_index], selection, count);
		}
		batch.CheckCardinality(current_size + count);
		return true;
	}

	bool TryReferenceProjection(DataChunk &output, DataChunk &input, const SljitExecutableRegionOp &projection_op) {
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    projection_op.projections.size() != output.ColumnCount()) {
			return false;
		}
		for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
			auto &projection = projection_op.projections[projection_idx].plan;
			if (projection.kind != SljitNativeRegionExpressionKind::REFERENCE ||
			    projection.source_index >= input.ColumnCount()) {
				return false;
			}
			auto &source = input.data[projection.source_index];
			if (projection.return_type != source.GetType() ||
			    output.data[projection_idx].GetType() != source.GetType()) {
				return false;
			}
		}
		for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
			auto &projection = projection_op.projections[projection_idx].plan;
			output.data[projection_idx].Reference(input.data[projection.source_index]);
		}
		output.SetChildCardinality(input.size());
		return true;
	}

	bool SelectedProjectionHasGeneratedExpression(const SljitExecutableRegionOp &projection_op,
	                                              const vector<uint8_t> &skip_projection) const {
		for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
			if (projection_idx < skip_projection.size() && skip_projection[projection_idx]) {
				continue;
			}
			if (projection_op.projections[projection_idx].plan.kind != SljitNativeRegionExpressionKind::REFERENCE) {
				return true;
			}
		}
		return false;
	}

	bool
	PrepareDirectProjectionBatchTargets(DataChunk &batch, SljitExecutableRegionOp &projection_op,
	                                    optional_ptr<const vector<idx_t>> output_to_projection,
	                                    DirectAppendSlice &slice, vector<uint8_t> &skip_projection,
	                                    vector<idx_t> &projection_to_output,
	                                    optional_ptr<const vector<uint8_t>> initial_skip_projection = nullptr) const {
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.empty()) {
			return false;
		}
		const auto current_size = batch.size();
		const auto target_size = current_size + slice.count;
		if (target_size > STANDARD_VECTOR_SIZE) {
			return false;
		}

		const auto output_count =
		    output_to_projection ? output_to_projection->size() : projection_op.projections.size();
		if (batch.ColumnCount() != output_count) {
			return false;
		}
		slice.targets.assign(projection_op.projections.size(), nullptr);
		slice.sources.assign(projection_op.projections.size(), DirectAppendColumnSource());
		skip_projection.assign(projection_op.projections.size(), output_to_projection ? 1 : 0);
		projection_to_output.assign(projection_op.projections.size(), DConstants::INVALID_INDEX);
		if (initial_skip_projection) {
			if (initial_skip_projection->size() > skip_projection.size()) {
				return false;
			}
			for (idx_t projection_idx = 0; projection_idx < initial_skip_projection->size(); projection_idx++) {
				if ((*initial_skip_projection)[projection_idx]) {
					skip_projection[projection_idx] = 1;
				}
			}
		}

		for (idx_t output_idx = 0; output_idx < output_count; output_idx++) {
			const auto projection_idx = output_to_projection ? (*output_to_projection)[output_idx] : output_idx;
			if (projection_idx >= projection_op.projections.size() || slice.targets[projection_idx]) {
				return false;
			}
			if (projection_idx < skip_projection.size() && skip_projection[projection_idx]) {
				continue;
			}
			auto &target = batch.data[output_idx];
			auto &projection = projection_op.projections[projection_idx].plan;
			if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != projection.return_type ||
			    !DirectAppendSupportsFixedSizeType(target.GetType()) || FlatVector::GetCapacity(target) < target_size) {
				return false;
			}
			auto target_data = FlatVector::GetDataMutable(target);
			if (!target_data) {
				return false;
			}
			slice.targets[projection_idx] = target_data + current_size * GetTypeIdSize(target.GetType().InternalType());
			skip_projection[projection_idx] = 0;
			projection_to_output[projection_idx] = output_idx;
		}
		return SelectedProjectionHasGeneratedExpression(projection_op, skip_projection);
	}

	void FinishDirectProjectionBatchTargets(DataChunk &batch, idx_t target_size, bool new_rows_all_valid = true) const {
		for (auto &target : batch.data) {
			if (new_rows_all_valid) {
				auto &validity = FlatVector::ValidityMutable(target);
				if (validity.CanHaveNull()) {
					for (idx_t row_idx = target.size(); row_idx < target_size; row_idx++) {
						validity.SetValid(row_idx);
					}
				}
			}
			FlatVector::SetSize(target, target_size);
		}
		batch.CheckCardinality(target_size);
	}

	bool FlatFusedFixedProjectionTargetsAreBound(SljitExecutableRegionOp &projection_op,
	                                             DirectAppendSlice &slice) const {
		if (slice.targets.size() != projection_op.projections.size()) {
			return false;
		}
		for (auto &direct_plan : projection_op.flat_fused_fixed_projection_plans) {
			for (auto projection_idx : direct_plan.projection_indices) {
				if (projection_idx >= slice.targets.size() || !slice.targets[projection_idx]) {
					return false;
				}
			}
		}
		return true;
	}

	void BuildPostFusedProjectionSkip(const vector<uint8_t> &skip_projection, const vector<uint8_t> &fused_projection,
	                                  vector<uint8_t> &post_fused_skip) const {
		post_fused_skip = skip_projection;
		if (post_fused_skip.size() < fused_projection.size()) {
			post_fused_skip.resize(fused_projection.size(), 0);
		}
		for (idx_t projection_idx = 0; projection_idx < fused_projection.size(); projection_idx++) {
			if (fused_projection[projection_idx]) {
				post_fused_skip[projection_idx] = 1;
			}
		}
	}

	void CopyDirectProjectionResultToBatch(Vector &result, Vector &target, data_ptr_t target_data, idx_t current_size,
	                                       idx_t count) const {
		const bool wrote_target =
		    result.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::GetData(result) == target_data;
		if (wrote_target) {
			auto &target_validity = FlatVector::ValidityMutable(target);
			target_validity.CopySel(FlatVector::Validity(result), *FlatVector::IncrementalSelectionVector(), 0,
			                        current_size, count);
		} else {
			target.Copy(result, *FlatVector::IncrementalSelectionVector(), count, 0, current_size, count);
		}
	}

	bool DirectProjectionBatchSupportsType(const LogicalType &type) const {
		return DirectAppendSupportsFixedSizeType(type) || type.id() == LogicalTypeId::VARCHAR;
	}

	bool ProjectionTargetCanReceiveExpression(Vector &target, const LogicalType &return_type, idx_t target_size) const {
		return target.GetVectorType() == VectorType::FLAT_VECTOR && target.GetType() == return_type &&
		       DirectProjectionBatchSupportsType(target.GetType()) && FlatVector::GetCapacity(target) >= target_size;
	}

	bool TryExecuteProjectionExpressionToBatch(SljitExecutableRegionExpression &expr, DataChunk &input, Vector &target,
	                                           idx_t current_size, idx_t count, const SelectionVector *execute_sel,
	                                           SljitExpressionAdapterScratch &adapter_scratch) {
		if (!ProjectionTargetCanReceiveExpression(target, expr.plan.return_type, current_size + count)) {
			return false;
		}
		if (DirectAppendSupportsFixedSizeType(target.GetType())) {
			auto target_data =
			    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
			Vector result(expr.plan.return_type, target_data, count);
			ExecuteProjectionExpression(expr, input, result, execute_sel, count, adapter_scratch);
			CopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
			return true;
		}
		Vector result(expr.plan.return_type);
		ExecuteProjectionExpression(expr, input, result, execute_sel, count, adapter_scratch);
		target.Copy(result, *FlatVector::IncrementalSelectionVector(), count, 0, current_size, count);
		return true;
	}

	void ExecuteProjectionExpressionsToBatch(SljitRegionExecutionScratch &scratch, idx_t projection_idx,
	                                         SljitExecutableRegionOp &projection_op, DataChunk &input, DataChunk &batch,
	                                         const vector<uint8_t> &skip_projection,
	                                         const vector<idx_t> &projection_to_output) {
		const auto current_size = batch.size();
		const auto count = input.size();
		for (idx_t projected_idx = 0; projected_idx < projection_op.projections.size(); projected_idx++) {
			if (projected_idx < skip_projection.size() && skip_projection[projected_idx]) {
				continue;
			}
			if (projected_idx >= projection_to_output.size() ||
			    projection_to_output[projected_idx] == DConstants::INVALID_INDEX) {
				throw InternalException("SLJIT direct batch projection target mapping is missing");
			}
			const auto output_idx = projection_to_output[projected_idx];
			auto &target = batch.data[output_idx];
			auto &projection = projection_op.projections[projected_idx];
			auto target_data =
			    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
			Vector result(projection.plan.return_type, target_data, count);
			ExecuteProjectionExpression(projection, input, result, nullptr, count,
			                            scratch.ExpressionAdapterScratch(projection_idx, projected_idx));
			CopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
		}
		FinishDirectProjectionBatchTargets(batch, current_size + count, false);
	}

	void RecordDirectProjectionBatchMaterialization(ExecutionRegionRuntime &runtime, idx_t projection_idx,
	                                                SljitExecutableRegionOp &projection_op, bool remapped_projection,
	                                                idx_t count, std::chrono::steady_clock::time_point stage_start) {
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
		                              remapped_projection ? "post_join_direct_remap_batch_projection"
		                                                  : "post_join_direct_batch_projection",
		                              stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind,
		                                         remapped_projection ? "direct_remap_post_join_batch_projection"
		                                                             : "direct_post_join_batch_projection",
		                                         count);
	}

	void HashDirectProjectionBatch(ExecutionRegionRuntime &runtime, idx_t projection_idx,
	                               SljitExecutableRegionOp &projection_op, bool remapped_projection, DataChunk &batch,
	                               Vector &hashes) {
		auto hash_start = SljitRegionStageStart(runtime);
		batch.Hash(hashes);
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
		                              remapped_projection ? "post_join_direct_remap_batch_projection_hash"
		                                                  : "post_join_direct_batch_projection_hash",
		                              hash_start);
	}

	bool TryDirectMaterializeFixedProjectionToBatch(ExecutionRegionRuntime &runtime,
	                                                SljitRegionExecutionScratch &scratch, idx_t projection_idx,
	                                                SljitExecutableRegionOp &projection_op, DataChunk &input,
	                                                DataChunk &batch,
	                                                optional_ptr<const vector<idx_t>> output_to_projection = nullptr,
	                                                optional_ptr<const vector<uint8_t>> extra_skip_projection = nullptr,
	                                                optional_ptr<Vector> projected_hashes = nullptr) {
		if (input.size() == 0) {
			return false;
		}

		const auto current_size = batch.size();
		DirectAppendSlice slice;
		slice.source_offset = 0;
		slice.count = input.size();
		vector<uint8_t> skip_projection;
		vector<idx_t> projection_to_output;
		if (!PrepareDirectProjectionBatchTargets(batch, projection_op, output_to_projection, slice, skip_projection,
		                                         projection_to_output, extra_skip_projection)) {
			return false;
		}

		FixedDirectAppendSourceCache source_cache;
		source_cache.Reset(input.ColumnCount());
		auto source_cache_ptr = optional_ptr<FixedDirectAppendSourceCache>(&source_cache);
		const auto stage_start = SljitRegionStageStart(runtime);
		auto &projection_scratch = scratch.ProjectionScratch(projection_idx);
		const bool remapped_projection = output_to_projection;
		if (TryPrepareFlatFusedFixedProjectionSources(projection_op, input, slice.source_offset, slice.count,
		                                              source_cache_ptr, projection_scratch) &&
		    FlatFusedFixedProjectionTargetsAreBound(projection_op, slice)) {
			vector<uint8_t> post_fused_skip;
			BuildPostFusedProjectionSkip(skip_projection, projection_scratch.fused, post_fused_skip);
			auto post_fused_skip_ptr = optional_ptr<vector<uint8_t>>(&post_fused_skip);
			if (!TryDirectMaterializeFixedProjection(projection_op, input, nullptr, source_cache_ptr,
			                                         post_fused_skip_ptr)) {
				ExecuteProjectionExpressionsToBatch(scratch, projection_idx, projection_op, input, batch,
				                                    skip_projection, projection_to_output);
				if (projected_hashes) {
					HashDirectProjectionBatch(runtime, projection_idx, projection_op, remapped_projection, batch,
					                          *projected_hashes);
				}
				RecordDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, remapped_projection,
				                                           input.size(), stage_start);
				return true;
			}
			BindFlatFusedFixedProjectionTargets(projection_op, slice, projection_scratch);
			RunFlatFusedFixedProjection(projection_op, slice.count, projection_scratch);
			if (!TryDirectMaterializeFixedProjection(projection_op, input, &slice, source_cache_ptr,
			                                         post_fused_skip_ptr,
			                                         optional_ptr<ExecutionRegionRuntime>(&runtime), projection_idx)) {
				throw InternalException(
				    "SLJIT fixed fused direct batch projection source shape changed after preflight");
			}
		} else {
			auto skip_projection_ptr = optional_ptr<vector<uint8_t>>(&skip_projection);
			if (!TryDirectMaterializeFixedProjection(projection_op, input, nullptr, source_cache_ptr,
			                                         skip_projection_ptr)) {
				ExecuteProjectionExpressionsToBatch(scratch, projection_idx, projection_op, input, batch,
				                                    skip_projection, projection_to_output);
				if (projected_hashes) {
					HashDirectProjectionBatch(runtime, projection_idx, projection_op, remapped_projection, batch,
					                          *projected_hashes);
				}
				RecordDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, remapped_projection,
				                                           input.size(), stage_start);
				return true;
			}
			if (!TryDirectMaterializeFixedProjection(projection_op, input, &slice, source_cache_ptr,
			                                         skip_projection_ptr,
			                                         optional_ptr<ExecutionRegionRuntime>(&runtime), projection_idx)) {
				throw InternalException("SLJIT fixed direct batch projection source shape changed after preflight");
			}
		}

		FinishDirectProjectionBatchTargets(batch, current_size + input.size());
		if (projected_hashes) {
			HashDirectProjectionBatch(runtime, projection_idx, projection_op, remapped_projection, batch,
			                          *projected_hashes);
		}
		RecordDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, remapped_projection,
		                                           input.size(), stage_start);
		return true;
	}

	struct DirectProjectionBatchPassthrough {
		idx_t output_idx = DConstants::INVALID_INDEX;
		Vector *source = nullptr;
		const SelectionVector *selection = nullptr;
		const char *trace_phase = "direct_batch_expression.passthrough";
	};

	static optional_ptr<const DirectProjectionBatchPassthrough>
	FindDirectProjectionBatchPassthrough(optional_ptr<const vector<DirectProjectionBatchPassthrough>> passthroughs,
	                                     idx_t output_idx) {
		if (!passthroughs) {
			return nullptr;
		}
		for (auto &passthrough : *passthroughs) {
			if (passthrough.output_idx == output_idx) {
				return optional_ptr<const DirectProjectionBatchPassthrough>(&passthrough);
			}
		}
		return nullptr;
	}

	bool TryCopyDirectProjectionPassthroughToBatch(const DirectProjectionBatchPassthrough &passthrough, Vector &target,
	                                               idx_t current_size, idx_t count) const {
		if (!passthrough.source || passthrough.source->GetType() != target.GetType() ||
		    target.GetVectorType() != VectorType::FLAT_VECTOR ||
		    FlatVector::GetCapacity(target) < current_size + count) {
			return false;
		}
		const auto &selection =
		    passthrough.selection ? *passthrough.selection : *FlatVector::IncrementalSelectionVector();
		target.Copy(*passthrough.source, selection, count, 0, current_size, count);
		return true;
	}

	bool TryMaterializeSelectedProjectionToBatch(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t projection_idx,
	    SljitExecutableRegionOp &projection_op, DataChunk &input, DataChunk &batch,
	    const vector<idx_t> &output_to_projection, optional_ptr<Vector> projected_hashes = nullptr,
	    optional_ptr<const vector<DirectProjectionBatchPassthrough>> passthroughs = nullptr) {
		if (input.size() == 0 || projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    batch.ColumnCount() != output_to_projection.size() || batch.size() + input.size() > STANDARD_VECTOR_SIZE) {
			return false;
		}
		const auto current_size = batch.size();
		const auto count = input.size();
		FixedDirectAppendSourceCache source_cache;
		source_cache.Reset(input.ColumnCount());
		auto source_cache_ptr = optional_ptr<FixedDirectAppendSourceCache>(&source_cache);
		auto stage_start = SljitRegionStageStart(runtime);
		bool all_direct_fixed = true;
		bool used_passthrough = false;
		for (idx_t output_idx = 0; output_idx < output_to_projection.size(); output_idx++) {
			const auto projected_idx = output_to_projection[output_idx];
			if (projected_idx >= projection_op.projections.size()) {
				return false;
			}
			auto &projection = projection_op.projections[projected_idx];
			auto &target = batch.data[output_idx];
			if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != projection.plan.return_type ||
			    FlatVector::GetCapacity(target) < current_size + count) {
				return false;
			}
			auto target_data =
			    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
			auto expression_stage_start =
			    runtime.TraceRuntime() ? SljitRegionStageStart(runtime) : std::chrono::steady_clock::time_point();
			auto passthrough = FindDirectProjectionBatchPassthrough(passthroughs, output_idx);
			if (passthrough && TryCopyDirectProjectionPassthroughToBatch(*passthrough, target, current_size, count)) {
				all_direct_fixed = false;
				used_passthrough = true;
				if (runtime.TraceRuntime()) {
					RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind, passthrough->trace_phase,
					                              expression_stage_start);
				}
				continue;
			}
			if (DirectAppendSupportsFixedSizeType(target.GetType()) &&
			    TryDirectMaterializeFixedExpression(projection, input, target_data, 0, count, true, source_cache_ptr)) {
				if (runtime.TraceRuntime()) {
					RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
					                              SljitFixedProjectionExpressionTracePhase(projection.plan),
					                              expression_stage_start);
				}
				continue;
			}
			all_direct_fixed = false;
			Vector result(projection.plan.return_type, target_data, count);
			ExecuteProjectionExpression(projection, input, result, nullptr, count,
			                            scratch.ExpressionAdapterScratch(projection_idx, projected_idx));
			CopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
			if (runtime.TraceRuntime()) {
				RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
				                              SljitFixedProjectionExpressionTracePhase(projection.plan),
				                              expression_stage_start);
			}
		}
		FinishDirectProjectionBatchTargets(batch, current_size + count, all_direct_fixed);
		if (projected_hashes) {
			HashDirectProjectionBatch(runtime, projection_idx, projection_op, true, batch, *projected_hashes);
		}
		if (used_passthrough) {
			RecordSljitRegionRuntimePath(runtime, projection_op.kind, "direct_batch_passthrough_projection", count);
		}
		RecordDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op, true, count, stage_start);
		return true;
	}

	bool BuildReferenceProjectionOutputMap(const SljitExecutableRegionOp &projection_op,
	                                       const SljitExecutableRegionOp &reference_projection_op,
	                                       vector<idx_t> &output_to_projection) const {
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    reference_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
		    !ProjectionOutputsAreFixedWidth(reference_projection_op)) {
			return false;
		}
		output_to_projection.clear();
		output_to_projection.reserve(reference_projection_op.projections.size());
		for (auto &reference_expr : reference_projection_op.projections) {
			auto &reference = reference_expr.plan;
			if (reference.kind != SljitNativeRegionExpressionKind::REFERENCE ||
			    reference.source_index >= projection_op.projections.size() ||
			    reference.return_type != projection_op.projections[reference.source_index].plan.return_type) {
				output_to_projection.clear();
				return false;
			}
			output_to_projection.push_back(reference.source_index);
		}
		return true;
	}

	bool AddProjectionSourceColumn(idx_t source_index, idx_t input_column_count, vector<uint8_t> &referenced) const {
		if (source_index >= input_column_count) {
			return false;
		}
		referenced[source_index] = 1;
		return true;
	}

	bool AddProjectionSourceColumns(const vector<idx_t> &source_indices, idx_t input_column_count,
	                                vector<uint8_t> &referenced) const {
		for (auto source_index : source_indices) {
			if (!AddProjectionSourceColumn(source_index, input_column_count, referenced)) {
				return false;
			}
		}
		return true;
	}

	bool AddProjectionPredicateSourceColumns(const SljitNativePredicate &predicate, idx_t input_column_count,
	                                         vector<uint8_t> &referenced) const {
		if (!AddProjectionSourceColumns(predicate.source_indices, input_column_count, referenced)) {
			return false;
		}
		switch (predicate.kind) {
		case SljitNativePredicateKind::REFERENCE:
		case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
		case SljitNativePredicateKind::INT128_COMPARE_CONSTANT:
		case SljitNativePredicateKind::INTEGER_IN_LIST:
		case SljitNativePredicateKind::INTEGER_BETWEEN:
		case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
		case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
		case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
		case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
		case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
		case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
		case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
		case SljitNativePredicateKind::NULL_CHECK:
			if (!AddProjectionSourceColumn(predicate.source_index, input_column_count, referenced)) {
				return false;
			}
			break;
		case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
		case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
		case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
			if (!AddProjectionSourceColumn(predicate.source_index, input_column_count, referenced) ||
			    !AddProjectionSourceColumn(predicate.right_source_index, input_column_count, referenced)) {
				return false;
			}
			break;
		default:
			break;
		}
		if (predicate.child && !AddProjectionPredicateSourceColumns(*predicate.child, input_column_count, referenced)) {
			return false;
		}
		for (auto &child : predicate.children) {
			if (!AddProjectionPredicateSourceColumns(*child, input_column_count, referenced)) {
				return false;
			}
		}
		return true;
	}

	bool AddProjectionExpressionSourceColumns(const SljitNativeRegionExpressionPlan &plan, idx_t input_column_count,
	                                          vector<uint8_t> &referenced) const {
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
			return AddProjectionSourceColumn(plan.source_index, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
			return AddProjectionSourceColumn(plan.source_index, input_column_count, referenced) &&
			       AddProjectionSourceColumn(plan.right_source_index, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
			if (!AddProjectionSourceColumn(plan.source_index, input_column_count, referenced)) {
				return false;
			}
			if (plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE &&
			    !AddProjectionSourceColumn(plan.right_source_index, input_column_count, referenced)) {
				return false;
			}
			return true;
		case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
			return AddProjectionSourceColumns(plan.constant_or_null.guard_source_indices, input_column_count,
			                                  referenced);
		case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
			return AddProjectionSourceColumn(plan.source_index, input_column_count, referenced) &&
			       AddProjectionSourceColumn(plan.guard_source_index, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::PREDICATE:
			if (plan.predicate) {
				return AddProjectionPredicateSourceColumns(*plan.predicate, input_column_count, referenced);
			}
			return AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			return AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::CONSTANT:
			return true;
		default:
			return false;
		}
	}

	bool BuildProjectionSourceColumnSet(const SljitExecutableRegionOp &projection_op, idx_t input_column_count,
	                                    optional_ptr<const vector<idx_t>> output_to_projection,
	                                    optional_ptr<const vector<uint8_t>> skip_projection,
	                                    vector<uint8_t> &referenced) const {
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || input_column_count == 0) {
			return false;
		}
		referenced.assign(input_column_count, 0);
		if (output_to_projection) {
			for (auto projection_idx : *output_to_projection) {
				if (skip_projection && projection_idx < skip_projection->size() && (*skip_projection)[projection_idx]) {
					continue;
				}
				if (projection_idx >= projection_op.projections.size() ||
				    !AddProjectionExpressionSourceColumns(projection_op.projections[projection_idx].plan,
				                                          input_column_count, referenced)) {
					return false;
				}
			}
			return true;
		}
		for (idx_t projection_idx = 0; projection_idx < projection_op.projections.size(); projection_idx++) {
			if (skip_projection && projection_idx < skip_projection->size() && (*skip_projection)[projection_idx]) {
				continue;
			}
			auto &projection = projection_op.projections[projection_idx];
			if (!AddProjectionExpressionSourceColumns(projection.plan, input_column_count, referenced)) {
				return false;
			}
		}
		return true;
	}

	static bool ProjectionSkipHasAny(const vector<uint8_t> &skip_projection) {
		for (auto skip : skip_projection) {
			if (skip) {
				return true;
			}
		}
		return false;
	}

	static bool OrProjectionSkips(idx_t projection_count, optional_ptr<const vector<uint8_t>> left,
	                              optional_ptr<const vector<uint8_t>> right, vector<uint8_t> &merged,
	                              optional_ptr<const vector<uint8_t>> &merged_ptr) {
		merged_ptr = nullptr;
		if (!left && !right) {
			return true;
		}
		merged.assign(projection_count, 0);
		auto merge_one = [&](const vector<uint8_t> &skip) {
			if (skip.size() != projection_count) {
				return false;
			}
			for (idx_t projection_idx = 0; projection_idx < projection_count; projection_idx++) {
				if (skip[projection_idx]) {
					merged[projection_idx] = 1;
				}
			}
			return true;
		};
		if (left && !merge_one(*left)) {
			return false;
		}
		if (right && !merge_one(*right)) {
			return false;
		}
		merged_ptr = optional_ptr<const vector<uint8_t>>(&merged);
		return true;
	}

	static bool SourceVectorSelectedRowsAllValid(Vector &source, const SelectionVector &sel, idx_t count) {
		UnifiedVectorFormat source_format;
		source.ToUnifiedFormat(source_format);
		return SljitNormalizedSourceAllValid(source_format, SljitNormalizedSourceSelectionData(source_format), &sel,
		                                     count);
	}

	static bool SourceVectorAllRowsAllValid(Vector &source, idx_t count) {
		UnifiedVectorFormat source_format;
		source.ToUnifiedFormat(source_format);
		return SljitNormalizedSourceAllValid(source_format, SljitNormalizedSourceSelectionData(source_format), count);
	}

	static void GatherHashJoinRHSColumn(const ExecutionHashJoinProbeBinding &binding, Vector &row_pointers, idx_t count,
	                                    idx_t rhs_col_idx, Vector &result) {
		if (ExecutionTryDirectGatherHashJoinRHSFixedColumn(binding, row_pointers, count, rhs_col_idx, result)) {
			return;
		}
		D_ASSERT(binding.hash_table);
		binding.hash_table->GatherRHSColumn(row_pointers, *FlatVector::IncrementalSelectionVector(), count, rhs_col_idx,
		                                    result);
	}

	bool TryMaterializeHashJoinReferenceProjectionsToBatch(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	    idx_t projection_idx, SljitExecutableRegionOp &projection_op, DataChunk &join_input,
	    const SelectionVector &match_selection, Vector &row_pointers, DataChunk &batch,
	    optional_ptr<const vector<idx_t>> output_to_projection, idx_t count, vector<uint8_t> &skip_projection) {
		if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx)) {
			return false;
		}
		auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
		const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
		const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
		if (!binding.ready || (!regular_hash_join && !perfect_hash_join) ||
		    (regular_hash_join && !binding.hash_table) || (perfect_hash_join && !binding.perfect_layout.ready) ||
		    (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		     binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY)) {
			return false;
		}

		const auto current_size = batch.size();
		const auto target_size = current_size + count;
		const auto output_count =
		    output_to_projection ? output_to_projection->size() : projection_op.projections.size();
		if (batch.ColumnCount() != output_count || target_size > STANDARD_VECTOR_SIZE) {
			return false;
		}

		if (skip_projection.empty()) {
			skip_projection.assign(projection_op.projections.size(), 0);
		} else if (skip_projection.size() != projection_op.projections.size()) {
			return false;
		}
		bool materialized_any = false;
		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		const auto rhs_column_count = binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD
		                                  ? binding.rhs_output_column_count
		                                  : 0;
		const auto join_output_column_count = lhs_column_count + rhs_column_count;
		const auto stage_start = SljitRegionStageStart(runtime);

		for (idx_t output_idx = 0; output_idx < output_count; output_idx++) {
			const auto projected_idx = output_to_projection ? (*output_to_projection)[output_idx] : output_idx;
			if (projected_idx >= projection_op.projections.size()) {
				return false;
			}
			if (skip_projection[projected_idx]) {
				continue;
			}
			auto &expr = projection_op.projections[projected_idx];
			SljitExecutableRegionExpression remapped_expr;
			idx_t join_output_source_index;
			if (!TryBuildSingleSourceProjectionExpression(expr, remapped_expr, join_output_source_index) ||
			    !ProjectionIsSingleSourceReferenceLike(remapped_expr.plan) ||
			    join_output_source_index >= join_output_column_count) {
				continue;
			}
			auto &plan = expr.plan;
			auto &target = batch.data[output_idx];
			if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != plan.return_type ||
			    FlatVector::GetCapacity(target) < target_size) {
				continue;
			}

			Vector reference(plan.return_type);
			if (join_output_source_index < lhs_column_count) {
				const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
				if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != plan.return_type ||
				    !SourceVectorSelectedRowsAllValid(join_input.data[input_col], match_selection, count)) {
					continue;
				}
				reference.Slice(join_input.data[input_col], match_selection, count);
			} else {
				if (!regular_hash_join) {
					continue;
				}
				const auto rhs_col_idx = join_output_source_index - lhs_column_count;
				Vector gathered_reference(plan.return_type);
				GatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, gathered_reference);
				if (!SourceVectorAllRowsAllValid(gathered_reference, count)) {
					continue;
				}
				reference.Reference(gathered_reference);
			}
			target.Copy(reference, *FlatVector::IncrementalSelectionVector(), count, 0, current_size, count);
			skip_projection[projected_idx] = 1;
			materialized_any = true;
		}

		if (materialized_any) {
			RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
			                              "post_join_direct_reference_projection", stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind,
			                                         "direct_post_join_reference_projection", count);
		}
		return materialized_any;
	}

	bool TryMaterializeHashJoinOutputReferenceToBatch(const ExecutionHashJoinProbeBinding &binding,
	                                                  DataChunk &join_input, const SelectionVector &match_selection,
	                                                  Vector &row_pointers, idx_t join_output_source_index,
	                                                  Vector &target, idx_t current_size, idx_t count) const {
		if (!binding.ready || !binding.hash_table ||
		    binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
		    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
		    target.GetVectorType() != VectorType::FLAT_VECTOR || !DirectProjectionBatchSupportsType(target.GetType()) ||
		    FlatVector::GetCapacity(target) < current_size + count) {
			return false;
		}
		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		if (join_output_source_index >= binding.output_types.size()) {
			return false;
		}
		if (binding.output_types[join_output_source_index] != target.GetType()) {
			return false;
		}

		Vector reference(target.GetType());
		if (join_output_source_index < lhs_column_count) {
			const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
			if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != target.GetType() ||
			    !SourceVectorSelectedRowsAllValid(join_input.data[input_col], match_selection, count)) {
				return false;
			}
			reference.Slice(join_input.data[input_col], match_selection, count);
		} else {
			const auto rhs_col_idx = join_output_source_index - lhs_column_count;
			if (rhs_col_idx >= binding.rhs_output_column_count) {
				return false;
			}
			GatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, reference);
			if (!SourceVectorAllRowsAllValid(reference, count)) {
				return false;
			}
		}
		target.Copy(reference, *FlatVector::IncrementalSelectionVector(), count, 0, current_size, count);
		return true;
	}

	bool SelectedProjectionOutputsAreSkipped(const SljitExecutableRegionOp &projection_op,
	                                         optional_ptr<const vector<idx_t>> output_to_projection,
	                                         const vector<uint8_t> &skip_projection) const {
		if (output_to_projection) {
			for (auto projected_idx : *output_to_projection) {
				if (projected_idx >= projection_op.projections.size() || projected_idx >= skip_projection.size() ||
				    !skip_projection[projected_idx]) {
					return false;
				}
			}
			return true;
		}
		if (skip_projection.size() < projection_op.projections.size()) {
			return false;
		}
		for (idx_t projected_idx = 0; projected_idx < projection_op.projections.size(); projected_idx++) {
			if (!skip_projection[projected_idx]) {
				return false;
			}
		}
		return true;
	}

	bool TryMapHashJoinProbeLHSOutputColumn(const ExecutionHashJoinProbeBinding &binding, DataChunk &join_input,
	                                        idx_t &source_index) const {
		if (source_index >= binding.lhs_output_column_indices.size()) {
			return false;
		}
		auto input_col = binding.lhs_output_column_indices[source_index];
		if (input_col >= join_input.ColumnCount()) {
			return false;
		}
		source_index = input_col;
		return true;
	}

	bool TryMapHashJoinProbeLHSOutputColumns(const ExecutionHashJoinProbeBinding &binding, DataChunk &join_input,
	                                         vector<idx_t> &source_indices) const {
		for (auto &source_index : source_indices) {
			if (!TryMapHashJoinProbeLHSOutputColumn(binding, join_input, source_index)) {
				return false;
			}
		}
		return true;
	}

	bool TryMapHashJoinProbeLHSConstantOrNullSources(const ExecutionHashJoinProbeBinding &binding,
	                                                 DataChunk &join_input,
	                                                 SljitNativeConstantOrNull &constant_or_null) const {
		return TryMapHashJoinProbeLHSOutputColumns(binding, join_input, constant_or_null.guard_source_indices);
	}

	bool TryMapHashJoinProbeLHSProjectionPlanSources(const ExecutionHashJoinProbeBinding &binding,
	                                                 DataChunk &join_input,
	                                                 SljitNativeRegionExpressionPlan &plan) const {
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::CONSTANT:
			return true;
		case SljitNativeRegionExpressionKind::REFERENCE:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
			return TryMapHashJoinProbeLHSOutputColumn(binding, join_input, plan.source_index) &&
			       TryMapHashJoinProbeLHSOutputColumns(binding, join_input, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
			return TryMapHashJoinProbeLHSOutputColumn(binding, join_input, plan.source_index) &&
			       TryMapHashJoinProbeLHSOutputColumn(binding, join_input, plan.right_source_index) &&
			       TryMapHashJoinProbeLHSOutputColumns(binding, join_input, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
			if (!TryMapHashJoinProbeLHSOutputColumn(binding, join_input, plan.source_index)) {
				return false;
			}
			if (plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE &&
			    !TryMapHashJoinProbeLHSOutputColumn(binding, join_input, plan.right_source_index)) {
				return false;
			}
			return TryMapHashJoinProbeLHSOutputColumns(binding, join_input, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
			return TryMapHashJoinProbeLHSConstantOrNullSources(binding, join_input, plan.constant_or_null) &&
			       TryMapHashJoinProbeLHSOutputColumns(binding, join_input, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
			return TryMapHashJoinProbeLHSOutputColumn(binding, join_input, plan.source_index) &&
			       TryMapHashJoinProbeLHSOutputColumn(binding, join_input, plan.guard_source_index) &&
			       TryMapHashJoinProbeLHSOutputColumns(binding, join_input, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			return TryMapHashJoinProbeLHSOutputColumns(binding, join_input, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::PREDICATE:
			return false;
		default:
			return false;
		}
	}

	bool TryNormalizeSingleSourceProjectionPlan(SljitNativeRegionExpressionPlan &plan, idx_t source_index) const {
		auto normalize_source = [&](idx_t &candidate) {
			if (candidate != source_index) {
				return false;
			}
			candidate = 0;
			return true;
		};
		auto normalize_sources = [&](vector<idx_t> &candidates) {
			for (auto &candidate : candidates) {
				if (!normalize_source(candidate)) {
					return false;
				}
			}
			return true;
		};

		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::CONSTANT:
			return true;
		case SljitNativeRegionExpressionKind::REFERENCE:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
			return normalize_source(plan.source_index) && normalize_sources(plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
			return normalize_source(plan.source_index) && normalize_source(plan.right_source_index) &&
			       normalize_sources(plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
			if (!normalize_source(plan.source_index)) {
				return false;
			}
			if (plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE &&
			    !normalize_source(plan.right_source_index)) {
				return false;
			}
			return normalize_sources(plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
			return normalize_sources(plan.constant_or_null.guard_source_indices) &&
			       normalize_sources(plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
			return normalize_source(plan.source_index) && normalize_source(plan.guard_source_index) &&
			       normalize_sources(plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			return normalize_sources(plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::PREDICATE:
			return false;
		default:
			return false;
		}
	}

	void BuildBorrowedProjectionExpression(const SljitExecutableRegionExpression &source,
	                                       SljitExecutableRegionExpression &target) const {
		target.plan = source.plan.Copy(true, false);
		target.input_source_indices = source.input_source_indices;
		target.function = source.function;
		target.flat_function = source.flat_function;
		target.select_function = source.select_function;
		target.predicate_function = source.predicate_function;
		target.predicate_select_function = source.predicate_select_function;
		target.overflow_message = source.overflow_message;
	}

	bool TryBuildHashJoinProbeLHSProjectionExpression(const ExecutionHashJoinProbeBinding &binding,
	                                                  DataChunk &join_input,
	                                                  const SljitExecutableRegionExpression &source,
	                                                  SljitExecutableRegionExpression &target) const {
		BuildBorrowedProjectionExpression(source, target);
		if (!TryMapHashJoinProbeLHSProjectionPlanSources(binding, join_input, target.plan)) {
			return false;
		}
		if (!target.input_source_indices.empty()) {
			return TryMapHashJoinProbeLHSOutputColumns(binding, join_input, target.input_source_indices);
		}
		return true;
	}

	bool TryBuildSingleSourceProjectionExpression(const SljitExecutableRegionExpression &source,
	                                              SljitExecutableRegionExpression &target,
	                                              idx_t &join_output_source_index) const {
		BuildBorrowedProjectionExpression(source, target);
		join_output_source_index = DConstants::INVALID_INDEX;
		if (!target.input_source_indices.empty()) {
			if (target.input_source_indices.size() != 1) {
				return false;
			}
			join_output_source_index = target.input_source_indices[0];
			target.input_source_indices[0] = 0;
			return TryNormalizeSingleSourceProjectionPlan(target.plan, join_output_source_index);
		}

		auto &plan = target.plan;
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
			join_output_source_index = plan.source_index;
			plan.source_index = 0;
			if (plan.expression_tree_source_indices.empty()) {
				return true;
			}
			if (plan.expression_tree_source_indices.size() != 1 ||
			    plan.expression_tree_source_indices[0] != join_output_source_index) {
				return false;
			}
			plan.expression_tree_source_indices[0] = 0;
			return true;
		case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
			if (plan.constant_or_null.guard_source_indices.size() != 1) {
				return false;
			}
			join_output_source_index = plan.constant_or_null.guard_source_indices[0];
			plan.constant_or_null.guard_source_indices[0] = 0;
			return true;
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			if (plan.expression_tree_source_indices.size() != 1) {
				return false;
			}
			join_output_source_index = plan.expression_tree_source_indices[0];
			plan.expression_tree_source_indices[0] = 0;
			return true;
		default:
			return false;
		}
	}

	bool SignedIntegerWidthMatchesPhysicalType(SljitNativeSignedIntegerWidth width, PhysicalType physical_type) const {
		switch (width) {
		case SljitNativeSignedIntegerWidth::INT8:
			return physical_type == PhysicalType::INT8;
		case SljitNativeSignedIntegerWidth::INT16:
			return physical_type == PhysicalType::INT16;
		case SljitNativeSignedIntegerWidth::INT32:
			return physical_type == PhysicalType::INT32;
		case SljitNativeSignedIntegerWidth::INT64:
			return physical_type == PhysicalType::INT64;
		default:
			return false;
		}
	}

	bool ProjectionIsSingleSourceReferenceLike(const SljitNativeRegionExpressionPlan &plan) const {
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			return plan.source_index == 0;
		}
		if (plan.kind != SljitNativeRegionExpressionKind::EXPRESSION_TREE &&
		    plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			return false;
		}
		if (!plan.expression_tree || plan.expression_tree->kind != ExecutionExpressionIRKind::REFERENCE ||
		    plan.expression_tree->ref_index != 0) {
			return false;
		}
		return plan.expression_tree_source_indices.size() == 1 && plan.expression_tree_source_indices[0] == 0 &&
		       plan.return_type == plan.expression_tree->return_type;
	}

	bool TryGatherHashJoinRHSReferenceProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
	                                                    const SljitNativeRegionExpressionPlan &plan, idx_t rhs_col_idx,
	                                                    Vector &row_pointers, Vector &target, idx_t current_size,
	                                                    idx_t count) const {
		if (!ProjectionIsSingleSourceReferenceLike(plan) || plan.return_type != target.GetType()) {
			return false;
		}
		data_ptr_t target_data = nullptr;
		unique_ptr<Vector> owned_result;
		if (DirectAppendSupportsFixedSizeType(target.GetType())) {
			target_data =
			    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
			owned_result = make_uniq<Vector>(target.GetType(), target_data, count);
		} else if (target.GetType().id() == LogicalTypeId::VARCHAR) {
			owned_result = make_uniq<Vector>(target.GetType());
		} else {
			return false;
		}
		auto &result = *owned_result;
		GatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, result);
		CopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
		return true;
	}

	static bool HashJoinRHSFixedColumnSourceIsValid(data_ptr_t row_pointer,
	                                                const ExecutionHashJoinRHSFixedColumnSource &source) {
		if (!row_pointer) {
			return false;
		}
		if (source.all_valid) {
			return true;
		}
		if (source.layout_column_idx == DConstants::INVALID_INDEX || source.layout_column_count == 0) {
			return false;
		}
		idx_t entry_idx;
		idx_t idx_in_entry;
		JoinHashTable::ValidityBytes::GetEntryIndex(source.layout_column_idx, entry_idx, idx_in_entry);
		return JoinHashTable::ValidityBytes::RowIsValid(
		    JoinHashTable::ValidityBytes(row_pointer, source.layout_column_count).GetValidityEntryUnsafe(entry_idx),
		    idx_in_entry);
	}

	static bool SljitDateDaysAreFinite(int32_t days) {
		return days != date_t::infinity().days && days != date_t::ninfinity().days;
	}

	static int64_t SljitExtractFiniteDateYear(int32_t days) {
		int32_t year = Date::EPOCH_YEAR;
		while (days < 0) {
			days += Date::DAYS_PER_YEAR_INTERVAL;
			year -= Date::YEAR_INTERVAL;
		}
		while (days >= Date::DAYS_PER_YEAR_INTERVAL) {
			days -= Date::DAYS_PER_YEAR_INTERVAL;
			year += Date::YEAR_INTERVAL;
		}
		auto year_offset = days / 365;
		while (days < Date::CUMULATIVE_YEAR_DAYS[year_offset]) {
			year_offset--;
			D_ASSERT(year_offset >= 0);
		}
		return year + year_offset;
	}

	bool TryMaterializeHashJoinRHSDateYearProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
	                                                        const SljitNativeRegionExpressionPlan &plan,
	                                                        idx_t rhs_col_idx, Vector &row_pointers, Vector &target,
	                                                        data_ptr_t target_data, idx_t current_size,
	                                                        idx_t count) const {
		if (plan.kind != SljitNativeRegionExpressionKind::DATE_YEAR || plan.source_index != 0 ||
		    plan.return_type != target.GetType() || target.GetType().InternalType() != PhysicalType::INT64 ||
		    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		if (!plan.expression_tree_source_indices.empty() &&
		    (plan.expression_tree_source_indices.size() != 1 || plan.expression_tree_source_indices[0] != 0)) {
			return false;
		}

		ExecutionHashJoinRHSFixedColumnSource rhs_source;
		if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source) ||
		    rhs_source.type.id() != LogicalTypeId::DATE || rhs_source.physical_type != PhysicalType::INT32 ||
		    rhs_source.layout_offset == DConstants::INVALID_INDEX) {
			return false;
		}

		Vector result(target.GetType(), target_data, count);
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetDataMutable<int64_t>(result);
		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		result_validity.EnsureWritable();
		result_validity.SetAllValid(count);

		auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
		if (rhs_source.all_valid) {
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				auto row_location = row_pointer_data[row_idx];
				if (!row_location) {
					result_validity.SetInvalid(row_idx);
					continue;
				}
				auto days = Load<int32_t>(row_location + rhs_source.layout_offset);
				if (!SljitDateDaysAreFinite(days)) {
					result_validity.SetInvalid(row_idx);
					continue;
				}
				result_data[row_idx] = SljitExtractFiniteDateYear(days);
			}
		} else {
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				auto row_location = row_pointer_data[row_idx];
				if (!HashJoinRHSFixedColumnSourceIsValid(row_location, rhs_source)) {
					result_validity.SetInvalid(row_idx);
					continue;
				}
				auto days = Load<int32_t>(row_location + rhs_source.layout_offset);
				if (!SljitDateDaysAreFinite(days)) {
					result_validity.SetInvalid(row_idx);
					continue;
				}
				result_data[row_idx] = SljitExtractFiniteDateYear(days);
			}
		}
		FlatVector::SetSize(result, count_t(count));
		CopyDirectProjectionResultToBatch(result, target, target_data, current_size, count);
		return true;
	}

	bool TryMaterializeHashJoinRHSFixedGeneratedProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
	                                                              const SljitNativeRegionExpressionPlan &plan,
	                                                              idx_t rhs_col_idx, Vector &row_pointers,
	                                                              Vector &target, data_ptr_t target_data,
	                                                              idx_t current_size, idx_t count) const {
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::DATE_YEAR:
			return TryMaterializeHashJoinRHSDateYearProjectionToBatch(binding, plan, rhs_col_idx, row_pointers, target,
			                                                          target_data, current_size, count);
		default:
			return false;
		}
	}

	bool TryMaterializeHashJoinComputedRHSProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
	                                                        SljitExecutableRegionExpression &source_expr,
	                                                        Vector &row_pointers, DataChunk &batch, idx_t output_idx,
	                                                        idx_t current_size, idx_t count,
	                                                        SljitExpressionAdapterScratch &adapter_scratch,
	                                                        bool &used_row_pointer_generated_source) {
		used_row_pointer_generated_source = false;
		SljitExecutableRegionExpression remapped_expr;
		idx_t join_output_source_index;
		if (!TryBuildSingleSourceProjectionExpression(source_expr, remapped_expr, join_output_source_index)) {
			return false;
		}
		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		if (join_output_source_index < lhs_column_count ||
		    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
			return false;
		}
		const auto rhs_col_idx = join_output_source_index - lhs_column_count;
		if (rhs_col_idx >= binding.rhs_output_column_count || join_output_source_index >= binding.output_types.size()) {
			return false;
		}

		auto &target = batch.data[output_idx];
		if (TryGatherHashJoinRHSReferenceProjectionToBatch(binding, remapped_expr.plan, rhs_col_idx, row_pointers,
		                                                   target, current_size, count)) {
			return true;
		}
		if (DirectAppendSupportsFixedSizeType(target.GetType())) {
			auto target_data =
			    FlatVector::GetDataMutable(target) + current_size * GetTypeIdSize(target.GetType().InternalType());
			if (TryMaterializeHashJoinRHSFixedGeneratedProjectionToBatch(
			        binding, remapped_expr.plan, rhs_col_idx, row_pointers, target, target_data, current_size, count)) {
				used_row_pointer_generated_source = true;
				return true;
			}
		}
		if (!remapped_expr.function && !remapped_expr.flat_function) {
			return false;
		}

		auto &source_type = binding.output_types[join_output_source_index];
		Vector gathered_source(source_type);
		GatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, gathered_source);

		DataChunk gathered_input;
		gathered_input.InitializeEmpty(vector<LogicalType> {source_type});
		gathered_input.data[0].Reference(gathered_source);
		gathered_input.SetChildCardinality(count);
		return TryExecuteProjectionExpressionToBatch(remapped_expr, gathered_input, target, current_size, count,
		                                             nullptr, adapter_scratch);
	}

	static bool TryRemapHashJoinProjectionSourceIndex(const vector<idx_t> &source_map, idx_t &source_index) {
		if (source_index >= source_map.size() || source_map[source_index] == DConstants::INVALID_INDEX) {
			return false;
		}
		source_index = source_map[source_index];
		return true;
	}

	static bool TryRemapHashJoinProjectionSourceIndices(const vector<idx_t> &source_map,
	                                                    vector<idx_t> &source_indices) {
		for (auto &source_index : source_indices) {
			if (!TryRemapHashJoinProjectionSourceIndex(source_map, source_index)) {
				return false;
			}
		}
		return true;
	}

	static bool TryRemapHashJoinProjectionPredicateSources(const vector<idx_t> &source_map,
	                                                       SljitNativePredicate &predicate) {
		switch (predicate.kind) {
		case SljitNativePredicateKind::REFERENCE:
		case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
		case SljitNativePredicateKind::INT128_COMPARE_CONSTANT:
		case SljitNativePredicateKind::INTEGER_IN_LIST:
		case SljitNativePredicateKind::INTEGER_BETWEEN:
		case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
		case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
		case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
		case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
		case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
		case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
		case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
		case SljitNativePredicateKind::NULL_CHECK:
			if (!TryRemapHashJoinProjectionSourceIndex(source_map, predicate.source_index)) {
				return false;
			}
			break;
		case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
		case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
		case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
			if (!TryRemapHashJoinProjectionSourceIndex(source_map, predicate.source_index) ||
			    !TryRemapHashJoinProjectionSourceIndex(source_map, predicate.right_source_index)) {
				return false;
			}
			break;
		default:
			break;
		}
		if (!TryRemapHashJoinProjectionSourceIndices(source_map, predicate.source_indices) ||
		    !TryRemapHashJoinProjectionSourceIndices(source_map, predicate.guard_source_indices)) {
			return false;
		}
		if (predicate.child && !TryRemapHashJoinProjectionPredicateSources(source_map, *predicate.child)) {
			return false;
		}
		for (auto &child : predicate.children) {
			if (!TryRemapHashJoinProjectionPredicateSources(source_map, *child)) {
				return false;
			}
		}
		return true;
	}

	static bool TryRemapHashJoinProjectionPlanSources(const vector<idx_t> &source_map,
	                                                  SljitNativeRegionExpressionPlan &plan) {
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::CONSTANT:
			return TryRemapHashJoinProjectionSourceIndices(source_map, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::REFERENCE:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
			return TryRemapHashJoinProjectionSourceIndex(source_map, plan.source_index) &&
			       TryRemapHashJoinProjectionSourceIndices(source_map, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
			return TryRemapHashJoinProjectionSourceIndex(source_map, plan.source_index) &&
			       TryRemapHashJoinProjectionSourceIndex(source_map, plan.guard_source_index) &&
			       TryRemapHashJoinProjectionSourceIndices(source_map, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
			return TryRemapHashJoinProjectionSourceIndices(source_map, plan.constant_or_null.guard_source_indices) &&
			       TryRemapHashJoinProjectionSourceIndices(source_map, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
			return TryRemapHashJoinProjectionSourceIndex(source_map, plan.source_index) &&
			       TryRemapHashJoinProjectionSourceIndex(source_map, plan.right_source_index) &&
			       TryRemapHashJoinProjectionSourceIndices(source_map, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
			if (!TryRemapHashJoinProjectionSourceIndex(source_map, plan.source_index)) {
				return false;
			}
			if (plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE &&
			    !TryRemapHashJoinProjectionSourceIndex(source_map, plan.right_source_index)) {
				return false;
			}
			return TryRemapHashJoinProjectionSourceIndices(source_map, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::PREDICATE:
			return plan.predicate && TryRemapHashJoinProjectionPredicateSources(source_map, *plan.predicate) &&
			       TryRemapHashJoinProjectionSourceIndices(source_map, plan.expression_tree_source_indices);
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			return TryRemapHashJoinProjectionSourceIndices(source_map, plan.expression_tree_source_indices);
		default:
			return false;
		}
	}

	bool TryCollectHashJoinProjectionExpressionSources(const SljitExecutableRegionExpression &expr,
	                                                   idx_t input_column_count, vector<uint8_t> &referenced) const {
		referenced.assign(input_column_count, 0);
		if (!expr.input_source_indices.empty()) {
			return AddProjectionSourceColumns(expr.input_source_indices, input_column_count, referenced);
		}
		auto &plan = expr.plan;
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::CONSTANT:
			return AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::REFERENCE:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE:
		case SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP:
		case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::STRING_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
		case SljitNativeRegionExpressionKind::NULL_CHECK:
			return AddProjectionSourceColumn(plan.source_index, input_column_count, referenced) &&
			       AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE:
			return AddProjectionSourceColumn(plan.source_index, input_column_count, referenced) &&
			       AddProjectionSourceColumn(plan.guard_source_index, input_column_count, referenced) &&
			       AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
			return AddProjectionSourceColumns(plan.constant_or_null.guard_source_indices, input_column_count,
			                                  referenced) &&
			       AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
			return AddProjectionSourceColumn(plan.source_index, input_column_count, referenced) &&
			       AddProjectionSourceColumn(plan.right_source_index, input_column_count, referenced) &&
			       AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
			if (!AddProjectionSourceColumn(plan.source_index, input_column_count, referenced)) {
				return false;
			}
			if (plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE &&
			    !AddProjectionSourceColumn(plan.right_source_index, input_column_count, referenced)) {
				return false;
			}
			return AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::PREDICATE:
			if (!plan.expression_tree_source_indices.empty()) {
				return AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
			}
			if (!plan.predicate) {
				return false;
			}
			return AddProjectionSourceColumns(plan.predicate->source_indices, input_column_count, referenced);
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
			return AddProjectionSourceColumns(plan.expression_tree_source_indices, input_column_count, referenced);
		default:
			return false;
		}
	}

	bool TryBuildHashJoinProjectionExpressionInput(const ExecutionHashJoinProbeBinding &binding,
	                                               SljitExecutableRegionExpression &source_expr, DataChunk &join_input,
	                                               const SelectionVector &match_selection, Vector &row_pointers,
	                                               idx_t count, SljitExecutableRegionExpression &remapped_expr,
	                                               DataChunk &expression_input,
	                                               vector<Vector> &expression_sources) const {
		if (!binding.ready || !binding.hash_table ||
		    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
			return false;
		}
		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		const auto join_output_column_count = lhs_column_count + binding.rhs_output_column_count;
		if (join_output_column_count == 0) {
			return false;
		}

		vector<uint8_t> referenced_columns;
		if (!TryCollectHashJoinProjectionExpressionSources(source_expr, join_output_column_count, referenced_columns)) {
			return false;
		}
		vector<idx_t> source_map(join_output_column_count, DConstants::INVALID_INDEX);
		vector<LogicalType> expression_types;
		expression_sources.clear();
		expression_sources.reserve(join_output_column_count);
		expression_types.reserve(join_output_column_count);

		for (idx_t source_idx = 0; source_idx < join_output_column_count; source_idx++) {
			if (!referenced_columns[source_idx]) {
				continue;
			}
			auto &source_type = binding.output_types[source_idx];
			expression_types.push_back(source_type);
			source_map[source_idx] = expression_sources.size();
			expression_sources.emplace_back(source_type);
			auto &source = expression_sources.back();
			if (source_idx < lhs_column_count) {
				const auto input_col = binding.lhs_output_column_indices[source_idx];
				if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != source_type) {
					return false;
				}
				source.Slice(join_input.data[input_col], match_selection, count);
				continue;
			}
			const auto rhs_col_idx = source_idx - lhs_column_count;
			if (rhs_col_idx >= binding.rhs_output_column_count) {
				return false;
			}
			GatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, source);
		}
		if (expression_sources.empty()) {
			return false;
		}

		BuildBorrowedProjectionExpression(source_expr, remapped_expr);
		if (!TryRemapHashJoinProjectionPlanSources(source_map, remapped_expr.plan)) {
			return false;
		}
		for (auto &input_source_idx : remapped_expr.input_source_indices) {
			if (input_source_idx >= source_map.size() || source_map[input_source_idx] == DConstants::INVALID_INDEX) {
				return false;
			}
			input_source_idx = source_map[input_source_idx];
		}

		expression_input.InitializeEmpty(expression_types);
		for (idx_t source_idx = 0; source_idx < expression_sources.size(); source_idx++) {
			expression_input.data[source_idx].Reference(expression_sources[source_idx]);
		}
		expression_input.SetChildCardinality(count);
		return true;
	}

	bool TryResolveReferenceThroughProjectionChain(idx_t first_projection_idx, idx_t aggregate_idx,
	                                               const SljitExecutableRegionExpression &source_expr,
	                                               idx_t &join_output_source_idx, LogicalType &source_type) const {
		SljitExecutableRegionExpression remapped_source;
		idx_t source_idx;
		if (!TryBuildSingleSourceProjectionExpression(source_expr, remapped_source, source_idx) ||
		    !ProjectionIsSingleSourceReferenceLike(remapped_source.plan)) {
			return false;
		}
		source_type = remapped_source.plan.return_type;
		for (idx_t op_idx = aggregate_idx; op_idx > first_projection_idx; op_idx--) {
			auto &projection_op = ops[op_idx - 1];
			if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
			    source_idx >= projection_op.projections.size() || source_idx >= projection_op.output_types.size() ||
			    projection_op.output_types[source_idx] != source_type) {
				return false;
			}
			SljitExecutableRegionExpression remapped_projection;
			idx_t next_source_idx;
			if (!TryBuildSingleSourceProjectionExpression(projection_op.projections[source_idx], remapped_projection,
			                                              next_source_idx) ||
			    !ProjectionIsSingleSourceReferenceLike(remapped_projection.plan) ||
			    remapped_projection.plan.return_type != source_type) {
				return false;
			}
			source_idx = next_source_idx;
		}
		join_output_source_idx = source_idx;
		return true;
	}

	bool TryBuildRemappedPayloadReference(const SljitExecutableRegionExpression &payload, idx_t payload_input_idx,
	                                      SljitExecutableRegionExpression &remapped_payload,
	                                      idx_t &join_output_source_idx) const {
		if (!TryBuildSingleSourceProjectionExpression(payload, remapped_payload, join_output_source_idx) ||
		    !ProjectionIsSingleSourceReferenceLike(remapped_payload.plan)) {
			return false;
		}
		remapped_payload.input_source_indices.clear();
		if (remapped_payload.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			remapped_payload.plan.source_index = payload_input_idx;
			remapped_payload.plan.expression_tree_source_indices.clear();
			return true;
		}
		remapped_payload.plan.expression_tree_source_indices.clear();
		remapped_payload.plan.expression_tree_source_indices.push_back(payload_input_idx);
		return true;
	}

	bool CanExecuteHashJoinUngroupedAggregateUpdate(idx_t hash_join_idx, idx_t first_projection_idx) const {
		if (hash_join_idx + 1 >= ops.size() || first_projection_idx > ops.size() ||
		    ops[hash_join_idx].kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE ||
		    ops.back().kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
			return false;
		}
		auto &hash_join_op = ops[hash_join_idx];
		if (hash_join_op.hash_join_probe.plan.output_mode !=
		    ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
			return false;
		}
		const auto aggregate_idx = ops.size() - 1;
		auto &aggregate_op = ops[aggregate_idx];
		if (aggregate_op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		    !aggregate_op.aggregate_update.plan.use_primitive_payloads ||
		    aggregate_op.aggregate_update.fused_payload_update_function ||
		    aggregate_op.aggregate_update.plan.use_grouped_state_addresses) {
			return false;
		}
		if (aggregate_op.aggregate_update.payloads.size() !=
		    aggregate_op.aggregate_update.payload_update_functions.size()) {
			return false;
		}
		for (idx_t op_idx = first_projection_idx; op_idx < aggregate_idx; op_idx++) {
			if (ops[op_idx].kind != SljitNativeRegionOpKind::PROJECTION) {
				return false;
			}
		}
		for (auto &payload : aggregate_op.aggregate_update.payloads) {
			idx_t join_output_source_idx;
			LogicalType source_type;
			if (!TryResolveReferenceThroughProjectionChain(first_projection_idx, aggregate_idx, payload,
			                                               join_output_source_idx, source_type)) {
				return false;
			}
		}
		return true;
	}

	bool CanExecuteHashJoinFilteredUngroupedAggregateUpdate(idx_t hash_join_idx) const {
		return hash_join_idx + 2 < ops.size() && ops[hash_join_idx + 1].kind == SljitNativeRegionOpKind::FILTER &&
		       CanExecuteHashJoinUngroupedAggregateUpdate(hash_join_idx, hash_join_idx + 2);
	}

	bool TryPrepareHashJoinFilteredUngroupedPayloadInput(
	    const ExecutionHashJoinProbeBinding &binding, DataChunk &join_input, const SelectionVector &match_selection,
	    Vector &row_pointers, const SelectionVector &filter_selection, idx_t selected_count, idx_t hash_join_idx,
	    idx_t first_projection_idx, idx_t aggregate_idx, SljitExecutableRegionOp &aggregate_op,
	    DataChunk &payload_input, vector<Vector> &payload_sources,
	    vector<SljitExecutableRegionExpression> &remapped_payloads, SelectionVector &compact_match_selection) {
		auto &payloads = aggregate_op.aggregate_update.payloads;
		if (!binding.ready || !binding.hash_table ||
		    binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
		    binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
		    row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		for (idx_t selected_idx = 0; selected_idx < selected_count; selected_idx++) {
			const auto match_idx = filter_selection.get_index(selected_idx);
			compact_match_selection.set_index(selected_idx, match_selection.get_index(match_idx));
		}

		vector<LogicalType> payload_types;
		payload_types.reserve(payloads.size());
		payload_sources.clear();
		payload_sources.reserve(payloads.size());
		remapped_payloads.clear();
		remapped_payloads.reserve(payloads.size());
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &payload = payloads[payload_idx];
			idx_t join_output_source_idx;
			LogicalType source_type;
			if (!TryResolveReferenceThroughProjectionChain(first_projection_idx, aggregate_idx, payload,
			                                               join_output_source_idx, source_type) ||
			    join_output_source_idx >= binding.output_types.size() ||
			    binding.output_types[join_output_source_idx] != source_type) {
				return false;
			}

			payload_types.push_back(source_type);
			payload_sources.emplace_back(source_type);
			auto &source = payload_sources.back();
			const auto lhs_column_count = binding.lhs_output_column_indices.size();
			if (join_output_source_idx < lhs_column_count) {
				const auto input_col = binding.lhs_output_column_indices[join_output_source_idx];
				if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != source_type) {
					return false;
				}
				source.Slice(join_input.data[input_col], compact_match_selection, selected_count);
			} else {
				const auto rhs_col_idx = join_output_source_idx - lhs_column_count;
				if (rhs_col_idx >= binding.rhs_output_column_count) {
					return false;
				}
				binding.hash_table->GatherRHSColumn(row_pointers, filter_selection, selected_count, rhs_col_idx,
				                                    source);
			}

			remapped_payloads.emplace_back();
			auto &remapped_payload = remapped_payloads.back();
			idx_t remapped_join_output_source_idx;
			if (!TryBuildRemappedPayloadReference(payload, payload_idx, remapped_payload,
			                                      remapped_join_output_source_idx) ||
			    remapped_join_output_source_idx != join_output_source_idx) {
				return false;
			}
		}

		payload_input.InitializeEmpty(payload_types);
		for (idx_t payload_idx = 0; payload_idx < payload_sources.size(); payload_idx++) {
			payload_input.data[payload_idx].Reference(payload_sources[payload_idx]);
		}
		payload_input.SetChildCardinality(selected_count);
		return true;
	}

	SinkResultType ExecuteNativeUngroupedAggregateUpdateWithRemappedPayloads(
	    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
	    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input,
	    vector<SljitExecutableRegionExpression> &remapped_payloads) {
		auto bind_stage_start = SljitRegionStageStart(runtime);
		bool bound = false;
		auto &binding =
		    BindNativeSink(native_runtime, scratch, op_idx, payload_input, op.aggregate_update.plan.sink_info,
		                   "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink", bound);
		if (bound) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_sink_contract", bind_stage_start);
		}
		if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready) {
			throw InternalException("SLJIT direct filtered aggregate update sink binding is incomplete");
		}
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		if (aggregates.size() != remapped_payloads.size()) {
			throw InternalException("SLJIT direct filtered aggregate payload count mismatch");
		}
		auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, binding.aggregate_update.primitive);
		for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			auto lane = payload_lanes[payload_idx];
			if (!lane) {
				throw InternalException("SLJIT direct filtered aggregate primitive lane is missing");
			}
			auto payload_stage_start = SljitRegionStageStart(runtime);
			ExecutePrimitiveAggregatePayloadUpdate(
			    remapped_payloads[payload_idx], op.aggregate_update.payload_update_functions[payload_idx], *lane,
			    payload_input, nullptr, payload_input.size(), scratch.ExpressionAdapterScratch(op_idx, payload_idx));
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_hash_join_filtered_payload_update",
			                              payload_stage_start);
		}
		RecordSljitRegionRuntimePath(runtime, op.kind, "direct_hash_join_filtered_payload_update", aggregates.size());
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_hash_join_filtered_state_update",
		                                         payload_input.size());
		return native_runtime.RecordSinkResult(payload_input, SinkResultType::NEED_MORE_INPUT);
	}

	bool TryMaterializeHashJoinMixedProjectionToBatch(const ExecutionHashJoinProbeBinding &binding,
	                                                  SljitExecutableRegionExpression &source_expr,
	                                                  DataChunk &join_input, const SelectionVector &match_selection,
	                                                  Vector &row_pointers, DataChunk &batch, idx_t output_idx,
	                                                  idx_t current_size, idx_t count,
	                                                  SljitExpressionAdapterScratch &adapter_scratch) {
		auto &target = batch.data[output_idx];
		if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != source_expr.plan.return_type ||
		    !DirectProjectionBatchSupportsType(target.GetType()) ||
		    FlatVector::GetCapacity(target) < current_size + count) {
			return false;
		}

		SljitExecutableRegionExpression remapped_expr;
		DataChunk expression_input;
		vector<Vector> expression_sources;
		if (!TryBuildHashJoinProjectionExpressionInput(binding, source_expr, join_input, match_selection, row_pointers,
		                                               count, remapped_expr, expression_input, expression_sources)) {
			return false;
		}

		return TryExecuteProjectionExpressionToBatch(remapped_expr, expression_input, target, current_size, count,
		                                             nullptr, adapter_scratch);
	}

	static bool SljitRuntimeExpressionIsDecimal64(const ExecutionExpressionIR &node) {
		return node.return_type.id() == LogicalTypeId::DECIMAL && node.physical_type == PhysicalType::INT64;
	}

	static bool SljitRuntimeDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
		if (!node.left || !node.right || !SljitRuntimeExpressionIsDecimal64(node) ||
		    !SljitRuntimeExpressionIsDecimal64(*node.left) || !SljitRuntimeExpressionIsDecimal64(*node.right)) {
			return false;
		}
		switch (node.binary_op) {
		case ExecutionExpressionBinaryOp::ADD:
		case ExecutionExpressionBinaryOp::SUBTRACT:
			return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
			       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type);
		case ExecutionExpressionBinaryOp::MULTIPLY:
			return DecimalType::GetScale(node.return_type) ==
			       DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type);
		default:
			return false;
		}
	}

	struct SljitRuntimeDecimal64Source {
		const int64_t *data = nullptr;
		const sel_t *source_sel = nullptr;
		const SelectionVector *match_selection = nullptr;
		const data_ptr_t *row_pointers = nullptr;
		idx_t layout_offset = DConstants::INVALID_INDEX;
		bool row_pointer_source = false;
	};

	struct SljitRuntimeDecimal64DiscountedAmountProgram {
		bool ready = false;
		idx_t gross_source_idx = DConstants::INVALID_INDEX;
		idx_t discount_source_idx = DConstants::INVALID_INDEX;
		idx_t cost_source_idx = DConstants::INVALID_INDEX;
		idx_t quantity_source_idx = DConstants::INVALID_INDEX;
		int64_t discount_base = 0;
	};

	static bool TryReadSljitRuntimeDecimal64Reference(const ExecutionExpressionIR &node, idx_t source_count,
	                                                  idx_t &source_idx) {
		if (node.kind != ExecutionExpressionIRKind::REFERENCE || !SljitRuntimeExpressionIsDecimal64(node) ||
		    node.ref_index >= source_count) {
			return false;
		}
		source_idx = node.ref_index;
		return true;
	}

	static bool TryReadSljitRuntimeDecimal64Constant(const ExecutionExpressionIR &node, int64_t &constant) {
		if (node.kind != ExecutionExpressionIRKind::CONSTANT || node.constant.IsNull() ||
		    node.constant.type().id() != LogicalTypeId::DECIMAL ||
		    node.constant.type().InternalType() != PhysicalType::INT64 || !SljitRuntimeExpressionIsDecimal64(node)) {
			return false;
		}
		constant = node.constant.GetValueUnsafe<int64_t>();
		return true;
	}

	static bool SljitRuntimeDecimal64BinaryCanRunUnchecked(const ExecutionExpressionIR &node,
	                                                       ExecutionExpressionBinaryOp op) {
		return node.kind == ExecutionExpressionIRKind::BINARY && node.binary_op == op &&
		       !node.arithmetic_overflow_check && SljitRuntimeDecimal64BinaryHasRawSemantics(node);
	}

	static bool
	TryBuildSljitRuntimeDecimal64DiscountedAmountProgram(const ExecutionExpressionIR &node, idx_t source_count,
	                                                     SljitRuntimeDecimal64DiscountedAmountProgram &program) {
		program = SljitRuntimeDecimal64DiscountedAmountProgram();
		if (!SljitRuntimeDecimal64BinaryCanRunUnchecked(node, ExecutionExpressionBinaryOp::SUBTRACT) || !node.left ||
		    !node.right ||
		    !SljitRuntimeDecimal64BinaryCanRunUnchecked(*node.left, ExecutionExpressionBinaryOp::MULTIPLY) ||
		    !SljitRuntimeDecimal64BinaryCanRunUnchecked(*node.right, ExecutionExpressionBinaryOp::MULTIPLY) ||
		    !node.left->left || !node.left->right || !node.right->left || !node.right->right) {
			return false;
		}
		if (!TryReadSljitRuntimeDecimal64Reference(*node.left->left, source_count, program.gross_source_idx) ||
		    !TryReadSljitRuntimeDecimal64Reference(*node.right->left, source_count, program.cost_source_idx) ||
		    !TryReadSljitRuntimeDecimal64Reference(*node.right->right, source_count, program.quantity_source_idx)) {
			return false;
		}
		auto &discount_expr = *node.left->right;
		if (!SljitRuntimeDecimal64BinaryCanRunUnchecked(discount_expr, ExecutionExpressionBinaryOp::SUBTRACT) ||
		    !discount_expr.left || !discount_expr.right ||
		    !TryReadSljitRuntimeDecimal64Constant(*discount_expr.left, program.discount_base) ||
		    !TryReadSljitRuntimeDecimal64Reference(*discount_expr.right, source_count, program.discount_source_idx)) {
			return false;
		}
		program.ready = true;
		return true;
	}

	static int64_t SljitRuntimeLoadDecimal64Source(const SljitRuntimeDecimal64Source &source, idx_t row_idx) {
		if (source.row_pointer_source) {
			auto row_location = source.row_pointers[row_idx];
			return Load<int64_t>(row_location + source.layout_offset);
		}
		const auto match_idx = source.match_selection->get_index(row_idx);
		const auto source_idx = source.source_sel ? source.source_sel[match_idx] : match_idx;
		return source.data[source_idx];
	}

	bool TryPrepareAllValidDecimal64HashJoinPayloadSource(const ExecutionHashJoinProbeBinding &binding,
	                                                      DataChunk &join_input, const SelectionVector &match_selection,
	                                                      Vector &row_pointers, idx_t count, idx_t source_idx,
	                                                      vector<UnifiedVectorFormat> &formats,
	                                                      SljitRuntimeDecimal64Source &source) const {
		if (source_idx >= binding.output_types.size() ||
		    binding.output_types[source_idx].InternalType() != PhysicalType::INT64 ||
		    binding.output_types[source_idx].id() != LogicalTypeId::DECIMAL) {
			return false;
		}
		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		if (source_idx < lhs_column_count) {
			const auto input_col = binding.lhs_output_column_indices[source_idx];
			if (input_col >= join_input.ColumnCount() ||
			    join_input.data[input_col].GetType() != binding.output_types[source_idx]) {
				return false;
			}
			formats.emplace_back();
			auto &format = formats.back();
			join_input.data[input_col].ToUnifiedFormat(format);
			auto source_sel = SljitNormalizedSourceSelectionData(format);
			if (!format.validity.CannotHaveNull()) {
				for (idx_t row_idx = 0; row_idx < count; row_idx++) {
					const auto match_idx = match_selection.get_index(row_idx);
					const auto selected_idx = source_sel ? source_sel[match_idx] : match_idx;
					if (!format.validity.RowIsValid(selected_idx)) {
						return false;
					}
				}
			}
			source.data = UnifiedVectorFormat::GetData<int64_t>(format);
			source.source_sel = source_sel;
			source.match_selection = &match_selection;
			source.row_pointer_source = false;
			return true;
		}

		if (row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		const auto rhs_col_idx = source_idx - lhs_column_count;
		ExecutionHashJoinRHSFixedColumnSource rhs_source;
		if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source) ||
		    rhs_source.type != binding.output_types[source_idx] || rhs_source.physical_type != PhysicalType::INT64 ||
		    rhs_source.type.id() != LogicalTypeId::DECIMAL || rhs_source.layout_offset == DConstants::INVALID_INDEX) {
			return false;
		}
		source.row_pointers = FlatVector::GetData<data_ptr_t>(row_pointers);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto row_location = source.row_pointers[row_idx];
			if (!row_location ||
			    (!rhs_source.all_valid && !HashJoinRHSFixedColumnSourceIsValid(row_location, rhs_source))) {
				return false;
			}
		}
		source.layout_offset = rhs_source.layout_offset;
		source.row_pointer_source = true;
		return true;
	}

	bool TryMaterializeHashJoinAllValidDecimal64ExpressionToBatch(
	    const ExecutionHashJoinProbeBinding &binding, SljitExecutableRegionExpression &source_expr,
	    DataChunk &join_input, const SelectionVector &match_selection, Vector &row_pointers, DataChunk &batch,
	    idx_t output_idx, idx_t current_size, idx_t count, const SljitRuntimeDecimal64DiscountedAmountProgram &program,
	    string *reason = nullptr) const {
		auto set_reason = [&](const char *value) {
			if (reason && reason->empty()) {
				*reason = value;
			}
			return false;
		};
		auto &target = batch.data[output_idx];
		if (source_expr.input_source_indices.empty() || !program.ready) {
			return set_reason("program");
		}
		if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != source_expr.plan.return_type ||
		    target.GetType().id() != LogicalTypeId::DECIMAL || target.GetType().InternalType() != PhysicalType::INT64 ||
		    FlatVector::GetCapacity(target) < current_size + count) {
			return set_reason("target");
		}

		vector<UnifiedVectorFormat> formats;
		formats.reserve(source_expr.input_source_indices.size());
		vector<SljitRuntimeDecimal64Source> sources(source_expr.input_source_indices.size());
		for (idx_t source_idx = 0; source_idx < source_expr.input_source_indices.size(); source_idx++) {
			if (!TryPrepareAllValidDecimal64HashJoinPayloadSource(binding, join_input, match_selection, row_pointers,
			                                                      count, source_expr.input_source_indices[source_idx],
			                                                      formats, sources[source_idx])) {
				return set_reason("source");
			}
		}
		if (program.gross_source_idx >= sources.size() || program.discount_source_idx >= sources.size() ||
		    program.cost_source_idx >= sources.size() || program.quantity_source_idx >= sources.size()) {
			return set_reason("program_source");
		}

		auto result_data = FlatVector::GetDataMutable<int64_t>(target);
		auto &result_validity = FlatVector::ValidityMutable(target);
		result_validity.Reset(current_size + count);
		result_validity.SetAllValid(current_size + count);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			const auto gross = SljitRuntimeLoadDecimal64Source(sources[program.gross_source_idx], row_idx);
			const auto discount = SljitRuntimeLoadDecimal64Source(sources[program.discount_source_idx], row_idx);
			const auto cost = SljitRuntimeLoadDecimal64Source(sources[program.cost_source_idx], row_idx);
			const auto quantity = SljitRuntimeLoadDecimal64Source(sources[program.quantity_source_idx], row_idx);
			result_data[current_size + row_idx] = gross * (program.discount_base - discount) - cost * quantity;
		}
		return true;
	}

	static bool TryMapGroupKeyCast(const SljitNativeRegionExpressionPlan &plan, const LogicalType &target_type,
	                               ExecutionRowPointerGroupKeySource &group_source) {
		auto signed_width_matches_physical_type = [](SljitNativeSignedIntegerWidth width, PhysicalType physical_type) {
			switch (width) {
			case SljitNativeSignedIntegerWidth::INT8:
				return physical_type == PhysicalType::INT8;
			case SljitNativeSignedIntegerWidth::INT16:
				return physical_type == PhysicalType::INT16;
			case SljitNativeSignedIntegerWidth::INT32:
				return physical_type == PhysicalType::INT32;
			case SljitNativeSignedIntegerWidth::INT64:
				return physical_type == PhysicalType::INT64;
			default:
				return false;
			}
		};
		auto unsigned_width_matches_physical_type = [](SljitNativeUnsignedIntegerWidth width,
		                                               PhysicalType physical_type) {
			switch (width) {
			case SljitNativeUnsignedIntegerWidth::UINT8:
				return physical_type == PhysicalType::UINT8;
			case SljitNativeUnsignedIntegerWidth::UINT16:
				return physical_type == PhysicalType::UINT16;
			case SljitNativeUnsignedIntegerWidth::UINT32:
				return physical_type == PhysicalType::UINT32;
			default:
				return false;
			}
		};

		group_source.ready = false;
		group_source.target_type = target_type;
		group_source.target_physical_type = target_type.InternalType();
		if (plan.return_type.InternalType() != target_type.InternalType()) {
			return false;
		}

		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST && !plan.try_cast && plan.source_index == 0) {
			if (group_source.source_physical_type == PhysicalType::INT64 &&
			    target_type.InternalType() == PhysicalType::INT32 &&
			    plan.cast_source_width == SljitNativeSignedIntegerWidth::INT64 &&
			    plan.cast_target_width == SljitNativeSignedIntegerWidth::INT32) {
				group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT32;
				group_source.ready = true;
				return true;
			}
			if (group_source.source_physical_type == PhysicalType::INT64 &&
			    target_type.InternalType() == PhysicalType::INT16 &&
			    plan.cast_source_width == SljitNativeSignedIntegerWidth::INT64 &&
			    plan.cast_target_width == SljitNativeSignedIntegerWidth::INT16) {
				group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT64_TO_INT16;
				group_source.ready = true;
				return true;
			}
			if (group_source.source_physical_type == PhysicalType::INT32 &&
			    target_type.InternalType() == PhysicalType::INT8 &&
			    plan.cast_source_width == SljitNativeSignedIntegerWidth::INT32 &&
			    plan.cast_target_width == SljitNativeSignedIntegerWidth::INT8) {
				group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INT32_TO_INT8;
				group_source.ready = true;
				return true;
			}
			return false;
		}

		if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS && plan.source_index == 0 &&
		    signed_width_matches_physical_type(plan.cast_source_width, group_source.source_physical_type) &&
		    unsigned_width_matches_physical_type(plan.unsigned_cast_target_width, target_type.InternalType())) {
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::INTEGRAL_COMPRESS;
			group_source.cast_constant = plan.constant;
			group_source.ready = true;
			return true;
		}

		if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS && plan.source_index == 0 &&
		    group_source.source_physical_type == PhysicalType::VARCHAR) {
			switch (target_type.InternalType()) {
			case PhysicalType::UINT8:
			case PhysicalType::UINT16:
			case PhysicalType::UINT32:
			case PhysicalType::UINT64:
			case PhysicalType::UINT128:
				group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::STRING_COMPRESS;
				group_source.ready = true;
				return true;
			default:
				break;
			}
		}
		return false;
	}

	static void InitializeRowPointerGroupKeySource(const ExecutionHashJoinRHSFixedColumnSource &rhs_source,
	                                               const LogicalType &target_type,
	                                               ExecutionRowPointerGroupKeySource &group_source) {
		group_source.source_kind = ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD;
		group_source.target_type = target_type;
		group_source.source_physical_type = rhs_source.physical_type;
		group_source.target_physical_type = target_type.InternalType();
		group_source.input_vector_index = DConstants::INVALID_INDEX;
		group_source.row_layout_offset = rhs_source.layout_offset;
		group_source.row_layout_column_idx = rhs_source.layout_column_idx;
		group_source.row_layout_column_count = rhs_source.layout_column_count;
		group_source.all_valid = rhs_source.all_valid;
	}

	static void InitializeInputVectorGroupKeySource(idx_t input_vector_index, PhysicalType source_physical_type,
	                                                const LogicalType &target_type,
	                                                ExecutionRowPointerGroupKeySource &group_source) {
		group_source.source_kind = ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR;
		group_source.target_type = target_type;
		group_source.source_physical_type = source_physical_type;
		group_source.target_physical_type = target_type.InternalType();
		group_source.input_vector_index = input_vector_index;
		group_source.row_layout_offset = DConstants::INVALID_INDEX;
		group_source.row_layout_column_idx = DConstants::INVALID_INDEX;
		group_source.row_layout_column_count = 0;
		group_source.all_valid = false;
	}

	bool TryBuildRowPointerGroupKeySource(const ExecutionHashJoinProbeBinding &binding,
	                                      SljitExecutableRegionExpression &projection,
	                                      const ExecutionRegionGroupInput &group,
	                                      ExecutionRowPointerGroupKeySource &group_source) const {
		SljitExecutableRegionExpression remapped_expr;
		idx_t join_output_source_index;
		if (!TryBuildSingleSourceProjectionExpression(projection, remapped_expr, join_output_source_index)) {
			return false;
		}
		if (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
		    join_output_source_index >= binding.output_types.size()) {
			return false;
		}
		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		if (join_output_source_index < lhs_column_count) {
			const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
			InitializeInputVectorGroupKeySource(
			    input_col, binding.output_types[join_output_source_index].InternalType(), group.type, group_source);
			if (ProjectionIsSingleSourceReferenceLike(remapped_expr.plan) &&
			    group_source.source_physical_type == group.type.InternalType() &&
			    remapped_expr.plan.return_type.InternalType() == group.type.InternalType()) {
				group_source.ready = true;
				group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
				return true;
			}
			return TryMapGroupKeyCast(remapped_expr.plan, group.type, group_source);
		}
		const auto rhs_col_idx = join_output_source_index - lhs_column_count;
		ExecutionHashJoinRHSFixedColumnSource rhs_source;
		if (!ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source)) {
			return false;
		}
		InitializeRowPointerGroupKeySource(rhs_source, group.type, group_source);
		if (ProjectionIsSingleSourceReferenceLike(remapped_expr.plan) &&
		    rhs_source.physical_type == group.type.InternalType() &&
		    remapped_expr.plan.return_type.InternalType() == group.type.InternalType()) {
			group_source.ready = true;
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
			return true;
		}
		return TryMapGroupKeyCast(remapped_expr.plan, group.type, group_source);
	}

	bool TryBuildRowPointerGroupKeySources(const ExecutionHashJoinProbeBinding &binding,
	                                       SljitExecutableRegionOp &projection_op,
	                                       SljitExecutableRegionOp &aggregate_op,
	                                       vector<ExecutionRowPointerGroupKeySource> &group_sources) const {
		auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
		if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty()) {
			return false;
		}
		group_sources.clear();
		group_sources.reserve(sink_info.groups.size());
		for (auto &group : sink_info.groups) {
			if (group.input_index >= projection_op.projections.size() ||
			    group.input_index >= projection_op.output_types.size() ||
			    projection_op.output_types[group.input_index].InternalType() != group.type.InternalType()) {
				group_sources.clear();
				return false;
			}
			ExecutionRowPointerGroupKeySource group_source;
			if (!TryBuildRowPointerGroupKeySource(binding, projection_op.projections[group.input_index], group,
			                                      group_source)) {
				group_sources.clear();
				return false;
			}
			group_sources.push_back(std::move(group_source));
		}
		return true;
	}

	static bool TryGetFusedTypedPayloadCombinedSources(vector<SljitExecutableRegionExpression> &payloads,
	                                                   const vector<ExecutionRegionAggregateInput> &aggregates,
	                                                   vector<idx_t> &combined_sources) {
		if (!FusedAggregatePayloadsUseTypedExpressionTrees(payloads, aggregates)) {
			return false;
		}
		combined_sources.clear();
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				continue;
			}
			if (payloads[payload_idx].input_source_indices.empty()) {
				combined_sources.clear();
				return false;
			}
			if (combined_sources.empty()) {
				combined_sources = payloads[payload_idx].input_source_indices;
			} else if (combined_sources != payloads[payload_idx].input_source_indices) {
				combined_sources.clear();
				return false;
			}
		}
		return !combined_sources.empty();
	}

	bool TryBuildRowPointerGroupedPayloadSourceOverride(const ExecutionHashJoinProbeBinding &binding,
	                                                    SljitExecutableRegionOp &projection_op,
	                                                    SljitExecutableRegionOp &aggregate_op, DataChunk &payload_input,
	                                                    vector<idx_t> &source_override) const {
		auto &aggregates = aggregate_op.aggregate_update.plan.sink_info.aggregates;
		vector<idx_t> combined_sources;
		if (!TryGetFusedTypedPayloadCombinedSources(aggregate_op.aggregate_update.payloads, aggregates,
		                                            combined_sources)) {
			return false;
		}
		if (!binding.ready || binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
			return false;
		}
		source_override.clear();
		source_override.reserve(combined_sources.size());
		for (auto projection_source_idx : combined_sources) {
			if (projection_source_idx >= projection_op.projections.size() ||
			    projection_source_idx >= projection_op.output_types.size()) {
				source_override.clear();
				return false;
			}
			SljitExecutableRegionExpression remapped_expr;
			idx_t join_output_source_idx;
			if (!TryBuildSingleSourceProjectionExpression(projection_op.projections[projection_source_idx],
			                                              remapped_expr, join_output_source_idx) ||
			    !ProjectionIsSingleSourceReferenceLike(remapped_expr.plan)) {
				source_override.clear();
				return false;
			}
			if (join_output_source_idx >= binding.lhs_output_column_indices.size()) {
				source_override.clear();
				return false;
			}
			const auto input_source_idx = binding.lhs_output_column_indices[join_output_source_idx];
			if (input_source_idx >= payload_input.ColumnCount() ||
			    projection_op.output_types[projection_source_idx] != payload_input.data[input_source_idx].GetType()) {
				source_override.clear();
				return false;
			}
			source_override.push_back(input_source_idx);
		}
		return true;
	}

	bool TryMaterializeHashJoinComputedProjectionsToBatch(ExecutionRegionRuntime &runtime,
	                                                      SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                                      idx_t projection_idx, SljitExecutableRegionOp &projection_op,
	                                                      DataChunk &join_input, const SelectionVector &match_selection,
	                                                      Vector &row_pointers, DataChunk &batch,
	                                                      optional_ptr<const vector<idx_t>> output_to_projection,
	                                                      idx_t count, vector<uint8_t> &skip_projection) {
		if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx)) {
			return false;
		}
		auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
		const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
		const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
		if (!binding.ready || (!regular_hash_join && !perfect_hash_join) ||
		    (regular_hash_join && !binding.hash_table) || (perfect_hash_join && !binding.perfect_layout.ready) ||
		    (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		     binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY)) {
			return false;
		}

		const auto current_size = batch.size();
		const auto target_size = current_size + count;
		const auto output_count =
		    output_to_projection ? output_to_projection->size() : projection_op.projections.size();
		if (batch.ColumnCount() != output_count || target_size > STANDARD_VECTOR_SIZE) {
			return false;
		}
		if (skip_projection.empty()) {
			skip_projection.assign(projection_op.projections.size(), 0);
		} else if (skip_projection.size() != projection_op.projections.size()) {
			return false;
		}

		bool materialized_any = false;
		const auto stage_start = SljitRegionStageStart(runtime);
		for (idx_t output_idx = 0; output_idx < output_count; output_idx++) {
			const auto projected_idx = output_to_projection ? (*output_to_projection)[output_idx] : output_idx;
			if (projected_idx >= projection_op.projections.size()) {
				return false;
			}
			if (skip_projection[projected_idx]) {
				continue;
			}
			auto &source_expr = projection_op.projections[projected_idx];
			if (source_expr.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				continue;
			}
			auto &target = batch.data[output_idx];
			if (target.GetVectorType() != VectorType::FLAT_VECTOR || target.GetType() != source_expr.plan.return_type ||
			    !DirectProjectionBatchSupportsType(target.GetType()) || FlatVector::GetCapacity(target) < target_size) {
				continue;
			}

			SljitExecutableRegionExpression remapped_expr;
			if (!TryBuildHashJoinProbeLHSProjectionExpression(binding, join_input, source_expr, remapped_expr)) {
				if (!regular_hash_join) {
					continue;
				}
				auto &adapter_scratch = scratch.ExpressionAdapterScratch(projection_idx, projected_idx);
				bool used_row_pointer_generated_source = false;
				if (!TryMaterializeHashJoinComputedRHSProjectionToBatch(
				        binding, source_expr, row_pointers, batch, output_idx, current_size, count, adapter_scratch,
				        used_row_pointer_generated_source)) {
					if (!TryMaterializeHashJoinMixedProjectionToBatch(binding, source_expr, join_input, match_selection,
					                                                  row_pointers, batch, output_idx, current_size,
					                                                  count, adapter_scratch)) {
						continue;
					}
				}
				if (used_row_pointer_generated_source) {
					RecordSljitRegionRuntimePath(runtime, projection_op.kind,
					                             "direct_rhs_row_pointer_generated_projection", count);
				}
				skip_projection[projected_idx] = 1;
				materialized_any = true;
				continue;
			}
			auto &adapter_scratch = scratch.ExpressionAdapterScratch(projection_idx, projected_idx);
			if (!TryExecuteProjectionExpressionToBatch(remapped_expr, join_input, target, current_size, count,
			                                           &match_selection, adapter_scratch)) {
				continue;
			}
			skip_projection[projected_idx] = 1;
			materialized_any = true;
		}

		if (materialized_any) {
			RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
			                              "post_join_direct_computed_projection", stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind,
			                                         "direct_post_join_computed_projection", count);
		}
		return materialized_any;
	}

	static bool BuildHashJoinBuildRequiredInputColumns(const ExecutionRegionSinkInfo &sink_info, idx_t column_count,
	                                                   vector<uint8_t> &required_columns) {
		if (sink_info.kind != ExecutionRegionSinkKind::HASH_JOIN_BUILD || !sink_info.hash_join_contract.present) {
			return false;
		}
		required_columns.assign(column_count, 0);
		for (auto &key : sink_info.hash_join_keys) {
			if (!key.supported_reference || key.input_index >= column_count) {
				return false;
			}
			required_columns[key.input_index] = 1;
		}
		auto &contract = sink_info.hash_join_contract;
		for (auto input_index : contract.payload_column_indices) {
			if (input_index >= column_count) {
				return false;
			}
			required_columns[input_index] = 1;
		}
		return true;
	}

	static bool RequiredColumnsAreStrictSubset(const vector<uint8_t> &required_columns) {
		bool has_required = false;
		bool has_dead = false;
		for (auto required : required_columns) {
			has_required = has_required || required;
			has_dead = has_dead || !required;
		}
		return has_required && has_dead;
	}

	bool MaterializeSelectionOnlyHashJoinProbeOutput(ExecutionRegionRuntime &runtime,
	                                                 SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                                 SljitExecutableRegionOp &hash_join_op, DataChunk &join_input,
	                                                 const SelectionVector &match_selection,
	                                                 const SelectionVector &build_selection, Vector &row_pointers,
	                                                 idx_t count, DataChunk &join_output) {
		if (!scratch.HasOperatorBinding(hash_join_idx)) {
			return false;
		}
		auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(
		    runtime, SljitRegionStageName(hash_join_idx, hash_join_op.kind, "materialize_output_fallback"));
		if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE) {
			RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "row_pointer_reference", count);
			RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "final_output", count);
			ExecutionMaterializeHashJoinProbe(binding, join_input, row_pointers, match_selection, count, join_output,
			                                  runtime.TraceRuntime() ? &recorder : nullptr);
		} else if (binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE) {
			RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "perfect_selection_reference", count);
			RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "final_output", count);
			ExecutionMaterializePerfectHashJoinProbe(binding, join_input, match_selection, build_selection, count,
			                                         join_output, runtime.TraceRuntime() ? &recorder : nullptr);
		} else {
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, "materialize_output_fallback",
		                              materialize_stage_start);
		return true;
	}

	bool TryMaterializeHashJoinRequiredSources(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                           idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op,
	                                           DataChunk &join_input, const SelectionVector &match_selection,
	                                           const SelectionVector &build_selection, Vector &row_pointers,
	                                           idx_t count, const vector<uint8_t> &required_columns,
	                                           DataChunk &sink_input) {
		if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx) ||
		    sink_input.ColumnCount() != required_columns.size()) {
			return false;
		}
		auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
		auto stage_start = SljitRegionStageStart(runtime);
		bool materialized = false;
		if (runtime.TraceRuntime()) {
			auto stage_name = SljitRegionStageName(hash_join_idx, hash_join_op.kind, "materialize_required_sources");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			materialized = ExecutionMaterializeHashJoinProbeProjectionSources(
			    binding, join_input, row_pointers, match_selection, count, required_columns, sink_input, &recorder,
			    optional_ptr<const SelectionVector>(&build_selection));
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			materialized = ExecutionMaterializeHashJoinProbeProjectionSources(
			    binding, join_input, row_pointers, match_selection, count, required_columns, sink_input, nullptr,
			    optional_ptr<const SelectionVector>(&build_selection));
		}
		if (!materialized) {
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, "materialize_required_sources",
		                              stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "required_sink_sources", count);
		return true;
	}

	bool TryMaterializeHashJoinRequiredProjectionViews(ExecutionRegionRuntime &runtime,
	                                                   SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                                   idx_t projection_idx, SljitExecutableRegionOp &projection_op,
	                                                   DataChunk &join_input, const SelectionVector &match_selection,
	                                                   const SelectionVector &build_selection, Vector &row_pointers,
	                                                   idx_t count, const vector<uint8_t> &required_columns,
	                                                   DataChunk &projected) {
		if (count == 0 || !scratch.HasOperatorBinding(hash_join_idx) ||
		    required_columns.size() != projection_op.projections.size() ||
		    projected.ColumnCount() != projection_op.projections.size()) {
			return false;
		}
		auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
		const bool regular_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE;
		const bool perfect_hash_join = binding.layout_kind == ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE;
		if (!binding.ready || (!regular_hash_join && !perfect_hash_join) ||
		    (regular_hash_join && !binding.hash_table) || (perfect_hash_join && !binding.perfect_layout.ready) ||
		    (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		     binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY)) {
			return false;
		}

		const auto lhs_column_count = binding.lhs_output_column_indices.size();
		const auto rhs_column_count =
		    binding.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD
		        ? (regular_hash_join ? binding.rhs_output_column_count : binding.perfect_layout.rhs_output_column_count)
		        : 0;
		const auto join_output_column_count = lhs_column_count + rhs_column_count;
		auto stage_start = SljitRegionStageStart(runtime);
		for (idx_t projected_idx = 0; projected_idx < projection_op.projections.size(); projected_idx++) {
			if (!required_columns[projected_idx]) {
				continue;
			}
			auto &expr = projection_op.projections[projected_idx];
			SljitExecutableRegionExpression remapped_expr;
			idx_t join_output_source_index;
			if (!TryBuildSingleSourceProjectionExpression(expr, remapped_expr, join_output_source_index) ||
			    !ProjectionIsSingleSourceReferenceLike(remapped_expr.plan) ||
			    join_output_source_index >= join_output_column_count) {
				return false;
			}
			auto &target = projected.data[projected_idx];
			if (target.GetType() != expr.plan.return_type) {
				return false;
			}
			if (join_output_source_index < lhs_column_count) {
				const auto input_col = binding.lhs_output_column_indices[join_output_source_index];
				if (input_col >= join_input.ColumnCount() || join_input.data[input_col].GetType() != target.GetType()) {
					return false;
				}
				target.Slice(join_input.data[input_col], match_selection, count);
			} else {
				if (binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD) {
					return false;
				}
				const auto rhs_col_idx = join_output_source_index - lhs_column_count;
				if (regular_hash_join) {
					GatherHashJoinRHSColumn(binding, row_pointers, count, rhs_col_idx, target);
				} else {
					if (binding.perfect_layout.rhs_dictionary_buffers.size() !=
					        binding.perfect_layout.rhs_output_column_count ||
					    rhs_col_idx >= binding.perfect_layout.rhs_output_column_count ||
					    target.GetType() != binding.perfect_layout.rhs_output_types[rhs_col_idx]) {
						return false;
					}
					target.Dictionary(binding.perfect_layout.rhs_dictionary_buffers[rhs_col_idx], build_selection,
					                  count);
				}
			}
		}
		projected.SetChildCardinality(count);
		RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind, "required_projection_views",
		                              stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "required_projection_view", count);
		return true;
	}

	bool TryDirectMaterializeHashJoinProjectionSourcesToBatch(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	    idx_t projection_idx, SljitExecutableRegionOp &projection_op, DataChunk &join_input,
	    const SelectionVector &match_selection, Vector &row_pointers, DataChunk &join_source, DataChunk &batch,
	    optional_ptr<const vector<idx_t>> output_to_projection = nullptr,
	    optional_ptr<Vector> projected_hashes = nullptr,
	    optional_ptr<const vector<uint8_t>> extra_skip_projection = nullptr) {
		if (join_source.size() == 0 || !scratch.HasOperatorBinding(hash_join_idx)) {
			return false;
		}
		vector<uint8_t> direct_reference_skip;
		if (extra_skip_projection) {
			if (extra_skip_projection->size() != projection_op.projections.size()) {
				return false;
			}
			direct_reference_skip = *extra_skip_projection;
		}
		const bool direct_references_materialized = TryMaterializeHashJoinReferenceProjectionsToBatch(
		    runtime, scratch, hash_join_idx, projection_idx, projection_op, join_input, match_selection, row_pointers,
		    batch, output_to_projection, join_source.size(), direct_reference_skip);
		const bool direct_computed_materialized = TryMaterializeHashJoinComputedProjectionsToBatch(
		    runtime, scratch, hash_join_idx, projection_idx, projection_op, join_input, match_selection, row_pointers,
		    batch, output_to_projection, join_source.size(), direct_reference_skip);
		const bool direct_projection_materialized = direct_references_materialized || direct_computed_materialized;
		auto direct_projection_skip_ptr = ProjectionSkipHasAny(direct_reference_skip)
		                                      ? optional_ptr<const vector<uint8_t>>(&direct_reference_skip)
		                                      : nullptr;
		if (direct_projection_materialized &&
		    SelectedProjectionOutputsAreSkipped(projection_op, output_to_projection, direct_reference_skip)) {
			FinishDirectProjectionBatchTargets(batch, batch.size() + join_source.size(), !direct_computed_materialized);
			if (projected_hashes) {
				HashDirectProjectionBatch(runtime, projection_idx, projection_op, output_to_projection != nullptr,
				                          batch, *projected_hashes);
			}
			RecordDirectProjectionBatchMaterialization(runtime, projection_idx, projection_op,
			                                           output_to_projection != nullptr, join_source.size(),
			                                           SljitRegionStageStart(runtime));
			return true;
		}
		vector<uint8_t> referenced_columns;
		if (!BuildProjectionSourceColumnSet(projection_op, join_source.ColumnCount(), output_to_projection,
		                                    direct_projection_skip_ptr, referenced_columns)) {
			return false;
		}
		auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
		auto stage_start = SljitRegionStageStart(runtime);
		bool materialized = false;
		if (runtime.TraceRuntime()) {
			auto stage_name =
			    SljitRegionStageName(hash_join_idx, ops[hash_join_idx].kind, "materialize_projection_sources");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			materialized = ExecutionMaterializeHashJoinProbeProjectionSources(
			    binding, join_input, row_pointers, match_selection, join_source.size(), referenced_columns, join_source,
			    &recorder, optional_ptr<const SelectionVector>(&scratch.HashJoinBuildSelection(hash_join_idx)));
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			materialized = ExecutionMaterializeHashJoinProbeProjectionSources(
			    binding, join_input, row_pointers, match_selection, join_source.size(), referenced_columns, join_source,
			    nullptr, optional_ptr<const SelectionVector>(&scratch.HashJoinBuildSelection(hash_join_idx)));
		}
		if (!materialized) {
			return false;
		}
		RecordSljitRegionStageRuntime(runtime, hash_join_idx, ops[hash_join_idx].kind, "materialize_projection_sources",
		                              stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, ops[hash_join_idx].kind, "projection_source",
		                                         join_source.size());
		return TryDirectMaterializeFixedProjectionToBatch(runtime, scratch, projection_idx, projection_op, join_source,
		                                                  batch, output_to_projection, direct_projection_skip_ptr,
		                                                  projected_hashes);
	}

	bool TryExecuteFullPipeline(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) override {
		if (!ExecutionRegionABIIsFullPipeline(abi)) {
			throw InternalException("SLJIT full pipeline kernel entered without full-pipeline ABI");
		}
		if (CanBatchFilteredSourceFullPipeline()) {
			return TryExecuteFullPipelineBatched(runtime, result);
		}
		if (CanBatchGeneratedFilterProjectionFullPipeline()) {
			return TryExecuteFullPipelineGeneratedFilterBatched(runtime, result);
		}
		if (CanBatchProjectionCountStarGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchHashJoinProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineHashJoinProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchHashJoinDelimJoinSinkFullPipeline()) {
			return TryExecuteFullPipelineSourceInputBatched(runtime, result);
		}
		if (CanBatchGeneratedFilterProjectionHashJoinBuildSinkFullPipeline()) {
			return TryExecuteFullPipelineGeneratedFilterProjectionHashJoinBuildSinkBatched(runtime, result);
		}
		if (CanBatchHashJoinBuildSinkFullPipeline()) {
			return TryExecuteFullPipelineSourceInputBatched(runtime, result);
		}
		if (CanBatchHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineHashJoinProjectionProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchMarkHashJoinFilterProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineMarkHashJoinFilterProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchProjectionHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineProjectionHashJoinProjectionProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchHashJoinProjectionHashJoinProjectionsGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineHashJoinProjectionHashJoinProjectionsGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchProjectionHashJoinProjectionHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineProjectionHashJoinProjectionHashJoinProjectionProjectionGroupedAggregateBatched(
			    runtime, result);
		}
		if (CanBatchHashJoinHashJoinProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineHashJoinHashJoinProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchHashJoinHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineHashJoinHashJoinProjectionProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchGeneratedFilterProjectionHashJoinProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineGeneratedFilterHashJoinProjectionGroupedAggregateBatched(runtime, result);
		}
		return TryExecuteFullPipelineUnbatched(runtime, result);
	}

	template <class T>
	bool TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated(ExecutionRegionRuntime &runtime,
	                                                                               ExecutionRegionResult &result) {
		static constexpr idx_t PROJECTION_IDX = 0;
		static constexpr idx_t AGGREGATE_IDX = 1;
		auto &projection_op = ops[PROJECTION_IDX];
		auto &aggregate_op = ops[AGGREGATE_IDX];
		auto &native_runtime = runtime.ExecutionOperators();
		auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);

		vector<LogicalType> group_types;
		group_types.reserve(sink_info.groups.size());
		for (auto &group : sink_info.groups) {
			group_types.push_back(group.type);
		}
		DataChunk accumulated_groups;
		accumulated_groups.Initialize(runtime.GetAllocator(), group_types);
		bool bound = false;
		auto bind_stage_start = SljitRegionStageStart(runtime);
		auto &binding = BindNativeSink(native_runtime, scratch, AGGREGATE_IDX, accumulated_groups, sink_info,
		                               "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink", bound);
		if (bound) {
			RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind, "bind_sink_contract",
			                              bind_stage_start);
		}
		if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready ||
		    !binding.aggregate_update.grouped_state.ready || !binding.aggregate_update.grouped_state.state) {
			return TryExecuteFullPipelineUnbatched(runtime, result);
		}
		auto &payload_lanes =
		    scratch.AggregatePayloadLanes(AGGREGATE_IDX, sink_info.aggregates, binding.aggregate_update.primitive);
		if (payload_lanes.size() != 1 || !payload_lanes[0] ||
		    payload_lanes[0]->kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
			return TryExecuteFullPipelineUnbatched(runtime, result);
		}

		std::array<T, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> accumulated_keys;
		std::array<int64_t, SLJIT_LOCAL_PREAGGREGATED_GROUP_LIMIT> accumulated_counts;
		idx_t accumulated_group_count = 0;
		idx_t accumulated_input_count = 0;
		vector<int64_t> accumulated_deltas;
		DataChunk chunk_groups;
		chunk_groups.Initialize(runtime.GetAllocator(), group_types);
		vector<int64_t> chunk_deltas;

		auto flush_accumulated_groups = [&]() {
			if (accumulated_group_count == 0) {
				return;
			}
			MaterializePreaggregatedCountStarGroups(accumulated_keys, accumulated_counts, accumulated_group_count,
			                                        accumulated_groups, accumulated_deltas);
			auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(AGGREGATE_IDX);
			preaggregate_scratch.Prepare(payload_lanes, accumulated_group_count);
			preaggregate_scratch.payloads[0].int64_values = accumulated_deltas;
			if (!TryExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
			        runtime, scratch, AGGREGATE_IDX, aggregate_op, accumulated_groups, preaggregate_scratch,
			        payload_lanes, binding.aggregate_update.grouped_state, accumulated_input_count, false, nullptr)) {
				throw InternalException("SLJIT projection count-star grouped accumulator failed to flush");
			}
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "cross_chunk_preaggregated_count_star_update",
			                             accumulated_input_count);
			accumulated_group_count = 0;
			accumulated_input_count = 0;
		};

		auto execute_projected_fallback = [&](DataChunk &projected) -> bool {
			flush_accumulated_groups();
			auto sink_result =
			    ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, AGGREGATE_IDX, aggregate_op, projected);
			if (sink_result == SinkResultType::BLOCKED) {
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			return false;
		};

		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		idx_t fetched_chunks = 0;
		while (true) {
			if (fetched_chunks >= max_source_fetches) {
				flush_accumulated_groups();
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				flush_accumulated_groups();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk) {
				auto next_batch_result =
				    runtime.AdvanceSinkBatch(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT);
				if (next_batch_result == SinkNextBatchType::BLOCKED) {
					flush_accumulated_groups();
					result = ExecutionRegionResult::INTERRUPTED;
					return true;
				}
			}
			if (source_chunk && source_chunk->size() > 0) {
				auto &projected = scratch.TemporaryChunk(PROJECTION_IDX);
				projected.Reset();
				auto projection_stage_start = SljitRegionStageStart(runtime);
				ExecuteProjection(scratch, PROJECTION_IDX, projection_op, *source_chunk, projected);
				RecordSljitRegionStageRuntime(runtime, PROJECTION_IDX, projection_op.kind, projection_stage_start);
				if (projected.size() > 0) {
					auto preaggregate_stage_start = SljitRegionStageStart(runtime);
					bool merged_projected = false;
					if (!TryPreaggregateFixedWidthCountStarGroups(projected, chunk_groups, chunk_deltas)) {
						if (execute_projected_fallback(projected)) {
							return true;
						}
					} else if (!MergePreaggregatedFixedWidthCountStarGroupsTemplated<T>(
					               chunk_groups, chunk_deltas, accumulated_keys, accumulated_counts,
					               accumulated_group_count)) {
						flush_accumulated_groups();
						if (!MergePreaggregatedFixedWidthCountStarGroupsTemplated<T>(
						        chunk_groups, chunk_deltas, accumulated_keys, accumulated_counts,
						        accumulated_group_count)) {
							if (execute_projected_fallback(projected)) {
								return true;
							}
						} else {
							merged_projected = true;
						}
					} else {
						merged_projected = true;
					}
					if (merged_projected) {
						accumulated_input_count += projected.size();
						RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
						                              "cross_chunk_preaggregate_count_star_groups",
						                              preaggregate_stage_start);
					}
				}
			}
			if (source_result == SourceResultType::FINISHED) {
				flush_accumulated_groups();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                      ExecutionRegionResult &result) {
		auto &group_type = ops[1].aggregate_update.plan.sink_info.groups[0].type;
		switch (group_type.InternalType()) {
		case PhysicalType::INT8:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<int8_t>(runtime, result);
		case PhysicalType::INT16:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<int16_t>(runtime, result);
		case PhysicalType::INT32:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<int32_t>(runtime, result);
		case PhysicalType::INT64:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<int64_t>(runtime, result);
		case PhysicalType::INT128:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<hugeint_t>(runtime,
			                                                                                            result);
		case PhysicalType::UINT8:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uint8_t>(runtime, result);
		case PhysicalType::UINT16:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uint16_t>(runtime, result);
		case PhysicalType::UINT32:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uint32_t>(runtime, result);
		case PhysicalType::UINT64:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uint64_t>(runtime, result);
		case PhysicalType::UINT128:
			return TryExecuteFullPipelineProjectionCountStarGroupedAggregateBatchedTemplated<uhugeint_t>(runtime,
			                                                                                             result);
		default:
			return TryExecuteFullPipelineUnbatched(runtime, result);
		}
	}

	bool TryExecuteFullPipelineSourceInputBatched(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_batches = 0;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		idx_t trace_op_idx = 0;
		for (idx_t op_idx = 0; op_idx < ops.size(); op_idx++) {
			if (ops[op_idx].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
				trace_op_idx = op_idx;
				break;
			}
		}
		auto &trace_op = ops[trace_op_idx];
		DataChunk source_batch;
		bool source_batch_initialized = false;

		auto execute_source_batch = [&](DataChunk &input) -> bool {
			if (input.size() == 0) {
				return false;
			}
			auto sink_result = ExecuteNativeFullPipeline(runtime, scratch, input);
			if (sink_result == SinkResultType::BLOCKED) {
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			processed_batches++;
			return false;
		};

		auto flush_source_batch = [&]() -> bool {
			if (!source_batch_initialized || source_batch.size() == 0) {
				return false;
			}
			if (execute_source_batch(source_batch)) {
				return true;
			}
			source_batch.Reset();
			return false;
		};

		auto append_source_batch = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			auto next_batch_result = runtime.AdvanceSinkBatch(source_chunk, have_more_output);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				if (flush_source_batch()) {
					return true;
				}
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			if (source_chunk.size() == 0) {
				return false;
			}
			if (!source_batch_initialized) {
				source_batch.Initialize(runtime.GetAllocator(), source_chunk.GetTypes());
				source_batch_initialized = true;
			}
			if (source_batch.ColumnCount() != source_chunk.ColumnCount()) {
				if (flush_source_batch()) {
					return true;
				}
				return execute_source_batch(source_chunk);
			}
			if (source_batch.size() + source_chunk.size() > STANDARD_VECTOR_SIZE) {
				if (flush_source_batch()) {
					return true;
				}
			}
			if (source_chunk.size() == STANDARD_VECTOR_SIZE && source_batch.size() == 0) {
				return execute_source_batch(source_chunk);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(source_batch, source_chunk)) {
				source_batch.Append(source_chunk);
			}
			RecordSljitRegionStageRuntime(runtime, trace_op_idx, trace_op.kind, "source_input_batch_append",
			                              append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, trace_op.kind, "source_input_batch", source_chunk.size());
			if (source_batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_source_batch()) {
					return true;
				}
			}
			return false;
		};

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
				if (flush_source_batch()) {
					return true;
				}
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				if (flush_source_batch()) {
					return true;
				}
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk &&
			    append_source_batch(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return true;
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_source_batch()) {
					return true;
				}
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineGeneratedFilterProjectionHashJoinBuildSinkBatched(ExecutionRegionRuntime &runtime,
	                                                                             ExecutionRegionResult &result) {
		static constexpr idx_t FILTER_IDX = 0;
		static constexpr idx_t PROJECTION_IDX = 1;
		static constexpr idx_t HASH_JOIN_IDX = 2;
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_batches = 0;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		auto &filter_op = ops[FILTER_IDX];
		auto &projection_op = ops[PROJECTION_IDX];
		auto &hash_join_op = ops[HASH_JOIN_IDX];
		DataChunk filtered_batch;
		bool filtered_batch_initialized = false;

		auto execute_filtered_batch = [&](DataChunk &input) -> bool {
			if (input.size() == 0) {
				return false;
			}
			auto sink_result = ExecuteNativeFullPipelineFrom(runtime, scratch, HASH_JOIN_IDX, input);
			if (sink_result == SinkResultType::BLOCKED) {
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			processed_batches++;
			return false;
		};

		auto flush_filtered_batch = [&]() -> bool {
			if (!filtered_batch_initialized || filtered_batch.size() == 0) {
				return false;
			}
			if (execute_filtered_batch(filtered_batch)) {
				return true;
			}
			filtered_batch.Reset();
			return false;
		};

		auto append_filtered_batch = [&](DataChunk &filtered) -> bool {
			if (filtered.size() == 0) {
				return false;
			}
			if (!filtered_batch_initialized) {
				filtered_batch.Initialize(runtime.GetAllocator(), projection_op.output_types);
				filtered_batch_initialized = true;
			}
			if (filtered_batch.ColumnCount() != filtered.ColumnCount()) {
				if (flush_filtered_batch()) {
					return true;
				}
				return execute_filtered_batch(filtered);
			}
			if (filtered_batch.size() + filtered.size() > STANDARD_VECTOR_SIZE) {
				if (flush_filtered_batch()) {
					return true;
				}
			}
			if (filtered.size() == STANDARD_VECTOR_SIZE && filtered_batch.size() == 0) {
				return execute_filtered_batch(filtered);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(filtered_batch, filtered)) {
				filtered_batch.Append(filtered);
			}
			RecordSljitRegionStageRuntime(runtime, HASH_JOIN_IDX, hash_join_op.kind, "filtered_input_batch_append",
			                              append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "filtered_input_batch",
			                                         filtered.size());
			if (filtered_batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_filtered_batch()) {
					return true;
				}
			}
			return false;
		};

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			auto next_batch_result = runtime.AdvanceSinkBatch(source_chunk, have_more_output);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				if (flush_filtered_batch()) {
					return true;
				}
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			if (source_chunk.size() == 0) {
				return false;
			}
			bool direct_stopped = false;
			if (TryAppendSelectedReferenceProjectionBatch(
			        runtime, scratch, FILTER_IDX, filter_op, projection_op, hash_join_op, source_chunk, filtered_batch,
			        filtered_batch_initialized, flush_filtered_batch, direct_stopped)) {
				return direct_stopped;
			}
			auto &filtered = scratch.TemporaryChunk(PROJECTION_IDX);
			filtered.Reset();
			auto filter_stage_start = SljitRegionStageStart(runtime);
			ExecuteFilterProjection(scratch, filter_op, projection_op, PROJECTION_IDX, source_chunk, filtered,
			                        scratch.FilterSelection(FILTER_IDX));
			RecordSljitRegionStageRuntimeWithSuffix(runtime, FILTER_IDX, filter_op.kind, "+projection",
			                                        filter_stage_start);
			return append_filtered_batch(filtered);
		};

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
				if (flush_filtered_batch()) {
					return true;
				}
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				if (flush_filtered_batch()) {
					return true;
				}
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk &&
			    execute_source_chunk(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return true;
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_filtered_batch()) {
					return true;
				}
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineUnbatched(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t processed_chunks = 0;
		const auto max_chunks =
		    CanUseExtendedRegularHashAggregateSourceBudget() || CanUseExtendedNativeSinkSourceBudget()
		        ? SljitBatchedSourceContractFetchBudget(runtime.MaxChunks())
		        : runtime.MaxChunks();
		while (true) {
			if (processed_chunks >= max_chunks) {
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			if (source_chunk) {
				auto next_batch_result =
				    runtime.AdvanceSinkBatch(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT);
				if (next_batch_result == SinkNextBatchType::BLOCKED) {
					result = ExecutionRegionResult::INTERRUPTED;
					return true;
				}
			}
			if (source_chunk && source_chunk->size() > 0) {
				auto sink_result = ExecuteNativeFullPipeline(runtime, scratch, *source_chunk);
				if (sink_result == SinkResultType::BLOCKED) {
					result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
					                                          : ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (sink_result == SinkResultType::FINISHED) {
					result = ExecutionRegionResult::FINISHED;
					return true;
				}
				processed_chunks++;
			}
			if (source_result == SourceResultType::FINISHED) {
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineBatched(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_batches = 0;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());

		auto execute_chunk = [&](DataChunk &input, bool have_more_output) -> bool {
			if (input.size() == 0) {
				return false;
			}
			auto next_batch_result = runtime.AdvanceSinkBatch(input, have_more_output);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			auto sink_result = ExecuteNativeFullPipeline(runtime, scratch, input);
			if (sink_result == SinkResultType::BLOCKED) {
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			processed_batches++;
			return false;
		};

		auto flush_batch = [&](bool have_more_output) -> bool {
			auto batch = runtime.PendingSourceContractBatch();
			if (!batch) {
				return false;
			}
			if (execute_chunk(*batch, have_more_output)) {
				return true;
			}
			runtime.ResetSourceContractBatch();
			return false;
		};

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk && source_chunk->size() > 0) {
				auto &batch = runtime.PrepareSourceContractBatch(source_chunk->GetTypes());
				if (batch.size() + source_chunk->size() > STANDARD_VECTOR_SIZE) {
					if (flush_batch(true)) {
						return true;
					}
				}
				if (source_chunk->size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
					if (execute_chunk(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
						return true;
					}
				} else {
					if (!SljitTryFastAppendFixedFlatAllValid(batch, *source_chunk)) {
						batch.Append(*source_chunk);
					}
					if (batch.size() == STANDARD_VECTOR_SIZE) {
						if (flush_batch(source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
							return true;
						}
					}
				}
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_batch(false)) {
					return true;
				}
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineGeneratedFilterBatched(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_batches = 0;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());

		auto execute_batch = [&](DataChunk &input) -> bool {
			if (input.size() == 0) {
				return false;
			}
			auto sink_result = ExecuteNativeFullPipelineFrom(runtime, scratch, 2, input);
			if (sink_result == SinkResultType::BLOCKED) {
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			processed_batches++;
			return false;
		};

		auto flush_batch = [&]() -> bool {
			auto batch = runtime.PendingSourceContractBatch();
			if (!batch) {
				return false;
			}
			if (execute_batch(*batch)) {
				return true;
			}
			runtime.ResetSourceContractBatch();
			return false;
		};

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk) {
				auto next_batch_result =
				    runtime.AdvanceSinkBatch(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT);
				if (next_batch_result == SinkNextBatchType::BLOCKED) {
					result = ExecutionRegionResult::INTERRUPTED;
					return true;
				}
			}
			if (source_chunk && source_chunk->size() > 0) {
				auto &filter_selection = scratch.FilterSelection(0);
				auto filter_stage_start = SljitRegionStageStart(runtime);
				auto selected_count =
				    SelectFilter(ops[0], *source_chunk, filter_selection, scratch.ExpressionAdapterScratch(0, 0));
				RecordSljitRegionStageRuntime(runtime, 0, ops[0].kind, "selection", filter_stage_start);
				if (selected_count > 0) {
					auto &batch = runtime.PrepareSourceContractBatch(ops[1].output_types);
					if (batch.size() + selected_count > STANDARD_VECTOR_SIZE) {
						if (flush_batch()) {
							return true;
						}
					}
					auto append_stage_start = SljitRegionStageStart(runtime);
					if (!TryAppendReferenceProjectionToBatch(batch, *source_chunk, ops[1], filter_selection,
					                                         selected_count)) {
						auto &filtered = scratch.TemporaryChunk(1);
						filtered.Reset();
						ExecuteProjection(scratch, 1, ops[1], *source_chunk, filtered, &filter_selection,
						                  selected_count);
						if (!SljitTryFastAppendFixedFlatAllValid(batch, filtered)) {
							batch.Append(filtered);
						}
					}
					RecordSljitRegionStageRuntime(runtime, 1, ops[1].kind, "batch_append", append_stage_start);
					if (batch.size() == STANDARD_VECTOR_SIZE) {
						if (flush_batch()) {
							return true;
						}
					}
				}
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_batch()) {
					return true;
				}
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineHashJoinProjectionGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                     ExecutionRegionResult &result) {
		auto prepare_join_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
		                              SourceResultType source_result, DataChunk *&join_input) -> bool {
			auto next_batch_result =
			    runtime.AdvanceSinkBatch(source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			join_input = &source_chunk;
			return false;
		};
		return TryExecuteFullPipelineHashJoinProjectionGroupedAggregateBatched(runtime, result, 0, 1, 2,
		                                                                       prepare_join_input);
	}

	bool TryExecuteFullPipelineHashJoinProjectionProjectionGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                               ExecutionRegionResult &result) {
		auto prepare_join_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
		                              SourceResultType source_result, DataChunk *&join_input) -> bool {
			auto next_batch_result =
			    runtime.AdvanceSinkBatch(source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			join_input = &source_chunk;
			return false;
		};
		auto project_join_output = [&](SljitRegionExecutionScratch &scratch, DataChunk &join_output,
		                               DataChunk &projected) {
			auto &first_projection_op = ops[1];
			auto &second_projection_op = ops[2];
			auto &intermediate = scratch.TemporaryChunk(1);

			intermediate.Reset();
			auto first_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(intermediate, join_output, first_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 1, first_projection_op.kind, "post_join_reference_projection",
				                              first_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, first_projection_op.kind,
				                                         "reference_post_join_projection", intermediate.size());
			} else {
				ExecuteProjection(scratch, 1, first_projection_op, join_output, intermediate);
				RecordSljitRegionStageRuntime(runtime, 1, first_projection_op.kind, "post_join_batch_projection",
				                              first_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, first_projection_op.kind,
				                                         "copied_post_join_projection", intermediate.size());
			}

			projected.Reset();
			auto second_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, intermediate, second_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 2, second_projection_op.kind, "post_join_reference_projection",
				                              second_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, second_projection_op.kind,
				                                         "reference_post_join_projection", projected.size());
			} else {
				ExecuteProjection(scratch, 2, second_projection_op, intermediate, projected);
				RecordSljitRegionStageRuntime(runtime, 2, second_projection_op.kind, "post_join_batch_projection",
				                              second_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, second_projection_op.kind,
				                                         "copied_post_join_projection", projected.size());
			}
		};
		auto direct_project_join_output_to_batch = [&](SljitRegionExecutionScratch &scratch, DataChunk *join_input,
		                                               const SelectionVector *match_selection, Vector *row_pointers,
		                                               DataChunk &join_output, DataChunk &batch) {
			vector<idx_t> output_to_projection;
			if (!BuildReferenceProjectionOutputMap(ops[1], ops[2], output_to_projection)) {
				return false;
			}
			auto output_map = optional_ptr<const vector<idx_t>>(&output_to_projection);
			if (join_input && match_selection && row_pointers) {
				return TryDirectMaterializeHashJoinProjectionSourcesToBatch(runtime, scratch, 0, 1, ops[1], *join_input,
				                                                            *match_selection, *row_pointers,
				                                                            join_output, batch, output_map);
			}
			return TryDirectMaterializeFixedProjectionToBatch(runtime, scratch, 1, ops[1], join_output, batch,
			                                                  output_map);
		};
		return TryExecuteFullPipelineHashJoinProjectedGroupedAggregateBatched(
		    runtime, result, 0, 2, 3, prepare_join_input, project_join_output, direct_project_join_output_to_batch);
	}

	bool TryExecuteFullPipelineMarkHashJoinFilterProjectionGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                               ExecutionRegionResult &result) {
		static constexpr idx_t HASH_JOIN_IDX = 0;
		static constexpr idx_t FILTER_IDX = 1;
		static constexpr idx_t PROJECTION_IDX = 2;
		static constexpr idx_t AGGREGATE_IDX = 3;

		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_output_rows = 0;
		bool deferred_grouped_finish = false;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		auto &hash_join_op = ops[HASH_JOIN_IDX];
		auto &filter_op = ops[FILTER_IDX];
		auto &projection_op = ops[PROJECTION_IDX];
		auto &aggregate_op = ops[AGGREGATE_IDX];
		auto &native_runtime = runtime.ExecutionOperators();
		DataChunk compact_groups;
		compact_groups.Initialize(runtime.GetAllocator(), projection_op.output_types);
		vector<int64_t> preaggregated_count_deltas;

		auto finish_deferred_grouped_update = [&]() {
			if (!deferred_grouped_finish) {
				return;
			}
			auto &binding = scratch.SinkBinding(AGGREGATE_IDX);
			if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
			    !binding.aggregate_update.grouped_state.state) {
				throw InternalException("SLJIT deferred grouped aggregate finish is missing aggregate state");
			}
			auto finish_stage_start = SljitRegionStageStart(runtime);
			binding.aggregate_update.grouped_state.state->FinishStateUpdates();
			RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
			                              "finish_deferred_grouped_state_updates", finish_stage_start);
			deferred_grouped_finish = false;
		};

		auto execute_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto sink_result =
			    ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, AGGREGATE_IDX, aggregate_op, projected,
			                                 nullptr, DConstants::INVALID_INDEX, true, &deferred_grouped_finish);
			if (sink_result == SinkResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			SljitChargeDownstreamRows(processed_output_rows, projected.size());
			return false;
		};

		auto flush_projected_batch = [&]() -> bool {
			auto batch = runtime.PendingSourceContractBatch();
			if (!batch) {
				return false;
			}
			if (execute_projected_batch(*batch)) {
				return true;
			}
			runtime.ResetSourceContractBatch();
			return false;
		};

		auto append_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(projection_op.output_types);
			if (batch.size() + projected.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (projected.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
				return execute_projected_batch(projected);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(batch, projected)) {
				batch.Append(projected);
			}
			RecordSljitRegionStageRuntime(runtime, PROJECTION_IDX, projection_op.kind, "direct_mark_batch_append",
			                              append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "direct_mark_batch",
			                                         projected.size());
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto try_preaggregated_count_star_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto preaggregate_stage_start = SljitRegionStageStart(runtime);
			if (!TryPreaggregateFixedWidthCountStarGroups(projected, compact_groups, preaggregated_count_deltas)) {
				return false;
			}
			RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
			                              "local_preaggregate_count_star_groups", preaggregate_stage_start);
			if (!TryExecutePreaggregatedCountStarGroupedAggregateUpdate(
			        runtime, native_runtime, scratch, AGGREGATE_IDX, aggregate_op, compact_groups,
			        preaggregated_count_deltas, projected.size(), true, &deferred_grouped_finish)) {
				return false;
			}
			SljitChargeDownstreamRows(processed_output_rows, projected.size());
			return true;
		};

		auto materialize_mark_output_fallback = [&](DataChunk &join_input, idx_t mark_count, DataChunk &join_output) {
			if (!scratch.HasOperatorBinding(HASH_JOIN_IDX)) {
				throw InternalException("SLJIT MARK hash join fallback has no hash join binding");
			}
			auto &binding = scratch.OperatorBinding(HASH_JOIN_IDX).hash_join_probe;
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(
			    runtime, SljitRegionStageName(HASH_JOIN_IDX, hash_join_op.kind, "materialize_mark_output_fallback"));
			RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "final_output", mark_count);
			ExecutionMaterializeHashJoinProbe(binding, join_input, scratch.HashJoinRowPointers(HASH_JOIN_IDX),
			                                  scratch.FilterSelection(HASH_JOIN_IDX), mark_count, join_output,
			                                  runtime.TraceRuntime() ? &recorder : nullptr);
			RecordSljitRegionStageRuntime(runtime, HASH_JOIN_IDX, hash_join_op.kind, "materialize_mark_output_fallback",
			                              materialize_stage_start);
		};

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			auto next_batch_result = runtime.AdvanceSinkBatch(source_chunk, have_more_output);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				if (flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}

			SljitHashJoinProbeDrainState state;
			auto &join_output = scratch.TemporaryChunk(HASH_JOIN_IDX);
			auto &projected = scratch.TemporaryChunk(PROJECTION_IDX);
			do {
				join_output.Reset();
				string deferred_reason;
				auto hash_join_stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, HASH_JOIN_IDX, hash_join_op, source_chunk, join_output,
				    scratch.FilterSelection(HASH_JOIN_IDX), scratch.HashJoinBuildSelection(HASH_JOIN_IDX),
				    scratch.HashJoinRowPointers(HASH_JOIN_IDX), scratch.HashJoinSourceFormats(HASH_JOIN_IDX),
				    scratch.HashJoinSourceData(HASH_JOIN_IDX), scratch.HashJoinSourceSelections(HASH_JOIN_IDX),
				    scratch.HashJoinSourceValidity(HASH_JOIN_IDX),
				    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(HASH_JOIN_IDX)
				                                                         : nullptr,
				    hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualSelection(HASH_JOIN_IDX)
				        : nullptr,
				    hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(HASH_JOIN_IDX)
				        : nullptr,
				    hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualRowPointers(HASH_JOIN_IDX)
				        : nullptr,
				    state, deferred_reason, false, true);
				RecordSljitRegionStageRuntime(runtime, HASH_JOIN_IDX, hash_join_op.kind, hash_join_stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					if (flush_projected_batch()) {
						return true;
					}
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				const auto mark_count = join_output.size();
				if (mark_count == 0) {
					continue;
				}

				const bool referenced_mark_input =
				    scratch.HasOperatorBinding(HASH_JOIN_IDX) &&
				    TryReferenceMarkProbeFilterInput(scratch.OperatorBinding(HASH_JOIN_IDX).hash_join_probe,
				                                     source_chunk, mark_count, join_output);
				if (referenced_mark_input) {
					idx_t preaggregated_selected_count = 0;
					auto direct_preaggregate_stage_start = SljitRegionStageStart(runtime);
					if (TryPreaggregateProjectedMarkedCountStarGroups(
					        projection_op, join_output, scratch.FilterSelection(HASH_JOIN_IDX), mark_count,
					        compact_groups, preaggregated_count_deltas, preaggregated_selected_count)) {
						RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
						                              "direct_mark_preaggregate_count_star_groups",
						                              direct_preaggregate_stage_start);
						RecordSljitRegionRuntimePath(runtime, filter_op.kind, "direct_mark_preaggregate_selection");
						if (preaggregated_selected_count == 0) {
							continue;
						}
						if (TryExecutePreaggregatedCountStarGroupedAggregateUpdate(
						        runtime, native_runtime, scratch, AGGREGATE_IDX, aggregate_op, compact_groups,
						        preaggregated_count_deltas, preaggregated_selected_count, true,
						        &deferred_grouped_finish)) {
							SljitChargeDownstreamRows(processed_output_rows, preaggregated_selected_count);
							continue;
						}
					}
				}

				auto selection_stage_start = SljitRegionStageStart(runtime);
				auto &mark_selection = scratch.FilterSelection(FILTER_IDX);
				auto selected_count =
				    SelectMarkProbeMatches(scratch.FilterSelection(HASH_JOIN_IDX), mark_count, mark_selection);
				RecordSljitRegionStageRuntimePath(runtime, FILTER_IDX, filter_op.kind, "direct_mark_selection",
				                                  selection_stage_start);
				if (selected_count == 0) {
					continue;
				}

				if (!referenced_mark_input) {
					materialize_mark_output_fallback(source_chunk, mark_count, join_output);
					auto sink_result = ExecuteNativeFullPipelineFrom(runtime, scratch, FILTER_IDX, join_output);
					if (sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED) {
						finish_deferred_grouped_update();
						result = sink_result == SinkResultType::FINISHED
						             ? ExecutionRegionResult::FINISHED
						             : (runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
						                                                 : ExecutionRegionResult::DEFERRED);
						return true;
					}
					continue;
				}

				const auto *execute_sel = selected_count == mark_count ? nullptr : &mark_selection;
				auto direct_preaggregate_stage_start = SljitRegionStageStart(runtime);
				if (TryPreaggregateProjectedCountStarGroups(projection_op, join_output, execute_sel, selected_count,
				                                            compact_groups, preaggregated_count_deltas)) {
					RecordSljitRegionStageRuntime(runtime, AGGREGATE_IDX, aggregate_op.kind,
					                              "direct_preaggregate_count_star_groups",
					                              direct_preaggregate_stage_start);
					if (TryExecutePreaggregatedCountStarGroupedAggregateUpdate(
					        runtime, native_runtime, scratch, AGGREGATE_IDX, aggregate_op, compact_groups,
					        preaggregated_count_deltas, selected_count, true, &deferred_grouped_finish)) {
						SljitChargeDownstreamRows(processed_output_rows, selected_count);
						continue;
					}
				}

				projected.Reset();
				auto projection_stage_start = SljitRegionStageStart(runtime);
				ExecuteProjection(scratch, PROJECTION_IDX, projection_op, join_output, projected, execute_sel,
				                  selected_count);
				RecordSljitRegionStageRuntime(runtime, PROJECTION_IDX, projection_op.kind, "direct_mark_projection",
				                              projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "direct_mark_projection",
				                                         selected_count);
				if (try_preaggregated_count_star_batch(projected)) {
					continue;
				}
				if (append_projected_batch(projected)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
			return false;
		};

		while (true) {
			if (SljitDownstreamRowBudgetReached(processed_output_rows, runtime.MaxChunks()) ||
			    fetched_chunks >= max_source_fetches) {
				if (flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				if (flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk && source_chunk->size() > 0 &&
			    execute_source_chunk(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return true;
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool
	TryExecuteFullPipelineProjectionHashJoinProjectionProjectionGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                                    ExecutionRegionResult &result) {
		const bool bypass_pre_join_projection = CanBypassInt64ToInt32PreJoinProjection();
		const bool first_join_unchecked_key_cast =
		    bypass_pre_join_projection && CanUseUncheckedInt64ToInt32PreJoinProjection(0, 1);
		const bool fast_q12_priority_projection = CanFastProjectQ12PriorityGroupedPayload();
		auto prepare_join_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
		                              SourceResultType source_result, DataChunk *&join_input) -> bool {
			auto next_batch_result =
			    runtime.AdvanceSinkBatch(source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			if (bypass_pre_join_projection) {
				join_input = &source_chunk;
				return false;
			}

			auto &pre_join = scratch.TemporaryChunk(0);
			pre_join.Reset();
			auto &pre_join_projection_op = ops[0];
			auto projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(pre_join, source_chunk, pre_join_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 0, pre_join_projection_op.kind, "pre_join_reference_projection",
				                              projection_stage_start);
			} else {
				ExecuteProjection(scratch, 0, pre_join_projection_op, source_chunk, pre_join);
				RecordSljitRegionStageRuntime(runtime, 0, pre_join_projection_op.kind, "pre_join_batch_projection",
				                              projection_stage_start);
			}
			join_input = pre_join.size() == 0 ? nullptr : &pre_join;
			return false;
		};
		auto project_join_output = [&](SljitRegionExecutionScratch &scratch, DataChunk &join_output,
		                               DataChunk &projected) {
			if (fast_q12_priority_projection) {
				auto q12_projection_stage_start = SljitRegionStageStart(runtime);
				if (TryFastProjectQ12PriorityGroupedPayload(join_output, projected)) {
					RecordSljitRegionStageRuntime(runtime, 3, ops[3].kind, "post_join_q12_priority_projection",
					                              q12_projection_stage_start);
					RecordSljitRegionMaterializationBoundary(runtime, ops[3].kind, "copied_post_join_projection",
					                                         projected.size());
					return;
				}
			}
			auto &first_projection_op = ops[2];
			auto &second_projection_op = ops[3];
			auto &intermediate = scratch.TemporaryChunk(2);

			intermediate.Reset();
			auto first_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(intermediate, join_output, first_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 2, first_projection_op.kind, "post_join_reference_projection",
				                              first_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, first_projection_op.kind,
				                                         "reference_post_join_projection", intermediate.size());
			} else {
				ExecuteProjection(scratch, 2, first_projection_op, join_output, intermediate);
				RecordSljitRegionStageRuntime(runtime, 2, first_projection_op.kind, "post_join_batch_projection",
				                              first_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, first_projection_op.kind,
				                                         "copied_post_join_projection", intermediate.size());
			}

			projected.Reset();
			auto second_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, intermediate, second_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 3, second_projection_op.kind, "post_join_reference_projection",
				                              second_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, second_projection_op.kind,
				                                         "reference_post_join_projection", projected.size());
			} else {
				ExecuteProjection(scratch, 3, second_projection_op, intermediate, projected);
				RecordSljitRegionStageRuntime(runtime, 3, second_projection_op.kind, "post_join_batch_projection",
				                              second_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, second_projection_op.kind,
				                                         "copied_post_join_projection", projected.size());
			}
		};
		auto direct_project_join_output_to_batch = [&](SljitRegionExecutionScratch &scratch, DataChunk *join_input,
		                                               const SelectionVector *match_selection, Vector *row_pointers,
		                                               DataChunk &join_output, DataChunk &batch) {
			vector<idx_t> output_to_projection;
			if (fast_q12_priority_projection ||
			    !BuildReferenceProjectionOutputMap(ops[2], ops[3], output_to_projection)) {
				return false;
			}
			auto output_map = optional_ptr<const vector<idx_t>>(&output_to_projection);
			if (join_input && match_selection && row_pointers) {
				return TryDirectMaterializeHashJoinProjectionSourcesToBatch(runtime, scratch, 1, 2, ops[2], *join_input,
				                                                            *match_selection, *row_pointers,
				                                                            join_output, batch, output_map);
			}
			return TryDirectMaterializeFixedProjectionToBatch(runtime, scratch, 2, ops[2], join_output, batch,
			                                                  output_map);
		};
		return TryExecuteFullPipelineHashJoinProjectedGroupedAggregateBatched(
		    runtime, result, 1, 3, 4, prepare_join_input, project_join_output, direct_project_join_output_to_batch,
		    first_join_unchecked_key_cast);
	}

	bool
	TryExecuteFullPipelineHashJoinProjectionHashJoinProjectionsGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                                   ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_batches = 0;
		bool deferred_grouped_finish = false;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		const auto aggregate_idx = ops.size() - 1;
		const auto final_projection_idx = aggregate_idx - 1;
		auto &first_hash_join_op = ops[0];
		auto &between_join_projection_op = ops[1];
		auto &second_hash_join_op = ops[2];
		auto &final_projection_op = ops[final_projection_idx];
		auto &aggregate_op = ops[aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		DataChunk second_join_batch;
		second_join_batch.Initialize(runtime.GetAllocator(), between_join_projection_op.output_types);
		struct BetweenJoinCompressedPassthrough {
			idx_t between_projection_idx = DConstants::INVALID_INDEX;
			idx_t first_join_output_source_idx = DConstants::INVALID_INDEX;
			idx_t sidecar_idx = DConstants::INVALID_INDEX;
		};
		DataChunk second_join_compressed_passthrough_batch;
		bool second_join_compressed_passthrough_initialized = false;
		bool second_join_compressed_passthrough_usable = true;
		vector<BetweenJoinCompressedPassthrough> between_join_compressed_passthroughs;
		vector<idx_t> compressed_passthrough_by_between_projection;
		struct BetweenJoinPrecomputedPayload {
			idx_t second_projection_idx = DConstants::INVALID_INDEX;
			idx_t sidecar_idx = DConstants::INVALID_INDEX;
			SljitExecutableRegionExpression first_join_expr;
			vector<idx_t> between_projection_indices;
			SljitRuntimeDecimal64DiscountedAmountProgram decimal64_discounted_amount_program;
		};
		DataChunk second_join_precomputed_payload_batch;
		bool second_join_precomputed_payload_initialized = false;
		bool second_join_precomputed_payload_usable = true;
		vector<BetweenJoinPrecomputedPayload> between_join_precomputed_payloads;
		struct FinalGroupCompressedPassthroughSource {
			idx_t final_group_output_idx = DConstants::INVALID_INDEX;
			idx_t final_projection_idx = DConstants::INVALID_INDEX;
			idx_t second_join_projection_idx = DConstants::INVALID_INDEX;
			idx_t second_join_input_col = DConstants::INVALID_INDEX;
			idx_t sidecar_idx = DConstants::INVALID_INDEX;
		};
		DataChunk final_group_key_batch;
		Vector final_group_key_hashes(LogicalType::HASH);
		bool final_group_key_batch_initialized = false;
		bool final_split_payload_descriptor_initialized = false;
		bool final_split_payload_descriptor_ready = false;
		bool final_split_payload_uses_fused_update = false;
		string final_split_payload_descriptor_blocker;
		bool final_row_pointer_group_descriptor_initialized = false;
		bool final_row_pointer_group_descriptor_ready = false;
		string final_row_pointer_group_descriptor_blocker;
		vector<LogicalType> final_group_key_types;
		vector<idx_t> final_group_projection_indices;
		vector<idx_t> final_payload_source_indices;
		vector<ExecutionRowPointerGroupKeySource> final_row_pointer_group_sources;
		vector<uint8_t> between_join_compressed_key_skip_projection;
		vector<uint8_t> second_join_compressed_key_skip_projection;
		vector<uint8_t> between_join_precomputed_payload_skip_projection;
		vector<uint8_t> second_join_precomputed_payload_skip_projection;
		bool compressed_group_key_skip_initialized = false;
		bool compressed_group_key_skip_ready = false;
		bool second_join_batch_omits_compressed_group_keys = false;
		bool precomputed_payload_skip_initialized = false;
		bool precomputed_payload_skip_ready = false;
		bool second_join_batch_omits_precomputed_payloads = false;

		auto initialize_between_join_compressed_passthroughs = [&]() {
			if (second_join_compressed_passthrough_initialized) {
				return;
			}
			second_join_compressed_passthrough_initialized = true;
			compressed_passthrough_by_between_projection.assign(between_join_projection_op.projections.size(),
			                                                    DConstants::INVALID_INDEX);
			vector<LogicalType> passthrough_types;
			for (idx_t projection_idx = 0; projection_idx < between_join_projection_op.projections.size();
			     projection_idx++) {
				auto &plan = between_join_projection_op.projections[projection_idx].plan;
				if (plan.kind != SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
				    plan.return_type.id() != LogicalTypeId::VARCHAR ||
				    plan.source_index >= first_hash_join_op.output_types.size()) {
					continue;
				}
				auto &compressed_type = first_hash_join_op.output_types[plan.source_index];
				if (!DirectAppendSupportsFixedSizeType(compressed_type)) {
					continue;
				}
				BetweenJoinCompressedPassthrough passthrough;
				passthrough.between_projection_idx = projection_idx;
				passthrough.first_join_output_source_idx = plan.source_index;
				passthrough.sidecar_idx = passthrough_types.size();
				compressed_passthrough_by_between_projection[projection_idx] = passthrough.sidecar_idx;
				passthrough_types.push_back(compressed_type);
				between_join_compressed_passthroughs.push_back(passthrough);
			}
			if (!passthrough_types.empty()) {
				second_join_compressed_passthrough_batch.Initialize(runtime.GetAllocator(), passthrough_types);
			}
		};

		auto reset_between_join_compressed_passthroughs = [&]() {
			if (second_join_compressed_passthrough_initialized &&
			    second_join_compressed_passthrough_batch.ColumnCount() > 0) {
				second_join_compressed_passthrough_batch.Reset();
			}
			second_join_compressed_passthrough_usable = true;
			second_join_batch_omits_compressed_group_keys = false;
		};

		auto reset_between_join_precomputed_payloads = [&]() {
			if (second_join_precomputed_payload_initialized &&
			    second_join_precomputed_payload_batch.ColumnCount() > 0) {
				second_join_precomputed_payload_batch.Reset();
			}
			second_join_precomputed_payload_usable = true;
			second_join_batch_omits_precomputed_payloads = false;
		};

		auto append_between_join_compressed_passthroughs =
		    [&](DataChunk &first_join_input, const SelectionVector &match_selection, Vector &row_pointers,
		        idx_t target_offset, idx_t count) -> bool {
			initialize_between_join_compressed_passthroughs();
			if (between_join_compressed_passthroughs.empty()) {
				return true;
			}
			auto record_passthrough_blocker = [&](const char *reason) {
				auto path = string("direct_between_join_compressed_passthrough_unsupported.") + reason;
				RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), count);
			};
			if (!scratch.HasOperatorBinding(0)) {
				record_passthrough_blocker("first_join_binding");
				return false;
			}
			if (second_join_compressed_passthrough_batch.size() != target_offset) {
				record_passthrough_blocker("target_offset");
				return false;
			}
			if (target_offset + count > STANDARD_VECTOR_SIZE) {
				record_passthrough_blocker("capacity");
				return false;
			}
			auto &binding = scratch.OperatorBinding(0).hash_join_probe;
			auto stage_start = SljitRegionStageStart(runtime);
			for (auto &passthrough : between_join_compressed_passthroughs) {
				if (passthrough.sidecar_idx >= second_join_compressed_passthrough_batch.ColumnCount()) {
					record_passthrough_blocker("sidecar_index");
					return false;
				}
				auto &target = second_join_compressed_passthrough_batch.data[passthrough.sidecar_idx];
				if (!TryMaterializeHashJoinOutputReferenceToBatch(
				        binding, first_join_input, match_selection, row_pointers,
				        passthrough.first_join_output_source_idx, target, target_offset, count)) {
					record_passthrough_blocker("source");
					return false;
				}
			}
			FinishDirectProjectionBatchTargets(second_join_compressed_passthrough_batch, target_offset + count, false);
			RecordSljitRegionStageRuntime(runtime, 1, between_join_projection_op.kind,
			                              "between_join_compressed_passthrough_projection", stage_start);
			RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
			                             "direct_between_join_compressed_passthrough_projection", count);
			return true;
		};

		auto append_between_join_precomputed_payloads =
		    [&](DataChunk &first_join_input, const SelectionVector &match_selection, Vector &row_pointers,
		        idx_t target_offset, idx_t count) -> bool {
			if (!second_join_precomputed_payload_usable || between_join_precomputed_payloads.empty()) {
				return true;
			}
			auto record_payload_blocker = [&](const char *reason) {
				auto path = string("direct_between_join_precomputed_payload_unsupported.") + reason;
				RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), count);
			};
			if (!scratch.HasOperatorBinding(0)) {
				record_payload_blocker("first_join_binding");
				return false;
			}
			if (!second_join_precomputed_payload_initialized ||
			    second_join_precomputed_payload_batch.size() != target_offset) {
				record_payload_blocker("target_offset");
				return false;
			}
			if (target_offset + count > STANDARD_VECTOR_SIZE) {
				record_payload_blocker("capacity");
				return false;
			}
			auto &binding = scratch.OperatorBinding(0).hash_join_probe;
			auto stage_start = SljitRegionStageStart(runtime);
			bool used_direct_decimal64_payload = false;
			for (auto &payload : between_join_precomputed_payloads) {
				if (payload.sidecar_idx >= second_join_precomputed_payload_batch.ColumnCount()) {
					record_payload_blocker("sidecar_index");
					return false;
				}
				string decimal64_payload_miss;
				if (TryMaterializeHashJoinAllValidDecimal64ExpressionToBatch(
				        binding, payload.first_join_expr, first_join_input, match_selection, row_pointers,
				        second_join_precomputed_payload_batch, payload.sidecar_idx, target_offset, count,
				        payload.decimal64_discounted_amount_program, &decimal64_payload_miss)) {
					used_direct_decimal64_payload = true;
					continue;
				}
				if (!decimal64_payload_miss.empty()) {
					auto path = string("direct_between_join_precomputed_payload_decimal64_unsupported.") +
					            decimal64_payload_miss;
					RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), count);
				}
				auto &adapter_scratch = scratch.ExpressionAdapterScratch(3, payload.second_projection_idx);
				if (!TryMaterializeHashJoinMixedProjectionToBatch(
				        binding, payload.first_join_expr, first_join_input, match_selection, row_pointers,
				        second_join_precomputed_payload_batch, payload.sidecar_idx, target_offset, count,
				        adapter_scratch)) {
					record_payload_blocker("payload_projection");
					return false;
				}
			}
			FinishDirectProjectionBatchTargets(second_join_precomputed_payload_batch, target_offset + count, false);
			RecordSljitRegionStageRuntime(runtime, 1, between_join_projection_op.kind,
			                              "between_join_precomputed_payload_projection", stage_start);
			RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
			                             "direct_between_join_precomputed_payload_projection", count);
			if (used_direct_decimal64_payload) {
				RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
				                             "direct_between_join_precomputed_payload_decimal64_projection", count);
			}
			return true;
		};

		auto copy_second_join_precomputed_payloads_to_projection = [&](DataChunk &direct_projection,
		                                                               idx_t count) -> bool {
			if (!second_join_batch_omits_precomputed_payloads) {
				return true;
			}
			auto record_payload_blocker = [&](const char *reason) {
				auto path = string("direct_second_join_precomputed_payload_unsupported.") + reason;
				RecordSljitRegionRuntimePath(runtime, ops[3].kind, path.c_str(), count);
			};
			if (!second_join_precomputed_payload_usable || !second_join_precomputed_payload_initialized ||
			    second_join_precomputed_payload_batch.size() != second_join_batch.size()) {
				record_payload_blocker("sidecar_batch");
				return false;
			}
			auto stage_start = SljitRegionStageStart(runtime);
			for (auto &payload : between_join_precomputed_payloads) {
				if (payload.sidecar_idx >= second_join_precomputed_payload_batch.ColumnCount() ||
				    payload.second_projection_idx >= direct_projection.ColumnCount()) {
					record_payload_blocker("sidecar_index");
					return false;
				}
				DirectProjectionBatchPassthrough passthrough;
				passthrough.output_idx = payload.second_projection_idx;
				passthrough.source = &second_join_precomputed_payload_batch.data[payload.sidecar_idx];
				passthrough.selection = &scratch.FilterSelection(2);
				passthrough.trace_phase = "direct_batch_expression.precomputed_payload";
				if (!TryCopyDirectProjectionPassthroughToBatch(
				        passthrough, direct_projection.data[payload.second_projection_idx], 0, count)) {
					record_payload_blocker("copy");
					return false;
				}
			}
			RecordSljitRegionStageRuntime(runtime, 3, ops[3].kind, "second_join_precomputed_payload_passthrough",
			                              stage_start);
			RecordSljitRegionRuntimePath(runtime, ops[3].kind, "direct_second_join_precomputed_payload_passthrough",
			                             count);
			return true;
		};

		auto collect_final_group_compressed_passthrough_sources =
		    [&](const ExecutionHashJoinProbeBinding &second_binding,
		        vector<FinalGroupCompressedPassthroughSource> &sources) -> bool {
			sources.clear();
			if (!second_join_compressed_passthrough_usable || !second_join_compressed_passthrough_initialized ||
			    second_join_compressed_passthrough_batch.ColumnCount() == 0 || final_group_projection_indices.empty() ||
			    !second_binding.ready ||
			    second_binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
			    second_binding.lhs_output_column_indices.empty() || ops.size() <= 3) {
				return false;
			}
			auto &second_join_projection_op = ops[3];
			if (second_join_projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
				return false;
			}
			for (idx_t output_idx = 0; output_idx < final_group_projection_indices.size(); output_idx++) {
				const auto projection_idx = final_group_projection_indices[output_idx];
				if (projection_idx >= final_projection_op.projections.size()) {
					return false;
				}
				auto &final_plan = final_projection_op.projections[projection_idx].plan;
				if (final_plan.kind != SljitNativeRegionExpressionKind::STRING_COMPRESS ||
				    final_plan.source_index >= second_join_projection_op.projections.size()) {
					continue;
				}
				SljitExecutableRegionExpression remapped_expr;
				idx_t second_join_output_source_idx;
				if (!TryBuildSingleSourceProjectionExpression(
				        second_join_projection_op.projections[final_plan.source_index], remapped_expr,
				        second_join_output_source_idx) ||
				    !ProjectionIsSingleSourceReferenceLike(remapped_expr.plan) ||
				    second_join_output_source_idx >= second_binding.lhs_output_column_indices.size()) {
					continue;
				}
				const auto second_join_input_col =
				    second_binding.lhs_output_column_indices[second_join_output_source_idx];
				if (second_join_input_col >= compressed_passthrough_by_between_projection.size()) {
					continue;
				}
				const auto sidecar_idx = compressed_passthrough_by_between_projection[second_join_input_col];
				if (sidecar_idx == DConstants::INVALID_INDEX ||
				    sidecar_idx >= second_join_compressed_passthrough_batch.ColumnCount()) {
					continue;
				}
				auto &source = second_join_compressed_passthrough_batch.data[sidecar_idx];
				if (source.GetType() != final_projection_op.output_types[projection_idx] ||
				    source.GetType() != final_plan.return_type) {
					continue;
				}
				FinalGroupCompressedPassthroughSource passthrough;
				passthrough.final_group_output_idx = output_idx;
				passthrough.final_projection_idx = projection_idx;
				passthrough.second_join_projection_idx = final_plan.source_index;
				passthrough.second_join_input_col = second_join_input_col;
				passthrough.sidecar_idx = sidecar_idx;
				sources.push_back(passthrough);
			}
			return !sources.empty();
		};

		auto second_join_projection_column_is_omitted = [&](idx_t projection_idx) -> bool {
			if (second_join_batch_omits_compressed_group_keys &&
			    projection_idx < second_join_compressed_key_skip_projection.size() &&
			    second_join_compressed_key_skip_projection[projection_idx]) {
				return true;
			}
			return second_join_batch_omits_precomputed_payloads &&
			       projection_idx < second_join_precomputed_payload_skip_projection.size() &&
			       second_join_precomputed_payload_skip_projection[projection_idx];
		};

		auto finish_deferred_grouped_update = [&]() {
			if (!deferred_grouped_finish) {
				return;
			}
			auto &binding = scratch.SinkBinding(aggregate_idx);
			if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
			    !binding.aggregate_update.grouped_state.state) {
				throw InternalException("SLJIT deferred grouped aggregate finish is missing aggregate state");
			}
			auto finish_stage_start = SljitRegionStageStart(runtime);
			binding.aggregate_update.grouped_state.state->FinishStateUpdates();
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "finish_deferred_grouped_state_updates", finish_stage_start);
			deferred_grouped_finish = false;
		};

		auto execute_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto sink_result =
			    ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, aggregate_idx, aggregate_op, projected,
			                                 nullptr, DConstants::INVALID_INDEX, true, &deferred_grouped_finish);
			if (sink_result == SinkResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			native_runtime.RecordSinkResult(projected.size(), sink_result);
			if (sink_result == SinkResultType::FINISHED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			processed_batches++;
			return false;
		};

		auto flush_projected_batch = [&]() -> bool {
			auto batch = runtime.PendingSourceContractBatch();
			if (!batch) {
				return false;
			}
			if (execute_projected_batch(*batch)) {
				return true;
			}
			runtime.ResetSourceContractBatch();
			return false;
		};

		auto record_final_split_payload_unsupported = [&](const string &reason, idx_t count) {
			auto path = string("direct_projected_group_payload_update_unsupported.") + reason;
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, path.c_str(), count);
		};

		auto build_final_split_payload_descriptor = [&](DataChunk &payload_input) -> bool {
			if (final_split_payload_descriptor_initialized) {
				return final_split_payload_descriptor_ready;
			}
			final_split_payload_descriptor_initialized = true;
			auto set_blocker = [&](const char *blocker) {
				final_split_payload_descriptor_blocker = blocker;
				final_split_payload_descriptor_ready = false;
				final_split_payload_uses_fused_update = false;
				return false;
			};
			if (final_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
			    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
				return set_blocker("operator_kind");
			}
			auto &aggregate_update = aggregate_op.aggregate_update;
			auto &aggregate_plan = aggregate_update.plan;
			auto &sink_info = aggregate_plan.sink_info;
			if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty() ||
			    sink_info.aggregates.empty() || !aggregate_plan.use_primitive_payloads ||
			    !aggregate_plan.use_grouped_state_addresses || aggregate_plan.use_perfect_hash_group_lookup ||
			    aggregate_update.fused_payload_update_owns_group_lookup) {
				return set_blocker("aggregate_shape");
			}
			if (aggregate_update.payloads.size() != sink_info.aggregates.size()) {
				return set_blocker("payload_count");
			}
			final_group_key_types.clear();
			final_group_projection_indices.clear();
			final_group_key_types.reserve(sink_info.groups.size());
			final_group_projection_indices.reserve(sink_info.groups.size());
			for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
				auto &group = sink_info.groups[group_idx];
				if (!group.supported_reference || group.input_index != group_idx ||
				    group.input_index >= final_projection_op.projections.size() ||
				    group.input_index >= final_projection_op.output_types.size()) {
					return set_blocker("group_key_not_dense");
				}
				auto &group_projection = final_projection_op.projections[group.input_index].plan;
				if (group_projection.return_type.InternalType() != group.type.InternalType() ||
				    final_projection_op.output_types[group.input_index].InternalType() != group.type.InternalType()) {
					return set_blocker("group_key_type");
				}
				final_group_projection_indices.push_back(group.input_index);
				final_group_key_types.push_back(final_projection_op.output_types[group.input_index]);
			}
			final_payload_source_indices.clear();
			vector<idx_t> fused_payload_sources;
			if (aggregate_update.fused_payload_update_function &&
			    TryGetFusedTypedPayloadCombinedSources(aggregate_update.payloads, sink_info.aggregates,
			                                           fused_payload_sources)) {
				final_payload_source_indices.reserve(fused_payload_sources.size());
				for (auto projection_source_idx : fused_payload_sources) {
					if (projection_source_idx >= final_projection_op.projections.size() ||
					    projection_source_idx >= final_projection_op.output_types.size()) {
						return set_blocker("fused_payload_source");
					}
					SljitExecutableRegionExpression remapped_expr;
					idx_t input_source_idx;
					if (!TryBuildSingleSourceProjectionExpression(
					        final_projection_op.projections[projection_source_idx], remapped_expr, input_source_idx) ||
					    !ProjectionIsSingleSourceReferenceLike(remapped_expr.plan)) {
						return set_blocker("fused_payload_projection");
					}
					if (input_source_idx >= payload_input.ColumnCount() ||
					    final_projection_op.output_types[projection_source_idx] !=
					        payload_input.data[input_source_idx].GetType()) {
						return set_blocker("fused_payload_type");
					}
					final_payload_source_indices.push_back(input_source_idx);
				}
				final_split_payload_uses_fused_update = true;
				final_split_payload_descriptor_ready = true;
				return true;
			}
			final_payload_source_indices.reserve(sink_info.aggregates.size());
			final_split_payload_uses_fused_update = false;
			for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
				auto &aggregate = sink_info.aggregates[payload_idx];
				auto &payload = aggregate_update.payloads[payload_idx].plan;
				if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					if (aggregate.child_count != 0) {
						return set_blocker("count_star_payload");
					}
					final_payload_source_indices.push_back(DConstants::INVALID_INDEX);
					continue;
				}
				if (aggregate.child_count != 1 || aggregate.child_types.size() != 1 ||
				    aggregate.payload_index >= final_projection_op.projections.size()) {
					return set_blocker("payload_contract");
				}
				if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
				    payload.source_index != aggregate.payload_index) {
					return set_blocker("payload_not_final_reference");
				}
				auto &payload_projection = final_projection_op.projections[aggregate.payload_index].plan;
				if (payload_projection.kind != SljitNativeRegionExpressionKind::REFERENCE ||
				    payload_projection.source_index >= payload_input.ColumnCount()) {
					return set_blocker("payload_source");
				}
				if (payload_projection.return_type.InternalType() != aggregate.child_types[0].InternalType() ||
				    payload_input.data[payload_projection.source_index].GetType().InternalType() !=
				        aggregate.child_types[0].InternalType()) {
					return set_blocker("payload_type");
				}
				final_payload_source_indices.push_back(payload_projection.source_index);
			}
			final_split_payload_descriptor_ready = true;
			return true;
		};

		auto build_final_row_pointer_group_descriptor = [&](DataChunk &payload_input) -> bool {
			if (final_row_pointer_group_descriptor_initialized) {
				return final_row_pointer_group_descriptor_ready;
			}
			final_row_pointer_group_descriptor_initialized = true;
			final_row_pointer_group_descriptor_ready = false;
			final_row_pointer_group_sources.clear();
			auto set_blocker = [&](const char *blocker) {
				final_row_pointer_group_descriptor_blocker = blocker;
				final_row_pointer_group_sources.clear();
				return false;
			};
			if (!build_final_split_payload_descriptor(payload_input)) {
				return set_blocker(final_split_payload_descriptor_blocker.empty()
				                       ? "payload_descriptor"
				                       : final_split_payload_descriptor_blocker.c_str());
			}
			auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
			if (sink_info.groups.size() != final_group_projection_indices.size() ||
			    final_payload_source_indices.empty()) {
				return set_blocker("aggregate_shape");
			}
			final_row_pointer_group_sources.reserve(sink_info.groups.size());
			for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
				auto &group = sink_info.groups[group_idx];
				const auto projection_idx = final_group_projection_indices[group_idx];
				if (projection_idx >= final_projection_op.projections.size() ||
				    projection_idx >= final_projection_op.output_types.size()) {
					return set_blocker("group_key_index");
				}
				SljitExecutableRegionExpression remapped_expr;
				idx_t input_source_idx;
				if (!TryBuildSingleSourceProjectionExpression(final_projection_op.projections[projection_idx],
				                                              remapped_expr, input_source_idx)) {
					return set_blocker("group_key_projection");
				}
				if (input_source_idx >= payload_input.ColumnCount()) {
					return set_blocker("group_key_source");
				}
				if (second_join_projection_column_is_omitted(input_source_idx)) {
					return set_blocker("group_key_omitted_input");
				}
				auto &input_type = payload_input.data[input_source_idx].GetType();
				ExecutionRowPointerGroupKeySource group_source;
				InitializeInputVectorGroupKeySource(input_source_idx, input_type.InternalType(), group.type,
				                                    group_source);
				if (ProjectionIsSingleSourceReferenceLike(remapped_expr.plan) &&
				    input_type.InternalType() == group.type.InternalType() &&
				    remapped_expr.plan.return_type.InternalType() == group.type.InternalType()) {
					group_source.ready = true;
					group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
				} else if (!TryMapGroupKeyCast(remapped_expr.plan, group.type, group_source)) {
					return set_blocker("group_key_cast");
				}
				final_row_pointer_group_sources.push_back(std::move(group_source));
			}
			final_row_pointer_group_descriptor_ready = !final_row_pointer_group_sources.empty();
			return final_row_pointer_group_descriptor_ready;
		};

		auto build_final_group_key_passthroughs = [&](vector<DirectProjectionBatchPassthrough> &passthroughs) -> bool {
			passthroughs.clear();
			if (second_join_compressed_passthrough_batch.size() != second_join_batch.size() ||
			    !scratch.HasOperatorBinding(2)) {
				return false;
			}
			auto &second_binding = scratch.OperatorBinding(2).hash_join_probe;
			vector<FinalGroupCompressedPassthroughSource> sources;
			if (!collect_final_group_compressed_passthrough_sources(second_binding, sources)) {
				return false;
			}
			for (auto &source_info : sources) {
				auto &source = second_join_compressed_passthrough_batch.data[source_info.sidecar_idx];
				DirectProjectionBatchPassthrough passthrough;
				passthrough.output_idx = source_info.final_group_output_idx;
				passthrough.source = &source;
				passthrough.selection = &scratch.FilterSelection(2);
				passthrough.trace_phase = "direct_batch_expression.compressed_passthrough";
				passthroughs.push_back(passthrough);
			}
			return !passthroughs.empty();
		};

		auto second_join_input_col_is_probe_key = [&](const ExecutionHashJoinProbeBinding &second_binding,
		                                              idx_t input_col) -> bool {
			for (auto key_input_idx : second_binding.probe_key_input_indices) {
				if (key_input_idx == input_col) {
					return true;
				}
			}
			for (auto &key : second_hash_join_op.hash_join_probe.plan.keys) {
				if (key.key_input_index == input_col) {
					return true;
				}
			}
			return false;
		};

		auto final_payload_uses_second_join_input_col = [&](const ExecutionHashJoinProbeBinding &second_binding,
		                                                    idx_t input_col) -> bool {
			if (ops.size() <= 3) {
				return true;
			}
			auto &second_join_projection_op = ops[3];
			if (second_join_projection_op.kind != SljitNativeRegionOpKind::PROJECTION) {
				return true;
			}
			for (auto payload_source_idx : final_payload_source_indices) {
				if (payload_source_idx == DConstants::INVALID_INDEX) {
					continue;
				}
				if (payload_source_idx >= second_join_projection_op.projections.size()) {
					return true;
				}
				vector<uint8_t> referenced_sources;
				if (!TryCollectHashJoinProjectionExpressionSources(
				        second_join_projection_op.projections[payload_source_idx], second_binding.output_types.size(),
				        referenced_sources)) {
					return true;
				}
				for (idx_t source_idx = 0; source_idx < referenced_sources.size(); source_idx++) {
					if (!referenced_sources[source_idx] ||
					    source_idx >= second_binding.lhs_output_column_indices.size()) {
						continue;
					}
					if (second_binding.lhs_output_column_indices[source_idx] == input_col) {
						return true;
					}
				}
			}
			return false;
		};

		auto second_join_input_col_used_by_live_projection =
		    [&](const ExecutionHashJoinProbeBinding &second_binding, idx_t input_col,
		        const vector<uint8_t> &second_projection_skip) -> bool {
			if (ops.size() <= 3) {
				return true;
			}
			auto &second_join_projection_op = ops[3];
			if (second_join_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
			    second_projection_skip.size() != second_join_projection_op.projections.size()) {
				return true;
			}
			for (idx_t projection_idx = 0; projection_idx < second_join_projection_op.projections.size();
			     projection_idx++) {
				if (second_projection_skip[projection_idx]) {
					continue;
				}
				vector<uint8_t> referenced_sources;
				if (!TryCollectHashJoinProjectionExpressionSources(
				        second_join_projection_op.projections[projection_idx], second_binding.output_types.size(),
				        referenced_sources)) {
					return true;
				}
				for (idx_t source_idx = 0; source_idx < referenced_sources.size(); source_idx++) {
					if (!referenced_sources[source_idx] ||
					    source_idx >= second_binding.lhs_output_column_indices.size()) {
						continue;
					}
					if (second_binding.lhs_output_column_indices[source_idx] == input_col) {
						return true;
					}
				}
			}
			return false;
		};

		auto build_precomputed_payload_projection_skips = [&]() -> bool {
			if (precomputed_payload_skip_initialized) {
				return precomputed_payload_skip_ready;
			}
			auto record_skip_blocker = [&](const char *reason) {
				auto path = string("direct_between_join_precomputed_payload_skip_unsupported.") + reason;
				RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), 1);
			};
			auto add_unique = [&](vector<idx_t> &values, idx_t value) {
				for (auto existing : values) {
					if (existing == value) {
						return;
					}
				}
				values.push_back(value);
			};
			precomputed_payload_skip_initialized = true;
			precomputed_payload_skip_ready = false;
			between_join_precomputed_payloads.clear();
			between_join_precomputed_payload_skip_projection.assign(between_join_projection_op.projections.size(), 0);
			if (ops.size() <= 3) {
				record_skip_blocker("operator_count");
				return false;
			}
			auto &second_join_projection_op = ops[3];
			second_join_precomputed_payload_skip_projection.assign(second_join_projection_op.projections.size(), 0);
			if (second_hash_join_op.hash_join_probe.plan.residual_predicate) {
				record_skip_blocker("second_join_residual");
				return false;
			}

			DataChunk descriptor_input;
			descriptor_input.InitializeEmpty(second_join_projection_op.output_types);
			if (!build_final_split_payload_descriptor(descriptor_input)) {
				record_skip_blocker("final_payload_descriptor");
				return false;
			}
			if (final_payload_source_indices.empty()) {
				record_skip_blocker("no_payload_sources");
				return false;
			}

			if (!scratch.HasOperatorBinding(2)) {
				ExecutionOperatorBinding *binding_ptr = nullptr;
				string deferred_reason;
				auto bind_result = BindNativeOperator(
				    native_runtime, scratch, 2, second_hash_join_op, second_join_batch,
				    second_hash_join_op.hash_join_probe.plan.operator_info, "native-operator-runtime-deferred",
				    "SLJIT native hash join probe", binding_ptr, deferred_reason);
				if (bind_result != ExecutionOperatorBindResult::READY) {
					record_skip_blocker("second_join_bind");
					return false;
				}
			}
			auto &second_binding = scratch.OperatorBinding(2).hash_join_probe;
			if (!second_binding.ready ||
			    second_binding.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
			    second_binding.lhs_output_column_indices.empty()) {
				record_skip_blocker("second_join_binding");
				return false;
			}

			vector<BetweenJoinPrecomputedPayload> candidates;
			vector<uint8_t> candidate_seen(second_join_projection_op.projections.size(), 0);
			for (auto payload_source_idx : final_payload_source_indices) {
				if (payload_source_idx == DConstants::INVALID_INDEX) {
					continue;
				}
				if (payload_source_idx >= second_join_projection_op.projections.size() ||
				    payload_source_idx >= second_join_projection_op.output_types.size()) {
					record_skip_blocker("payload_source_index");
					continue;
				}
				if (candidate_seen[payload_source_idx]) {
					continue;
				}
				candidate_seen[payload_source_idx] = 1;
				auto &payload_projection = second_join_projection_op.projections[payload_source_idx];
				auto &payload_type = second_join_projection_op.output_types[payload_source_idx];
				if (payload_projection.plan.return_type != payload_type ||
				    !DirectProjectionBatchSupportsType(payload_type)) {
					record_skip_blocker("payload_type");
					continue;
				}
				vector<uint8_t> referenced_sources;
				if (!TryCollectHashJoinProjectionExpressionSources(
				        payload_projection, second_binding.output_types.size(), referenced_sources)) {
					record_skip_blocker("payload_sources");
					continue;
				}
				vector<idx_t> source_map(second_binding.output_types.size(), DConstants::INVALID_INDEX);
				vector<idx_t> between_projection_indices;
				bool supported = false;
				for (idx_t source_idx = 0; source_idx < referenced_sources.size(); source_idx++) {
					if (!referenced_sources[source_idx]) {
						continue;
					}
					supported = true;
					if (source_idx >= second_binding.lhs_output_column_indices.size()) {
						supported = false;
						record_skip_blocker("rhs_dependency");
						break;
					}
					const auto between_projection_idx = second_binding.lhs_output_column_indices[source_idx];
					if (between_projection_idx >= between_join_projection_op.projections.size() ||
					    between_projection_idx >= between_join_projection_op.output_types.size() ||
					    between_projection_idx >= second_join_batch.ColumnCount() ||
					    second_binding.output_types[source_idx] !=
					        between_join_projection_op.output_types[between_projection_idx]) {
						supported = false;
						record_skip_blocker("between_projection_index");
						break;
					}
					SljitExecutableRegionExpression remapped_between_expr;
					idx_t first_join_output_source_idx;
					if (!TryBuildSingleSourceProjectionExpression(
					        between_join_projection_op.projections[between_projection_idx], remapped_between_expr,
					        first_join_output_source_idx) ||
					    !ProjectionIsSingleSourceReferenceLike(remapped_between_expr.plan) ||
					    first_join_output_source_idx >= first_hash_join_op.output_types.size() ||
					    first_hash_join_op.output_types[first_join_output_source_idx] !=
					        between_join_projection_op.output_types[between_projection_idx]) {
						supported = false;
						record_skip_blocker("between_projection_source");
						break;
					}
					source_map[source_idx] = first_join_output_source_idx;
					add_unique(between_projection_indices, between_projection_idx);
				}
				if (!supported || between_projection_indices.empty()) {
					continue;
				}
				BetweenJoinPrecomputedPayload candidate;
				candidate.second_projection_idx = payload_source_idx;
				candidate.between_projection_indices = std::move(between_projection_indices);
				BuildBorrowedProjectionExpression(payload_projection, candidate.first_join_expr);
				if (!TryRemapHashJoinProjectionPlanSources(source_map, candidate.first_join_expr.plan)) {
					record_skip_blocker("payload_remap_plan");
					continue;
				}
				bool remapped_sources = true;
				for (auto &input_source_idx : candidate.first_join_expr.input_source_indices) {
					if (input_source_idx >= source_map.size() ||
					    source_map[input_source_idx] == DConstants::INVALID_INDEX) {
						remapped_sources = false;
						break;
					}
					input_source_idx = source_map[input_source_idx];
				}
				if (!remapped_sources || candidate.first_join_expr.plan.return_type != payload_type) {
					record_skip_blocker("payload_remap_sources");
					continue;
				}
				if (candidate.first_join_expr.plan.expression_tree) {
					TryBuildSljitRuntimeDecimal64DiscountedAmountProgram(
					    *candidate.first_join_expr.plan.expression_tree,
					    candidate.first_join_expr.input_source_indices.size(),
					    candidate.decimal64_discounted_amount_program);
				}
				if (!candidate.decimal64_discounted_amount_program.ready) {
					record_skip_blocker("payload_program");
					continue;
				}
				candidates.push_back(std::move(candidate));
			}
			if (candidates.empty()) {
				record_skip_blocker("no_candidate");
				return false;
			}

			vector<uint8_t> selected_candidate(candidates.size(), 1);
			bool changed = true;
			while (changed) {
				changed = false;
				vector<uint8_t> live_second_projection_skip(second_join_projection_op.projections.size(), 0);
				if (compressed_group_key_skip_ready &&
				    second_join_compressed_key_skip_projection.size() == live_second_projection_skip.size()) {
					for (idx_t projection_idx = 0; projection_idx < live_second_projection_skip.size();
					     projection_idx++) {
						if (second_join_compressed_key_skip_projection[projection_idx]) {
							live_second_projection_skip[projection_idx] = 1;
						}
					}
				}
				for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
					if (selected_candidate[candidate_idx]) {
						live_second_projection_skip[candidates[candidate_idx].second_projection_idx] = 1;
					}
				}
				for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
					if (!selected_candidate[candidate_idx]) {
						continue;
					}
					bool skips_input = false;
					for (auto between_projection_idx : candidates[candidate_idx].between_projection_indices) {
						if (between_projection_idx >= between_join_precomputed_payload_skip_projection.size() ||
						    second_join_input_col_is_probe_key(second_binding, between_projection_idx) ||
						    second_join_input_col_used_by_live_projection(second_binding, between_projection_idx,
						                                                  live_second_projection_skip)) {
							continue;
						}
						skips_input = true;
						break;
					}
					if (!skips_input) {
						selected_candidate[candidate_idx] = 0;
						changed = true;
					}
				}
			}

			vector<LogicalType> payload_types;
			for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
				if (!selected_candidate[candidate_idx]) {
					continue;
				}
				auto &candidate = candidates[candidate_idx];
				vector<uint8_t> live_second_projection_skip(second_join_projection_op.projections.size(), 0);
				if (compressed_group_key_skip_ready &&
				    second_join_compressed_key_skip_projection.size() == live_second_projection_skip.size()) {
					for (idx_t projection_idx = 0; projection_idx < live_second_projection_skip.size();
					     projection_idx++) {
						if (second_join_compressed_key_skip_projection[projection_idx]) {
							live_second_projection_skip[projection_idx] = 1;
						}
					}
				}
				for (idx_t other_idx = 0; other_idx < candidates.size(); other_idx++) {
					if (selected_candidate[other_idx]) {
						live_second_projection_skip[candidates[other_idx].second_projection_idx] = 1;
					}
				}
				bool skipped_between_input = false;
				for (auto between_projection_idx : candidate.between_projection_indices) {
					if (between_projection_idx >= between_join_precomputed_payload_skip_projection.size() ||
					    second_join_input_col_is_probe_key(second_binding, between_projection_idx) ||
					    second_join_input_col_used_by_live_projection(second_binding, between_projection_idx,
					                                                  live_second_projection_skip)) {
						continue;
					}
					between_join_precomputed_payload_skip_projection[between_projection_idx] = 1;
					skipped_between_input = true;
				}
				if (!skipped_between_input) {
					continue;
				}
				candidate.sidecar_idx = payload_types.size();
				second_join_precomputed_payload_skip_projection[candidate.second_projection_idx] = 1;
				payload_types.push_back(second_join_projection_op.output_types[candidate.second_projection_idx]);
				between_join_precomputed_payloads.push_back(std::move(candidate));
			}
			if (between_join_precomputed_payloads.empty() ||
			    !ProjectionSkipHasAny(between_join_precomputed_payload_skip_projection)) {
				record_skip_blocker("no_dead_between_projection");
				between_join_precomputed_payloads.clear();
				return false;
			}
			second_join_precomputed_payload_batch.Initialize(runtime.GetAllocator(), payload_types);
			second_join_precomputed_payload_initialized = true;
			precomputed_payload_skip_ready = true;
			return true;
		};

		auto build_compressed_group_key_projection_skips = [&]() -> bool {
			if (compressed_group_key_skip_initialized) {
				return compressed_group_key_skip_ready;
			}
			auto record_skip_blocker = [&](const char *reason) {
				auto path = string("direct_between_join_compressed_group_key_skip_unsupported.") + reason;
				RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, path.c_str(), 1);
			};
			compressed_group_key_skip_initialized = true;
			compressed_group_key_skip_ready = false;
			between_join_compressed_key_skip_projection.assign(between_join_projection_op.projections.size(), 0);
			if (ops.size() <= 3) {
				record_skip_blocker("operator_count");
				return false;
			}
			second_join_compressed_key_skip_projection.assign(ops[3].projections.size(), 0);
			if (second_hash_join_op.hash_join_probe.plan.residual_predicate) {
				record_skip_blocker("second_join_residual");
				return false;
			}
			initialize_between_join_compressed_passthroughs();
			if (between_join_compressed_passthroughs.empty()) {
				record_skip_blocker("no_compressed_passthrough_source");
				return false;
			}

			DataChunk descriptor_input;
			descriptor_input.InitializeEmpty(ops[3].output_types);
			if (!build_final_split_payload_descriptor(descriptor_input)) {
				record_skip_blocker("final_payload_descriptor");
				return false;
			}

			if (!scratch.HasOperatorBinding(2)) {
				ExecutionOperatorBinding *binding_ptr = nullptr;
				string deferred_reason;
				auto bind_result = BindNativeOperator(
				    native_runtime, scratch, 2, second_hash_join_op, second_join_batch,
				    second_hash_join_op.hash_join_probe.plan.operator_info, "native-operator-runtime-deferred",
				    "SLJIT native hash join probe", binding_ptr, deferred_reason);
				if (bind_result != ExecutionOperatorBindResult::READY) {
					record_skip_blocker("second_join_bind");
					return false;
				}
			}
			auto &second_binding = scratch.OperatorBinding(2).hash_join_probe;
			vector<FinalGroupCompressedPassthroughSource> sources;
			if (!collect_final_group_compressed_passthrough_sources(second_binding, sources)) {
				record_skip_blocker("final_group_passthrough");
				return false;
			}
			bool skipped_any = false;
			for (auto &source_info : sources) {
				if (source_info.second_join_input_col >= between_join_compressed_key_skip_projection.size() ||
				    source_info.second_join_projection_idx >= second_join_compressed_key_skip_projection.size()) {
					record_skip_blocker("source_index");
					return false;
				}
				if (second_join_input_col_is_probe_key(second_binding, source_info.second_join_input_col)) {
					record_skip_blocker("probe_key");
					return false;
				}
				if (final_payload_uses_second_join_input_col(second_binding, source_info.second_join_input_col)) {
					record_skip_blocker("payload_dependency");
					return false;
				}
				between_join_compressed_key_skip_projection[source_info.second_join_input_col] = 1;
				second_join_compressed_key_skip_projection[source_info.second_join_projection_idx] = 1;
				skipped_any = true;
			}
			compressed_group_key_skip_ready = skipped_any;
			return compressed_group_key_skip_ready;
		};

		auto direct_update_final_projection_group_keys = [&](DataChunk &input, bool &handled) -> bool {
			handled = false;
			if (input.size() == 0) {
				return false;
			}
			if (!build_final_split_payload_descriptor(input)) {
				record_final_split_payload_unsupported(final_split_payload_descriptor_blocker, input.size());
				return false;
			}
			if (runtime.PendingSourceContractBatch() && flush_projected_batch()) {
				return true;
			}
			if (build_final_row_pointer_group_descriptor(input)) {
				if (TryExecuteNativeRowPointerGroupedAggregateUpdate(
				        runtime, native_runtime, scratch, aggregate_idx, aggregate_op, input,
				        scratch.HashJoinRowPointers(2), final_row_pointer_group_sources, final_payload_source_indices,
				        true, optional_ptr<bool>(&deferred_grouped_finish))) {
					processed_batches++;
					handled = true;
					return false;
				}
				record_final_split_payload_unsupported("row_pointer_grouped_lookup_update", input.size());
			} else if (!final_row_pointer_group_descriptor_blocker.empty()) {
				auto path = string("direct_final_row_pointer_grouped_lookup_update_unsupported.") +
				            final_row_pointer_group_descriptor_blocker;
				RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, path.c_str(), input.size());
			}
			if (!final_group_key_batch_initialized) {
				final_group_key_batch.Initialize(runtime.GetAllocator(), final_group_key_types);
				final_group_key_batch_initialized = true;
			}
			final_group_key_batch.Reset();
			vector<DirectProjectionBatchPassthrough> final_group_key_passthroughs;
			optional_ptr<const vector<DirectProjectionBatchPassthrough>> final_group_key_passthroughs_ptr;
			if (build_final_group_key_passthroughs(final_group_key_passthroughs)) {
				final_group_key_passthroughs_ptr = &final_group_key_passthroughs;
			}
			if (!TryMaterializeSelectedProjectionToBatch(runtime, scratch, final_projection_idx, final_projection_op,
			                                             input, final_group_key_batch, final_group_projection_indices,
			                                             optional_ptr<Vector>(&final_group_key_hashes),
			                                             final_group_key_passthroughs_ptr)) {
				record_final_split_payload_unsupported("group_key_projection", input.size());
				return false;
			}

			auto bind_stage_start = SljitRegionStageStart(runtime);
			bool bound = false;
			auto &binding =
			    BindNativeSink(native_runtime, scratch, aggregate_idx, final_group_key_batch,
			                   aggregate_op.aggregate_update.plan.sink_info, "aggregate-update-runtime-binding-failed",
			                   "SLJIT aggregate update sink", bound);
			if (bound) {
				RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind, "bind_sink_contract",
				                              bind_stage_start);
			}
			if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
			    !binding.aggregate_update.grouped_state.state || !binding.aggregate_update.primitive.ready) {
				record_final_split_payload_unsupported("sink_binding", input.size());
				return false;
			}
			auto &aggregates = aggregate_op.aggregate_update.plan.sink_info.aggregates;
			auto &payload_lanes =
			    scratch.AggregatePayloadLanes(aggregate_idx, aggregates, binding.aggregate_update.primitive);
			const bool finish = false;
			bool updated = false;
			if (final_split_payload_uses_fused_update) {
				if (!CanExecuteDirectGroupedFusedPayloadUpdate(scratch, aggregate_idx, aggregate_op, input,
				                                               payload_lanes, nullptr, input.size(), false)) {
					record_final_split_payload_unsupported("fused_payload_update_shape", input.size());
					return false;
				}
				auto &payload_scratch = scratch.AggregatePayloadScratch(aggregate_idx);
				updated = TryExecuteDirectProjectedGroupedFusedPayloadUpdate(
				    runtime, scratch, aggregate_idx, aggregate_op, final_group_key_batch, input,
				    final_payload_source_indices, payload_lanes, binding.aggregate_update.grouped_state,
				    payload_scratch, finish, optional_ptr<Vector>(&final_group_key_hashes));
			} else {
				auto stage_start = SljitRegionStageStart(runtime);
				if (runtime.TraceRuntime()) {
					auto stage_name =
					    SljitRegionStageName(aggregate_idx, aggregate_op.kind, "direct_projected_group_payload_update");
					SljitRegionStageRecorder recorder(runtime, stage_name);
					updated = binding.aggregate_update.grouped_state.state->TryUpdateNewGroupsWithPayloadInput(
					    final_group_key_batch, input, final_payload_source_indices,
					    aggregate_op.aggregate_update.plan.sink_info, payload_lanes, &recorder, finish,
					    optional_ptr<Vector>(&final_group_key_hashes));
					auto runtime_us = SljitRegionElapsedMicros(stage_start);
					auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
					if (unattributed_runtime_us > 0) {
						runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
					}
				} else {
					updated = binding.aggregate_update.grouped_state.state->TryUpdateNewGroupsWithPayloadInput(
					    final_group_key_batch, input, final_payload_source_indices,
					    aggregate_op.aggregate_update.plan.sink_info, payload_lanes, nullptr, finish,
					    optional_ptr<Vector>(&final_group_key_hashes));
				}
				if (updated) {
					RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
					                              "direct_projected_group_payload_update", stage_start);
					RecordSljitRegionMaterializationBoundary(runtime, aggregate_op.kind,
					                                         "projected_group_payload_update", input.size());
				} else {
					RecordSljitRegionStageRuntimePath(runtime, aggregate_idx, aggregate_op.kind,
					                                  "direct_projected_group_payload_update_miss", stage_start);
				}
			}
			if (!updated) {
				return false;
			}
			MarkDeferredGroupedFinish(true, optional_ptr<bool>(&deferred_grouped_finish));
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_group_payload_update",
			                             input.size());
			processed_batches++;
			handled = true;
			return false;
		};

		auto append_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + projected.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (projected.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
				return execute_projected_batch(projected);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(batch, projected)) {
				batch.Append(projected);
			}
			RecordSljitRegionStageRuntime(runtime, final_projection_idx, final_projection_op.kind,
			                              "post_join_batch_append", append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind, "copied_post_join_batch",
			                                         projected.size());
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto project_second_join_output_from = [&](idx_t start_projection_idx, DataChunk &input,
		                                           DataChunk *&projected) {
			DataChunk *current = &input;
			for (idx_t projection_idx = start_projection_idx; projection_idx < aggregate_idx; projection_idx++) {
				auto &projection_op = ops[projection_idx];
				auto &projection_output = scratch.TemporaryChunk(projection_idx);
				projection_output.Reset();
				auto projection_stage_start = SljitRegionStageStart(runtime);
				if (TryReferenceProjection(projection_output, *current, projection_op)) {
					RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
					                              "post_second_join_reference_projection", projection_stage_start);
					RecordSljitRegionMaterializationBoundary(
					    runtime, projection_op.kind, "reference_post_join_projection", projection_output.size());
				} else {
					ExecuteProjection(scratch, projection_idx, projection_op, *current, projection_output);
					RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
					                              "post_second_join_batch_projection", projection_stage_start);
					RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "copied_post_join_projection",
					                                         projection_output.size());
				}
				current = &projection_output;
				if (current->size() == 0) {
					projected = nullptr;
					return;
				}
			}
			projected = current;
		};

		auto materialize_selection_only_hash_join_output =
		    [&](idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &join_input,
		        const SelectionVector &match_selection, const SelectionVector &build_selection, Vector &row_pointers,
		        DataChunk &join_output) -> bool {
			if (!scratch.HasOperatorBinding(hash_join_idx)) {
				return false;
			}
			auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
			if (!binding.ready) {
				return false;
			}
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(
			    runtime, SljitRegionStageName(hash_join_idx, hash_join_op.kind, "materialize_output_fallback"));
			switch (binding.layout_kind) {
			case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
				RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "final_output",
				                                         join_output.size());
				RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "row_pointer_reference",
				                                         join_output.size());
				ExecutionMaterializeHashJoinProbe(binding, join_input, row_pointers, match_selection,
				                                  join_output.size(), join_output,
				                                  runtime.TraceRuntime() ? &recorder : nullptr);
				break;
			case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
				RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "final_output",
				                                         join_output.size());
				ExecutionMaterializePerfectHashJoinProbe(binding, join_input, match_selection, build_selection,
				                                         join_output.size(), join_output,
				                                         runtime.TraceRuntime() ? &recorder : nullptr);
				break;
			default:
				return false;
			}
			RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, "materialize_output_fallback",
			                              materialize_stage_start);
			return true;
		};

		auto direct_project_final_projection_to_batch = [&](DataChunk &input, bool &handled) -> bool {
			handled = false;
			if (input.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + input.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (!TryDirectMaterializeFixedProjectionToBatch(runtime, scratch, final_projection_idx, final_projection_op,
			                                                input, batch)) {
				return false;
			}
			handled = true;
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto direct_project_second_join_output = [&](DataChunk &second_join_input, DataChunk &second_join_output,
		                                             DataChunk *&projected, bool &handled) -> bool {
			handled = false;
			projected = nullptr;
			if (second_join_output.size() == 0 || aggregate_idx <= 3) {
				return false;
			}
			auto &direct_projection = scratch.TemporaryChunk(3);
			direct_projection.Reset();
			optional_ptr<const vector<uint8_t>> compressed_skip_ptr;
			if (second_join_batch_omits_compressed_group_keys) {
				compressed_skip_ptr = &second_join_compressed_key_skip_projection;
			}
			optional_ptr<const vector<uint8_t>> precomputed_payload_skip_ptr;
			if (second_join_batch_omits_precomputed_payloads) {
				precomputed_payload_skip_ptr = &second_join_precomputed_payload_skip_projection;
			}
			vector<uint8_t> second_join_projection_skip;
			optional_ptr<const vector<uint8_t>> second_join_projection_skip_ptr;
			if (!OrProjectionSkips(ops[3].projections.size(), compressed_skip_ptr, precomputed_payload_skip_ptr,
			                       second_join_projection_skip, second_join_projection_skip_ptr)) {
				return false;
			}
			if (!TryDirectMaterializeHashJoinProjectionSourcesToBatch(
			        runtime, scratch, 2, 3, ops[3], second_join_input, scratch.FilterSelection(2),
			        scratch.HashJoinRowPointers(2), second_join_output, direct_projection, nullptr, nullptr,
			        second_join_projection_skip_ptr)) {
				if (second_join_batch_omits_compressed_group_keys || second_join_batch_omits_precomputed_payloads) {
					throw InternalException(
					    "SLJIT second-join projection skip became unsupported after descriptor preflight");
				}
				return false;
			}
			if (!copy_second_join_precomputed_payloads_to_projection(direct_projection, second_join_output.size())) {
				throw InternalException("SLJIT precomputed payload passthrough became unsupported");
			}
			handled = true;
			RecordSljitRegionRuntimePath(runtime, ops[3].kind, "direct_second_join_projection",
			                             second_join_output.size());
			bool final_projection_handled = false;
			if (direct_update_final_projection_group_keys(direct_projection, final_projection_handled)) {
				return true;
			}
			if (final_projection_handled) {
				return false;
			}
			if (second_join_batch_omits_compressed_group_keys) {
				throw InternalException("SLJIT compressed group-key aggregate update skip became unsupported");
			}
			if (second_join_batch_omits_precomputed_payloads) {
				throw InternalException("SLJIT precomputed payload aggregate update skip became unsupported");
			}
			if (direct_project_final_projection_to_batch(direct_projection, final_projection_handled)) {
				return true;
			}
			if (final_projection_handled) {
				return false;
			}
			project_second_join_output_from(4, direct_projection, projected);
			return false;
		};

		auto drain_second_hash_join = [&](DataChunk &second_join_input) -> bool {
			if (second_join_input.size() == 0) {
				return false;
			}
			SljitHashJoinProbeDrainState second_state;
			auto &second_join_output = scratch.TemporaryChunk(2);
			do {
				second_join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, 2, second_hash_join_op, second_join_input, second_join_output,
				    scratch.FilterSelection(2), scratch.HashJoinBuildSelection(2), scratch.HashJoinRowPointers(2),
				    scratch.HashJoinSourceFormats(2), scratch.HashJoinSourceData(2),
				    scratch.HashJoinSourceSelections(2), scratch.HashJoinSourceValidity(2),
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(2)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(2)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(2)
				        : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualRowPointers(2)
				        : nullptr,
				    second_state, deferred_reason, false, true);
				RecordSljitRegionStageRuntime(runtime, 2, second_hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (second_join_output.size() == 0) {
					continue;
				}
				DataChunk *projected = nullptr;
				bool direct_projected = false;
				if (direct_project_second_join_output(second_join_input, second_join_output, projected,
				                                      direct_projected)) {
					return true;
				}
				if (!direct_projected) {
					if (!materialize_selection_only_hash_join_output(
					        2, second_hash_join_op, second_join_input, scratch.FilterSelection(2),
					        scratch.HashJoinBuildSelection(2), scratch.HashJoinRowPointers(2), second_join_output)) {
						throw InternalException("SLJIT direct second-join fallback materialization failed");
					}
					project_second_join_output_from(3, second_join_output, projected);
				}
				if (projected && append_projected_batch(*projected)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(second_hash_join_op.hash_join_probe.plan.output_mode, second_state));
			return false;
		};

		auto flush_second_join_batch = [&]() -> bool {
			if (second_join_batch.size() == 0) {
				return false;
			}
			if (drain_second_hash_join(second_join_batch)) {
				return true;
			}
			second_join_batch.Reset();
			reset_between_join_compressed_passthroughs();
			reset_between_join_precomputed_payloads();
			return false;
		};

		auto process_first_join_output = [&](DataChunk &first_join_output) -> bool {
			if (first_join_output.size() == 0) {
				return false;
			}
			auto &second_join_input = scratch.TemporaryChunk(1);
			second_join_input.Reset();
			auto projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(second_join_input, first_join_output, between_join_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 1, between_join_projection_op.kind,
				                              "between_join_reference_projection", projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
				                                         "reference_post_join_projection", second_join_input.size());
			} else {
				ExecuteProjection(scratch, 1, between_join_projection_op, first_join_output, second_join_input);
				RecordSljitRegionStageRuntime(runtime, 1, between_join_projection_op.kind,
				                              "between_join_batch_projection", projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
				                                         "copied_post_join_projection", second_join_input.size());
			}
			return drain_second_hash_join(second_join_input);
		};

		auto append_direct_first_join_projection = [&](DataChunk &first_join_input, DataChunk &first_join_output,
		                                               idx_t selected_count, bool &handled) -> bool {
			handled = false;
			if (selected_count == 0) {
				return false;
			}
			bool use_compressed_group_key_skip = build_compressed_group_key_projection_skips();
			bool use_precomputed_payload_skip = build_precomputed_payload_projection_skips();
			if (second_join_batch.size() > 0 &&
			    (second_join_batch_omits_compressed_group_keys != use_compressed_group_key_skip ||
			     second_join_batch_omits_precomputed_payloads != use_precomputed_payload_skip)) {
				if (flush_second_join_batch()) {
					return true;
				}
			}
			if (second_join_batch.size() + selected_count > STANDARD_VECTOR_SIZE) {
				if (flush_second_join_batch()) {
					return true;
				}
			}
			const auto target_offset = second_join_batch.size();
			optional_ptr<const vector<uint8_t>> compressed_skip_ptr;
			if (use_compressed_group_key_skip) {
				compressed_skip_ptr = &between_join_compressed_key_skip_projection;
			}
			optional_ptr<const vector<uint8_t>> precomputed_payload_skip_ptr;
			if (use_precomputed_payload_skip) {
				precomputed_payload_skip_ptr = &between_join_precomputed_payload_skip_projection;
			}
			vector<uint8_t> between_projection_skip;
			optional_ptr<const vector<uint8_t>> between_projection_skip_ptr;
			if (!OrProjectionSkips(between_join_projection_op.projections.size(), compressed_skip_ptr,
			                       precomputed_payload_skip_ptr, between_projection_skip,
			                       between_projection_skip_ptr)) {
				return false;
			}
			if (!TryDirectMaterializeHashJoinProjectionSourcesToBatch(
			        runtime, scratch, 0, 1, between_join_projection_op, first_join_input, scratch.FilterSelection(0),
			        scratch.HashJoinRowPointers(0), first_join_output, second_join_batch, nullptr, nullptr,
			        between_projection_skip_ptr)) {
				if (!(use_compressed_group_key_skip || use_precomputed_payload_skip) ||
				    !TryDirectMaterializeHashJoinProjectionSourcesToBatch(
				        runtime, scratch, 0, 1, between_join_projection_op, first_join_input,
				        scratch.FilterSelection(0), scratch.HashJoinRowPointers(0), first_join_output,
				        second_join_batch)) {
					return false;
				}
				use_compressed_group_key_skip = false;
				use_precomputed_payload_skip = false;
			}
			second_join_batch_omits_compressed_group_keys = use_compressed_group_key_skip;
			second_join_batch_omits_precomputed_payloads = use_precomputed_payload_skip;
			if (second_join_compressed_passthrough_usable &&
			    !append_between_join_compressed_passthroughs(first_join_input, scratch.FilterSelection(0),
			                                                 scratch.HashJoinRowPointers(0), target_offset,
			                                                 selected_count)) {
				if (use_compressed_group_key_skip) {
					throw InternalException("SLJIT compressed group-key sidecar became unsupported after skip");
				}
				second_join_compressed_passthrough_usable = false;
				if (second_join_compressed_passthrough_initialized &&
				    second_join_compressed_passthrough_batch.ColumnCount() > 0) {
					second_join_compressed_passthrough_batch.Reset();
				}
			}
			if (use_precomputed_payload_skip && !append_between_join_precomputed_payloads(
			                                        first_join_input, scratch.FilterSelection(0),
			                                        scratch.HashJoinRowPointers(0), target_offset, selected_count)) {
				throw InternalException("SLJIT precomputed payload sidecar became unsupported after skip");
			}
			handled = true;
			RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind, "direct_between_join_projection",
			                             selected_count);
			if (use_compressed_group_key_skip) {
				RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
				                             "direct_between_join_compressed_group_key_skip_projection",
				                             selected_count);
			}
			if (use_precomputed_payload_skip) {
				RecordSljitRegionRuntimePath(runtime, between_join_projection_op.kind,
				                             "direct_between_join_precomputed_payload_skip_projection", selected_count);
			}
			if (second_join_batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_second_join_batch()) {
					return true;
				}
			}
			return false;
		};

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			auto next_batch_result = runtime.AdvanceSinkBatch(source_chunk, have_more_output);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}

			SljitHashJoinProbeDrainState first_state;
			auto &first_join_output = scratch.TemporaryChunk(0);
			do {
				first_join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, 0, first_hash_join_op, source_chunk, first_join_output,
				    scratch.FilterSelection(0), scratch.HashJoinBuildSelection(0), scratch.HashJoinRowPointers(0),
				    scratch.HashJoinSourceFormats(0), scratch.HashJoinSourceData(0),
				    scratch.HashJoinSourceSelections(0), scratch.HashJoinSourceValidity(0),
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(0)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(0)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(0)
				        : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(0)
				                                                               : nullptr,
				    first_state, deferred_reason, false, true);
				RecordSljitRegionStageRuntime(runtime, 0, first_hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (first_join_output.size() == 0) {
					continue;
				}
				bool direct_projected = false;
				if (append_direct_first_join_projection(source_chunk, first_join_output, first_join_output.size(),
				                                        direct_projected)) {
					return true;
				}
				if (direct_projected) {
					continue;
				}
				if (!materialize_selection_only_hash_join_output(
				        0, first_hash_join_op, source_chunk, scratch.FilterSelection(0),
				        scratch.HashJoinBuildSelection(0), scratch.HashJoinRowPointers(0), first_join_output)) {
					throw InternalException("SLJIT direct between-join fallback materialization failed");
				}
				if (process_first_join_output(first_join_output)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(first_hash_join_op.hash_join_probe.plan.output_mode, first_state));
			return false;
		};

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
				if (flush_second_join_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk && source_chunk->size() > 0 &&
			    execute_source_chunk(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return true;
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_second_join_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineProjectionHashJoinProjectionHashJoinProjectionProjectionGroupedAggregateBatched(
	    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_batches = 0;
		bool deferred_grouped_finish = false;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		const bool bypass_pre_join_projection = CanBypassInt64ToInt32PreJoinProjection(0, 1);
		const bool first_join_unchecked_key_cast =
		    bypass_pre_join_projection && CanUseUncheckedInt64ToInt32PreJoinProjection(0, 1);
		const bool direct_first_join_to_second_join = CanDirectBuildQ7SecondJoinInput();
		auto &pre_join_projection_op = ops[0];
		auto &first_hash_join_op = ops[1];
		auto &between_join_projection_op = ops[2];
		auto &second_hash_join_op = ops[3];
		auto &first_final_projection_op = ops[4];
		auto &final_projection_op = ops[5];
		auto &aggregate_op = ops[6];
		auto &native_runtime = runtime.ExecutionOperators();
		DataChunk first_join_batch;
		first_join_batch.Initialize(runtime.GetAllocator(), first_hash_join_op.output_types);
		DataChunk direct_second_join_batch;
		if (direct_first_join_to_second_join) {
			direct_second_join_batch.Initialize(runtime.GetAllocator(), between_join_projection_op.output_types);
		}

		auto finish_deferred_grouped_update = [&]() {
			if (!deferred_grouped_finish) {
				return;
			}
			auto &binding = scratch.SinkBinding(6);
			if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
			    !binding.aggregate_update.grouped_state.state) {
				throw InternalException("SLJIT deferred grouped aggregate finish is missing aggregate state");
			}
			auto finish_stage_start = SljitRegionStageStart(runtime);
			binding.aggregate_update.grouped_state.state->FinishStateUpdates();
			RecordSljitRegionStageRuntime(runtime, 6, aggregate_op.kind, "finish_deferred_grouped_state_updates",
			                              finish_stage_start);
			deferred_grouped_finish = false;
		};

		auto execute_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto sink_result =
			    ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, 6, aggregate_op, projected, nullptr,
			                                 DConstants::INVALID_INDEX, true, &deferred_grouped_finish);
			if (sink_result == SinkResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			processed_batches++;
			return false;
		};

		auto flush_projected_batch = [&]() -> bool {
			auto batch = runtime.PendingSourceContractBatch();
			if (!batch) {
				return false;
			}
			if (execute_projected_batch(*batch)) {
				return true;
			}
			runtime.ResetSourceContractBatch();
			return false;
		};

		auto append_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + projected.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (projected.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
				return execute_projected_batch(projected);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(batch, projected)) {
				batch.Append(projected);
			}
			RecordSljitRegionStageRuntime(runtime, 5, final_projection_op.kind, "post_join_batch_append",
			                              append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind, "copied_post_join_batch",
			                                         projected.size());
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto project_second_join_output = [&](DataChunk &second_join_output, DataChunk &projected) {
			auto &intermediate = scratch.TemporaryChunk(4);

			intermediate.Reset();
			auto first_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(intermediate, second_join_output, first_final_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 4, first_final_projection_op.kind,
				                              "post_second_join_reference_projection", first_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, first_final_projection_op.kind,
				                                         "reference_post_join_projection", intermediate.size());
			} else {
				ExecuteProjection(scratch, 4, first_final_projection_op, second_join_output, intermediate);
				RecordSljitRegionStageRuntime(runtime, 4, first_final_projection_op.kind,
				                              "post_second_join_batch_projection", first_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, first_final_projection_op.kind,
				                                         "copied_post_join_projection", intermediate.size());
			}

			projected.Reset();
			auto final_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, intermediate, final_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 5, final_projection_op.kind,
				                              "post_second_join_reference_projection", final_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind,
				                                         "reference_post_join_projection", projected.size());
			} else {
				ExecuteProjection(scratch, 5, final_projection_op, intermediate, projected);
				RecordSljitRegionStageRuntime(runtime, 5, final_projection_op.kind, "post_second_join_batch_projection",
				                              final_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind,
				                                         "copied_post_join_projection", projected.size());
			}
		};

		auto drain_second_hash_join = [&](DataChunk &second_join_input) -> bool {
			if (second_join_input.size() == 0) {
				return false;
			}
			SljitHashJoinProbeDrainState second_state;
			auto &second_join_output = scratch.TemporaryChunk(3);
			auto &projected = scratch.TemporaryChunk(5);
			do {
				second_join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, 3, second_hash_join_op, second_join_input, second_join_output,
				    scratch.FilterSelection(3), scratch.HashJoinBuildSelection(3), scratch.HashJoinRowPointers(3),
				    scratch.HashJoinSourceFormats(3), scratch.HashJoinSourceData(3),
				    scratch.HashJoinSourceSelections(3), scratch.HashJoinSourceValidity(3),
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(3)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(3)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(3)
				        : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualRowPointers(3)
				        : nullptr,
				    second_state, deferred_reason);
				RecordSljitRegionStageRuntime(runtime, 3, second_hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (second_join_output.size() == 0) {
					continue;
				}
				project_second_join_output(second_join_output, projected);
				if (append_projected_batch(projected)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(second_hash_join_op.hash_join_probe.plan.output_mode, second_state));
			return false;
		};

		auto flush_direct_second_join_batch = [&]() -> bool {
			if (!direct_first_join_to_second_join || direct_second_join_batch.size() == 0) {
				return false;
			}
			if (drain_second_hash_join(direct_second_join_batch)) {
				return true;
			}
			direct_second_join_batch.Reset();
			return false;
		};

		auto append_direct_first_join_selection = [&](DataChunk &first_join_input, idx_t selected_count) -> bool {
			if (selected_count == 0) {
				return false;
			}
			if (direct_second_join_batch.size() + selected_count > STANDARD_VECTOR_SIZE) {
				if (flush_direct_second_join_batch()) {
					return true;
				}
			}
			const auto target_offset = direct_second_join_batch.size();
			auto projection_stage_start = SljitRegionStageStart(runtime);
			if (!TryDirectBuildQ7SecondJoinInput(scratch, first_join_input, scratch.FilterSelection(1),
			                                     scratch.HashJoinRowPointers(1), selected_count,
			                                     direct_second_join_batch, target_offset)) {
				throw InternalException("SLJIT Q7 direct first-join selection projection failed");
			}
			RecordSljitRegionStageRuntime(runtime, 2, between_join_projection_op.kind,
			                              "between_join_direct_selection_projection", projection_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
			                                         "direct_row_pointer_projection", selected_count);
			if (direct_second_join_batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_direct_second_join_batch()) {
					return true;
				}
			}
			return false;
		};

		auto process_first_join_batch = [&](DataChunk &batch) -> bool {
			if (batch.size() == 0) {
				return false;
			}
			auto &second_join_input = scratch.TemporaryChunk(2);
			second_join_input.Reset();
			auto projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(second_join_input, batch, between_join_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 2, between_join_projection_op.kind,
				                              "between_join_reference_projection", projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
				                                         "reference_post_join_projection", second_join_input.size());
			} else {
				ExecuteProjection(scratch, 2, between_join_projection_op, batch, second_join_input);
				RecordSljitRegionStageRuntime(runtime, 2, between_join_projection_op.kind,
				                              "between_join_batch_projection", projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, between_join_projection_op.kind,
				                                         "copied_post_join_projection", second_join_input.size());
			}
			return drain_second_hash_join(second_join_input);
		};

		auto flush_first_join_batch = [&]() -> bool {
			if (first_join_batch.size() == 0) {
				return false;
			}
			if (process_first_join_batch(first_join_batch)) {
				return true;
			}
			first_join_batch.Reset();
			return false;
		};

		auto append_first_join_batch = [&](DataChunk &first_join_output) -> bool {
			if (first_join_output.size() == 0) {
				return false;
			}
			if (first_join_batch.size() + first_join_output.size() > STANDARD_VECTOR_SIZE) {
				if (flush_first_join_batch()) {
					return true;
				}
			}
			if (first_join_output.size() == STANDARD_VECTOR_SIZE && first_join_batch.size() == 0) {
				return process_first_join_batch(first_join_output);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(first_join_batch, first_join_output)) {
				first_join_batch.Append(first_join_output);
			}
			RecordSljitRegionStageRuntime(runtime, 1, first_hash_join_op.kind, "first_join_batch_append",
			                              append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, first_hash_join_op.kind, "copied_post_join_batch",
			                                         first_join_output.size());
			if (first_join_batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_first_join_batch()) {
					return true;
				}
			}
			return false;
		};

		auto prepare_first_join_input = [&](DataChunk &source_chunk, DataChunk *&first_join_input) {
			if (bypass_pre_join_projection) {
				first_join_input = &source_chunk;
				return;
			}
			auto &pre_join = scratch.TemporaryChunk(0);
			pre_join.Reset();
			auto projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(pre_join, source_chunk, pre_join_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 0, pre_join_projection_op.kind, "pre_join_reference_projection",
				                              projection_stage_start);
			} else {
				ExecuteProjection(scratch, 0, pre_join_projection_op, source_chunk, pre_join);
				RecordSljitRegionStageRuntime(runtime, 0, pre_join_projection_op.kind, "pre_join_batch_projection",
				                              projection_stage_start);
			}
			first_join_input = pre_join.size() == 0 ? nullptr : &pre_join;
		};

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			auto next_batch_result = runtime.AdvanceSinkBatch(source_chunk, have_more_output);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}

			DataChunk *first_join_input = nullptr;
			prepare_first_join_input(source_chunk, first_join_input);
			if (!first_join_input || first_join_input->size() == 0) {
				return false;
			}

			SljitHashJoinProbeDrainState first_state;
			auto &first_join_output = scratch.TemporaryChunk(1);
			do {
				first_join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, 1, first_hash_join_op, *first_join_input, first_join_output,
				    scratch.FilterSelection(1), scratch.HashJoinBuildSelection(1), scratch.HashJoinRowPointers(1),
				    scratch.HashJoinSourceFormats(1), scratch.HashJoinSourceData(1),
				    scratch.HashJoinSourceSelections(1), scratch.HashJoinSourceValidity(1),
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(1)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(1)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(1)
				        : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(1)
				                                                               : nullptr,
				    first_state, deferred_reason, first_join_unchecked_key_cast, direct_first_join_to_second_join);
				RecordSljitRegionStageRuntime(runtime, 1, first_hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (direct_first_join_to_second_join) {
					if (first_join_output.size() != 0) {
						if (append_direct_first_join_selection(*first_join_input, first_join_output.size())) {
							return true;
						}
					}
				} else {
					if (append_first_join_batch(first_join_output)) {
						return true;
					}
				}
			} while (!HashJoinProbeDrainFinished(first_hash_join_op.hash_join_probe.plan.output_mode, first_state));
			return false;
		};

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
				if (flush_first_join_batch() || flush_direct_second_join_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk && source_chunk->size() > 0 &&
			    execute_source_chunk(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return true;
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_first_join_batch() || flush_direct_second_join_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineHashJoinHashJoinProjectionGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                             ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_output_rows = 0;
		bool deferred_grouped_finish = false;
		bool pending_second_probe_input_initialized = false;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		auto &first_hash_join_op = ops[0];
		auto &second_hash_join_op = ops[1];
		auto &final_projection_op = ops[2];
		auto &aggregate_op = ops[3];
		auto &native_runtime = runtime.ExecutionOperators();
		DataChunk pending_second_probe_input;
		Vector pending_second_probe_row_pointers(LogicalType::POINTER);
		vector<ExecutionRowPointerGroupKeySource> pending_second_probe_group_sources;
		vector<idx_t> pending_second_probe_payload_sources;

		auto finish_deferred_grouped_update = [&]() {
			if (!deferred_grouped_finish) {
				return;
			}
			auto &binding = scratch.SinkBinding(3);
			if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
			    !binding.aggregate_update.grouped_state.state) {
				throw InternalException("SLJIT deferred grouped aggregate finish is missing aggregate state");
			}
			auto finish_stage_start = SljitRegionStageStart(runtime);
			binding.aggregate_update.grouped_state.state->FinishStateUpdates();
			RecordSljitRegionStageRuntime(runtime, 3, aggregate_op.kind, "finish_deferred_grouped_state_updates",
			                              finish_stage_start);
			deferred_grouped_finish = false;
		};

		auto execute_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto sink_result =
			    ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, 3, aggregate_op, projected, nullptr,
			                                 DConstants::INVALID_INDEX, true, &deferred_grouped_finish);
			if (sink_result == SinkResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			SljitChargeDownstreamRows(processed_output_rows, projected.size());
			return false;
		};

		auto flush_projected_batch = [&]() -> bool {
			auto batch = runtime.PendingSourceContractBatch();
			if (!batch) {
				return false;
			}
			if (execute_projected_batch(*batch)) {
				return true;
			}
			runtime.ResetSourceContractBatch();
			return false;
		};

		auto append_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + projected.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (projected.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
				return execute_projected_batch(projected);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(batch, projected)) {
				batch.Append(projected);
			}
			RecordSljitRegionStageRuntime(runtime, 2, final_projection_op.kind, "post_join_batch_append",
			                              append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind, "copied_post_join_batch",
			                                         projected.size());
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto project_second_join_output = [&](DataChunk &second_join_output, DataChunk &projected) {
			projected.Reset();
			auto projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, second_join_output, final_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 2, final_projection_op.kind,
				                              "post_second_join_reference_projection", projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind,
				                                         "reference_post_join_projection", projected.size());
			} else {
				ExecuteProjection(scratch, 2, final_projection_op, second_join_output, projected);
				RecordSljitRegionStageRuntime(runtime, 2, final_projection_op.kind, "post_second_join_batch_projection",
				                              projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind,
				                                         "copied_post_join_projection", projected.size());
			}
		};

		auto reset_pending_second_probe_batch = [&]() {
			if (pending_second_probe_input_initialized) {
				pending_second_probe_input.Reset();
			}
			pending_second_probe_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::SetSize(pending_second_probe_row_pointers, 0);
		};

		auto materialize_selection_only_second_join_output =
		    [&](DataChunk &join_input, const SelectionVector &match_selection, const SelectionVector &build_selection,
		        Vector &row_pointers, DataChunk &join_output) -> bool {
			if (!scratch.HasOperatorBinding(1)) {
				return false;
			}
			auto &binding = scratch.OperatorBinding(1).hash_join_probe;
			if (!binding.ready) {
				return false;
			}
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(
			    runtime, SljitRegionStageName(1, second_hash_join_op.kind, "materialize_output_fallback"));
			switch (binding.layout_kind) {
			case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
				RecordSljitRegionMaterializationBoundary(runtime, second_hash_join_op.kind, "row_pointer_reference",
				                                         join_output.size());
				RecordSljitRegionMaterializationBoundary(runtime, second_hash_join_op.kind, "final_output",
				                                         join_output.size());
				ExecutionMaterializeHashJoinProbe(binding, join_input, row_pointers, match_selection,
				                                  join_output.size(), join_output,
				                                  runtime.TraceRuntime() ? &recorder : nullptr);
				break;
			case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
				RecordSljitRegionMaterializationBoundary(runtime, second_hash_join_op.kind, "final_output",
				                                         join_output.size());
				ExecutionMaterializePerfectHashJoinProbe(binding, join_input, match_selection, build_selection,
				                                         join_output.size(), join_output,
				                                         runtime.TraceRuntime() ? &recorder : nullptr);
				break;
			default:
				return false;
			}
			RecordSljitRegionStageRuntime(runtime, 1, second_hash_join_op.kind, "materialize_output_fallback",
			                              materialize_stage_start);
			return true;
		};

		auto append_direct_projected_batch_from_second_probe =
		    [&](DataChunk &join_input, const SelectionVector &match_selection, Vector &row_pointers,
		        DataChunk &join_output, bool &handled) -> bool {
			handled = false;
			if (join_output.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + join_output.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (!TryDirectMaterializeHashJoinProjectionSourcesToBatch(runtime, scratch, 1, 2, final_projection_op,
			                                                          join_input, match_selection, row_pointers,
			                                                          join_output, batch)) {
				return false;
			}
			handled = true;
			RecordSljitRegionRuntimePath(runtime, final_projection_op.kind, "direct_second_join_projection",
			                             join_output.size());
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto try_execute_pending_second_probe_grouped_update = [&](bool &handled) -> bool {
			handled = false;
			if (!pending_second_probe_input_initialized || pending_second_probe_input.size() == 0) {
				return false;
			}
			if (runtime.PendingSourceContractBatch() && flush_projected_batch()) {
				return true;
			}
			if (!scratch.HasOperatorBinding(1)) {
				RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
				                             "direct_row_pointer_grouped_lookup_update_unsupported.no_join_binding");
				return false;
			}
			auto &binding = scratch.OperatorBinding(1).hash_join_probe;
			if (!binding.ready || !TryBuildRowPointerGroupKeySources(binding, final_projection_op, aggregate_op,
			                                                         pending_second_probe_group_sources)) {
				RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
				                             "direct_row_pointer_grouped_lookup_update_unsupported.group_key_sources",
				                             pending_second_probe_input.size());
				return false;
			}
			if (!TryBuildRowPointerGroupedPayloadSourceOverride(binding, final_projection_op, aggregate_op,
			                                                    pending_second_probe_input,
			                                                    pending_second_probe_payload_sources)) {
				RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
				                             "direct_row_pointer_grouped_lookup_update_unsupported.payload_sources",
				                             pending_second_probe_input.size());
				return false;
			}
			if (!TryExecuteNativeRowPointerGroupedAggregateUpdate(
			        runtime, native_runtime, scratch, 3, aggregate_op, pending_second_probe_input,
			        pending_second_probe_row_pointers, pending_second_probe_group_sources,
			        pending_second_probe_payload_sources, true, &deferred_grouped_finish)) {
				return false;
			}
			native_runtime.RecordSinkResult(pending_second_probe_input.size(), SinkResultType::NEED_MORE_INPUT);
			handled = true;
			SljitChargeDownstreamRows(processed_output_rows, pending_second_probe_input.size());
			reset_pending_second_probe_batch();
			return false;
		};

		auto flush_pending_second_probe_batch = [&]() -> bool {
			if (!pending_second_probe_input_initialized || pending_second_probe_input.size() == 0) {
				return false;
			}
			bool row_pointer_grouped_update = false;
			if (try_execute_pending_second_probe_grouped_update(row_pointer_grouped_update)) {
				return true;
			}
			if (row_pointer_grouped_update) {
				return false;
			}

			auto &second_join_output = scratch.TemporaryChunk(1);
			auto &projected = scratch.TemporaryChunk(2);
			second_join_output.Reset();
			second_join_output.SetChildCardinality(pending_second_probe_input.size());
			if (!materialize_selection_only_second_join_output(
			        pending_second_probe_input, *FlatVector::IncrementalSelectionVector(),
			        *FlatVector::IncrementalSelectionVector(), pending_second_probe_row_pointers, second_join_output)) {
				return false;
			}
			project_second_join_output(second_join_output, projected);
			if (append_projected_batch(projected)) {
				return true;
			}
			reset_pending_second_probe_batch();
			return false;
		};

		auto append_pending_second_probe_batch = [&](DataChunk &join_input, SelectionVector &match_selection,
		                                             Vector &row_pointers, idx_t count, bool &handled) -> bool {
			handled = false;
			if (count == 0) {
				return false;
			}
			if (count == STANDARD_VECTOR_SIZE &&
			    (!pending_second_probe_input_initialized || pending_second_probe_input.size() == 0)) {
				return false;
			}
			if (!pending_second_probe_input_initialized) {
				pending_second_probe_input.Initialize(runtime.GetAllocator(), join_input.GetTypes());
				pending_second_probe_input_initialized = true;
				reset_pending_second_probe_batch();
			}
			if (pending_second_probe_input.ColumnCount() != join_input.ColumnCount()) {
				return false;
			}
			if (pending_second_probe_input.size() + count > STANDARD_VECTOR_SIZE) {
				if (flush_pending_second_probe_batch()) {
					return true;
				}
			}
			if (count == STANDARD_VECTOR_SIZE && pending_second_probe_input.size() == 0) {
				return false;
			}
			if (!SljitAppendSelectedProbeBatch(runtime, 1, second_hash_join_op.kind, join_input, match_selection,
			                                   row_pointers, count, pending_second_probe_input,
			                                   pending_second_probe_row_pointers)) {
				return false;
			}
			handled = true;
			if (pending_second_probe_input.size() == STANDARD_VECTOR_SIZE) {
				if (flush_pending_second_probe_batch()) {
					return true;
				}
			}
			return false;
		};

		auto drain_second_hash_join = [&](DataChunk &second_join_input) -> bool {
			if (second_join_input.size() == 0) {
				return false;
			}
			SljitHashJoinProbeDrainState second_state;
			auto &second_join_output = scratch.TemporaryChunk(1);
			auto &projected = scratch.TemporaryChunk(2);
			do {
				second_join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, 1, second_hash_join_op, second_join_input, second_join_output,
				    scratch.FilterSelection(1), scratch.HashJoinBuildSelection(1), scratch.HashJoinRowPointers(1),
				    scratch.HashJoinSourceFormats(1), scratch.HashJoinSourceData(1),
				    scratch.HashJoinSourceSelections(1), scratch.HashJoinSourceValidity(1),
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(1)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(1)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(1)
				        : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualRowPointers(1)
				        : nullptr,
				    second_state, deferred_reason, false, true);
				RecordSljitRegionStageRuntime(runtime, 1, second_hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					if (flush_pending_second_probe_batch() || flush_projected_batch()) {
						return true;
					}
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (second_join_output.size() == 0) {
					continue;
				}
				bool pending_buffered = false;
				if (append_pending_second_probe_batch(second_join_input, scratch.FilterSelection(1),
				                                      scratch.HashJoinRowPointers(1), second_join_output.size(),
				                                      pending_buffered)) {
					return true;
				}
				if (pending_buffered) {
					continue;
				}
				bool direct_projected = false;
				if (append_direct_projected_batch_from_second_probe(second_join_input, scratch.FilterSelection(1),
				                                                    scratch.HashJoinRowPointers(1), second_join_output,
				                                                    direct_projected)) {
					return true;
				}
				if (direct_projected) {
					continue;
				}
				if (!materialize_selection_only_second_join_output(
				        second_join_input, scratch.FilterSelection(1), scratch.HashJoinBuildSelection(1),
				        scratch.HashJoinRowPointers(1), second_join_output)) {
					throw InternalException("SLJIT direct second-join fallback materialization failed");
				}
				project_second_join_output(second_join_output, projected);
				if (append_projected_batch(projected)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(second_hash_join_op.hash_join_probe.plan.output_mode, second_state));
			return false;
		};

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			auto next_batch_result = runtime.AdvanceSinkBatch(source_chunk, have_more_output);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				if (flush_pending_second_probe_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}

			SljitHashJoinProbeDrainState first_state;
			auto &first_join_output = scratch.TemporaryChunk(0);
			do {
				first_join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, 0, first_hash_join_op, source_chunk, first_join_output,
				    scratch.FilterSelection(0), scratch.HashJoinBuildSelection(0), scratch.HashJoinRowPointers(0),
				    scratch.HashJoinSourceFormats(0), scratch.HashJoinSourceData(0),
				    scratch.HashJoinSourceSelections(0), scratch.HashJoinSourceValidity(0),
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(0)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(0)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(0)
				        : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(0)
				                                                               : nullptr,
				    first_state, deferred_reason);
				RecordSljitRegionStageRuntime(runtime, 0, first_hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					if (flush_pending_second_probe_batch() || flush_projected_batch()) {
						return true;
					}
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (first_join_output.size() == 0) {
					continue;
				}
				if (drain_second_hash_join(first_join_output)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(first_hash_join_op.hash_join_probe.plan.output_mode, first_state));
			return false;
		};

		while (true) {
			if (SljitDownstreamRowBudgetReached(processed_output_rows, runtime.MaxChunks()) ||
			    fetched_chunks >= max_source_fetches) {
				if (flush_pending_second_probe_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				if (flush_pending_second_probe_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk && source_chunk->size() > 0 &&
			    execute_source_chunk(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return true;
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_pending_second_probe_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool
	TryExecuteFullPipelineHashJoinHashJoinProjectionProjectionGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                                  ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_batches = 0;
		bool deferred_grouped_finish = false;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		auto &first_hash_join_op = ops[0];
		auto &second_hash_join_op = ops[1];
		auto &first_projection_op = ops[2];
		auto &final_projection_op = ops[3];
		auto &aggregate_op = ops[4];
		auto &native_runtime = runtime.ExecutionOperators();
		DataChunk source_batch;
		bool source_batch_initialized = false;

		auto finish_deferred_grouped_update = [&]() {
			if (!deferred_grouped_finish) {
				return;
			}
			auto &binding = scratch.SinkBinding(4);
			if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
			    !binding.aggregate_update.grouped_state.state) {
				throw InternalException("SLJIT deferred grouped aggregate finish is missing aggregate state");
			}
			auto finish_stage_start = SljitRegionStageStart(runtime);
			binding.aggregate_update.grouped_state.state->FinishStateUpdates();
			RecordSljitRegionStageRuntime(runtime, 4, aggregate_op.kind, "finish_deferred_grouped_state_updates",
			                              finish_stage_start);
			deferred_grouped_finish = false;
		};

		auto execute_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto sink_result =
			    ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, 4, aggregate_op, projected, nullptr,
			                                 DConstants::INVALID_INDEX, true, &deferred_grouped_finish);
			if (sink_result == SinkResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			processed_batches++;
			return false;
		};

		auto flush_projected_batch = [&]() -> bool {
			auto batch = runtime.PendingSourceContractBatch();
			if (!batch) {
				return false;
			}
			if (execute_projected_batch(*batch)) {
				return true;
			}
			runtime.ResetSourceContractBatch();
			return false;
		};

		auto append_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + projected.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (projected.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
				return execute_projected_batch(projected);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(batch, projected)) {
				batch.Append(projected);
			}
			RecordSljitRegionStageRuntime(runtime, 3, final_projection_op.kind, "post_join_batch_append",
			                              append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind, "copied_post_join_batch",
			                                         projected.size());
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto project_join_output = [&](DataChunk &join_output, DataChunk &projected) {
			auto &intermediate = scratch.TemporaryChunk(2);

			intermediate.Reset();
			auto first_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(intermediate, join_output, first_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 2, first_projection_op.kind, "post_join_reference_projection",
				                              first_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, first_projection_op.kind,
				                                         "reference_post_join_projection", intermediate.size());
			} else {
				ExecuteProjection(scratch, 2, first_projection_op, join_output, intermediate);
				RecordSljitRegionStageRuntime(runtime, 2, first_projection_op.kind, "post_join_batch_projection",
				                              first_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, first_projection_op.kind,
				                                         "copied_post_join_projection", intermediate.size());
			}

			projected.Reset();
			auto second_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, intermediate, final_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 3, final_projection_op.kind, "post_join_reference_projection",
				                              second_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind,
				                                         "reference_post_join_projection", projected.size());
			} else {
				ExecuteProjection(scratch, 3, final_projection_op, intermediate, projected);
				RecordSljitRegionStageRuntime(runtime, 3, final_projection_op.kind, "post_join_batch_projection",
				                              second_projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind,
				                                         "copied_post_join_projection", projected.size());
			}
		};

		auto drain_second_hash_join = [&](DataChunk &first_join_output) -> bool {
			SljitHashJoinProbeDrainState second_state;
			auto &second_join_output = scratch.TemporaryChunk(1);
			auto &projected = scratch.TemporaryChunk(3);
			do {
				second_join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, 1, second_hash_join_op, first_join_output, second_join_output,
				    scratch.FilterSelection(1), scratch.HashJoinBuildSelection(1), scratch.HashJoinRowPointers(1),
				    scratch.HashJoinSourceFormats(1), scratch.HashJoinSourceData(1),
				    scratch.HashJoinSourceSelections(1), scratch.HashJoinSourceValidity(1),
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(1)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(1)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(1)
				        : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualRowPointers(1)
				        : nullptr,
				    second_state, deferred_reason);
				RecordSljitRegionStageRuntime(runtime, 1, second_hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (second_join_output.size() == 0) {
					continue;
				}
				project_join_output(second_join_output, projected);
				if (append_projected_batch(projected)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(second_hash_join_op.hash_join_probe.plan.output_mode, second_state));
			return false;
		};

		auto execute_source_batch = [&](DataChunk &source_chunk) -> bool {
			SljitHashJoinProbeDrainState first_state;
			auto &first_join_output = scratch.TemporaryChunk(0);
			do {
				first_join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, 0, first_hash_join_op, source_chunk, first_join_output,
				    scratch.FilterSelection(0), scratch.HashJoinBuildSelection(0), scratch.HashJoinRowPointers(0),
				    scratch.HashJoinSourceFormats(0), scratch.HashJoinSourceData(0),
				    scratch.HashJoinSourceSelections(0), scratch.HashJoinSourceValidity(0),
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(0)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(0)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(0)
				        : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(0)
				                                                               : nullptr,
				    first_state, deferred_reason);
				RecordSljitRegionStageRuntime(runtime, 0, first_hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (first_join_output.size() == 0) {
					continue;
				}
				if (drain_second_hash_join(first_join_output)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(first_hash_join_op.hash_join_probe.plan.output_mode, first_state));
			return false;
		};

		auto flush_source_batch = [&]() -> bool {
			if (!source_batch_initialized || source_batch.size() == 0) {
				return false;
			}
			if (execute_source_batch(source_batch)) {
				return true;
			}
			source_batch.Reset();
			return false;
		};

		auto append_source_batch = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			auto next_batch_result = runtime.AdvanceSinkBatch(source_chunk, have_more_output);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				if (flush_source_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			if (source_chunk.size() == 0) {
				return false;
			}
			if (!source_batch_initialized) {
				source_batch.Initialize(runtime.GetAllocator(), source_chunk.GetTypes());
				source_batch_initialized = true;
			}
			if (source_batch.ColumnCount() != source_chunk.ColumnCount()) {
				if (flush_source_batch()) {
					return true;
				}
				return execute_source_batch(source_chunk);
			}
			if (source_batch.size() + source_chunk.size() > STANDARD_VECTOR_SIZE) {
				if (flush_source_batch()) {
					return true;
				}
			}
			if (source_chunk.size() == STANDARD_VECTOR_SIZE && source_batch.size() == 0) {
				return execute_source_batch(source_chunk);
			}
			if (!SljitTryFastAppendFixedFlatAllValid(source_batch, source_chunk)) {
				source_batch.Append(source_chunk);
			}
			if (source_batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_source_batch()) {
					return true;
				}
			}
			return false;
		};

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
				if (flush_source_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk && source_chunk->size() > 0 &&
			    append_source_batch(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return true;
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_source_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	bool TryExecuteFullPipelineGeneratedFilterHashJoinProjectionGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                                    ExecutionRegionResult &result) {
		auto prepare_join_input = [&](SljitRegionExecutionScratch &scratch, DataChunk &source_chunk,
		                              SourceResultType source_result, DataChunk *&join_input) -> bool {
			auto next_batch_result =
			    runtime.AdvanceSinkBatch(source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT);
			if (next_batch_result == SinkNextBatchType::BLOCKED) {
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}

			auto &filtered = scratch.TemporaryChunk(1);
			filtered.Reset();
			auto stage_start = SljitRegionStageStart(runtime);
			ExecuteFilterProjection(scratch, ops[0], ops[1], 1, source_chunk, filtered, scratch.FilterSelection(0));
			RecordSljitRegionStageRuntimeWithSuffix(runtime, 0, ops[0].kind, "+projection", stage_start);
			join_input = filtered.size() == 0 ? nullptr : &filtered;
			return false;
		};
		return TryExecuteFullPipelineHashJoinProjectionGroupedAggregateBatched(runtime, result, 2, 3, 4,
		                                                                       prepare_join_input);
	}

	template <class PREPARE_JOIN_INPUT>
	bool TryExecuteFullPipelineHashJoinProjectionGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
	                                                                     ExecutionRegionResult &result,
	                                                                     idx_t hash_join_idx, idx_t projection_idx,
	                                                                     idx_t aggregate_idx,
	                                                                     PREPARE_JOIN_INPUT prepare_join_input) {
		auto project_join_output = [&](SljitRegionExecutionScratch &scratch, DataChunk &join_output,
		                               DataChunk &projected) {
			auto &projection_op = ops[projection_idx];
			projected.Reset();
			auto projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, join_output, projection_op)) {
				RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind,
				                              "post_join_reference_projection", projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "reference_post_join_projection",
				                                         projected.size());
			} else {
				ExecuteProjection(scratch, projection_idx, projection_op, join_output, projected);
				RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind, "post_join_batch_projection",
				                              projection_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, projection_op.kind, "copied_post_join_projection",
				                                         projected.size());
			}
		};
		auto direct_project_join_output_to_batch = [&](SljitRegionExecutionScratch &scratch, DataChunk *join_input,
		                                               const SelectionVector *match_selection, Vector *row_pointers,
		                                               DataChunk &join_output, DataChunk &batch) {
			if (join_input && match_selection && row_pointers) {
				return TryDirectMaterializeHashJoinProjectionSourcesToBatch(
				    runtime, scratch, hash_join_idx, projection_idx, ops[projection_idx], *join_input, *match_selection,
				    *row_pointers, join_output, batch);
			}
			return TryDirectMaterializeFixedProjectionToBatch(runtime, scratch, projection_idx, ops[projection_idx],
			                                                  join_output, batch);
		};
		return TryExecuteFullPipelineHashJoinProjectedGroupedAggregateBatched(
		    runtime, result, hash_join_idx, projection_idx, aggregate_idx, prepare_join_input, project_join_output,
		    direct_project_join_output_to_batch);
	}

	template <class PREPARE_JOIN_INPUT, class PROJECT_JOIN_OUTPUT, class TRY_DIRECT_PROJECT_JOIN_OUTPUT_TO_BATCH>
	bool TryExecuteFullPipelineHashJoinProjectedGroupedAggregateBatched(
	    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result, idx_t hash_join_idx, idx_t final_projection_idx,
	    idx_t aggregate_idx, PREPARE_JOIN_INPUT prepare_join_input, PROJECT_JOIN_OUTPUT project_join_output,
	    TRY_DIRECT_PROJECT_JOIN_OUTPUT_TO_BATCH try_direct_project_join_output_to_batch,
	    bool source_key0_int64_to_int32_unchecked = false) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_output_rows = 0;
		bool deferred_grouped_finish = false;
		bool pending_probe_input_initialized = false;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		auto &hash_join_op = ops[hash_join_idx];
		auto &final_projection_op = ops[final_projection_idx];
		auto &aggregate_op = ops[aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		DataChunk pending_join_input;
		bool pending_join_input_initialized = false;
		DataChunk pending_probe_input;
		Vector pending_probe_row_pointers(LogicalType::POINTER);
		DataChunk pending_probe_group_key_batch;
		Vector pending_probe_group_key_hashes(LogicalType::HASH);
		bool pending_probe_group_key_batch_initialized = false;
		bool pending_probe_split_payload_descriptor_initialized = false;
		bool pending_probe_split_payload_descriptor_ready = false;
		string pending_probe_split_payload_descriptor_blocker;
		vector<ExecutionRowPointerGroupKeySource> pending_probe_group_sources;
		vector<idx_t> pending_probe_payload_sources;
		vector<LogicalType> pending_probe_group_key_types;
		vector<idx_t> pending_probe_group_projection_indices;
		vector<idx_t> pending_probe_split_payload_sources;

		auto finish_deferred_grouped_update = [&]() {
			if (!deferred_grouped_finish) {
				return;
			}
			auto &binding = scratch.SinkBinding(aggregate_idx);
			if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
			    !binding.aggregate_update.grouped_state.state) {
				throw InternalException("SLJIT deferred grouped aggregate finish is missing aggregate state");
			}
			auto finish_stage_start = SljitRegionStageStart(runtime);
			binding.aggregate_update.grouped_state.state->FinishStateUpdates();
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "finish_deferred_grouped_state_updates", finish_stage_start);
			deferred_grouped_finish = false;
		};

		auto execute_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto sink_result =
			    ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, aggregate_idx, aggregate_op, projected,
			                                 nullptr, DConstants::INVALID_INDEX, true, &deferred_grouped_finish);
			if (sink_result == SinkResultType::BLOCKED) {
				finish_deferred_grouped_update();
				result = runtime.DeferredReason().empty() ? ExecutionRegionResult::INTERRUPTED
				                                          : ExecutionRegionResult::DEFERRED;
				return true;
			}
			if (sink_result == SinkResultType::FINISHED) {
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
			SljitChargeDownstreamRows(processed_output_rows, projected.size());
			return false;
		};

		auto flush_projected_batch = [&]() -> bool {
			auto batch = runtime.PendingSourceContractBatch();
			if (!batch) {
				return false;
			}
			if (execute_projected_batch(*batch)) {
				return true;
			}
			runtime.ResetSourceContractBatch();
			return false;
		};

		auto append_projected_batch = [&](DataChunk &projected) -> bool {
			if (projected.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + projected.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (projected.size() == STANDARD_VECTOR_SIZE && batch.size() == 0) {
				return execute_projected_batch(projected);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(batch, projected)) {
				batch.Append(projected);
			}
			RecordSljitRegionStageRuntime(runtime, final_projection_idx, final_projection_op.kind,
			                              "post_join_batch_append", append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, final_projection_op.kind, "copied_post_join_batch",
			                                         projected.size());
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto reset_pending_probe_batch = [&]() {
			if (pending_probe_input_initialized) {
				pending_probe_input.Reset();
			}
			pending_probe_row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::SetSize(pending_probe_row_pointers, 0);
		};

		auto append_direct_projected_batch = [&](DataChunk &join_output, bool &handled) -> bool {
			handled = false;
			if (join_output.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + join_output.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (!try_direct_project_join_output_to_batch(scratch, nullptr, nullptr, nullptr, join_output, batch)) {
				return false;
			}
			handled = true;
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto append_direct_projected_batch_from_probe =
		    [&](DataChunk &join_input, const SelectionVector &match_selection, Vector &row_pointers,
		        DataChunk &join_output, bool &handled) -> bool {
			handled = false;
			if (join_output.size() == 0) {
				return false;
			}
			auto &batch = runtime.PrepareSourceContractBatch(final_projection_op.output_types);
			if (batch.size() + join_output.size() > STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			if (!try_direct_project_join_output_to_batch(scratch, &join_input, &match_selection, &row_pointers,
			                                             join_output, batch)) {
				return false;
			}
			handled = true;
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto try_execute_pending_row_pointer_grouped_update = [&](bool &handled) -> bool {
			handled = false;
			if (!pending_probe_input_initialized || pending_probe_input.size() == 0) {
				return false;
			}
			if (!scratch.HasOperatorBinding(hash_join_idx)) {
				RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
				                             "direct_row_pointer_grouped_lookup_update_unsupported.no_join_binding");
				return false;
			}
			auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
			if (!binding.ready || !TryBuildRowPointerGroupKeySources(binding, final_projection_op, aggregate_op,
			                                                         pending_probe_group_sources)) {
				RecordSljitRegionRuntimePath(runtime, aggregate_op.kind,
				                             "direct_row_pointer_grouped_lookup_update_unsupported.group_key_sources");
				return false;
			}
			if (!TryBuildRowPointerGroupedPayloadSourceOverride(binding, final_projection_op, aggregate_op,
			                                                    pending_probe_input, pending_probe_payload_sources)) {
				return false;
			}
			if (!TryExecuteNativeRowPointerGroupedAggregateUpdate(
			        runtime, native_runtime, scratch, aggregate_idx, aggregate_op, pending_probe_input,
			        pending_probe_row_pointers, pending_probe_group_sources, pending_probe_payload_sources, true,
			        &deferred_grouped_finish)) {
				return false;
			}
			handled = true;
			SljitChargeDownstreamRows(processed_output_rows, pending_probe_input.size());
			reset_pending_probe_batch();
			return false;
		};

		auto record_pending_probe_split_payload_unsupported = [&](const string &reason, idx_t count) {
			auto path = string("direct_projected_group_payload_update_unsupported.") + reason;
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, path.c_str(), count);
		};

		auto build_pending_probe_split_payload_descriptor = [&](DataChunk &payload_input) -> bool {
			if (pending_probe_split_payload_descriptor_initialized) {
				return pending_probe_split_payload_descriptor_ready;
			}
			pending_probe_split_payload_descriptor_initialized = true;
			auto set_blocker = [&](const char *blocker) {
				pending_probe_split_payload_descriptor_blocker = blocker;
				pending_probe_split_payload_descriptor_ready = false;
				return false;
			};
			if (final_projection_op.kind != SljitNativeRegionOpKind::PROJECTION ||
			    aggregate_op.kind != SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
				return set_blocker("operator_kind");
			}
			auto &aggregate_update = aggregate_op.aggregate_update;
			auto &aggregate_plan = aggregate_update.plan;
			auto &sink_info = aggregate_plan.sink_info;
			if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty() ||
			    sink_info.aggregates.empty() || !aggregate_plan.use_primitive_payloads ||
			    !aggregate_plan.use_grouped_state_addresses || aggregate_plan.use_perfect_hash_group_lookup ||
			    aggregate_update.fused_payload_update_owns_group_lookup ||
			    aggregate_update.fused_payload_update_function) {
				return set_blocker("aggregate_shape");
			}
			if (aggregate_update.payloads.size() != sink_info.aggregates.size()) {
				return set_blocker("payload_count");
			}
			if (!scratch.HasOperatorBinding(hash_join_idx)) {
				return set_blocker("no_join_binding");
			}
			auto &binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
			if (!binding.ready) {
				return set_blocker("join_binding");
			}
			pending_probe_group_key_types.clear();
			pending_probe_group_projection_indices.clear();
			pending_probe_group_key_types.reserve(sink_info.groups.size());
			pending_probe_group_projection_indices.reserve(sink_info.groups.size());
			for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
				auto &group = sink_info.groups[group_idx];
				if (!group.supported_reference || group.input_index != group_idx ||
				    group.input_index >= final_projection_op.projections.size() ||
				    group.input_index >= final_projection_op.output_types.size()) {
					return set_blocker("group_key_not_dense");
				}
				auto &group_projection = final_projection_op.projections[group.input_index].plan;
				if (group_projection.return_type.InternalType() != group.type.InternalType() ||
				    final_projection_op.output_types[group.input_index].InternalType() != group.type.InternalType()) {
					return set_blocker("group_key_type");
				}
				pending_probe_group_projection_indices.push_back(group.input_index);
				pending_probe_group_key_types.push_back(final_projection_op.output_types[group.input_index]);
			}
			pending_probe_split_payload_sources.clear();
			pending_probe_split_payload_sources.reserve(sink_info.aggregates.size());
			for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
				auto &aggregate = sink_info.aggregates[payload_idx];
				auto &payload = aggregate_update.payloads[payload_idx].plan;
				if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					if (aggregate.child_count != 0) {
						return set_blocker("count_star_payload");
					}
					pending_probe_split_payload_sources.push_back(DConstants::INVALID_INDEX);
					continue;
				}
				if (aggregate.child_count != 1 || aggregate.child_types.size() != 1 ||
				    aggregate.payload_index >= final_projection_op.projections.size()) {
					return set_blocker("payload_contract");
				}
				if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
				    payload.source_index != aggregate.payload_index) {
					return set_blocker("payload_not_final_reference");
				}
				SljitExecutableRegionExpression remapped_expr;
				idx_t payload_source_idx;
				if (!TryBuildSingleSourceProjectionExpression(final_projection_op.projections[aggregate.payload_index],
				                                              remapped_expr, payload_source_idx) ||
				    !ProjectionIsSingleSourceReferenceLike(remapped_expr.plan) ||
				    payload_source_idx >= binding.lhs_output_column_indices.size()) {
					return set_blocker("payload_source");
				}
				const auto input_source_idx = binding.lhs_output_column_indices[payload_source_idx];
				if (input_source_idx >= payload_input.ColumnCount() ||
				    remapped_expr.plan.return_type.InternalType() != aggregate.child_types[0].InternalType() ||
				    payload_input.data[input_source_idx].GetType().InternalType() !=
				        aggregate.child_types[0].InternalType()) {
					return set_blocker("payload_type");
				}
				pending_probe_split_payload_sources.push_back(input_source_idx);
			}
			pending_probe_split_payload_descriptor_ready = true;
			return true;
		};

		auto try_execute_pending_probe_split_payload_update = [&](bool &handled) -> bool {
			handled = false;
			if (!pending_probe_input_initialized || pending_probe_input.size() == 0) {
				return false;
			}
			if (!build_pending_probe_split_payload_descriptor(pending_probe_input)) {
				record_pending_probe_split_payload_unsupported(pending_probe_split_payload_descriptor_blocker,
				                                               pending_probe_input.size());
				return false;
			}
			if (runtime.PendingSourceContractBatch() && flush_projected_batch()) {
				return true;
			}
			if (!pending_probe_group_key_batch_initialized) {
				pending_probe_group_key_batch.Initialize(runtime.GetAllocator(), pending_probe_group_key_types);
				pending_probe_group_key_batch_initialized = true;
			}
			pending_probe_group_key_batch.Reset();
			auto &join_output = scratch.TemporaryChunk(hash_join_idx);
			join_output.Reset();
			join_output.SetChildCardinality(pending_probe_input.size());
			auto group_projection_map = optional_ptr<const vector<idx_t>>(&pending_probe_group_projection_indices);
			if (!TryDirectMaterializeHashJoinProjectionSourcesToBatch(
			        runtime, scratch, hash_join_idx, final_projection_idx, final_projection_op, pending_probe_input,
			        *FlatVector::IncrementalSelectionVector(), pending_probe_row_pointers, join_output,
			        pending_probe_group_key_batch, group_projection_map,
			        optional_ptr<Vector>(&pending_probe_group_key_hashes))) {
				record_pending_probe_split_payload_unsupported("group_key_projection", pending_probe_input.size());
				return false;
			}

			auto bind_stage_start = SljitRegionStageStart(runtime);
			bool bound = false;
			auto &binding =
			    BindNativeSink(native_runtime, scratch, aggregate_idx, pending_probe_group_key_batch,
			                   aggregate_op.aggregate_update.plan.sink_info, "aggregate-update-runtime-binding-failed",
			                   "SLJIT aggregate update sink", bound);
			if (bound) {
				RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind, "bind_sink_contract",
				                              bind_stage_start);
			}
			if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.grouped_state.ready ||
			    !binding.aggregate_update.grouped_state.state || !binding.aggregate_update.primitive.ready) {
				record_pending_probe_split_payload_unsupported("sink_binding", pending_probe_input.size());
				return false;
			}
			auto &aggregates = aggregate_op.aggregate_update.plan.sink_info.aggregates;
			auto &payload_lanes =
			    scratch.AggregatePayloadLanes(aggregate_idx, aggregates, binding.aggregate_update.primitive);
			const bool finish = false;
			auto stage_start = SljitRegionStageStart(runtime);
			bool updated = false;
			if (runtime.TraceRuntime()) {
				auto stage_name =
				    SljitRegionStageName(aggregate_idx, aggregate_op.kind, "direct_projected_group_payload_update");
				SljitRegionStageRecorder recorder(runtime, stage_name);
				updated = binding.aggregate_update.grouped_state.state->TryUpdateNewGroupsWithPayloadInput(
				    pending_probe_group_key_batch, pending_probe_input, pending_probe_split_payload_sources,
				    aggregate_op.aggregate_update.plan.sink_info, payload_lanes, &recorder, finish,
				    optional_ptr<Vector>(&pending_probe_group_key_hashes));
				auto runtime_us = SljitRegionElapsedMicros(stage_start);
				auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
				if (unattributed_runtime_us > 0) {
					runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
				}
			} else {
				updated = binding.aggregate_update.grouped_state.state->TryUpdateNewGroupsWithPayloadInput(
				    pending_probe_group_key_batch, pending_probe_input, pending_probe_split_payload_sources,
				    aggregate_op.aggregate_update.plan.sink_info, payload_lanes, nullptr, finish,
				    optional_ptr<Vector>(&pending_probe_group_key_hashes));
			}
			if (!updated) {
				RecordSljitRegionStageRuntimePath(runtime, aggregate_idx, aggregate_op.kind,
				                                  "direct_projected_group_payload_update_miss", stage_start);
				return false;
			}
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "direct_projected_group_payload_update", stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, aggregate_op.kind, "projected_group_payload_update",
			                                         pending_probe_input.size());
			RecordSljitRegionRuntimePath(runtime, aggregate_op.kind, "direct_projected_group_payload_update",
			                             pending_probe_input.size());
			MarkDeferredGroupedFinish(true, optional_ptr<bool>(&deferred_grouped_finish));
			handled = true;
			SljitChargeDownstreamRows(processed_output_rows, pending_probe_input.size());
			reset_pending_probe_batch();
			return false;
		};

		auto flush_pending_probe_batch = [&]() -> bool {
			if (!pending_probe_input_initialized || pending_probe_input.size() == 0) {
				return false;
			}
			bool row_pointer_grouped_update = false;
			if (try_execute_pending_row_pointer_grouped_update(row_pointer_grouped_update)) {
				return true;
			}
			if (row_pointer_grouped_update) {
				return false;
			}
			bool split_payload_update = false;
			if (try_execute_pending_probe_split_payload_update(split_payload_update)) {
				return true;
			}
			if (split_payload_update) {
				return false;
			}
			auto &join_output = scratch.TemporaryChunk(hash_join_idx);
			auto &projected = scratch.TemporaryChunk(final_projection_idx);
			join_output.Reset();
			join_output.SetChildCardinality(pending_probe_input.size());
			bool direct_projected = false;
			if (append_direct_projected_batch_from_probe(pending_probe_input, *FlatVector::IncrementalSelectionVector(),
			                                             pending_probe_row_pointers, join_output, direct_projected)) {
				return true;
			}
			if (!direct_projected) {
				MaterializeSelectionOnlyHashJoinProbeOutput(
				    runtime, scratch, hash_join_idx, hash_join_op, pending_probe_input,
				    *FlatVector::IncrementalSelectionVector(), scratch.HashJoinBuildSelection(hash_join_idx),
				    pending_probe_row_pointers, join_output.size(), join_output);
				if (append_direct_projected_batch(join_output, direct_projected)) {
					return true;
				}
				if (!direct_projected) {
					project_join_output(scratch, join_output, projected);
					if (append_projected_batch(projected)) {
						return true;
					}
				}
			}
			reset_pending_probe_batch();
			return false;
		};

		auto append_pending_probe_batch = [&](DataChunk &join_input, SelectionVector &match_selection,
		                                      Vector &row_pointers, idx_t count, bool &handled) -> bool {
			handled = false;
			if (count == 0) {
				return false;
			}
			if (count == STANDARD_VECTOR_SIZE &&
			    (!pending_probe_input_initialized || pending_probe_input.size() == 0)) {
				return false;
			}
			if (!pending_probe_input_initialized) {
				pending_probe_input.Initialize(runtime.GetAllocator(), join_input.GetTypes());
				pending_probe_input_initialized = true;
				reset_pending_probe_batch();
			}
			if (pending_probe_input.ColumnCount() != join_input.ColumnCount()) {
				return false;
			}
			if (pending_probe_input.size() + count > STANDARD_VECTOR_SIZE) {
				if (flush_pending_probe_batch()) {
					return true;
				}
			}
			if (count == STANDARD_VECTOR_SIZE && pending_probe_input.size() == 0) {
				return false;
			}
			if (!SljitAppendSelectedProbeBatch(runtime, hash_join_idx, hash_join_op.kind, join_input, match_selection,
			                                   row_pointers, count, pending_probe_input, pending_probe_row_pointers)) {
				return false;
			}
			handled = true;
			if (pending_probe_input.size() == STANDARD_VECTOR_SIZE) {
				if (flush_pending_probe_batch()) {
					return true;
				}
			}
			return false;
		};

		auto execute_join_input_batch = [&](DataChunk &join_input) -> bool {
			if (join_input.size() == 0) {
				return false;
			}
			SljitHashJoinProbeDrainState state;
			auto &join_output = scratch.TemporaryChunk(hash_join_idx);
			auto &projected = scratch.TemporaryChunk(final_projection_idx);
			do {
				join_output.Reset();
				string deferred_reason;
				auto stage_start = SljitRegionStageStart(runtime);
				auto bind_result = ExecuteNativeHashJoinProbe(
				    runtime, native_runtime, scratch, hash_join_idx, hash_join_op, join_input, join_output,
				    scratch.FilterSelection(hash_join_idx), scratch.HashJoinBuildSelection(hash_join_idx),
				    scratch.HashJoinRowPointers(hash_join_idx), scratch.HashJoinSourceFormats(hash_join_idx),
				    scratch.HashJoinSourceData(hash_join_idx), scratch.HashJoinSourceSelections(hash_join_idx),
				    scratch.HashJoinSourceValidity(hash_join_idx),
				    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(hash_join_idx)
				                                                         : nullptr,
				    hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualSelection(hash_join_idx)
				        : nullptr,
				    hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(hash_join_idx)
				        : nullptr,
				    hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualRowPointers(hash_join_idx)
				        : nullptr,
				    state, deferred_reason, source_key0_int64_to_int32_unchecked, true);
				RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					if (flush_pending_probe_batch()) {
						return true;
					}
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (join_output.size() == 0) {
					continue;
				}
				bool pending_buffered = false;
				if (append_pending_probe_batch(join_input, scratch.FilterSelection(hash_join_idx),
				                               scratch.HashJoinRowPointers(hash_join_idx), join_output.size(),
				                               pending_buffered)) {
					return true;
				}
				if (pending_buffered) {
					continue;
				}
				bool direct_projected = false;
				if (append_direct_projected_batch_from_probe(join_input, scratch.FilterSelection(hash_join_idx),
				                                             scratch.HashJoinRowPointers(hash_join_idx), join_output,
				                                             direct_projected)) {
					return true;
				}
				if (direct_projected) {
					continue;
				}
				MaterializeSelectionOnlyHashJoinProbeOutput(
				    runtime, scratch, hash_join_idx, hash_join_op, join_input, scratch.FilterSelection(hash_join_idx),
				    scratch.HashJoinBuildSelection(hash_join_idx), scratch.HashJoinRowPointers(hash_join_idx),
				    join_output.size(), join_output);
				if (append_direct_projected_batch(join_output, direct_projected)) {
					return true;
				}
				if (direct_projected) {
					continue;
				}
				project_join_output(scratch, join_output, projected);
				if (append_projected_batch(projected)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
			return false;
		};

		auto flush_join_input_batch = [&]() -> bool {
			if (!pending_join_input_initialized || pending_join_input.size() == 0) {
				return false;
			}
			if (execute_join_input_batch(pending_join_input)) {
				return true;
			}
			pending_join_input.Reset();
			return false;
		};

		auto append_join_input_batch = [&](DataChunk &join_input) -> bool {
			if (join_input.size() == 0) {
				return false;
			}
			if (!pending_join_input_initialized) {
				pending_join_input.Initialize(runtime.GetAllocator(), join_input.GetTypes());
				pending_join_input_initialized = true;
			}
			if (pending_join_input.ColumnCount() != join_input.ColumnCount()) {
				if (flush_join_input_batch()) {
					return true;
				}
				return execute_join_input_batch(join_input);
			}
			if (pending_join_input.size() + join_input.size() > STANDARD_VECTOR_SIZE) {
				if (flush_join_input_batch()) {
					return true;
				}
			}
			if (join_input.size() == STANDARD_VECTOR_SIZE && pending_join_input.size() == 0) {
				return execute_join_input_batch(join_input);
			}
			auto append_stage_start = SljitRegionStageStart(runtime);
			if (!SljitTryFastAppendFixedFlatAllValid(pending_join_input, join_input)) {
				pending_join_input.Append(join_input);
			}
			RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, "source_input_batch_append",
			                              append_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "source_input_batch",
			                                         join_input.size());
			if (pending_join_input.size() == STANDARD_VECTOR_SIZE) {
				if (flush_join_input_batch()) {
					return true;
				}
			}
			return false;
		};

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			DataChunk *join_input = nullptr;
			auto source_result = have_more_output ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
			if (prepare_join_input(scratch, source_chunk, source_result, join_input)) {
				if (flush_join_input_batch() || flush_pending_probe_batch()) {
					return true;
				}
				return true;
			}
			if (!join_input) {
				return false;
			}
			return append_join_input_batch(*join_input);
		};

		while (true) {
			if (SljitDownstreamRowBudgetReached(processed_output_rows, runtime.MaxChunks()) ||
			    fetched_chunks >= max_source_fetches) {
				if (flush_join_input_batch() || flush_pending_probe_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::NOT_FINISHED;
				return true;
			}

			DataChunk *source_chunk = nullptr;
			auto source_result = runtime.FetchSourceContract(source_chunk);
			if (source_result == SourceResultType::BLOCKED) {
				if (flush_join_input_batch() || flush_pending_probe_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::INTERRUPTED;
				return true;
			}
			fetched_chunks++;

			if (source_chunk && source_chunk->size() > 0 &&
			    execute_source_chunk(*source_chunk, source_result == SourceResultType::HAVE_MORE_OUTPUT)) {
				return true;
			}
			if (source_result == SourceResultType::FINISHED) {
				if (flush_join_input_batch() || flush_pending_probe_batch() || flush_projected_batch()) {
					return true;
				}
				finish_deferred_grouped_update();
				result = ExecutionRegionResult::FINISHED;
				return true;
			}
		}
	}

	SinkResultType ExecuteNativeFullPipeline(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                         DataChunk &input) {
		return ExecuteNativeFullPipelineFrom(runtime, scratch, 0, input);
	}

	bool TryExecuteHashJoinFilteredUngroupedAggregateUpdate(ExecutionRegionRuntime &runtime,
	                                                        ExecutionOperatorRuntime &native_runtime,
	                                                        SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                                        SljitExecutableRegionOp &hash_join_op,
	                                                        DataChunk &join_input, SinkResultType &sink_result) {
		if (hash_join_op.kind != SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
			return false;
		}
		const bool has_post_join_filter =
		    hash_join_idx + 1 < ops.size() && ops[hash_join_idx + 1].kind == SljitNativeRegionOpKind::FILTER;
		const auto first_projection_idx = has_post_join_filter ? hash_join_idx + 2 : hash_join_idx + 1;
		if (!CanExecuteHashJoinUngroupedAggregateUpdate(hash_join_idx, first_projection_idx)) {
			return false;
		}
		const auto filter_idx = hash_join_idx + 1;
		const auto aggregate_idx = ops.size() - 1;
		auto &aggregate_op = ops[aggregate_idx];
		auto &join_selection_output = scratch.TemporaryChunk(hash_join_idx);
		auto &match_selection = scratch.FilterSelection(hash_join_idx);
		auto &build_selection = scratch.HashJoinBuildSelection(hash_join_idx);
		auto &row_pointers = scratch.HashJoinRowPointers(hash_join_idx);
		auto &compact_match_selection = build_selection;
		SljitHashJoinProbeDrainState state;
		bool updated_aggregate = false;
		auto fail = [&](const char *reason) -> bool {
			auto path = string("direct_filtered_ungrouped_aggregate_unsupported.") + reason;
			RecordSljitRegionRuntimePath(runtime, hash_join_op.kind, path.c_str());
			if (updated_aggregate || hash_join_op.hash_join_probe.plan.mark_build_match) {
				throw InternalException(
				    "SLJIT direct filtered aggregate route failed after entering stateful join/aggregate path: %s",
				    reason);
			}
			return false;
		};

		do {
			join_selection_output.Reset();
			string deferred_reason;
			auto probe_stage_start = SljitRegionStageStart(runtime);
			auto bind_result = ExecuteNativeHashJoinProbe(
			    runtime, native_runtime, scratch, hash_join_idx, hash_join_op, join_input, join_selection_output,
			    match_selection, build_selection, row_pointers, scratch.HashJoinSourceFormats(hash_join_idx),
			    scratch.HashJoinSourceData(hash_join_idx), scratch.HashJoinSourceSelections(hash_join_idx),
			    scratch.HashJoinSourceValidity(hash_join_idx),
			    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(hash_join_idx)
			                                                         : nullptr,
			    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(hash_join_idx)
			                                                         : nullptr,
			    hash_join_op.hash_join_probe.plan.residual_predicate
			        ? &scratch.HashJoinResidualMatchSelection(hash_join_idx)
			        : nullptr,
			    hash_join_op.hash_join_probe.plan.residual_predicate
			        ? &scratch.HashJoinResidualRowPointers(hash_join_idx)
			        : nullptr,
			    state, deferred_reason, false, true);
			RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, probe_stage_start);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				runtime.Defer(std::move(deferred_reason));
				sink_result = SinkResultType::BLOCKED;
				return true;
			}

			const auto match_count = join_selection_output.size();
			if (match_count == 0) {
				continue;
			}
			if (!scratch.HasOperatorBinding(hash_join_idx)) {
				return fail("operator_binding");
			}
			auto &join_binding = scratch.OperatorBinding(hash_join_idx).hash_join_probe;
			if (!join_binding.ready ||
			    join_binding.layout_kind != ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE ||
			    !join_binding.hash_table) {
				return fail("non_regular_hash_table");
			}

			const SelectionVector *selected_row_indices = FlatVector::IncrementalSelectionVector();
			auto selected_count = match_count;
			if (has_post_join_filter) {
				auto &filter_op = ops[filter_idx];
				SljitExecutableRegionExpression remapped_filter;
				DataChunk filter_input;
				vector<Vector> filter_sources;
				auto filter_input_stage_start = SljitRegionStageStart(runtime);
				if (!TryBuildHashJoinProjectionExpressionInput(join_binding, filter_op.filter, join_input,
				                                               match_selection, row_pointers, match_count,
				                                               remapped_filter, filter_input, filter_sources)) {
					return fail("filter_input");
				}
				RecordSljitRegionStageRuntime(runtime, filter_idx, filter_op.kind, "direct_hash_join_filter_input",
				                              filter_input_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, hash_join_op.kind, "filtered_expression_input",
				                                         match_count);

				auto filter_stage_start = SljitRegionStageStart(runtime);
				auto &filter_selection = scratch.FilterSelection(filter_idx);
				selected_count = SelectExpression(remapped_filter, filter_input, filter_selection,
				                                  scratch.ExpressionAdapterScratch(filter_idx, 0));
				selected_row_indices = &filter_selection;
				RecordSljitRegionStageRuntime(runtime, filter_idx, filter_op.kind, "direct_hash_join_selection",
				                              filter_stage_start);
			}
			if (selected_count == 0) {
				continue;
			}

			DataChunk payload_input;
			vector<Vector> payload_sources;
			vector<SljitExecutableRegionExpression> remapped_payloads;
			auto payload_input_stage_start = SljitRegionStageStart(runtime);
			if (!TryPrepareHashJoinFilteredUngroupedPayloadInput(
			        join_binding, join_input, match_selection, row_pointers, *selected_row_indices, selected_count,
			        hash_join_idx, first_projection_idx, aggregate_idx, aggregate_op, payload_input, payload_sources,
			        remapped_payloads, compact_match_selection)) {
				return fail("payload_input");
			}
			RecordSljitRegionStageRuntime(runtime, aggregate_idx, aggregate_op.kind,
			                              "direct_hash_join_filtered_payload_input", payload_input_stage_start);

			updated_aggregate = true;
			sink_result = ExecuteNativeUngroupedAggregateUpdateWithRemappedPayloads(
			    runtime, native_runtime, scratch, aggregate_idx, aggregate_op, payload_input, remapped_payloads);
			if (sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED) {
				return true;
			}
		} while (!HashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));

		sink_result = SinkResultType::NEED_MORE_INPUT;
		return true;
	}

	SinkResultType ExecuteNativeFullPipelineFrom(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                             idx_t start_op_idx, DataChunk &input) {
		if (input.size() == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}

		auto &native_runtime = runtime.ExecutionOperators();
		DataChunk *current = &input;
		for (idx_t op_idx = start_op_idx; op_idx < ops.size(); op_idx++) {
			auto &op = ops[op_idx];
			if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT hash join build sink must be the final full pipeline operator");
				}
				auto sink_result = ExecuteNativeHashJoinBuild(
				    runtime, native_runtime, scratch, op_idx, op, *current, scratch.HashJoinBuildSourceChunk(op_idx),
				    scratch.HashJoinBuildHashValues(op_idx), scratch.HashJoinBuildSelection(op_idx));
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::NESTED_LOOP_JOIN_BUILD) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException(
					    "SLJIT nested loop join build sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result = ExecuteNativeNestedLoopJoinBuild(native_runtime, scratch, op_idx, op, *current,
				                                                    scratch.NestedLoopConditionChunk(op_idx));
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::ORDER_SINK) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT ordered sink must be the final full pipeline operator");
				}
				auto sink_result =
				    ExecuteNativeOrderSink(runtime, native_runtime, scratch, op_idx, op, *current,
				                           scratch.OrderKeyChunk(op_idx), scratch.OrderPayloadChunk(op_idx));
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::APPEND_SINK) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT append sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result = ExecuteNativeAppendSink(native_runtime, scratch, op_idx, op, *current);
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::DELIM_JOIN_SINK) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT delimiter join sink must be the final full pipeline operator");
				}
				auto stage_start = SljitRegionStageStart(runtime);
				auto sink_result = ExecuteNativeDelimJoinSink(native_runtime, scratch, op_idx, op, *current);
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
				if (op_idx + 1 != ops.size()) {
					throw InternalException("SLJIT aggregate update sink must be the final full pipeline operator");
				}
				auto sink_result = ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, op_idx, op, *current);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (CanExecuteGeneratedFilterAggregateUpdate(op_idx)) {
				auto &aggregate_op = ops[op_idx + 1];
				auto sink_result = ExecuteNativeFilteredAggregateUpdate(runtime, native_runtime, scratch, op_idx + 1,
				                                                        aggregate_op, *current);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (CanExecuteFilterAggregateUpdate(op_idx)) {
				auto &aggregate_op = ops[op_idx + 1];
				auto &filter_selection = scratch.FilterSelection(op_idx);
				auto filter_stage_start = SljitRegionStageStart(runtime);
				auto selected_count =
				    SelectFilter(op, *current, filter_selection, scratch.ExpressionAdapterScratch(op_idx, 0));
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "selection", filter_stage_start);
				if (selected_count == 0) {
					return SinkResultType::NEED_MORE_INPUT;
				}
				auto sink_result =
				    ExecuteNativeAggregateUpdate(runtime, native_runtime, scratch, op_idx + 1, aggregate_op, *current,
				                                 &filter_selection, selected_count);
				return native_runtime.RecordSinkResult(*current, sink_result);
			}
			if (CanExecuteFilterProjection(op_idx)) {
				auto &output = scratch.TemporaryChunk(op_idx + 1);
				output.Reset();
				auto stage_start = SljitRegionStageStart(runtime);
				ExecuteFilterProjection(scratch, op, ops[op_idx + 1], op_idx + 1, *current, output,
				                        scratch.FilterSelection(op_idx));
				RecordSljitRegionStageRuntimeWithSuffix(runtime, op_idx, op.kind, "+projection", stage_start);
				current = &output;
				op_idx++;
				if (current->size() == 0) {
					return SinkResultType::NEED_MORE_INPUT;
				}
				continue;
			}
			SinkResultType direct_append_result;
			if (TryExecuteProjectionDirectAppend(runtime, native_runtime, scratch, op_idx, op, *current,
			                                     direct_append_result)) {
				if (direct_append_result == SinkResultType::BLOCKED && !runtime.DeferredReason().empty()) {
					return direct_append_result;
				}
				return native_runtime.RecordSinkResult(*current, direct_append_result);
			}
			SinkResultType direct_hash_join_filtered_aggregate_result;
			if (TryExecuteHashJoinFilteredUngroupedAggregateUpdate(runtime, native_runtime, scratch, op_idx, op,
			                                                       *current,
			                                                       direct_hash_join_filtered_aggregate_result)) {
				return direct_hash_join_filtered_aggregate_result;
			}
			auto &output = scratch.TemporaryChunk(op_idx);
			output.Reset();
			switch (op.kind) {
			case SljitNativeRegionOpKind::FILTER: {
				auto stage_start = SljitRegionStageStart(runtime);
				ExecuteFilter(op, *current, output, scratch.FilterSelection(op_idx),
				              scratch.ExpressionAdapterScratch(op_idx, 0));
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
				break;
			}
			case SljitNativeRegionOpKind::PROJECTION: {
				auto stage_start = SljitRegionStageStart(runtime);
				ExecuteProjection(scratch, op_idx, op, *current, output);
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
				break;
			}
			case SljitNativeRegionOpKind::HASH_JOIN_PROBE: {
				return DrainNativeHashJoinProbe(runtime, scratch, op_idx, *current, output);
			}
			case SljitNativeRegionOpKind::NESTED_LOOP_JOIN_PROBE: {
				return DrainNativeNestedLoopJoinProbe(runtime, scratch, op_idx, *current, output);
			}
			default:
				throw InternalException("Invalid SLJIT full pipeline operator before sink");
			}
			current = &output;
			if (current->size() == 0) {
				return SinkResultType::NEED_MORE_INPUT;
			}
		}
		throw InternalException("SLJIT full pipeline region has no native sink operator");
	}

	bool TryExecuteHashJoinProbeDirectHashJoinBuild(ExecutionRegionRuntime &runtime,
	                                                ExecutionOperatorRuntime &native_runtime,
	                                                SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                                SljitExecutableRegionOp &hash_join_op, DataChunk &join_input,
	                                                DataChunk &join_output, SinkResultType &sink_result) {
		if (hash_join_op.hash_join_probe.plan.output_mode !=
		        ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
		    hash_join_op.hash_join_probe.plan.output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
			return false;
		}

		idx_t projection_idx = DConstants::INVALID_INDEX;
		idx_t sink_idx = DConstants::INVALID_INDEX;
		if (hash_join_idx + 1 < ops.size() && ops[hash_join_idx + 1].kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD &&
		    hash_join_idx + 2 == ops.size()) {
			sink_idx = hash_join_idx + 1;
		} else if (hash_join_idx + 2 < ops.size() &&
		           ops[hash_join_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION &&
		           ops[hash_join_idx + 2].kind == SljitNativeRegionOpKind::HASH_JOIN_BUILD &&
		           hash_join_idx + 3 == ops.size()) {
			projection_idx = hash_join_idx + 1;
			sink_idx = hash_join_idx + 2;
		} else {
			return false;
		}

		auto &sink_op = ops[sink_idx];
		auto sink_input_column_count = projection_idx == DConstants::INVALID_INDEX
		                                   ? hash_join_op.output_types.size()
		                                   : ops[projection_idx].output_types.size();
		vector<uint8_t> required_columns;
		if (!BuildHashJoinBuildRequiredInputColumns(sink_op.hash_join_build.plan.sink_info, sink_input_column_count,
		                                            required_columns)) {
			return false;
		}
		if (projection_idx == DConstants::INVALID_INDEX && !RequiredColumnsAreStrictSubset(required_columns)) {
			return false;
		}

		SljitHashJoinProbeDrainState state;
		auto &match_selection = scratch.FilterSelection(hash_join_idx);
		auto &build_selection = scratch.HashJoinBuildSelection(hash_join_idx);
		auto &row_pointers = scratch.HashJoinRowPointers(hash_join_idx);
		do {
			join_output.Reset();
			string deferred_reason;
			auto stage_start = SljitRegionStageStart(runtime);
			auto bind_result = ExecuteNativeHashJoinProbe(
			    runtime, native_runtime, scratch, hash_join_idx, hash_join_op, join_input, join_output, match_selection,
			    build_selection, row_pointers, scratch.HashJoinSourceFormats(hash_join_idx),
			    scratch.HashJoinSourceData(hash_join_idx), scratch.HashJoinSourceSelections(hash_join_idx),
			    scratch.HashJoinSourceValidity(hash_join_idx),
			    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(hash_join_idx)
			                                                         : nullptr,
			    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(hash_join_idx)
			                                                         : nullptr,
			    hash_join_op.hash_join_probe.plan.residual_predicate
			        ? &scratch.HashJoinResidualMatchSelection(hash_join_idx)
			        : nullptr,
			    hash_join_op.hash_join_probe.plan.residual_predicate
			        ? &scratch.HashJoinResidualRowPointers(hash_join_idx)
			        : nullptr,
			    state, deferred_reason, false, true);
			RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, stage_start);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				runtime.Defer(std::move(deferred_reason));
				sink_result = SinkResultType::BLOCKED;
				return true;
			}
			if (join_output.size() == 0) {
				continue;
			}

			DataChunk *sink_input = nullptr;
			if (projection_idx == DConstants::INVALID_INDEX) {
				if (TryMaterializeHashJoinRequiredSources(runtime, scratch, hash_join_idx, hash_join_op, join_input,
				                                          match_selection, build_selection, row_pointers,
				                                          join_output.size(), required_columns, join_output)) {
					sink_input = &join_output;
					RecordSljitRegionRuntimePath(runtime, hash_join_op.kind, "direct_required_hash_build_sources",
					                             join_output.size());
				}
			} else {
				auto &projected = scratch.TemporaryChunk(projection_idx);
				projected.Reset();
				if (TryMaterializeHashJoinRequiredProjectionViews(runtime, scratch, hash_join_idx, projection_idx,
				                                                  ops[projection_idx], join_input, match_selection,
				                                                  build_selection, row_pointers, join_output.size(),
				                                                  required_columns, projected)) {
					sink_input = &projected;
					RecordSljitRegionRuntimePath(runtime, hash_join_op.kind, "direct_projected_hash_build_views",
					                             projected.size());
				}
			}

			if (!sink_input) {
				if (!MaterializeSelectionOnlyHashJoinProbeOutput(runtime, scratch, hash_join_idx, hash_join_op,
				                                                 join_input, match_selection, build_selection,
				                                                 row_pointers, join_output.size(), join_output)) {
					return false;
				}
				sink_result = ExecuteNativeFullPipelineFrom(runtime, scratch, hash_join_idx + 1, join_output);
			} else {
				auto build_result = ExecuteNativeHashJoinBuild(runtime, native_runtime, scratch, sink_idx, sink_op,
				                                               *sink_input, scratch.HashJoinBuildSourceChunk(sink_idx),
				                                               scratch.HashJoinBuildHashValues(sink_idx),
				                                               scratch.HashJoinBuildSelection(sink_idx));
				sink_result = native_runtime.RecordSinkResult(*sink_input, build_result);
			}
			if (sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED) {
				return true;
			}
		} while (!HashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
		sink_result = SinkResultType::NEED_MORE_INPUT;
		return true;
	}

	SinkResultType DrainNativeHashJoinProbe(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                        idx_t op_idx, DataChunk &input, DataChunk &output) {
		auto &op = ops[op_idx];
		auto &native_runtime = runtime.ExecutionOperators();
		SinkResultType direct_hash_build_result;
		if (TryExecuteHashJoinProbeDirectHashJoinBuild(runtime, native_runtime, scratch, op_idx, op, input, output,
		                                               direct_hash_build_result)) {
			return direct_hash_build_result;
		}
		SljitHashJoinProbeDrainState state;
		do {
			output.Reset();
			string deferred_reason;
			auto stage_start = SljitRegionStageStart(runtime);
			auto bind_result = ExecuteNativeHashJoinProbe(
			    runtime, native_runtime, scratch, op_idx, op, input, output, scratch.FilterSelection(op_idx),
			    scratch.HashJoinBuildSelection(op_idx), scratch.HashJoinRowPointers(op_idx),
			    scratch.HashJoinSourceFormats(op_idx), scratch.HashJoinSourceData(op_idx),
			    scratch.HashJoinSourceSelections(op_idx), scratch.HashJoinSourceValidity(op_idx),
			    op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(op_idx) : nullptr,
			    op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(op_idx) : nullptr,
			    op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualMatchSelection(op_idx) : nullptr,
			    op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(op_idx) : nullptr,
			    state, deferred_reason);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				runtime.Defer(std::move(deferred_reason));
				return SinkResultType::BLOCKED;
			}
			if (output.size() == 0) {
				continue;
			}
			auto sink_result = ExecuteNativeFullPipelineFrom(runtime, scratch, op_idx + 1, output);
			if (sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED) {
				return sink_result;
			}
		} while (!HashJoinProbeDrainFinished(op.hash_join_probe.plan.output_mode, state));
		return SinkResultType::NEED_MORE_INPUT;
	}

	SinkResultType DrainNativeNestedLoopJoinProbe(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                              idx_t op_idx, DataChunk &input, DataChunk &output) {
		auto &op = ops[op_idx];
		SljitNestedLoopJoinProbeDrainState state;
		auto &native_runtime = runtime.ExecutionOperators();
		auto &left_condition = scratch.NestedLoopLeftConditionChunk(op_idx);
		do {
			output.Reset();
			string deferred_reason;
			auto stage_start = SljitRegionStageStart(runtime);
			auto bind_result =
			    ExecuteNativeNestedLoopJoinProbe(native_runtime, scratch, op_idx, op, input, left_condition, output,
			                                     scratch.NestedLoopLeftSelection(op_idx),
			                                     scratch.NestedLoopRightSelection(op_idx), state, deferred_reason);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_start);
			if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
				runtime.Defer(std::move(deferred_reason));
				return SinkResultType::BLOCKED;
			}
			if (output.size() == 0) {
				continue;
			}
			auto sink_result = ExecuteNativeFullPipelineFrom(runtime, scratch, op_idx + 1, output);
			if (sink_result == SinkResultType::BLOCKED || sink_result == SinkResultType::FINISHED) {
				return sink_result;
			}
		} while (!state.finished);
		return SinkResultType::NEED_MORE_INPUT;
	}

	static bool IsLeftHashJoinProbeOutputMode(ExecutionHashJoinProbeOutputMode output_mode) {
		return output_mode == ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD;
	}

	static bool HashJoinProbeDrainFinished(ExecutionHashJoinProbeOutputMode output_mode,
	                                       const SljitHashJoinProbeDrainState &state) {
		return state.finished && (!IsLeftHashJoinProbeOutputMode(output_mode) || state.left_unmatched_emitted);
	}

	static void InitializeLeftHashJoinProbeState(SljitHashJoinProbeDrainState &state, idx_t input_count) {
		if (state.left_initialized) {
			return;
		}
		state.found_match.assign(input_count, 0);
		state.left_initialized = true;
		state.left_unmatched_emitted = false;
	}

	static void MarkLeftHashJoinProbeMatches(SljitHashJoinProbeDrainState &state,
	                                         const SelectionVector &match_selection, idx_t count) {
		for (idx_t match_idx = 0; match_idx < count; match_idx++) {
			auto input_idx = match_selection.get_index(match_idx);
			if (input_idx >= state.found_match.size()) {
				throw InternalException("SLJIT native LEFT hash join match selection index out of range");
			}
			state.found_match[input_idx] = 1;
		}
	}

	static void MaterializeLeftHashJoinProbeUnmatched(const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
	                                                  DataChunk &output, SelectionVector &unmatched_selection,
	                                                  SljitHashJoinProbeDrainState &state,
	                                                  optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		InitializeLeftHashJoinProbeState(state, input.size());
		idx_t unmatched_count = 0;
		for (idx_t input_idx = 0; input_idx < input.size(); input_idx++) {
			if (state.found_match[input_idx]) {
				continue;
			}
			unmatched_selection.set_index(unmatched_count++, input_idx);
		}
		state.left_unmatched_emitted = true;
		if (unmatched_count == 0) {
			output.Reset();
			return;
		}
		ExecutionMaterializeHashJoinProbeLeftUnmatched(probe, input, unmatched_selection, unmatched_count, output,
		                                               recorder);
	}

	static idx_t SelectMarkProbeMatches(const SelectionVector &mark_flags, idx_t count, SelectionVector &selection) {
		idx_t selected_count = 0;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (mark_flags.get_index(row_idx) != 0) {
				selection.set_index(selected_count++, row_idx);
			}
		}
		return selected_count;
	}

	static bool TryReferenceMarkProbeFilterInput(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
	                                             idx_t count, DataChunk &output) {
		if (!binding.ready || binding.output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE ||
		    binding.correlated_mark_counts_required ||
		    output.ColumnCount() != binding.lhs_output_column_indices.size() + 1) {
			return false;
		}
		output.Reset();
		for (idx_t col_idx = 0; col_idx < binding.lhs_output_column_indices.size(); col_idx++) {
			auto input_col = binding.lhs_output_column_indices[col_idx];
			if (input_col >= input.ColumnCount() || input.data[input_col].GetType() != output.data[col_idx].GetType()) {
				return false;
			}
			output.data[col_idx].Reference(input.data[input_col]);
		}
		auto &mark_vector = output.data.back();
		if (mark_vector.GetType().id() != LogicalTypeId::BOOLEAN) {
			return false;
		}
		mark_vector.Reference(Value::BOOLEAN(true), count_t(count));
		output.SetChildCardinality(count);
		return true;
	}

	bool CanExecuteFilterProjection(idx_t op_idx) const {
		return op_idx + 1 < ops.size() && ops[op_idx].kind == SljitNativeRegionOpKind::FILTER &&
		       ops[op_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION;
	}

	bool CanExecuteFilterAggregateUpdate(idx_t op_idx) const {
		if (op_idx + 2 != ops.size()) {
			return false;
		}
		return ops[op_idx].kind == SljitNativeRegionOpKind::FILTER &&
		       ops[op_idx + 1].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[op_idx + 1].aggregate_update.plan.use_primitive_payloads;
	}

	bool CanExecuteGeneratedFilterAggregateUpdate(idx_t op_idx) const {
		return CanExecuteFilterAggregateUpdate(op_idx) &&
		       ops[op_idx + 1].aggregate_update.filtered_update.IsExecutable();
	}

	template <class T>
	static bool TryFastSelectFlatAllValidInclusiveExclusiveBetween(const SljitNativeRegionExpressionPlan &filter,
	                                                               DataChunk &input, SelectionVector &filter_selection,
	                                                               idx_t &selected_count) {
		if (filter.lower < static_cast<int64_t>(std::numeric_limits<T>::min()) ||
		    filter.upper > static_cast<int64_t>(std::numeric_limits<T>::max())) {
			return false;
		}
		if (filter.upper <= filter.lower) {
			selected_count = 0;
			return true;
		}

		UnifiedVectorFormat source_format;
		input.data[filter.source_index].ToUnifiedFormat(source_format);
		if (!SljitUnifiedFormatHasIdentitySelection(source_format)) {
			return false;
		}
		if (source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(input.size())) {
			return false;
		}

		using UNSIGNED_T = typename std::make_unsigned<T>::type;
		const auto lower = static_cast<UNSIGNED_T>(static_cast<T>(filter.lower));
		const auto upper = static_cast<UNSIGNED_T>(static_cast<T>(filter.upper));
		const auto width = static_cast<UNSIGNED_T>(upper - lower);
		const auto source_data = UnifiedVectorFormat::GetData<T>(source_format);
		auto result_data = filter_selection.data();

		selected_count = 0;
		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			const auto value = static_cast<UNSIGNED_T>(source_data[row_idx]);
			if (static_cast<UNSIGNED_T>(value - lower) < width) {
				result_data[selected_count++] = UnsafeNumericCast<sel_t>(row_idx);
			}
		}
		return true;
	}

	static bool TryFastSelectFlatAllValidIntegerBetween(const SljitNativeRegionExpressionPlan &filter, DataChunk &input,
	                                                    SelectionVector &filter_selection, idx_t &selected_count) {
		if (filter.kind != SljitNativeRegionExpressionKind::INTEGER_BETWEEN || filter.not_between ||
		    !filter.lower_inclusive || filter.upper_inclusive || filter.source_index >= input.ColumnCount()) {
			return false;
		}
		switch (filter.integer_kind) {
		case SljitNativeIntegerKind::INT32:
		case SljitNativeIntegerKind::DATE:
			return TryFastSelectFlatAllValidInclusiveExclusiveBetween<int32_t>(filter, input, filter_selection,
			                                                                   selected_count);
		default:
			return false;
		}
	}

	idx_t SelectExpression(SljitExecutableRegionExpression &expression, DataChunk &input,
	                       SelectionVector &filter_selection, SljitExpressionAdapterScratch &adapter_scratch) {
		auto &filter = expression.plan;
		if (filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
			auto &predicate_sources = adapter_scratch.predicate_sources;
			predicate_sources.Prepare(&input, expression.input_source_indices);

			SljitNativePredicateInput native_input;
			native_input.source_data = predicate_sources.source_data.data();
			native_input.source_sel = predicate_sources.source_sel.data();
			native_input.source_validity = predicate_sources.source_validity.data();
			native_input.sources_all_valid = predicate_sources.sources_all_valid;
			native_input.execute_sel = nullptr;
			native_input.result_data = nullptr;
			native_input.result_validity = nullptr;
			native_input.true_sel = filter_selection.data();
			native_input.false_sel = nullptr;
			native_input.selected_count = 0;
			native_input.count = input.size();
			expression.predicate_select_function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			return native_input.selected_count;
		}
		if (filter.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			if (!expression.select_function) {
				throw InternalException("SLJIT typed filter expression has no generated selector");
			}
			SljitNativeVectorInput native_input;
			adapter_scratch.PrepareExpressionTree(input, expression, native_input, nullptr, input.size());
			native_input.execute_sel = nullptr;
			native_input.true_sel = filter_selection.data();
			native_input.false_sel = nullptr;
			native_input.selected_count = 0;
			native_input.overflow_message = expression.overflow_message.c_str();
			native_input.query_location = filter.query_location;
			native_input.count = input.size();
			expression.select_function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			return native_input.selected_count;
		}
		D_ASSERT(filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT ||
		         filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
		         filter.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST ||
		         filter.kind == SljitNativeRegionExpressionKind::INTEGER_BETWEEN ||
		         filter.kind == SljitNativeRegionExpressionKind::NULL_CHECK);
		idx_t fast_selected_count;
		if (TryFastSelectFlatAllValidIntegerBetween(filter, input, filter_selection, fast_selected_count)) {
			return fast_selected_count;
		}
		UnifiedVectorFormat source_format;
		UnifiedVectorFormat right_source_format;
		input.data[filter.source_index].ToUnifiedFormat(source_format);
		if (filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES) {
			D_ASSERT(filter.right_source_index < input.ColumnCount());
			input.data[filter.right_source_index].ToUnifiedFormat(right_source_format);
		}

		SljitNativeVectorInput native_input;
		native_input.source_data = filter.kind == SljitNativeRegionExpressionKind::NULL_CHECK
		                               ? nullptr
		                               : NativeIntegerSourceData(source_format, filter.integer_kind);
		native_input.right_source_data = filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES
		                                     ? NativeIntegerSourceData(right_source_format, filter.integer_kind)
		                                     : nullptr;
		native_input.execute_sel = nullptr;
		native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
		native_input.right_source_sel =
		    filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES && right_source_format.sel
		        ? right_source_format.sel->data()
		        : nullptr;
		native_input.source_validity = source_format.validity.GetData();
		native_input.right_source_validity = filter.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES
		                                         ? right_source_format.validity.GetData()
		                                         : nullptr;
		native_input.constants = filter.constants.data();
		native_input.constant = filter.constant;
		native_input.result_data = nullptr;
		native_input.result_validity = nullptr;
		native_input.true_sel = filter_selection.data();
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
		native_input.overflow_message = nullptr;
		native_input.count = input.size();
		expression.select_function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}

		return native_input.selected_count;
	}

	idx_t SelectFilter(SljitExecutableRegionOp &op, DataChunk &input, SelectionVector &filter_selection,
	                   SljitExpressionAdapterScratch &adapter_scratch) {
		return SelectExpression(op.filter, input, filter_selection, adapter_scratch);
	}

	void ExecuteFilter(SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
	                   SelectionVector &filter_selection, SljitExpressionAdapterScratch &adapter_scratch) {
		auto selected_count = SelectFilter(op, input, filter_selection, adapter_scratch);
		if (selected_count == input.size()) {
			output.Reference(input);
		} else if (selected_count > 0) {
			output.Slice(input, filter_selection, selected_count);
		}
	}

	void ExecuteFilterProjection(SljitRegionExecutionScratch &scratch, SljitExecutableRegionOp &filter_op,
	                             SljitExecutableRegionOp &projection_op, idx_t projection_op_idx, DataChunk &input,
	                             DataChunk &output, SelectionVector &filter_selection) {
		auto &filter_scratch = scratch.ExpressionAdapterScratch(projection_op_idx - 1, 0);
		auto selected_count = SelectFilter(filter_op, input, filter_selection, filter_scratch);
		if (selected_count == 0) {
			output.Reset();
			return;
		}
		auto *execute_sel = selected_count == input.size() ? nullptr : &filter_selection;
		ExecuteProjection(scratch, projection_op_idx, projection_op, input, output, execute_sel, selected_count);
	}

	static bool AllValidSource(UnifiedVectorFormat &format, Vector &source, idx_t count) {
		source.ToUnifiedFormat(format);
		auto sel = SljitNormalizedSourceSelectionData(format);
		return SljitNormalizedSourceAllValid(format, sel, count);
	}

	static bool TryReadDirectReferenceProjectionSource(const SljitExecutableRegionExpression &expression,
	                                                   idx_t &source_index) {
		auto &plan = expression.plan;
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			source_index = plan.source_index;
			return true;
		}
		if (plan.kind != SljitNativeRegionExpressionKind::EXPRESSION_TREE &&
		    plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			return false;
		}
		if (!plan.expression_tree || plan.expression_tree->kind != ExecutionExpressionIRKind::REFERENCE ||
		    plan.return_type != plan.expression_tree->return_type) {
			return false;
		}
		auto &source_indices = expression.input_source_indices.empty() ? plan.expression_tree_source_indices
		                                                               : expression.input_source_indices;
		if (plan.expression_tree->ref_index >= source_indices.size()) {
			return false;
		}
		source_index = source_indices[plan.expression_tree->ref_index];
		return true;
	}

	template <class T>
	static void CopySelectedFixedValues(const_data_ptr_t source_data, const sel_t *source_sel,
	                                    const SelectionVector *filter_selection, idx_t source_offset, idx_t count,
	                                    data_ptr_t target_data, idx_t target_offset) {
		auto source = reinterpret_cast<const T *>(source_data);
		auto target = reinterpret_cast<T *>(target_data);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto logical_idx =
			    filter_selection ? filter_selection->get_index(source_offset + row_idx) : source_offset + row_idx;
			auto source_idx = source_sel ? source_sel[logical_idx] : logical_idx;
			target[target_offset + row_idx] = source[source_idx];
		}
	}

	static bool CopySelectedFixedValues(const LogicalType &type, const_data_ptr_t source_data, const sel_t *source_sel,
	                                    const SelectionVector *filter_selection, idx_t source_offset, idx_t count,
	                                    data_ptr_t target_data, idx_t target_offset) {
		switch (GetTypeIdSize(type.InternalType())) {
		case 1:
			CopySelectedFixedValues<uint8_t>(source_data, source_sel, filter_selection, source_offset, count,
			                                 target_data, target_offset);
			return true;
		case 2:
			CopySelectedFixedValues<uint16_t>(source_data, source_sel, filter_selection, source_offset, count,
			                                  target_data, target_offset);
			return true;
		case 4:
			CopySelectedFixedValues<uint32_t>(source_data, source_sel, filter_selection, source_offset, count,
			                                  target_data, target_offset);
			return true;
		case 8:
			CopySelectedFixedValues<uint64_t>(source_data, source_sel, filter_selection, source_offset, count,
			                                  target_data, target_offset);
			return true;
		case 16:
			CopySelectedFixedValues<hugeint_t>(source_data, source_sel, filter_selection, source_offset, count,
			                                   target_data, target_offset);
			return true;
		default:
			return false;
		}
	}

	template <class T, SljitNativeIntegerCompareOp COMPARE_OP>
	struct CompareNativeIntegerValues;

	template <class T>
	struct CompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::EQUAL> {
		static inline bool Operation(T left, T right) {
			return left == right;
		}
	};

	template <class T>
	struct CompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::NOT_EQUAL> {
		static inline bool Operation(T left, T right) {
			return left != right;
		}
	};

	template <class T>
	struct CompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::LESS_THAN> {
		static inline bool Operation(T left, T right) {
			return left < right;
		}
	};

	template <class T>
	struct CompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::GREATER_THAN> {
		static inline bool Operation(T left, T right) {
			return left > right;
		}
	};

	template <class T>
	struct CompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL> {
		static inline bool Operation(T left, T right) {
			return left <= right;
		}
	};

	template <class T>
	struct CompareNativeIntegerValues<T, SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL> {
		static inline bool Operation(T left, T right) {
			return left >= right;
		}
	};

	template <bool HAS_SELECTION>
	static inline idx_t SljitSourceIndex(const sel_t *sel, idx_t logical_idx) {
		return HAS_SELECTION ? sel[logical_idx] : logical_idx;
	}

	template <class T, class PROJECT_T, SljitNativeIntegerCompareOp COMPARE_OP, bool HAS_LEFT_SEL, bool HAS_RIGHT_SEL,
	          bool HAS_PROJECT_SEL>
	static idx_t CompactComparedReferenceValues(const_data_ptr_t left_data_p, const sel_t *left_sel,
	                                            const_data_ptr_t right_data_p, const sel_t *right_sel,
	                                            const_data_ptr_t project_data_p, const sel_t *project_sel, idx_t count,
	                                            idx_t source_offset, data_ptr_t target_data_p, idx_t target_offset) {
		auto left_data = reinterpret_cast<const T *>(left_data_p);
		auto right_data = reinterpret_cast<const T *>(right_data_p);
		auto project_data = reinterpret_cast<const PROJECT_T *>(project_data_p);
		auto target_data = reinterpret_cast<PROJECT_T *>(target_data_p);
		idx_t selected_count = 0;
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto logical_idx = source_offset + row_idx;
			auto left_idx = SljitSourceIndex<HAS_LEFT_SEL>(left_sel, logical_idx);
			auto right_idx = SljitSourceIndex<HAS_RIGHT_SEL>(right_sel, logical_idx);
			if (!CompareNativeIntegerValues<T, COMPARE_OP>::Operation(left_data[left_idx], right_data[right_idx])) {
				continue;
			}
			auto project_idx = SljitSourceIndex<HAS_PROJECT_SEL>(project_sel, logical_idx);
			target_data[target_offset + selected_count] = project_data[project_idx];
			selected_count++;
		}
		return selected_count;
	}

	template <class T, class PROJECT_T, bool HAS_LEFT_SEL, bool HAS_RIGHT_SEL, bool HAS_PROJECT_SEL>
	static idx_t CompactComparedReferenceValuesForSelections(const_data_ptr_t left_data, const sel_t *left_sel,
	                                                         const_data_ptr_t right_data, const sel_t *right_sel,
	                                                         const_data_ptr_t project_data, const sel_t *project_sel,
	                                                         idx_t count, idx_t source_offset,
	                                                         SljitNativeIntegerCompareOp compare_op,
	                                                         data_ptr_t target_data, idx_t target_offset) {
		switch (compare_op) {
		case SljitNativeIntegerCompareOp::EQUAL:
			return CompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::EQUAL, HAS_LEFT_SEL,
			                                      HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
			    target_data, target_offset);
		case SljitNativeIntegerCompareOp::NOT_EQUAL:
			return CompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::NOT_EQUAL, HAS_LEFT_SEL,
			                                      HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
			    target_data, target_offset);
		case SljitNativeIntegerCompareOp::LESS_THAN:
			return CompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::LESS_THAN, HAS_LEFT_SEL,
			                                      HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
			    target_data, target_offset);
		case SljitNativeIntegerCompareOp::GREATER_THAN:
			return CompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::GREATER_THAN, HAS_LEFT_SEL,
			                                      HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
			    target_data, target_offset);
		case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
			return CompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL,
			                                      HAS_LEFT_SEL, HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
			    target_data, target_offset);
		case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
			return CompactComparedReferenceValues<T, PROJECT_T, SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL,
			                                      HAS_LEFT_SEL, HAS_RIGHT_SEL, HAS_PROJECT_SEL>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
			    target_data, target_offset);
		default:
			throw InternalException("Unsupported SLJIT integer comparison op");
		}
	}

	template <class T, class PROJECT_T>
	static idx_t CompactComparedReferenceValuesForSelectionShape(
	    const_data_ptr_t left_data, const sel_t *left_sel, const_data_ptr_t right_data, const sel_t *right_sel,
	    const_data_ptr_t project_data, const sel_t *project_sel, idx_t count, idx_t source_offset,
	    SljitNativeIntegerCompareOp compare_op, data_ptr_t target_data, idx_t target_offset) {
		if (left_sel) {
			if (right_sel) {
				if (project_sel) {
					return CompactComparedReferenceValuesForSelections<T, PROJECT_T, true, true, true>(
					    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
					    compare_op, target_data, target_offset);
				}
				return CompactComparedReferenceValuesForSelections<T, PROJECT_T, true, true, false>(
				    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
				    compare_op, target_data, target_offset);
			}
			if (project_sel) {
				return CompactComparedReferenceValuesForSelections<T, PROJECT_T, true, false, true>(
				    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
				    compare_op, target_data, target_offset);
			}
			return CompactComparedReferenceValuesForSelections<T, PROJECT_T, true, false, false>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
		}
		if (right_sel) {
			if (project_sel) {
				return CompactComparedReferenceValuesForSelections<T, PROJECT_T, false, true, true>(
				    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset,
				    compare_op, target_data, target_offset);
			}
			return CompactComparedReferenceValuesForSelections<T, PROJECT_T, false, true, false>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
		}
		if (project_sel) {
			return CompactComparedReferenceValuesForSelections<T, PROJECT_T, false, false, true>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
		}
		return CompactComparedReferenceValuesForSelections<T, PROJECT_T, false, false, false>(
		    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
		    target_data, target_offset);
	}

	template <class T>
	static bool TryCompactComparedReferenceValuesForProjectType(
	    const LogicalType &project_type, const_data_ptr_t left_data, const sel_t *left_sel, const_data_ptr_t right_data,
	    const sel_t *right_sel, const_data_ptr_t project_data, const sel_t *project_sel, idx_t count,
	    idx_t source_offset, SljitNativeIntegerCompareOp compare_op, data_ptr_t target_data, idx_t target_offset,
	    idx_t &selected_count) {
		switch (GetTypeIdSize(project_type.InternalType())) {
		case 1:
			selected_count = CompactComparedReferenceValuesForSelectionShape<T, uint8_t>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
			return true;
		case 2:
			selected_count = CompactComparedReferenceValuesForSelectionShape<T, uint16_t>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
			return true;
		case 4:
			selected_count = CompactComparedReferenceValuesForSelectionShape<T, uint32_t>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
			return true;
		case 8:
			selected_count = CompactComparedReferenceValuesForSelectionShape<T, uint64_t>(
			    left_data, left_sel, right_data, right_sel, project_data, project_sel, count, source_offset, compare_op,
			    target_data, target_offset);
			return true;
		default:
			return false;
		}
	}

	static bool TryCompactComparedReferenceValues(SljitNativeIntegerKind compare_kind, const LogicalType &project_type,
	                                              const_data_ptr_t left_data, const sel_t *left_sel,
	                                              const_data_ptr_t right_data, const sel_t *right_sel,
	                                              const_data_ptr_t project_data, const sel_t *project_sel, idx_t count,
	                                              idx_t source_offset, SljitNativeIntegerCompareOp compare_op,
	                                              data_ptr_t target_data, idx_t target_offset, idx_t &selected_count) {
		switch (compare_kind) {
		case SljitNativeIntegerKind::INT8:
			return TryCompactComparedReferenceValuesForProjectType<int8_t>(
			    project_type, left_data, left_sel, right_data, right_sel, project_data, project_sel, count,
			    source_offset, compare_op, target_data, target_offset, selected_count);
		case SljitNativeIntegerKind::UINT8:
			return TryCompactComparedReferenceValuesForProjectType<uint8_t>(
			    project_type, left_data, left_sel, right_data, right_sel, project_data, project_sel, count,
			    source_offset, compare_op, target_data, target_offset, selected_count);
		case SljitNativeIntegerKind::INT32:
		case SljitNativeIntegerKind::DATE:
			return TryCompactComparedReferenceValuesForProjectType<int32_t>(
			    project_type, left_data, left_sel, right_data, right_sel, project_data, project_sel, count,
			    source_offset, compare_op, target_data, target_offset, selected_count);
		case SljitNativeIntegerKind::INT64:
		case SljitNativeIntegerKind::DECIMAL64:
			return TryCompactComparedReferenceValuesForProjectType<int64_t>(
			    project_type, left_data, left_sel, right_data, right_sel, project_data, project_sel, count,
			    source_offset, compare_op, target_data, target_offset, selected_count);
		default:
			return false;
		}
	}

	template <class FLUSH>
	bool TryAppendComparedReferenceProjectionBatch(ExecutionRegionRuntime &runtime, idx_t filter_idx,
	                                               SljitExecutableRegionOp &filter_op,
	                                               SljitExecutableRegionOp &projection_op,
	                                               SljitExecutableRegionOp &append_trace_op, DataChunk &source_chunk,
	                                               DataChunk &filtered_batch, bool &filtered_batch_initialized,
	                                               idx_t projection_source_index, FLUSH &&flush_filtered_batch,
	                                               bool &stopped) {
		stopped = false;
		auto &filter = filter_op.filter.plan;
		if (filter.kind != SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
		    filter.source_index >= source_chunk.ColumnCount() ||
		    filter.right_source_index >= source_chunk.ColumnCount()) {
			return false;
		}
		if (!filtered_batch_initialized) {
			filtered_batch.Initialize(runtime.GetAllocator(), projection_op.output_types);
			filtered_batch_initialized = true;
		}
		if (filtered_batch.ColumnCount() != 1 || filtered_batch.data[0].GetType() != projection_op.output_types[0]) {
			return false;
		}

		UnifiedVectorFormat left_format;
		UnifiedVectorFormat right_format;
		UnifiedVectorFormat project_format;
		auto &left = source_chunk.data[filter.source_index];
		auto &right = source_chunk.data[filter.right_source_index];
		auto &project = source_chunk.data[projection_source_index];
		if (!AllValidSource(left_format, left, source_chunk.size()) ||
		    !AllValidSource(right_format, right, source_chunk.size()) ||
		    !AllValidSource(project_format, project, source_chunk.size())) {
			return false;
		}
		auto left_data = NativeIntegerSourceData(left_format, filter.integer_kind);
		auto right_data = NativeIntegerSourceData(right_format, filter.integer_kind);
		auto project_data = project_format.data;
		auto left_sel = SljitNormalizedSourceSelectionData(left_format);
		auto right_sel = SljitNormalizedSourceSelectionData(right_format);
		auto project_sel = SljitNormalizedSourceSelectionData(project_format);

		auto stage_start = SljitRegionStageStart(runtime);
		idx_t appended_count = 0;
		idx_t source_offset = 0;
		while (source_offset < source_chunk.size()) {
			const auto current_size = filtered_batch.size();
			if (current_size == STANDARD_VECTOR_SIZE) {
				if (flush_filtered_batch()) {
					stopped = true;
					return true;
				}
				continue;
			}
			const auto append_capacity = STANDARD_VECTOR_SIZE - current_size;
			const auto input_count = MinValue<idx_t>(append_capacity, source_chunk.size() - source_offset);
			idx_t selected_count = 0;
			if (!TryCompactComparedReferenceValues(
			        filter.integer_kind, projection_op.output_types[0], left_data, left_sel, right_data, right_sel,
			        project_data, project_sel, input_count, source_offset, filter.compare_op,
			        FlatVector::GetDataMutable(filtered_batch.data[0]), current_size, selected_count)) {
				return false;
			}
			if (selected_count > 0) {
				FinishDirectProjectionBatchTargets(filtered_batch, current_size + selected_count);
				appended_count += selected_count;
			}
			source_offset += input_count;
			if (filtered_batch.size() == STANDARD_VECTOR_SIZE && flush_filtered_batch()) {
				stopped = true;
				return true;
			}
		}
		RecordSljitRegionStageRuntimeWithSuffix(runtime, filter_idx, filter_op.kind,
		                                        "+direct_compact_reference_projection", stage_start);
		RecordSljitRegionRuntimePath(runtime, filter_op.kind, "direct_compact_reference_projection", appended_count);
		RecordSljitRegionMaterializationBoundary(runtime, append_trace_op.kind, "filtered_input_batch", appended_count);
		return true;
	}

	template <class FLUSH>
	bool TryAppendSelectedReferenceProjectionBatch(ExecutionRegionRuntime &runtime,
	                                               SljitRegionExecutionScratch &scratch, idx_t filter_idx,
	                                               SljitExecutableRegionOp &filter_op,
	                                               SljitExecutableRegionOp &projection_op,
	                                               SljitExecutableRegionOp &append_trace_op, DataChunk &source_chunk,
	                                               DataChunk &filtered_batch, bool &filtered_batch_initialized,
	                                               FLUSH &&flush_filtered_batch, bool &stopped) {
		stopped = false;
		if (projection_op.kind != SljitNativeRegionOpKind::PROJECTION || projection_op.projections.size() != 1 ||
		    projection_op.output_types.size() != 1) {
			return false;
		}
		auto &projection_expr = projection_op.projections[0];
		auto &projection = projection_expr.plan;
		idx_t projection_source_index;
		if (!TryReadDirectReferenceProjectionSource(projection_expr, projection_source_index) ||
		    projection_source_index >= source_chunk.ColumnCount()) {
			return false;
		}
		auto &project_source = source_chunk.data[projection_source_index];
		if (projection.return_type != project_source.GetType() ||
		    projection.return_type != projection_op.output_types[0] ||
		    !DirectAppendSupportsFixedSizeType(projection.return_type)) {
			return false;
		}

		if (!filtered_batch_initialized) {
			filtered_batch.Initialize(runtime.GetAllocator(), projection_op.output_types);
			filtered_batch_initialized = true;
		}
		if (filtered_batch.ColumnCount() != 1 || filtered_batch.data[0].GetType() != projection.return_type) {
			return false;
		}
		if (TryAppendComparedReferenceProjectionBatch(runtime, filter_idx, filter_op, projection_op, append_trace_op,
		                                              source_chunk, filtered_batch, filtered_batch_initialized,
		                                              projection_source_index, flush_filtered_batch, stopped)) {
			return true;
		}

		auto &filter_selection = scratch.FilterSelection(filter_idx);
		auto &filter_scratch = scratch.ExpressionAdapterScratch(filter_idx, 0);
		auto stage_start = SljitRegionStageStart(runtime);
		auto selected_count = SelectFilter(filter_op, source_chunk, filter_selection, filter_scratch);
		RecordSljitRegionStageRuntimeWithSuffix(runtime, filter_idx, filter_op.kind, "+direct_reference_projection",
		                                        stage_start);
		if (selected_count == 0) {
			return true;
		}
		UnifiedVectorFormat project_format;
		const bool fast_fixed_append = AllValidSource(project_format, project_source, source_chunk.size()) &&
		                               filtered_batch.data[0].GetVectorType() == VectorType::FLAT_VECTOR &&
		                               FlatVector::GetCapacity(filtered_batch.data[0]) >= STANDARD_VECTOR_SIZE;
		auto project_source_sel = fast_fixed_append ? SljitNormalizedSourceSelectionData(project_format) : nullptr;
		auto project_source_data = fast_fixed_append ? project_format.data : nullptr;
		auto filter_selection_ptr = selected_count == source_chunk.size() ? nullptr : &filter_selection;

		idx_t appended_count = 0;
		idx_t source_offset = 0;
		while (source_offset < selected_count) {
			const auto current_size = filtered_batch.size();
			if (current_size == STANDARD_VECTOR_SIZE) {
				if (flush_filtered_batch()) {
					stopped = true;
					return true;
				}
				continue;
			}
			const auto append_count =
			    MinValue<idx_t>(STANDARD_VECTOR_SIZE - current_size, selected_count - source_offset);
			if (fast_fixed_append &&
			    CopySelectedFixedValues(projection.return_type, project_source_data, project_source_sel,
			                            filter_selection_ptr, source_offset, append_count,
			                            FlatVector::GetDataMutable(filtered_batch.data[0]), current_size)) {
				FinishDirectProjectionBatchTargets(filtered_batch, current_size + append_count);
			} else if (selected_count == source_chunk.size()) {
				SelectionVector append_selection(source_offset, append_count);
				filtered_batch.data[0].Append(project_source, append_selection, append_count);
				filtered_batch.CheckCardinality(current_size + append_count);
			} else {
				SelectionVector append_selection(filter_selection.data() + source_offset, append_count);
				filtered_batch.data[0].Append(project_source, append_selection, append_count);
				filtered_batch.CheckCardinality(current_size + append_count);
			}
			source_offset += append_count;
			appended_count += append_count;
			if (filtered_batch.size() == STANDARD_VECTOR_SIZE && flush_filtered_batch()) {
				stopped = true;
				return true;
			}
		}
		RecordSljitRegionRuntimePath(runtime, filter_op.kind, "direct_selected_reference_projection", appended_count);
		RecordSljitRegionMaterializationBoundary(runtime, append_trace_op.kind, "filtered_input_batch", appended_count);
		return true;
	}

	static const_data_ptr_t OffsetFixedSizeData(const_data_ptr_t data, const LogicalType &type, idx_t offset) {
		return data + offset * GetTypeIdSize(type.InternalType());
	}

	struct FixedDirectAppendSourceCache {
		void Reset(idx_t column_count) {
			source_indices.clear();
			source_formats.clear();
			source_indices.reserve(column_count);
			source_formats.reserve(column_count);
		}

		vector<idx_t> source_indices;
		vector<UnifiedVectorFormat> source_formats;
	};

	static bool PrepareFixedDirectAppendSource(DataChunk &input, idx_t source_index, idx_t source_offset, idx_t count,
	                                           UnifiedVectorFormat &source_format) {
		if (source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT fixed direct append source is out of range");
		}
		input.data[source_index].ToUnifiedFormat(source_format);
		if (!SljitUnifiedFormatHasIdentitySelection(source_format)) {
			return false;
		}
		if (source_format.validity.CanHaveNull() &&
		    !source_format.validity.CheckAllValid(source_offset + count, source_offset)) {
			return false;
		}
		return true;
	}

	static bool PrepareFixedDirectAppendFullSource(DataChunk &input, idx_t source_index,
	                                               UnifiedVectorFormat &source_format) {
		if (source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT fixed direct append source is out of range");
		}
		input.data[source_index].ToUnifiedFormat(source_format);
		if (!SljitUnifiedFormatHasIdentitySelection(source_format)) {
			return false;
		}
		if (source_format.validity.CanHaveNull() && !source_format.validity.CheckAllValid(input.size())) {
			return false;
		}
		return true;
	}

	static bool PrepareFixedDirectAppendSource(DataChunk &input, idx_t source_index, idx_t source_offset, idx_t count,
	                                           optional_ptr<FixedDirectAppendSourceCache> source_cache,
	                                           UnifiedVectorFormat &local_source_format,
	                                           UnifiedVectorFormat *&source_format) {
		source_format = nullptr;
		if (!source_cache) {
			if (!PrepareFixedDirectAppendSource(input, source_index, source_offset, count, local_source_format)) {
				return false;
			}
			source_format = &local_source_format;
			return true;
		}
		for (idx_t prepared_idx = 0; prepared_idx < source_cache->source_indices.size(); prepared_idx++) {
			if (source_cache->source_indices[prepared_idx] == source_index) {
				source_format = &source_cache->source_formats[prepared_idx];
				return true;
			}
		}
		source_cache->source_formats.emplace_back();
		auto &prepared_format = source_cache->source_formats.back();
		if (!PrepareFixedDirectAppendFullSource(input, source_index, prepared_format)) {
			source_cache->source_formats.pop_back();
			return false;
		}
		source_cache->source_indices.push_back(source_index);
		source_format = &prepared_format;
		return true;
	}

	static bool FixedDirectAppendSignedStatsType(PhysicalType physical_type) {
		switch (physical_type) {
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
			return true;
		default:
			return false;
		}
	}

	template <class T>
	static void ScanFixedDirectAppendSignedStats(const UnifiedVectorFormat &source_format, idx_t source_offset,
	                                             idx_t count, int64_t &min_value, int64_t &max_value) {
		D_ASSERT(count > 0);
		auto values = reinterpret_cast<const T *>(source_format.data) + source_offset;
		auto local_min = values[0];
		auto local_max = values[0];
		for (idx_t row_idx = 1; row_idx < count; row_idx++) {
			auto value = values[row_idx];
			local_min = MinValue<T>(local_min, value);
			local_max = MaxValue<T>(local_max, value);
		}
		min_value = int64_t(local_min);
		max_value = int64_t(local_max);
	}

	static bool TryScanFixedDirectAppendSourceStats(DataChunk &input, idx_t source_index, idx_t source_offset,
	                                                idx_t count, vector<DirectAppendColumnStats> &source_stats,
	                                                optional_ptr<FixedDirectAppendSourceCache> source_cache) {
		if (source_index >= input.ColumnCount() || source_index >= source_stats.size() || count == 0) {
			return false;
		}
		auto &cached_stats = source_stats[source_index];
		if (cached_stats.has_stats) {
			return true;
		}
		auto physical_type = input.data[source_index].GetType().InternalType();
		if (!FixedDirectAppendSignedStatsType(physical_type)) {
			return false;
		}
		UnifiedVectorFormat local_source_format;
		UnifiedVectorFormat *source_format;
		if (!PrepareFixedDirectAppendSource(input, source_index, source_offset, count, source_cache,
		                                    local_source_format, source_format)) {
			return false;
		}
		cached_stats.has_stats = true;
		cached_stats.physical_type = physical_type;
		switch (physical_type) {
		case PhysicalType::INT8:
			ScanFixedDirectAppendSignedStats<int8_t>(*source_format, source_offset, count, cached_stats.signed_min,
			                                         cached_stats.signed_max);
			return true;
		case PhysicalType::INT16:
			ScanFixedDirectAppendSignedStats<int16_t>(*source_format, source_offset, count, cached_stats.signed_min,
			                                          cached_stats.signed_max);
			return true;
		case PhysicalType::INT32:
			ScanFixedDirectAppendSignedStats<int32_t>(*source_format, source_offset, count, cached_stats.signed_min,
			                                          cached_stats.signed_max);
			return true;
		case PhysicalType::INT64:
			ScanFixedDirectAppendSignedStats<int64_t>(*source_format, source_offset, count, cached_stats.signed_min,
			                                          cached_stats.signed_max);
			return true;
		default:
			cached_stats = DirectAppendColumnStats();
			return false;
		}
	}

	static bool TryAddFixedStatsBound(int64_t left, int64_t right, int64_t &result) {
		if ((right > 0 && left > NumericLimits<int64_t>::Maximum() - right) ||
		    (right < 0 && left < NumericLimits<int64_t>::Minimum() - right)) {
			return false;
		}
		result = left + right;
		return true;
	}

	static bool TrySubtractFixedStatsBound(int64_t left, int64_t right, int64_t &result) {
		if ((right < 0 && left > NumericLimits<int64_t>::Maximum() + right) ||
		    (right > 0 && left < NumericLimits<int64_t>::Minimum() + right)) {
			return false;
		}
		result = left - right;
		return true;
	}

	static bool SetFixedSignedDirectAppendStats(DirectAppendColumnStats &stats, PhysicalType physical_type,
	                                            int64_t min_value, int64_t max_value) {
		if (!FixedDirectAppendSignedStatsType(physical_type)) {
			return false;
		}
		stats = DirectAppendColumnStats();
		stats.has_stats = true;
		stats.physical_type = physical_type;
		stats.signed_min = min_value;
		stats.signed_max = max_value;
		return true;
	}

	static void TrySetDirectAppendDistinctCount(DirectAppendColumnStats &stats, idx_t source_index,
	                                            const vector<idx_t> &source_distinct_counts) {
		if (source_index >= source_distinct_counts.size() || source_distinct_counts[source_index] == 0) {
			return;
		}
		stats.has_distinct_count = true;
		stats.distinct_count = source_distinct_counts[source_index];
	}

	static bool TryDeriveFixedBinaryConstantStats(const SljitNativeRegionExpressionPlan &plan,
	                                              const DirectAppendColumnStats &source_stats,
	                                              DirectAppendColumnStats &result_stats) {
		int64_t min_value;
		int64_t max_value;
		switch (plan.binary_op) {
		case SljitNativeIntegerBinaryOp::ADD:
			if (!TryAddFixedStatsBound(source_stats.signed_min, plan.constant, min_value) ||
			    !TryAddFixedStatsBound(source_stats.signed_max, plan.constant, max_value)) {
				return false;
			}
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			if (plan.constant_on_left) {
				if (!TrySubtractFixedStatsBound(plan.constant, source_stats.signed_max, min_value) ||
				    !TrySubtractFixedStatsBound(plan.constant, source_stats.signed_min, max_value)) {
					return false;
				}
			} else {
				if (!TrySubtractFixedStatsBound(source_stats.signed_min, plan.constant, min_value) ||
				    !TrySubtractFixedStatsBound(source_stats.signed_max, plan.constant, max_value)) {
					return false;
				}
			}
			break;
		default:
			return false;
		}
		return SetFixedSignedDirectAppendStats(result_stats, plan.return_type.InternalType(), min_value, max_value);
	}

	static bool TryDeriveFixedBinaryReferenceStats(const SljitNativeRegionExpressionPlan &plan,
	                                               const DirectAppendColumnStats &left_stats,
	                                               const DirectAppendColumnStats &right_stats,
	                                               DirectAppendColumnStats &result_stats) {
		int64_t min_value;
		int64_t max_value;
		switch (plan.binary_op) {
		case SljitNativeIntegerBinaryOp::ADD:
			if (!TryAddFixedStatsBound(left_stats.signed_min, right_stats.signed_min, min_value) ||
			    !TryAddFixedStatsBound(left_stats.signed_max, right_stats.signed_max, max_value)) {
				return false;
			}
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			if (!TrySubtractFixedStatsBound(left_stats.signed_min, right_stats.signed_max, min_value) ||
			    !TrySubtractFixedStatsBound(left_stats.signed_max, right_stats.signed_min, max_value)) {
				return false;
			}
			break;
		default:
			return false;
		}
		return SetFixedSignedDirectAppendStats(result_stats, plan.return_type.InternalType(), min_value, max_value);
	}

	static bool TryComputeFixedDirectAppendStats(const SljitExecutableRegionExpression &expr, DataChunk &input,
	                                             idx_t source_offset, idx_t count,
	                                             vector<DirectAppendColumnStats> &source_stats,
	                                             DirectAppendColumnStats &result_stats,
	                                             optional_ptr<FixedDirectAppendSourceCache> source_cache,
	                                             const vector<idx_t> &source_distinct_counts) {
		auto &plan = expr.plan;
		if (!FixedDirectAppendSignedStatsType(plan.return_type.InternalType())) {
			return false;
		}
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			if (!TryScanFixedDirectAppendSourceStats(input, plan.source_index, source_offset, count, source_stats,
			                                         source_cache)) {
				return false;
			}
			if (source_stats[plan.source_index].physical_type != plan.return_type.InternalType()) {
				return false;
			}
			result_stats = source_stats[plan.source_index];
			TrySetDirectAppendDistinctCount(result_stats, plan.source_index, source_distinct_counts);
			return true;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
			if (plan.check_arithmetic_overflow || plan.check_result_range ||
			    !TryScanFixedDirectAppendSourceStats(input, plan.source_index, source_offset, count, source_stats,
			                                         source_cache)) {
				return false;
			}
			if (!TryDeriveFixedBinaryConstantStats(plan, source_stats[plan.source_index], result_stats)) {
				return false;
			}
			TrySetDirectAppendDistinctCount(result_stats, plan.source_index, source_distinct_counts);
			return true;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			if (plan.check_arithmetic_overflow || plan.check_result_range ||
			    !TryScanFixedDirectAppendSourceStats(input, plan.source_index, source_offset, count, source_stats,
			                                         source_cache) ||
			    !TryScanFixedDirectAppendSourceStats(input, plan.right_source_index, source_offset, count, source_stats,
			                                         source_cache)) {
				return false;
			}
			return TryDeriveFixedBinaryReferenceStats(plan, source_stats[plan.source_index],
			                                          source_stats[plan.right_source_index], result_stats);
		default:
			return false;
		}
	}

	enum class SljitDirectProjectionMaterializerKind : uint8_t { NONE, FLOATING_FUSED, FIXED_FUSED, FIXED_SCALAR };

	struct SljitDirectProjectionCandidate {
		SljitDirectProjectionMaterializerKind kind = SljitDirectProjectionMaterializerKind::NONE;
		SljitDirectProjectionStatsMode stats_mode = SljitDirectProjectionStatsMode::NONE;
		const char *shape_changed_message = nullptr;

		bool IsSet() const {
			return kind != SljitDirectProjectionMaterializerKind::NONE;
		}

		bool IsFloatingFused() const {
			return kind == SljitDirectProjectionMaterializerKind::FLOATING_FUSED;
		}

		bool IsFixedFused() const {
			return kind == SljitDirectProjectionMaterializerKind::FIXED_FUSED;
		}

		bool IsFixedScalar() const {
			return kind == SljitDirectProjectionMaterializerKind::FIXED_SCALAR;
		}

		const char *ShapeChangedMessage() const {
			return shape_changed_message ? shape_changed_message
			                             : "SLJIT direct append source shape changed after preflight";
		}
	};

	struct SljitDirectProjectionStageAccumulators {
		SljitRegionStageAccumulator *source_prepare = nullptr;
		SljitRegionStageAccumulator *run = nullptr;
		SljitRegionStageAccumulator *generated = nullptr;
		SljitRegionStageAccumulator *stats = nullptr;
	};

	static bool IsFixedDirectAppendGeneratedExpression(const SljitExecutableRegionExpression &expr) {
		auto &plan = expr.plan;
		if (!expr.function || !DirectAppendSupportsFixedSizeType(plan.return_type)) {
			return false;
		}
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
			return true;
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
			return !plan.try_cast;
		default:
			return false;
		}
	}

	static bool IsSourceAppendDirectProjection(const SljitExecutableRegionExpression &expr, DataChunk &input) {
		auto &plan = expr.plan;
		return plan.kind == SljitNativeRegionExpressionKind::REFERENCE && plan.source_index < input.ColumnCount() &&
		       input.data[plan.source_index].GetType() == plan.return_type &&
		       DirectAppendSupportsSourceAppendType(plan.return_type);
	}

	static bool TryBindDirectAppendSourceProjection(const SljitExecutableRegionExpression &expr, DataChunk &input,
	                                                optional_ptr<DirectAppendSlice> slice, idx_t projection_idx,
	                                                idx_t source_offset, idx_t count, bool execute) {
		if (!IsSourceAppendDirectProjection(expr, input)) {
			return false;
		}
		if (!execute) {
			return true;
		}
		if (!slice) {
			throw InternalException("SLJIT source direct append projection missing reservation slice");
		}
		if (slice->sources.size() != slice->targets.size() || projection_idx >= slice->sources.size()) {
			throw InternalException("SLJIT source direct append source count mismatch");
		}
		auto &plan = expr.plan;
		auto &source_vector = input.data[plan.source_index];
		if (source_offset + count > source_vector.size()) {
			throw InternalException("SLJIT source direct append source slice out of range");
		}
		slice->sources[projection_idx].vector = &source_vector;
		slice->sources[projection_idx].offset = source_offset;
		return true;
	}

	static bool TryDirectMaterializeFixedReference(const SljitExecutableRegionExpression &expr, DataChunk &input,
	                                               data_ptr_t target, idx_t source_offset, idx_t count, bool execute,
	                                               optional_ptr<FixedDirectAppendSourceCache> source_cache) {
		auto &plan = expr.plan;
		if (plan.source_index >= input.ColumnCount() || input.data[plan.source_index].GetType() != plan.return_type ||
		    !DirectAppendSupportsFixedSizeType(plan.return_type)) {
			return false;
		}

		UnifiedVectorFormat local_source_format;
		UnifiedVectorFormat *source_format;
		if (!PrepareFixedDirectAppendSource(input, plan.source_index, source_offset, count, source_cache,
		                                    local_source_format, source_format)) {
			return false;
		}
		if (!execute) {
			return true;
		}
		if (!target) {
			throw InternalException("SLJIT fixed direct append reference target is null");
		}
		auto source_data = OffsetFixedSizeData(source_format->data, plan.return_type, source_offset);
		memcpy(target, source_data, count * GetTypeIdSize(plan.return_type.InternalType()));
		return true;
	}

	static data_ptr_t FixedDirectAppendResultData(const SljitExecutableRegionExpression &expr, data_ptr_t target) {
		if (!target) {
			throw InternalException("SLJIT fixed direct append generated target is null");
		}
		auto &plan = expr.plan;
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			return target;
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
		case SljitNativeRegionExpressionKind::DATE_YEAR:
			return target;
		default:
			throw InternalException("SLJIT fixed direct append generated expression has no direct result pointer");
		}
	}

	static bool TryDirectMaterializeFixedGenerated(const SljitExecutableRegionExpression &expr, DataChunk &input,
	                                               data_ptr_t target, idx_t source_offset, idx_t count, bool execute,
	                                               optional_ptr<FixedDirectAppendSourceCache> source_cache) {
		if (!IsFixedDirectAppendGeneratedExpression(expr)) {
			return false;
		}
		auto &plan = expr.plan;
		UnifiedVectorFormat local_source_format;
		UnifiedVectorFormat *source_format;
		if (!PrepareFixedDirectAppendSource(input, plan.source_index, source_offset, count, source_cache,
		                                    local_source_format, source_format)) {
			return false;
		}
		const bool has_right_source = plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES;
		UnifiedVectorFormat local_right_source_format;
		UnifiedVectorFormat *right_source_format = nullptr;
		if (has_right_source) {
			if (!PrepareFixedDirectAppendSource(input, plan.right_source_index, source_offset, count, source_cache,
			                                    local_right_source_format, right_source_format)) {
				return false;
			}
		}
		if (!execute) {
			return true;
		}

		SljitNativeVectorInput native_input;
		if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS) {
			native_input.source_data = source_format->data;
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
			native_input.source_data = NativeUnsignedIntegerSourceData(*source_format, plan.unsigned_source_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
		           plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
			native_input.source_data = NativeSignedIntegerSourceData(*source_format, plan.cast_source_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
			native_input.source_data = NativeIntegerSourceData(*source_format, SljitNativeIntegerKind::INT32);
		} else {
			native_input.source_data = NativeIntegerSourceData(*source_format, plan.integer_kind);
		}
		native_input.source_data =
		    OffsetFixedSizeData(native_input.source_data, input.data[plan.source_index].GetType(), source_offset);
		if (has_right_source) {
			native_input.right_source_data = NativeIntegerSourceData(*right_source_format, plan.integer_kind);
			native_input.right_source_data = OffsetFixedSizeData(
			    native_input.right_source_data, input.data[plan.right_source_index].GetType(), source_offset);
		}
		native_input.constants = plan.constants.data();
		native_input.constant = plan.constant;
		native_input.result_data = FixedDirectAppendResultData(expr, target);
		native_input.overflow_message =
		    plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
		            plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		            plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
		            plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST
		        ? expr.overflow_message.c_str()
		        : nullptr;
		native_input.query_location = plan.query_location;
		native_input.count = count;
		native_input.has_error = false;
		auto function = expr.flat_function ? expr.flat_function : expr.function;
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		return true;
	}

	static bool TryDirectMaterializeFixedExpression(const SljitExecutableRegionExpression &expr, DataChunk &input,
	                                                data_ptr_t target, idx_t source_offset, idx_t count, bool execute,
	                                                optional_ptr<FixedDirectAppendSourceCache> source_cache) {
		if (expr.plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			return TryDirectMaterializeFixedReference(expr, input, target, source_offset, count, execute, source_cache);
		}
		return TryDirectMaterializeFixedGenerated(expr, input, target, source_offset, count, execute, source_cache);
	}

	static const char *SljitFixedProjectionExpressionTracePhase(const SljitNativeRegionExpressionPlan &plan) {
		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			return "direct_batch_expression.reference";
		case SljitNativeRegionExpressionKind::STRING_COMPRESS:
			return "direct_batch_expression.string_compress";
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
			return "direct_batch_expression.integral_compress";
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
			return "direct_batch_expression.integral_decompress";
		case SljitNativeRegionExpressionKind::DATE_YEAR:
			return "direct_batch_expression.date_year";
		case SljitNativeRegionExpressionKind::INTEGER_CAST:
		case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
			return "direct_batch_expression.integer_cast";
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			return "direct_batch_expression.integer_binary";
		default:
			return "direct_batch_expression.generated";
		}
	}

	bool TryDirectMaterializeFixedProjection(SljitExecutableRegionOp &op, DataChunk &input,
	                                         optional_ptr<DirectAppendSlice> slice,
	                                         optional_ptr<FixedDirectAppendSourceCache> source_cache = nullptr,
	                                         optional_ptr<vector<uint8_t>> skip_projection = nullptr,
	                                         optional_ptr<ExecutionRegionRuntime> trace_runtime = nullptr,
	                                         idx_t trace_op_idx = DConstants::INVALID_INDEX) {
		if (op.kind != SljitNativeRegionOpKind::PROJECTION || op.projections.empty()) {
			return false;
		}
		const bool execute = slice != nullptr;
		const auto source_offset = execute ? slice->source_offset : 0;
		const auto count = execute ? slice->count : input.size();
		const bool trace_expressions =
		    execute && trace_runtime && trace_runtime->TraceRuntime() && trace_op_idx != DConstants::INVALID_INDEX;
		for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
			if (skip_projection && projection_idx < skip_projection->size() && (*skip_projection)[projection_idx]) {
				continue;
			}
			auto &projection = op.projections[projection_idx];
			auto expression_stage_start =
			    trace_expressions ? SljitRegionStageStart(*trace_runtime) : std::chrono::steady_clock::time_point();
			if (TryBindDirectAppendSourceProjection(op.projections[projection_idx], input, slice, projection_idx,
			                                        source_offset, count, execute)) {
				if (trace_expressions) {
					RecordSljitRegionStageRuntime(*trace_runtime, trace_op_idx, op.kind,
					                              SljitFixedProjectionExpressionTracePhase(projection.plan),
					                              expression_stage_start);
				}
				continue;
			}
			data_ptr_t target = nullptr;
			if (execute) {
				if (slice->targets.size() != op.projections.size()) {
					throw InternalException("SLJIT fixed direct append target count mismatch");
				}
				target = slice->targets[projection_idx];
			}
			if (!TryDirectMaterializeFixedExpression(projection, input, target, source_offset, count, execute,
			                                         source_cache)) {
				return false;
			}
			if (trace_expressions) {
				RecordSljitRegionStageRuntime(*trace_runtime, trace_op_idx, op.kind,
				                              SljitFixedProjectionExpressionTracePhase(projection.plan),
				                              expression_stage_start);
			}
		}
		return true;
	}

	bool TrySelectDirectProjectionCandidate(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
	                                        DataChunk &input, optional_ptr<FixedDirectAppendSourceCache> source_cache,
	                                        SljitProjectionAdapterScratch &projection_scratch,
	                                        SljitDirectProjectionCandidate &candidate) {
		candidate = SljitDirectProjectionCandidate();
		const bool use_floating_direct_append =
		    op.flat_fused_floating_projection_function && op.flat_fused_floating_projection_plan.covers_all_projections;
		auto preflight_stage_start = SljitRegionStageStart(runtime);
		if (use_floating_direct_append) {
			if (!PrepareFlatFusedFloatingProjectionSources(op, input, nullptr, input.size(), projection_scratch,
			                                               false)) {
				return false;
			}
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_append_floating_preflight",
			                              preflight_stage_start);
			candidate.kind = SljitDirectProjectionMaterializerKind::FLOATING_FUSED;
			candidate.stats_mode = op.flat_fused_floating_projection_plan.stats_mode;
			candidate.shape_changed_message = "SLJIT direct append source shape changed after direct-append preflight";
			return true;
		}

		preflight_stage_start = SljitRegionStageStart(runtime);
		if (TryPrepareFlatFusedFixedProjectionSources(op, input, 0, input.size(), source_cache, projection_scratch)) {
			if (!TryDirectMaterializeFixedProjection(op, input, nullptr, source_cache, &projection_scratch.fused)) {
				return false;
			}
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_append_fixed_preflight",
			                              preflight_stage_start);
			candidate.kind = SljitDirectProjectionMaterializerKind::FIXED_FUSED;
			candidate.stats_mode = SljitDirectProjectionStatsMode::POSTPASS_FIXED_STATS;
			candidate.shape_changed_message =
			    "SLJIT fixed fused direct append source shape changed after direct-append preflight";
			return true;
		}
		if (TryDirectMaterializeFixedProjection(op, input, nullptr, source_cache)) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "direct_append_fixed_preflight",
			                              preflight_stage_start);
			candidate.kind = SljitDirectProjectionMaterializerKind::FIXED_SCALAR;
			candidate.stats_mode = SljitDirectProjectionStatsMode::POSTPASS_FIXED_STATS;
			candidate.shape_changed_message =
			    "SLJIT fixed direct append source shape changed after direct-append preflight";
			return true;
		}
		return false;
	}

	SljitDirectProjectionStageAccumulators DirectProjectionStageAccumulatorsFor(
	    const SljitDirectProjectionCandidate &candidate, SljitRegionStageAccumulator &floating_source_prepare,
	    SljitRegionStageAccumulator &floating_run, SljitRegionStageAccumulator &floating_generated,
	    SljitRegionStageAccumulator &floating_stats, SljitRegionStageAccumulator &fixed_generated,
	    SljitRegionStageAccumulator &fixed_fused_generated, SljitRegionStageAccumulator &fixed_stats) {
		SljitDirectProjectionStageAccumulators result;
		switch (candidate.kind) {
		case SljitDirectProjectionMaterializerKind::FLOATING_FUSED:
			result.source_prepare = &floating_source_prepare;
			result.run = &floating_run;
			result.generated = &floating_generated;
			result.stats = &floating_stats;
			break;
		case SljitDirectProjectionMaterializerKind::FIXED_FUSED:
			result.generated = &fixed_fused_generated;
			result.stats = &fixed_stats;
			break;
		case SljitDirectProjectionMaterializerKind::FIXED_SCALAR:
			result.generated = &fixed_generated;
			result.stats = &fixed_stats;
			break;
		default:
			break;
		}
		return result;
	}

	bool TryMaterializeDirectProjectionSlice(ExecutionRegionRuntime &runtime,
	                                         const SljitDirectProjectionCandidate &candidate,
	                                         SljitExecutableRegionOp &op, DataChunk &input, DirectAppendSlice &slice,
	                                         optional_ptr<FixedDirectAppendSourceCache> source_cache,
	                                         SljitProjectionAdapterScratch &projection_scratch,
	                                         SljitDirectProjectionStageAccumulators &stage_accumulators) {
		switch (candidate.kind) {
		case SljitDirectProjectionMaterializerKind::FLOATING_FUSED: {
			auto source_stage_start = SljitRegionStageStart(runtime);
			if (!PrepareFlatFusedFloatingProjectionSources(op, input, nullptr, slice.count, projection_scratch, false,
			                                               slice.source_offset)) {
				return false;
			}
			if (stage_accumulators.source_prepare) {
				stage_accumulators.source_prepare->Add(source_stage_start);
			}

			for (auto projection_idx : op.flat_fused_floating_projection_plan.projection_indices) {
				auto &plan = op.projections[projection_idx].plan;
				if (!slice.targets[projection_idx]) {
					throw InternalException("SLJIT direct append target pointer is null");
				}
				projection_scratch.result_data[projection_idx] = slice.targets[projection_idx];
				if (plan.return_type.InternalType() == PhysicalType::FLOAT) {
					projection_scratch.float_constants[projection_idx] = static_cast<float>(plan.double_constant);
				} else {
					projection_scratch.double_constants[projection_idx] = plan.double_constant;
				}
			}
			projection_scratch.PrepareFloatingStats(op.projections.size(),
			                                        op.flat_fused_floating_projection_plan.SinglePrecision());

			auto run_stage_start = SljitRegionStageStart(runtime);
			RunFlatFusedFloatingProjection(op, slice.count, projection_scratch);
			if (stage_accumulators.run) {
				stage_accumulators.run->Add(run_stage_start);
			}
			return true;
		}
		case SljitDirectProjectionMaterializerKind::FIXED_FUSED:
			if (!TryPrepareFlatFusedFixedProjectionSources(op, input, slice.source_offset, slice.count, source_cache,
			                                               projection_scratch)) {
				return false;
			}
			BindFlatFusedFixedProjectionTargets(op, slice, projection_scratch);
			RunFlatFusedFixedProjection(op, slice.count, projection_scratch);
			return TryDirectMaterializeFixedProjection(op, input, &slice, source_cache, &projection_scratch.fused);
		case SljitDirectProjectionMaterializerKind::FIXED_SCALAR:
			return TryDirectMaterializeFixedProjection(op, input, &slice, source_cache);
		default:
			return false;
		}
	}

	void FinishDirectProjectionStats(const SljitDirectProjectionCandidate &candidate, SljitExecutableRegionOp &op,
	                                 DataChunk &input, DirectAppendSlice &slice, idx_t op_idx,
	                                 optional_ptr<FixedDirectAppendSourceCache> source_cache,
	                                 SljitProjectionAdapterScratch &projection_scratch) {
		switch (candidate.stats_mode) {
		case SljitDirectProjectionStatsMode::NONE:
			slice.stats.clear();
			return;
		case SljitDirectProjectionStatsMode::GENERATED_FLOATING_MIN_MAX:
			if (!candidate.IsFloatingFused()) {
				throw InternalException(
				    "SLJIT generated floating stats requested for a non-floating direct append path");
			}
			projection_scratch.FinishFloatingStats(op.projections,
			                                       op.flat_fused_floating_projection_plan.SinglePrecision());
			slice.stats = projection_scratch.direct_append_stats;
			return;
		case SljitDirectProjectionStatsMode::POSTPASS_FIXED_STATS: {
			if (!candidate.IsFixedFused() && !candidate.IsFixedScalar()) {
				throw InternalException("SLJIT fixed postpass stats requested for a non-fixed direct append path");
			}
			slice.stats.assign(op.projections.size(), DirectAppendColumnStats());
			vector<DirectAppendColumnStats> fixed_source_stats(input.ColumnCount());
			vector<idx_t> empty_distinct_counts;
			const auto &direct_distinct_counts = op_idx == 0 ? source_distinct_counts : empty_distinct_counts;
			for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
				TryComputeFixedDirectAppendStats(op.projections[projection_idx], input, slice.source_offset,
				                                 slice.count, fixed_source_stats, slice.stats[projection_idx],
				                                 source_cache, direct_distinct_counts);
			}
			return;
		}
		default:
			throw InternalException("Unsupported SLJIT direct projection stats mode");
		}
	}

	bool TryExecuteProjectionDirectAppend(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
	                                      SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                      SljitExecutableRegionOp &op, DataChunk &input, SinkResultType &sink_result) {
		if (op.kind != SljitNativeRegionOpKind::PROJECTION || op_idx + 1 >= ops.size()) {
			return false;
		}
		auto &sink_op = ops[op_idx + 1];
		if (sink_op.kind != SljitNativeRegionOpKind::APPEND_SINK || op_idx + 2 != ops.size()) {
			return false;
		}
		auto &projection_scratch = scratch.ProjectionScratch(op_idx);
		FixedDirectAppendSourceCache fixed_source_cache;
		fixed_source_cache.Reset(input.ColumnCount());
		auto fixed_source_cache_ptr = optional_ptr<FixedDirectAppendSourceCache>(&fixed_source_cache);
		SljitDirectProjectionCandidate direct_candidate;
		if (!TrySelectDirectProjectionCandidate(runtime, op_idx, op, input, fixed_source_cache_ptr, projection_scratch,
		                                        direct_candidate)) {
			return false;
		}
		D_ASSERT(direct_candidate.IsSet());

		auto bind_stage_start = SljitRegionStageStart(runtime);
		bool bound = false;
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx + 1, input, sink_op.append_sink.plan.sink_info,
		                               "append-sink-runtime-binding-failed", "SLJIT append sink", bound);
		if (bound) {
			RecordSljitRegionStageRuntime(runtime, op_idx + 1, sink_op.kind, "bind_sink_contract", bind_stage_start);
		}
		if (!binding.ready || !binding.append_sink.ready) {
			throw InternalException("SLJIT append sink binding did not return a ready append state");
		}

		SljitRegionStageAccumulator prepare_stage_accumulator(runtime, op_idx + 1, sink_op.kind,
		                                                      "direct_append_prepare");
		SljitRegionStageAccumulator floating_source_prepare_stage_accumulator(runtime, op_idx, op.kind,
		                                                                      "direct_append_floating_source_prepare");
		SljitRegionStageAccumulator floating_run_stage_accumulator(runtime, op_idx, op.kind,
		                                                           "direct_append_floating_run");
		SljitRegionStageAccumulator floating_generated_stage_accumulator(runtime, op_idx, op.kind,
		                                                                 "direct_materialize_generated");
		SljitRegionStageAccumulator floating_stats_stage_accumulator(runtime, op_idx, op.kind,
		                                                             "direct_append_floating_finish_stats");
		SljitRegionStageAccumulator fixed_generated_stage_accumulator(runtime, op_idx, op.kind,
		                                                              "direct_materialize_fixed_generated");
		SljitRegionStageAccumulator fixed_fused_generated_stage_accumulator(runtime, op_idx, op.kind,
		                                                                    "direct_materialize_fixed_fused_generated");
		SljitRegionStageAccumulator fixed_stats_stage_accumulator(runtime, op_idx, op.kind,
		                                                          "direct_append_fixed_stats");
		SljitRegionStageAccumulator commit_stage_accumulator(runtime, op_idx + 1, sink_op.kind, "direct_append_commit");
		auto flush_direct_append_stage_accumulators = [&]() {
			prepare_stage_accumulator.Flush();
			floating_source_prepare_stage_accumulator.Flush();
			floating_run_stage_accumulator.Flush();
			floating_generated_stage_accumulator.Flush();
			floating_stats_stage_accumulator.Flush();
			fixed_generated_stage_accumulator.Flush();
			fixed_fused_generated_stage_accumulator.Flush();
			fixed_stats_stage_accumulator.Flush();
			commit_stage_accumulator.Flush();
		};
		auto direct_stage_accumulators = DirectProjectionStageAccumulatorsFor(
		    direct_candidate, floating_source_prepare_stage_accumulator, floating_run_stage_accumulator,
		    floating_generated_stage_accumulator, floating_stats_stage_accumulator, fixed_generated_stage_accumulator,
		    fixed_fused_generated_stage_accumulator, fixed_stats_stage_accumulator);
		D_ASSERT(direct_stage_accumulators.generated);
		D_ASSERT(direct_stage_accumulators.stats);

		idx_t source_offset = 0;
		while (source_offset < input.size()) {
			string blocker;
			auto &reservation = scratch.direct_append_reservation;
			DirectAppendProfile direct_append_profile;
			auto prepare_stage_start = SljitRegionStageStart(runtime);
			auto direct_result =
			    ExecutionPrepareDirectAppend(binding.append_sink, op.output_types, input.size() - source_offset,
			                                 reservation, blocker, &direct_append_profile);
			prepare_stage_accumulator.Add(prepare_stage_start);
			if (direct_result == ExecutionOperatorBindResult::DEFERRED) {
				if (source_offset > 0) {
					throw InternalException("SLJIT direct append became deferred after a partial commit: %s",
					                        blocker.c_str());
				}
				runtime.Defer(blocker.empty() ? "direct-append-deferred" : blocker);
				sink_result = SinkResultType::BLOCKED;
				RecordSljitDirectAppendProfile(runtime, op_idx + 1, sink_op.kind, direct_append_profile);
				flush_direct_append_stage_accumulators();
				return true;
			}
			if (direct_result != ExecutionOperatorBindResult::READY) {
				if (source_offset > 0) {
					throw InternalException("SLJIT direct append became unavailable after a partial commit: %s",
					                        blocker.c_str());
				}
				RecordSljitDirectAppendProfile(runtime, op_idx + 1, sink_op.kind, direct_append_profile);
				flush_direct_append_stage_accumulators();
				return false;
			}
			if (reservation.slices.size() != 1) {
				throw InternalException("SLJIT direct append expected exactly one storage reservation slice");
			}
			auto &slice = reservation.slices[0];
			slice.source_offset = source_offset;
			if (slice.count == 0 || source_offset + slice.count > input.size()) {
				throw InternalException("SLJIT direct append reservation slice is out of range");
			}
			if (slice.targets.size() != op.projections.size()) {
				throw InternalException("SLJIT direct append target count mismatch");
			}
			if (slice.sources.size() != op.projections.size()) {
				throw InternalException("SLJIT direct append source count mismatch");
			}
			auto generated_stage_start = SljitRegionStageStart(runtime);
			if (!TryMaterializeDirectProjectionSlice(runtime, direct_candidate, op, input, slice,
			                                         fixed_source_cache_ptr, projection_scratch,
			                                         direct_stage_accumulators)) {
				throw InternalException(direct_candidate.ShapeChangedMessage());
			}
			direct_stage_accumulators.generated->Add(generated_stage_start);
			auto stats_stage_start = SljitRegionStageStart(runtime);
			FinishDirectProjectionStats(direct_candidate, op, input, slice, op_idx, fixed_source_cache_ptr,
			                            projection_scratch);
			direct_stage_accumulators.stats->Add(stats_stage_start);
			auto commit_stage_start = SljitRegionStageStart(runtime);
			sink_result = ExecutionCommitDirectAppend(binding.append_sink, reservation, &direct_append_profile);
			commit_stage_accumulator.Add(commit_stage_start);
			RecordSljitDirectAppendProfile(runtime, op_idx + 1, sink_op.kind, direct_append_profile);
			if (sink_result != SinkResultType::NEED_MORE_INPUT) {
				flush_direct_append_stage_accumulators();
				return true;
			}
			source_offset += slice.count;
		}
		flush_direct_append_stage_accumulators();
		return true;
	}

	void ExecuteProjection(SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
	                       DataChunk &input, DataChunk &output, const SelectionVector *execute_sel = nullptr,
	                       idx_t count = DConstants::INVALID_INDEX) {
		if (count == DConstants::INVALID_INDEX) {
			count = input.size();
		}
		if (!execute_sel && TryFastInlineStringDecompressProjection(op, input, output, count)) {
			return;
		}
		auto &projection_scratch = scratch.ProjectionScratch(op_idx);
		auto fused_projection_executed =
		    ExecuteFlatFusedFloatingProjection(op, input, output, execute_sel, count, projection_scratch);
		if (fused_projection_executed && op.flat_fused_floating_projection_plan.covers_all_projections) {
			output.SetChildCardinality(count);
			return;
		}
		for (idx_t col_idx = 0; col_idx < op.projections.size(); col_idx++) {
			if (fused_projection_executed && projection_scratch.fused[col_idx]) {
				continue;
			}
			ExecuteProjectionExpression(op.projections[col_idx], input, output.data[col_idx], execute_sel, count,
			                            scratch.ExpressionAdapterScratch(op_idx, col_idx));
		}
		output.SetChildCardinality(count);
	}

	bool ExecuteFlatFusedFloatingProjection(SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
	                                        const SelectionVector *execute_sel, idx_t count,
	                                        SljitProjectionAdapterScratch &adapter_scratch) {
		auto all_projections_fused = op.flat_fused_floating_projection_plan.covers_all_projections;
		if (!PrepareFlatFusedFloatingProjectionSources(op, input, execute_sel, count, adapter_scratch,
		                                               !all_projections_fused)) {
			return false;
		}

		for (auto projection_idx : op.flat_fused_floating_projection_plan.projection_indices) {
			auto &plan = op.projections[projection_idx].plan;
			auto &result = output.data[projection_idx];
			result.SetVectorType(VectorType::FLAT_VECTOR);
			FlatVector::ValidityMutable(result).Reset(count);
			if (plan.return_type.InternalType() == PhysicalType::FLOAT) {
				adapter_scratch.result_data[projection_idx] =
				    reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<float>(result));
				adapter_scratch.float_constants[projection_idx] = static_cast<float>(plan.double_constant);
			} else {
				adapter_scratch.result_data[projection_idx] =
				    reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<double>(result));
				adapter_scratch.double_constants[projection_idx] = plan.double_constant;
			}
			FlatVector::SetSize(result, count_t(count));
			if (!all_projections_fused) {
				adapter_scratch.fused[projection_idx] = 1;
			}
		}

		RunFlatFusedFloatingProjection(op, count, adapter_scratch);
		return true;
	}

	bool PrepareFlatFusedFloatingProjectionSources(SljitExecutableRegionOp &op, DataChunk &input,
	                                               const SelectionVector *execute_sel, idx_t count,
	                                               SljitProjectionAdapterScratch &adapter_scratch, bool track_fused,
	                                               idx_t source_offset = 0) {
		if (!op.flat_fused_floating_projection_function || execute_sel) {
			return false;
		}
		adapter_scratch.Prepare(op.projections.size(), track_fused);
		for (auto projection_idx : op.flat_fused_floating_projection_plan.projection_indices) {
			auto &plan = op.projections[projection_idx].plan;
			D_ASSERT(plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
			         plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES);
			if (!PrepareFlatFusedFloatingSource(input, plan.source_index, projection_idx, false, count, adapter_scratch,
			                                    source_offset)) {
				return false;
			}
			if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
				if (!PrepareFlatFusedFloatingSource(input, plan.right_source_index, projection_idx, true, count,
				                                    adapter_scratch, source_offset)) {
					return false;
				}
			}
		}
		return true;
	}

	static bool HasDirectProjectionSourceRef(const SljitDirectProjectionPlan &direct_plan, idx_t source_index) {
		for (auto &source : direct_plan.sources) {
			if (source.input_index == source_index) {
				return true;
			}
		}
		return false;
	}

	bool TryPrepareFlatFusedFixedProjectionSources(SljitExecutableRegionOp &op, DataChunk &input, idx_t source_offset,
	                                               idx_t count, optional_ptr<FixedDirectAppendSourceCache> source_cache,
	                                               SljitProjectionAdapterScratch &adapter_scratch) {
		if (op.flat_fused_fixed_projection_plans.empty() ||
		    op.flat_fused_fixed_projection_plans.size() != op.flat_fused_fixed_projection_functions.size()) {
			return false;
		}
		adapter_scratch.Prepare(op.projections.size(), true);
		for (idx_t plan_idx = 0; plan_idx < op.flat_fused_fixed_projection_plans.size(); plan_idx++) {
			if (!op.flat_fused_fixed_projection_functions[plan_idx]) {
				return false;
			}
			auto &direct_plan = op.flat_fused_fixed_projection_plans[plan_idx];
			if (direct_plan.sources.empty()) {
				return false;
			}
			for (auto &source : direct_plan.sources) {
				if (source.projection_index >= op.projections.size()) {
					throw InternalException("SLJIT fixed fused direct append source projection is out of range");
				}
				auto &plan = op.projections[source.projection_index].plan;
				UnifiedVectorFormat local_source_format;
				UnifiedVectorFormat *source_format;
				if (!PrepareFixedDirectAppendSource(input, source.input_index, source_offset, count, source_cache,
				                                    local_source_format, source_format)) {
					return false;
				}
				auto source_data = NativeIntegerSourceData(*source_format, plan.integer_kind);
				auto source_pointer =
				    OffsetFixedSizeData(source_data, input.data[source.input_index].GetType(), source_offset);
				if (source.right_source) {
					adapter_scratch.right_source_data[source.projection_index] = source_pointer;
				} else {
					adapter_scratch.source_data[source.projection_index] = source_pointer;
				}
			}
			for (auto projection_idx : direct_plan.projection_indices) {
				auto &expr = op.projections[projection_idx];
				auto &plan = expr.plan;
				if (plan.kind != SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT &&
				    plan.kind != SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
					return false;
				}
				if (!HasDirectProjectionSourceRef(direct_plan, plan.source_index)) {
					return false;
				}
				if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
					if (!HasDirectProjectionSourceRef(direct_plan, plan.right_source_index)) {
						return false;
					}
				}
				adapter_scratch.integer_constants[projection_idx] = plan.constant;
				adapter_scratch.overflow_messages[projection_idx] = expr.overflow_message.c_str();
				adapter_scratch.fused[projection_idx] = 1;
			}
		}
		return true;
	}

	void BindFlatFusedFixedProjectionTargets(SljitExecutableRegionOp &op, DirectAppendSlice &slice,
	                                         SljitProjectionAdapterScratch &adapter_scratch) {
		if (slice.targets.size() != op.projections.size()) {
			throw InternalException("SLJIT fixed fused direct append target count mismatch");
		}
		for (auto &direct_plan : op.flat_fused_fixed_projection_plans) {
			for (auto projection_idx : direct_plan.projection_indices) {
				auto target = slice.targets[projection_idx];
				if (!target) {
					throw InternalException("SLJIT fixed fused direct append target pointer is null");
				}
				adapter_scratch.result_data[projection_idx] = target;
			}
		}
	}

	void RunFlatFusedFixedProjection(SljitExecutableRegionOp &op, idx_t count,
	                                 SljitProjectionAdapterScratch &adapter_scratch) {
		SljitNativeVectorInput native_input;
		native_input.source_data_array = adapter_scratch.source_data.data();
		native_input.right_source_data_array = adapter_scratch.right_source_data.data();
		native_input.result_data_array = adapter_scratch.result_data.data();
		native_input.constants = adapter_scratch.integer_constants.data();
		native_input.overflow_messages = adapter_scratch.overflow_messages.data();
		native_input.count = count;
		native_input.has_error = false;
		for (auto function : op.flat_fused_fixed_projection_functions) {
			native_input.error = nullptr;
			function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
		}
	}

	void RunFlatFusedFloatingProjection(SljitExecutableRegionOp &op, idx_t count,
	                                    SljitProjectionAdapterScratch &adapter_scratch) {
		SljitNativeVectorInput native_input;
		native_input.source_data_array = adapter_scratch.source_data.data();
		native_input.right_source_data_array = adapter_scratch.right_source_data.data();
		native_input.result_data_array = adapter_scratch.result_data.data();
		native_input.floating_constants =
		    op.flat_fused_floating_projection_plan.SinglePrecision()
		        ? reinterpret_cast<const_data_ptr_t>(adapter_scratch.float_constants.data())
		        : reinterpret_cast<const_data_ptr_t>(adapter_scratch.double_constants.data());
		if (adapter_scratch.collect_floating_stats) {
			native_input.floating_stats_min =
			    op.flat_fused_floating_projection_plan.SinglePrecision()
			        ? reinterpret_cast<data_ptr_t>(adapter_scratch.float_stats_min.data())
			        : reinterpret_cast<data_ptr_t>(adapter_scratch.double_stats_min.data());
			native_input.floating_stats_max =
			    op.flat_fused_floating_projection_plan.SinglePrecision()
			        ? reinterpret_cast<data_ptr_t>(adapter_scratch.float_stats_max.data())
			        : reinterpret_cast<data_ptr_t>(adapter_scratch.double_stats_max.data());
		}
		native_input.count = count;
		native_input.has_error = false;
		op.flat_fused_floating_projection_function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	bool PrepareFlatFusedFloatingSource(DataChunk &input, idx_t source_index, idx_t projection_idx, bool right_source,
	                                    idx_t count, SljitProjectionAdapterScratch &adapter_scratch,
	                                    idx_t source_offset = 0) {
		if (source_index >= input.ColumnCount()) {
			throw InternalException("SLJIT fused projection source is out of range");
		}
		for (idx_t prepared_idx = 0; prepared_idx < adapter_scratch.prepared_input_indices.size(); prepared_idx++) {
			if (adapter_scratch.prepared_input_indices[prepared_idx] == source_index) {
				auto source_data = adapter_scratch.prepared_input_data[prepared_idx];
				if (right_source) {
					adapter_scratch.right_source_data[projection_idx] = source_data;
				} else {
					adapter_scratch.source_data[projection_idx] = source_data;
				}
				return true;
			}
		}

		auto &source_format = right_source ? adapter_scratch.right_source_formats[projection_idx]
		                                   : adapter_scratch.source_formats[projection_idx];
		input.data[source_index].ToUnifiedFormat(source_format);
		if (!SljitUnifiedFormatHasIdentitySelection(source_format) ||
		    (source_format.validity.CanHaveNull() &&
		     !source_format.validity.CheckAllValid(source_offset + count, source_offset))) {
			return false;
		}
		auto source_data =
		    source_format.data + source_offset * GetTypeIdSize(input.data[source_index].GetType().InternalType());
		adapter_scratch.prepared_input_indices.push_back(source_index);
		adapter_scratch.prepared_input_data.push_back(source_data);
		if (right_source) {
			adapter_scratch.right_source_data[projection_idx] = source_data;
		} else {
			adapter_scratch.source_data[projection_idx] = source_data;
		}
		return true;
	}

	void ExecuteProjectionExpression(SljitExecutableRegionExpression &expr, DataChunk &input, Vector &result,
	                                 const SelectionVector *execute_sel, idx_t count,
	                                 SljitExpressionAdapterScratch &adapter_scratch) {
		auto &plan = expr.plan;
		if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			D_ASSERT(plan.source_index < input.ColumnCount());
			if (execute_sel) {
				result.Slice(input.data[plan.source_index], *execute_sel, count);
			} else {
				result.Reference(input.data[plan.source_index]);
			}
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::CONSTANT) {
			result.Reference(plan.constant_value, count_t(count));
			result.Flatten();
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::PREDICATE) {
			auto &predicate_sources = adapter_scratch.predicate_sources;
			predicate_sources.Prepare(&input, expr.input_source_indices);

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.Reset(count);
			result_validity.EnsureWritable();
			result_validity.SetAllValid(count);

			SljitNativePredicateInput native_input;
			native_input.source_data = predicate_sources.source_data.data();
			native_input.source_sel = predicate_sources.source_sel.data();
			native_input.source_validity = predicate_sources.source_validity.data();
			native_input.sources_all_valid = predicate_sources.sources_all_valid;
			native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result));
			native_input.result_validity = result_validity.GetData();
			native_input.true_sel = nullptr;
			native_input.false_sel = nullptr;
			native_input.selected_count = 0;
			native_input.count = count;
			expr.predicate_function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::CONSTANT_OR_NULL) {
			auto constant = plan.constant_or_null.guard_has_null_constant || plan.constant_or_null.constant.IsNull()
			                    ? Value(plan.return_type)
			                    : plan.constant_or_null.constant;
			result.Reference(constant, count_t(count));
			result.Flatten();
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.EnsureWritable();
			if (!constant.IsNull()) {
				result_validity.SetAllValid(count);
				auto &predicate_sources = adapter_scratch.predicate_sources;
				predicate_sources.Prepare(&input, expr.input_source_indices);

				SljitNativePredicateInput native_input;
				native_input.source_data = predicate_sources.source_data.data();
				native_input.source_sel = predicate_sources.source_sel.data();
				native_input.source_validity = predicate_sources.source_validity.data();
				native_input.sources_all_valid = predicate_sources.sources_all_valid;
				native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
				native_input.result_data = nullptr;
				native_input.result_validity = result_validity.GetData();
				native_input.true_sel = nullptr;
				native_input.false_sel = nullptr;
				native_input.selected_count = 0;
				native_input.count = count;
				expr.predicate_function(&native_input);
				if (native_input.error) {
					std::rethrow_exception(native_input.error);
				}
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE ||
		    plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
			SljitNativeVectorInput native_input;
			adapter_scratch.PrepareExpressionTree(input, expr, native_input, execute_sel, count);
			auto result_kind = plan.kind == SljitNativeRegionExpressionKind::EXPRESSION_TREE
			                       ? SljitNativeIntegerKind::DECIMAL64
			                       : plan.integer_kind;

			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.Reset(count);
			validity_t *result_validity_data = nullptr;
			if (adapter_scratch.source_can_have_null ||
			    plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
				result_validity.EnsureWritable();
				result_validity.SetAllValid(count);
				result_validity_data = result_validity.GetData();
			}

			native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
			native_input.result_data = NativeIntegerResultData(result, result_kind);
			native_input.result_validity = result_validity_data;
			native_input.overflow_message = expr.overflow_message.c_str();
			native_input.query_location = plan.query_location;
			native_input.count = count;
			expr.function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE) {
			UnifiedVectorFormat source_format;
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.Reset(count);
			result_validity.EnsureWritable();
			result_validity.SetAllValid(count);

			auto source_data = UnifiedVectorFormat::GetData<int64_t>(source_format);
			auto result_data = FlatVector::GetDataMutable<double>(result);
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
				auto source_idx = source_format.sel ? source_format.sel->get_index(input_idx) : input_idx;
				if (!source_format.validity.RowIsValid(source_idx)) {
					result_validity.SetInvalid(row_idx);
					continue;
				}
				result_data[row_idx] = static_cast<double>(source_data[source_idx]) / plan.double_constant;
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		if (plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP) {
			UnifiedVectorFormat source_format;
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &result_validity = FlatVector::ValidityMutable(result);
			result_validity.Reset(count);
			result_validity.EnsureWritable();
			result_validity.SetAllValid(count);

			auto source_data = UnifiedVectorFormat::GetData<hugeint_t>(source_format);
			auto result_data = FlatVector::GetDataMutable<hugeint_t>(result);
			hugeint_t scale_factor(plan.constant);
			for (idx_t row_idx = 0; row_idx < count; row_idx++) {
				auto input_idx = execute_sel ? execute_sel->get_index(row_idx) : row_idx;
				auto source_idx = source_format.sel ? source_format.sel->get_index(input_idx) : input_idx;
				if (!source_format.validity.RowIsValid(source_idx)) {
					result_validity.SetInvalid(row_idx);
					continue;
				}
				hugeint_t scaled;
				if (!Hugeint::TryMultiply(source_data[source_idx], scale_factor, scaled)) {
					throw OutOfRangeException("Overflow in DECIMAL128 scale-up");
				}
				result_data[row_idx] = scaled;
			}
			FlatVector::SetSize(result, count_t(count));
			return;
		}
		D_ASSERT(plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		         plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
		         plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
		         plan.kind == SljitNativeRegionExpressionKind::DECIMAL64_TO_DOUBLE ||
		         plan.kind == SljitNativeRegionExpressionKind::DECIMAL128_SCALE_UP ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGER_BETWEEN ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS ||
		         plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR ||
		         plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ||
		         plan.kind == SljitNativeRegionExpressionKind::NULL_CHECK);
		UnifiedVectorFormat source_format;
		UnifiedVectorFormat right_source_format;
		input.data[plan.source_index].ToUnifiedFormat(source_format);
		auto has_right_source = plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		                        plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES ||
		                        plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES ||
		                        plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ||
		                        (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE &&
		                         plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE);
		if (has_right_source) {
			auto right_source_index = plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE
			                              ? plan.guard_source_index
			                              : plan.right_source_index;
			D_ASSERT(right_source_index < input.ColumnCount());
			input.data[right_source_index].ToUnifiedFormat(right_source_format);
		}
		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto &result_validity = FlatVector::ValidityMutable(result);
		result_validity.Reset(count);
		validity_t *result_validity_data = nullptr;
		if ((plan.kind == SljitNativeRegionExpressionKind::INTEGER_IN_LIST && plan.list_has_null) ||
		    (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST && plan.try_cast) ||
		    (plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST && plan.try_cast) ||
		    plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR ||
		    (plan.kind != SljitNativeRegionExpressionKind::NULL_CHECK &&
		     (source_format.validity.CanHaveNull() ||
		      (has_right_source && right_source_format.validity.CanHaveNull())))) {
			result_validity.EnsureWritable();
			result_validity.SetAllValid(count);
			result_validity_data = result_validity.GetData();
		}

		SljitNativeVectorInput native_input;
		if (plan.kind == SljitNativeRegionExpressionKind::NULL_CHECK) {
			native_input.source_data = nullptr;
		} else if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		           plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS) {
			native_input.source_data = source_format.data;
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
			native_input.source_data = NativeUnsignedIntegerSourceData(source_format, plan.unsigned_source_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
		           plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
			native_input.source_data = NativeSignedIntegerSourceData(source_format, plan.cast_source_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
			native_input.source_data = NativeIntegerSourceData(source_format, SljitNativeIntegerKind::INT32);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
		           plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			native_input.source_data = source_format.data;
		} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
			native_input.source_data = source_format.data;
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE) {
			native_input.source_data = NativeSignedIntegerSourceData(source_format, plan.signed_integer_width);
		} else {
			native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
		}
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE &&
		    plan.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE) {
			native_input.right_source_data =
			    NativeSignedIntegerSourceData(right_source_format, plan.signed_integer_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			native_input.right_source_data = right_source_format.data;
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES) {
			native_input.right_source_data = NativeIntegerSourceData(right_source_format, plan.integer_kind);
		} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
			native_input.right_source_data =
			    NativeIntegerSourceData(right_source_format, SljitNativeIntegerKind::INT64);
		} else {
			native_input.right_source_data = nullptr;
		}
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.source_sel = source_format.sel ? source_format.sel->data() : nullptr;
		native_input.right_source_sel =
		    has_right_source && right_source_format.sel ? right_source_format.sel->data() : nullptr;
		native_input.source_validity = source_format.validity.GetData();
		native_input.right_source_validity = has_right_source ? right_source_format.validity.GetData() : nullptr;
		native_input.constants = plan.constants.data();
		native_input.constant =
		    plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE ? plan.guard_constant : plan.constant;
		native_input.double_constant = plan.double_constant;
		native_input.source_double_scale = plan.double_source_scale;
		native_input.right_source_double_scale = plan.double_right_source_scale;
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST) {
			native_input.result_data = NativeUnsignedIntegerResultData(result, plan.unsigned_cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_COALESCE) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.signed_integer_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS ||
		           plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS) {
			native_input.result_data = FlatVector::GetDataMutable(result);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS) {
			native_input.result_data = NativeUnsignedIntegerResultData(result, plan.unsigned_cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
			native_input.result_data = NativeSignedIntegerResultData(result, plan.cast_target_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::DATE_YEAR) {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<int64_t>(result));
		} else if (plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT ||
		           plan.kind == SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES) {
			if (plan.return_type.InternalType() == PhysicalType::FLOAT) {
				native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<float>(result));
			} else {
				native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<double>(result));
			}
		} else if (plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE) {
			native_input.result_data = FlatVector::GetDataMutable(result);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			native_input.result_data = NativeIntegerResultData(result, plan.integer_kind);
		} else {
			native_input.result_data = reinterpret_cast<data_ptr_t>(FlatVector::GetDataMutable<bool>(result));
		}
		native_input.result_vector =
		    plan.kind == SljitNativeRegionExpressionKind::STRING_DECOMPRESS ? &result : nullptr;
		native_input.result_validity = result_validity_data;
		native_input.true_sel = nullptr;
		native_input.false_sel = nullptr;
		native_input.selected_count = 0;
		native_input.overflow_message =
		    plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT ||
		            plan.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES ||
		            plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST ||
		            plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST
		        ? expr.overflow_message.c_str()
		        : nullptr;
		native_input.error_message = plan.kind == SljitNativeRegionExpressionKind::ERROR_GUARDED_REFERENCE
		                                 ? plan.error_message.c_str()
		                                 : nullptr;
		native_input.query_location = plan.query_location;
		native_input.overflow_value = 0;
		native_input.string_decompress_source_size = plan.string_decompress_source_size;
		native_input.active_source_index = 0;
		native_input.active_result_index = 0;
		native_input.count = count;
		native_input.has_error = false;
		auto use_flat_function = expr.flat_function && !execute_sel &&
		                         SljitUnifiedFormatHasIdentitySelection(source_format) &&
		                         source_format.validity.CannotHaveNull() &&
		                         (!has_right_source || (SljitUnifiedFormatHasIdentitySelection(right_source_format) &&
		                                                right_source_format.validity.CannotHaveNull()));
		auto vector_function = use_flat_function ? expr.flat_function : expr.function;
		vector_function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		FlatVector::SetSize(result, count_t(count));
	}

	void ExecutePrimitiveAggregatePayloadUpdate(SljitExecutableRegionExpression &payload,
	                                            SljitNativeAggregateUpdateFunction function,
	                                            const ExecutionPrimitiveAggregateUpdateLane &lane, DataChunk &input,
	                                            const SelectionVector *execute_sel, idx_t count,
	                                            SljitExpressionAdapterScratch &adapter_scratch,
	                                            optional_ptr<Vector> grouped_state_addresses = nullptr) {
		if (!function) {
			throw InternalException("SLJIT aggregate primitive payload update is missing generated code");
		}
		if (lane.kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (grouped_state_addresses) {
				if (!lane.ready || lane.state_size == 0) {
					auto blocker = lane.blocker.empty() ? "aggregate-count-star-grouped-lane-incomplete" : lane.blocker;
					throw InternalException("SLJIT grouped aggregate count-star lane is incomplete: %s",
					                        blocker.c_str());
				}
			} else if (!lane.ready || !lane.sum_int64_value || !lane.row_count) {
				auto blocker = lane.blocker.empty() ? "aggregate-count-star-lane-incomplete" : lane.blocker;
				throw InternalException("SLJIT aggregate count-star lane is incomplete: %s", blocker.c_str());
			}

			SljitNativeVectorInput native_input;
			native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
			native_input.count = count;
			native_input.aggregate_int64_value = lane.sum_int64_value;
			native_input.aggregate_row_count = lane.row_count;
			if (grouped_state_addresses) {
				grouped_state_addresses->Flatten();
				native_input.aggregate_state_addresses = FlatVector::GetData<uintptr_t>(*grouped_state_addresses);
				native_input.aggregate_state_offset = lane.state_offset;
				native_input.aggregate_state_value_offset = lane.state_value_offset;
			}
			native_input.has_error = false;
			function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			return;
		}
		if (grouped_state_addresses) {
			if (!lane.ready || lane.state_size == 0) {
				auto blocker = lane.blocker.empty() ? "aggregate-primitive-grouped-lane-incomplete" : lane.blocker;
				throw InternalException("SLJIT grouped aggregate primitive lane is incomplete: %s", blocker.c_str());
			}
		} else {
			auto has_sum_state = (AggregatePrimitiveUpdateUsesInt64State(lane.kind) && lane.sum_int64_value) ||
			                     (AggregatePrimitiveUpdateUsesHugeintState(lane.kind) && lane.sum_hugeint_value) ||
			                     (AggregatePrimitiveUpdateUsesDoubleState(lane.kind) && lane.sum_double_value);
			if (!lane.ready || !has_sum_state || !lane.state_is_set || !lane.row_count) {
				auto blocker = lane.blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane.blocker;
				throw InternalException("SLJIT aggregate primitive lane is incomplete: %s", blocker.c_str());
			}
		}
		auto &plan = payload.plan;
		if (plan.return_type.InternalType() != lane.payload_type) {
			throw InternalException("SLJIT aggregate primitive payload type mismatch");
		}

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.count = count;
		native_input.aggregate_int64_value = lane.sum_int64_value;
		native_input.aggregate_hugeint_value = lane.sum_hugeint_value;
		native_input.aggregate_double_value = lane.sum_double_value;
		native_input.aggregate_state_is_set = lane.state_is_set;
		native_input.aggregate_row_count = lane.row_count;
		if (grouped_state_addresses) {
			grouped_state_addresses->Flatten();
			native_input.aggregate_state_addresses = FlatVector::GetData<uintptr_t>(*grouped_state_addresses);
			native_input.aggregate_state_offset = lane.state_offset;
			native_input.aggregate_state_value_offset = lane.state_value_offset;
			native_input.aggregate_state_is_set_offset = lane.state_is_set_offset;
		}
		native_input.has_error = false;

		UnifiedVectorFormat source_format;
		UnifiedVectorFormat right_source_format;

		switch (plan.kind) {
		case SljitNativeRegionExpressionKind::REFERENCE:
			if (plan.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT aggregate primitive reference source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			native_input.source_data = plan.return_type.InternalType() == PhysicalType::DOUBLE
			                               ? source_format.data
			                               : NativeIntegerSourceData(source_format, plan.integer_kind);
			native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
			native_input.source_validity = source_format.validity.GetData();
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
			if (plan.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT aggregate primitive binary source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
			native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
			native_input.source_validity = source_format.validity.GetData();
			native_input.constant = plan.constant;
			break;
		case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
			if (plan.source_index >= input.ColumnCount() || plan.right_source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT aggregate primitive binary source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			input.data[plan.right_source_index].ToUnifiedFormat(right_source_format);
			native_input.source_data = NativeIntegerSourceData(source_format, plan.integer_kind);
			native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
			native_input.source_validity = source_format.validity.GetData();
			native_input.right_source_data = NativeIntegerSourceData(right_source_format, plan.integer_kind);
			native_input.right_source_sel = SljitNormalizedSourceSelectionData(right_source_format);
			native_input.right_source_validity = right_source_format.validity.GetData();
			break;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
			if (plan.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT aggregate primitive double binary source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			native_input.source_data = source_format.data;
			native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
			native_input.source_validity = source_format.validity.GetData();
			native_input.double_constant = plan.double_constant;
			native_input.source_double_scale = plan.double_source_scale;
			break;
		case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
			if (plan.source_index >= input.ColumnCount() || plan.right_source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT aggregate primitive double binary source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_format);
			input.data[plan.right_source_index].ToUnifiedFormat(right_source_format);
			native_input.source_data = source_format.data;
			native_input.source_sel = SljitNormalizedSourceSelectionData(source_format);
			native_input.source_validity = source_format.validity.GetData();
			native_input.right_source_data = right_source_format.data;
			native_input.right_source_sel = SljitNormalizedSourceSelectionData(right_source_format);
			native_input.right_source_validity = right_source_format.validity.GetData();
			native_input.source_double_scale = plan.double_source_scale;
			native_input.right_source_double_scale = plan.double_right_source_scale;
			break;
		case SljitNativeRegionExpressionKind::EXPRESSION_TREE:
		case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE: {
			if (!plan.expression_tree) {
				throw InternalException("SLJIT aggregate primitive expression-tree payload is missing IR");
			}
			adapter_scratch.PrepareExpressionTree(input, payload, native_input, execute_sel, count);
			break;
		}
		default:
			throw InternalException("SLJIT aggregate primitive payload has no runtime input adapter");
		}

		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	static bool FusedAggregatePayloadsUseTypedExpressionTrees(vector<SljitExecutableRegionExpression> &payloads,
	                                                          const vector<ExecutionRegionAggregateInput> &aggregates) {
		if (payloads.size() != aggregates.size()) {
			return false;
		}
		bool has_typed_payload = false;
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				continue;
			}
			if (payloads[payload_idx].plan.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE) {
				has_typed_payload = true;
				continue;
			}
			if (payloads[payload_idx].plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				continue;
			}
			return false;
		}
		return has_typed_payload;
	}

	void ExecuteFusedTypedExpressionAggregatePayloadUpdate(
	    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
	    const vector<ExecutionRegionAggregateInput> &aggregates,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, DataChunk &input,
	    const SelectionVector *execute_sel, idx_t count, SljitAggregatePayloadAdapterScratch &adapter_scratch) {
		optional_ptr<const vector<idx_t>> combined_sources;
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				continue;
			}
			if (payloads[payload_idx].input_source_indices.empty()) {
				throw InternalException("SLJIT fused typed aggregate payload is missing combined sources");
			}
			if (!combined_sources) {
				combined_sources = payloads[payload_idx].input_source_indices;
			} else if (*combined_sources != payloads[payload_idx].input_source_indices) {
				throw InternalException("SLJIT fused typed aggregate payload sources are not normalized");
			}
		}
		if (!combined_sources) {
			throw InternalException("SLJIT fused typed aggregate payload has no typed payloads");
		}

		adapter_scratch.PrepareFiltered(combined_sources->size(), aggregates.size());
		auto &source_formats = adapter_scratch.source_formats;
		auto &source_data = adapter_scratch.source_data;
		auto &source_sel = adapter_scratch.source_sel;
		auto &source_validity = adapter_scratch.source_validity;
		auto &aggregate_int64_values = adapter_scratch.aggregate_int64_values;
		auto &aggregate_hugeint_values = adapter_scratch.aggregate_hugeint_values;
		auto &aggregate_state_is_sets = adapter_scratch.aggregate_state_is_sets;
		auto &aggregate_row_counts = adapter_scratch.aggregate_row_counts;

		for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			auto lane = lanes[payload_idx];
			if (!lane) {
				throw InternalException("SLJIT fused typed aggregate primitive lane missing for aggregate %llu",
				                        static_cast<unsigned long long>(aggregates[payload_idx].aggregate_index));
			}
			auto &aggregate = aggregates[payload_idx];
			if (lane->kind != aggregate.primitive_update_kind) {
				throw InternalException("SLJIT fused typed aggregate primitive lane kind mismatch");
			}
			if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				if (!lane->ready || !lane->sum_int64_value || !lane->row_count) {
					auto blocker = lane->blocker.empty() ? "aggregate-count-star-lane-incomplete" : lane->blocker;
					throw InternalException("SLJIT fused typed aggregate count-star lane is incomplete: %s",
					                        blocker.c_str());
				}
				aggregate_int64_values[payload_idx] = lane->sum_int64_value;
				aggregate_row_counts[payload_idx] = lane->row_count;
				continue;
			}
			if (lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
			    lane->kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
				throw InternalException("SLJIT fused typed aggregate primitive lane has unsupported state kind");
			}
			if (payloads[payload_idx].plan.return_type.InternalType() != lane->payload_type) {
				throw InternalException("SLJIT fused typed aggregate primitive payload type mismatch");
			}
			if (payloads[payload_idx].plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				if (payloads[payload_idx].plan.source_index >= combined_sources->size()) {
					throw InternalException("SLJIT fused typed aggregate reference source is out of range");
				}
			} else if (payloads[payload_idx].plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
			           !payloads[payload_idx].plan.expression_tree) {
				throw InternalException("SLJIT fused typed aggregate payload is unsupported");
			}
			const auto has_sum_state =
			    (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64 && lane->sum_int64_value) ||
			    (lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT && lane->sum_hugeint_value);
			if (!lane->ready || !has_sum_state || !lane->state_is_set || !lane->row_count) {
				auto blocker = lane->blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT fused typed aggregate primitive lane is incomplete: %s",
				                        blocker.c_str());
			}
			if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
				aggregate_int64_values[payload_idx] = lane->sum_int64_value;
			} else {
				aggregate_hugeint_values[payload_idx] = lane->sum_hugeint_value;
			}
			aggregate_state_is_sets[payload_idx] = lane->state_is_set;
			aggregate_row_counts[payload_idx] = lane->row_count;
		}

		bool flat_no_selection = execute_sel == nullptr;
		bool flat_all_valid = execute_sel == nullptr;
		bool all_valid = true;
		for (idx_t source_idx = 0; source_idx < combined_sources->size(); source_idx++) {
			auto input_index = (*combined_sources)[source_idx];
			if (input_index >= input.ColumnCount()) {
				throw InternalException("SLJIT fused typed aggregate expression-tree source is out of range");
			}
			input.data[input_index].ToUnifiedFormat(source_formats[source_idx]);
			source_data[source_idx] =
			    SljitTypedExpressionTreeSourceData(source_formats[source_idx], input.data[input_index].GetType());
			source_sel[source_idx] = SljitNormalizedSourceSelectionData(source_formats[source_idx]);
			source_validity[source_idx] = SljitNormalizedSourceValidityData(source_formats[source_idx],
			                                                                source_sel[source_idx], execute_sel, count);
			flat_no_selection = flat_no_selection && source_sel[source_idx] == nullptr;
			flat_all_valid =
			    flat_all_valid && source_sel[source_idx] == nullptr && source_validity[source_idx] == nullptr;
			all_valid = all_valid && source_validity[source_idx] == nullptr;
		}

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.source_data_array = source_data.data();
		native_input.source_sel_array = source_sel.data();
		native_input.source_validity_array = source_validity.data();
		native_input.expression_tree_flat_no_selection = flat_no_selection;
		native_input.expression_tree_flat_all_valid = flat_all_valid;
		native_input.expression_tree_all_valid = all_valid;
		native_input.aggregate_int64_values = aggregate_int64_values.data();
		native_input.aggregate_hugeint_values = aggregate_hugeint_values.data();
		native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
		native_input.aggregate_row_counts = aggregate_row_counts.data();
		native_input.count = count;
		native_input.has_error = false;
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	void ExecuteFusedPrimitiveAggregatePayloadUpdate(vector<SljitExecutableRegionExpression> &payloads,
	                                                 SljitNativeAggregateUpdateFunction function,
	                                                 const vector<ExecutionRegionAggregateInput> &aggregates,
	                                                 const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                                 DataChunk &input, const SelectionVector *execute_sel, idx_t count,
	                                                 SljitAggregatePayloadAdapterScratch &adapter_scratch) {
		if (!function) {
			throw InternalException("SLJIT fused aggregate primitive payload update is missing generated code");
		}
		if (aggregates.size() != payloads.size()) {
			throw InternalException("SLJIT fused aggregate primitive payload count mismatch");
		}
		if (FusedAggregatePayloadsUseTypedExpressionTrees(payloads, aggregates)) {
			ExecuteFusedTypedExpressionAggregatePayloadUpdate(payloads, function, aggregates, lanes, input, execute_sel,
			                                                  count, adapter_scratch);
			return;
		}

		adapter_scratch.PrepareUngrouped(payloads.size());
		auto &source_formats = adapter_scratch.source_formats;
		auto &right_source_formats = adapter_scratch.right_source_formats;
		auto &source_data = adapter_scratch.source_data;
		auto &right_source_data = adapter_scratch.right_source_data;
		auto &source_sel = adapter_scratch.source_sel;
		auto &right_source_sel = adapter_scratch.right_source_sel;
		auto &source_validity = adapter_scratch.source_validity;
		auto &right_source_validity = adapter_scratch.right_source_validity;
		auto &aggregate_int64_values = adapter_scratch.aggregate_int64_values;
		auto &aggregate_state_is_sets = adapter_scratch.aggregate_state_is_sets;
		auto &aggregate_row_counts = adapter_scratch.aggregate_row_counts;
		auto &constants = adapter_scratch.constants;

		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto lane = lanes[payload_idx];
			if (!lane) {
				throw InternalException("SLJIT fused aggregate primitive lane missing for aggregate %llu",
				                        static_cast<unsigned long long>(aggregates[payload_idx].aggregate_index));
			}
			auto &plan = payloads[payload_idx].plan;
			if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				if (!lane->ready || !lane->sum_int64_value || !lane->row_count) {
					auto blocker = lane->blocker.empty() ? "aggregate-count-star-lane-incomplete" : lane->blocker;
					throw InternalException("SLJIT fused aggregate count-star lane is incomplete: %s", blocker.c_str());
				}
				aggregate_int64_values[payload_idx] = lane->sum_int64_value;
				aggregate_row_counts[payload_idx] = lane->row_count;
				continue;
			}
			if (lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
				throw InternalException("SLJIT fused aggregate primitive lane has unsupported state kind");
			}
			if (plan.return_type.InternalType() != lane->payload_type) {
				throw InternalException("SLJIT fused aggregate primitive payload type mismatch");
			}
			if (!lane->ready || !lane->sum_int64_value || !lane->state_is_set || !lane->row_count) {
				auto blocker = lane->blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT fused aggregate primitive lane is incomplete: %s", blocker.c_str());
			}
			aggregate_int64_values[payload_idx] = lane->sum_int64_value;
			aggregate_state_is_sets[payload_idx] = lane->state_is_set;
			aggregate_row_counts[payload_idx] = lane->row_count;

			switch (plan.kind) {
			case SljitNativeRegionExpressionKind::REFERENCE:
				if (plan.source_index >= input.ColumnCount()) {
					throw InternalException("SLJIT fused aggregate reference source is out of range");
				}
				input.data[plan.source_index].ToUnifiedFormat(source_formats[payload_idx]);
				source_data[payload_idx] = NativeIntegerSourceData(source_formats[payload_idx], plan.integer_kind);
				source_sel[payload_idx] = SljitNormalizedSourceSelectionData(source_formats[payload_idx]);
				source_validity[payload_idx] = SljitNormalizedSourceValidityData(
				    source_formats[payload_idx], source_sel[payload_idx], execute_sel, count);
				break;
			case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
				if (plan.source_index >= input.ColumnCount()) {
					throw InternalException("SLJIT fused aggregate binary source is out of range");
				}
				input.data[plan.source_index].ToUnifiedFormat(source_formats[payload_idx]);
				source_data[payload_idx] = NativeIntegerSourceData(source_formats[payload_idx], plan.integer_kind);
				source_sel[payload_idx] = SljitNormalizedSourceSelectionData(source_formats[payload_idx]);
				source_validity[payload_idx] = SljitNormalizedSourceValidityData(
				    source_formats[payload_idx], source_sel[payload_idx], execute_sel, count);
				constants[payload_idx] = plan.constant;
				break;
			case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
				if (plan.source_index >= input.ColumnCount() || plan.right_source_index >= input.ColumnCount()) {
					throw InternalException("SLJIT fused aggregate binary source is out of range");
				}
				input.data[plan.source_index].ToUnifiedFormat(source_formats[payload_idx]);
				input.data[plan.right_source_index].ToUnifiedFormat(right_source_formats[payload_idx]);
				source_data[payload_idx] = NativeIntegerSourceData(source_formats[payload_idx], plan.integer_kind);
				source_sel[payload_idx] = SljitNormalizedSourceSelectionData(source_formats[payload_idx]);
				source_validity[payload_idx] = SljitNormalizedSourceValidityData(
				    source_formats[payload_idx], source_sel[payload_idx], execute_sel, count);
				right_source_data[payload_idx] =
				    NativeIntegerSourceData(right_source_formats[payload_idx], plan.integer_kind);
				right_source_sel[payload_idx] = SljitNormalizedSourceSelectionData(right_source_formats[payload_idx]);
				right_source_validity[payload_idx] = SljitNormalizedSourceValidityData(
				    right_source_formats[payload_idx], right_source_sel[payload_idx], execute_sel, count);
				break;
			default:
				throw InternalException("SLJIT fused aggregate primitive payload has no runtime input adapter");
			}
		}

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.source_data_array = source_data.data();
		native_input.right_source_data_array = right_source_data.data();
		native_input.source_sel_array = SljitPointerArrayOrNull(source_sel);
		native_input.right_source_sel_array = SljitPointerArrayOrNull(right_source_sel);
		native_input.source_validity_array = SljitPointerArrayOrNull(source_validity);
		native_input.right_source_validity_array = SljitPointerArrayOrNull(right_source_validity);
		native_input.constants = constants.data();
		native_input.aggregate_int64_values = aggregate_int64_values.data();
		native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
		native_input.aggregate_row_counts = aggregate_row_counts.data();
		native_input.count = count;
		native_input.has_error = false;
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	void ExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
	    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
	    const vector<ExecutionRegionAggregateInput> &aggregates, const ExecutionRegionAggregateContract &contract,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes, DataChunk &input,
	    const uintptr_t *grouped_state_addresses, const sel_t *grouped_state_address_sel,
	    const SelectionVector *execute_sel, bool state_addresses_by_loop_index, idx_t count,
	    SljitAggregatePayloadAdapterScratch &adapter_scratch,
	    optional_ptr<const vector<idx_t>> input_source_indices_override = nullptr) {
		if (!function) {
			throw InternalException("SLJIT fused grouped aggregate primitive payload update is missing generated code");
		}
		if (!grouped_state_addresses) {
			throw InternalException(
			    "SLJIT fused grouped aggregate primitive payload update is missing state addresses");
		}
		if (aggregates.size() != payloads.size()) {
			throw InternalException("SLJIT fused grouped aggregate primitive payload count mismatch");
		}
		if (!contract.grouped_state_layout_ready || contract.grouped_state_offsets.size() < aggregates.size()) {
			throw InternalException("SLJIT fused grouped aggregate state layout is incomplete");
		}

		if (FusedAggregatePayloadsUseTypedExpressionTrees(payloads, aggregates)) {
			optional_ptr<const vector<idx_t>> combined_sources;
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					continue;
				}
				if (payloads[payload_idx].input_source_indices.empty()) {
					throw InternalException("SLJIT fused grouped typed aggregate payload is missing sources");
				}
				if (!combined_sources) {
					combined_sources = payloads[payload_idx].input_source_indices;
				} else if (*combined_sources != payloads[payload_idx].input_source_indices) {
					throw InternalException("SLJIT fused grouped typed aggregate payload sources are not normalized");
				}
			}
			if (!combined_sources) {
				throw InternalException("SLJIT fused grouped typed aggregate payload has no typed payloads");
			}
			if (input_source_indices_override) {
				if (input_source_indices_override->size() != combined_sources->size()) {
					throw InternalException(
					    "SLJIT fused grouped typed aggregate payload source override size mismatch");
				}
				combined_sources = input_source_indices_override;
			}

			adapter_scratch.PrepareGrouped(combined_sources->size());
			auto &source_formats = adapter_scratch.source_formats;
			auto &source_data = adapter_scratch.source_data;
			auto &source_sel = adapter_scratch.source_sel;
			auto &source_validity = adapter_scratch.source_validity;

			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				auto &aggregate = aggregates[payload_idx];
				auto lane = lanes[payload_idx];
				if (!lane) {
					throw InternalException(
					    "SLJIT fused grouped typed aggregate primitive lane missing for aggregate %llu",
					    static_cast<unsigned long long>(aggregate.aggregate_index));
				}
				if (aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
					throw InternalException("SLJIT fused grouped typed aggregate state offset is out of range");
				}
				if (!lane->ready || lane->state_size == 0) {
					auto blocker =
					    lane->blocker.empty() ? "aggregate-primitive-grouped-lane-incomplete" : lane->blocker;
					throw InternalException("SLJIT fused grouped typed aggregate primitive lane is incomplete: %s",
					                        blocker.c_str());
				}
				if (lane->state_offset != contract.grouped_state_offsets[aggregate.aggregate_index] ||
				    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
				    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
					throw InternalException("SLJIT fused grouped typed aggregate primitive lane layout mismatch");
				}
				auto &plan = payloads[payload_idx].plan;
				if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					if (aggregate.child_count != 0) {
						throw InternalException(
						    "SLJIT fused grouped typed count-star aggregate has unexpected payload");
					}
					continue;
				}
				if (!AggregatePrimitiveUpdateRequiresPayload(lane->kind)) {
					throw InternalException(
					    "SLJIT fused grouped typed aggregate primitive lane has unsupported state kind");
				}
				if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
					if (plan.source_index >= combined_sources->size()) {
						throw InternalException("SLJIT fused grouped typed aggregate reference source is out of range");
					}
				} else if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
				           !plan.expression_tree) {
					throw InternalException("SLJIT fused grouped typed aggregate payload is unsupported");
				}
				if (plan.return_type.InternalType() != lane->payload_type) {
					throw InternalException("SLJIT fused grouped typed aggregate primitive payload type mismatch");
				}
			}

			bool flat_no_selection = true;
			bool flat_all_valid = true;
			bool all_valid = true;
			for (idx_t source_idx = 0; source_idx < combined_sources->size(); source_idx++) {
				auto input_index = (*combined_sources)[source_idx];
				if (input_index >= input.ColumnCount()) {
					throw InternalException("SLJIT fused grouped typed aggregate source is out of range");
				}
				input.data[input_index].ToUnifiedFormat(source_formats[source_idx]);
				source_data[source_idx] =
				    SljitTypedExpressionTreeSourceData(source_formats[source_idx], input.data[input_index].GetType());
				source_sel[source_idx] = SljitNormalizedSourceSelectionData(source_formats[source_idx]);
				source_validity[source_idx] = SljitNormalizedSourceValidityData(
				    source_formats[source_idx], source_sel[source_idx], execute_sel, count);
				flat_no_selection = flat_no_selection && execute_sel == nullptr && source_sel[source_idx] == nullptr;
				flat_all_valid = flat_all_valid && execute_sel == nullptr && source_sel[source_idx] == nullptr &&
				                 source_validity[source_idx] == nullptr;
				all_valid = all_valid && source_validity[source_idx] == nullptr;
			}

			SljitNativeVectorInput native_input;
			native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
			native_input.source_data_array = source_data.data();
			native_input.source_sel_array = source_sel.data();
			native_input.source_validity_array = source_validity.data();
			native_input.expression_tree_flat_no_selection = flat_no_selection;
			native_input.expression_tree_flat_all_valid = flat_all_valid;
			native_input.expression_tree_all_valid = all_valid;
			native_input.aggregate_state_addresses = grouped_state_addresses;
			native_input.aggregate_state_address_sel = grouped_state_address_sel;
			native_input.aggregate_state_addresses_by_loop_index = state_addresses_by_loop_index;
			native_input.count = count;
			native_input.has_error = false;
			function(&native_input);
			if (native_input.error) {
				std::rethrow_exception(native_input.error);
			}
			return;
		}

		adapter_scratch.PrepareGrouped(payloads.size());
		auto &source_formats = adapter_scratch.source_formats;
		auto &source_data = adapter_scratch.source_data;
		auto &source_sel = adapter_scratch.source_sel;
		auto &source_validity = adapter_scratch.source_validity;

		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			auto lane = lanes[payload_idx];
			if (!lane) {
				throw InternalException("SLJIT fused grouped aggregate primitive lane missing for aggregate %llu",
				                        static_cast<unsigned long long>(aggregate.aggregate_index));
			}
			if (aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
				throw InternalException("SLJIT fused grouped aggregate state offset is out of range");
			}
			if (!lane->ready || lane->state_size == 0) {
				auto blocker = lane->blocker.empty() ? "aggregate-primitive-grouped-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT fused grouped aggregate primitive lane is incomplete: %s",
				                        blocker.c_str());
			}
			if (lane->state_offset != contract.grouped_state_offsets[aggregate.aggregate_index] ||
			    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
			    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
				throw InternalException("SLJIT fused grouped aggregate primitive lane layout mismatch");
			}
			auto &plan = payloads[payload_idx].plan;
			if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				if (aggregate.child_count != 0) {
					throw InternalException("SLJIT fused grouped count-star aggregate has unexpected payload");
				}
				continue;
			}
			if (!AggregatePrimitiveUpdateRequiresPayload(lane->kind)) {
				throw InternalException("SLJIT fused grouped aggregate primitive lane has unsupported state kind");
			}
			if (plan.kind != SljitNativeRegionExpressionKind::REFERENCE) {
				throw InternalException("SLJIT fused grouped aggregate payload has unsupported expression kind");
			}
			if (plan.return_type.InternalType() != lane->payload_type) {
				throw InternalException("SLJIT fused grouped aggregate primitive payload type mismatch");
			}
			if (plan.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT fused grouped aggregate reference source is out of range");
			}
			input.data[plan.source_index].ToUnifiedFormat(source_formats[payload_idx]);
			source_data[payload_idx] = NativeIntegerSourceData(source_formats[payload_idx], plan.integer_kind);
			source_sel[payload_idx] = SljitNormalizedSourceSelectionData(source_formats[payload_idx]);
			source_validity[payload_idx] = SljitNormalizedSourceValidityData(
			    source_formats[payload_idx], source_sel[payload_idx], execute_sel, count);
		}

		SljitNativeVectorInput native_input;
		native_input.execute_sel = execute_sel ? execute_sel->data() : nullptr;
		native_input.source_data_array = source_data.data();
		native_input.source_sel_array = SljitPointerArrayOrNull(source_sel);
		native_input.source_validity_array = SljitPointerArrayOrNull(source_validity);
		native_input.aggregate_state_addresses = grouped_state_addresses;
		native_input.aggregate_state_address_sel = grouped_state_address_sel;
		native_input.aggregate_state_addresses_by_loop_index = state_addresses_by_loop_index;
		native_input.count = count;
		native_input.has_error = false;
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	struct SljitGroupedStateAddressUpdateState {
		SljitNativeRegionKernel *kernel = nullptr;
		vector<SljitExecutableRegionExpression> *payloads = nullptr;
		SljitNativeAggregateUpdateFunction function = nullptr;
		const vector<ExecutionRegionAggregateInput> *aggregates = nullptr;
		const ExecutionRegionAggregateContract *contract = nullptr;
		const vector<const ExecutionPrimitiveAggregateUpdateLane *> *lanes = nullptr;
		DataChunk *input = nullptr;
		SljitAggregatePayloadAdapterScratch *adapter_scratch = nullptr;
		optional_ptr<const vector<idx_t>> input_source_indices_override;
	};

	static void ExecuteSljitGroupedStateAddressUpdate(const uintptr_t *addresses, const sel_t *address_sel, idx_t count,
	                                                  void *state_p) {
		auto &state = *reinterpret_cast<SljitGroupedStateAddressUpdateState *>(state_p);
		if (!state.kernel || !state.payloads || !state.function || !state.aggregates || !state.contract ||
		    !state.lanes || !state.input || !state.adapter_scratch) {
			throw InternalException("SLJIT grouped state-address callback is incomplete");
		}
		state.kernel->ExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
		    *state.payloads, state.function, *state.aggregates, *state.contract, *state.lanes, *state.input, addresses,
		    address_sel, nullptr, false, count, *state.adapter_scratch, state.input_source_indices_override);
	}

	static void ExecuteSljitGroupedSelectedStateAddressUpdate(const uintptr_t *addresses, const sel_t *address_sel,
	                                                          const sel_t *execute_sel, idx_t count, void *state_p) {
		auto &state = *reinterpret_cast<SljitGroupedStateAddressUpdateState *>(state_p);
		if (!state.kernel || !state.payloads || !state.function || !state.aggregates || !state.contract ||
		    !state.lanes || !state.input || !state.adapter_scratch) {
			throw InternalException("SLJIT grouped selected state-address callback is incomplete");
		}
		SelectionVector execute_selection(const_cast<sel_t *>(execute_sel), execute_sel ? count : 0);
		const bool state_addresses_by_loop_index = execute_sel && !address_sel;
		state.kernel->ExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
		    *state.payloads, state.function, *state.aggregates, *state.contract, *state.lanes, *state.input, addresses,
		    address_sel, execute_sel ? &execute_selection : nullptr, state_addresses_by_loop_index, count,
		    *state.adapter_scratch, state.input_source_indices_override);
	}

	struct SljitPreaggregatedCountStarUpdateState {
		const ExecutionPrimitiveAggregateUpdateLane *lane = nullptr;
		const int64_t *counts = nullptr;
	};

	static inline idx_t SljitSelectedGroupedStateAddressIndex(const sel_t *address_sel, const sel_t *execute_sel,
	                                                          idx_t idx, idx_t row_idx) {
		if (address_sel) {
			return address_sel[row_idx];
		}
		return execute_sel ? idx : row_idx;
	}

	static void ExecuteSljitPreaggregatedCountStarUpdate(const uintptr_t *addresses, const sel_t *address_sel,
	                                                     const sel_t *execute_sel, idx_t count, void *state_p) {
		auto &state = *reinterpret_cast<SljitPreaggregatedCountStarUpdateState *>(state_p);
		if (!addresses || !state.lane || !state.counts || !state.lane->ready ||
		    state.lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR || state.lane->state_size == 0) {
			throw InternalException("SLJIT preaggregated count-star grouped update state is incomplete");
		}
		for (idx_t idx = 0; idx < count; idx++) {
			const auto row_idx = execute_sel ? execute_sel[idx] : idx;
			const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
			auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
			auto state_base = state_address + state.lane->state_offset;
			auto count_ptr = reinterpret_cast<int64_t *>(state_base + state.lane->state_value_offset);
			*count_ptr += state.counts[row_idx];
		}
	}

	struct SljitPreaggregatedPrimitiveUpdateState {
		const vector<const ExecutionPrimitiveAggregateUpdateLane *> *lanes = nullptr;
		const vector<SljitPreaggregatedPrimitivePayloadDeltas> *payloads = nullptr;
	};

	static void ExecuteSljitPreaggregatedPrimitiveUpdate(const uintptr_t *addresses, const sel_t *address_sel,
	                                                     const sel_t *execute_sel, idx_t count, void *state_p) {
		auto &state = *reinterpret_cast<SljitPreaggregatedPrimitiveUpdateState *>(state_p);
		if (!addresses || !state.lanes || !state.payloads || state.lanes->size() != state.payloads->size()) {
			throw InternalException("SLJIT preaggregated primitive grouped update state is incomplete");
		}
		auto &lanes = *state.lanes;
		auto &payloads = *state.payloads;
		for (idx_t idx = 0; idx < count; idx++) {
			const auto row_idx = execute_sel ? execute_sel[idx] : idx;
			const auto address_idx = SljitSelectedGroupedStateAddressIndex(address_sel, execute_sel, idx, row_idx);
			auto state_address = reinterpret_cast<data_ptr_t>(addresses[address_idx]);
			for (idx_t payload_idx = 0; payload_idx < lanes.size(); payload_idx++) {
				auto lane = lanes[payload_idx];
				if (!lane || !lane->ready || lane->state_size == 0) {
					throw InternalException("SLJIT preaggregated primitive grouped update lane is incomplete");
				}
				auto state_base = state_address + lane->state_offset;
				auto value_ptr = state_base + lane->state_value_offset;
				auto &payload = payloads[payload_idx];
				switch (lane->kind) {
				case AggregatePrimitiveUpdateKind::COUNT_STAR: {
					if (row_idx >= payload.int64_values.size()) {
						throw InternalException("SLJIT preaggregated count-star delta is out of range");
					}
					auto count_ptr = reinterpret_cast<int64_t *>(value_ptr);
					*count_ptr += payload.int64_values[row_idx];
					break;
				}
				case AggregatePrimitiveUpdateKind::SUM_INT64: {
					if (row_idx >= payload.int64_values.size()) {
						throw InternalException("SLJIT preaggregated int64 sum delta is out of range");
					}
					auto sum = reinterpret_cast<int64_t *>(value_ptr);
					*sum += payload.int64_values[row_idx];
					auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
					*state_is_set = true;
					break;
				}
				case AggregatePrimitiveUpdateKind::SUM_HUGEINT: {
					if (row_idx >= payload.hugeint_values.size()) {
						throw InternalException("SLJIT preaggregated hugeint sum delta is out of range");
					}
					auto sum = reinterpret_cast<hugeint_t *>(value_ptr);
					*sum += payload.hugeint_values[row_idx];
					auto state_is_set = reinterpret_cast<bool *>(state_base + lane->state_is_set_offset);
					*state_is_set = true;
					break;
				}
				default:
					throw InternalException("Unsupported SLJIT preaggregated primitive update kind");
				}
			}
		}
	}

	static void ExecuteSljitPreaggregatedPrimitiveAddressUpdate(const uintptr_t *addresses, const sel_t *address_sel,
	                                                            idx_t count, void *state_p) {
		ExecuteSljitPreaggregatedPrimitiveUpdate(addresses, address_sel, nullptr, count, state_p);
	}

	void ExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
	    vector<SljitExecutableRegionExpression> &payloads, SljitNativeAggregateUpdateFunction function,
	    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<ExecutionRegionGroupInput> &groups,
	    const vector<SljitNativeRegionExpressionPlan> &group_expressions,
	    const ExecutionRegionAggregateContract &contract,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	    const ExecutionPerfectAggregateStateAddressLayout &layout, DataChunk &input, const SelectionVector *execute_sel,
	    idx_t count, SljitAggregatePayloadAdapterScratch &adapter_scratch) {
		if (!function) {
			throw InternalException("SLJIT fused perfect-hash aggregate update is missing generated code");
		}
		if (!layout.ready || !layout.data || !layout.group_is_set || layout.total_groups == 0 ||
		    layout.tuple_size == 0) {
			auto blocker = layout.blocker.empty() ? "perfect-hash-state-layout-missing" : layout.blocker;
			throw InternalException("SLJIT fused perfect-hash aggregate state layout is incomplete: %s",
			                        blocker.c_str());
		}
		if (!contract.grouped_state_layout_ready || contract.grouped_state_offsets.size() < aggregates.size()) {
			throw InternalException("SLJIT fused perfect-hash aggregate state contract is incomplete");
		}
		if (aggregates.size() != payloads.size()) {
			throw InternalException("SLJIT fused perfect-hash aggregate primitive payload count mismatch");
		}
		if (contract.perfect_required_bits.size() != groups.size() ||
		    contract.perfect_group_minima.size() != groups.size()) {
			throw InternalException("SLJIT fused perfect-hash aggregate group contract is incomplete");
		}
		if (!group_expressions.empty() && group_expressions.size() != groups.size()) {
			throw InternalException("SLJIT fused perfect-hash aggregate group expression count mismatch");
		}

		const bool typed_payloads = FusedAggregatePayloadsUseTypedExpressionTrees(payloads, aggregates);
		optional_ptr<const vector<idx_t>> combined_sources;
		if (typed_payloads) {
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					continue;
				}
				if (payloads[payload_idx].input_source_indices.empty()) {
					throw InternalException("SLJIT fused perfect-hash typed aggregate payload is missing sources");
				}
				if (!combined_sources) {
					combined_sources = payloads[payload_idx].input_source_indices;
				} else if (*combined_sources != payloads[payload_idx].input_source_indices) {
					throw InternalException(
					    "SLJIT fused perfect-hash typed aggregate payload sources are not normalized");
				}
			}
			if (!combined_sources) {
				throw InternalException("SLJIT fused perfect-hash typed aggregate payload has no typed payloads");
			}
			adapter_scratch.PreparePerfectHash(combined_sources->size(), groups.size());
		} else {
			adapter_scratch.PreparePerfectHash(payloads.size(), groups.size());
		}
		auto &group_formats = adapter_scratch.group_formats;
		auto &group_data = adapter_scratch.group_data;
		auto &group_sel = adapter_scratch.group_sel;
		auto &group_validity = adapter_scratch.group_validity;
		bool flat_all_valid = true;
		bool all_valid = true;
		for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
			auto &group = groups[group_idx];
			if (!group.supported_reference) {
				throw InternalException("SLJIT fused perfect-hash aggregate group source is unsupported");
			}
			SljitNativeRegionExpressionPlan reference_group;
			reference_group.kind = SljitNativeRegionExpressionKind::REFERENCE;
			reference_group.return_type = group.type;
			reference_group.source_index = group.input_index;
			auto &group_expression = group_expressions.empty() ? reference_group : group_expressions[group_idx];
			if (group_expression.return_type.InternalType() != group.type.InternalType() ||
			    group_expression.source_index >= input.ColumnCount()) {
				throw InternalException("SLJIT fused perfect-hash aggregate group expression is unsupported");
			}
			input.data[group_expression.source_index].ToUnifiedFormat(group_formats[group_idx]);
			if (group_expression.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				group_data[group_idx] =
				    NativeIntegerSourceData(group_formats[group_idx], SljitPerfectHashGroupIntegerKind(group.type));
			} else if (group_expression.kind == SljitNativeRegionExpressionKind::STRING_COMPRESS &&
			           group.type.InternalType() == PhysicalType::UINT8 &&
			           group_expression.string_compress_target_size == sizeof(uint8_t)) {
				group_data[group_idx] = reinterpret_cast<const_data_ptr_t>(group_formats[group_idx].data);
			} else {
				throw InternalException("SLJIT fused perfect-hash aggregate group expression is unsupported");
			}
			group_sel[group_idx] = SljitNormalizedSourceSelectionData(group_formats[group_idx]);
			group_validity[group_idx] =
			    SljitNormalizedSourceValidityData(group_formats[group_idx], group_sel[group_idx], execute_sel, count);
			flat_all_valid = flat_all_valid && group_sel[group_idx] == nullptr && group_validity[group_idx] == nullptr;
			all_valid = all_valid && group_validity[group_idx] == nullptr;
		}

		auto &source_formats = adapter_scratch.source_formats;
		auto &source_data = adapter_scratch.source_data;
		auto &source_sel = adapter_scratch.source_sel;
		auto &source_validity = adapter_scratch.source_validity;
		if (typed_payloads) {
			for (idx_t source_idx = 0; source_idx < combined_sources->size(); source_idx++) {
				auto input_index = (*combined_sources)[source_idx];
				if (input_index >= input.ColumnCount()) {
					throw InternalException("SLJIT fused perfect-hash typed aggregate source is out of range");
				}
				input.data[input_index].ToUnifiedFormat(source_formats[source_idx]);
				source_data[source_idx] =
				    SljitTypedExpressionTreeSourceData(source_formats[source_idx], input.data[input_index].GetType());
				source_sel[source_idx] = SljitNormalizedSourceSelectionData(source_formats[source_idx]);
				source_validity[source_idx] = SljitNormalizedSourceValidityData(
				    source_formats[source_idx], source_sel[source_idx], execute_sel, count);
				flat_all_valid =
				    flat_all_valid && source_sel[source_idx] == nullptr && source_validity[source_idx] == nullptr;
				all_valid = all_valid && source_validity[source_idx] == nullptr;
			}
		}
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			auto lane = lanes[payload_idx];
			if (!lane) {
				throw InternalException("SLJIT fused perfect-hash aggregate primitive lane missing for aggregate %llu",
				                        static_cast<unsigned long long>(aggregate.aggregate_index));
			}
			if (!lane->ready || lane->state_size == 0) {
				auto blocker = lane->blocker.empty() ? "aggregate-primitive-grouped-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT fused perfect-hash aggregate primitive lane is incomplete: %s",
				                        blocker.c_str());
			}
			if (lane->state_offset != contract.grouped_state_offsets[aggregate.aggregate_index] ||
			    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
			    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
				throw InternalException("SLJIT fused perfect-hash aggregate primitive lane layout mismatch");
			}
			auto &plan = payloads[payload_idx].plan;
			if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				if (aggregate.child_count != 0) {
					throw InternalException("SLJIT fused perfect-hash count-star aggregate has unexpected payload");
				}
				continue;
			}
			if (!AggregatePrimitiveUpdateRequiresPayload(lane->kind)) {
				throw InternalException("SLJIT fused perfect-hash aggregate primitive lane has unsupported state kind");
			}
			if (typed_payloads) {
				if (plan.kind == SljitNativeRegionExpressionKind::REFERENCE) {
					if (plan.source_index >= combined_sources->size()) {
						throw InternalException("SLJIT fused perfect-hash typed aggregate source is out of range");
					}
				} else if (plan.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
				           !plan.expression_tree) {
					throw InternalException("SLJIT fused perfect-hash typed aggregate payload is unsupported");
				}
			} else {
				if (plan.kind != SljitNativeRegionExpressionKind::REFERENCE ||
				    plan.source_index >= input.ColumnCount()) {
					throw InternalException("SLJIT fused perfect-hash aggregate payload source is unsupported");
				}
			}
			if (plan.return_type.InternalType() != lane->payload_type) {
				throw InternalException("SLJIT fused perfect-hash aggregate primitive payload type mismatch");
			}
			if (typed_payloads) {
				continue;
			}
			input.data[plan.source_index].ToUnifiedFormat(source_formats[payload_idx]);
			source_data[payload_idx] = NativeIntegerSourceData(source_formats[payload_idx], plan.integer_kind);
			source_sel[payload_idx] = SljitNormalizedSourceSelectionData(source_formats[payload_idx]);
			source_validity[payload_idx] = SljitNormalizedSourceValidityData(
			    source_formats[payload_idx], source_sel[payload_idx], execute_sel, count);
		}

		const auto native_execute_sel =
		    execute_sel ? execute_sel->data() : SljitCanonicalizeCommonSelection(source_sel, group_sel);
		const auto source_common_sel =
		    typed_payloads && !native_execute_sel ? SljitCanonicalizeCommonSourceSelection(source_sel) : nullptr;
		bool flat_no_selection = native_execute_sel == nullptr && source_common_sel == nullptr;
		flat_all_valid = native_execute_sel == nullptr && source_common_sel == nullptr;
		all_valid = true;
		for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
			flat_no_selection = flat_no_selection && group_sel[group_idx] == nullptr;
			flat_all_valid = flat_all_valid && group_sel[group_idx] == nullptr && group_validity[group_idx] == nullptr;
			all_valid = all_valid && group_validity[group_idx] == nullptr;
		}
		for (idx_t source_idx = 0; source_idx < source_sel.size(); source_idx++) {
			flat_no_selection = flat_no_selection && source_sel[source_idx] == nullptr;
			flat_all_valid =
			    flat_all_valid && source_sel[source_idx] == nullptr && source_validity[source_idx] == nullptr;
			all_valid = all_valid && source_validity[source_idx] == nullptr;
		}
		const auto group_selection_all_present = SljitAllSelectionsPresent(group_sel);

		SljitNativeVectorInput native_input;
		native_input.execute_sel = native_execute_sel;
		native_input.source_data_array = source_data.data();
		native_input.source_sel_array = typed_payloads ? source_sel.data() : SljitPointerArrayOrNull(source_sel);
		native_input.source_common_sel = source_common_sel;
		native_input.source_validity_array =
		    typed_payloads ? source_validity.data() : SljitPointerArrayOrNull(source_validity);
		native_input.group_data_array = group_data.data();
		native_input.group_sel_array = typed_payloads ? group_sel.data() : SljitPointerArrayOrNull(group_sel);
		native_input.group_validity_array =
		    typed_payloads ? group_validity.data() : SljitPointerArrayOrNull(group_validity);
		if (typed_payloads) {
			native_input.expression_tree_flat_no_selection = flat_no_selection;
			native_input.expression_tree_flat_all_valid = flat_all_valid;
			native_input.expression_tree_all_valid = all_valid;
			native_input.group_selection_all_present = group_selection_all_present;
		}
		native_input.perfect_hash_state_data = layout.data;
		native_input.perfect_hash_group_is_set = layout.group_is_set;
		native_input.perfect_hash_total_groups = layout.total_groups;
		native_input.perfect_hash_tuple_size = layout.tuple_size;
		native_input.perfect_hash_aggregate_state_offset = layout.aggregate_state_offset;
		native_input.error_message =
		    "Perfect hash aggregate group exceeded total groups; source statistics may be corrupt";
		native_input.count = count;
		native_input.has_error = false;
		function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	idx_t ApplyNativeHashJoinResidualPredicate(ExecutionRegionRuntime &runtime, SljitExecutableRegionOp &op,
	                                           const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
	                                           Vector &row_pointers, SelectionVector &match_selection, idx_t count,
	                                           DataChunk *residual_chunk, SelectionVector *residual_selection,
	                                           SelectionVector *compact_match_selection, Vector *compact_row_pointers,
	                                           SljitExpressionAdapterScratch *adapter_scratch,
	                                           optional_ptr<ExecutionOperatorStageRecorder> recorder) {
		if (!op.hash_join_probe.plan.residual_predicate) {
			return count;
		}
		if (!residual_chunk || !residual_selection || !compact_match_selection || !compact_row_pointers ||
		    !adapter_scratch) {
			throw InternalException("SLJIT native hash join residual predicate requires residual scratch state");
		}
		auto &residual_filter = op.hash_join_probe.residual_filter;
		if (count == 0) {
			return 0;
		}

		residual_chunk->Reset();
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "residual_source_chunk", count);
		ExecutionMaterializeHashJoinResidualSources(probe, input, row_pointers, match_selection, count, *residual_chunk,
		                                            recorder);

		const auto selected_count =
		    SelectExpression(residual_filter, *residual_chunk, *residual_selection, *adapter_scratch);
		compact_row_pointers->SetVectorType(VectorType::FLAT_VECTOR);
		auto row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(row_pointers);
		auto compact_row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(*compact_row_pointers);
		for (idx_t out_idx = 0; out_idx < selected_count; out_idx++) {
			auto dense_idx = residual_selection->get_index(out_idx);
			if (dense_idx >= count) {
				throw InternalException("SLJIT native hash join residual predicate selected row out of range");
			}
			compact_match_selection->set_index(out_idx, match_selection.get_index(dense_idx));
			compact_row_pointer_data[out_idx] = row_pointer_data[dense_idx];
		}
		for (idx_t out_idx = 0; out_idx < selected_count; out_idx++) {
			match_selection.set_index(out_idx, compact_match_selection->get_index(out_idx));
			row_pointer_data[out_idx] = compact_row_pointer_data[out_idx];
		}
		FlatVector::SetSize(row_pointers, count_t(selected_count));
		return selected_count;
	}

	static void MarkHashJoinBuildMatchesAfterResidual(const SljitNativeHashJoinProbePlan &plan, Vector &row_pointers,
	                                                  idx_t count) {
		if (!plan.mark_build_match_after_residual) {
			return;
		}
		auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto row_pointer = row_pointer_data[row_idx];
			if (!row_pointer) {
				throw InternalException("SLJIT native hash join residual build match has no row pointer");
			}
			row_pointer[plan.found_match_offset] = 1;
		}
	}

	static SljitRegularHashJoinProbeRuntimeTraits
	BuildRegularHashJoinProbeRuntimeTraits(bool source_selection_present, bool source_common_selection_present,
	                                       bool source_validity_present, bool rhs_keys_all_valid) {
		SljitRegularHashJoinProbeRuntimeTraits traits;
		if (!rhs_keys_all_valid || source_validity_present) {
			return traits;
		}
		if (!source_selection_present) {
			traits.input_kind = SljitRegularHashJoinProbeInputKind::FLAT_ALL_VALID;
		} else if (source_common_selection_present) {
			traits.input_kind = SljitRegularHashJoinProbeInputKind::SELECTED_ALL_VALID;
		}
		return traits;
	}

	const char *ExecuteFlatAllValidRegularHashJoinProbeVariant(ExecutionRegionRuntime &runtime,
	                                                           SljitExecutableHashJoinProbe &hash_join_probe,
	                                                           const ExecutionHashJoinTableLayout &layout,
	                                                           SljitNativeHashJoinProbeInput &native_input) {
		if (TryExecuteAllValidInt64PairNoChainProbe<false>(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_FLAT_ALL_VALID_INT64_PAIR_HASH_JOIN_PROBE_STAGE;
		}
		if (TryExecuteAllValidInt64PairChainProbe<false>(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_FLAT_ALL_VALID_INT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE;
		}
		if (TryExecuteAllValidSingleKeyNotEqualPredicateChainProbe<false>(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE;
		}
		if (TryExecuteFlatAllValidSingleKeyNoChainProbe(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE;
		}
		if (TryExecuteAllValidSingleKeyChainProbe<false>(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE;
		}
		auto function = EnsureFlatAllValidRegularHashJoinProbeCode(
		    runtime, hash_join_probe, layout.use_salt, layout.chains_longer_than_one, layout.dictionary_emission);
		function(&native_input);
		return SLJIT_GENERATED_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE;
	}

	const char *ExecuteSelectedAllValidRegularHashJoinProbeVariant(ExecutionRegionRuntime &runtime,
	                                                               SljitExecutableHashJoinProbe &hash_join_probe,
	                                                               const ExecutionHashJoinTableLayout &layout,
	                                                               SljitNativeHashJoinProbeInput &native_input) {
		if (TryExecuteAllValidInt64PairNoChainProbe<true>(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_SELECTED_ALL_VALID_INT64_PAIR_HASH_JOIN_PROBE_STAGE;
		}
		if (TryExecuteAllValidInt64PairChainProbe<true>(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_SELECTED_ALL_VALID_INT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE;
		}
		if (TryExecuteSelectedAllValidSingleKeyNoChainProbe(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE;
		}
		if (TryExecuteAllValidSingleKeyChainProbe<true>(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE;
		}
		if (TryExecuteAllValidSingleKeyNotEqualPredicateChainProbe<true>(hash_join_probe.plan, layout, native_input)) {
			return SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE;
		}
		auto function = EnsureSelectedAllValidRegularHashJoinProbeCode(
		    runtime, hash_join_probe, layout.use_salt, layout.chains_longer_than_one, layout.dictionary_emission);
		function(&native_input);
		return SLJIT_GENERATED_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE;
	}

	const char *ExecuteRegularHashJoinProbeVariant(ExecutionRegionRuntime &runtime,
	                                               const SljitRegularHashJoinProbeRuntimeTraits &traits,
	                                               SljitExecutableHashJoinProbe &hash_join_probe,
	                                               const ExecutionHashJoinTableLayout &layout,
	                                               SljitNativeHashJoinProbeInput &native_input) {
		if (traits.UsesFlatAllValidProbe()) {
			return ExecuteFlatAllValidRegularHashJoinProbeVariant(runtime, hash_join_probe, layout, native_input);
		}
		if (traits.UsesSelectedAllValidProbe()) {
			return ExecuteSelectedAllValidRegularHashJoinProbeVariant(runtime, hash_join_probe, layout, native_input);
		}
		EnsureRegularHashJoinProbeCode(runtime, hash_join_probe);
		hash_join_probe.function(&native_input);
		return SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_STAGE;
	}

	ExecutionOperatorBindResult BindNativeOperator(ExecutionOperatorRuntime &native_runtime,
	                                               SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                               SljitExecutableRegionOp &op, DataChunk &input,
	                                               const ExecutionRegionOperatorInfo &operator_info,
	                                               const char *blocker_prefix, const char *error_prefix,
	                                               ExecutionOperatorBinding *&binding_out, string &deferred_reason) {
		auto &binding = scratch.OperatorBinding(op_idx);
		binding_out = &binding;
		if (scratch.HasOperatorBinding(op_idx)) {
			return ExecutionOperatorBindResult::READY;
		}
		if (op.operator_index == DConstants::INVALID_INDEX) {
			throw InternalException("%s is missing an operator index", error_prefix);
		}
		auto bind_result = native_runtime.BindOperator(op.operator_index, input, operator_info, binding);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			deferred_reason = binding.blocker.empty() ? string(blocker_prefix) : binding.blocker;
			return bind_result;
		}
		if (bind_result != ExecutionOperatorBindResult::READY) {
			auto blocker = binding.blocker.empty() ? string("unknown") : binding.blocker;
			throw InternalException("%s operator binding failed: %s", error_prefix, blocker);
		}
		scratch.MarkOperatorBindingReady(op_idx);
		return bind_result;
	}

	ExecutionOperatorBindResult
	ExecutePerfectHashJoinProbe(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
	                            const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output,
	                            SelectionVector &match_selection, SelectionVector &build_selection,
	                            SljitHashJoinProbeDrainState &state, bool selection_only = false) {
		if (!op.hash_join_probe.plan.perfect_hash_probe) {
			throw InternalException("SLJIT native hash join probe received a perfect layout without perfect code");
		}
		EnsurePerfectHashJoinProbeCode(runtime, op.hash_join_probe);
		if (op.hash_join_probe.plan.residual_predicate) {
			throw InternalException("SLJIT native perfect hash join probe does not support residual predicates");
		}
		if (op.hash_join_probe.plan.keys.size() != 1 || probe.probe_key_input_indices.size() != 1) {
			throw InternalException("SLJIT native perfect hash join probe requires one key");
		}
		auto &perfect_layout = probe.perfect_layout;
		if (!perfect_layout.ready || perfect_layout.build_capacity == 0) {
			throw InternalException("SLJIT native perfect hash join probe received an incomplete layout");
		}
		auto &key = op.hash_join_probe.plan.keys[0];
		if (probe.probe_key_input_indices[0] != key.key_input_index) {
			throw InternalException("SLJIT native perfect hash join probe key binding mismatch");
		}
		if (input.data[key.key_input_index].GetType().InternalType() != perfect_layout.key_physical_type) {
			throw InternalException("SLJIT native perfect hash join probe key type mismatch");
		}

		UnifiedVectorFormat source_format;
		input.data[key.key_input_index].ToUnifiedFormat(source_format);
		auto source_data = NativeHashJoinKeySourceData(source_format, key.key_kind);
		auto source_sel = SljitNormalizedSourceSelectionData(source_format);
		auto source_validity = source_format.validity.CannotHaveNull() ? nullptr : source_format.validity.GetData();
		const_data_ptr_t source_data_array[] = {source_data};
		const sel_t *source_sel_array[] = {source_sel};
		const validity_t *source_validity_array[] = {source_validity};

		SljitNativeHashJoinProbeInput native_input;
		native_input.source_data = source_data_array;
		native_input.source_sel = source_sel ? source_sel_array : nullptr;
		native_input.source_validity = source_validity ? source_validity_array : nullptr;
		native_input.count = input.size();
		native_input.match_sel = match_selection.data();
		native_input.build_sel = build_selection.data();
		native_input.perfect_min = perfect_layout.build_min;
		native_input.perfect_max = perfect_layout.build_max;
		native_input.perfect_validity = perfect_layout.build_validity;
		native_input.selected_count = 0;
		native_input.input_offset = state.input_offset;
		native_input.finished = false;

		auto generated_stage_start = SljitRegionStageStart(runtime);
		op.hash_join_probe.perfect_function(&native_input);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, SLJIT_GENERATED_PERFECT_HASH_JOIN_PROBE_STAGE,
		                                  generated_stage_start);
		state.input_offset = native_input.input_offset;
		state.resume_row_pointer = nullptr;
		state.finished = native_input.finished;
		if (native_input.selected_count == 0) {
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		if (selection_only) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_selection_reference",
			                                         native_input.selected_count);
			output.SetChildCardinality(native_input.selected_count);
			return ExecutionOperatorBindResult::READY;
		}
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output", native_input.selected_count);
		ExecutionMaterializePerfectHashJoinProbe(probe, input, match_selection, build_selection,
		                                         native_input.selected_count, output, &recorder);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_output", materialize_stage_start);
		return ExecutionOperatorBindResult::READY;
	}

	ExecutionOperatorBindResult ExecuteRegularHashJoinProbe(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output,
	    SelectionVector &match_selection, Vector &row_pointers, vector<UnifiedVectorFormat> &source_formats,
	    vector<const_data_ptr_t> &source_data, vector<const sel_t *> &source_sel,
	    vector<const validity_t *> &source_validity, DataChunk *residual_chunk, SelectionVector *residual_selection,
	    SelectionVector *compact_match_selection, Vector *compact_row_pointers, SljitHashJoinProbeDrainState &state,
	    bool left_probe_output, bool source_key0_int64_to_int32_unchecked = false, bool selection_only = false) {
		auto &layout = probe.table_layout;
		if (!layout.ready || !layout.entries || layout.layout_offsets.empty()) {
			throw InternalException("SLJIT native hash join probe received an incomplete hash table layout");
		}
		if (layout.layout_offsets.size() < op.hash_join_probe.plan.keys.size()) {
			throw InternalException("SLJIT native hash join probe layout key count mismatch");
		}
		if (layout.pointer_offset != op.hash_join_probe.plan.pointer_offset) {
			throw InternalException("SLJIT native hash join probe pointer offset mismatch");
		}
		if (op.hash_join_probe.plan.mark_build_match) {
			if (!layout.found_match_column_present) {
				throw InternalException("SLJIT native hash join probe expected a build-side found-match column");
			}
			if (layout.tuple_size != op.hash_join_probe.plan.found_match_offset) {
				throw InternalException("SLJIT native hash join probe found-match offset mismatch");
			}
			if (op.hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY &&
			    layout.dictionary_emission && layout.chains_longer_than_one && !layout.aux_next_ptrs) {
				throw InternalException("SLJIT native hash join mark-only probe requires dictionary chain pointers");
			}
		}
		for (idx_t key_idx = 0; key_idx < op.hash_join_probe.plan.keys.size(); key_idx++) {
			auto &key = op.hash_join_probe.plan.keys[key_idx];
			if (probe.probe_key_input_indices[key_idx] != key.key_input_index) {
				throw InternalException("SLJIT native hash join probe key binding mismatch");
			}
			if (layout.layout_offsets[key_idx] != key.key_layout_offset) {
				throw InternalException("SLJIT native hash join probe key layout offset mismatch");
			}
		}

		if (source_formats.size() != op.hash_join_probe.plan.keys.size() ||
		    source_data.size() != op.hash_join_probe.plan.keys.size() ||
		    source_sel.size() != op.hash_join_probe.plan.keys.size() ||
		    source_validity.size() != op.hash_join_probe.plan.keys.size()) {
			throw InternalException("SLJIT native hash join probe key scratch width mismatch");
		}
		auto vector_setup_stage_start = SljitRegionStageStart(runtime);
		bool source_key0_int64_to_int32 = false;
		for (idx_t key_idx = 0; key_idx < op.hash_join_probe.plan.keys.size(); key_idx++) {
			auto &key = op.hash_join_probe.plan.keys[key_idx];
			auto &source_vector = input.data[key.key_input_index];
			source_vector.ToUnifiedFormat(source_formats[key_idx]);
			if (key_idx == 0 && key.key_kind == SljitNativeHashJoinKeyKind::INT32 &&
			    source_vector.GetType().InternalType() == PhysicalType::INT64) {
				source_data[key_idx] =
				    reinterpret_cast<const_data_ptr_t>(UnifiedVectorFormat::GetData<int64_t>(source_formats[key_idx]));
				source_key0_int64_to_int32 = true;
			} else {
				source_data[key_idx] = NativeHashJoinKeySourceData(source_formats[key_idx], key.key_kind);
			}
			source_sel[key_idx] = SljitNormalizedSourceSelectionData(source_formats[key_idx]);
			source_validity[key_idx] =
			    SljitNormalizedSourceValidityData(source_formats[key_idx], source_sel[key_idx], input.size());
		}
		row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
		auto row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(row_pointers);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "vector_setup", vector_setup_stage_start);
		auto source_sel_array = SljitPointerArrayOrNull(source_sel);
		auto source_validity_array = SljitPointerArrayOrNull(source_validity);
		const bool source_selection_present = source_sel_array != nullptr;
		const bool source_validity_present = source_validity_array != nullptr;
		const bool source_common_selection_present =
		    source_selection_present && SljitCommonSelectionOrNull(source_sel) != nullptr;
		const bool rhs_keys_all_valid =
		    !layout.can_have_null || (layout.null_keys_are_filtered && !layout.found_match_column_present);
		const auto probe_traits = BuildRegularHashJoinProbeRuntimeTraits(
		    source_selection_present, source_common_selection_present, source_validity_present, rhs_keys_all_valid);

		SljitNativeHashJoinProbeInput native_input;
		native_input.source_data = source_data.data();
		native_input.source_sel = source_sel_array;
		native_input.source_validity = source_validity_array;
		native_input.source_key0_int64_to_int32 = source_key0_int64_to_int32;
		native_input.source_key0_int64_to_int32_unchecked =
		    source_key0_int64_to_int32 && source_key0_int64_to_int32_unchecked;
		native_input.count = input.size();
		native_input.entries = reinterpret_cast<const_data_ptr_t>(layout.entries);
		native_input.bitmask = layout.bitmask;
		native_input.pointer_mask = layout.pointer_mask;
		native_input.use_salt = layout.use_salt;
		native_input.rhs_keys_have_validity = layout.can_have_null;
		native_input.chains_longer_than_one = layout.chains_longer_than_one;
		native_input.dictionary_emission = layout.dictionary_emission;
		native_input.key_offset = layout.layout_offsets[0];
		native_input.pointer_offset = layout.pointer_offset;
		native_input.aux_next_ptrs = layout.aux_next_ptrs;
		native_input.bloom_filter = layout.bloom_filter;
		native_input.bloom_filter_bits = layout.bloom_filter ? layout.bloom_filter->Data() : nullptr;
		native_input.bloom_filter_bitmask = layout.bloom_filter ? layout.bloom_filter->Bitmask() : 0;
		native_input.match_sel = match_selection.data();
		native_input.row_pointers = row_pointer_data;
		native_input.output_capacity = STANDARD_VECTOR_SIZE;
		native_input.selected_count = 0;
		native_input.input_offset = state.input_offset;
		native_input.resume_row_pointer = state.resume_row_pointer;
		native_input.finished = false;

		auto generated_stage_start = SljitRegionStageStart(runtime);
		const auto executed_probe_stage =
		    ExecuteRegularHashJoinProbeVariant(runtime, probe_traits, op.hash_join_probe, layout, native_input);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, executed_probe_stage, generated_stage_start);
		state.input_offset = native_input.input_offset;
		state.resume_row_pointer = native_input.resume_row_pointer;
		state.finished = native_input.finished;
		auto residual_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder residual_recorder(runtime, op_idx, op.kind, "residual_predicate");
		auto residual_scratch =
		    op.hash_join_probe.plan.residual_predicate ? &scratch.ExpressionAdapterScratch(op_idx, 0) : nullptr;
		auto selected_count = ApplyNativeHashJoinResidualPredicate(
		    runtime, op, probe, input, row_pointers, match_selection, native_input.selected_count, residual_chunk,
		    residual_selection, compact_match_selection, compact_row_pointers, residual_scratch, &residual_recorder);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "residual_predicate", residual_stage_start);
		if (left_probe_output && selected_count != 0) {
			auto mark_stage_start = SljitRegionStageStart(runtime);
			MarkLeftHashJoinProbeMatches(state, match_selection, selected_count);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "mark_left_matches", mark_stage_start);
		}
		auto mark_build_stage_start = SljitRegionStageStart(runtime);
		MarkHashJoinBuildMatchesAfterResidual(op.hash_join_probe.plan, row_pointers, selected_count);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "mark_build_matches", mark_build_stage_start);
		if (op.hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		if (selected_count == 0) {
			if (left_probe_output && state.finished) {
				auto materialize_stage_start = SljitRegionStageStart(runtime);
				SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
				MaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
				RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output_left_unmatched",
				                                         output.size());
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched",
				                              materialize_stage_start);
				return ExecutionOperatorBindResult::READY;
			}
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		const bool mark_probe = op.hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE;
		if (!mark_probe) {
			FlatVector::SetSize(row_pointers, count_t(selected_count));
		}
		if (selection_only) {
			RecordSljitRegionMaterializationBoundary(
			    runtime, op.kind, mark_probe ? "direct_mark_flags" : "direct_row_pointer_reference", selected_count);
			output.SetChildCardinality(selected_count);
			return ExecutionOperatorBindResult::READY;
		}
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
		if (!mark_probe) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_reference", selected_count);
		}
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output", selected_count);
		ExecutionMaterializeHashJoinProbe(probe, input, row_pointers, match_selection, selected_count, output,
		                                  &recorder);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_output", materialize_stage_start);
		return ExecutionOperatorBindResult::READY;
	}

	ExecutionOperatorBindResult ExecuteEmptyHashJoinProbe(ExecutionRegionRuntime &runtime, idx_t op_idx,
	                                                      SljitExecutableRegionOp &op,
	                                                      const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
	                                                      DataChunk &output, SelectionVector &match_selection,
	                                                      Vector &row_pointers, SljitHashJoinProbeDrainState &state) {
		state.finished = true;
		RecordSljitRegionRuntimePath(runtime, op.kind, "empty_build_side");
		switch (op.hash_join_probe.plan.output_mode) {
		case ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD: {
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
			MaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output_left_unmatched", output.size());
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched",
			                              materialize_stage_start);
		} break;
		case ExecutionHashJoinProbeOutputMode::MARK_PROBE: {
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output", input.size());
			ExecutionMaterializeHashJoinProbe(probe, input, row_pointers, match_selection, input.size(), output,
			                                  &recorder);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_output", materialize_stage_start);
		} break;
		case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD:
		case ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY:
		case ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY:
			output.Reset();
			break;
		default:
			throw InternalException("SLJIT native hash join probe cannot execute empty build side for output mode");
		}
		return ExecutionOperatorBindResult::READY;
	}

	ExecutionOperatorBindResult ExecuteNativeHashJoinProbe(
	    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
	    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input, DataChunk &output,
	    SelectionVector &match_selection, SelectionVector &build_selection, Vector &row_pointers,
	    vector<UnifiedVectorFormat> &source_formats, vector<const_data_ptr_t> &source_data,
	    vector<const sel_t *> &source_sel, vector<const validity_t *> &source_validity, DataChunk *residual_chunk,
	    SelectionVector *residual_selection, SelectionVector *compact_match_selection, Vector *compact_row_pointers,
	    SljitHashJoinProbeDrainState &state, string &deferred_reason, bool source_key0_int64_to_int32_unchecked = false,
	    bool selection_only = false) {
		ExecutionOperatorBinding *binding_ptr = nullptr;
		auto bind_stage_start = SljitRegionStageStart(runtime);
		auto bind_result = BindNativeOperator(native_runtime, scratch, op_idx, op, input,
		                                      op.hash_join_probe.plan.operator_info, "native-operator-runtime-deferred",
		                                      "SLJIT native hash join probe", binding_ptr, deferred_reason);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_operator_contract", bind_stage_start);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return bind_result;
		}
		auto &binding = *binding_ptr;
		if (!binding.ready || !binding.hash_join_probe.ready) {
			throw InternalException("SLJIT native hash join probe received an incomplete operator binding");
		}
		auto &probe = binding.hash_join_probe;
		if (op.hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::NONE ||
		    probe.output_mode != op.hash_join_probe.plan.output_mode) {
			throw InternalException("SLJIT native hash join probe output mode mismatch");
		}
		const bool left_probe_output = IsLeftHashJoinProbeOutputMode(op.hash_join_probe.plan.output_mode);
		if (left_probe_output) {
			InitializeLeftHashJoinProbeState(state, input.size());
			if (state.finished && !state.left_unmatched_emitted) {
				auto materialize_stage_start = SljitRegionStageStart(runtime);
				SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
				MaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
				RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output_left_unmatched",
				                                         output.size());
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched",
				                              materialize_stage_start);
				return ExecutionOperatorBindResult::READY;
			}
		}
		if (probe.probe_key_input_indices.size() != op.hash_join_probe.plan.keys.size()) {
			throw InternalException("SLJIT native hash join probe key binding count mismatch");
		}
		if (probe.empty_build_side) {
			return ExecuteEmptyHashJoinProbe(runtime, op_idx, op, probe, input, output, match_selection, row_pointers,
			                                 state);
		}
		runtime.RecordHashJoinProbeLayout(SljitHashJoinProbeLayoutName(probe.layout_kind));
		switch (probe.layout_kind) {
		case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
			return ExecutePerfectHashJoinProbe(runtime, op_idx, op, probe, input, output, match_selection,
			                                   build_selection, state, selection_only);
		case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
			return ExecuteRegularHashJoinProbe(
			    runtime, scratch, op_idx, op, probe, input, output, match_selection, row_pointers, source_formats,
			    source_data, source_sel, source_validity, residual_chunk, residual_selection, compact_match_selection,
			    compact_row_pointers, state, left_probe_output, source_key0_int64_to_int32_unchecked, selection_only);
		default:
			throw InternalException("SLJIT native hash join probe received an unknown layout kind");
		}
	}

	ExecutionOperatorBindResult
	ExecuteNativeNestedLoopJoinProbe(ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
	                                 idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
	                                 DataChunk &left_condition, DataChunk &output, SelectionVector &left_selection,
	                                 SelectionVector &right_selection, SljitNestedLoopJoinProbeDrainState &state,
	                                 string &deferred_reason) {
		if (!op.nested_loop_join_probe.function) {
			throw InternalException("SLJIT native nested loop join probe reached runtime without generated code");
		}
		if (op.nested_loop_join_probe.plan.conditions.size() != 1 ||
		    op.nested_loop_join_probe.lhs_conditions.size() != 1) {
			throw InternalException("SLJIT native nested loop join probe requires one executable condition");
		}

		ExecutionOperatorBinding *binding_ptr = nullptr;
		auto bind_result = BindNativeOperator(
		    native_runtime, scratch, op_idx, op, input, op.nested_loop_join_probe.plan.operator_info,
		    "native-operator-runtime-deferred", "SLJIT native nested loop join probe", binding_ptr, deferred_reason);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return bind_result;
		}
		auto &binding = *binding_ptr;
		if (!binding.ready || !binding.nested_loop_join_probe.ready) {
			throw InternalException("SLJIT native nested loop join probe received an incomplete operator binding");
		}
		auto &probe = binding.nested_loop_join_probe;
		if (probe.join_type != ExecutionRegionJoinType::INNER ||
		    op.nested_loop_join_probe.plan.join_type != ExecutionRegionJoinType::INNER) {
			throw InternalException("SLJIT native nested loop join probe currently requires INNER join");
		}
		if (probe.empty_build_side) {
			state.finished = true;
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		if (!state.lhs_materialized) {
			left_condition.Reset();
			for (idx_t condition_idx = 0; condition_idx < op.nested_loop_join_probe.lhs_conditions.size();
			     condition_idx++) {
				ExecuteProjectionExpression(op.nested_loop_join_probe.lhs_conditions[condition_idx], input,
				                            left_condition.data[condition_idx], nullptr, input.size(),
				                            scratch.ExpressionAdapterScratch(op_idx, condition_idx));
			}
			left_condition.SetChildCardinality(input.size());
			state.lhs_materialized = true;
		}

		if (!state.started) {
			state.started = true;
			state.left_offset = 0;
			state.right_offset = 0;
			state.right_chunk_finished = false;
			if (!ExecutionNestedLoopJoinProbeStartInput(probe)) {
				state.finished = true;
				output.Reset();
				return ExecutionOperatorBindResult::READY;
			}
		}
		while (!state.finished) {
			if (state.right_chunk_finished) {
				state.left_offset = 0;
				state.right_offset = 0;
				state.right_chunk_finished = false;
				if (!ExecutionNestedLoopJoinProbeAdvanceRight(probe)) {
					state.finished = true;
					output.Reset();
					return ExecutionOperatorBindResult::READY;
				}
			}
			if (!probe.right_condition || probe.right_condition->size() == 0) {
				state.right_chunk_finished = true;
				continue;
			}

			auto &condition_plan = op.nested_loop_join_probe.plan.conditions[0];
			if (left_condition.ColumnCount() != 1 || probe.right_condition->ColumnCount() != 1) {
				throw InternalException("SLJIT native nested loop join probe condition width mismatch");
			}
			UnifiedVectorFormat left_format;
			UnifiedVectorFormat right_format;
			left_condition.data[0].ToUnifiedFormat(left_format);
			probe.right_condition->data[0].ToUnifiedFormat(right_format);

			SljitNativeNestedLoopJoinProbeInput native_input;
			native_input.left_data = NativeNestedLoopJoinConditionSourceData(left_format, condition_plan.value_kind);
			native_input.right_data = NativeNestedLoopJoinConditionSourceData(right_format, condition_plan.value_kind);
			native_input.left_sel = SljitNormalizedSourceSelectionData(left_format);
			native_input.right_sel = SljitNormalizedSourceSelectionData(right_format);
			native_input.left_validity =
			    left_format.validity.CannotHaveNull() ? nullptr : left_format.validity.GetData();
			native_input.right_validity =
			    right_format.validity.CannotHaveNull() ? nullptr : right_format.validity.GetData();
			native_input.left_count = left_condition.size();
			native_input.right_count = probe.right_condition->size();
			native_input.left_offset = state.left_offset;
			native_input.right_offset = state.right_offset;
			native_input.output_capacity = STANDARD_VECTOR_SIZE;
			native_input.left_match_sel = left_selection.data();
			native_input.right_match_sel = right_selection.data();
			native_input.selected_count = 0;
			native_input.right_chunk_finished = false;

			op.nested_loop_join_probe.function(&native_input);
			state.left_offset = native_input.left_offset;
			state.right_offset = native_input.right_offset;
			state.right_chunk_finished = native_input.right_chunk_finished;
			if (probe.left_tuple) {
				*probe.left_tuple = state.left_offset;
			}
			if (probe.right_tuple) {
				*probe.right_tuple = state.right_offset;
			}
			if (native_input.selected_count == 0) {
				continue;
			}
			ExecutionMaterializeNestedLoopJoinProbe(probe, input, left_selection, right_selection,
			                                        native_input.selected_count, output);
			return ExecutionOperatorBindResult::READY;
		}
		output.Reset();
		return ExecutionOperatorBindResult::READY;
	}

	ExecutionSinkBinding &BindNativeSink(ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
	                                     idx_t op_idx, DataChunk &input, const ExecutionRegionSinkInfo &sink_info,
	                                     const char *blocker_prefix, const char *error_prefix,
	                                     optional_ptr<bool> bound = nullptr) {
		auto &binding = scratch.SinkBinding(op_idx);
		if (bound) {
			*bound = false;
		}
		if (scratch.HasSinkBinding(op_idx)) {
			return binding;
		}
		if (!native_runtime.BindSink(input, sink_info, binding)) {
			auto blocker = binding.blocker.empty() ? string(blocker_prefix) : binding.blocker;
			throw InternalException("%s binding failed: %s", error_prefix, blocker);
		}
		scratch.MarkSinkBindingReady(op_idx);
		if (bound) {
			*bound = true;
		}
		return binding;
	}

	SinkResultType ExecuteNativeHashJoinBuild(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
	                                          SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                          SljitExecutableRegionOp &op, DataChunk &input, DataChunk &source_chunk,
	                                          Vector &hash_values, SelectionVector &build_sel) {
		auto bind_stage_start = SljitRegionStageStart(runtime);
		bool bound = false;
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.hash_join_build.plan.sink_info,
		                               "hash-join-build-runtime-binding-failed", "SLJIT hash join build sink", bound);
		if (bound) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_sink_contract", bind_stage_start);
		}
		if (!binding.ready || !binding.hash_join_build.ready) {
			throw InternalException("SLJIT hash join build sink binding did not return a ready build state");
		}
		auto &build = binding.hash_join_build;

		auto stage_start = SljitRegionStageStart(runtime);
		ExecutionHashJoinBuildReferenceKeys(build, input);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "reference_keys", stage_start);

		stage_start = SljitRegionStageStart(runtime);
		ExecutionHashJoinBuildApplyFilterPushdown(build);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "filter_pushdown", stage_start);

		stage_start = SljitRegionStageStart(runtime);
		ExecutionHashJoinBuildReferencePayload(build, input);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "reference_payload", stage_start);

		optional_ptr<const SelectionVector> build_selection;
		stage_start = SljitRegionStageStart(runtime);
		auto build_count = ExecutionHashJoinBuildPrepare(build, source_chunk, hash_values, build_sel, build_selection);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "hash_table_prepare", stage_start);
		if (build_count == 0) {
			return SinkResultType::NEED_MORE_INPUT;
		}
		if (!build_selection) {
			throw InternalException("SLJIT hash join build prepare did not return a build selection");
		}

		stage_start = SljitRegionStageStart(runtime);
		ExecutionHashJoinBuildHash(build, source_chunk, hash_values, *build_selection, build_count);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "hash_table_hash", stage_start);

		stage_start = SljitRegionStageStart(runtime);
		ExecutionHashJoinBuildAppend(build, source_chunk, *build_selection, build_count);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "hash_table_append", stage_start);
		return SinkResultType::NEED_MORE_INPUT;
	}

	SinkResultType ExecuteNativeNestedLoopJoinBuild(ExecutionOperatorRuntime &native_runtime,
	                                                SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                                SljitExecutableRegionOp &op, DataChunk &input,
	                                                DataChunk &right_condition) {
		if (op.nested_loop_join_build.rhs_conditions.size() != right_condition.ColumnCount()) {
			throw InternalException("SLJIT nested loop join build condition expression count mismatch");
		}
		right_condition.Reset();
		for (idx_t condition_idx = 0; condition_idx < op.nested_loop_join_build.rhs_conditions.size();
		     condition_idx++) {
			ExecuteProjectionExpression(op.nested_loop_join_build.rhs_conditions[condition_idx], input,
			                            right_condition.data[condition_idx], nullptr, input.size(),
			                            scratch.ExpressionAdapterScratch(op_idx, condition_idx));
		}
		right_condition.SetChildCardinality(input.size());

		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.nested_loop_join_build.plan.sink_info,
		                               "nested-loop-join-build-native-runtime-binding-failed",
		                               "SLJIT native nested loop join build sink");
		if (!binding.ready || !binding.nested_loop_join_build.ready) {
			throw InternalException(
			    "SLJIT native nested loop join build sink binding did not return a ready build state");
		}
		return ExecutionSinkNestedLoopJoinBuild(binding.nested_loop_join_build, input, right_condition);
	}

	SinkResultType ExecuteNativeAppendSink(ExecutionOperatorRuntime &native_runtime,
	                                       SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                       SljitExecutableRegionOp &op, DataChunk &input) {
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.append_sink.plan.sink_info,
		                               "append-sink-runtime-binding-failed", "SLJIT append sink");
		if (!binding.ready || !binding.append_sink.ready) {
			throw InternalException("SLJIT append sink binding did not return a ready append state");
		}
		return ExecutionSinkAppend(binding.append_sink, input);
	}

	SinkResultType ExecuteNativeDelimJoinSink(ExecutionOperatorRuntime &native_runtime,
	                                          SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                          SljitExecutableRegionOp &op, DataChunk &input) {
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.delim_join_sink.plan.sink_info,
		                               "delim-join-sink-runtime-binding-failed", "SLJIT delimiter join sink");
		if (!binding.ready || !binding.delim_join_sink.ready) {
			throw InternalException("SLJIT delimiter join sink binding did not return a ready delimiter state");
		}
		return ExecutionSinkDelimJoin(binding.delim_join_sink, input);
	}

	void ExecuteFilteredPrimitiveAggregateUpdate(SljitExecutableFilteredAggregateUpdate &filtered_update,
	                                             const vector<ExecutionRegionAggregateInput> &aggregates,
	                                             const vector<const ExecutionPrimitiveAggregateUpdateLane *> &lanes,
	                                             DataChunk &input, idx_t count,
	                                             SljitAggregatePayloadAdapterScratch &adapter_scratch) {
		if (!filtered_update.function) {
			throw InternalException("SLJIT filtered aggregate primitive payload update is missing generated code");
		}
		if (aggregates.size() != filtered_update.payloads.size() || aggregates.size() != lanes.size()) {
			throw InternalException("SLJIT filtered aggregate primitive payload count mismatch");
		}

		adapter_scratch.PrepareFiltered(filtered_update.input_source_indices.size(), aggregates.size());
		auto &aggregate_int64_values = adapter_scratch.aggregate_int64_values;
		auto &aggregate_hugeint_values = adapter_scratch.aggregate_hugeint_values;
		auto &aggregate_state_is_sets = adapter_scratch.aggregate_state_is_sets;
		auto &aggregate_row_counts = adapter_scratch.aggregate_row_counts;
		for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			auto lane = lanes[payload_idx];
			if (!lane) {
				throw InternalException("SLJIT filtered aggregate primitive lane is missing");
			}
			auto &aggregate = aggregates[payload_idx];
			if (lane->kind != aggregate.primitive_update_kind) {
				throw InternalException("SLJIT filtered aggregate primitive lane kind mismatch");
			}
			if (lane->kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				if (!lane->ready || !lane->sum_int64_value || !lane->row_count) {
					auto blocker = lane->blocker.empty() ? "aggregate-count-star-lane-incomplete" : lane->blocker;
					throw InternalException("SLJIT filtered aggregate count-star lane is incomplete: %s",
					                        blocker.c_str());
				}
				aggregate_int64_values[payload_idx] = lane->sum_int64_value;
				aggregate_row_counts[payload_idx] = lane->row_count;
				continue;
			}
			if (lane->kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
			    lane->kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
				throw InternalException("SLJIT filtered aggregate primitive lane has unsupported state kind");
			}
			if (filtered_update.payloads[payload_idx].plan.return_type.InternalType() != lane->payload_type) {
				throw InternalException("SLJIT filtered aggregate primitive payload type mismatch");
			}
			auto has_sum_state = (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64 && lane->sum_int64_value) ||
			                     (lane->kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT && lane->sum_hugeint_value);
			if (!lane->ready || !has_sum_state || !lane->state_is_set || !lane->row_count) {
				auto blocker = lane->blocker.empty() ? "aggregate-primitive-lane-incomplete" : lane->blocker;
				throw InternalException("SLJIT filtered aggregate primitive lane is incomplete: %s", blocker.c_str());
			}
			if (lane->kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
				aggregate_int64_values[payload_idx] = lane->sum_int64_value;
			} else {
				aggregate_hugeint_values[payload_idx] = lane->sum_hugeint_value;
			}
			aggregate_state_is_sets[payload_idx] = lane->state_is_set;
			aggregate_row_counts[payload_idx] = lane->row_count;
		}

		auto &source_formats = adapter_scratch.source_formats;
		auto &source_data = adapter_scratch.source_data;
		auto &source_sel = adapter_scratch.source_sel;
		auto &source_validity = adapter_scratch.source_validity;
		bool flat_no_selection = true;
		bool flat_all_valid = true;
		bool all_valid = true;
		for (idx_t source_idx = 0; source_idx < filtered_update.input_source_indices.size(); source_idx++) {
			auto input_index = filtered_update.input_source_indices[source_idx];
			if (input_index >= input.ColumnCount()) {
				throw InternalException("SLJIT filtered aggregate expression-tree source is out of range");
			}
			input.data[input_index].ToUnifiedFormat(source_formats[source_idx]);
			source_data[source_idx] =
			    SljitTypedExpressionTreeSourceData(source_formats[source_idx], input.data[input_index].GetType());
			source_sel[source_idx] = SljitNormalizedSourceSelectionData(source_formats[source_idx]);
			source_validity[source_idx] =
			    SljitNormalizedSourceValidityData(source_formats[source_idx], source_sel[source_idx], count);
			flat_no_selection = flat_no_selection && source_sel[source_idx] == nullptr;
			flat_all_valid =
			    flat_all_valid && source_sel[source_idx] == nullptr && source_validity[source_idx] == nullptr;
			all_valid = all_valid && source_validity[source_idx] == nullptr;
		}

		SljitNativeVectorInput native_input;
		native_input.source_data_array = source_data.data();
		native_input.source_sel_array = source_sel.data();
		native_input.source_validity_array = source_validity.data();
		native_input.expression_tree_flat_no_selection = flat_no_selection;
		native_input.expression_tree_flat_all_valid = flat_all_valid;
		native_input.expression_tree_all_valid = all_valid;
		native_input.aggregate_int64_values = aggregate_int64_values.data();
		native_input.aggregate_hugeint_values = aggregate_hugeint_values.data();
		native_input.aggregate_state_is_sets = aggregate_state_is_sets.data();
		native_input.aggregate_row_counts = aggregate_row_counts.data();
		native_input.count = count;
		native_input.has_error = false;
		filtered_update.function(&native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
	}

	SinkResultType ExecuteNativeFilteredAggregateUpdate(ExecutionRegionRuntime &runtime,
	                                                    ExecutionOperatorRuntime &native_runtime,
	                                                    SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                                    SljitExecutableRegionOp &op, DataChunk &input) {
		auto bind_stage_start = SljitRegionStageStart(runtime);
		bool bound = false;
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.aggregate_update.plan.sink_info,
		                               "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink", bound);
		if (bound) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_sink_contract", bind_stage_start);
		}
		if (!binding.ready || !binding.aggregate_update.ready) {
			throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
		}
		auto &primitive = binding.aggregate_update.primitive;
		if (!primitive.ready) {
			auto blocker = primitive.blocker.empty() ? "aggregate-primitive-binding-missing" : primitive.blocker;
			throw InternalException("SLJIT aggregate primitive update binding failed: %s", blocker.c_str());
		}
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		if (aggregates.empty() || aggregates.size() != op.aggregate_update.payloads.size()) {
			throw InternalException("SLJIT filtered aggregate update requires matching primitive aggregate lanes");
		}
		auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, primitive);
		if (payload_lanes.size() != aggregates.size()) {
			throw InternalException("SLJIT filtered aggregate primitive lane count mismatch");
		}

		auto aggregate_stage_start = SljitRegionStageStart(runtime);
		auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
		if (op.aggregate_update.filtered_update.owns_perfect_hash_group_lookup) {
			auto &grouped_state = binding.aggregate_update.grouped_state;
			ExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
			    op.aggregate_update.filtered_update.payloads, op.aggregate_update.filtered_update.function, aggregates,
			    op.aggregate_update.plan.sink_info.groups, op.aggregate_update.plan.group_expressions,
			    op.aggregate_update.plan.sink_info.aggregate_contract, payload_lanes, grouped_state.perfect_hash_layout,
			    input, nullptr, input.size(), payload_scratch);
			RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "filtered_perfect_hash_update",
			                                  aggregate_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_perfect_hash_state_update",
			                                         input.size());
		} else {
			ExecuteFilteredPrimitiveAggregateUpdate(op.aggregate_update.filtered_update, aggregates, payload_lanes,
			                                        input, input.size(), payload_scratch);
			RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "filtered_primitive_update",
			                                  aggregate_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", input.size());
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	bool CanExecuteGroupedPrimitiveAggregateUpdateShape(
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
	    idx_t count) const {
		auto &aggregate_update = op.aggregate_update;
		auto &plan = aggregate_update.plan;
		auto &sink_info = plan.sink_info;
		if (execute_sel != nullptr || count != input.size() || !plan.use_primitive_payloads ||
		    !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
		    aggregate_update.fused_payload_update_owns_group_lookup ||
		    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
		    sink_info.aggregates.size() != payload_lanes.size()) {
			return false;
		}
		for (idx_t aggregate_idx = 0; aggregate_idx < sink_info.aggregates.size(); aggregate_idx++) {
			auto &aggregate = sink_info.aggregates[aggregate_idx];
			auto lane = payload_lanes[aggregate_idx];
			if (!lane || !lane->ready || lane->aggregate_index != aggregate.aggregate_index) {
				return false;
			}
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				if (aggregate.child_count != 0) {
					return false;
				}
				continue;
			}
			if (aggregate.child_count != 1 || aggregate.child_indices.size() != 1) {
				return false;
			}
			auto &payload = aggregate_update.payloads[aggregate_idx].plan;
			if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
			    payload.source_index != aggregate.child_indices[0] || payload.source_index >= input.ColumnCount() ||
			    input.data[payload.source_index].GetType().InternalType() != lane->payload_type) {
				return false;
			}
		}
		return true;
	}

	bool CanExecuteDirectNewGroupedPrimitiveAggregateUpdate(
	    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
	    idx_t count) const {
		return !scratch.DirectNewAggregateUpdateDisabled(op_idx) &&
		       CanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, payload_lanes, execute_sel, count);
	}

	bool CanExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(
	    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
	    idx_t count) const {
		return !scratch.DirectAppendNewAggregateUpdateDisabled(op_idx) &&
		       CanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, payload_lanes, execute_sel, count);
	}

	bool CanExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
	    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
	    idx_t count) const {
		if (scratch.DirectNewAggregateUpdateDisabled(op_idx) ||
		    !CanExecuteGroupedPrimitiveAggregateUpdateShape(op, input, payload_lanes, execute_sel, count)) {
			return false;
		}
		auto &sink_info = op.aggregate_update.plan.sink_info;
		if (sink_info.groups.size() != 1 || sink_info.groups[0].input_index >= input.ColumnCount()) {
			return false;
		}
		for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
			auto lane = payload_lanes[payload_idx];
			if (!lane || lane->kind != sink_info.aggregates[payload_idx].primitive_update_kind ||
			    !SljitPreaggregatedPrimitivePayloadSupported(lane->kind, lane->payload_type)) {
				return false;
			}
		}
		return true;
	}

	bool CanExecuteDirectGroupedFusedPayloadUpdate(
	    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
	    idx_t count, bool existing_groups) const {
		auto &aggregate_update = op.aggregate_update;
		auto &plan = aggregate_update.plan;
		auto &sink_info = plan.sink_info;
		if ((existing_groups ? scratch.DirectExistingAggregateUpdateDisabled(op_idx)
		                     : scratch.DirectNewAggregateUpdateDisabled(op_idx)) ||
		    execute_sel != nullptr || count != input.size() || !plan.use_primitive_payloads ||
		    !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
		    aggregate_update.fused_payload_update_owns_group_lookup ||
		    !aggregate_update.fused_payload_update_function ||
		    sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    sink_info.aggregates.size() != aggregate_update.payloads.size() ||
		    sink_info.aggregates.size() != payload_lanes.size()) {
			return false;
		}
		return FusedAggregatePayloadsUseTypedExpressionTrees(aggregate_update.payloads, sink_info.aggregates);
	}

	bool CanExecuteDirectAppendNewGroupedFusedPayloadUpdate(
	    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
	    idx_t count) const {
		return !scratch.DirectAppendNewAggregateUpdateDisabled(op_idx) &&
		       CanExecuteDirectGroupedFusedPayloadUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel, count,
		                                                 false);
	}

	bool CanResolveDirectNewGroupedStateAddresses(SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                              SljitExecutableRegionOp &op, DataChunk &input,
	                                              const SelectionVector *execute_sel, idx_t count) const {
		auto &aggregate_update = op.aggregate_update;
		auto &plan = aggregate_update.plan;
		auto &sink_info = plan.sink_info;
		return !scratch.DirectNewAggregateUpdateDisabled(op_idx) && execute_sel == nullptr && count == input.size() &&
		       plan.use_primitive_payloads && plan.use_grouped_state_addresses && !plan.use_perfect_hash_group_lookup &&
		       !aggregate_update.fused_payload_update_owns_group_lookup &&
		       sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE && !sink_info.groups.empty();
	}

	bool TryResolveDirectNewGroupedStateAddresses(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                              idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
	                                              ExecutionGroupedAggregateStateAddressBinding &grouped_state,
	                                              Vector &addresses, bool finish = true) {
		auto stage_start = SljitRegionStageStart(runtime);
		bool resolved = false;
		if (runtime.TraceRuntime()) {
			auto stage_name = SljitRegionStageName(op_idx, op.kind, "direct_new_grouped_state_addresses");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			resolved = grouped_state.state->TryResolveNewGroups(input, op.aggregate_update.plan.sink_info, addresses,
			                                                    &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			resolved = grouped_state.state->TryResolveNewGroups(input, op.aggregate_update.plan.sink_info, addresses,
			                                                    nullptr, finish);
		}
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, resolved);
		RecordSljitRegionStageRuntimePath(
		    runtime, op_idx, op.kind,
		    resolved ? "direct_new_grouped_state_addresses" : "direct_new_grouped_state_addresses_miss", stage_start);
		if (resolved) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "address_vector_direct_new", input.size());
		}
		return resolved;
	}

	bool TryExecuteDirectGroupedFusedPayloadUpdate(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state,
	    SljitAggregatePayloadAdapterScratch &payload_scratch, bool existing_groups, bool finish = true) {
		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		SljitGroupedStateAddressUpdateState update_state;
		update_state.kernel = this;
		update_state.payloads = &op.aggregate_update.payloads;
		update_state.function = op.aggregate_update.fused_payload_update_function;
		update_state.aggregates = &aggregates;
		update_state.contract = &op.aggregate_update.plan.sink_info.aggregate_contract;
		update_state.lanes = &payload_lanes;
		update_state.input = &input;
		update_state.adapter_scratch = &payload_scratch;
		const char *stage_name = existing_groups ? "direct_existing_grouped_fused_payload_update"
		                                         : "direct_new_grouped_fused_payload_update";
		const char *miss_stage_name = existing_groups ? "direct_existing_grouped_fused_payload_update_miss"
		                                              : "direct_new_grouped_fused_payload_update_miss";
		const char *boundary_name =
		    existing_groups ? "state_address_selection_existing_update" : "state_address_selection_new_update";
		if (runtime.TraceRuntime()) {
			auto trace_stage_name = SljitRegionStageName(op_idx, op.kind, stage_name);
			SljitRegionStageRecorder recorder(runtime, trace_stage_name);
			if (existing_groups) {
				updated = grouped_state.state->TryUpdateExistingGroupsWithSelectedStateAddresses(
				    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedSelectedStateAddressUpdate,
				    &update_state, &recorder, finish);
			} else {
				updated = grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
				    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedSelectedStateAddressUpdate,
				    &update_state, &recorder, finish);
			}
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(trace_stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			if (existing_groups) {
				updated = grouped_state.state->TryUpdateExistingGroupsWithSelectedStateAddresses(
				    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedSelectedStateAddressUpdate,
				    &update_state, nullptr, finish);
			} else {
				updated = grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
				    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedSelectedStateAddressUpdate,
				    &update_state, nullptr, finish);
			}
		}
		if (existing_groups) {
			scratch.RecordDirectExistingAggregateUpdateResult(op_idx, updated);
		} else {
			scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		}
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, updated ? stage_name : miss_stage_name,
		                                  stage_start);
		if (updated) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, boundary_name, input.size());
		}
		return updated;
	}

	bool TryExecuteDirectRowPointerGroupedFusedPayloadUpdate(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &payload_input, Vector &row_pointers, idx_t count,
	    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state,
	    SljitAggregatePayloadAdapterScratch &payload_scratch, bool finish = true) {
		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		SljitGroupedStateAddressUpdateState update_state;
		update_state.kernel = this;
		update_state.payloads = &op.aggregate_update.payloads;
		update_state.function = op.aggregate_update.fused_payload_update_function;
		update_state.aggregates = &aggregates;
		update_state.contract = &op.aggregate_update.plan.sink_info.aggregate_contract;
		update_state.lanes = &payload_lanes;
		update_state.input = &payload_input;
		update_state.adapter_scratch = &payload_scratch;
		update_state.input_source_indices_override = &payload_source_indices;
		const char *stage_name = "direct_row_pointer_grouped_lookup_update";
		const char *miss_stage_name = "direct_row_pointer_grouped_lookup_update_miss";
		if (runtime.TraceRuntime()) {
			auto trace_stage_name = SljitRegionStageName(op_idx, op.kind, stage_name);
			SljitRegionStageRecorder recorder(runtime, trace_stage_name);
			updated = grouped_state.state->TryUpdateNewGroupsWithRowPointerKeys(
			    payload_input, row_pointers, count, group_sources, op.aggregate_update.plan.sink_info,
			    ExecuteSljitGroupedSelectedStateAddressUpdate, &update_state, &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(trace_stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryUpdateNewGroupsWithRowPointerKeys(
			    payload_input, row_pointers, count, group_sources, op.aggregate_update.plan.sink_info,
			    ExecuteSljitGroupedSelectedStateAddressUpdate, &update_state, nullptr, finish);
		}
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, updated ? stage_name : miss_stage_name,
		                                  stage_start);
		if (updated) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_grouped_lookup_update", count);
		}
		return updated;
	}

	bool TryExecuteDirectProjectedGroupedFusedPayloadUpdate(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &groups, DataChunk &payload_input,
	    const vector<idx_t> &payload_source_indices,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state,
	    SljitAggregatePayloadAdapterScratch &payload_scratch, bool finish = true,
	    optional_ptr<Vector> precomputed_hashes = nullptr) {
		if (groups.size() != payload_input.size()) {
			return false;
		}
		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		SljitGroupedStateAddressUpdateState update_state;
		update_state.kernel = this;
		update_state.payloads = &op.aggregate_update.payloads;
		update_state.function = op.aggregate_update.fused_payload_update_function;
		update_state.aggregates = &aggregates;
		update_state.contract = &op.aggregate_update.plan.sink_info.aggregate_contract;
		update_state.lanes = &payload_lanes;
		update_state.input = &payload_input;
		update_state.adapter_scratch = &payload_scratch;
		update_state.input_source_indices_override = &payload_source_indices;
		const char *stage_name = "direct_projected_group_payload_update";
		const char *miss_stage_name = "direct_projected_group_payload_update_miss";
		if (runtime.TraceRuntime()) {
			auto trace_stage_name = SljitRegionStageName(op_idx, op.kind, stage_name);
			SljitRegionStageRecorder recorder(runtime, trace_stage_name);
			updated = grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
			    groups, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedSelectedStateAddressUpdate,
			    &update_state, &recorder, finish, precomputed_hashes);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(trace_stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
			    groups, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedSelectedStateAddressUpdate,
			    &update_state, nullptr, finish, precomputed_hashes);
		}
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		if (updated) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, stage_name, stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "projected_group_payload_update",
			                                         payload_input.size());
		} else {
			RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, miss_stage_name, stage_start);
		}
		return updated;
	}

	bool TryExecuteDirectAppendNewGroupedFusedPayloadUpdate(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state,
	    SljitAggregatePayloadAdapterScratch &payload_scratch, bool finish = true) {
		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		SljitGroupedStateAddressUpdateState update_state;
		update_state.kernel = this;
		update_state.payloads = &op.aggregate_update.payloads;
		update_state.function = op.aggregate_update.fused_payload_update_function;
		update_state.aggregates = &aggregates;
		update_state.contract = &op.aggregate_update.plan.sink_info.aggregate_contract;
		update_state.lanes = &payload_lanes;
		update_state.input = &input;
		update_state.adapter_scratch = &payload_scratch;
		const char *stage_name = "direct_append_new_grouped_fused_payload_update";
		const char *miss_stage_name = "direct_append_new_grouped_fused_payload_update_miss";
		if (runtime.TraceRuntime()) {
			auto trace_stage_name = SljitRegionStageName(op_idx, op.kind, stage_name);
			SljitRegionStageRecorder recorder(runtime, trace_stage_name);
			updated = grouped_state.state->TryAppendNewGroupsWithStateAddresses(
			    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedStateAddressUpdate, &update_state,
			    &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(trace_stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryAppendNewGroupsWithStateAddresses(
			    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedStateAddressUpdate, &update_state,
			    nullptr, finish);
		}
		scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, updated ? stage_name : miss_stage_name,
		                                  stage_start);
		if (updated) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "state_address_selection_append_new_update",
			                                         input.size());
		}
		return updated;
	}

	bool TryExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		if (runtime.TraceRuntime()) {
			auto stage_name = SljitRegionStageName(op_idx, op.kind, "direct_append_new_grouped_primitive_update");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			updated = grouped_state.state->TryAppendNewGroups(input, op.aggregate_update.plan.sink_info, payload_lanes,
			                                                  &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryAppendNewGroups(input, op.aggregate_update.plan.sink_info, payload_lanes,
			                                                  nullptr, finish);
		}
		scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  updated ? "direct_append_new_grouped_primitive_update"
		                                          : "direct_append_new_grouped_primitive_update_miss",
		                                  stage_start);
		if (updated) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", input.size());
		}
		return updated;
	}

	bool TryExecuteDirectNewGroupedPrimitiveAggregateUpdate(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		if (runtime.TraceRuntime()) {
			auto stage_name = SljitRegionStageName(op_idx, op.kind, "direct_new_grouped_primitive_update");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			updated = grouped_state.state->TryUpdateNewGroups(input, op.aggregate_update.plan.sink_info, payload_lanes,
			                                                  &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryUpdateNewGroups(input, op.aggregate_update.plan.sink_info, payload_lanes,
			                                                  nullptr, finish);
		}
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntimePath(
		    runtime, op_idx, op.kind,
		    updated ? "direct_new_grouped_primitive_update" : "direct_new_grouped_primitive_update_miss", stage_start);
		if (updated) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", input.size());
		}
		return updated;
	}

	bool TryExecutePreaggregatedCountStarGroupedAggregateUpdate(
	    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
	    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &compact_groups, const vector<int64_t> &count_deltas,
	    idx_t preaggregated_row_count, bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
		if (compact_groups.size() == 0 || count_deltas.size() < compact_groups.size() ||
		    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    !op.aggregate_update.plan.use_primitive_payloads || !op.aggregate_update.plan.use_grouped_state_addresses) {
			return false;
		}
		auto &sink_info = op.aggregate_update.plan.sink_info;
		if (sink_info.groups.size() != compact_groups.ColumnCount() || sink_info.aggregates.size() != 1 ||
		    op.aggregate_update.payloads.size() != 1) {
			return false;
		}
		auto &aggregate = sink_info.aggregates[0];
		if (aggregate.child_count != 0 || aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::COUNT_STAR) {
			return false;
		}

		auto bind_stage_start = SljitRegionStageStart(runtime);
		bool bound = false;
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, compact_groups, sink_info,
		                               "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink", bound);
		if (bound) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_sink_contract", bind_stage_start);
		}
		if (!binding.ready || !binding.aggregate_update.ready || !binding.aggregate_update.primitive.ready ||
		    !binding.aggregate_update.grouped_state.ready || !binding.aggregate_update.grouped_state.state) {
			return false;
		}
		auto &grouped_state = binding.aggregate_update.grouped_state;
		auto &payload_lanes =
		    scratch.AggregatePayloadLanes(op_idx, sink_info.aggregates, binding.aggregate_update.primitive);
		if (payload_lanes.size() != 1 || !payload_lanes[0]) {
			return false;
		}
		auto lane = payload_lanes[0];
		if (!lane->ready || lane->kind != AggregatePrimitiveUpdateKind::COUNT_STAR ||
		    lane->aggregate_index != aggregate.aggregate_index ||
		    aggregate.aggregate_index >= sink_info.aggregate_contract.grouped_state_offsets.size() ||
		    lane->state_offset != sink_info.aggregate_contract.grouped_state_offsets[aggregate.aggregate_index] ||
		    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
		    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset) {
			return false;
		}

		SljitPreaggregatedCountStarUpdateState update_state;
		update_state.lane = lane;
		update_state.counts = count_deltas.data();
		const bool finish = !defer_grouped_finish;
		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		if (runtime.TraceRuntime()) {
			auto stage_name = SljitRegionStageName(op_idx, op.kind, "direct_preaggregated_count_star_update");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			updated = grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
			    compact_groups, sink_info, ExecuteSljitPreaggregatedCountStarUpdate, &update_state, &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
			    compact_groups, sink_info, ExecuteSljitPreaggregatedCountStarUpdate, &update_state, nullptr, finish);
		}
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  updated ? "direct_preaggregated_count_star_update"
		                                          : "direct_preaggregated_count_star_update_miss",
		                                  stage_start);
		if (!updated) {
			return false;
		}
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_count_star_groups",
		                                         compact_groups.size());
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}

	bool TryExecuteSplitPreaggregatedGroupedPrimitiveAggregateUpdate(
	    ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t preaggregated_row_count,
	    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
		static constexpr idx_t MAX_EXISTING_PREFIX_GROUPS = 8;
		static constexpr idx_t MAX_SPLIT_PREAGGREGATED_GROUPS = 64;
		if (compact_groups.size() <= 1 || compact_groups.size() > MAX_SPLIT_PREAGGREGATED_GROUPS ||
		    !grouped_state.ready || !grouped_state.state) {
			return false;
		}
		auto split_stage_start = SljitRegionStageStart(runtime);
		auto &sink_info = op.aggregate_update.plan.sink_info;
		const auto max_prefix_count = MinValue<idx_t>(MAX_EXISTING_PREFIX_GROUPS, compact_groups.size());
		const auto group_types = compact_groups.GetTypes();

		DataChunk prefix_groups;
		DataChunk suffix_groups;
		prefix_groups.Initialize(runtime.GetAllocator(), group_types);
		suffix_groups.Initialize(runtime.GetAllocator(), group_types);
		SljitPreaggregatedPrimitiveAggregateScratch prefix_scratch;
		SljitPreaggregatedPrimitiveAggregateScratch suffix_scratch;

		for (idx_t prefix_count = 1; prefix_count <= max_prefix_count; prefix_count++) {
			prefix_groups.Reset();
			prefix_groups.Slice(compact_groups, 0, prefix_count);
			if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, 0, prefix_count,
			                                        prefix_scratch)) {
				return false;
			}

			const auto suffix_count = compact_groups.size() - prefix_count;
			if (suffix_count > 0) {
				suffix_groups.Reset();
				suffix_groups.Slice(compact_groups, prefix_count, compact_groups.size());
				if (!SlicePreaggregatedPrimitiveScratch(preaggregate_scratch, payload_lanes, prefix_count, suffix_count,
				                                        suffix_scratch)) {
					return false;
				}
				SljitPreaggregatedPrimitiveUpdateState suffix_update_state;
				suffix_update_state.lanes = &payload_lanes;
				suffix_update_state.payloads = &suffix_scratch.payloads;

				auto suffix_stage_start = SljitRegionStageStart(runtime);
				bool suffix_appended = false;
				if (runtime.TraceRuntime()) {
					auto stage_name = SljitRegionStageName(op_idx, op.kind, "split_preaggregated_append_new_suffix");
					SljitRegionStageRecorder recorder(runtime, stage_name);
					suffix_appended = grouped_state.state->TryAppendNewGroupsWithStateAddresses(
					    suffix_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &suffix_update_state,
					    &recorder, false);
					auto runtime_us = SljitRegionElapsedMicros(suffix_stage_start);
					auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
					if (unattributed_runtime_us > 0) {
						runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
					}
				} else {
					suffix_appended = grouped_state.state->TryAppendNewGroupsWithStateAddresses(
					    suffix_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &suffix_update_state,
					    nullptr, false);
				}
				RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
				                                  suffix_appended ? "split_preaggregated_append_new_suffix"
				                                                  : "split_preaggregated_append_new_suffix_miss",
				                                  suffix_stage_start);
				if (!suffix_appended) {
					continue;
				}
			}

			SljitPreaggregatedPrimitiveUpdateState prefix_update_state;
			prefix_update_state.lanes = &payload_lanes;
			prefix_update_state.payloads = &prefix_scratch.payloads;
			const bool finish = !defer_grouped_finish;
			auto prefix_stage_start = SljitRegionStageStart(runtime);
			bool prefix_updated = false;
			if (runtime.TraceRuntime()) {
				auto stage_name = SljitRegionStageName(op_idx, op.kind, "split_preaggregated_update_existing_prefix");
				SljitRegionStageRecorder recorder(runtime, stage_name);
				prefix_updated = grouped_state.state->TryUpdateExistingGroupsWithStateAddresses(
				    prefix_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &prefix_update_state,
				    &recorder, finish);
				auto runtime_us = SljitRegionElapsedMicros(prefix_stage_start);
				auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
				if (unattributed_runtime_us > 0) {
					runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
				}
			} else {
				prefix_updated = grouped_state.state->TryUpdateExistingGroupsWithStateAddresses(
				    prefix_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &prefix_update_state,
				    nullptr, finish);
			}
			RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
			                                  prefix_updated ? "split_preaggregated_update_existing_prefix"
			                                                 : "split_preaggregated_update_existing_prefix_miss",
			                                  prefix_stage_start);
			if (!prefix_updated) {
				throw InternalException("SLJIT split preaggregated grouped primitive prefix update failed after "
				                        "successful suffix append");
			}

			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "split_preaggregated_grouped_primitive_update",
			                              split_stage_start);
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups",
			                                         compact_groups.size());
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		return false;
	}

	bool TryExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &compact_groups,
	    SljitPreaggregatedPrimitiveAggregateScratch &preaggregate_scratch,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state, idx_t preaggregated_row_count,
	    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
		if (compact_groups.size() == 0 ||
		    op.aggregate_update.plan.sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE ||
		    !op.aggregate_update.plan.use_primitive_payloads || !op.aggregate_update.plan.use_grouped_state_addresses ||
		    !grouped_state.ready || !grouped_state.state) {
			return false;
		}
		auto &sink_info = op.aggregate_update.plan.sink_info;
		if (sink_info.groups.size() != compact_groups.ColumnCount() ||
		    sink_info.aggregates.size() != op.aggregate_update.payloads.size() ||
		    sink_info.aggregates.size() != payload_lanes.size() ||
		    sink_info.aggregates.size() != preaggregate_scratch.payloads.size()) {
			return false;
		}
		for (idx_t payload_idx = 0; payload_idx < sink_info.aggregates.size(); payload_idx++) {
			auto &aggregate = sink_info.aggregates[payload_idx];
			auto lane = payload_lanes[payload_idx];
			if (!lane || !lane->ready || lane->aggregate_index != aggregate.aggregate_index ||
			    aggregate.aggregate_index >= sink_info.aggregate_contract.grouped_state_offsets.size() ||
			    lane->state_offset != sink_info.aggregate_contract.grouped_state_offsets[aggregate.aggregate_index] ||
			    lane->state_value_offset != aggregate.primitive_update_state_value_offset ||
			    lane->state_is_set_offset != aggregate.primitive_update_state_is_set_offset ||
			    lane->kind != aggregate.primitive_update_kind ||
			    lane->kind != preaggregate_scratch.payloads[payload_idx].kind) {
				return false;
			}
		}

		SljitPreaggregatedPrimitiveUpdateState update_state;
		update_state.lanes = &payload_lanes;
		update_state.payloads = &preaggregate_scratch.payloads;
		const bool finish = !defer_grouped_finish;
		if (!scratch.DirectAppendNewAggregateUpdateDisabled(op_idx)) {
			auto append_stage_start = SljitRegionStageStart(runtime);
			bool appended = false;
			if (runtime.TraceRuntime()) {
				auto stage_name =
				    SljitRegionStageName(op_idx, op.kind, "direct_append_preaggregated_grouped_primitive_update");
				SljitRegionStageRecorder recorder(runtime, stage_name);
				appended = grouped_state.state->TryAppendNewGroupsWithStateAddresses(
				    compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &update_state,
				    &recorder, finish);
				auto runtime_us = SljitRegionElapsedMicros(append_stage_start);
				auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
				if (unattributed_runtime_us > 0) {
					runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
				}
			} else {
				appended = grouped_state.state->TryAppendNewGroupsWithStateAddresses(
				    compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveAddressUpdate, &update_state, nullptr,
				    finish);
			}
			if (appended) {
				scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, true);
				RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
				                                  "direct_append_preaggregated_grouped_primitive_update",
				                                  append_stage_start);
				RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups",
				                                         compact_groups.size());
				RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update",
				                                         preaggregated_row_count);
				MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
				return true;
			}
			if (TryExecuteSplitPreaggregatedGroupedPrimitiveAggregateUpdate(
			        runtime, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes, grouped_state,
			        preaggregated_row_count, defer_grouped_finish, deferred_grouped_finish)) {
				scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, true);
				RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
				                                  "direct_split_preaggregated_grouped_primitive_update",
				                                  append_stage_start);
				return true;
			}
			scratch.RecordDirectAppendNewAggregateUpdateResult(op_idx, false);
			RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
			                                  "direct_append_preaggregated_grouped_primitive_update_miss",
			                                  append_stage_start);
		}

		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		if (runtime.TraceRuntime()) {
			auto stage_name = SljitRegionStageName(op_idx, op.kind, "direct_preaggregated_grouped_primitive_update");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			updated = grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
			    compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveUpdate, &update_state, &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryUpdateNewGroupsWithSelectedStateAddresses(
			    compact_groups, sink_info, ExecuteSljitPreaggregatedPrimitiveUpdate, &update_state, nullptr, finish);
		}
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  updated ? "direct_preaggregated_grouped_primitive_update"
		                                          : "direct_preaggregated_grouped_primitive_update_miss",
		                                  stage_start);
		if (updated) {
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups",
			                                         compact_groups.size());
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}

		auto &addresses = scratch.AggregateStateAddresses(op_idx);
		auto fallback_stage_start = SljitRegionStageStart(runtime);
		if (runtime.TraceRuntime()) {
			auto stage_name = SljitRegionStageName(op_idx, op.kind, "preaggregated_grouped_primitive_address_update");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			grouped_state.state->ResolveStateAddresses(compact_groups, addresses, &recorder);
			auto runtime_us = SljitRegionElapsedMicros(fallback_stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			grouped_state.state->ResolveStateAddresses(compact_groups, addresses);
		}
		addresses.Flatten();
		ExecuteSljitPreaggregatedPrimitiveAddressUpdate(FlatVector::GetData<uintptr_t>(addresses), nullptr,
		                                                compact_groups.size(), &update_state);
		if (finish) {
			auto finish_stage_start = SljitRegionStageStart(runtime);
			grouped_state.state->FinishStateUpdates();
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "finish_grouped_state_updates", finish_stage_start);
		}
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, true);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, "preaggregated_grouped_primitive_address_update",
		                                  fallback_stage_start);
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_primitive_groups",
		                                         compact_groups.size());
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "preaggregated_address_vector_resolve",
		                                         compact_groups.size());
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", preaggregated_row_count);
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}

	static SljitGroupedAggregateRuntimeTraits
	BuildGroupedAggregateRuntimeTraits(const SljitExecutableAggregateUpdate &aggregate_update) {
		SljitGroupedAggregateRuntimeTraits traits;
		traits.use_grouped_state_addresses = aggregate_update.plan.use_grouped_state_addresses;
		traits.fused_payload_update_owns_group_lookup = aggregate_update.fused_payload_update_owns_group_lookup;
		return traits;
	}

	static void MarkDeferredGroupedFinish(bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
		if (defer_grouped_finish && deferred_grouped_finish) {
			*deferred_grouped_finish = true;
		}
	}

	bool TryExecuteDirectGroupedAggregateUpdateRoute(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
	    idx_t count, ExecutionGroupedAggregateStateAddressBinding &grouped_state,
	    SljitAggregatePayloadAdapterScratch &payload_scratch, bool defer_grouped_finish,
	    optional_ptr<bool> deferred_grouped_finish) {
		const bool finish = !defer_grouped_finish;
		if (CanExecutePreaggregatedGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes,
		                                                           execute_sel, count)) {
			auto &compact_groups = scratch.AggregatePreaggregatedGroups(op_idx);
			auto &preaggregate_scratch = scratch.AggregatePreaggregateScratch(op_idx);
			auto preaggregate_stage_start = SljitRegionStageStart(runtime);
			if (TryPreaggregateConsecutivePrimitiveGroups(op, input, payload_lanes, compact_groups,
			                                              preaggregate_scratch)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "local_preaggregate_primitive_groups",
				                              preaggregate_stage_start);
				if (TryExecutePreaggregatedGroupedPrimitiveAggregateUpdate(
				        runtime, scratch, op_idx, op, compact_groups, preaggregate_scratch, payload_lanes,
				        grouped_state, input.size(), defer_grouped_finish, deferred_grouped_finish)) {
					return true;
				}
			}
		}
		if (CanExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes,
		                                                             execute_sel, count) &&
		    TryExecuteDirectAppendNewGroupedPrimitiveAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
		                                                             grouped_state, finish)) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		if (CanExecuteDirectNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel,
		                                                       count) &&
		    TryExecuteDirectNewGroupedPrimitiveAggregateUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
		                                                       grouped_state, finish)) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		if (CanExecuteDirectGroupedFusedPayloadUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel, count,
		                                              true) &&
		    TryExecuteDirectGroupedFusedPayloadUpdate(runtime, scratch, op_idx, op, input, payload_lanes, grouped_state,
		                                              payload_scratch, true, finish)) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		if (CanExecuteDirectAppendNewGroupedFusedPayloadUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel,
		                                                       count) &&
		    TryExecuteDirectAppendNewGroupedFusedPayloadUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
		                                                       grouped_state, payload_scratch, finish)) {
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}
		return false;
	}

	bool TryExecuteNativeRowPointerGroupedAggregateUpdate(
	    ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime, SljitRegionExecutionScratch &scratch,
	    idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &payload_input, Vector &row_pointers,
	    const vector<ExecutionRowPointerGroupKeySource> &group_sources, const vector<idx_t> &payload_source_indices,
	    bool defer_grouped_finish, optional_ptr<bool> deferred_grouped_finish) {
		auto record_unsupported = [&](const char *reason) {
			auto path = string("direct_row_pointer_grouped_lookup_update_unsupported.") + reason;
			RecordSljitRegionRuntimePath(runtime, op.kind, path.c_str());
		};
		const auto count = payload_input.size();
		if (count == 0) {
			return false;
		}
		if (row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
			record_unsupported("non_flat_row_pointers");
			return false;
		}
		if (payload_source_indices.empty()) {
			record_unsupported("payload_sources");
			return false;
		}
		auto bind_stage_start = SljitRegionStageStart(runtime);
		bool bound = false;
		auto &binding =
		    BindNativeSink(native_runtime, scratch, op_idx, payload_input, op.aggregate_update.plan.sink_info,
		                   "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink", bound);
		if (bound) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_sink_contract", bind_stage_start);
		}
		if (!binding.ready || !binding.aggregate_update.ready || !op.aggregate_update.plan.use_primitive_payloads) {
			record_unsupported("sink_binding");
			return false;
		}
		auto &primitive = binding.aggregate_update.primitive;
		if (!primitive.ready) {
			record_unsupported("primitive_binding");
			return false;
		}
		const auto aggregate_traits = BuildGroupedAggregateRuntimeTraits(op.aggregate_update);
		if (!aggregate_traits.NeedsGroupedStateAddressPlan()) {
			record_unsupported("grouped_state_plan");
			return false;
		}
		auto &grouped_state = binding.aggregate_update.grouped_state;
		if (!grouped_state.ready || !grouped_state.state) {
			record_unsupported("grouped_state_binding");
			return false;
		}
		auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
		if (aggregates.size() != op.aggregate_update.payloads.size()) {
			record_unsupported("aggregate_payload_count");
			return false;
		}

		auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, primitive);
		auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
		const bool finish = !defer_grouped_finish;
		if (CanExecuteDirectGroupedFusedPayloadUpdate(scratch, op_idx, op, payload_input, payload_lanes, nullptr, count,
		                                              false)) {
			if (!TryExecuteDirectRowPointerGroupedFusedPayloadUpdate(
			        runtime, scratch, op_idx, op, payload_input, row_pointers, count, group_sources,
			        payload_source_indices, payload_lanes, grouped_state, payload_scratch, finish)) {
				record_unsupported("grouped_lookup_update");
				return false;
			}
			MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
			return true;
		}

		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		if (runtime.TraceRuntime()) {
			auto trace_stage_name = SljitRegionStageName(op_idx, op.kind, "direct_row_pointer_grouped_lookup_update");
			SljitRegionStageRecorder recorder(runtime, trace_stage_name);
			updated = grouped_state.state->TryUpdateNewGroupsWithRowPointerKeysPayloadInput(
			    payload_input, row_pointers, count, group_sources, payload_source_indices,
			    op.aggregate_update.plan.sink_info, payload_lanes, &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(trace_stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryUpdateNewGroupsWithRowPointerKeysPayloadInput(
			    payload_input, row_pointers, count, group_sources, payload_source_indices,
			    op.aggregate_update.plan.sink_info, payload_lanes, nullptr, finish);
		}
		scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind,
		                                  updated ? "direct_row_pointer_grouped_lookup_update"
		                                          : "direct_row_pointer_grouped_lookup_update_miss",
		                                  stage_start);
		if (!updated) {
			record_unsupported("row_pointer_payload_update");
			return false;
		}
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "row_pointer_grouped_lookup_update", count);
		MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
		return true;
	}

	SinkResultType
	ExecuteNativeAggregateUpdate(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
	                             SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op,
	                             DataChunk &input, const SelectionVector *execute_sel = nullptr,
	                             idx_t count = DConstants::INVALID_INDEX, bool defer_grouped_finish = false,
	                             optional_ptr<bool> deferred_grouped_finish = nullptr) {
		if (count == DConstants::INVALID_INDEX) {
			count = input.size();
		}
		auto bind_stage_start = SljitRegionStageStart(runtime);
		bool bound = false;
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.aggregate_update.plan.sink_info,
		                               "aggregate-update-runtime-binding-failed", "SLJIT aggregate update sink", bound);
		if (bound) {
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "bind_sink_contract", bind_stage_start);
		}
		if (!binding.ready || !binding.aggregate_update.ready) {
			throw InternalException("SLJIT aggregate update sink binding did not return a ready aggregate state");
		}
		if (op.aggregate_update.plan.use_primitive_payloads) {
			auto &primitive = binding.aggregate_update.primitive;
			if (!primitive.ready) {
				auto blocker = primitive.blocker.empty() ? "aggregate-primitive-binding-missing" : primitive.blocker;
				throw InternalException("SLJIT aggregate primitive update binding failed: %s", blocker.c_str());
			}
			auto &aggregates = op.aggregate_update.plan.sink_info.aggregates;
			if (aggregates.size() != op.aggregate_update.payloads.size()) {
				throw InternalException("SLJIT aggregate primitive payload count mismatch");
			}
			if (!op.aggregate_update.fused_payload_update_function &&
			    aggregates.size() != op.aggregate_update.payload_update_functions.size()) {
				throw InternalException("SLJIT aggregate primitive payload function count mismatch");
			}
			auto &payload_lanes = scratch.AggregatePayloadLanes(op_idx, aggregates, primitive);
			auto &payload_scratch = scratch.AggregatePayloadScratch(op_idx);
			const auto aggregate_traits = BuildGroupedAggregateRuntimeTraits(op.aggregate_update);
			optional_ptr<Vector> grouped_state_addresses;
			if (aggregate_traits.NeedsGroupedStateAddressPlan()) {
				auto &grouped_state = binding.aggregate_update.grouped_state;
				if (!grouped_state.ready || !grouped_state.state) {
					auto blocker = grouped_state.blocker.empty() ? "aggregate-grouped-state-binding-missing"
					                                             : grouped_state.blocker;
					throw InternalException("SLJIT aggregate grouped-state binding failed: %s", blocker.c_str());
				}
				if (TryExecuteDirectGroupedAggregateUpdateRoute(runtime, scratch, op_idx, op, input, payload_lanes,
				                                                execute_sel, count, grouped_state, payload_scratch,
				                                                defer_grouped_finish, deferred_grouped_finish)) {
					return SinkResultType::NEED_MORE_INPUT;
				}
				grouped_state_addresses = &scratch.AggregateStateAddresses(op_idx);
				if (CanResolveDirectNewGroupedStateAddresses(scratch, op_idx, op, input, execute_sel, count) &&
				    TryResolveDirectNewGroupedStateAddresses(runtime, scratch, op_idx, op, input, grouped_state,
				                                             *grouped_state_addresses, !defer_grouped_finish)) {
					MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
				} else if (runtime.TraceRuntime()) {
					auto resolve_stage_name = SljitRegionStageName(op_idx, op.kind, "resolve_grouped_state_addresses");
					auto resolve_stage_start = SljitRegionStageStart(runtime);
					SljitRegionStageRecorder resolve_recorder(runtime, resolve_stage_name);
					grouped_state.state->ResolveStateAddresses(input, *grouped_state_addresses, &resolve_recorder);
					RecordSljitRegionMaterializationBoundary(runtime, op.kind, "address_vector_resolve", input.size());
					auto resolve_runtime_us = SljitRegionElapsedMicros(resolve_stage_start);
					auto unattributed_runtime_us = resolve_runtime_us - resolve_recorder.RecordedRuntimeTimeUs();
					if (unattributed_runtime_us > 0) {
						runtime.RecordGeneratedStageRuntime(resolve_stage_name + ".unattributed",
						                                    unattributed_runtime_us);
					}
				} else {
					grouped_state.state->ResolveStateAddresses(input, *grouped_state_addresses);
					RecordSljitRegionMaterializationBoundary(runtime, op.kind, "address_vector_resolve", input.size());
				}
				RecordSljitRegionRuntimePath(runtime, op.kind, "resolve_grouped_state_addresses");
			}
			if (op.aggregate_update.fused_payload_update_function) {
				auto payload_stage_start = SljitRegionStageStart(runtime);
				if (op.aggregate_update.fused_payload_update_owns_group_lookup) {
					auto &grouped_state = binding.aggregate_update.grouped_state;
					ExecuteFusedPerfectHashGroupedPrimitiveAggregatePayloadUpdate(
					    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
					    op.aggregate_update.plan.sink_info.groups, op.aggregate_update.plan.group_expressions,
					    op.aggregate_update.plan.sink_info.aggregate_contract, payload_lanes,
					    grouped_state.perfect_hash_layout, input, execute_sel, count, payload_scratch);
					RecordSljitRegionRuntimePath(runtime, op.kind,
					                             "fused_payload_update_owns_perfect_hash_group_lookup");
					RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_perfect_hash_state_update",
					                                         count);
				} else if (op.aggregate_update.plan.use_grouped_state_addresses) {
					if (!grouped_state_addresses) {
						throw InternalException("SLJIT fused grouped aggregate update is missing state addresses");
					}
					grouped_state_addresses->Flatten();
					const auto grouped_state_address_data = FlatVector::GetData<uintptr_t>(*grouped_state_addresses);
					ExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
					    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
					    op.aggregate_update.plan.sink_info.aggregate_contract, payload_lanes, input,
					    grouped_state_address_data, nullptr, execute_sel, false, count, payload_scratch);
					RecordSljitRegionRuntimePath(runtime, op.kind, "fused_payload_update_with_grouped_state_addresses");
					RecordSljitRegionMaterializationBoundary(runtime, op.kind, "address_vector_payload_update", count);
				} else {
					ExecuteFusedPrimitiveAggregatePayloadUpdate(
					    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
					    payload_lanes, input, execute_sel, count, payload_scratch);
					RecordSljitRegionRuntimePath(runtime, op.kind, "fused_payload_update");
					RecordSljitRegionMaterializationBoundary(runtime, op.kind, "direct_state_update", count);
				}
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "primitive_payload_update_fused",
				                              payload_stage_start);
			} else {
				for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
					auto &aggregate = aggregates[payload_idx];
					auto lane = payload_lanes[payload_idx];
					if (!lane) {
						throw InternalException("SLJIT aggregate primitive lane missing for aggregate %llu",
						                        static_cast<unsigned long long>(aggregate.aggregate_index));
					}
					auto payload_stage_start = SljitRegionStageStart(runtime);
					ExecutePrimitiveAggregatePayloadUpdate(
					    op.aggregate_update.payloads[payload_idx],
					    op.aggregate_update.payload_update_functions[payload_idx], *lane, input, execute_sel, count,
					    scratch.ExpressionAdapterScratch(op_idx, payload_idx), grouped_state_addresses);
					RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "primitive_payload_update",
					                              payload_stage_start);
				}
				RecordSljitRegionRuntimePath(runtime, op.kind, "primitive_payload_update", aggregates.size());
				RecordSljitRegionMaterializationBoundary(
				    runtime, op.kind, grouped_state_addresses ? "address_vector_payload_update" : "direct_state_update",
				    count);
			}
			if (aggregate_traits.NeedsGroupedStateAddressPlan()) {
				if (defer_grouped_finish) {
					MarkDeferredGroupedFinish(defer_grouped_finish, deferred_grouped_finish);
				} else {
					auto finish_stage_start = SljitRegionStageStart(runtime);
					binding.aggregate_update.grouped_state.state->FinishStateUpdates();
					RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "finish_grouped_state_updates",
					                              finish_stage_start);
				}
			}
			return SinkResultType::NEED_MORE_INPUT;
		}
		throw InternalException("SLJIT aggregate update reached runtime without generated primitive payload code");
	}

	SinkResultType ExecuteNativeOrderSink(ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
	                                      SljitRegionExecutionScratch &scratch, idx_t op_idx,
	                                      SljitExecutableRegionOp &op, DataChunk &input, DataChunk &order_keys,
	                                      DataChunk &payload) {
		if (op.order_sink.order_keys.size() != order_keys.ColumnCount()) {
			throw InternalException("SLJIT ordered sink key expression count mismatch");
		}
		auto key_stage_start = SljitRegionStageStart(runtime);
		order_keys.Reset();
		for (idx_t key_idx = 0; key_idx < op.order_sink.order_keys.size(); key_idx++) {
			ExecuteProjectionExpression(op.order_sink.order_keys[key_idx], input, order_keys.data[key_idx], nullptr,
			                            input.size(), scratch.ExpressionAdapterScratch(op_idx, key_idx));
		}
		order_keys.SetChildCardinality(input.size());
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "key_projection", key_stage_start);

		auto sink_stage_start = SljitRegionStageStart(runtime);
		auto &binding = BindNativeSink(native_runtime, scratch, op_idx, input, op.order_sink.plan.sink_info,
		                               "ordered-sink-runtime-binding-failed", "SLJIT ordered sink");
		if (!binding.ready || !binding.ordered_sink.ready) {
			throw InternalException("SLJIT ordered sink binding did not return a ready ordered sink state");
		}
		payload.Reset();
		payload.Reference(input);
		auto result = ExecutionSinkOrdered(binding.ordered_sink, order_keys, payload);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "sink_update", sink_stage_start);
		return result;
	}

private:
	string backend_name;
	vector<SljitExecutableRegionOp> ops;
	vector<idx_t> source_distinct_counts;
	vector<Value> source_min_values;
	vector<Value> source_max_values;
	ExecutionRegionABI abi;
	mutex codegen_lock;
};

unique_ptr<ExecutionRegionKernel> CreateSljitNativeRegionKernel(ClientContext &context, string backend_name,
                                                                SljitExecutableRegion &&region,
                                                                ExecutionRegionABI abi) {
	(void)context;
	return make_uniq<SljitNativeRegionKernel>(
	    std::move(backend_name), std::move(region.ops), std::move(region.source_distinct_counts),
	    std::move(region.source_min_values), std::move(region.source_max_values), abi);
}

} // namespace duckdb
