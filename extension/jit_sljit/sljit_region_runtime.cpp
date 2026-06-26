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
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/execution/execution_region_backend.hpp"
#include "duckdb/execution/execution_operator_runtime.hpp"
#include "duckdb/execution/execution_region_runtime.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

#include <chrono>
#include <cstring>
#include <exception>

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
	static constexpr idx_t N_BITS = 4;
	static constexpr uint64_t SHIFT_MASK = 0x3F3F3F3F3F3F3F3F;
	const uint64_t shifts = hash & SHIFT_MASK;
	const auto shifts_8 = reinterpret_cast<const uint8_t *>(&shifts);
	uint64_t mask = 0;
	for (idx_t bit_idx = 8 - N_BITS; bit_idx < 8; bit_idx++) {
		mask |= (1ULL << shifts_8[bit_idx]);
	}
	return mask;
}

static inline bool SljitBloomFilterMayContain(const SljitNativeHashJoinProbeInput &input, hash_t hash) {
	if (!input.bloom_filter_bits) {
		return true;
	}
	const auto slot = input.bloom_filter_bits[hash & input.bloom_filter_bitmask];
	const auto mask = SljitBloomFilterMask(hash);
	return (slot & mask) == mask;
}

static bool SljitHashJoinMatchedProbeOutputMode(ExecutionHashJoinProbeOutputMode mode) {
	return mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	       mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
}

static bool SljitHashJoinCanUseFlatAllValidNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                        const ExecutionHashJoinTableLayout &layout,
                                                        const SljitNativeHashJoinProbeInput &input) {
	return !plan.residual_predicate && !plan.mark_build_match &&
	       SljitHashJoinMatchedProbeOutputMode(plan.output_mode) && !layout.chains_longer_than_one &&
	       !input.source_sel && !input.source_validity && !input.resume_row_pointer &&
	       input.count <= input.output_capacity;
}

static bool SljitHashJoinCanUseSelectedAllValidNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                            const ExecutionHashJoinTableLayout &layout,
                                                            const SljitNativeHashJoinProbeInput &input) {
	return !plan.residual_predicate && !plan.mark_build_match &&
	       SljitHashJoinMatchedProbeOutputMode(plan.output_mode) && !layout.chains_longer_than_one &&
	       input.source_sel && !input.source_validity && !input.resume_row_pointer &&
	       input.count <= input.output_capacity;
}

static bool SljitHashJoinCanUseAllValidSingleKeyChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                           const ExecutionHashJoinTableLayout &layout,
                                                           const SljitNativeHashJoinProbeInput &input) {
	return plan.keys.size() == 1 && plan.equality_key_count == 1 && !plan.residual_predicate &&
	       !plan.mark_build_match && SljitHashJoinMatchedProbeOutputMode(plan.output_mode) &&
	       layout.chains_longer_than_one && !layout.needs_chain_matcher && !input.source_validity &&
	       input.output_capacity > 0 && (!layout.dictionary_emission || layout.aux_next_ptrs);
}

static bool SljitHashJoinCanUseFlatAllValidPairChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                          const ExecutionHashJoinTableLayout &layout,
                                                          const SljitNativeHashJoinProbeInput &input) {
	return plan.keys.size() == 2 && plan.equality_key_count == 2 && !plan.residual_predicate &&
	       SljitHashJoinMatchedProbeOutputMode(plan.output_mode) && layout.chains_longer_than_one &&
	       !layout.needs_chain_matcher && !input.source_sel && !input.source_validity && input.output_capacity > 0 &&
	       (!layout.dictionary_emission || layout.aux_next_ptrs);
}

static bool SljitHashJoinCanUseSelectedAllValidPairChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                              const ExecutionHashJoinTableLayout &layout,
                                                              const SljitNativeHashJoinProbeInput &input) {
	return plan.keys.size() == 2 && plan.equality_key_count == 2 && !plan.residual_predicate &&
	       SljitHashJoinMatchedProbeOutputMode(plan.output_mode) && layout.chains_longer_than_one &&
	       !layout.needs_chain_matcher && input.source_sel && input.source_sel[0] && !input.source_validity &&
	       input.output_capacity > 0 && (!layout.dictionary_emission || layout.aux_next_ptrs);
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

template <bool SELECTED, bool USE_SALT, bool HAS_BLOOM>
static void ExecuteAllValidInt64PairNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                 SljitNativeHashJoinProbeInput &input) {
	const auto key0_data = reinterpret_cast<const uint64_t *>(input.source_data[0]);
	const auto key1_data = reinterpret_cast<const uint64_t *>(input.source_data[1]);
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const sel_t *key_sel = nullptr;
	if (SELECTED) {
		key_sel = input.source_sel[0];
	}
	auto selected_count = input.selected_count;
	const auto key0_offset = plan.keys[0].key_layout_offset;
	const auto key1_offset = plan.keys[1].key_layout_offset;
	const auto bitmask = input.bitmask;

	auto row_idx = input.input_offset;
	if (row_idx < input.count) {
		auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		auto key0 = key0_data[source_idx];
		auto key1 = key1_data[source_idx];
		auto hash = SljitHashJoinCombineHashScalar(Hash<uint64_t>(key0), Hash<uint64_t>(key1));
		auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);
		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			uint64_t next_key0 = 0;
			uint64_t next_key1 = 0;
			hash_t next_hash = 0;
			hash_t next_salt = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				const auto next_source_idx = SELECTED ? key_sel[next_row_idx] : UnsafeNumericCast<sel_t>(next_row_idx);
				next_key0 = key0_data[next_source_idx];
				next_key1 = key1_data[next_source_idx];
				next_hash = SljitHashJoinCombineHashScalar(Hash<uint64_t>(next_key0), Hash<uint64_t>(next_key1));
				next_salt = ht_entry_t::ExtractSalt(next_hash);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}
			if (!HAS_BLOOM || SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto entry_value = entries[ht_offset].GetValue();
					if (!entry_value) {
						break;
					}
					if (!USE_SALT || ht_entry_t::ExtractSalt(entry_value) == salt) {
						auto row_location = SljitHashJoinEntryPointer(entry_value);
						if (SljitHashJoinKeysEqual<uint64_t>(row_location, key0_offset, key0) &&
						    SljitHashJoinKeysEqual<uint64_t>(row_location, key1_offset, key1)) {
							input.row_pointers[selected_count] = row_location;
							input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
							selected_count++;
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
			salt = next_salt;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

template <bool SELECTED>
static void ExecuteAllValidInt64PairNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                 SljitNativeHashJoinProbeInput &input) {
	if (input.use_salt) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidInt64PairNoChainProbe<SELECTED, true, true>(plan, input);
		} else {
			ExecuteAllValidInt64PairNoChainProbe<SELECTED, true, false>(plan, input);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidInt64PairNoChainProbe<SELECTED, false, true>(plan, input);
		} else {
			ExecuteAllValidInt64PairNoChainProbe<SELECTED, false, false>(plan, input);
		}
	}
}

static bool TryExecuteFlatAllValidInt64PairNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                        const ExecutionHashJoinTableLayout &layout,
                                                        SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseInt64PairProbe(plan) || !SljitHashJoinCanUseFlatAllValidNoChainProbe(plan, layout, input)) {
		return false;
	}
	ExecuteAllValidInt64PairNoChainProbe<false>(plan, input);
	return true;
}

static bool TryExecuteSelectedAllValidInt64PairNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                            const ExecutionHashJoinTableLayout &layout,
                                                            SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseInt64PairProbe(plan) ||
	    !SljitHashJoinCanUseSelectedAllValidNoChainProbe(plan, layout, input) || !input.source_sel[0]) {
		return false;
	}
	ExecuteAllValidInt64PairNoChainProbe<true>(plan, input);
	return true;
}

static bool TryExecuteFlatAllValidInt64PairChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                      const ExecutionHashJoinTableLayout &layout,
                                                      SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseFlatAllValidPairChainProbe(plan, layout, input)) {
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
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto key0_offset = plan.keys[0].key_layout_offset;
	const auto key1_offset = plan.keys[1].key_layout_offset;
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto selected_count = input.selected_count;
	auto row_idx = input.input_offset;
	auto resume_row_pointer = input.resume_row_pointer;

	while (row_idx < input.count) {
		const auto key0 = key0_data[row_idx];
		const auto key1 = key1_data[row_idx];
		data_ptr_t row_location = resume_row_pointer;
		resume_row_pointer = nullptr;
		if (!row_location) {
			const auto hash = SljitHashJoinCombineHashScalar(Hash<uint64_t>(key0), Hash<uint64_t>(key1));
			const auto salt = ht_entry_t::ExtractSalt(hash);
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			if (SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto &entry = entries[ht_offset];
					if (!entry.IsOccupied()) {
						break;
					}
					if (!input.use_salt || entry.GetSalt() == salt) {
						row_location = entry.GetPointer();
						SljitPrefetchHashJoinRow(row_location, key0_offset);
						if (SljitHashJoinKeysEqual<uint64_t>(row_location, key0_offset, key0) &&
						    SljitHashJoinKeysEqual<uint64_t>(row_location, key1_offset, key1)) {
							break;
						}
					}
					row_location = nullptr;
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
				}
			}
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

static bool TryExecuteSelectedAllValidInt64PairChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                          const ExecutionHashJoinTableLayout &layout,
                                                          SljitNativeHashJoinProbeInput &input) {
	if (!SljitHashJoinCanUseSelectedAllValidPairChainProbe(plan, layout, input)) {
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
	const auto key_sel = input.source_sel[0];
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const auto key0_offset = plan.keys[0].key_layout_offset;
	const auto key1_offset = plan.keys[1].key_layout_offset;
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto selected_count = input.selected_count;
	auto row_idx = input.input_offset;
	auto resume_row_pointer = input.resume_row_pointer;

	while (row_idx < input.count) {
		const auto source_idx = key_sel[row_idx];
		const auto key0 = key0_data[source_idx];
		const auto key1 = key1_data[source_idx];
		data_ptr_t row_location = resume_row_pointer;
		resume_row_pointer = nullptr;
		if (!row_location) {
			const auto hash = SljitHashJoinCombineHashScalar(Hash<uint64_t>(key0), Hash<uint64_t>(key1));
			const auto salt = ht_entry_t::ExtractSalt(hash);
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			if (SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto &entry = entries[ht_offset];
					if (!entry.IsOccupied()) {
						break;
					}
					if (!input.use_salt || entry.GetSalt() == salt) {
						row_location = entry.GetPointer();
						SljitPrefetchHashJoinRow(row_location, key0_offset);
						if (SljitHashJoinKeysEqual<uint64_t>(row_location, key0_offset, key0) &&
						    SljitHashJoinKeysEqual<uint64_t>(row_location, key1_offset, key1)) {
							break;
						}
					}
					row_location = nullptr;
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
				}
			}
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
			const auto salt = ht_entry_t::ExtractSalt(hash);
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			if (SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto &entry = entries[ht_offset];
					if (!entry.IsOccupied()) {
						break;
					}
					if (!input.use_salt || entry.GetSalt() == salt) {
						row_location = entry.GetPointer();
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<uint64_t>(row_location, key_offset, key_value)) {
							break;
						}
					}
					row_location = nullptr;
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
				}
			}
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

template <class T, bool SELECTED, bool USE_SALT, bool HAS_BLOOM>
static void ExecuteAllValidSingleKeyNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                 SljitNativeHashJoinProbeInput &input) {
	const auto key_data = reinterpret_cast<const T *>(input.source_data[0]);
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	const sel_t *key_sel = nullptr;
	if (SELECTED) {
		key_sel = input.source_sel[0];
	}
	auto selected_count = input.selected_count;
	const auto key_offset = plan.keys[0].key_layout_offset;
	const auto bitmask = input.bitmask;

	auto row_idx = input.input_offset;
	if (row_idx < input.count) {
		auto source_idx = SELECTED ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
		auto key = key_data[source_idx];
		auto hash = Hash<T>(key);
		auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			T next_key = 0;
			hash_t next_hash = 0;
			hash_t next_salt = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				const auto next_source_idx = SELECTED ? key_sel[next_row_idx] : UnsafeNumericCast<sel_t>(next_row_idx);
				next_key = key_data[next_source_idx];
				next_hash = Hash<T>(next_key);
				next_salt = ht_entry_t::ExtractSalt(next_hash);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			if (!HAS_BLOOM || SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto entry_value = entries[ht_offset].GetValue();
					if (!entry_value) {
						break;
					}
					if (!USE_SALT || ht_entry_t::ExtractSalt(entry_value) == salt) {
						auto row_location = SljitHashJoinEntryPointer(entry_value);
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<T>(row_location, key_offset, key)) {
							input.row_pointers[selected_count] = row_location;
							input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
							selected_count++;
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
			salt = next_salt;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

static void ExecuteFlatAllValidSingleKeyNoChainProbeInt64ToInt32(const SljitNativeHashJoinProbePlan &plan,
                                                                 SljitNativeHashJoinProbeInput &input) {
	const auto key_data = reinterpret_cast<const int64_t *>(input.source_data[0]);
	const auto entries = reinterpret_cast<const ht_entry_t *>(input.entries);
	auto selected_count = input.selected_count;
	const auto key_offset = plan.keys[0].key_layout_offset;
	const auto bitmask = input.bitmask;

	auto row_idx = input.input_offset;
	if (row_idx < input.count) {
		auto key = SljitCastHashJoinKeyInt64ToInt32(input, key_data[row_idx]);
		auto hash = Hash<int32_t>(key);
		auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			int32_t next_key = 0;
			hash_t next_hash = 0;
			hash_t next_salt = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				next_key = SljitCastHashJoinKeyInt64ToInt32(input, key_data[next_row_idx]);
				next_hash = Hash<int32_t>(next_key);
				next_salt = ht_entry_t::ExtractSalt(next_hash);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			if (SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto entry_value = entries[ht_offset].GetValue();
					if (!entry_value) {
						break;
					}
					if (!input.use_salt || ht_entry_t::ExtractSalt(entry_value) == salt) {
						auto row_location = SljitHashJoinEntryPointer(entry_value);
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<int32_t>(row_location, key_offset, key)) {
							input.row_pointers[selected_count] = row_location;
							input.match_sel[selected_count] = UnsafeNumericCast<sel_t>(row_idx);
							selected_count++;
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
			salt = next_salt;
			ht_offset = next_ht_offset;
		}
	}

	input.selected_count = selected_count;
	input.input_offset = input.count;
	input.resume_row_pointer = nullptr;
	input.finished = true;
}

template <class T, bool SELECTED>
static void ExecuteAllValidSingleKeyNoChainProbe(const SljitNativeHashJoinProbePlan &plan,
                                                 SljitNativeHashJoinProbeInput &input) {
	if (input.use_salt) {
		if (input.bloom_filter_bits) {
			ExecuteAllValidSingleKeyNoChainProbe<T, SELECTED, true, true>(plan, input);
		} else {
			ExecuteAllValidSingleKeyNoChainProbe<T, SELECTED, true, false>(plan, input);
		}
	} else {
		if (input.bloom_filter_bits) {
			ExecuteAllValidSingleKeyNoChainProbe<T, SELECTED, false, true>(plan, input);
		} else {
			ExecuteAllValidSingleKeyNoChainProbe<T, SELECTED, false, false>(plan, input);
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
	    !SljitHashJoinCanUseSelectedAllValidNoChainProbe(plan, layout, input) || !input.source_sel[0]) {
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
	if (!SljitHashJoinCanUseSingleKeyProbe(plan) || !SljitHashJoinCanUseFlatAllValidNoChainProbe(plan, layout, input)) {
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
			const auto salt = ht_entry_t::ExtractSalt(hash);
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			if (SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto &entry = entries[ht_offset];
					if (!entry.IsOccupied()) {
						break;
					}
					if (!input.use_salt || entry.GetSalt() == salt) {
						row_location = entry.GetPointer();
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<T>(row_location, key_offset, key)) {
							break;
						}
					}
					row_location = nullptr;
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
				}
			}
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
			const auto salt = ht_entry_t::ExtractSalt(hash);
			auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);
			if (SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto &entry = entries[ht_offset];
					if (!entry.IsOccupied()) {
						break;
					}
					if (!input.use_salt || entry.GetSalt() == salt) {
						row_location = entry.GetPointer();
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<int32_t>(row_location, key_offset, key)) {
							break;
						}
					}
					row_location = nullptr;
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
				}
			}
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
		auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			T next_key = 0;
			hash_t next_hash = 0;
			hash_t next_salt = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				next_key = key_data[next_row_idx];
				next_hash = Hash<T>(next_key);
				next_salt = ht_entry_t::ExtractSalt(next_hash);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & input.bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			data_ptr_t row_location = nullptr;
			if (SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto &entry = entries[ht_offset];
					if (!entry.IsOccupied()) {
						break;
					}
					if (!input.use_salt || entry.GetSalt() == salt) {
						row_location = entry.GetPointer();
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<T>(row_location, key_offset, key)) {
							break;
						}
					}
					row_location = nullptr;
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
				}
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
			salt = next_salt;
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
		auto salt = ht_entry_t::ExtractSalt(hash);
		auto ht_offset = UnsafeNumericCast<idx_t>(hash & input.bitmask);

		while (true) {
			const auto next_row_idx = row_idx + 1;
			const bool has_next = next_row_idx < input.count;
			int32_t next_key = 0;
			hash_t next_hash = 0;
			hash_t next_salt = 0;
			idx_t next_ht_offset = 0;
			if (has_next) {
				next_key = SljitCastHashJoinKeyInt64ToInt32(input, key_data[next_row_idx]);
				next_hash = Hash<int32_t>(next_key);
				next_salt = ht_entry_t::ExtractSalt(next_hash);
				next_ht_offset = UnsafeNumericCast<idx_t>(next_hash & input.bitmask);
				SljitPrefetchHashJoinEntry(entries, next_ht_offset);
			}

			data_ptr_t row_location = nullptr;
			if (SljitBloomFilterMayContain(input, hash)) {
				while (true) {
					const auto &entry = entries[ht_offset];
					if (!entry.IsOccupied()) {
						break;
					}
					if (!input.use_salt || entry.GetSalt() == salt) {
						row_location = entry.GetPointer();
						SljitPrefetchHashJoinRow(row_location, key_offset);
						if (SljitHashJoinKeysEqual<int32_t>(row_location, key_offset, key)) {
							break;
						}
					}
					row_location = nullptr;
					ht_offset = UnsafeNumericCast<idx_t>((ht_offset + 1) & input.bitmask);
				}
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
			salt = next_salt;
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
		return ops.size() == 4 && ops[0].kind == SljitNativeRegionOpKind::FILTER &&
		       ops[1].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[2].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[3].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[3].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE;
	}

	bool CanBatchHashJoinProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 3 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[2].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[2].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
		       ProjectionOutputsAreFixedWidth(ops[1]);
	}

	bool CanBatchHashJoinProjectionProjectionGroupedAggregateFullPipeline() const {
		return ops.size() == 4 && ops[0].kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		       ops[1].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[2].kind == SljitNativeRegionOpKind::PROJECTION &&
		       ops[3].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       ops[3].aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE &&
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
		    shipmode_decompress.source_index != 1 ||
		    shipmode_decompress.return_type.id() != LogicalTypeId::VARCHAR) {
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
		    shipmode_compress.source_index != 1 ||
		    shipmode_compress.return_type.id() != LogicalTypeId::UBIGINT) {
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
				if (plan.source_index >= input.ColumnCount() || plan.return_type != input.data[plan.source_index].GetType() ||
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
			if (!TryFastDecodeInlineCompressedString16(input.data[plan.source_index], count, output.data[projection_idx])) {
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
		if (!CanDirectBuildQ7SecondJoinInput() || source_chunk.ColumnCount() < 2 || second_join_input.ColumnCount() != 5 ||
		    !scratch.HasOperatorBinding(1) || target_offset + count > STANDARD_VECTOR_SIZE) {
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
		if (CanBatchHashJoinProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineHashJoinProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineHashJoinProjectionProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchProjectionHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineProjectionHashJoinProjectionProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchProjectionHashJoinProjectionHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineProjectionHashJoinProjectionHashJoinProjectionProjectionGroupedAggregateBatched(
			    runtime, result);
		}
		if (CanBatchHashJoinHashJoinProjectionProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineHashJoinHashJoinProjectionProjectionGroupedAggregateBatched(runtime, result);
		}
		if (CanBatchGeneratedFilterProjectionHashJoinProjectionGroupedAggregateFullPipeline()) {
			return TryExecuteFullPipelineGeneratedFilterHashJoinProjectionGroupedAggregateBatched(runtime, result);
		}
		return TryExecuteFullPipelineUnbatched(runtime, result);
	}

	bool TryExecuteFullPipelineUnbatched(ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t processed_chunks = 0;
		const auto max_chunks = CanUseExtendedRegularHashAggregateSourceBudget()
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
				RecordSljitRegionStageRuntime(runtime, 1, first_projection_op.kind,
				                              "post_join_reference_projection", first_projection_stage_start);
			} else {
				ExecuteProjection(scratch, 1, first_projection_op, join_output, intermediate);
				RecordSljitRegionStageRuntime(runtime, 1, first_projection_op.kind, "post_join_batch_projection",
				                              first_projection_stage_start);
			}

			projected.Reset();
			auto second_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, intermediate, second_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 2, second_projection_op.kind,
				                              "post_join_reference_projection", second_projection_stage_start);
			} else {
				ExecuteProjection(scratch, 2, second_projection_op, intermediate, projected);
				RecordSljitRegionStageRuntime(runtime, 2, second_projection_op.kind, "post_join_batch_projection",
				                              second_projection_stage_start);
			}
		};
		return TryExecuteFullPipelineHashJoinProjectedGroupedAggregateBatched(runtime, result, 0, 2, 3,
		                                                                      prepare_join_input, project_join_output);
	}

	bool TryExecuteFullPipelineProjectionHashJoinProjectionProjectionGroupedAggregateBatched(
	    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
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
					return;
				}
			}
			auto &first_projection_op = ops[2];
			auto &second_projection_op = ops[3];
			auto &intermediate = scratch.TemporaryChunk(2);

			intermediate.Reset();
			auto first_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(intermediate, join_output, first_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 2, first_projection_op.kind,
				                              "post_join_reference_projection", first_projection_stage_start);
			} else {
				ExecuteProjection(scratch, 2, first_projection_op, join_output, intermediate);
				RecordSljitRegionStageRuntime(runtime, 2, first_projection_op.kind, "post_join_batch_projection",
				                              first_projection_stage_start);
			}

			projected.Reset();
			auto second_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, intermediate, second_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 3, second_projection_op.kind,
				                              "post_join_reference_projection", second_projection_stage_start);
			} else {
				ExecuteProjection(scratch, 3, second_projection_op, intermediate, projected);
				RecordSljitRegionStageRuntime(runtime, 3, second_projection_op.kind, "post_join_batch_projection",
				                              second_projection_stage_start);
			}
		};
		return TryExecuteFullPipelineHashJoinProjectedGroupedAggregateBatched(runtime, result, 1, 3, 4,
		                                                                      prepare_join_input, project_join_output,
		                                                                      first_join_unchecked_key_cast);
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
			} else {
				ExecuteProjection(scratch, 4, first_final_projection_op, second_join_output, intermediate);
				RecordSljitRegionStageRuntime(runtime, 4, first_final_projection_op.kind,
				                              "post_second_join_batch_projection", first_projection_stage_start);
			}

			projected.Reset();
			auto final_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, intermediate, final_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 5, final_projection_op.kind,
				                              "post_second_join_reference_projection", final_projection_stage_start);
			} else {
				ExecuteProjection(scratch, 5, final_projection_op, intermediate, projected);
				RecordSljitRegionStageRuntime(runtime, 5, final_projection_op.kind,
				                              "post_second_join_batch_projection", final_projection_stage_start);
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
				    scratch.HashJoinSourceFormats(3), scratch.HashJoinSourceData(3), scratch.HashJoinSourceSelections(3),
				    scratch.HashJoinSourceValidity(3),
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(3)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(3)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(3)
				        : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(3)
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
			} else {
				ExecuteProjection(scratch, 2, between_join_projection_op, batch, second_join_input);
				RecordSljitRegionStageRuntime(runtime, 2, between_join_projection_op.kind,
				                              "between_join_batch_projection", projection_stage_start);
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
				    scratch.HashJoinSourceFormats(1), scratch.HashJoinSourceData(1), scratch.HashJoinSourceSelections(1),
				    scratch.HashJoinSourceValidity(1),
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(1)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(1)
				                                                               : nullptr,
				    first_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(1)
				        : nullptr,
					    first_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(1)
					                                                               : nullptr,
						    first_state, deferred_reason, first_join_unchecked_key_cast,
						    direct_first_join_to_second_join);
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

	bool TryExecuteFullPipelineHashJoinHashJoinProjectionProjectionGroupedAggregateBatched(
	    ExecutionRegionRuntime &runtime, ExecutionRegionResult &result) {
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
				RecordSljitRegionStageRuntime(runtime, 2, first_projection_op.kind,
				                              "post_join_reference_projection", first_projection_stage_start);
			} else {
				ExecuteProjection(scratch, 2, first_projection_op, join_output, intermediate);
				RecordSljitRegionStageRuntime(runtime, 2, first_projection_op.kind, "post_join_batch_projection",
				                              first_projection_stage_start);
			}

			projected.Reset();
			auto second_projection_stage_start = SljitRegionStageStart(runtime);
			if (TryReferenceProjection(projected, intermediate, final_projection_op)) {
				RecordSljitRegionStageRuntime(runtime, 3, final_projection_op.kind,
				                              "post_join_reference_projection", second_projection_stage_start);
			} else {
				ExecuteProjection(scratch, 3, final_projection_op, intermediate, projected);
				RecordSljitRegionStageRuntime(runtime, 3, final_projection_op.kind, "post_join_batch_projection",
				                              second_projection_stage_start);
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
				    scratch.HashJoinSourceFormats(1), scratch.HashJoinSourceData(1), scratch.HashJoinSourceSelections(1),
				    scratch.HashJoinSourceValidity(1),
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(1)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(1)
				                                                                : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate
				        ? &scratch.HashJoinResidualMatchSelection(1)
				        : nullptr,
				    second_hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(1)
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
				    scratch.HashJoinSourceFormats(0), scratch.HashJoinSourceData(0), scratch.HashJoinSourceSelections(0),
				    scratch.HashJoinSourceValidity(0),
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

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
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
				if (flush_projected_batch()) {
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
			} else {
				ExecuteProjection(scratch, projection_idx, projection_op, join_output, projected);
				RecordSljitRegionStageRuntime(runtime, projection_idx, projection_op.kind, "post_join_batch_projection",
				                              projection_stage_start);
			}
		};
		return TryExecuteFullPipelineHashJoinProjectedGroupedAggregateBatched(runtime, result, hash_join_idx,
		                                                                      projection_idx, aggregate_idx,
		                                                                      prepare_join_input, project_join_output);
	}

	template <class PREPARE_JOIN_INPUT, class PROJECT_JOIN_OUTPUT>
		bool TryExecuteFullPipelineHashJoinProjectedGroupedAggregateBatched(ExecutionRegionRuntime &runtime,
		                                                                    ExecutionRegionResult &result,
		                                                                    idx_t hash_join_idx,
		                                                                    idx_t final_projection_idx,
		                                                                    idx_t aggregate_idx,
		                                                                    PREPARE_JOIN_INPUT prepare_join_input,
		                                                                    PROJECT_JOIN_OUTPUT project_join_output,
		                                                                    bool source_key0_int64_to_int32_unchecked =
		                                                                        false) {
		SljitRegionExecutionScratch scratch(runtime.GetAllocator(), ops);
		idx_t fetched_chunks = 0;
		idx_t processed_batches = 0;
		bool deferred_grouped_finish = false;
		const auto max_source_fetches = SljitBatchedSourceContractFetchBudget(runtime.MaxChunks());
		auto &hash_join_op = ops[hash_join_idx];
		auto &final_projection_op = ops[final_projection_idx];
		auto &aggregate_op = ops[aggregate_idx];
		auto &native_runtime = runtime.ExecutionOperators();

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
			RecordSljitRegionStageRuntime(runtime, final_projection_idx, final_projection_op.kind, "post_join_batch_append",
			                              append_stage_start);
			if (batch.size() == STANDARD_VECTOR_SIZE) {
				if (flush_projected_batch()) {
					return true;
				}
			}
			return false;
		};

		auto execute_source_chunk = [&](DataChunk &source_chunk, bool have_more_output) -> bool {
			DataChunk *join_input = nullptr;
			auto source_result = have_more_output ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
			if (prepare_join_input(scratch, source_chunk, source_result, join_input)) {
				return true;
			}
			if (!join_input || join_input->size() == 0) {
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
				    runtime, native_runtime, scratch, hash_join_idx, hash_join_op, *join_input, join_output,
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
					    state, deferred_reason, source_key0_int64_to_int32_unchecked);
				RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, stage_start);
				if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
					finish_deferred_grouped_update();
					runtime.Defer(std::move(deferred_reason));
					result = ExecutionRegionResult::DEFERRED;
					return true;
				}
				if (join_output.size() == 0) {
					continue;
				}
				project_join_output(scratch, join_output, projected);
				if (append_projected_batch(projected)) {
					return true;
				}
			} while (!HashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
			return false;
		};

		while (true) {
			if (processed_batches >= runtime.MaxChunks() || fetched_chunks >= max_source_fetches) {
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
				if (flush_projected_batch()) {
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

	SinkResultType DrainNativeHashJoinProbe(ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch,
	                                        idx_t op_idx, DataChunk &input, DataChunk &output) {
		auto &op = ops[op_idx];
		SljitHashJoinProbeDrainState state;
		auto &native_runtime = runtime.ExecutionOperators();
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
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
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
		case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
		case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
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
		if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS) {
			native_input.source_data = NativeUnsignedIntegerSourceData(*source_format, plan.unsigned_source_width);
		} else if (plan.kind == SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS ||
		           plan.kind == SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST ||
		           plan.kind == SljitNativeRegionExpressionKind::INTEGER_CAST) {
			native_input.source_data = NativeSignedIntegerSourceData(*source_format, plan.cast_source_width);
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

	bool TryDirectMaterializeFixedProjection(SljitExecutableRegionOp &op, DataChunk &input,
	                                         optional_ptr<DirectAppendSlice> slice,
	                                         optional_ptr<FixedDirectAppendSourceCache> source_cache = nullptr,
	                                         optional_ptr<vector<uint8_t>> skip_projection = nullptr) {
		if (op.kind != SljitNativeRegionOpKind::PROJECTION || op.projections.empty()) {
			return false;
		}
		const bool execute = slice != nullptr;
		const auto source_offset = execute ? slice->source_offset : 0;
		const auto count = execute ? slice->count : input.size();
		for (idx_t projection_idx = 0; projection_idx < op.projections.size(); projection_idx++) {
			if (skip_projection && projection_idx < skip_projection->size() && (*skip_projection)[projection_idx]) {
				continue;
			}
			if (TryBindDirectAppendSourceProjection(op.projections[projection_idx], input, slice, projection_idx,
			                                        source_offset, count, execute)) {
				continue;
			}
			data_ptr_t target = nullptr;
			if (execute) {
				if (slice->targets.size() != op.projections.size()) {
					throw InternalException("SLJIT fixed direct append target count mismatch");
				}
				target = slice->targets[projection_idx];
			}
			if (!TryDirectMaterializeFixedExpression(op.projections[projection_idx], input, target, source_offset,
			                                         count, execute, source_cache)) {
				return false;
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
	    Vector &grouped_state_addresses, idx_t count, SljitAggregatePayloadAdapterScratch &adapter_scratch) {
		if (!function) {
			throw InternalException("SLJIT fused grouped aggregate primitive payload update is missing generated code");
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
				source_validity[source_idx] =
				    SljitNormalizedSourceValidityData(source_formats[source_idx], source_sel[source_idx], count);
				flat_no_selection = flat_no_selection && source_sel[source_idx] == nullptr;
				flat_all_valid =
				    flat_all_valid && source_sel[source_idx] == nullptr && source_validity[source_idx] == nullptr;
				all_valid = all_valid && source_validity[source_idx] == nullptr;
			}

			grouped_state_addresses.Flatten();
			SljitNativeVectorInput native_input;
			native_input.source_data_array = source_data.data();
			native_input.source_sel_array = source_sel.data();
			native_input.source_validity_array = source_validity.data();
			native_input.expression_tree_flat_no_selection = flat_no_selection;
			native_input.expression_tree_flat_all_valid = flat_all_valid;
			native_input.expression_tree_all_valid = all_valid;
			native_input.aggregate_state_addresses = FlatVector::GetData<uintptr_t>(grouped_state_addresses);
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
			source_validity[payload_idx] =
			    SljitNormalizedSourceValidityData(source_formats[payload_idx], source_sel[payload_idx], input.size());
		}

		grouped_state_addresses.Flatten();
		SljitNativeVectorInput native_input;
		native_input.source_data_array = source_data.data();
		native_input.source_sel_array = SljitPointerArrayOrNull(source_sel);
		native_input.source_validity_array = SljitPointerArrayOrNull(source_validity);
		native_input.aggregate_state_addresses = FlatVector::GetData<uintptr_t>(grouped_state_addresses);
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
		idx_t count = 0;
		SljitAggregatePayloadAdapterScratch *adapter_scratch = nullptr;
	};

	static void ExecuteSljitGroupedStateAddressUpdate(Vector &addresses, void *state_p) {
		auto &state = *reinterpret_cast<SljitGroupedStateAddressUpdateState *>(state_p);
		if (!state.kernel || !state.payloads || !state.function || !state.aggregates || !state.contract ||
		    !state.lanes || !state.input || !state.adapter_scratch) {
			throw InternalException("SLJIT grouped state-address callback is incomplete");
		}
		state.kernel->ExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
		    *state.payloads, state.function, *state.aggregates, *state.contract, *state.lanes, *state.input, addresses,
		    state.count, *state.adapter_scratch);
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

	idx_t ApplyNativeHashJoinResidualPredicate(SljitExecutableRegionOp &op, const ExecutionHashJoinProbeBinding &probe,
	                                           DataChunk &input, Vector &row_pointers, SelectionVector &match_selection,
	                                           idx_t count, DataChunk *residual_chunk,
	                                           SelectionVector *residual_selection,
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
	                            SljitHashJoinProbeDrainState &state) {
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
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, SLJIT_GENERATED_PERFECT_HASH_JOIN_PROBE_STAGE,
		                              generated_stage_start);
		state.input_offset = native_input.input_offset;
		state.resume_row_pointer = nullptr;
		state.finished = native_input.finished;
		if (native_input.selected_count == 0) {
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
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
		const bool rhs_keys_all_valid =
		    !layout.can_have_null || (layout.null_keys_are_filtered && !layout.found_match_column_present);
		const bool use_flat_all_valid_probe = !source_sel_array && !source_validity_array && rhs_keys_all_valid;
		const bool use_selected_all_valid_probe =
		    source_sel_array && SljitCommonSelectionOrNull(source_sel) && !source_validity_array && rhs_keys_all_valid;

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
		if (use_flat_all_valid_probe) {
			if (TryExecuteFlatAllValidInt64PairNoChainProbe(op.hash_join_probe.plan, layout, native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_FLAT_ALL_VALID_INT64_PAIR_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else if (TryExecuteFlatAllValidInt64PairChainProbe(op.hash_join_probe.plan, layout, native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_FLAT_ALL_VALID_INT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else if (TryExecuteAllValidSingleKeyNotEqualPredicateChainProbe<false>(op.hash_join_probe.plan, layout,
			                                                                         native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else if (TryExecuteFlatAllValidSingleKeyNoChainProbe(op.hash_join_probe.plan, layout, native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else if (TryExecuteAllValidSingleKeyChainProbe<false>(op.hash_join_probe.plan, layout, native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_FLAT_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else {
				auto function = EnsureFlatAllValidRegularHashJoinProbeCode(runtime, op.hash_join_probe, layout.use_salt,
				                                                           layout.chains_longer_than_one,
				                                                           layout.dictionary_emission);
				function(&native_input);
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_GENERATED_FLAT_ALL_VALID_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			}
		} else if (use_selected_all_valid_probe) {
			if (TryExecuteSelectedAllValidInt64PairNoChainProbe(op.hash_join_probe.plan, layout, native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_SELECTED_ALL_VALID_INT64_PAIR_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else if (TryExecuteSelectedAllValidInt64PairChainProbe(op.hash_join_probe.plan, layout, native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_SELECTED_ALL_VALID_INT64_PAIR_CHAIN_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else if (TryExecuteSelectedAllValidSingleKeyNoChainProbe(op.hash_join_probe.plan, layout, native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else if (TryExecuteAllValidSingleKeyChainProbe<true>(op.hash_join_probe.plan, layout, native_input)) {
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_CHAIN_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			} else if (TryExecuteAllValidSingleKeyNotEqualPredicateChainProbe<true>(op.hash_join_probe.plan, layout,
			                                                                        native_input)) {
				RecordSljitRegionStageRuntime(
				    runtime, op_idx, op.kind,
				    SLJIT_FAST_SELECTED_ALL_VALID_SINGLE_KEY_NOTEQUAL_CHAIN_HASH_JOIN_PROBE_STAGE,
				    generated_stage_start);
			} else {
				auto function = EnsureSelectedAllValidRegularHashJoinProbeCode(
				    runtime, op.hash_join_probe, layout.use_salt, layout.chains_longer_than_one,
				    layout.dictionary_emission);
				function(&native_input);
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
				                              SLJIT_GENERATED_SELECTED_ALL_VALID_HASH_JOIN_PROBE_STAGE,
				                              generated_stage_start);
			}
		} else {
			EnsureRegularHashJoinProbeCode(runtime, op.hash_join_probe);
			op.hash_join_probe.function(&native_input);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, SLJIT_GENERATED_REGULAR_HASH_JOIN_PROBE_STAGE,
			                              generated_stage_start);
		}
		state.input_offset = native_input.input_offset;
		state.resume_row_pointer = native_input.resume_row_pointer;
		state.finished = native_input.finished;
		auto residual_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder residual_recorder(runtime, op_idx, op.kind, "residual_predicate");
		auto residual_scratch =
		    op.hash_join_probe.plan.residual_predicate ? &scratch.ExpressionAdapterScratch(op_idx, 0) : nullptr;
		auto selected_count = ApplyNativeHashJoinResidualPredicate(
		    op, probe, input, row_pointers, match_selection, native_input.selected_count, residual_chunk,
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
				RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched",
				                              materialize_stage_start);
				return ExecutionOperatorBindResult::READY;
			}
			output.Reset();
			return ExecutionOperatorBindResult::READY;
		}
		FlatVector::SetSize(row_pointers, count_t(selected_count));
		if (selection_only) {
			output.SetChildCardinality(selected_count);
			return ExecutionOperatorBindResult::READY;
		}
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
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
		switch (op.hash_join_probe.plan.output_mode) {
		case ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD: {
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
			MaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched",
			                              materialize_stage_start);
		} break;
		case ExecutionHashJoinProbeOutputMode::MARK_PROBE: {
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
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
	    SljitHashJoinProbeDrainState &state, string &deferred_reason,
	    bool source_key0_int64_to_int32_unchecked = false, bool selection_only = false) {
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
			                                   build_selection, state);
		case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
			return ExecuteRegularHashJoinProbe(runtime, scratch, op_idx, op, probe, input, output, match_selection,
			                                   row_pointers, source_formats, source_data, source_sel, source_validity,
			                                   residual_chunk, residual_selection, compact_match_selection,
			                                   compact_row_pointers, state, left_probe_output,
			                                   source_key0_int64_to_int32_unchecked, selection_only);
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
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "filtered_perfect_hash_update",
			                              aggregate_stage_start);
		} else {
			ExecuteFilteredPrimitiveAggregateUpdate(op.aggregate_update.filtered_update, aggregates, payload_lanes,
			                                        input, input.size(), payload_scratch);
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "filtered_primitive_update", aggregate_stage_start);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	bool CanExecuteDirectExistingGroupedPrimitiveAggregateUpdate(
	    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes, const SelectionVector *execute_sel,
	    idx_t count) const {
		auto &aggregate_update = op.aggregate_update;
		auto &plan = aggregate_update.plan;
		auto &sink_info = plan.sink_info;
		if (scratch.DirectExistingAggregateUpdateDisabled(op_idx) || execute_sel != nullptr || count != input.size() ||
		    !plan.use_primitive_payloads || !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
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
		auto &aggregate_update = op.aggregate_update;
		auto &plan = aggregate_update.plan;
		auto &sink_info = plan.sink_info;
		if (scratch.DirectNewAggregateUpdateDisabled(op_idx) || execute_sel != nullptr || count != input.size() ||
		    !plan.use_primitive_payloads || !plan.use_grouped_state_addresses || plan.use_perfect_hash_group_lookup ||
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
		RecordSljitRegionStageRuntime(
		    runtime, op_idx, op.kind,
		    resolved ? "direct_new_grouped_state_addresses" : "direct_new_grouped_state_addresses_miss", stage_start);
		return resolved;
	}

	bool TryExecuteDirectExistingGroupedPrimitiveAggregateUpdate(
	    ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
	    SljitExecutableRegionOp &op, DataChunk &input,
	    const vector<const ExecutionPrimitiveAggregateUpdateLane *> &payload_lanes,
	    ExecutionGroupedAggregateStateAddressBinding &grouped_state, bool finish = true) {
		auto stage_start = SljitRegionStageStart(runtime);
		bool updated = false;
		if (runtime.TraceRuntime()) {
			auto stage_name = SljitRegionStageName(op_idx, op.kind, "direct_existing_grouped_primitive_update");
			SljitRegionStageRecorder recorder(runtime, stage_name);
			updated = grouped_state.state->TryUpdateExistingGroups(input, op.aggregate_update.plan.sink_info,
			                                                       payload_lanes, &recorder, finish);
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else {
			updated = grouped_state.state->TryUpdateExistingGroups(input, op.aggregate_update.plan.sink_info,
			                                                       payload_lanes, nullptr, finish);
		}
		scratch.RecordDirectExistingAggregateUpdateResult(op_idx, updated);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind,
		                              updated ? "direct_existing_grouped_primitive_update"
		                                      : "direct_existing_grouped_primitive_update_miss",
		                              stage_start);
		return updated;
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
		update_state.count = input.size();
		update_state.adapter_scratch = &payload_scratch;
		const char *stage_name = existing_groups ? "direct_existing_grouped_fused_payload_update"
		                                         : "direct_new_grouped_fused_payload_update";
		const char *miss_stage_name = existing_groups ? "direct_existing_grouped_fused_payload_update_miss"
		                                              : "direct_new_grouped_fused_payload_update_miss";
		if (runtime.TraceRuntime()) {
			auto trace_stage_name = SljitRegionStageName(op_idx, op.kind, stage_name);
			SljitRegionStageRecorder recorder(runtime, trace_stage_name);
			if (existing_groups) {
				updated = grouped_state.state->TryUpdateExistingGroupsWithStateAddresses(
				    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedStateAddressUpdate, &update_state,
				    &recorder, finish);
			} else {
				updated = grouped_state.state->TryUpdateNewGroupsWithStateAddresses(
				    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedStateAddressUpdate, &update_state,
				    &recorder, finish);
			}
			auto runtime_us = SljitRegionElapsedMicros(stage_start);
			auto unattributed_runtime_us = runtime_us - recorder.RecordedRuntimeTimeUs();
			if (unattributed_runtime_us > 0) {
				runtime.RecordGeneratedStageRuntime(trace_stage_name + ".unattributed", unattributed_runtime_us);
			}
		} else if (existing_groups) {
			updated = grouped_state.state->TryUpdateExistingGroupsWithStateAddresses(
			    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedStateAddressUpdate, &update_state,
			    nullptr, finish);
		} else {
			updated = grouped_state.state->TryUpdateNewGroupsWithStateAddresses(
			    input, op.aggregate_update.plan.sink_info, ExecuteSljitGroupedStateAddressUpdate, &update_state,
			    nullptr, finish);
		}
		if (existing_groups) {
			scratch.RecordDirectExistingAggregateUpdateResult(op_idx, updated);
		} else {
			scratch.RecordDirectNewAggregateUpdateResult(op_idx, updated);
		}
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, updated ? stage_name : miss_stage_name, stage_start);
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
		update_state.count = input.size();
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
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, updated ? stage_name : miss_stage_name, stage_start);
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
		RecordSljitRegionStageRuntime(
		    runtime, op_idx, op.kind,
		    updated ? "direct_new_grouped_primitive_update" : "direct_new_grouped_primitive_update_miss", stage_start);
		return updated;
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
			optional_ptr<Vector> grouped_state_addresses;
			if (op.aggregate_update.plan.use_grouped_state_addresses &&
			    !op.aggregate_update.fused_payload_update_owns_group_lookup) {
				auto &grouped_state = binding.aggregate_update.grouped_state;
				if (!grouped_state.ready || !grouped_state.state) {
					auto blocker = grouped_state.blocker.empty() ? "aggregate-grouped-state-binding-missing"
					                                             : grouped_state.blocker;
					throw InternalException("SLJIT aggregate grouped-state binding failed: %s", blocker.c_str());
				}
				if (CanExecuteDirectExistingGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes,
				                                                            execute_sel, count) &&
				    TryExecuteDirectExistingGroupedPrimitiveAggregateUpdate(
				        runtime, scratch, op_idx, op, input, payload_lanes, grouped_state, !defer_grouped_finish)) {
					if (defer_grouped_finish && deferred_grouped_finish) {
						*deferred_grouped_finish = true;
					}
					return SinkResultType::NEED_MORE_INPUT;
				}
				if (CanExecuteDirectNewGroupedPrimitiveAggregateUpdate(scratch, op_idx, op, input, payload_lanes,
				                                                       execute_sel, count) &&
				    TryExecuteDirectNewGroupedPrimitiveAggregateUpdate(
				        runtime, scratch, op_idx, op, input, payload_lanes, grouped_state, !defer_grouped_finish)) {
					if (defer_grouped_finish && deferred_grouped_finish) {
						*deferred_grouped_finish = true;
					}
					return SinkResultType::NEED_MORE_INPUT;
				}
				if (CanExecuteDirectGroupedFusedPayloadUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel,
				                                              count, true) &&
				    TryExecuteDirectGroupedFusedPayloadUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
				                                              grouped_state, payload_scratch, true,
				                                              !defer_grouped_finish)) {
					if (defer_grouped_finish && deferred_grouped_finish) {
						*deferred_grouped_finish = true;
					}
					return SinkResultType::NEED_MORE_INPUT;
				}
				if (CanExecuteDirectAppendNewGroupedFusedPayloadUpdate(scratch, op_idx, op, input, payload_lanes,
				                                                       execute_sel, count) &&
				    TryExecuteDirectAppendNewGroupedFusedPayloadUpdate(runtime, scratch, op_idx, op, input,
				                                                       payload_lanes, grouped_state, payload_scratch,
				                                                       !defer_grouped_finish)) {
					if (defer_grouped_finish && deferred_grouped_finish) {
						*deferred_grouped_finish = true;
					}
					return SinkResultType::NEED_MORE_INPUT;
				}
				if (CanExecuteDirectGroupedFusedPayloadUpdate(scratch, op_idx, op, input, payload_lanes, execute_sel,
				                                              count, false) &&
				    TryExecuteDirectGroupedFusedPayloadUpdate(runtime, scratch, op_idx, op, input, payload_lanes,
				                                              grouped_state, payload_scratch, false,
				                                              !defer_grouped_finish)) {
					if (defer_grouped_finish && deferred_grouped_finish) {
						*deferred_grouped_finish = true;
					}
					return SinkResultType::NEED_MORE_INPUT;
				}
				grouped_state_addresses = &scratch.AggregateStateAddresses(op_idx);
				if (CanResolveDirectNewGroupedStateAddresses(scratch, op_idx, op, input, execute_sel, count) &&
				    TryResolveDirectNewGroupedStateAddresses(runtime, scratch, op_idx, op, input, grouped_state,
				                                             *grouped_state_addresses, !defer_grouped_finish)) {
					if (defer_grouped_finish && deferred_grouped_finish) {
						*deferred_grouped_finish = true;
					}
				} else if (runtime.TraceRuntime()) {
					auto resolve_stage_name = SljitRegionStageName(op_idx, op.kind, "resolve_grouped_state_addresses");
					auto resolve_stage_start = SljitRegionStageStart(runtime);
					SljitRegionStageRecorder resolve_recorder(runtime, resolve_stage_name);
					grouped_state.state->ResolveStateAddresses(input, *grouped_state_addresses, &resolve_recorder);
					auto resolve_runtime_us = SljitRegionElapsedMicros(resolve_stage_start);
					auto unattributed_runtime_us = resolve_runtime_us - resolve_recorder.RecordedRuntimeTimeUs();
					if (unattributed_runtime_us > 0) {
						runtime.RecordGeneratedStageRuntime(resolve_stage_name + ".unattributed",
						                                    unattributed_runtime_us);
					}
				} else {
					grouped_state.state->ResolveStateAddresses(input, *grouped_state_addresses);
				}
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
				} else if (op.aggregate_update.plan.use_grouped_state_addresses) {
					if (!grouped_state_addresses) {
						throw InternalException("SLJIT fused grouped aggregate update is missing state addresses");
					}
					ExecuteFusedGroupedPrimitiveAggregatePayloadUpdate(
					    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
					    op.aggregate_update.plan.sink_info.aggregate_contract, payload_lanes, input,
					    *grouped_state_addresses, input.size(), payload_scratch);
				} else {
					ExecuteFusedPrimitiveAggregatePayloadUpdate(
					    op.aggregate_update.payloads, op.aggregate_update.fused_payload_update_function, aggregates,
					    payload_lanes, input, execute_sel, count, payload_scratch);
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
			}
			if (op.aggregate_update.plan.use_grouped_state_addresses &&
			    !op.aggregate_update.fused_payload_update_owns_group_lookup) {
				if (defer_grouped_finish) {
					if (deferred_grouped_finish) {
						*deferred_grouped_finish = true;
					}
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
	return make_uniq<SljitNativeRegionKernel>(std::move(backend_name), std::move(region.ops),
	                                          std::move(region.source_distinct_counts),
	                                          std::move(region.source_min_values), std::move(region.source_max_values),
	                                          abi);
}

} // namespace duckdb
