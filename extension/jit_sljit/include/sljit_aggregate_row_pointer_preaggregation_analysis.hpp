//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_row_pointer_preaggregation_analysis.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_row_pointer_preaggregation_groups.hpp"

#include <array>

namespace duckdb {

enum class SljitRowPointerPreaggregationStrategy : uint8_t { DISABLED, CONSECUTIVE_GROUPS, LOCAL_DISTINCT_GROUPS };

static constexpr idx_t SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT = 64;
static constexpr idx_t SLJIT_ROW_POINTER_LOCAL_PREAGGREGATION_MAX_GROUPS = 128;

struct SljitRowPointerPreaggregationSampleStats {
	idx_t sample_count = 0;
	idx_t sample_group_count = 0;
	idx_t sample_unique_group_count = 0;

	bool HasConsecutiveRepeat() const {
		return sample_group_count < sample_count;
	}

	bool HasInterleavedRepeat() const {
		return sample_unique_group_count < sample_group_count;
	}

	bool LocalDistinctBeatsConsecutive(idx_t min_reduction = 2) const {
		return HasInterleavedRepeat() && sample_unique_group_count * min_reduction <= sample_group_count;
	}
};

struct SljitRowPointerPreaggregationDecision {
	SljitRowPointerPreaggregationStrategy strategy = SljitRowPointerPreaggregationStrategy::DISABLED;
	SljitRowPointerPreaggregationSampleStats sample;
	bool has_variable_width_groups = false;

	bool UseConsecutiveGroups() const {
		return strategy == SljitRowPointerPreaggregationStrategy::CONSECUTIVE_GROUPS;
	}

	bool UseLocalDistinctGroups() const {
		return strategy == SljitRowPointerPreaggregationStrategy::LOCAL_DISTINCT_GROUPS;
	}
};

static bool SljitTryCollectRowPointerIdentityPreaggregationSampleStats(Vector &row_pointers, idx_t count,
                                                                       SljitRowPointerPreaggregationSampleStats &stats);

static bool
SljitTryCollectRowPointerPreaggregationSampleStats(DataChunk &payload_input, Vector &row_pointers, idx_t count,
                                                   const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                                   vector<UnifiedVectorFormat> &input_group_formats,
                                                   SljitRowPointerPreaggregationSampleStats &stats) {
	stats = SljitRowPointerPreaggregationSampleStats();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	vector<ExecutionRowPointerGroupKeySource> row_pointer_group_sources;
	if (!SljitTryPrepareRowPointerGroupSources(group_sources, row_pointer_group_sources)) {
		return false;
	}
	if (SljitRowPointerPreaggregationSourcesRepeatWithRowPointer(group_sources) &&
	    SljitSameRowPointerIsEqual(row_pointer_group_sources)) {
		return SljitTryCollectRowPointerIdentityPreaggregationSampleStats(row_pointers, count, stats);
	}
	const bool uses_input_vector_groups = SljitRowPointerPreaggregationUsesInputVectorGroups(group_sources);
	if (uses_input_vector_groups &&
	    !SljitTryPrepareInputVectorGroupFormats(payload_input, group_sources, input_group_formats)) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	stats.sample_count = MinValue<idx_t>(count, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT);
	const bool same_row_pointer_is_equal = SljitSameRowPointerIsEqual(row_pointer_group_sources);
	data_ptr_t active_row_pointer = nullptr;
	idx_t active_row_idx = DConstants::INVALID_INDEX;
	bool has_active_row_pointer = false;
	std::array<data_ptr_t, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT> unique_row_pointers;
	std::array<idx_t, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT> unique_row_indices;
	for (idx_t row_idx = 0; row_idx < stats.sample_count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			return false;
		}
		bool same_group = false;
		if (has_active_row_pointer) {
			same_group = SljitRowPointerPreaggregationRowsEqual(
			    active_row_pointer, active_row_idx, row_pointer, row_idx, uses_input_vector_groups,
			    same_row_pointer_is_equal, group_sources, row_pointer_group_sources, input_group_formats);
		}
		if (!same_group) {
			active_row_pointer = row_pointer;
			active_row_idx = row_idx;
			has_active_row_pointer = true;
			stats.sample_group_count++;
		}
		bool found_unique_group = false;
		for (idx_t unique_idx = 0; unique_idx < stats.sample_unique_group_count; unique_idx++) {
			if (SljitRowPointerPreaggregationRowsEqual(unique_row_pointers[unique_idx], unique_row_indices[unique_idx],
			                                           row_pointer, row_idx, uses_input_vector_groups,
			                                           same_row_pointer_is_equal, group_sources,
			                                           row_pointer_group_sources, input_group_formats)) {
				found_unique_group = true;
				break;
			}
		}
		if (!found_unique_group) {
			unique_row_pointers[stats.sample_unique_group_count] = row_pointer;
			unique_row_indices[stats.sample_unique_group_count] = row_idx;
			stats.sample_unique_group_count++;
		}
	}
	return stats.HasConsecutiveRepeat() || stats.HasInterleavedRepeat();
}

static bool
SljitTryCollectRowPointerIdentityPreaggregationSampleStats(Vector &row_pointers, idx_t count,
                                                           SljitRowPointerPreaggregationSampleStats &stats) {
	stats = SljitRowPointerPreaggregationSampleStats();
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	stats.sample_count = MinValue<idx_t>(count, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT);
	data_ptr_t active_row_pointer = nullptr;
	bool has_active_row_pointer = false;
	std::array<data_ptr_t, SLJIT_ROW_POINTER_PREAGGREGATION_SAMPLE_COUNT> unique_row_pointers;
	for (idx_t row_idx = 0; row_idx < stats.sample_count; row_idx++) {
		auto row_pointer = row_pointer_data[row_idx];
		if (!row_pointer) {
			return false;
		}
		if (!has_active_row_pointer || active_row_pointer != row_pointer) {
			active_row_pointer = row_pointer;
			has_active_row_pointer = true;
			stats.sample_group_count++;
		}
		bool found_unique_group = false;
		for (idx_t unique_idx = 0; unique_idx < stats.sample_unique_group_count; unique_idx++) {
			if (unique_row_pointers[unique_idx] == row_pointer) {
				found_unique_group = true;
				break;
			}
		}
		if (!found_unique_group) {
			unique_row_pointers[stats.sample_unique_group_count] = row_pointer;
			stats.sample_unique_group_count++;
		}
	}
	return stats.HasConsecutiveRepeat() || stats.HasInterleavedRepeat();
}

static SljitRowPointerPreaggregationDecision
SljitChooseRowPointerPreaggregationStrategy(DataChunk &payload_input, Vector &row_pointers, idx_t count,
                                            const vector<ExecutionRowPointerGroupKeySource> &group_sources,
                                            vector<UnifiedVectorFormat> &input_group_formats) {
	SljitRowPointerPreaggregationDecision decision;
	decision.has_variable_width_groups = SljitRowPointerPreaggregationHasVariableWidthGroups(group_sources);
	if (!SljitTryCollectRowPointerPreaggregationSampleStats(payload_input, row_pointers, count, group_sources,
	                                                        input_group_formats, decision.sample)) {
		return decision;
	}
	if (!decision.has_variable_width_groups) {
		if (decision.sample.LocalDistinctBeatsConsecutive()) {
			decision.strategy = SljitRowPointerPreaggregationStrategy::LOCAL_DISTINCT_GROUPS;
		} else if (decision.sample.HasConsecutiveRepeat()) {
			decision.strategy = SljitRowPointerPreaggregationStrategy::CONSECUTIVE_GROUPS;
		}
		return decision;
	}
	constexpr idx_t VARIABLE_WIDTH_PREAGGREGATION_MIN_REDUCTION = 4;
	if (decision.sample.LocalDistinctBeatsConsecutive(VARIABLE_WIDTH_PREAGGREGATION_MIN_REDUCTION)) {
		decision.strategy = SljitRowPointerPreaggregationStrategy::LOCAL_DISTINCT_GROUPS;
	} else if (decision.sample.HasConsecutiveRepeat() &&
	           decision.sample.sample_group_count * VARIABLE_WIDTH_PREAGGREGATION_MIN_REDUCTION <=
	               decision.sample.sample_count) {
		decision.strategy = SljitRowPointerPreaggregationStrategy::CONSECUTIVE_GROUPS;
	}
	return decision;
}

static bool
SljitShouldPreferDirectSparseRowPointerTargetUpdate(Vector &row_pointers, idx_t count,
                                                    const vector<ExecutionRowPointerGroupKeySource> &group_sources) {
	if (count < 2 || row_pointers.GetVectorType() != VectorType::FLAT_VECTOR || group_sources.size() != 1) {
		return false;
	}
	auto &source = group_sources[0];
	if (source.source_kind != ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD || !source.ready ||
	    !SljitSparseRowPointerTargetCacheTypeSupported(source.target_physical_type)) {
		return false;
	}
	auto row_pointer_data = FlatVector::GetData<data_ptr_t>(row_pointers);
	const auto sample_count = MinValue<idx_t>(count, 64);
	for (idx_t row_idx = 1; row_idx < sample_count; row_idx++) {
		for (idx_t prev_idx = 0; prev_idx < row_idx; prev_idx++) {
			if (ExecutionRowPointerGroupKeysEqual(row_pointer_data[prev_idx], row_pointer_data[row_idx],
			                                      group_sources)) {
				return true;
			}
		}
	}
	return false;
}

} // namespace duckdb
