//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_route_kind.hpp"
#include "sljit_region_runtime_state.hpp"

#include "duckdb/execution/execution_hash_join_runtime.hpp"
#include "duckdb/execution/join_hashtable.hpp"

namespace duckdb {

static bool SljitIsLeftHashJoinProbeOutputMode(ExecutionHashJoinProbeOutputMode output_mode) {
	return output_mode == ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD;
}

static bool SljitHashJoinProbeDrainFinished(ExecutionHashJoinProbeOutputMode output_mode,
                                            const SljitHashJoinProbeDrainState &state) {
	return state.finished && (!SljitIsLeftHashJoinProbeOutputMode(output_mode) || state.left_unmatched_emitted);
}

static void SljitInitializeLeftHashJoinProbeState(SljitHashJoinProbeDrainState &state, idx_t input_count) {
	if (state.left_initialized) {
		return;
	}
	state.found_match.assign(input_count, 0);
	state.left_initialized = true;
	state.left_unmatched_emitted = false;
}

static void SljitMarkLeftHashJoinProbeMatches(SljitHashJoinProbeDrainState &state,
                                              const SelectionVector &match_selection, idx_t count) {
	for (idx_t match_idx = 0; match_idx < count; match_idx++) {
		auto input_idx = match_selection.get_index(match_idx);
		if (input_idx >= state.found_match.size()) {
			throw InternalException("SLJIT native LEFT hash join match selection index out of range");
		}
		state.found_match[input_idx] = 1;
	}
}

static void SljitMaterializeLeftHashJoinProbeUnmatched(const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
                                                       DataChunk &output, SelectionVector &unmatched_selection,
                                                       SljitHashJoinProbeDrainState &state,
                                                       optional_ptr<ExecutionOperatorStageRecorder> recorder) {
	SljitInitializeLeftHashJoinProbeState(state, input.size());
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

static idx_t SljitSelectMarkProbeMatches(const SelectionVector &mark_flags, idx_t count, SelectionVector &selection) {
	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (mark_flags.get_index(row_idx) != 0) {
			selection.set_index(selected_count++, row_idx);
		}
	}
	return selected_count;
}

static idx_t SljitSelectMarkProbeNonMatches(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                            const SelectionVector &mark_flags, idx_t count,
                                            SelectionVector &selection) {
	if (!binding.ready || !binding.hash_table) {
		throw InternalException("SLJIT MARK probe non-match selection requires a bound hash join table");
	}
	if (count > input.size()) {
		throw InternalException("SLJIT MARK probe non-match selection count exceeds input size");
	}
	if (binding.hash_table->has_null) {
		return 0;
	}

	vector<UnifiedVectorFormat> nullable_key_formats;
	for (idx_t key_idx = 0; key_idx < binding.probe_key_input_indices.size(); key_idx++) {
		if (binding.hash_table->NullValuesAreEqual(key_idx)) {
			continue;
		}
		auto input_col = binding.probe_key_input_indices[key_idx];
		if (input_col >= input.ColumnCount()) {
			throw InternalException("SLJIT MARK probe key column index out of range");
		}
		UnifiedVectorFormat key_format;
		input.data[input_col].ToUnifiedFormat(key_format);
		if (key_format.validity.CanHaveNull()) {
			nullable_key_formats.push_back(std::move(key_format));
		}
	}

	idx_t selected_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (mark_flags.get_index(row_idx) != 0) {
			continue;
		}
		bool key_has_null = false;
		for (auto &key_format : nullable_key_formats) {
			auto key_row = key_format.sel->get_index(row_idx);
			if (!key_format.validity.RowIsValidUnsafe(key_row)) {
				key_has_null = true;
				break;
			}
		}
		if (!key_has_null) {
			selection.set_index(selected_count++, row_idx);
		}
	}
	return selected_count;
}

static idx_t SljitSelectMarkProbeFilterRows(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                            const SelectionVector &mark_flags, idx_t count,
                                            SljitMarkProbeFilterMode mode, SelectionVector &selection) {
	switch (mode) {
	case SljitMarkProbeFilterMode::MATCHES:
		return SljitSelectMarkProbeMatches(mark_flags, count, selection);
	case SljitMarkProbeFilterMode::NON_MATCHES:
		return SljitSelectMarkProbeNonMatches(binding, input, mark_flags, count, selection);
	default:
		throw InternalException("SLJIT MARK probe filter has no direct selection mode");
	}
}

static const char *SljitMarkProbeFilterSelectionPath(SljitMarkProbeFilterMode mode) {
	switch (mode) {
	case SljitMarkProbeFilterMode::MATCHES:
		return "direct_mark_selection";
	case SljitMarkProbeFilterMode::NON_MATCHES:
		return "direct_mark_nonmatch_selection";
	default:
		return "direct_mark_unknown_selection";
	}
}

static bool SljitTryReferenceMarkProbeFilterInput(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
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

static bool SljitTryBuildMarkProbeFilterInput(const ExecutionHashJoinProbeBinding &binding, DataChunk &input,
                                              const SelectionVector &mark_flags, idx_t count, DataChunk &output) {
	if (!binding.ready || !binding.hash_table || binding.output_mode != ExecutionHashJoinProbeOutputMode::MARK_PROBE ||
	    binding.correlated_mark_counts_required ||
	    output.ColumnCount() != binding.lhs_output_column_indices.size() + 1 || count > input.size()) {
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
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(mark_vector, count_t(count));
	auto bool_result = FlatVector::GetDataMutable<bool>(mark_vector);
	auto &mask = FlatVector::ValidityMutable(mark_vector);
	mask.SetAllValid(count);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		bool_result[row_idx] = mark_flags.get_index(row_idx) != 0;
	}

	for (idx_t key_idx = 0; key_idx < binding.probe_key_input_indices.size(); key_idx++) {
		if (binding.hash_table->NullValuesAreEqual(key_idx)) {
			continue;
		}
		auto input_col = binding.probe_key_input_indices[key_idx];
		if (input_col >= input.ColumnCount()) {
			throw InternalException("SLJIT MARK probe key column index out of range");
		}
		UnifiedVectorFormat key_data;
		input.data[input_col].ToUnifiedFormat(key_data);
		if (!key_data.validity.CanHaveNull()) {
			continue;
		}
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto key_row = key_data.sel->get_index(row_idx);
			if (!key_data.validity.RowIsValidUnsafe(key_row)) {
				mask.SetInvalid(row_idx);
			}
		}
	}
	if (binding.hash_table->has_null) {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (!bool_result[row_idx]) {
				mask.SetInvalid(row_idx);
			}
		}
	}
	output.SetChildCardinality(count);
	return true;
}

} // namespace duckdb
