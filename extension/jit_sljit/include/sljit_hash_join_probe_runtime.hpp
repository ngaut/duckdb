//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_runtime_state.hpp"

#include "duckdb/execution/execution_hash_join_runtime.hpp"

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

} // namespace duckdb
