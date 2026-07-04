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

} // namespace duckdb
