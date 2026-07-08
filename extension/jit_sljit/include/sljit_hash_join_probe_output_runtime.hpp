//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_output_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_execution_contract.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

static ExecutionOperatorBindResult SljitMaterializeLeftHashJoinProbeUnmatchedOutput(
    ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
    const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output, SelectionVector &match_selection,
    SljitHashJoinProbeDrainState &state) {
	auto materialize_stage_start = SljitRegionStageStart(runtime);
	SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
	SljitMaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched", materialize_stage_start);
	return ExecutionOperatorBindResult::READY;
}

static const char *SljitHashJoinProbeSelectedViewBoundaryName(bool mark_probe,
                                                              SljitHashJoinMarkSelectionMode mark_selection_mode) {
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES) {
		return "mark_match_selection_reference";
	}
	if (mark_selection_mode == SljitHashJoinMarkSelectionMode::NON_MATCHES) {
		return "mark_nonmatch_selection_reference";
	}
	return mark_probe ? "mark_flags" : "selection_reference.regular";
}

static idx_t SljitSelectHashJoinProbeNonNullKeys(const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
                                                 SelectionVector &selection) {
	if (!probe.ready || !probe.hash_table) {
		throw InternalException("SLJIT MARK probe non-match selection requires a bound hash join table");
	}
	vector<UnifiedVectorFormat> nullable_key_formats;
	for (idx_t key_idx = 0; key_idx < probe.probe_key_input_indices.size(); key_idx++) {
		if (probe.hash_table->NullValuesAreEqual(key_idx)) {
			continue;
		}
		const auto input_col = probe.probe_key_input_indices[key_idx];
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
	for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
		bool key_has_null = false;
		for (auto &key_format : nullable_key_formats) {
			const auto key_row = key_format.sel->get_index(row_idx);
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

static ExecutionOperatorBindResult SljitExecuteMarkProbeNoTrueNonMatches(ExecutionRegionRuntime &runtime,
                                                                         SljitExecutableRegionOp &op, DataChunk &output,
                                                                         SljitHashJoinProbeDrainState &state,
                                                                         idx_t input_count) {
	state.input_offset = input_count;
	state.resume_row_pointer = nullptr;
	state.finished = true;
	state.source_key0_int64_to_int32_matches_are_proven = false;
	RecordSljitRegionRuntimePath(runtime, op.kind, "mark_nonmatch_empty_due_to_build_null");
	RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::NO_WORK,
	                              "mark_nonmatch_empty_due_to_build_null");
	output.Reset();
	return ExecutionOperatorBindResult::READY;
}

static ExecutionOperatorBindResult SljitExecuteEmptyHashJoinProbe(
    ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
    const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output, SelectionVector &match_selection,
	Vector &row_pointers, SljitHashJoinProbeDrainState &state,
	SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT) {
	state.finished = true;
	RecordSljitRegionRuntimePath(runtime, op.kind, "empty_build_side");
	RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::NO_WORK, "empty_build_side");
	switch (op.hash_join_probe.plan.output_mode) {
	case ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD:
		return SljitMaterializeLeftHashJoinProbeUnmatchedOutput(runtime, op_idx, op, probe, input, output,
		                                                        match_selection, state);
	case ExecutionHashJoinProbeOutputMode::MARK_PROBE: {
		if (SljitHashJoinProbeOutputIsFilteredMarkNonMatches(output_contract)) {
			const auto selected_count = SljitSelectHashJoinProbeNonNullKeys(probe, input, match_selection);
			if (selected_count == 0) {
				output.Reset();
				break;
			}
			output.SetChildCardinality(selected_count);
			break;
		}
		if (SljitHashJoinProbeOutputIsFilteredMarkMatches(output_contract)) {
			output.Reset();
			break;
		}
		if (SljitHashJoinProbeProducesSelectedView(output_contract)) {
			for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
				match_selection.set_index(row_idx, 0);
			}
			output.SetChildCardinality(input.size());
			break;
		}
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
		ExecutionMaterializeHashJoinProbe(probe, input, row_pointers, match_selection, input.size(), output, &recorder);
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

} // namespace duckdb
