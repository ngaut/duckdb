//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_executor_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_path_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

template <class KERNEL>
static ExecutionOperatorBindResult SljitExecuteNativeHashJoinProbeWithScratch(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
    DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
    bool source_key0_int64_to_int32_unchecked = false, bool selection_only = false) {
	return SljitExecuteNativeHashJoinProbe(
	    kernel, runtime, native_runtime, scratch, hash_join_idx, hash_join_op, input, output,
	    scratch.FilterSelection(hash_join_idx), scratch.HashJoinBuildSelection(hash_join_idx),
	    scratch.HashJoinRowPointers(hash_join_idx), scratch.HashJoinSources(hash_join_idx),
	    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualChunk(hash_join_idx) : nullptr,
	    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualSelection(hash_join_idx)
	                                                         : nullptr,
	    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualMatchSelection(hash_join_idx)
	                                                         : nullptr,
	    hash_join_op.hash_join_probe.plan.residual_predicate ? &scratch.HashJoinResidualRowPointers(hash_join_idx)
	                                                         : nullptr,
	    state, deferred_reason, source_key0_int64_to_int32_unchecked, selection_only);
}

template <class KERNEL>
static ExecutionOperatorBindResult SljitExecuteRecordedNativeHashJoinProbeWithScratch(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
    DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
    bool source_key0_int64_to_int32_unchecked = false, bool selection_only = false) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto bind_result = SljitExecuteNativeHashJoinProbeWithScratch(
	    kernel, runtime, native_runtime, scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
	    source_key0_int64_to_int32_unchecked, selection_only);
	RecordSljitRegionStageRuntime(runtime, hash_join_idx, hash_join_op.kind, stage_start);
	return bind_result;
}

template <class OWNER>
struct SljitRecordedHashJoinProbeCallback {
	OWNER &owner;
	ExecutionRegionRuntime &runtime;
	ExecutionOperatorRuntime &native_runtime;
	optional_ptr<SljitRegionExecutionScratch> fixed_scratch;

	ExecutionOperatorBindResult Execute(SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                    SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
	                                    SljitHashJoinProbeDrainState &state, string &deferred_reason,
	                                    bool source_key0_int64_to_int32_unchecked, bool selection_only) {
		return SljitExecuteRecordedNativeHashJoinProbeWithScratch(
		    owner, runtime, native_runtime, scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		    source_key0_int64_to_int32_unchecked, selection_only);
	}

	ExecutionOperatorBindResult operator()(SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                       SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
	                                       SljitHashJoinProbeDrainState &state, string &deferred_reason,
	                                       bool source_key0_int64_to_int32_unchecked, bool selection_only) {
		return Execute(scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		               source_key0_int64_to_int32_unchecked, selection_only);
	}

	ExecutionOperatorBindResult operator()(idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
	                                       DataChunk &output, SljitHashJoinProbeDrainState &state,
	                                       string &deferred_reason, bool source_key0_int64_to_int32_unchecked,
	                                       bool selection_only) {
		if (!fixed_scratch) {
			throw InternalException("SLJIT recorded hash join probe callback requires fixed scratch");
		}
		return Execute(*fixed_scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		               source_key0_int64_to_int32_unchecked, selection_only);
	}
};

template <class OWNER>
static SljitRecordedHashJoinProbeCallback<OWNER>
SljitMakeRecordedHashJoinProbeCallback(OWNER &owner, ExecutionRegionRuntime &runtime,
                                       ExecutionOperatorRuntime &native_runtime) {
	return {owner, runtime, native_runtime, nullptr};
}

template <class OWNER>
static SljitRecordedHashJoinProbeCallback<OWNER>
SljitMakeFixedScratchRecordedHashJoinProbeCallback(OWNER &owner, ExecutionRegionRuntime &runtime,
                                                   ExecutionOperatorRuntime &native_runtime,
                                                   SljitRegionExecutionScratch &scratch) {
	return {owner, runtime, native_runtime, optional_ptr<SljitRegionExecutionScratch>(&scratch)};
}

template <class OWNER>
static ExecutionOperatorBindResult SljitExecutePerfectHashJoinProbe(
    OWNER &owner, ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
    const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output, SelectionVector &match_selection,
    SelectionVector &build_selection, SljitHashJoinProbeDrainState &state, bool selection_only = false) {
	owner.EnsurePerfectHashJoinProbeCode(runtime, op.hash_join_probe);
	auto &key = SljitValidatePerfectHashJoinProbeExecutionLayout(op.hash_join_probe.plan, probe, input);
	SljitPreparedPerfectHashJoinProbeInput prepared_input;
	SljitPreparePerfectHashJoinProbeInput(key, probe.perfect_layout, input, match_selection, build_selection, state,
	                                      prepared_input);
	auto &native_input = prepared_input.native_input;

	auto generated_stage_start = SljitRegionStageStart(runtime);
	op.hash_join_probe.perfect.function(&native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, SljitGeneratedPerfectHashJoinProbeStage(),
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

template <class OWNER>
static ExecutionOperatorBindResult SljitExecuteRegularHashJoinProbe(
    OWNER &owner, ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
    SljitExecutableRegionOp &op, const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output,
    SelectionVector &match_selection, Vector &row_pointers, SljitHashJoinProbeSourceScratch &source_scratch,
    DataChunk *residual_chunk, SelectionVector *residual_selection, SelectionVector *compact_match_selection,
    Vector *compact_row_pointers, SljitHashJoinProbeDrainState &state, bool left_probe_output,
    bool source_key0_int64_to_int32_unchecked = false, bool selection_only = false) {
	auto &layout = probe.table_layout;
	const auto table_layout_kind = SljitValidateRegularHashJoinProbeExecutionLayout(op.hash_join_probe.plan, probe);

	auto prepared_input = SljitPrepareRegularHashJoinProbeInput(
	    runtime, op_idx, op.kind, op.hash_join_probe.plan, layout, input, match_selection, row_pointers, source_scratch,
	    state, table_layout_kind, source_key0_int64_to_int32_unchecked);
	auto &native_input = prepared_input.native_input;

	auto generated_stage_start = SljitRegionStageStart(runtime);
	const auto executed_probe_stage = SljitExecuteRegularHashJoinProbePath(
	    owner, runtime, prepared_input.input_kind, op.hash_join_probe, layout.needs_chain_matcher, native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, executed_probe_stage, generated_stage_start);
	state.input_offset = native_input.input_offset;
	state.resume_row_pointer = native_input.resume_row_pointer;
	state.finished = native_input.finished;
	auto selected_count = native_input.selected_count;
	if (op.hash_join_probe.plan.residual_predicate) {
		auto residual_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder residual_recorder(runtime, op_idx, op.kind, "residual_predicate");
		auto &residual_scratch = scratch.ExpressionAdapterScratch(op_idx, 0);
		selected_count = SljitApplyNativeHashJoinResidualPredicate(
		    runtime, op, probe, input, row_pointers, match_selection, selected_count, residual_chunk,
		    residual_selection, compact_match_selection, compact_row_pointers, &residual_scratch, &residual_recorder);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "residual_predicate", residual_stage_start);
	}
	if (left_probe_output && selected_count != 0) {
		auto mark_stage_start = SljitRegionStageStart(runtime);
		SljitMarkLeftHashJoinProbeMatches(state, match_selection, selected_count);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "mark_left_matches", mark_stage_start);
	}
	if (op.hash_join_probe.plan.mark_build_match_after_residual) {
		auto mark_build_stage_start = SljitRegionStageStart(runtime);
		SljitMarkHashJoinBuildMatchesAfterResidual(op.hash_join_probe.plan, row_pointers, selected_count);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "mark_build_matches", mark_build_stage_start);
	}
	if (op.hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
		output.Reset();
		return ExecutionOperatorBindResult::READY;
	}
	if (selected_count == 0) {
		if (left_probe_output && state.finished) {
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
			SljitMaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output_left_unmatched", output.size());
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
	ExecutionMaterializeHashJoinProbe(probe, input, row_pointers, match_selection, selected_count, output, &recorder);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_output", materialize_stage_start);
	return ExecutionOperatorBindResult::READY;
}

static ExecutionOperatorBindResult
SljitExecuteEmptyHashJoinProbe(ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
                               const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output,
                               SelectionVector &match_selection, Vector &row_pointers,
                               SljitHashJoinProbeDrainState &state) {
	state.finished = true;
	RecordSljitRegionRuntimePath(runtime, op.kind, "empty_build_side");
	switch (op.hash_join_probe.plan.output_mode) {
	case ExecutionHashJoinProbeOutputMode::LEFT_PROBE_AND_BUILD: {
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
		SljitMaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output_left_unmatched", output.size());
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched", materialize_stage_start);
	} break;
	case ExecutionHashJoinProbeOutputMode::MARK_PROBE: {
		auto materialize_stage_start = SljitRegionStageStart(runtime);
		SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output", input.size());
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

template <class OWNER>
static ExecutionOperatorBindResult SljitExecuteNativeHashJoinProbe(
    OWNER &owner, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    DataChunk &output, SelectionVector &match_selection, SelectionVector &build_selection, Vector &row_pointers,
    SljitHashJoinProbeSourceScratch &source_scratch, DataChunk *residual_chunk, SelectionVector *residual_selection,
    SelectionVector *compact_match_selection, Vector *compact_row_pointers, SljitHashJoinProbeDrainState &state,
    string &deferred_reason, bool source_key0_int64_to_int32_unchecked = false, bool selection_only = false) {
	ExecutionOperatorBinding *binding_ptr = nullptr;
	auto bind_result = SljitBindRecordedNativeOperator(
	    runtime, native_runtime, scratch, op_idx, op, input, op.hash_join_probe.plan.operator_info,
	    "native-operator-runtime-deferred", "SLJIT native hash join probe", binding_ptr, deferred_reason);
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
	const bool left_probe_output = SljitIsLeftHashJoinProbeOutputMode(op.hash_join_probe.plan.output_mode);
	if (left_probe_output) {
		SljitInitializeLeftHashJoinProbeState(state, input.size());
		if (state.finished && !state.left_unmatched_emitted) {
			auto materialize_stage_start = SljitRegionStageStart(runtime);
			SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
			SljitMaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output_left_unmatched", output.size());
			RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched",
			                              materialize_stage_start);
			return ExecutionOperatorBindResult::READY;
		}
	}
	if (probe.probe_key_input_indices.size() != op.hash_join_probe.plan.keys.size()) {
		throw InternalException("SLJIT native hash join probe key binding count mismatch");
	}
	if (probe.empty_build_side) {
		return SljitExecuteEmptyHashJoinProbe(runtime, op_idx, op, probe, input, output, match_selection, row_pointers,
		                                      state);
	}
	runtime.RecordHashJoinProbeLayout(SljitHashJoinProbeLayoutName(probe.layout_kind));
	switch (probe.layout_kind) {
	case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
		return SljitExecutePerfectHashJoinProbe(owner, runtime, op_idx, op, probe, input, output, match_selection,
		                                        build_selection, state, selection_only);
	case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
		return SljitExecuteRegularHashJoinProbe(
		    owner, runtime, scratch, op_idx, op, probe, input, output, match_selection, row_pointers, source_scratch,
		    residual_chunk, residual_selection, compact_match_selection, compact_row_pointers, state, left_probe_output,
		    source_key0_int64_to_int32_unchecked, selection_only);
	default:
		throw InternalException("SLJIT native hash join probe received an unknown layout kind");
	}
}

} // namespace duckdb
