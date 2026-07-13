//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_executor_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_exact_perfect_hash_join_runtime.hpp"
#include "sljit_hash_join_probe_execution_contract.hpp"
#include "sljit_hash_join_probe_output_runtime.hpp"
#include "sljit_hash_join_probe_primitive.hpp"
#include "sljit_hash_join_probe_path_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_native_function_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static void SljitPopulateExactPerfectHashJoinSelections(const SljitNativeHashJoinProbeKeyPlan &key,
                                                        SljitNativePerfectHashJoinProbeInput &input) {
	if (input.source_key0_int64_to_int32) {
		SljitPopulateExactPerfectHashJoinSelections<int32_t, int64_t>(input);
		return;
	}
	switch (key.key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return SljitPopulateExactPerfectHashJoinSelections<int8_t>(input);
	case SljitNativeHashJoinKeyKind::INT16:
		return SljitPopulateExactPerfectHashJoinSelections<int16_t>(input);
	case SljitNativeHashJoinKeyKind::INT32:
		return SljitPopulateExactPerfectHashJoinSelections<int32_t>(input);
	case SljitNativeHashJoinKeyKind::INT64:
		return SljitPopulateExactPerfectHashJoinSelections<int64_t>(input);
	case SljitNativeHashJoinKeyKind::UINT8:
		return SljitPopulateExactPerfectHashJoinSelections<uint8_t>(input);
	case SljitNativeHashJoinKeyKind::UINT16:
		return SljitPopulateExactPerfectHashJoinSelections<uint16_t>(input);
	case SljitNativeHashJoinKeyKind::UINT32:
		return SljitPopulateExactPerfectHashJoinSelections<uint32_t>(input);
	case SljitNativeHashJoinKeyKind::UINT64:
		return SljitPopulateExactPerfectHashJoinSelections<uint64_t>(input);
	default:
		throw InternalException("exact perfect hash join filter proof has an unsupported key width");
	}
}

template <class OWNER>
static ExecutionOperatorBindResult SljitExecutePerfectHashJoinProbe(
    OWNER &owner, ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
    const SljitNativeHashJoinProbePlan &plan, const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
    DataChunk &output, SelectionVector &match_selection, SelectionVector &build_selection,
    SljitHashJoinProbeDrainState &state, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT) {
	if (plan.exact_source_filter_identity) {
		runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.exact_source_filter_candidate");
	}
	if (plan.exact_source_filter_identity &&
	    plan.exact_source_filter_identity == probe.perfect_layout.runtime_filter_identity) {
		auto &key = SljitValidatePerfectHashJoinProbeExecutionLayout(plan, probe, input);
		SljitPreparedPerfectHashJoinProbeInput prepared_input;
		SljitPreparePerfectHashJoinProbeInput(key, probe.perfect_layout, input, match_selection, build_selection, state,
		                                      source_key0_int64_to_int32_unchecked, prepared_input);
		auto &native_input = prepared_input.native_input;
		SljitPopulateExactPerfectHashJoinSelections(key, native_input);
		state.input_offset = native_input.input_offset;
		state.resume_row_pointer = nullptr;
		state.finished = native_input.finished;
		state.output_proof.source_key0_int64_to_int32 = native_input.source_key0_int64_to_int32;
		runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.exact_source_filter", native_input.selected_count);
		RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK,
		                              "exact_source_filter", native_input.selected_count);
		if (SljitHashJoinProbeProducesSelectedView(output_contract)) {
			output.SetChildCardinality(native_input.selected_count);
			return ExecutionOperatorBindResult::READY;
		}
		ExecutionMaterializePerfectHashJoinProbe(probe, input, match_selection, build_selection,
		                                         native_input.selected_count, output, nullptr);
		return ExecutionOperatorBindResult::READY;
	}
	auto function = owner.EnsurePerfectHashJoinProbeCode(runtime, op.hash_join_probe);
	auto &key = SljitValidatePerfectHashJoinProbeExecutionLayout(plan, probe, input);
	SljitPreparedPerfectHashJoinProbeInput prepared_input;
	SljitPreparePerfectHashJoinProbeInput(key, probe.perfect_layout, input, match_selection, build_selection, state,
	                                      source_key0_int64_to_int32_unchecked, prepared_input);
	auto &native_input = prepared_input.native_input;

	auto generated_stage_start = SljitRegionStageStart(runtime);
	SljitExecuteNativeFunction(function, native_input);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, SljitGeneratedPerfectHashJoinProbeStage(),
	                                  generated_stage_start);
	state.input_offset = native_input.input_offset;
	state.resume_row_pointer = nullptr;
	state.finished = native_input.finished;
	state.output_proof.source_key0_int64_to_int32 =
	    native_input.selected_count != 0 && native_input.source_key0_int64_to_int32;
	if (native_input.selected_count == 0) {
		output.Reset();
		return ExecutionOperatorBindResult::READY;
	}
	if (SljitHashJoinProbeProducesSelectedView(output_contract)) {
		output.SetChildCardinality(native_input.selected_count);
		return ExecutionOperatorBindResult::READY;
	}
	auto materialize_stage_start = SljitRegionStageStart(runtime);
	SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
	ExecutionMaterializePerfectHashJoinProbe(probe, input, match_selection, build_selection,
	                                         native_input.selected_count, output, &recorder);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_output", materialize_stage_start);
	return ExecutionOperatorBindResult::READY;
}

template <class OWNER>
static ExecutionOperatorBindResult SljitExecuteRegularHashJoinProbe(
    OWNER &owner, ExecutionRegionRuntime &runtime, SljitRegionExecutionScratch &scratch, idx_t op_idx,
    SljitExecutableRegionOp &op, const SljitNativeHashJoinProbePlan &plan, const ExecutionHashJoinProbeBinding &probe,
    DataChunk &input, DataChunk &output, SelectionVector &match_selection, Vector &row_pointers,
    SljitHashJoinProbeSourceScratch &source_scratch, DataChunk *residual_chunk, SelectionVector *residual_selection,
    SelectionVector *compact_match_selection, Vector *compact_row_pointers, SljitHashJoinProbeDrainState &state,
    bool left_probe_output, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT) {
	auto &layout = probe.table_layout;
	const auto table_layout_kind = SljitValidateRegularHashJoinProbeExecutionLayout(plan, probe);
	if (plan.exact_source_filter_identity) {
		runtime.RecordJitRuntimePath("hash_join_probe.regular_probe.exact_source_filter_candidate");
	}
	const bool exact_source_filter =
	    plan.exact_source_filter_identity &&
	    plan.exact_source_filter_identity == probe.table_layout.runtime_filter_identity &&
	    probe.table_layout.exact_filter_build_keys_unique && plan.keys.size() == 1 && plan.equality_key_count == 1 &&
	    !plan.residual_predicate && !plan.mark_build_match &&
	    probe.exact_rhs_output_probe_input_indices.size() == probe.rhs_output_column_count &&
	    std::all_of(probe.exact_rhs_output_probe_input_indices.begin(),
	                probe.exact_rhs_output_probe_input_indices.end(),
	                [](idx_t input_idx) { return input_idx != DConstants::INVALID_INDEX; }) &&
	    (plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	     plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) &&
	    SljitHashJoinProbeProducesSelectedView(output_contract);
	if (exact_source_filter) {
		state.input_offset = input.size();
		state.resume_row_pointer = nullptr;
		state.finished = true;
		state.output_proof.SetExactSourceFilterMatches(probe.exact_rhs_output_probe_input_indices);
		output.SetChildCardinality(input.size());
		runtime.RecordJitRuntimePath("hash_join_probe.regular_probe.exact_source_filter", input.size());
		RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK,
		                              "exact_source_filter", input.size());
		return ExecutionOperatorBindResult::READY;
	}
	const bool rhs_keys_all_valid =
	    !layout.can_have_null || layout.null_keys_are_filtered || (probe.hash_table && !probe.hash_table->has_null);

	auto prepared_input = SljitPrepareRegularHashJoinProbeInput(
	    runtime, op_idx, op.kind, plan, layout, input, match_selection, row_pointers, source_scratch, state,
	    table_layout_kind, source_key0_int64_to_int32_unchecked, rhs_keys_all_valid, probe.use_bloom_filter);
	auto &native_input = prepared_input.native_input;
	const auto mark_selection_mode = SljitHashJoinMarkSelectionModeForOutputContract(output_contract);

	auto generated_stage_start = SljitRegionStageStart(runtime);
	const auto executed_probe_stage =
	    SljitExecuteRegularHashJoinProbePath(owner, runtime, prepared_input.input_kind, op.hash_join_probe, plan,
	                                         layout.needs_chain_matcher, native_input, mark_selection_mode);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, executed_probe_stage, generated_stage_start);
	state.input_offset = native_input.input_offset;
	state.resume_row_pointer = native_input.resume_row_pointer;
	state.finished = native_input.finished;
	auto selected_count = native_input.selected_count;
	if (plan.residual_predicate) {
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
	if (plan.mark_build_match_after_residual) {
		auto mark_build_stage_start = SljitRegionStageStart(runtime);
		SljitMarkHashJoinBuildMatchesAfterResidual(plan, row_pointers, selected_count);
		RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "mark_build_matches", mark_build_stage_start);
	}
	state.output_proof.source_key0_int64_to_int32 = selected_count != 0 && native_input.source_key0_int64_to_int32;
	if (plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY) {
		output.Reset();
		return ExecutionOperatorBindResult::READY;
	}
	if (selected_count == 0) {
		if (left_probe_output && state.finished) {
			return SljitMaterializeLeftHashJoinProbeUnmatchedOutput(runtime, op_idx, op, probe, input, output,
			                                                        match_selection, state);
		}
		output.Reset();
		return ExecutionOperatorBindResult::READY;
	}
	const bool mark_probe = plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE;
	if (!mark_probe) {
		FlatVector::SetSize(row_pointers, count_t(selected_count));
	}
	if (SljitHashJoinProbeProducesSelectedView(output_contract)) {
		const auto boundary_name = SljitHashJoinProbeSelectedViewBoundaryName(mark_probe, mark_selection_mode);
		output.SetChildCardinality(selected_count);
		return ExecutionOperatorBindResult::READY;
	}
	auto materialize_stage_start = SljitRegionStageStart(runtime);
	SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_output");
	ExecutionMaterializeHashJoinProbe(probe, input, row_pointers, match_selection, selected_count, output, &recorder);
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_output", materialize_stage_start);
	return ExecutionOperatorBindResult::READY;
}

template <class OWNER>
static ExecutionOperatorBindResult SljitExecuteNativeHashJoinProbe(
    OWNER &owner, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    SljitRegionExecutionScratch &scratch, idx_t op_idx, SljitExecutableRegionOp &op, DataChunk &input,
    DataChunk &output, SelectionVector &match_selection, SelectionVector &build_selection, Vector &row_pointers,
    SljitHashJoinProbeSourceScratch &source_scratch, DataChunk *residual_chunk, SelectionVector *residual_selection,
    SelectionVector *compact_match_selection, Vector *compact_row_pointers, SljitHashJoinProbeDrainState &state,
    string &deferred_reason, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	auto contract_view = SljitBuildHashJoinProbeExecutionContractView(op, input_remap, output_contract);
	auto &plan = *contract_view.plan;
	ExecutionOperatorBinding *binding_ptr = nullptr;
	auto bind_result = SljitBindRecordedNativeOperator(runtime, native_runtime, scratch, op_idx, op, input,
	                                                   *contract_view.operator_info, "native-operator-runtime-deferred",
	                                                   "SLJIT native hash join probe", binding_ptr, deferred_reason);
	if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
		return bind_result;
	}
	auto &binding = *binding_ptr;
	if (!binding.ready || !binding.hash_join_probe.ready) {
		throw InternalException("SLJIT native hash join probe received an incomplete operator binding");
	}
	auto &probe = binding.hash_join_probe;
	if (plan.output_mode == ExecutionHashJoinProbeOutputMode::NONE || probe.output_mode != plan.output_mode) {
		throw InternalException("SLJIT native hash join probe output mode mismatch");
	}
	const bool left_probe_output = SljitIsLeftHashJoinProbeOutputMode(plan.output_mode);
	if (left_probe_output) {
		SljitInitializeLeftHashJoinProbeState(state, input.size());
		if (state.finished && !state.left_unmatched_emitted) {
			return SljitMaterializeLeftHashJoinProbeUnmatchedOutput(runtime, op_idx, op, probe, input, output,
			                                                        match_selection, state);
		}
	}
	if (probe.probe_key_input_indices.size() != plan.keys.size()) {
		throw InternalException("SLJIT native hash join probe key binding count mismatch");
	}
	state.output_proof.Reset();
	if (SljitHashJoinProbeOutputIsFilteredMarkNonMatches(output_contract)) {
		if (!probe.hash_table) {
			throw InternalException("SLJIT native MARK non-match probe requires a bound hash join table");
		}
		if (probe.hash_table->has_null) {
			return SljitExecuteMarkProbeNoTrueNonMatches(runtime, op, output, state, input.size());
		}
	}
	if (probe.empty_build_side) {
		return SljitExecuteEmptyHashJoinProbe(runtime, op_idx, op, probe, input, output, match_selection, row_pointers,
		                                      state, output_contract);
	}
	runtime.RecordHashJoinProbeLayout(SljitHashJoinProbeLayoutName(probe.layout_kind));
	switch (probe.layout_kind) {
	case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
		return SljitExecutePerfectHashJoinProbe(owner, runtime, op_idx, op, plan, probe, input, output, match_selection,
		                                        build_selection, state, source_key0_int64_to_int32_unchecked,
		                                        output_contract);
	case ExecutionHashJoinProbeLayoutKind::REGULAR_HASH_TABLE:
		return SljitExecuteRegularHashJoinProbe(
		    owner, runtime, scratch, op_idx, op, plan, probe, input, output, match_selection, row_pointers,
		    source_scratch, residual_chunk, residual_selection, compact_match_selection, compact_row_pointers, state,
		    left_probe_output, source_key0_int64_to_int32_unchecked, output_contract);
	default:
		throw InternalException("SLJIT native hash join probe received an unknown layout kind");
	}
}

} // namespace duckdb
