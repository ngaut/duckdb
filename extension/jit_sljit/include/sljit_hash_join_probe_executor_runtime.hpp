//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_executor_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_all_valid_probe_matcher_runtime.hpp"
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

template <class OWNER>
static ExecutionOperatorBindResult SljitExecutePerfectHashJoinProbe(
    OWNER &owner, ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
    const SljitNativeHashJoinProbePlan &plan, const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
    DataChunk &output, SelectionVector &match_selection, SelectionVector &build_selection,
    SljitHashJoinProbeDrainState &state, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT) {
	state.output_proof.perfect_build_selection_is_key_offset = false;
	const bool exact_membership_filter =
	    plan.exact_source_filter_identity &&
	    plan.exact_source_filter_identity == probe.perfect_layout.runtime_filter_identity;
	if (plan.exact_source_filter_identity) {
		runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.exact_source_filter_candidate");
	}
	auto &key = SljitValidatePerfectHashJoinProbeExecutionLayout(plan, probe, input);
	const bool prefer_identity_selection = SljitHashJoinProbePrefersIdentitySelection(output_contract);
	const bool direct_consumer_output =
	    SljitHashJoinProbePrefersDirectConsumerOutput(output_contract) &&
	    SljitCanDerivePerfectHashBuildSelectionFromIdentity(key, probe.perfect_layout, input);
	auto function = owner.EnsurePerfectHashJoinProbeCode(runtime, op.hash_join_probe, prefer_identity_selection,
	                                                     direct_consumer_output);
	SljitPreparedPerfectHashJoinProbeInput prepared_input;
	SljitPreparePerfectHashJoinProbeInput(key, probe.perfect_layout, input, match_selection, build_selection, state,
	                                      source_key0_int64_to_int32_unchecked, prepared_input);
	auto &native_input = prepared_input.native_input;
	const auto initial_input_offset = native_input.input_offset;

	auto generated_stage_start = SljitRegionStageStart(runtime);
	SljitExecuteNativeFunction(function, native_input);
	if (native_input.error) {
		std::rethrow_exception(native_input.error);
	}
	if (prefer_identity_selection && native_input.selected_count != input.size()) {
		// The identity kernel intentionally does not materialize match indices.
		// A miss makes that compact representation necessary, so rerun the same
		// probe with the normal kernel. The first pass has no externally visible
		// state and its compact build selection is overwritten by this retry.
		native_input.input_offset = initial_input_offset;
		native_input.selected_count = 0;
		native_input.finished = false;
		auto compact_function = owner.EnsurePerfectHashJoinProbeCode(runtime, op.hash_join_probe, false);
		SljitExecuteNativeFunction(compact_function, native_input);
		if (native_input.error) {
			std::rethrow_exception(native_input.error);
		}
		runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.identity_selection_retry", input.size());
	} else if (prefer_identity_selection) {
		runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.identity_selection_elided", input.size());
		if (direct_consumer_output) {
			state.output_proof.perfect_build_selection_is_key_offset = true;
			runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.build_selection_elided", input.size());
		}
	}
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, SljitGeneratedPerfectHashJoinProbeStage(),
	                                  generated_stage_start);
	if (exact_membership_filter) {
		runtime.RecordJitRuntimePath("hash_join_probe.perfect_probe.exact_source_filter", native_input.selected_count);
		RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK,
		                              "exact_source_filter", native_input.selected_count);
	}
	state.input_offset = native_input.input_offset;
	state.resume_row_pointer = nullptr;
	state.finished = native_input.finished;
	state.output_proof.source_key0_int64_to_int32 =
	    native_input.selected_count != 0 && native_input.source_key0_int64_to_int32;
	state.output_proof.match_selection_is_identity = native_input.selected_count == input.size();
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

struct SljitExactRegularHashJoinMembershipFilterDispatch {
	const ExecutionHashJoinTableLayout &layout;
	SljitNativeRegularHashJoinProbeInput &input;

	template <class KEY_READER>
	void Execute() {
		using KEY_TYPE = typename KEY_READER::Key;
		using UNSIGNED_KEY_TYPE = typename MakeUnsigned<KEY_TYPE>::type;
		KEY_READER reader(input);
		const auto key_sel = input.source_sel ? input.source_sel[0] : nullptr;
		const auto key_validity = input.source_validity ? input.source_validity[0] : nullptr;
		idx_t selected_count = 0;
		for (idx_t row_idx = 0; row_idx < input.count; row_idx++) {
			const auto source_idx = key_sel ? key_sel[row_idx] : UnsafeNumericCast<sel_t>(row_idx);
			if (key_validity && !(key_validity[source_idx / ValidityMask::BITS_PER_VALUE] &
			                      (validity_t(1) << (source_idx % ValidityMask::BITS_PER_VALUE)))) {
				continue;
			}
			const auto comparable = static_cast<uint64_t>(static_cast<UNSIGNED_KEY_TYPE>(reader.Load(source_idx)));
			const auto filter_idx = comparable - layout.exact_membership_filter_min;
			if (filter_idx > layout.exact_membership_filter_span ||
			    !(layout.exact_membership_filter_bitmap[filter_idx / ValidityMask::BITS_PER_VALUE] &
			      (uint64_t(1) << (filter_idx % ValidityMask::BITS_PER_VALUE)))) {
				continue;
			}
			input.match_sel[selected_count++] = UnsafeNumericCast<sel_t>(row_idx);
		}
		input.selected_count = selected_count;
	}
};

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
	auto prepared_input = SljitPrepareRegularHashJoinProbeInput(
	    runtime, op_idx, op.kind, plan, layout, input, match_selection, row_pointers, source_scratch, state,
	    table_layout_kind, source_key0_int64_to_int32_unchecked, layout.stored_keys_can_have_null,
	    probe.use_bloom_filter);
	auto &native_input = prepared_input.native_input;
	const bool exact_membership_filter =
	    plan.exact_source_filter_identity &&
	    plan.exact_source_filter_identity == probe.table_layout.runtime_filter_identity &&
	    probe.table_layout.exact_membership_filter_build_keys_unique &&
	    probe.table_layout.exact_membership_filter_bitmap && plan.keys.size() == 1 && plan.equality_key_count == 1 &&
	    !plan.residual_predicate && !plan.mark_build_match && !layout.stored_keys_can_have_null &&
	    probe.exact_rhs_output_probe_input_indices.size() == probe.rhs_output_column_count &&
	    std::all_of(probe.exact_rhs_output_probe_input_indices.begin(),
	                probe.exact_rhs_output_probe_input_indices.end(),
	                [](idx_t input_idx) { return input_idx != DConstants::INVALID_INDEX; }) &&
	    (plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD ||
	     plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) &&
	    SljitHashJoinProbeProducesSelectedView(output_contract);
	if (exact_membership_filter) {
		SljitExactRegularHashJoinMembershipFilterDispatch dispatch {layout, native_input};
		if (!SljitDispatchHashJoinSingleKeyReader(plan, native_input, dispatch)) {
			throw InternalException("SLJIT exact regular membership filter has an unsupported key type");
		}
		state.input_offset = input.size();
		state.resume_row_pointer = nullptr;
		state.finished = true;
		state.output_proof.source_key0_int64_to_int32 =
		    native_input.selected_count != 0 && native_input.source_key0_int64_to_int32;
		state.output_proof.SetExactSourceFilterMatches(probe.exact_rhs_output_probe_input_indices,
		                                               native_input.selected_count == input.size());
		output.SetChildCardinality(native_input.selected_count);
		runtime.RecordJitRuntimePath("hash_join_probe.regular_probe.exact_source_filter", native_input.selected_count);
		RecordSljitRegionRuntimeProof(runtime, op.kind, ExecutionRegionJitRuntimeProof::GENERATED_BACKEND_WORK,
		                              "exact_source_filter", native_input.selected_count);
		return ExecutionOperatorBindResult::READY;
	}
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
		if (probe.hash_table->has_filtered_null) {
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
