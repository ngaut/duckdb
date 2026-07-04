//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_executor_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_primitive.hpp"
#include "sljit_hash_join_probe_path_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_native_binding_runtime.hpp"
#include "sljit_native_function_runtime.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

enum class SljitHashJoinProbeOutputContract {
	MATERIALIZED_OUTPUT,
	SELECTED_VIEW,
	FILTERED_MARK_MATCHES,
	FILTERED_MARK_NON_MATCHES
};

static bool SljitHashJoinProbeOutputIsFilteredMarkMatches(SljitHashJoinProbeOutputContract output_contract) {
	return output_contract == SljitHashJoinProbeOutputContract::FILTERED_MARK_MATCHES;
}

static bool SljitHashJoinProbeOutputIsFilteredMarkNonMatches(SljitHashJoinProbeOutputContract output_contract) {
	return output_contract == SljitHashJoinProbeOutputContract::FILTERED_MARK_NON_MATCHES;
}

static SljitHashJoinMarkSelectionMode
SljitHashJoinMarkSelectionModeForOutputContract(SljitHashJoinProbeOutputContract output_contract) {
	if (SljitHashJoinProbeOutputIsFilteredMarkMatches(output_contract)) {
		return SljitHashJoinMarkSelectionMode::MATCHES;
	}
	if (SljitHashJoinProbeOutputIsFilteredMarkNonMatches(output_contract)) {
		return SljitHashJoinMarkSelectionMode::NON_MATCHES;
	}
	return SljitHashJoinMarkSelectionMode::NONE;
}

static bool SljitHashJoinProbeProducesSelectedView(SljitHashJoinProbeOutputContract output_contract) {
	return output_contract == SljitHashJoinProbeOutputContract::SELECTED_VIEW ||
	       SljitHashJoinProbeOutputIsFilteredMarkMatches(output_contract) ||
	       SljitHashJoinProbeOutputIsFilteredMarkNonMatches(output_contract);
}

struct SljitHashJoinProbeExecutionContractView {
	const SljitNativeHashJoinProbePlan *plan = nullptr;
	const ExecutionRegionOperatorInfo *operator_info = nullptr;
	SljitNativeHashJoinProbePlan remapped_plan;
	ExecutionRegionOperatorInfo remapped_operator_info;
};

static bool SljitHashJoinProbeKeyKindMatchesPhysicalType(SljitNativeHashJoinKeyKind key_kind,
                                                         PhysicalType physical_type) {
	switch (key_kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return physical_type == PhysicalType::INT8;
	case SljitNativeHashJoinKeyKind::INT16:
		return physical_type == PhysicalType::INT16;
	case SljitNativeHashJoinKeyKind::INT32:
		return physical_type == PhysicalType::INT32;
	case SljitNativeHashJoinKeyKind::INT64:
		return physical_type == PhysicalType::INT64;
	case SljitNativeHashJoinKeyKind::INT128:
		return physical_type == PhysicalType::INT128;
	case SljitNativeHashJoinKeyKind::UINT8:
		return physical_type == PhysicalType::UINT8;
	case SljitNativeHashJoinKeyKind::UINT16:
		return physical_type == PhysicalType::UINT16;
	case SljitNativeHashJoinKeyKind::UINT32:
		return physical_type == PhysicalType::UINT32;
	case SljitNativeHashJoinKeyKind::UINT64:
		return physical_type == PhysicalType::UINT64;
	case SljitNativeHashJoinKeyKind::UINT128:
		return physical_type == PhysicalType::UINT128;
	default:
		return false;
	}
}

static bool SljitRemappedHashJoinProbeKeySourceSupported(const SljitNativeHashJoinProbeKeyPlan &key, idx_t key_idx,
                                                         PhysicalType source_type) {
	if (SljitHashJoinProbeKeyKindMatchesPhysicalType(key.key_kind, source_type)) {
		return true;
	}
	return key_idx == 0 && key.key_kind == SljitNativeHashJoinKeyKind::INT32 && source_type == PhysicalType::INT64;
}

static void SljitApplyHashJoinResidualProbeSourceRemap(ExecutionRegionOperatorInfo &operator_info, DataChunk &input,
                                                       const vector<idx_t> &residual_probe_source_indices) {
	if (residual_probe_source_indices.empty()) {
		return;
	}
	auto &residual_sources = operator_info.hash_join_contract.residual_sources;
	if (residual_probe_source_indices.size() != residual_sources.size()) {
		throw InternalException("SLJIT remapped hash join residual source count mismatch");
	}
	for (auto &source : residual_sources) {
		if (source.kind == ExecutionHashJoinResidualSourceKind::BUILD) {
			continue;
		}
		if (source.kind != ExecutionHashJoinResidualSourceKind::PROBE ||
		    source.source_index >= residual_probe_source_indices.size()) {
			throw InternalException("SLJIT remapped hash join residual source shape mismatch");
		}
		const auto input_idx = residual_probe_source_indices[source.source_index];
		if (input_idx == DConstants::INVALID_INDEX) {
			continue;
		}
		if (input_idx >= input.ColumnCount()) {
			throw InternalException("SLJIT remapped hash join residual probe source is out of range");
		}
		if (input.data[input_idx].GetType() != source.type) {
			throw InternalException("SLJIT remapped hash join residual probe source type mismatch");
		}
		source.input_index = input_idx;
	}
}

static SljitHashJoinProbeExecutionContractView
SljitBuildHashJoinProbeExecutionContractView(SljitExecutableRegionOp &op, DataChunk &input,
                                             optional_ptr<const SljitHashJoinProbeInputRemap> input_remap,
                                             SljitHashJoinProbeOutputContract output_contract) {
	auto &plan = op.hash_join_probe.plan;
	SljitHashJoinProbeExecutionContractView view;
	view.plan = &plan;
	view.operator_info = &plan.operator_info;
	if (!input_remap || (!input_remap->HasKeyInputRemap() && !input_remap->HasResidualProbeSourceRemap())) {
		return view;
	}
	if (!SljitHashJoinProbeProducesSelectedView(output_contract)) {
		throw InternalException("SLJIT remapped hash join probe input requires selected-view execution");
	}

	view.remapped_plan = plan.Copy(false);
	view.remapped_operator_info = plan.operator_info;
	view.remapped_plan.input_types.clear();
	view.remapped_plan.input_types.reserve(input.ColumnCount());
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		view.remapped_plan.input_types.push_back(input.data[col_idx].GetType());
	}
	if (input_remap->HasKeyInputRemap()) {
		if (input_remap->key_input_indices.size() != plan.keys.size() ||
		    plan.operator_info.hash_join_keys.size() != plan.keys.size()) {
			throw InternalException("SLJIT remapped hash join probe key count mismatch");
		}
		for (idx_t key_idx = 0; key_idx < view.remapped_plan.keys.size(); key_idx++) {
			const auto input_idx = input_remap->key_input_indices[key_idx];
			if (input_idx >= input.ColumnCount()) {
				throw InternalException("SLJIT remapped hash join probe key input is out of range");
			}
			auto &key = view.remapped_plan.keys[key_idx];
			if (!SljitRemappedHashJoinProbeKeySourceSupported(key, key_idx,
			                                                  input.data[input_idx].GetType().InternalType())) {
				throw InternalException("SLJIT remapped hash join probe key source type is unsupported");
			}
			key.key_input_index = input_idx;
			view.remapped_operator_info.hash_join_keys[key_idx].input_index = input_idx;
		}
	}
	SljitApplyHashJoinResidualProbeSourceRemap(view.remapped_operator_info, input,
	                                           input_remap->residual_probe_source_indices);
	view.remapped_plan.operator_info = view.remapped_operator_info;
	view.plan = &view.remapped_plan;
	view.operator_info = &view.remapped_operator_info;
	return view;
}

template <class KERNEL>
static ExecutionOperatorBindResult SljitExecuteNativeHashJoinProbeWithScratch(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
    DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
    bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
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
	    state, deferred_reason, source_key0_int64_to_int32_unchecked, output_contract, input_remap);
}

template <class KERNEL>
static ExecutionOperatorBindResult SljitExecuteRecordedNativeHashJoinProbeWithScratch(
    KERNEL &kernel, ExecutionRegionRuntime &runtime, ExecutionOperatorRuntime &native_runtime,
    SljitRegionExecutionScratch &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
    DataChunk &output, SljitHashJoinProbeDrainState &state, string &deferred_reason,
    bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	auto stage_start = SljitRegionStageStart(runtime);
	auto bind_result = SljitExecuteNativeHashJoinProbeWithScratch(
	    kernel, runtime, native_runtime, scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
	    source_key0_int64_to_int32_unchecked, output_contract, input_remap);
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
	                                    bool source_key0_int64_to_int32_unchecked,
	                                    SljitHashJoinProbeOutputContract output_contract,
	                                    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
		return SljitExecuteRecordedNativeHashJoinProbeWithScratch(
		    owner, runtime, native_runtime, scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		    source_key0_int64_to_int32_unchecked, output_contract, input_remap);
	}

	ExecutionOperatorBindResult operator()(SljitRegionExecutionScratch &scratch, idx_t hash_join_idx,
	                                       SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
	                                       SljitHashJoinProbeDrainState &state, string &deferred_reason,
	                                       bool source_key0_int64_to_int32_unchecked,
	                                       SljitHashJoinProbeOutputContract output_contract,
	                                       optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
		return Execute(scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		               source_key0_int64_to_int32_unchecked, output_contract, input_remap);
	}

	ExecutionOperatorBindResult operator()(idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input,
	                                       DataChunk &output, SljitHashJoinProbeDrainState &state,
	                                       string &deferred_reason, bool source_key0_int64_to_int32_unchecked,
	                                       SljitHashJoinProbeOutputContract output_contract,
	                                       optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
		if (!fixed_scratch) {
			throw InternalException("SLJIT recorded hash join probe callback requires fixed scratch");
		}
		return Execute(*fixed_scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		               source_key0_int64_to_int32_unchecked, output_contract, input_remap);
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

template <class SCRATCH, class EXECUTE_HASH_JOIN_PROBE, class HANDLE_OUTPUT, class HANDLE_DEFER>
static bool SljitDrainHashJoinProbeOutputsWithState(
    SCRATCH &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe, HANDLE_OUTPUT &&handle_output, HANDLE_DEFER &&handle_defer,
    bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	SljitHashJoinProbeDrainState state;
	do {
		output.Reset();
		string deferred_reason;
		auto bind_result =
		    execute_hash_join_probe(scratch, hash_join_idx, hash_join_op, input, output, state, deferred_reason,
		                            source_key0_int64_to_int32_unchecked, output_contract, input_remap);
		if (bind_result == ExecutionOperatorBindResult::DEFERRED) {
			return handle_defer(deferred_reason);
		}
		if (output.size() != 0 && handle_output(output, state)) {
			return true;
		}
	} while (!SljitHashJoinProbeDrainFinished(hash_join_op.hash_join_probe.plan.output_mode, state));
	return false;
}

template <class SCRATCH, class EXECUTE_HASH_JOIN_PROBE, class HANDLE_OUTPUT, class HANDLE_DEFER>
static bool SljitDrainHashJoinProbeOutputs(
    SCRATCH &scratch, idx_t hash_join_idx, SljitExecutableRegionOp &hash_join_op, DataChunk &input, DataChunk &output,
    EXECUTE_HASH_JOIN_PROBE &&execute_hash_join_probe, HANDLE_OUTPUT &&handle_output, HANDLE_DEFER &&handle_defer,
    bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	auto handle_output_without_state = [&](DataChunk &output, SljitHashJoinProbeDrainState &) {
		return handle_output(output);
	};
	return SljitDrainHashJoinProbeOutputsWithState(
	    scratch, hash_join_idx, hash_join_op, input, output,
	    std::forward<EXECUTE_HASH_JOIN_PROBE>(execute_hash_join_probe), handle_output_without_state,
	    std::forward<HANDLE_DEFER>(handle_defer), source_key0_int64_to_int32_unchecked, output_contract, input_remap);
}

static ExecutionOperatorBindResult SljitMaterializeLeftHashJoinProbeUnmatchedOutput(
    ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
    const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output, SelectionVector &match_selection,
    SljitHashJoinProbeDrainState &state) {
	auto materialize_stage_start = SljitRegionStageStart(runtime);
	SljitRegionStageRecorder recorder(runtime, op_idx, op.kind, "materialize_left_unmatched");
	SljitMaterializeLeftHashJoinProbeUnmatched(probe, input, output, match_selection, state, &recorder);
	RecordSljitRegionMaterializationBoundary(runtime, op.kind, "final_output_left_unmatched", output.size());
	RecordSljitRegionStageRuntime(runtime, op_idx, op.kind, "materialize_left_unmatched", materialize_stage_start);
	return ExecutionOperatorBindResult::READY;
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
	output.Reset();
	return ExecutionOperatorBindResult::READY;
}

template <class OWNER>
static ExecutionOperatorBindResult SljitExecutePerfectHashJoinProbe(
    OWNER &owner, ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
    const SljitNativeHashJoinProbePlan &plan, const ExecutionHashJoinProbeBinding &probe, DataChunk &input,
    DataChunk &output, SelectionVector &match_selection, SelectionVector &build_selection,
    SljitHashJoinProbeDrainState &state, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT) {
	owner.EnsurePerfectHashJoinProbeCode(runtime, op.hash_join_probe);
	auto &key = SljitValidatePerfectHashJoinProbeExecutionLayout(plan, probe, input);
	SljitPreparedPerfectHashJoinProbeInput prepared_input;
	SljitPreparePerfectHashJoinProbeInput(key, probe.perfect_layout, input, match_selection, build_selection, state,
	                                      source_key0_int64_to_int32_unchecked, prepared_input);
	auto &native_input = prepared_input.native_input;

	auto generated_stage_start = SljitRegionStageStart(runtime);
	SljitExecuteNativeFunction(op.hash_join_probe.perfect.function, native_input);
	RecordSljitRegionStageRuntimePath(runtime, op_idx, op.kind, SljitGeneratedPerfectHashJoinProbeStage(),
	                                  generated_stage_start);
	state.input_offset = native_input.input_offset;
	state.resume_row_pointer = nullptr;
	state.finished = native_input.finished;
	state.source_key0_int64_to_int32_matches_are_proven =
	    native_input.selected_count != 0 && native_input.source_key0_int64_to_int32;
	if (native_input.selected_count == 0) {
		output.Reset();
		return ExecutionOperatorBindResult::READY;
	}
	if (SljitHashJoinProbeProducesSelectedView(output_contract)) {
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, "perfect_selection_reference",
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
    SljitExecutableRegionOp &op, const SljitNativeHashJoinProbePlan &plan, const ExecutionHashJoinProbeBinding &probe,
    DataChunk &input, DataChunk &output, SelectionVector &match_selection, Vector &row_pointers,
    SljitHashJoinProbeSourceScratch &source_scratch, DataChunk *residual_chunk, SelectionVector *residual_selection,
    SelectionVector *compact_match_selection, Vector *compact_row_pointers, SljitHashJoinProbeDrainState &state,
    bool left_probe_output, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT) {
	auto &layout = probe.table_layout;
	const auto table_layout_kind = SljitValidateRegularHashJoinProbeExecutionLayout(plan, probe);
	state.source_key0_int64_to_int32_matches_are_proven = false;
	const bool rhs_keys_all_valid =
	    !layout.can_have_null || layout.null_keys_are_filtered || (probe.hash_table && !probe.hash_table->has_null);

	auto prepared_input = SljitPrepareRegularHashJoinProbeInput(
	    runtime, op_idx, op.kind, plan, layout, input, match_selection, row_pointers, source_scratch, state,
	    table_layout_kind, source_key0_int64_to_int32_unchecked, rhs_keys_all_valid);
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
	state.source_key0_int64_to_int32_matches_are_proven =
	    selected_count != 0 && native_input.source_key0_int64_to_int32;
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
		const char *boundary_name = "row_pointer_selection_reference";
		if (mark_selection_mode == SljitHashJoinMarkSelectionMode::MATCHES) {
			boundary_name = "mark_match_selection_reference";
		} else if (mark_selection_mode == SljitHashJoinMarkSelectionMode::NON_MATCHES) {
			boundary_name = "mark_nonmatch_selection_reference";
		} else if (mark_probe) {
			boundary_name = "mark_flags";
		}
		RecordSljitRegionMaterializationBoundary(runtime, op.kind, boundary_name, selected_count);
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

static ExecutionOperatorBindResult SljitExecuteEmptyHashJoinProbe(
    ExecutionRegionRuntime &runtime, idx_t op_idx, SljitExecutableRegionOp &op,
    const ExecutionHashJoinProbeBinding &probe, DataChunk &input, DataChunk &output, SelectionVector &match_selection,
    Vector &row_pointers, SljitHashJoinProbeDrainState &state,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT) {
	state.finished = true;
	RecordSljitRegionRuntimePath(runtime, op.kind, "empty_build_side");
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
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "mark_nonmatch_selection_reference",
			                                         selected_count);
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
			RecordSljitRegionMaterializationBoundary(runtime, op.kind, "mark_flags", input.size());
			output.SetChildCardinality(input.size());
			break;
		}
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
    string &deferred_reason, bool source_key0_int64_to_int32_unchecked = false,
    SljitHashJoinProbeOutputContract output_contract = SljitHashJoinProbeOutputContract::MATERIALIZED_OUTPUT,
    optional_ptr<const SljitHashJoinProbeInputRemap> input_remap = nullptr) {
	auto contract_view = SljitBuildHashJoinProbeExecutionContractView(op, input, input_remap, output_contract);
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
	if (SljitHashJoinProbeOutputIsFilteredMarkNonMatches(output_contract)) {
		if (!probe.hash_table) {
			throw InternalException("SLJIT native MARK non-match probe requires a bound hash join table");
		}
		if (probe.hash_table->has_null) {
			return SljitExecuteMarkProbeNoTrueNonMatches(runtime, op, output, state, input.size());
		}
	}
	if (probe.empty_build_side) {
		state.source_key0_int64_to_int32_matches_are_proven = false;
		return SljitExecuteEmptyHashJoinProbe(runtime, op_idx, op, probe, input, output, match_selection, row_pointers,
		                                      state, output_contract);
	}
	runtime.RecordHashJoinProbeLayout(SljitHashJoinProbeLayoutName(probe.layout_kind));
	switch (probe.layout_kind) {
	case ExecutionHashJoinProbeLayoutKind::PERFECT_HASH_TABLE:
		state.source_key0_int64_to_int32_matches_are_proven = false;
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
