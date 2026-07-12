//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_path_runtime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_filter_runtime.hpp"
#include "sljit_hash_join_all_valid_probe_fast_path_runtime.hpp"
#include "sljit_hash_join_probe_runtime.hpp"
#include "sljit_hash_join_probe_specialization.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_native_function_runtime.hpp"
#include "sljit_region_adapter_scratch.hpp"
#include "sljit_region_runtime_trace.hpp"

namespace duckdb {

static idx_t SljitApplyNativeHashJoinResidualPredicate(
    ExecutionRegionRuntime &runtime, SljitExecutableRegionOp &op, const ExecutionHashJoinProbeBinding &probe,
    DataChunk &input, Vector &row_pointers, SelectionVector &match_selection, idx_t count, DataChunk *residual_chunk,
    SelectionVector *residual_selection, SelectionVector *compact_match_selection, Vector *compact_row_pointers,
    SljitExpressionAdapterScratch *adapter_scratch, optional_ptr<ExecutionOperatorStageRecorder> recorder) {
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
	    SljitSelectExpression(residual_filter, *residual_chunk, *residual_selection, *adapter_scratch);
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

template <bool SELECTED, class OWNER>
static const char *SljitExecuteAllValidRegularHashJoinProbePath(OWNER &owner, ExecutionRegionRuntime &runtime,
                                                                SljitExecutableHashJoinProbe &hash_join_probe,
                                                                const SljitNativeHashJoinProbePlan &plan,
                                                                bool needs_chain_matcher,
                                                                SljitNativeRegularHashJoinProbeInput &native_input) {
	const bool can_use_chain_input = SljitHashJoinCanUseAllValidChainInput(native_input);
	const SljitAllValidHashJoinProbeFacts facts {can_use_chain_input, can_use_chain_input && !needs_chain_matcher};
	for (auto &fast_path : SljitAllValidHashJoinProbeFastPaths(SELECTED)) {
		if (fast_path.execute(plan, native_input, facts)) {
			return SljitAllValidHashJoinProbeFastPathStage(fast_path, SELECTED);
		}
	}
	auto key = SljitHashJoinProbeAllValidSpecializationKey::FromLayoutKind(SELECTED, native_input.layout_kind);
	auto function = owner.EnsureAllValidRegularHashJoinProbeCode(runtime, hash_join_probe, key);
	SljitExecuteNativeFunction(function, native_input);
	return SljitGeneratedAllValidRegularHashJoinProbeStage(SELECTED);
}

template <bool SELECTED, class OWNER>
static const char *SljitExecuteAllValidRegularHashJoinMarkSelectionProbePath(
    OWNER &owner, ExecutionRegionRuntime &runtime, SljitExecutableHashJoinProbe &hash_join_probe,
    const SljitNativeHashJoinProbePlan &plan, bool needs_chain_matcher,
    SljitNativeRegularHashJoinProbeInput &native_input, SljitHashJoinMarkSelectionMode mark_selection_mode) {
	const bool can_use_chain_input = SljitHashJoinCanUseAllValidChainInput(native_input);
	const SljitAllValidHashJoinProbeFacts facts {can_use_chain_input, can_use_chain_input && !needs_chain_matcher};
	for (auto &fast_path : SljitAllValidHashJoinMarkSelectionProbeFastPaths(SELECTED)) {
		if (fast_path.execute(plan, native_input, facts, mark_selection_mode)) {
			return SljitAllValidHashJoinMarkSelectionProbeFastPathStage(fast_path, SELECTED, mark_selection_mode);
		}
	}
	auto key = SljitHashJoinProbeAllValidSpecializationKey::FromLayoutKind(SELECTED, native_input.layout_kind);
	auto function = owner.EnsureAllValidRegularHashJoinProbeCode(runtime, hash_join_probe, key, mark_selection_mode);
	SljitExecuteNativeFunction(function, native_input);
	return SljitGeneratedAllValidRegularHashJoinProbeStage(SELECTED, mark_selection_mode);
}

template <class OWNER>
static const char *SljitExecuteRegularHashJoinProbePath(
    OWNER &owner, ExecutionRegionRuntime &runtime, SljitHashJoinProbeInputKind input_kind,
    SljitExecutableHashJoinProbe &hash_join_probe, const SljitNativeHashJoinProbePlan &plan, bool needs_chain_matcher,
    SljitNativeRegularHashJoinProbeInput &native_input,
    SljitHashJoinMarkSelectionMode mark_selection_mode = SljitHashJoinMarkSelectionMode::NONE) {
	if (input_kind == SljitHashJoinProbeInputKind::FLAT_ALL_VALID) {
		if (!SljitHashJoinEmitsMarkSelection(mark_selection_mode)) {
			return SljitExecuteAllValidRegularHashJoinProbePath<false>(owner, runtime, hash_join_probe, plan,
			                                                           needs_chain_matcher, native_input);
		}
		return SljitExecuteAllValidRegularHashJoinMarkSelectionProbePath<false>(
		    owner, runtime, hash_join_probe, plan, needs_chain_matcher, native_input, mark_selection_mode);
	}
	if (input_kind == SljitHashJoinProbeInputKind::SELECTED_ALL_VALID) {
		if (!SljitHashJoinEmitsMarkSelection(mark_selection_mode)) {
			return SljitExecuteAllValidRegularHashJoinProbePath<true>(owner, runtime, hash_join_probe, plan,
			                                                          needs_chain_matcher, native_input);
		}
		return SljitExecuteAllValidRegularHashJoinMarkSelectionProbePath<true>(
		    owner, runtime, hash_join_probe, plan, needs_chain_matcher, native_input, mark_selection_mode);
	}
	const bool uses_bloom_filter = native_input.bloom_filter_bits != nullptr;
	auto function =
	    owner.EnsureRegularHashJoinProbeCode(runtime, hash_join_probe, uses_bloom_filter, mark_selection_mode);
	SljitExecuteNativeFunction(function, native_input);
	return SljitGeneratedRegularHashJoinProbeStage(uses_bloom_filter, mark_selection_mode);
}

static SljitPreparedRegularHashJoinProbeInput SljitPrepareRegularHashJoinProbeInput(
    ExecutionRegionRuntime &runtime, idx_t op_idx, SljitNativeRegionOpKind op_kind,
    const SljitNativeHashJoinProbePlan &plan, const ExecutionHashJoinTableLayout &layout, DataChunk &input,
    SelectionVector &match_selection, Vector &row_pointers, SljitHashJoinProbeSourceScratch &source_scratch,
    SljitHashJoinProbeDrainState &state, SljitHashJoinProbeLayoutKind table_layout_kind,
    bool allow_unchecked_int64_to_int32, bool rhs_keys_all_valid, bool use_bloom_filter) {
	auto vector_setup_stage_start = SljitRegionStageStart(runtime);
	auto source_key0_int64_to_int32 = source_scratch.Prepare(input, plan);
	row_pointers.SetVectorType(VectorType::FLAT_VECTOR);
	auto row_pointer_data = FlatVector::GetDataMutable<data_ptr_t>(row_pointers);
	RecordSljitRegionStageRuntime(runtime, op_idx, op_kind, "vector_setup", vector_setup_stage_start);

	auto source_sel_array = source_scratch.SelectionArrayOrNull();
	auto source_validity_array = source_scratch.ValidityArrayOrNull();
	const bool source_selection_present = source_sel_array != nullptr;
	const SljitRegularHashJoinProbeInputShape input_shape {
	    source_selection_present, source_selection_present && source_scratch.HasCommonSelection(),
	    source_validity_array != nullptr, rhs_keys_all_valid};

	SljitPreparedRegularHashJoinProbeInput result;
	result.input_kind = input_shape.PathKind();
	auto &native_input = result.native_input;
	native_input.source_data = source_scratch.DataArray();
	native_input.source_sel = source_sel_array;
	native_input.source_validity = source_validity_array;
	native_input.source_key0_int64_to_int32 = source_key0_int64_to_int32;
	native_input.source_key0_int64_to_int32_unchecked = source_key0_int64_to_int32 && allow_unchecked_int64_to_int32;
	native_input.count = input.size();
	native_input.entries = reinterpret_cast<const_data_ptr_t>(layout.entries);
	native_input.bitmask = layout.bitmask;
	native_input.pointer_mask = layout.pointer_mask;
	native_input.layout_kind = table_layout_kind;
	native_input.rhs_keys_have_validity = layout.can_have_null && !rhs_keys_all_valid;
	native_input.pointer_offset = layout.pointer_offset;
	native_input.aux_next_ptrs = layout.aux_next_ptrs;
	native_input.bloom_filter_bits = use_bloom_filter && layout.bloom_filter ? layout.bloom_filter->Data() : nullptr;
	native_input.bloom_filter_bitmask = use_bloom_filter && layout.bloom_filter ? layout.bloom_filter->Bitmask() : 0;
	native_input.match_sel = match_selection.data();
	native_input.row_pointers = row_pointer_data;
	native_input.output_capacity = STANDARD_VECTOR_SIZE;
	native_input.selected_count = 0;
	native_input.input_offset = state.input_offset;
	native_input.resume_row_pointer = state.resume_row_pointer;
	native_input.finished = false;
	return result;
}

} // namespace duckdb
