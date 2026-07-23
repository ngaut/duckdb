//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_regular_output_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_hash_join_probe_chain_codegen.hpp"
#include "sljit_hash_join_probe_loop_codegen.hpp"
#include "sljit_hash_join_probe_regular_codegen_util.hpp"
#include "sljit_hash_join_probe_regular_key_codegen.hpp"
#include "sljit_hash_join_probe_regular_setup_codegen.hpp"
#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

struct SljitRegularHashJoinProbeControlFlow {
	struct sljit_label *row_loop = nullptr;
	struct sljit_label *probe_loop = nullptr;
	struct sljit_label *row_pointer_ready = nullptr;
	struct sljit_jump *done = nullptr;
	struct sljit_jump *empty_slot = nullptr;
	struct sljit_jump *salt_mismatch = nullptr;
	sljit_s32 bitmask_reg = 0;
};

static inline void EmitRetryRegularHashJoinProbeSlot(struct sljit_compiler *compiler,
                                                     const SljitRegularHashJoinProbeControlFlow &control,
                                                     const SljitRegularHashJoinProbeKeyJumps &key_jumps,
                                                     bool restore_probe_offset,
                                                     bool include_predicate_mismatches = false) {
	EmitRetryHashJoinProbeSlot(compiler, control.probe_loop, control.salt_mismatch, key_jumps.equality_key_mismatches,
	                           restore_probe_offset, control.bitmask_reg,
	                           include_predicate_mismatches ? &key_jumps.predicate_key_mismatches : nullptr);
}

static inline void EmitRegularHashJoinOutputMatch(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, row_pointers));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM2(SLJIT_R4, SLJIT_S3), 3, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, match_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R4, SLJIT_S3), 2, SLJIT_S1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

static inline void EmitRegularHashJoinMarkMatchSelection(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, match_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R4, SLJIT_S3), 2, SLJIT_S1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
}

static inline void SetSljitHashJoinProbeSkipJumpLabels(const SljitRegularHashJoinProbeHashJumps &hash_jumps,
                                                       struct sljit_label *label) {
	SetSljitJumpLabels(hash_jumps.source_is_null, label);
	SetSljitJumpLabels(hash_jumps.no_match, label);
}

static inline void EmitSkipRegularHashJoinProbeRow(struct sljit_compiler *compiler,
                                                   const SljitRegularHashJoinProbeControlFlow &control,
                                                   const SljitRegularHashJoinProbeHashJumps &hash_jumps,
                                                   struct sljit_jump *advance_row = nullptr,
                                                   struct sljit_jump *advance_after_predicate = nullptr) {
	auto skip_row = sljit_emit_label(compiler);
	sljit_set_label(control.empty_slot, skip_row);
	SetSljitHashJoinProbeSkipJumpLabels(hash_jumps, skip_row);
	if (advance_row) {
		sljit_set_label(advance_row, skip_row);
	}
	if (advance_after_predicate) {
		sljit_set_label(advance_after_predicate, skip_row);
	}
	EmitRepeatHashJoinProbeRow(compiler, control.row_loop);
}

static inline unique_ptr<ExecutionRegionCodeHandle> FinishRegularHashJoinMarkBuildOnlyOutput(
    struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function, string &error,
    const SljitRegularHashJoinProbeControlFlow &control, const SljitRegularHashJoinProbeHashJumps &hash_jumps,
    const SljitRegularHashJoinProbeKeyJumps &key_jumps, bool restore_probe_offset) {
	auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);
	EmitRetryRegularHashJoinProbeSlot(compiler, control, key_jumps, restore_probe_offset, true);
	EmitSkipRegularHashJoinProbeRow(compiler, control, hash_jumps, advance_row);

	sljit_set_label(control.done, sljit_emit_label(compiler));
	return FinishSljitRegularHashJoinProbeCode(compiler, function, error, SLJIT_IMM, 0);
}

static inline unique_ptr<ExecutionRegionCodeHandle>
FinishRegularHashJoinMarkProbeOutput(struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function,
                                     string &error, const SljitRegularHashJoinProbeControlFlow &control,
                                     const SljitRegularHashJoinProbeHashJumps &hash_jumps,
                                     const SljitRegularHashJoinProbeKeyJumps &key_jumps, bool restore_probe_offset) {
	EmitStoreHashJoinMarkProbeFlag(compiler, 1);
	auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);

	EmitRetryRegularHashJoinProbeSlot(compiler, control, key_jumps, restore_probe_offset, true);
	EmitSkipRegularHashJoinProbeRow(compiler, control, hash_jumps, advance_row);

	sljit_set_label(control.done, sljit_emit_label(compiler));
	return FinishSljitRegularHashJoinProbeCode(compiler, function, error, SLJIT_S2);
}

static inline unique_ptr<ExecutionRegionCodeHandle> FinishRegularHashJoinMarkMatchSelectionOutput(
    struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function, string &error,
    const SljitRegularHashJoinProbeControlFlow &control, const SljitRegularHashJoinProbeHashJumps &hash_jumps,
    const SljitRegularHashJoinProbeKeyJumps &key_jumps, bool restore_probe_offset) {
	EmitRegularHashJoinMarkMatchSelection(compiler);
	auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);

	EmitRetryRegularHashJoinProbeSlot(compiler, control, key_jumps, restore_probe_offset, true);
	EmitSkipRegularHashJoinProbeRow(compiler, control, hash_jumps, advance_row);

	sljit_set_label(control.done, sljit_emit_label(compiler));
	return FinishSljitRegularHashJoinProbeCode(compiler, function, error, SLJIT_S3);
}

static inline unique_ptr<ExecutionRegionCodeHandle> FinishRegularHashJoinMarkNonMatchSelectionOutput(
    struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function, string &error,
    const SljitRegularHashJoinProbeControlFlow &control, const SljitRegularHashJoinProbeHashJumps &hash_jumps,
    const SljitRegularHashJoinProbeKeyJumps &key_jumps, bool restore_probe_offset) {
	auto advance_matched_row = sljit_emit_jump(compiler, SLJIT_JUMP);

	EmitRetryRegularHashJoinProbeSlot(compiler, control, key_jumps, restore_probe_offset, true);

	auto no_match = sljit_emit_label(compiler);
	sljit_set_label(control.empty_slot, no_match);
	SetSljitJumpLabels(hash_jumps.no_match, no_match);
	EmitRegularHashJoinMarkMatchSelection(compiler);
	auto advance_nonmatch_row = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto skip_row = sljit_emit_label(compiler);
	SetSljitJumpLabels(hash_jumps.source_is_null, skip_row);
	sljit_set_label(advance_matched_row, skip_row);
	sljit_set_label(advance_nonmatch_row, skip_row);
	EmitRepeatHashJoinProbeRow(compiler, control.row_loop);

	sljit_set_label(control.done, sljit_emit_label(compiler));
	return FinishSljitRegularHashJoinProbeCode(compiler, function, error, SLJIT_S3);
}

static inline unique_ptr<ExecutionRegionCodeHandle>
FinishRegularHashJoinNoChainOutput(struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function,
                                   string &error, const SljitRegularHashJoinProbeControlFlow &control,
                                   const SljitRegularHashJoinProbeHashJumps &hash_jumps,
                                   const SljitRegularHashJoinProbeKeyJumps &key_jumps, bool restore_probe_offset) {
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat_after_match = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_after_match, control.row_loop);

	auto predicate_mismatch = sljit_emit_label(compiler);
	SetSljitJumpLabels(key_jumps.predicate_key_mismatches, predicate_mismatch);
	auto advance_after_predicate = sljit_emit_jump(compiler, SLJIT_JUMP);

	EmitRetryRegularHashJoinProbeSlot(compiler, control, key_jumps, restore_probe_offset);
	EmitSkipRegularHashJoinProbeRow(compiler, control, hash_jumps, nullptr, advance_after_predicate);

	sljit_set_label(control.done, sljit_emit_label(compiler));
	return FinishSljitRegularHashJoinProbeCode(compiler, function, error, SLJIT_S3);
}

static inline unique_ptr<ExecutionRegionCodeHandle> FinishRegularHashJoinMatchedProbeOnlyOutput(
    struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function, string &error,
    const SljitNativeHashJoinProbePlan &plan, const SljitHashJoinProbeCodegenConfig &config,
    const SljitRegularHashJoinProbeControlFlow &control, const SljitRegularHashJoinProbeHashJumps &hash_jumps,
    const SljitRegularHashJoinProbeKeyJumps &key_jumps, sljit_s32 aux_next_ptrs_reg, bool restore_probe_offset) {
	auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto predicate_mismatch = sljit_emit_label(compiler);
	SetSljitJumpLabels(key_jumps.predicate_key_mismatches, predicate_mismatch);
	EmitLoadHashJoinNextPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, plan.pointer_offset, config, aux_next_ptrs_reg);
	auto continue_chain = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto advance_after_predicate = sljit_emit_jump(compiler, SLJIT_JUMP);

	EmitRetryRegularHashJoinProbeSlot(compiler, control, key_jumps, restore_probe_offset);
	EmitSkipRegularHashJoinProbeRow(compiler, control, hash_jumps, advance_row, advance_after_predicate);
	sljit_set_label(continue_chain, control.row_pointer_ready);

	sljit_set_label(control.done, sljit_emit_label(compiler));
	return FinishSljitRegularHashJoinProbeCode(compiler, function, error, SLJIT_S3);
}

static inline unique_ptr<ExecutionRegionCodeHandle> FinishRegularHashJoinChainedOutput(
    struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function, string &error,
    const SljitNativeHashJoinProbePlan &plan, const SljitHashJoinProbeCodegenConfig &config,
    const SljitRegularHashJoinProbeControlFlow &control, const SljitRegularHashJoinProbeHashJumps &hash_jumps,
    const SljitRegularHashJoinProbeKeyJumps &key_jumps, sljit_s32 aux_next_ptrs_reg, bool restore_probe_offset) {
	EmitLoadHashJoinNextPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, plan.pointer_offset, config, aux_next_ptrs_reg);
	auto output_has_capacity = sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S3, 0, SLJIT_IMM, STANDARD_VECTOR_SIZE);
	auto pause_same_row = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto pause_output = sljit_emit_label(compiler);
	sljit_set_label(pause_same_row, pause_output);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, selected_count), SLJIT_S3, 0);
	EmitPauseRegularHashJoinProbe(compiler, SLJIT_R0);
	sljit_emit_return_void(compiler);

	auto output_has_capacity_label = sljit_emit_label(compiler);
	sljit_set_label(output_has_capacity, output_has_capacity_label);
	auto continue_chain_after_match = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto predicate_mismatch = sljit_emit_label(compiler);
	SetSljitJumpLabels(key_jumps.predicate_key_mismatches, predicate_mismatch);
	EmitLoadHashJoinNextPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, plan.pointer_offset, config, aux_next_ptrs_reg);
	auto continue_chain_after_predicate = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto advance_after_predicate = sljit_emit_jump(compiler, SLJIT_JUMP);

	EmitRetryRegularHashJoinProbeSlot(compiler, control, key_jumps, restore_probe_offset);
	EmitSkipRegularHashJoinProbeRow(compiler, control, hash_jumps, advance_row, advance_after_predicate);
	sljit_set_label(continue_chain_after_match, control.row_pointer_ready);
	sljit_set_label(continue_chain_after_predicate, control.row_pointer_ready);

	sljit_set_label(control.done, sljit_emit_label(compiler));
	return FinishSljitRegularHashJoinProbeCode(compiler, function, error, SLJIT_S3);
}

} // namespace duckdb
