#include "sljit_join_probe_codegen.hpp"

#include "sljit_hash_join_probe_chain_codegen.hpp"
#include "sljit_hash_join_probe_codegen_validation.hpp"
#include "sljit_hash_join_probe_loop_codegen.hpp"
#include "sljit_hash_join_probe_regular_codegen_util.hpp"
#include "sljit_hash_join_probe_regular_key_codegen.hpp"
#include "sljit_hash_join_probe_regular_output_codegen.hpp"
#include "sljit_hash_join_probe_regular_setup_codegen.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle> BuildSljitRegularHashJoinProbe(const SljitNativeHashJoinProbePlan &plan,
                                                                     SljitNativeRegularHashJoinProbeFunction &function,
                                                                     string &error,
                                                                     const SljitHashJoinProbeCodegenConfig &config) {
	if (!SljitValidateRegularHashJoinProbePlan(plan, error)) {
		return nullptr;
	}
	auto &keys = plan.keys;
	const bool mark_build_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY;
	const bool mark_probe = plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE;
	const bool mark_match_selection = mark_probe && config.EmitsMarkMatchesAsSelection();
	const bool mark_nonmatch_selection = mark_probe && config.EmitsMarkNonMatchesAsSelection();
	const bool matched_probe_only = plan.output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	SljitRegularHashJoinProbeRegisters registers;
	if (!PrepareRegularHashJoinProbeRegisters(plan, config, registers, error)) {
		sljit_free_compiler(compiler);
		return nullptr;
	}
	const bool assume_all_keys_valid = registers.assume_all_keys_valid;
	const auto aux_next_ptrs_reg = registers.aux_next_ptrs_reg;
	const auto pointer_mask_reg = registers.pointer_mask_reg;
	EmitEnterRegularHashJoinProbe(compiler, registers);

	struct sljit_jump *resume_row_pointer_ready = nullptr;
	if (!config.SpecializesNoChainLayout()) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeRegularHashJoinProbeInput, resume_row_pointer));
		auto no_resume_row_pointer = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		resume_row_pointer_ready = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(no_resume_row_pointer, sljit_emit_label(compiler));
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	if (mark_probe && !config.EmitsMarkSelection()) {
		EmitStoreHashJoinMarkProbeFlag(compiler, 0);
	}
	auto hash_jumps = EmitRegularHashJoinProbeHash(compiler, plan, config, registers);

	auto probe_loop = sljit_emit_label(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_S4, SLJIT_R1), 3);
	auto empty_slot = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	auto salt_mismatch = EmitRegularHashJoinSaltMismatch(compiler, config, pointer_mask_reg);

	EmitLoadHashJoinPointerMask(compiler, SLJIT_R4, pointer_mask_reg);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op_src(compiler, SLJIT_PREFETCH_L1, SLJIT_MEM1(SLJIT_R0),
	                  NumericCast<sljit_sw>(keys[0].key_layout_offset));
	if (!assume_all_keys_valid) {
		EmitSaveHashJoinProbeOffset(compiler);
	}
	auto row_pointer_ready = sljit_emit_label(compiler);
	if (resume_row_pointer_ready) {
		sljit_set_label(resume_row_pointer_ready, row_pointer_ready);
	}
	auto key_jumps = EmitRegularHashJoinKeyChecks(compiler, plan, registers);
	if (plan.mark_build_match) {
		if (mark_build_only) {
			EmitMarkHashJoinBuildChain(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, plan.found_match_offset,
			                           plan.pointer_offset, config, aux_next_ptrs_reg);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), NumericCast<sljit_sw>(plan.found_match_offset),
			               SLJIT_IMM, 1);
		}
	}
	SljitRegularHashJoinProbeControlFlow control;
	control.row_loop = loop;
	control.probe_loop = probe_loop;
	control.row_pointer_ready = row_pointer_ready;
	control.done = done;
	control.empty_slot = empty_slot;
	control.salt_mismatch = salt_mismatch;
	control.bitmask_reg = registers.bitmask_reg;
	if (mark_build_only) {
		return FinishRegularHashJoinMarkBuildOnlyOutput(compiler, function, error, control, hash_jumps, key_jumps,
		                                                !assume_all_keys_valid);
	}

	if (mark_probe) {
		if (mark_match_selection) {
			return FinishRegularHashJoinMarkMatchSelectionOutput(compiler, function, error, control, hash_jumps,
			                                                     key_jumps, !assume_all_keys_valid);
		}
		if (mark_nonmatch_selection) {
			return FinishRegularHashJoinMarkNonMatchSelectionOutput(compiler, function, error, control, hash_jumps,
			                                                        key_jumps, !assume_all_keys_valid);
		}
		return FinishRegularHashJoinMarkProbeOutput(compiler, function, error, control, hash_jumps, key_jumps,
		                                            !assume_all_keys_valid);
	}

	EmitRegularHashJoinOutputMatch(compiler);
	if (config.SpecializesNoChainLayout()) {
		return FinishRegularHashJoinNoChainOutput(compiler, function, error, control, hash_jumps, key_jumps,
		                                          !assume_all_keys_valid);
	}
	if (matched_probe_only) {
		return FinishRegularHashJoinMatchedProbeOnlyOutput(compiler, function, error, plan, config, control, hash_jumps,
		                                                   key_jumps, aux_next_ptrs_reg, !assume_all_keys_valid);
	}
	return FinishRegularHashJoinChainedOutput(compiler, function, error, plan, config, control, hash_jumps, key_jumps,
	                                          aux_next_ptrs_reg, !assume_all_keys_valid);
}

} // namespace duckdb
