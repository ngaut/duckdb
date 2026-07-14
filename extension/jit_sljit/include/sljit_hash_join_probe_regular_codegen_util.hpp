//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_regular_codegen_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_util.hpp"
#include "sljit_hash_join_probe_key_codegen.hpp"
#include "sljit_hash_join_probe_regular_input_codegen.hpp"
#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static inline void EmitFinishRegularHashJoinProbe(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, input_offset), SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, resume_row_pointer), SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, finished), SLJIT_IMM, 1);
}

static inline void EmitPauseRegularHashJoinProbe(struct sljit_compiler *compiler, sljit_s32 resume_row_pointer) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, input_offset), SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, resume_row_pointer), resume_row_pointer, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, finished), SLJIT_IMM, 0);
}

static inline unique_ptr<ExecutionRegionCodeHandle>
FinishSljitRegularHashJoinProbeCode(struct sljit_compiler *compiler, SljitNativeRegularHashJoinProbeFunction &function,
                                    string &error, sljit_s32 selected_count_src, sljit_sw selected_count_srcw = 0) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, selected_count), selected_count_src,
	               selected_count_srcw);
	EmitFinishRegularHashJoinProbe(compiler);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

static inline void EmitAbortHashJoinProbeWithCastError(struct sljit_compiler *compiler, sljit_s32 value_reg) {
	EmitCallHashJoinInt64ToInt32CastError(compiler, offsetof(SljitNativeRegularHashJoinProbeInput, error), value_reg);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, selected_count), SLJIT_S3, 0);
	EmitPauseRegularHashJoinProbe(compiler, SLJIT_IMM);
	sljit_emit_return_void(compiler);
}

static inline void EmitCheckedHashJoinInt64ToInt32Range(struct sljit_compiler *compiler, sljit_s32 value_reg,
                                                        sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, source_key0_int64_to_int32_unchecked));
	auto unchecked = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	auto below_range = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, value_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(NumericLimits<int32_t>::Minimum()));
	auto above_range = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, value_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(NumericLimits<int32_t>::Maximum()));
	auto in_range = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto range_error = sljit_emit_label(compiler);
	sljit_set_label(below_range, range_error);
	sljit_set_label(above_range, range_error);
	EmitAbortHashJoinProbeWithCastError(compiler, value_reg);

	auto done = sljit_emit_label(compiler);
	sljit_set_label(unchecked, done);
	sljit_set_label(in_range, done);
}

static inline void EmitLoadHashJoinSourceKey(struct sljit_compiler *compiler, idx_t key_idx,
                                             SljitNativeHashJoinKeyKind kind, sljit_s32 target, sljit_s32 base,
                                             sljit_s32 index, sljit_sw offset, sljit_s32 scratch,
                                             bool check_int64_to_int32_range = true) {
	if (key_idx != 0 || kind != SljitNativeHashJoinKeyKind::INT32) {
		EmitLoadHashJoinKey(compiler, kind, target, base, index, offset);
		return;
	}

	D_ASSERT(target != base);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, source_key0_int64_to_int32));
	auto use_int32_source = sljit_emit_cmp(compiler, SLJIT_EQUAL, target, 0, SLJIT_IMM, 0);
	if (index == SLJIT_IMM) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM1(base), offset);
	} else if (offset == 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM2(base, index), 3);
	} else {
		sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, index, 0, SLJIT_IMM, 3);
		sljit_emit_op2(compiler, SLJIT_ADD, scratch, 0, scratch, 0, SLJIT_IMM, offset);
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM2(base, scratch), 0);
	}
	if (check_int64_to_int32_range) {
		EmitCheckedHashJoinInt64ToInt32Range(compiler, target, scratch);
	}
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(use_int32_source, sljit_emit_label(compiler));
	EmitLoadHashJoinKey(compiler, kind, target, base, index, offset);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static inline void EmitHashJoinKeyHashFromSourceData(struct sljit_compiler *compiler, idx_t key_idx,
                                                     SljitNativeHashJoinKeyKind key_kind, sljit_s32 hash_reg,
                                                     sljit_s32 source_data, sljit_s32 source_index, sljit_s32 scratch,
                                                     sljit_s32 multiplier_reg = 0) {
	if (!SljitHashJoinKeyKindIs128(key_kind)) {
		EmitLoadHashJoinSourceKey(compiler, key_idx, key_kind, hash_reg, source_data, source_index, 0, scratch);
		EmitHashJoinKeyHash(compiler, key_kind, hash_reg, scratch, multiplier_reg);
		return;
	}

	EmitLoadHashJoinKeyWord(compiler, hash_reg, source_data, source_index, offsetof(hugeint_t, lower), scratch);
	EmitDuckDBMurmurHash64(compiler, hash_reg, scratch, multiplier_reg);
	EmitLoadHashJoinKeyWord(compiler, scratch, source_data, source_index, offsetof(hugeint_t, upper), scratch);
	// Keep the hoisted source pointer live across rows. Using it as the hash scratch register corrupts the
	// saved source-data register and turns the next probe into an invalid load.
	EmitDuckDBMurmurHash64(compiler, scratch, SLJIT_TMP_R0, multiplier_reg);
	sljit_emit_op2(compiler, SLJIT_XOR, hash_reg, 0, hash_reg, 0, scratch, 0);
}

static inline void EmitHashJoinEqualityKeyMismatch(struct sljit_compiler *compiler, idx_t key_idx,
                                                   SljitNativeHashJoinKeyKind key_kind, sljit_sw key_layout_offset,
                                                   vector<struct sljit_jump *> &equality_key_mismatches,
                                                   const vector<sljit_s32> &source_data_regs,
                                                   sljit_s32 source_index_reg) {
	auto source_data_reg = EmitPrepareRegularHashJoinSourceData(compiler, key_idx, SLJIT_R4, source_data_regs);
	if (!SljitHashJoinKeyKindIs128(key_kind)) {
		EmitLoadHashJoinSourceKey(compiler, key_idx, key_kind, SLJIT_R2, source_data_reg, source_index_reg, 0, SLJIT_R3,
		                          false);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, key_layout_offset);
		EmitLoadHashJoinKey(compiler, key_kind, SLJIT_R4, SLJIT_R4, SLJIT_IMM, 0);
		equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));
		return;
	}

	EmitLoadHashJoinKeyWord(compiler, SLJIT_R2, source_data_reg, source_index_reg, offsetof(hugeint_t, lower),
	                        SLJIT_R2);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R4, SLJIT_R0, SLJIT_IMM,
	                        key_layout_offset + NumericCast<sljit_sw>(offsetof(hugeint_t, lower)), SLJIT_R4);
	equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));

	source_data_reg = EmitPrepareRegularHashJoinSourceData(compiler, key_idx, SLJIT_R4, source_data_regs);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R2, source_data_reg, source_index_reg, offsetof(hugeint_t, upper),
	                        SLJIT_R2);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R4, SLJIT_R0, SLJIT_IMM,
	                        key_layout_offset + NumericCast<sljit_sw>(offsetof(hugeint_t, upper)), SLJIT_R4);
	equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));
}

} // namespace duckdb
