//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_regular_input_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_probe_codegen.hpp"

#include "duckdb/common/numeric_utils.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static inline void EmitLoadRegularHashJoinSourceIndex(struct sljit_compiler *compiler, idx_t key_idx,
                                                      sljit_s32 target, sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, source_sel));
	auto no_sel_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(scratch),
	               NumericCast<sljit_sw>(key_idx * sizeof(sel_t *)));
	auto no_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(scratch, SLJIT_S1), 2);
	auto have_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_sel_array, use_logical_index);
	sljit_set_label(no_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_index, sljit_emit_label(compiler));
}

static inline struct sljit_jump *EmitJumpIfRegularHashJoinSourceNull(struct sljit_compiler *compiler, idx_t key_idx,
                                                                     sljit_s32 source_index, sljit_s32 scratch,
                                                                     sljit_s32 scratch2,
                                                                     bool assume_source_all_valid = false) {
	if (assume_source_all_valid) {
		return nullptr;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, source_validity));
	auto source_all_valid_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(scratch),
	               NumericCast<sljit_sw>(key_idx * sizeof(validity_t *)));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch2, 0, source_index, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch2, 0, SLJIT_MEM2(scratch, scratch2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, scratch, 0, source_index, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, SLJIT_IMM, 1, scratch, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, scratch, 0, scratch, 0, scratch2, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto all_valid = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid_array, all_valid);
	sljit_set_label(source_all_valid, all_valid);
	return source_is_null;
}

static inline struct sljit_jump *EmitJumpIfHashJoinRhsKeyNull(struct sljit_compiler *compiler, idx_t key_idx,
                                                              sljit_s32 row_pointer, sljit_s32 scratch,
                                                              sljit_s32 scratch2,
                                                              bool assume_rhs_all_valid = false) {
	if (assume_rhs_all_valid) {
		return nullptr;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, rhs_keys_have_validity));
	auto rhs_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(row_pointer), NumericCast<sljit_sw>(key_idx / 8));
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, scratch2, 0, scratch, 0, SLJIT_IMM, 1ULL << (key_idx % 8));
	auto rhs_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(rhs_all_valid, sljit_emit_label(compiler));
	return rhs_is_null;
}

static inline void EmitLoadRegularHashJoinSourceData(struct sljit_compiler *compiler, idx_t key_idx,
                                                     sljit_s32 target,
                                                     const vector<sljit_s32> &source_data_regs) {
	if (key_idx < source_data_regs.size() && source_data_regs[key_idx] != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, source_data_regs[key_idx], 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeRegularHashJoinProbeInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(target),
	               NumericCast<sljit_sw>(key_idx * sizeof(const_data_ptr_t)));
}

static inline sljit_s32 EmitPrepareRegularHashJoinSourceData(struct sljit_compiler *compiler, idx_t key_idx,
                                                             sljit_s32 target,
                                                             const vector<sljit_s32> &source_data_regs) {
	if (key_idx < source_data_regs.size() && source_data_regs[key_idx] != 0) {
		return source_data_regs[key_idx];
	}
	EmitLoadRegularHashJoinSourceData(compiler, key_idx, target, source_data_regs);
	return target;
}

} // namespace duckdb
