//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_probe_perfect_input_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_join_probe_codegen.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static inline void EmitLoadPerfectHashJoinSourceIndex(struct sljit_compiler *compiler, sljit_s32 target,
                                                      sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, source_sel));
	auto no_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(scratch, SLJIT_S1), 2);
	auto have_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_index, sljit_emit_label(compiler));
}

static inline struct sljit_jump *EmitJumpIfPerfectHashJoinSourceNull(struct sljit_compiler *compiler,
                                                                     sljit_s32 source_index, sljit_s32 scratch,
                                                                     sljit_s32 scratch2) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, source_validity));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch2, 0, source_index, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch2, 0, SLJIT_MEM2(scratch, scratch2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, scratch, 0, source_index, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, SLJIT_IMM, 1, scratch, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, scratch, 0, scratch, 0, scratch2, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

} // namespace duckdb
