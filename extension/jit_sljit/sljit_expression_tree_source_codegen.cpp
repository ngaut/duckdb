#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "sljitLir.h"

namespace duckdb {

void EmitLoadSljitExpressionTreeLogicalIndex(struct sljit_compiler *compiler) {
	EmitLoadLogicalIndex(compiler, SLJIT_S3);
}

void EmitLoadSljitExpressionTreeSourceIndex(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target) {
	// SLJIT_S4 holds the loop-invariant source_sel_array base.
	auto no_source_sel_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S4, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S4),
	               NumericCast<sljit_sw>(source_index * sizeof(const sel_t *)));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto use_common_source_sel = sljit_emit_label(compiler);
	sljit_set_label(no_source_sel_array, use_common_source_sel);
	sljit_set_label(no_source_sel, use_common_source_sel);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_common_sel));
	auto no_common_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_common_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_common_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S3, 0);
	auto done = sljit_emit_label(compiler);
	sljit_set_label(have_source_index, done);
	sljit_set_label(have_common_source_index, done);
}

sljit_jump *EmitJumpIfSljitExpressionTreeSourceNull(struct sljit_compiler *compiler, idx_t source_index) {
	EmitLoadSljitNativeSourceValidity(compiler, source_index, SLJIT_R0);
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_R0, 0);
	EmitLoadSljitExpressionTreeSourceIndex(compiler, source_index, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_R1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

sljit_jump *EmitJumpIfSljitExpressionTreeFlatSourceNull(struct sljit_compiler *compiler, idx_t source_index) {
	EmitLoadSljitNativeSourceValidity(compiler, source_index, SLJIT_R0);
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

} // namespace duckdb
