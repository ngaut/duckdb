#pragma once

#include "sljit_codegen_internal.hpp"

#include "duckdb/common/common.hpp"

namespace duckdb {

static inline bool IsSljitNativeFixedByteStorageSize(idx_t storage_size) {
	switch (storage_size) {
	case 1:
	case 2:
	case 4:
	case 8:
	case 16:
		return true;
	default:
		return false;
	}
}

static inline void EmitLoadSljitNativeFixedWidthValue(struct sljit_compiler *compiler, sljit_sw data_offset,
                                                      sljit_s32 index_reg, sljit_s32 load_op, sljit_sw data_scale,
                                                      sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), data_offset);
	sljit_emit_op1(compiler, load_op, target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg), data_scale);
}

static inline void EmitStoreSljitNativeFixedWidthResult(struct sljit_compiler *compiler, sljit_s32 store_op,
                                                        sljit_sw data_scale, sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale, value_reg, 0);
}

static inline void EmitCopySljitNativeFixedWidthSourceToResult(struct sljit_compiler *compiler, idx_t value_size,
                                                               sljit_s32 source_index_reg) {
	sljit_s32 move_op;
	sljit_sw data_scale;
	switch (value_size) {
	case 1:
		move_op = SLJIT_MOV_U8;
		data_scale = 0;
		break;
	case 2:
		move_op = SLJIT_MOV_U16;
		data_scale = 1;
		break;
	case 4:
		move_op = SLJIT_MOV_U32;
		data_scale = 2;
		break;
	case 8:
		move_op = SLJIT_MOV;
		data_scale = 3;
		break;
	case 16:
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_data));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data));
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, source_index_reg, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), 8);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R3), 8, SLJIT_R2, 0);
		return;
	default:
		D_ASSERT(false);
		return;
	}
	EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), source_index_reg,
	                                   move_op, data_scale, SLJIT_R2);
	EmitStoreSljitNativeFixedWidthResult(compiler, move_op, data_scale, SLJIT_R2);
}

template <class EMIT_ROW>
static inline sljit_jump *EmitSljitInvalidResultLoop(struct sljit_compiler *compiler, EMIT_ROW &&emit_row) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	vector<sljit_jump *> invalid_jumps;
	vector<sljit_jump *> next_jumps;
	emit_row(invalid_jumps, next_jumps);
	next_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));

	auto invalid_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(invalid_jumps, invalid_label);
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(next_jumps, next_label);
	EmitNextSljitNativeVectorLoop(compiler, loop);

	return done;
}

template <class EMIT_VALID_ROW>
static inline sljit_jump *EmitSljitSelectedSourceInvalidResultBranchLoop(struct sljit_compiler *compiler,
                                                                         EMIT_VALID_ROW &&emit_valid_row) {
	return EmitSljitInvalidResultLoop(compiler,
	                                  [&](vector<sljit_jump *> &invalid_jumps, vector<sljit_jump *> &next_jumps) {
		                                  EmitLoadSelectedIndex(compiler);
		                                  invalid_jumps.push_back(EmitSkipInvalidSourceRow(compiler));
		                                  emit_valid_row(invalid_jumps, next_jumps);
	                                  });
}

template <class EMIT_VALID_ROW>
static inline sljit_jump *EmitSljitSelectedSourceInvalidResultLoop(struct sljit_compiler *compiler,
                                                                   EMIT_VALID_ROW &&emit_valid_row) {
	return EmitSljitSelectedSourceInvalidResultBranchLoop(
	    compiler, [&](vector<sljit_jump *> &invalid_jumps, vector<sljit_jump *> &) { emit_valid_row(invalid_jumps); });
}

template <class EMIT_VALID_ROW>
static inline sljit_jump *EmitSljitTwoSourceInvalidResultBranchLoop(struct sljit_compiler *compiler,
                                                                    EMIT_VALID_ROW &&emit_valid_row) {
	return EmitSljitInvalidResultLoop(
	    compiler, [&](vector<sljit_jump *> &invalid_jumps, vector<sljit_jump *> &next_jumps) {
		    EmitLoadLogicalIndex(compiler, SLJIT_R1);
		    EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
		    EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
		    invalid_jumps.push_back(
		        EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3));
		    invalid_jumps.push_back(
		        EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4));
		    emit_valid_row(invalid_jumps, next_jumps);
	    });
}

template <class EMIT_VALID_ROW>
static inline sljit_jump *EmitSljitTwoSourceInvalidResultLoop(struct sljit_compiler *compiler,
                                                              EMIT_VALID_ROW &&emit_valid_row) {
	return EmitSljitTwoSourceInvalidResultBranchLoop(
	    compiler, [&](vector<sljit_jump *> &invalid_jumps, vector<sljit_jump *> &) { emit_valid_row(invalid_jumps); });
}

} // namespace duckdb
