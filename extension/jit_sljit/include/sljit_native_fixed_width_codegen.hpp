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

//! Jumps when the input pointer at offset is set
static inline void EmitSljitJumpIfInputPointerSet(struct sljit_compiler *compiler, sljit_sw offset,
                                                  vector<sljit_jump *> &jumps) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), offset);
	jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
}

//! Emits the row loop twice: a vector without a selection whose sources are flat and free of NULLs reads row i at
//! source index i and runs the row body alone; any other vector runs the loop that resolves each row's indexes and
//! NULLs. The flat loop exits into the head of the other loop, which then exits at once, so one exit jump serves both.
//! The row body is emitted once per loop, so it must collect every jump it leaves open into a vector.
template <class EMIT_ROW>
static inline sljit_jump *EmitSljitFlatVersionedLoop(struct sljit_compiler *compiler,
                                                     std::initializer_list<sljit_sw> resolve_offsets,
                                                     EMIT_ROW &&emit_row) {
	vector<sljit_jump *> resolve_jumps;
	for (auto offset : resolve_offsets) {
		EmitSljitJumpIfInputPointerSet(compiler, offset, resolve_jumps);
	}
	auto flat_done = emit_row(true);
	auto resolve_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(resolve_jumps, resolve_label);
	sljit_set_label(flat_done, resolve_label);
	return emit_row(false);
}

//! How a row loop finds a row's source index
enum class SljitRowIndexMode : uint8_t {
	//! no selection, flat sources free of NULLs: row i reads index i
	FLAT,
	//! a selection over flat sources free of NULLs: row i reads the index the selection gives
	SELECTED,
	//! anything else: each row resolves its indexes and NULLs
	RESOLVED
};

//! Emits the row loop once per SljitRowIndexMode, the one the vector's shape allows chosen up front. Each loop exits
//! into the head of the next, which then exits at once, so one exit jump serves all three. The row body is emitted once
//! per loop, so it must collect every jump it leaves open into a vector.
template <class EMIT_ROW>
static inline sljit_jump *EmitSljitRowIndexVersionedLoop(struct sljit_compiler *compiler,
                                                         std::initializer_list<sljit_sw> source_offsets,
                                                         EMIT_ROW &&emit_row) {
	vector<sljit_jump *> resolve_jumps;
	for (auto offset : source_offsets) {
		EmitSljitJumpIfInputPointerSet(compiler, offset, resolve_jumps);
	}
	vector<sljit_jump *> selected_jumps;
	EmitSljitJumpIfInputPointerSet(compiler, offsetof(SljitNativeVectorInput, execute_sel), selected_jumps);
	auto flat_done = emit_row(SljitRowIndexMode::FLAT);
	auto selected_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(selected_jumps, selected_label);
	sljit_set_label(flat_done, selected_label);
	auto selected_done = emit_row(SljitRowIndexMode::SELECTED);
	auto resolve_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(resolve_jumps, resolve_label);
	sljit_set_label(selected_done, resolve_label);
	return emit_row(SljitRowIndexMode::RESOLVED);
}

template <class EMIT_VALID_ROW>
static inline sljit_jump *EmitSljitSelectedSourceInvalidResultBranchLoop(struct sljit_compiler *compiler,
                                                                         EMIT_VALID_ROW &&emit_valid_row) {
	return EmitSljitRowIndexVersionedLoop(
	    compiler, {offsetof(SljitNativeVectorInput, source_sel), offsetof(SljitNativeVectorInput, source_validity)},
	    [&](SljitRowIndexMode mode) {
		    return EmitSljitInvalidResultLoop(
		        compiler, [&](vector<sljit_jump *> &invalid_jumps, vector<sljit_jump *> &next_jumps) {
			        switch (mode) {
			        case SljitRowIndexMode::FLAT:
				        sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_S1, 0);
				        break;
			        case SljitRowIndexMode::SELECTED:
				        sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
				                       offsetof(SljitNativeVectorInput, execute_sel));
				        sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
				        break;
			        case SljitRowIndexMode::RESOLVED:
				        EmitLoadSelectedIndex(compiler);
				        invalid_jumps.push_back(EmitSkipInvalidSourceRow(compiler));
				        break;
			        }
			        emit_valid_row(invalid_jumps, next_jumps);
		        });
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
	return EmitSljitFlatVersionedLoop(
	    compiler,
	    {offsetof(SljitNativeVectorInput, execute_sel), offsetof(SljitNativeVectorInput, source_sel),
	     offsetof(SljitNativeVectorInput, right_source_sel), offsetof(SljitNativeVectorInput, source_validity),
	     offsetof(SljitNativeVectorInput, right_source_validity)},
	    [&](bool flat) {
		    return EmitSljitInvalidResultLoop(compiler, [&](vector<sljit_jump *> &invalid_jumps,
		                                                    vector<sljit_jump *> &next_jumps) {
			    if (flat) {
				    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
				    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_S1, 0);
			    } else {
				    EmitLoadLogicalIndex(compiler, SLJIT_R1);
				    EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
				    EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1,
				                        SLJIT_S4);
				    invalid_jumps.push_back(
				        EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3));
				    invalid_jumps.push_back(EmitJumpIfValidityNull(
				        compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4));
			    }
			    emit_valid_row(invalid_jumps, next_jumps);
		    });
	    });
}

template <class EMIT_VALID_ROW>
static inline sljit_jump *EmitSljitTwoSourceInvalidResultLoop(struct sljit_compiler *compiler,
                                                              EMIT_VALID_ROW &&emit_valid_row) {
	return EmitSljitTwoSourceInvalidResultBranchLoop(
	    compiler, [&](vector<sljit_jump *> &invalid_jumps, vector<sljit_jump *> &) { emit_valid_row(invalid_jumps); });
}

} // namespace duckdb
