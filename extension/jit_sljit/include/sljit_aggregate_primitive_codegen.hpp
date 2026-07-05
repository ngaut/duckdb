//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_primitive_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_internal.hpp"

namespace duckdb {

void EmitSljitGroupedAggregateStatePointer(struct sljit_compiler *compiler, sljit_s32 logical_index,
                                           sljit_s32 target);

template <class EMIT_ROW>
static inline sljit_jump *EmitSljitAggregateSelectedSourceLoop(struct sljit_compiler *compiler, EMIT_ROW &&emit_row) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	emit_row();
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitNextSljitNativeVectorLoop(compiler, loop);

	return done;
}

template <class EMIT_ROW>
static inline sljit_jump *EmitSljitAggregateTwoSourceLoop(struct sljit_compiler *compiler, EMIT_ROW &&emit_row) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
	auto left_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	auto right_is_null =
	    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);

	emit_row();
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, invalid_label);
	sljit_set_label(right_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitNextSljitNativeVectorLoop(compiler, loop);

	return done;
}

template <class EMIT_ROW>
static inline sljit_jump *EmitSljitGroupedAggregateLoop(struct sljit_compiler *compiler, EMIT_ROW &&emit_row) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	emit_row();
	EmitNextSljitNativeVectorLoop(compiler, loop);

	return done;
}

template <class EMIT_ROW>
static inline sljit_jump *EmitSljitGroupedAggregateSelectedSourceLoop(struct sljit_compiler *compiler,
                                                                      EMIT_ROW &&emit_row) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	auto source_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	emit_row();
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitNextSljitNativeVectorLoop(compiler, loop);

	return done;
}

void EmitSljitAggregateAddRowCount(struct sljit_compiler *compiler, sljit_s32 count_reg);
void EmitSljitAggregateIncrementLocalCount(struct sljit_compiler *compiler, sljit_sw local_count_offset);

void EmitSljitGroupedAggregateSetStateIsSet(struct sljit_compiler *compiler, sljit_s32 state_reg);
void EmitSljitGroupedAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                              sljit_s32 value_reg);
void EmitSljitGroupedAggregateIncrementInt64(struct sljit_compiler *compiler, sljit_s32 state_reg);
void EmitSljitGroupedAggregateAccumulateHugeintInt64(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                                     sljit_s32 value_reg);
void EmitSljitGroupedAggregateAccumulateDouble(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                               sljit_s32 value_freg);
void EmitLoadGroupedAggregateStateAddress(struct sljit_compiler *compiler, sljit_s32 target_reg,
                                          sljit_s32 logical_index_reg);
void EmitSljitGroupedAggregateValuePointerImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                    idx_t state_offset, idx_t value_offset);
void EmitSljitGroupedAggregateSetStateIsSetImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                     idx_t state_offset, idx_t state_is_set_offset);
void EmitSljitGroupedAggregateAccumulateInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                       idx_t state_offset, idx_t value_offset,
                                                       idx_t state_is_set_offset, sljit_s32 value_reg);
void EmitSljitGroupedAggregateAccumulateInt64ImmediateNoStateSet(struct sljit_compiler *compiler,
                                                                 sljit_s32 base_reg, idx_t state_offset,
                                                                 idx_t value_offset, sljit_s32 value_reg);
void EmitSljitGroupedAggregateIncrementInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                      idx_t state_offset, idx_t value_offset);
void EmitSljitGroupedAggregateIncrementInt64ImmediateDirect(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                            idx_t state_offset, idx_t value_offset);
void EmitSljitAccumulateHugeintUpperIfNeeded(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                             sljit_sw upper_offset, sljit_s32 upper_value_reg, sljit_s32 carry_reg);
void EmitSljitGroupedAggregateAccumulateHugeintImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                         idx_t state_offset, idx_t value_offset,
                                                         idx_t state_is_set_offset, sljit_s32 value_reg);
void EmitSljitGroupedAggregateAccumulateHugeintImmediateNoStateSet(struct sljit_compiler *compiler,
                                                                   sljit_s32 base_reg, idx_t state_offset,
                                                                   idx_t value_offset, sljit_s32 value_reg);
void EmitSljitGroupedAggregateAccumulateDoubleImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                        idx_t state_offset, idx_t value_offset,
                                                        idx_t state_is_set_offset, sljit_s32 value_freg);

void EmitSljitStoreZeroDoubleLocal(struct sljit_compiler *compiler, sljit_sw local_sum_offset);
void EmitSljitAggregateAccumulateDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                        sljit_sw saw_value_offset, sljit_s32 value_freg);
void EmitSljitAggregateCommitDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                    sljit_sw saw_value_offset);

} // namespace duckdb
