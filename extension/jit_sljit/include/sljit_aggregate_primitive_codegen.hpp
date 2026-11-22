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

void EmitSljitGroupedAggregateStatePointer(struct sljit_compiler *compiler, sljit_s32 logical_index, sljit_s32 target);

//! Scratch registers the all-valid selected-source loop keeps its selections and source data in; a kernel using it
//! enters with SLJIT_ALL_VALID_SOURCE_LOOP_SCRATCHES scratch registers.
static constexpr sljit_s32 SLJIT_ALL_VALID_SOURCE_LOOP_SCRATCHES = 8;

//! The selected-source loop for a source without a validity mask: no row is skipped, and the execute and source
//! selections the input has are chosen once, before the rows, so the loop body is straight-line code with one branch
//! per row. emit_row receives the source row index in R1 and the data pointer in R7; it is emitted once for each
//! combination of selections. A source with a validity mask jumps to the returned jump instead; the loops fall through
//! when they are done.
template <class EMIT_ROW>
static inline sljit_jump *EmitSljitAggregateAllValidSelectedSourceLoop(struct sljit_compiler *compiler,
                                                                       EMIT_ROW &&emit_row) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity));
	auto generic = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R6, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R7, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));

	vector<sljit_jump *> done_jumps;
	auto emit_loop = [&](bool execute_sel, bool source_sel) {
		done_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0));
		auto loop = sljit_emit_label(compiler);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_S1, 0);
		if (execute_sel) {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R5, SLJIT_R1), 2);
		}
		if (source_sel) {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R6, SLJIT_R1), 2);
		}
		emit_row();
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		sljit_set_label(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S1, 0, SLJIT_S2, 0), loop);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	};
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R5, 0, SLJIT_IMM, 0);
	auto execute_sel_only = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R6, 0, SLJIT_IMM, 0);
	emit_loop(true, true);
	sljit_set_label(execute_sel_only, sljit_emit_label(compiler));
	emit_loop(true, false);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	auto no_selection = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R6, 0, SLJIT_IMM, 0);
	emit_loop(false, true);
	sljit_set_label(no_selection, sljit_emit_label(compiler));
	emit_loop(false, false);
	auto done = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done);
	}
	return generic;
}

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
void EmitSljitGroupedAggregateAccumulateHugeint(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                                sljit_s32 lower_value_reg, sljit_s32 upper_value_reg);
void EmitSljitGroupedAggregateAccumulateDouble(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                               sljit_s32 value_freg);
void EmitLoadGroupedAggregateStateAddress(struct sljit_compiler *compiler, sljit_s32 target_reg,
                                          sljit_s32 logical_index_reg);
void EmitLoadGroupedAggregateStateAddressByLoopIndex(struct sljit_compiler *compiler, sljit_s32 target_reg);
void EmitSljitGroupedAggregateValuePointerImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                    idx_t state_offset, idx_t value_offset);
void EmitSljitGroupedAggregateSetStateIsSetImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                     idx_t state_offset, idx_t state_is_set_offset);
void EmitSljitGroupedAggregateAccumulateInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                       idx_t state_offset, idx_t value_offset,
                                                       idx_t state_is_set_offset, sljit_s32 value_reg);
void EmitSljitGroupedAggregateAccumulateInt64ImmediateNoStateSet(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                                 idx_t state_offset, idx_t value_offset,
                                                                 sljit_s32 value_reg);
void EmitSljitGroupedAggregateIncrementInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                      idx_t state_offset, idx_t value_offset);
void EmitSljitAccumulateHugeintUpperIfNeeded(struct sljit_compiler *compiler, sljit_s32 base_reg, sljit_sw upper_offset,
                                             sljit_s32 upper_value_reg, sljit_s32 carry_reg);
void EmitSljitGroupedAggregateAccumulateHugeintImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                         idx_t state_offset, idx_t value_offset,
                                                         idx_t state_is_set_offset, sljit_s32 value_reg);
void EmitSljitGroupedAggregateAccumulateHugeintValueImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                              idx_t state_offset, idx_t value_offset,
                                                              idx_t state_is_set_offset, sljit_s32 lower_value_reg,
                                                              sljit_s32 upper_value_reg);
void EmitSljitGroupedAggregateAccumulateHugeintImmediateNoStateSet(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                                   idx_t state_offset, idx_t value_offset,
                                                                   sljit_s32 value_reg);
void EmitSljitGroupedAggregateAccumulateDoubleImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                        idx_t state_offset, idx_t value_offset,
                                                        idx_t state_is_set_offset, sljit_s32 value_freg);
//! Adds the value in value_reg (a DOUBLE's bits for a DOUBLE sum) to a SUM state at base_reg + state_offset,
//! marking it set unless the loop marks states on first touch (which DOUBLE sums never use)
void EmitSljitGroupedAggregateAccumulateSumImmediate(struct sljit_compiler *compiler, AggregatePrimitiveUpdateKind kind,
                                                     sljit_s32 base_reg, idx_t state_offset, idx_t value_offset,
                                                     idx_t state_is_set_offset, sljit_s32 value_reg,
                                                     bool states_marked_on_first_touch = false);

void EmitSljitLoadDoubleStateLocal(struct sljit_compiler *compiler, sljit_sw local_sum_offset);
void EmitLoadSljitHugeintReference(struct sljit_compiler *compiler, sljit_sw source_data_offset,
                                   sljit_s32 source_index_reg, sljit_s32 lower_target_reg, sljit_s32 upper_target_reg);
void EmitSljitAccumulateHugeintWords(struct sljit_compiler *compiler, sljit_s32 base_reg, sljit_sw lower_offset,
                                     sljit_sw upper_offset, sljit_s32 lower_value_reg, sljit_s32 upper_value_reg);
void EmitSljitAggregateAccumulateDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                        sljit_sw saw_value_offset, sljit_s32 value_freg);
//! The value an ungrouped double sum adds per row: its source, or its source combined with a constant
struct SljitUngroupedDoubleSumValue {
	SljitNativeDoubleSourceKind kind = SljitNativeDoubleSourceKind::DOUBLE;
	bool combines_constant = false;
	sljit_s32 binary_op = 0;
	bool constant_on_left = false;
};
unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumDouble(const SljitUngroupedDoubleSumValue &value,
                                                                         SljitNativeAggregateUpdateFunction &function,
                                                                         string &error);
void EmitSljitAggregateCommitDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                    sljit_sw saw_value_offset);

} // namespace duckdb
