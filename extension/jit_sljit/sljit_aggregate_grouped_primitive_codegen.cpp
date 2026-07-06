#include "sljit_aggregate_primitive_codegen.hpp"

#include "duckdb/common/types/hugeint.hpp"

#include "sljitLir.h"

namespace duckdb {

void EmitSljitGroupedAggregateStatePointer(struct sljit_compiler *compiler, sljit_s32 logical_index, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 3);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_R0, 0);
}

void EmitSljitGroupedAggregateSetStateIsSet(struct sljit_compiler *compiler, sljit_s32 state_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
}

void EmitSljitGroupedAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                              sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSet(compiler, state_reg);
}

void EmitSljitGroupedAggregateIncrementInt64(struct sljit_compiler *compiler, sljit_s32 state_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
}

void EmitSljitGroupedAggregateAccumulateHugeintInt64(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                                     sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper), SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSet(compiler, state_reg);
}

void EmitSljitGroupedAggregateAccumulateDouble(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                               sljit_s32 value_freg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR1, 0, value_freg, 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1,
	                SLJIT_MEM1(SLJIT_R0), 0);
	EmitSljitGroupedAggregateSetStateIsSet(compiler, state_reg);
}

void EmitLoadGroupedAggregateStateAddress(struct sljit_compiler *compiler, sljit_s32 target_reg,
                                          sljit_s32 logical_index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses_by_loop_index));
	auto use_logical_index = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_S1, 0);
	auto have_address_index_input = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(use_logical_index, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, logical_index_reg, 0);
	sljit_set_label(have_address_index_input, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_address_sel));
	auto no_address_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_R2, SLJIT_R1), 2);
	auto have_address_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_address_sel, sljit_emit_label(compiler));
	sljit_set_label(have_address_index, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 3);
}

void EmitLoadGroupedAggregateStateAddressByLoopIndex(struct sljit_compiler *compiler, sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 3);
}

void EmitSljitGroupedAggregateValuePointerImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                    idx_t state_offset, idx_t value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, base_reg, 0);
	const auto offset = NumericCast<sljit_sw>(state_offset + value_offset);
	if (offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, offset);
	}
}

void EmitSljitGroupedAggregateSetStateIsSetImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                     idx_t state_offset, idx_t state_is_set_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, base_reg, 0);
	const auto offset = NumericCast<sljit_sw>(state_offset + state_is_set_offset);
	if (offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, offset);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
}

void EmitSljitGroupedAggregateAccumulateInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                       idx_t state_offset, idx_t value_offset,
                                                       idx_t state_is_set_offset, sljit_s32 value_reg) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, base_reg, state_offset, state_is_set_offset);
}

void EmitSljitGroupedAggregateAccumulateInt64ImmediateNoStateSet(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                                 idx_t state_offset, idx_t value_offset,
                                                                 sljit_s32 value_reg) {
	const auto offset = NumericCast<sljit_sw>(state_offset + value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(base_reg), offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), offset, SLJIT_R3, 0);
}

void EmitSljitGroupedAggregateIncrementInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                      idx_t state_offset, idx_t value_offset) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
}

void EmitSljitGroupedAggregateIncrementInt64ImmediateDirect(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                            idx_t state_offset, idx_t value_offset) {
	const auto offset = NumericCast<sljit_sw>(state_offset + value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(base_reg), offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), offset, SLJIT_R3, 0);
}

void EmitSljitAccumulateHugeintUpperIfNeeded(struct sljit_compiler *compiler, sljit_s32 base_reg, sljit_sw upper_offset,
                                             sljit_s32 upper_value_reg, sljit_s32 carry_reg) {
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R2, 0, upper_value_reg, 0, carry_reg, 0);
	auto no_upper_update = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(base_reg), upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, upper_value_reg, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, carry_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), upper_offset, SLJIT_R3, 0);
	sljit_set_label(no_upper_update, sljit_emit_label(compiler));
}

void EmitSljitGroupedAggregateAccumulateHugeintImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                         idx_t state_offset, idx_t value_offset,
                                                         idx_t state_is_set_offset, sljit_s32 value_reg) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	EmitSljitAccumulateHugeintUpperIfNeeded(compiler, SLJIT_R0, offsetof(hugeint_t, upper), SLJIT_R4, SLJIT_R1);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, base_reg, state_offset, state_is_set_offset);
}

void EmitSljitGroupedAggregateAccumulateHugeintImmediateNoStateSet(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                                   idx_t state_offset, idx_t value_offset,
                                                                   sljit_s32 value_reg) {
	const auto lower_offset = NumericCast<sljit_sw>(state_offset + value_offset + offsetof(hugeint_t, lower));
	const auto upper_offset = NumericCast<sljit_sw>(state_offset + value_offset + offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(base_reg), lower_offset);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), lower_offset, SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	EmitSljitAccumulateHugeintUpperIfNeeded(compiler, base_reg, upper_offset, SLJIT_R4, SLJIT_R1);
}

void EmitSljitGroupedAggregateAccumulateDoubleImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                        idx_t state_offset, idx_t value_offset,
                                                        idx_t state_is_set_offset, sljit_s32 value_freg) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR1, 0, value_freg, 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1,
	                SLJIT_MEM1(SLJIT_R0), 0);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, base_reg, state_offset, state_is_set_offset);
}

} // namespace duckdb
