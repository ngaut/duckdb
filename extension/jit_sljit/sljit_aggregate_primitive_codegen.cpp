#include "sljit_aggregate_primitive_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"

#include "sljitLir.h"

#include <exception>

namespace duckdb {

static void SljitNativeAggregateTreeOverflow(SljitNativeVectorInput *input, const char *message) {
	try {
		throw OutOfRangeException("%s", message);
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

static void SLJIT_FUNC SljitNativeAggregateTreeAddOverflow(SljitNativeVectorInput *input) {
	SljitNativeAggregateTreeOverflow(input, "Overflow in addition of DECIMAL");
}

static void SLJIT_FUNC SljitNativeAggregateTreeSubtractOverflow(SljitNativeVectorInput *input) {
	SljitNativeAggregateTreeOverflow(input, "Overflow in subtract of DECIMAL");
}

static void SLJIT_FUNC SljitNativeAggregateTreeMultiplyOverflow(SljitNativeVectorInput *input) {
	SljitNativeAggregateTreeOverflow(input, "Overflow in multiplication of DECIMAL");
}

void SLJIT_FUNC SljitNativeAggregateHugeintCommit(SljitNativeVectorInput *input) {
	try {
		if (!input->aggregate_hugeint_value || !input->aggregate_state_is_set) {
			throw InternalException("SLJIT hugeint aggregate primitive state is incomplete");
		}
		*input->aggregate_hugeint_value = Hugeint::Add(*input->aggregate_hugeint_value, input->aggregate_local_hugeint);
		*input->aggregate_state_is_set = true;
	} catch (...) {
		input->has_error = true;
		input->error = std::current_exception();
	}
}

void EmitSljitAggregateExpressionTreeOverflowCall(struct sljit_compiler *compiler, SljitNativeIntegerBinaryOp op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	switch (op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeAggregateTreeAddOverflow));
		return;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeAggregateTreeSubtractOverflow));
		return;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeAggregateTreeMultiplyOverflow));
		return;
	default:
		throw InternalException("Unknown SLJIT aggregate expression-tree overflow operator");
	}
}

void EmitSljitAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                       sljit_sw saw_value_offset, sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

void EmitSljitAggregateAddRowCount(struct sljit_compiler *compiler, sljit_s32 count_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, count_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));
}

void EmitSljitAggregateCommitInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                   sljit_sw saw_value_offset) {
	EmitSljitAggregateAddRowCount(compiler, SLJIT_S2);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_int64_value));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

void EmitSljitAggregateAccumulateHugeintInt64(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                              sljit_sw local_upper_offset, sljit_sw saw_value_offset,
                                              sljit_s32 value_reg) {
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_lower_offset);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_lower_offset, SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_CARRY);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_upper_offset, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

void EmitSljitAggregateAccumulateHugeint(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                         sljit_sw local_upper_offset, sljit_sw saw_value_offset,
                                         sljit_s32 lower_value_reg, sljit_s32 upper_value_reg) {
	EmitSljitAccumulateHugeintWords(compiler, SLJIT_SP, local_lower_offset, local_upper_offset, lower_value_reg,
	                                upper_value_reg);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

void EmitLoadSljitHugeintReference(struct sljit_compiler *compiler, sljit_sw source_data_offset,
                                   sljit_s32 source_index_reg, sljit_s32 lower_target_reg, sljit_s32 upper_target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_data_offset);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, source_index_reg, 0, SLJIT_IMM, 4);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, lower_target_reg, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op1(compiler, SLJIT_MOV, upper_target_reg, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper));
}

void EmitSljitAccumulateHugeintWords(struct sljit_compiler *compiler, sljit_s32 base_reg, sljit_sw lower_offset,
                                     sljit_sw upper_offset, sljit_s32 lower_value_reg, sljit_s32 upper_value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(base_reg), lower_offset);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R4, 0, SLJIT_R4, 0, lower_value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), lower_offset, SLJIT_R4, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, lower_value_reg, 0, SLJIT_CARRY);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(base_reg), upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, upper_value_reg, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, lower_value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), upper_offset, SLJIT_R4, 0);
}

void EmitSljitAggregateCommitHugeint(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                     sljit_sw local_upper_offset, sljit_sw saw_value_offset) {
	EmitSljitAggregateAddRowCount(compiler, SLJIT_S2);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, lower), SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_upper_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, upper), SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
	                 SLJIT_FUNC_ADDR(SljitNativeAggregateHugeintCommit));
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

void EmitSljitAggregateAccumulateSumState(struct sljit_compiler *compiler, SljitNativeAggregateSumStateKind state_kind,
                                          sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                          sljit_sw saw_value_offset, sljit_s32 value_reg) {
	if (state_kind == SljitNativeAggregateSumStateKind::HUGEINT) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_lower_offset, local_upper_offset, saw_value_offset,
		                                         value_reg);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_lower_offset, saw_value_offset, value_reg);
	}
}

void EmitSljitAggregateCommitSumState(struct sljit_compiler *compiler, SljitNativeAggregateSumStateKind state_kind,
                                      sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                      sljit_sw saw_value_offset) {
	if (state_kind == SljitNativeAggregateSumStateKind::HUGEINT) {
		EmitSljitAggregateCommitHugeint(compiler, local_lower_offset, local_upper_offset, saw_value_offset);
	} else {
		EmitSljitAggregateCommitInt64(compiler, local_lower_offset, saw_value_offset);
	}
}

void EmitSljitAggregateIncrementLocalCount(struct sljit_compiler *compiler, sljit_sw local_count_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), local_count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_count_offset, SLJIT_R0, 0);
}

void EmitSljitStoreZeroDoubleLocal(struct sljit_compiler *compiler, sljit_sw local_sum_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (sizeof(double) > sizeof(sljit_sw)) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset + sizeof(sljit_sw), SLJIT_IMM, 0);
	}
}

void EmitSljitAggregateAccumulateDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                        sljit_sw saw_value_offset, sljit_s32 value_freg) {
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_SP),
	                local_sum_offset);
	sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR1, 0, value_freg, 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1,
	                SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

void EmitSljitAggregateCommitDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                    sljit_sw saw_value_offset) {
	EmitSljitAggregateAddRowCount(compiler, SLJIT_S2);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_double_value));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_SP),
	                local_sum_offset);
	sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0,
	                SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

} // namespace duckdb
