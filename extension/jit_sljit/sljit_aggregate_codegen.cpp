#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
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

void EmitSljitAggregateCommitInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                   sljit_sw saw_value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

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

void EmitSljitAggregateCommitHugeint(struct sljit_compiler *compiler, sljit_sw local_lower_offset,
                                     sljit_sw local_upper_offset, sljit_sw saw_value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

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

static void EmitSljitAggregateIncrementLocalCount(struct sljit_compiler *compiler, sljit_sw local_count_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), local_count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_count_offset, SLJIT_R0, 0);
}

static void EmitSljitGroupedAggregateStatePointer(struct sljit_compiler *compiler, sljit_s32 logical_index,
                                                  sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 3);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_R0, 0);
}

static void EmitSljitGroupedAggregateSetStateIsSet(struct sljit_compiler *compiler, sljit_s32 state_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
}

static void EmitSljitGroupedAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_s32 state_reg,
                                                     sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSet(compiler, state_reg);
}

static void EmitSljitGroupedAggregateIncrementInt64(struct sljit_compiler *compiler, sljit_s32 state_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, state_reg, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
}

static void EmitSljitGroupedAggregateAccumulateHugeintInt64(struct sljit_compiler *compiler, sljit_s32 state_reg,
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

void EmitSljitAggregateLoopStep(struct sljit_compiler *compiler, struct sljit_label *loop) {
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64Reference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                           string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

static void EmitSljitStoreZeroDoubleLocal(struct sljit_compiler *compiler, sljit_sw local_sum_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (sizeof(double) > sizeof(sljit_sw)) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset + sizeof(sljit_sw), SLJIT_IMM, 0);
	}
}

static void EmitSljitAggregateAccumulateDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                               sljit_sw saw_value_offset, sljit_s32 value_freg) {
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_SP),
	                local_sum_offset);
	sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR1, 0, value_freg, 0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1,
	                SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

static void EmitSljitAggregateCommitDouble(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                           sljit_sw saw_value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

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

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumDoubleReference(SljitNativeDoubleSourceKind kind,
                                            SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(double));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	EmitSljitStoreZeroDoubleLocal(compiler, local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	EmitLoadNativeDoubleOperand(compiler, kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
	                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0);
	EmitSljitAggregateAccumulateDouble(compiler, local_sum_offset, saw_value_offset, SLJIT_TMP_FR0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedCountStar(SljitNativeAggregateUpdateFunction &function,
                                                                         string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 3, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_row_count));
	auto no_row_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_row_count, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_int64_value));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

static unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumReference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                    string &error, SljitNativeAggregateSumStateKind state_kind) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	const bool hugeint_state = state_kind == SljitNativeAggregateSumStateKind::HUGEINT;

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	auto source_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale);
	if (hugeint_state) {
		EmitSljitGroupedAggregateAccumulateHugeintInt64(compiler, SLJIT_S4, SLJIT_R2);
	} else {
		EmitSljitGroupedAggregateAccumulateInt64(compiler, SLJIT_S4, SLJIT_R2);
	}
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumInt64Reference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                         string &error) {
	return BuildSljitNativeGroupedSumReference(kind, function, error, SljitNativeAggregateSumStateKind::INT64);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedSumHugeintReference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                           string &error) {
	return BuildSljitNativeGroupedSumReference(kind, function, error, SljitNativeAggregateSumStateKind::HUGEINT);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedCountStar(SljitNativeAggregateUpdateFunction &function,
                                                                       string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	EmitSljitGroupedAggregateIncrementInt64(compiler, SLJIT_S4);
	EmitSljitAggregateLoopStep(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, bool constant_on_left,
    SljitNativeAggregateUpdateFunction &function, string &error, bool check_result_range, int64_t result_min,
    int64_t result_max) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	if (constant_on_left) {
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	}
	auto overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	if (check_result_range) {
		range_too_small =
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
		range_too_large =
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
	}
	EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto overflow_label = sljit_emit_label(compiler);
	sljit_set_label(overflow, overflow_label);
	if (check_result_range) {
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
	}
	EmitSljitAggregateExpressionTreeOverflowCall(compiler, op);
	auto helper_done = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	sljit_set_label(helper_done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, SljitNativeAggregateUpdateFunction &function,
    string &error, bool check_result_range, int64_t result_min, int64_t result_max) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
	auto left_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	auto right_is_null =
	    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, right_source_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), data_scale);
	sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	auto overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	if (check_result_range) {
		range_too_small =
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
		range_too_large =
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
	}
	EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, invalid_label);
	sljit_set_label(right_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto overflow_label = sljit_emit_label(compiler);
	sljit_set_label(overflow, overflow_label);
	if (check_result_range) {
		sljit_set_label(range_too_small, overflow_label);
		sljit_set_label(range_too_large, overflow_label);
	}
	EmitSljitAggregateExpressionTreeOverflowCall(compiler, op);
	auto helper_done = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	sljit_set_label(helper_done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumDoubleDoubleBinaryConstant(SljitNativeDoubleBinaryOp op,
                                                       SljitNativeDoubleSourceKind source_kind, bool constant_on_left,
                                                       SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(double));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	EmitSljitStoreZeroDoubleLocal(compiler, local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);
	EmitLoadNativeDoubleOperand(compiler, source_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
	                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0);
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_S0),
	                offsetof(SljitNativeVectorInput, double_constant));
	if (constant_on_left) {
		sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0, SLJIT_TMP_FR0, 0);
	} else {
		sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
	}
	EmitSljitAggregateAccumulateDouble(compiler, local_sum_offset, saw_value_offset, SLJIT_TMP_FR0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumDoubleDoubleBinaryReferences(
    SljitNativeDoubleBinaryOp op, SljitNativeDoubleSourceKind left_kind, SljitNativeDoubleSourceKind right_kind,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto binary_op = NativeDoubleBinaryOp(op);
	auto needs_helper_spill = NativeDoubleSourceUsesHelper(left_kind) || NativeDoubleSourceUsesHelper(right_kind);
	const auto local_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value_offset = NumericCast<sljit_sw>(sizeof(double));
	const auto left_spill_offset = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto right_spill_offset = left_spill_offset + NumericCast<sljit_sw>(sizeof(double));
	const auto local_size = right_spill_offset + (needs_helper_spill ? NumericCast<sljit_sw>(sizeof(double)) : 0);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	EmitSljitStoreZeroDoubleLocal(compiler, local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
	auto left_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	auto right_is_null =
	    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);

	if (needs_helper_spill) {
		EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0,
		                SLJIT_MEM1(SLJIT_SP), left_spill_offset);
		EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                            offsetof(SljitNativeVectorInput, right_source_double_scale), SLJIT_TMP_FR0);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0,
		                SLJIT_MEM1(SLJIT_SP), right_spill_offset);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR0, SLJIT_MEM1(SLJIT_SP),
		                left_spill_offset);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_TMP_FR1, SLJIT_MEM1(SLJIT_SP),
		                right_spill_offset);
	} else {
		EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_TMP_FR0);
		EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                            offsetof(SljitNativeVectorInput, right_source_double_scale), SLJIT_TMP_FR1);
	}
	sljit_emit_fop2(compiler, binary_op, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR0, 0, SLJIT_TMP_FR1, 0);
	EmitSljitAggregateAccumulateDouble(compiler, local_sum_offset, saw_value_offset, SLJIT_TMP_FR0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	sljit_set_label(left_is_null, invalid_label);
	sljit_set_label(right_is_null, invalid_label);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitSljitAggregateLoopStep(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

static bool SljitFusedUngroupedPrimitiveAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                                  const ExecutionRegionAggregateInput &aggregate) {
	if (!aggregate.primitive_update_ready) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return aggregate.child_count == 0 && aggregate.child_types.empty();
	}
	if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    aggregate.child_types.size() != 1 ||
	    aggregate.primitive_update_input_type != aggregate.child_types[0].InternalType() ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return true;
	default:
		return false;
	}
}

static void EmitLoadFusedAggregateExecuteIndex(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));
}

static void EmitLoadFusedAggregateSourceIndex(struct sljit_compiler *compiler, sljit_sw source_sel_array_offset,
                                              idx_t lane_idx, sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_sel_array_offset);
	auto no_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_array, use_logical_index);
	sljit_set_label(no_source_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitFusedAggregateJumpIfValidityNull(struct sljit_compiler *compiler,
                                                               sljit_sw validity_array_offset, idx_t lane_idx,
                                                               sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), validity_array_offset);
	auto source_all_valid_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto all_valid = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid_array, all_valid);
	sljit_set_label(source_all_valid, all_valid);
	return source_is_null;
}

static void EmitLoadFusedAggregateIntegerData(struct sljit_compiler *compiler, sljit_sw source_data_array_offset,
                                              idx_t lane_idx, SljitNativeIntegerKind kind, sljit_s32 index_reg,
                                              sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_data_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(kind), target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg),
	               NativeIntegerDataScale(kind));
}

static void EmitLoadFusedAggregatePointer(struct sljit_compiler *compiler, sljit_sw pointer_array_offset,
                                          idx_t lane_idx, sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(SLJIT_S0), pointer_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(target_reg), SljitPointerArrayOffset(lane_idx));
}

static void EmitFusedAggregateAccumulateInt64(struct sljit_compiler *compiler, sljit_sw local_sum_offset,
                                              sljit_sw saw_value_offset, sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

static void EmitFusedAggregateCommitCountStar(struct sljit_compiler *compiler, idx_t lane_idx) {
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_row_counts), lane_idx, SLJIT_R0);
	auto no_row_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_row_count, sljit_emit_label(compiler));

	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_int64_values), lane_idx,
	                              SLJIT_R0);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

static void EmitFusedAggregateCommitSumInt64(struct sljit_compiler *compiler, idx_t lane_idx, sljit_sw local_sum_offset,
                                             sljit_sw saw_value_offset) {
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_row_counts), lane_idx, SLJIT_R0);
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_int64_values), lane_idx,
	                              SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_state_is_sets), lane_idx,
	                              SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

static void EmitFusedFilteredAggregateCommitLocalCount(struct sljit_compiler *compiler, idx_t lane_idx,
                                                       sljit_sw local_count_offset) {
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_row_counts), lane_idx, SLJIT_R0);
	auto no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count, sljit_emit_label(compiler));
}

static void EmitFusedFilteredAggregateCommitCountStar(struct sljit_compiler *compiler, idx_t lane_idx,
                                                      sljit_sw local_count_offset) {
	EmitFusedFilteredAggregateCommitLocalCount(compiler, lane_idx, local_count_offset);

	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_int64_values), lane_idx,
	                              SLJIT_R0);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

static void EmitFusedFilteredAggregateCommitSumInt64(struct sljit_compiler *compiler, idx_t lane_idx,
                                                     sljit_sw local_sum_offset, sljit_sw saw_value_offset,
                                                     sljit_sw local_count_offset) {
	EmitFusedFilteredAggregateCommitLocalCount(compiler, lane_idx, local_count_offset);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_int64_values), lane_idx,
	                              SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_sum_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_state_is_sets), lane_idx,
	                              SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

static void EmitFusedFilteredAggregateCommitSumHugeint(struct sljit_compiler *compiler, idx_t lane_idx,
                                                       sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                                       sljit_sw saw_value_offset, sljit_sw local_count_offset) {
	EmitFusedFilteredAggregateCommitLocalCount(compiler, lane_idx, local_count_offset);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), saw_value_offset);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, lower), SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_upper_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_local_hugeint) + offsetof(hugeint_t, upper), SLJIT_R2, 0);
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_hugeint_values), lane_idx,
	                              SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_hugeint_value), SLJIT_R0, 0);
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_state_is_sets), lane_idx,
	                              SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set), SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
	                 SLJIT_FUNC_ADDR(SljitNativeAggregateHugeintCommit));
	sljit_set_label(no_value, sljit_emit_label(compiler));
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedFusedPrimitiveAggregateUpdate(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                       const vector<ExecutionRegionAggregateInput> &aggregates,
                                                       SljitNativeAggregateUpdateFunction &function, string &error) {
	if (payloads.size() != aggregates.size() || payloads.size() < 2) {
		error = "unsupported fused aggregate payload shape";
		return nullptr;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedUngroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx])) {
			error = "unsupported fused aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	vector<sljit_sw> local_sum_offsets(payloads.size(), -1);
	vector<sljit_sw> saw_value_offsets(payloads.size(), -1);
	bool has_sum_lane = false;
	sljit_sw local_size = 0;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
			continue;
		}
		has_sum_lane = true;
		local_sum_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		saw_value_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	if (!has_sum_lane) {
		sljit_free_compiler(compiler);
		error = "unsupported fused aggregate payload shape";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (local_sum_offsets[payload_idx] < 0) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
	}

	vector<SljitExpressionTreeOverflowJumps> overflows;
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		auto &payload = payloads[payload_idx];
		vector<sljit_jump *> invalid_jumps;
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1);
		invalid_jumps.push_back(EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1));
		if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel_array),
			                                  payload_idx, SLJIT_S4);
			invalid_jumps.push_back(EmitFusedAggregateJumpIfValidityNull(
			    compiler, offsetof(SljitNativeVectorInput, right_source_validity_array), payload_idx, SLJIT_S4));
		}
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array), payload_idx,
		                                  payload.integer_kind, SLJIT_R1, SLJIT_R2);
		if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, constants));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0),
			               NumericCast<sljit_sw>(payload_idx * sizeof(int64_t)));
			if (payload.constant_on_left) {
				sljit_emit_op2(compiler,
				               NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW,
				               SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
			} else {
				sljit_emit_op2(compiler,
				               NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW,
				               SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			}
			AddSljitExpressionOverflowJump(overflows, payload.binary_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
		} else if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, right_source_data_array),
			                                  payload_idx, payload.integer_kind, SLJIT_S4, SLJIT_R3);
			sljit_emit_op2(compiler,
			               NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW,
			               SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			AddSljitExpressionOverflowJump(overflows, payload.binary_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
		}
		if (payload.check_result_range) {
			AddSljitExpressionOverflowJump(overflows, payload.binary_op,
			                               sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
			                                              NumericCast<sljit_sw>(payload.result_min)));
			AddSljitExpressionOverflowJump(overflows, payload.binary_op,
			                               sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
			                                              NumericCast<sljit_sw>(payload.result_max)));
		}
		EmitFusedAggregateAccumulateInt64(compiler, local_sum_offsets[payload_idx], saw_value_offsets[payload_idx],
		                                  SLJIT_R2);
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		for (auto invalid_jump : invalid_jumps) {
			sljit_set_label(invalid_jump, payload_invalid);
		}
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}

	EmitSljitAggregateLoopStep(compiler, loop);

	vector<sljit_jump *> helper_done;
	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitAggregateExpressionTreeOverflowCall(compiler, overflow.op);
		helper_done.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitFusedAggregateCommitCountStar(compiler, payload_idx);
		} else {
			EmitFusedAggregateCommitSumInt64(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                 saw_value_offsets[payload_idx]);
		}
	}
	auto return_label = sljit_emit_label(compiler);
	for (auto jump : helper_done) {
		sljit_set_label(jump, return_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

struct SljitFilteredFusedPrimitiveAggregateCodegenPlan {
	SljitTypedExpressionTreePlan predicate;
	vector<SljitTypedExpressionTreePlan> payloads;
	idx_t tree_node_count = 0;
	bool fast_path_supported = false;
};

static bool BuildSljitFilteredFusedPrimitiveAggregatePayloadPlan(const SljitNativeRegionExpressionPlan &payload,
                                                                 const ExecutionRegionAggregateInput &aggregate,
                                                                 SljitTypedExpressionTreePlan &payload_plan) {
	if (!aggregate.primitive_update_ready) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return aggregate.child_count == 0 && aggregate.child_types.empty();
	}
	if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	    aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		return false;
	}
	if (aggregate.child_types.size() != 1 ||
	    aggregate.primitive_update_input_type != aggregate.child_types[0].InternalType() ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType() || !payload.expression_tree) {
		return false;
	}
	payload_plan = BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false);
	if (payload_plan.supported && payload_plan.result_is_int64) {
		return true;
	}
	if ((aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64 ||
	     aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) &&
	    payload.expression_tree->kind == ExecutionExpressionIRKind::REFERENCE &&
	    payload.expression_tree->physical_type == PhysicalType::INT64) {
		payload_plan.supported = true;
		payload_plan.result_kind = SljitNativeIntegerKind::INT64;
		payload_plan.result_is_int64 = true;
		payload_plan.node_count = CountSljitTypedExpressionTreeNodes(*payload.expression_tree);
		return true;
	}
	return false;
}

static bool
BuildSljitFilteredFusedPrimitiveAggregateCodegenPlan(const ExecutionExpressionIR &predicate,
                                                     const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                     const vector<ExecutionRegionAggregateInput> &aggregates,
                                                     SljitFilteredFusedPrimitiveAggregateCodegenPlan &codegen_plan) {
	if (payloads.empty() || payloads.size() != aggregates.size()) {
		return false;
	}
	codegen_plan = SljitFilteredFusedPrimitiveAggregateCodegenPlan();
	codegen_plan.predicate = BuildSljitTypedExpressionTreePlan(predicate, false);
	if (!codegen_plan.predicate.supported || !codegen_plan.predicate.result_is_bool) {
		return false;
	}
	codegen_plan.tree_node_count = codegen_plan.predicate.node_count;
	codegen_plan.fast_path_supported = codegen_plan.predicate.fast_path.fast_path_supported;
	codegen_plan.payloads.resize(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!BuildSljitFilteredFusedPrimitiveAggregatePayloadPlan(payloads[payload_idx], aggregates[payload_idx],
		                                                          codegen_plan.payloads[payload_idx])) {
			return false;
		}
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		codegen_plan.tree_node_count += codegen_plan.payloads[payload_idx].node_count;
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported && codegen_plan.payloads[payload_idx].fast_path.fast_path_supported;
	}
	return true;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFilteredUngroupedFusedPrimitiveAggregateUpdate(
    const ExecutionExpressionIR &predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, SljitNativeAggregateUpdateFunction &function,
    string &error) {
	SljitFilteredFusedPrimitiveAggregateCodegenPlan codegen_plan;
	if (!BuildSljitFilteredFusedPrimitiveAggregateCodegenPlan(predicate, payloads, aggregates, codegen_plan)) {
		error = "unsupported filtered fused aggregate payload shape";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto tree_local_size = NumericCast<sljit_sw>(codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	vector<sljit_sw> local_count_offsets(payloads.size(), -1);
	vector<sljit_sw> local_sum_offsets(payloads.size(), -1);
	vector<sljit_sw> local_sum_upper_offsets(payloads.size(), -1);
	vector<sljit_sw> saw_value_offsets(payloads.size(), -1);
	sljit_sw local_size = tree_local_size;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		local_count_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		auto kind = aggregates[payload_idx].primitive_update_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		local_sum_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			local_sum_upper_offsets[payload_idx] = local_size;
			local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		}
		saw_value_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_count_offsets[payload_idx], SLJIT_IMM, 0);
		if (local_sum_offsets[payload_idx] >= 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_IMM, 0);
			if (local_sum_upper_offsets[payload_idx] >= 0) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[payload_idx],
				               SLJIT_IMM, 0);
			}
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
		}
	}
	EmitInitSljitNativeVectorSourceArrays(compiler);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	struct sljit_jump *fast_done = nullptr;
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

		auto fast_loop = sljit_emit_label(compiler);
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		idx_t predicate_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, predicate, predicate_spill_index, overflows);
		auto predicate_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto kind = aggregates[payload_idx].primitive_update_kind;
			if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
				continue;
			}
			idx_t payload_spill_index = 0;
			EmitSljitTypedExpressionTreeFastValueReg(compiler, *payloads[payload_idx].expression_tree,
			                                         payload_spill_index, overflows);
			if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
				EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offsets[payload_idx],
				                                         local_sum_upper_offsets[payload_idx],
				                                         saw_value_offsets[payload_idx], SLJIT_R2);
			} else {
				EmitSljitAggregateAccumulateInt64(compiler, local_sum_offsets[payload_idx],
				                                  saw_value_offsets[payload_idx], SLJIT_R2);
			}
			EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
		}
		sljit_set_label(predicate_false, sljit_emit_label(compiler));
		EmitSljitAggregateLoopStep(compiler, fast_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<sljit_jump *> row_skip_jumps;
	idx_t predicate_slot_index = 0;
	auto predicate_slot = EmitSljitTypedExpressionTreeValue(compiler, predicate, predicate_slot_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.valid_offset);
	row_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.value_offset);
	row_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = aggregates[payload_idx].primitive_update_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
			continue;
		}
		vector<sljit_jump *> payload_skip_jumps;
		idx_t payload_slot_index = 0;
		auto payload_slot = EmitSljitTypedExpressionTreeValue(compiler, *payloads[payload_idx].expression_tree,
		                                                      payload_slot_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.valid_offset);
		payload_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
		if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			EmitSljitAggregateAccumulateHugeintInt64(compiler, local_sum_offsets[payload_idx],
			                                         local_sum_upper_offsets[payload_idx],
			                                         saw_value_offsets[payload_idx], SLJIT_R2);
		} else {
			EmitSljitAggregateAccumulateInt64(compiler, local_sum_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                  SLJIT_R2);
		}
		EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_skip_label = sljit_emit_label(compiler);
		for (auto payload_skip : payload_skip_jumps) {
			sljit_set_label(payload_skip, payload_skip_label);
		}
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto row_skip_label = sljit_emit_label(compiler);
	for (auto row_skip : row_skip_jumps) {
		sljit_set_label(row_skip, row_skip_label);
	}
	sljit_set_label(row_done, sljit_emit_label(compiler));
	EmitSljitAggregateLoopStep(compiler, loop);

	vector<sljit_jump *> helper_done;
	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitAggregateExpressionTreeOverflowCall(compiler, overflow.op);
		helper_done.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}

	auto done_label = sljit_emit_label(compiler);
	if (fast_done) {
		sljit_set_label(fast_done, done_label);
	}
	sljit_set_label(done, done_label);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = aggregates[payload_idx].primitive_update_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitFusedFilteredAggregateCommitCountStar(compiler, payload_idx, local_count_offsets[payload_idx]);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			EmitFusedFilteredAggregateCommitSumHugeint(
			    compiler, payload_idx, local_sum_offsets[payload_idx], local_sum_upper_offsets[payload_idx],
			    saw_value_offsets[payload_idx], local_count_offsets[payload_idx]);
		} else {
			EmitFusedFilteredAggregateCommitSumInt64(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                         saw_value_offsets[payload_idx], local_count_offsets[payload_idx]);
		}
	}
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

static bool SljitFusedGroupedPrimitiveAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                                const ExecutionRegionAggregateInput &aggregate,
                                                                const ExecutionRegionAggregateContract &contract) {
	if (!aggregate.primitive_update_ready || aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return aggregate.child_count == 0 && aggregate.child_types.empty();
	}
	if ((aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) ||
	    aggregate.child_types.size() != 1 ||
	    aggregate.primitive_update_input_type != aggregate.child_types[0].InternalType() ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	return payload.kind == SljitNativeRegionExpressionKind::REFERENCE;
}

static void EmitSljitGroupedAggregateValuePointerImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                           idx_t state_offset, idx_t value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, base_reg, 0);
	const auto offset = NumericCast<sljit_sw>(state_offset + value_offset);
	if (offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, offset);
	}
}

static void EmitSljitGroupedAggregateSetStateIsSetImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                            idx_t state_offset, idx_t state_is_set_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, base_reg, 0);
	const auto offset = NumericCast<sljit_sw>(state_offset + state_is_set_offset);
	if (offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, offset);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
}

static void EmitSljitGroupedAggregateAccumulateInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                              idx_t state_offset, idx_t value_offset,
                                                              idx_t state_is_set_offset, sljit_s32 value_reg) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, base_reg, state_offset, state_is_set_offset);
}

static void EmitSljitGroupedAggregateIncrementInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                             idx_t state_offset, idx_t value_offset) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
}

static void EmitSljitGroupedAggregateAccumulateHugeintImmediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                                idx_t state_offset, idx_t value_offset,
                                                                idx_t state_is_set_offset, sljit_s32 value_reg) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper), SLJIT_R3, 0);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, base_reg, state_offset, state_is_set_offset);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error) {
	if (!contract.grouped_state_layout_ready || payloads.size() != aggregates.size() || payloads.size() < 2) {
		error = "unsupported fused grouped aggregate payload shape";
		return nullptr;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx],
		                                                         contract)) {
			error = "unsupported fused grouped aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 3);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 aggregate.primitive_update_state_value_offset);
			continue;
		}

		auto &payload = payloads[payload_idx];
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1);
		auto source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1);
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array), payload_idx,
		                                  payload.integer_kind, SLJIT_R1, SLJIT_R2);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                  aggregate.primitive_update_state_value_offset,
			                                                  aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		} else {
			EmitSljitGroupedAggregateAccumulateHugeintImmediate(
			    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		}
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		sljit_set_label(source_is_null, payload_invalid);
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}

	EmitSljitAggregateLoopStep(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

struct SljitPerfectHashGroupPlan {
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	int64_t minimum = 0;
	idx_t shift = 0;
};

static bool TryGetSljitPerfectHashGroupIntegerKind(const LogicalType &type, SljitNativeIntegerKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		kind = SljitNativeIntegerKind::INT8;
		return true;
	case PhysicalType::UINT8:
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	case PhysicalType::INT32:
		kind = SljitNativeIntegerKind::INT32;
		return true;
	case PhysicalType::INT64:
		kind = SljitNativeIntegerKind::INT64;
		return true;
	default:
		return false;
	}
}

static bool TryGetSljitPerfectHashGroupMinimum(const LogicalType &type, const Value &minimum, int64_t &result) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::UINT8:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<uint8_t>());
		return true;
	case PhysicalType::INT32:
		result = NumericCast<int64_t>(minimum.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		result = minimum.GetValueUnsafe<int64_t>();
		return true;
	default:
		return false;
	}
}

static bool TryBuildSljitPerfectHashGroupPlans(const vector<ExecutionRegionGroupInput> &groups,
                                               const ExecutionRegionAggregateContract &contract,
                                               vector<SljitPerfectHashGroupPlan> &result) {
	if (contract.kind != ExecutionRegionAggregateOperatorKind::PERFECT_HASH ||
	    contract.perfect_required_bits.size() != groups.size() ||
	    contract.perfect_group_minima.size() != groups.size()) {
		return false;
	}
	result.reserve(groups.size());
	idx_t shift = contract.perfect_required_bits_total;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		if (!group.supported_reference) {
			return false;
		}
		if (shift < contract.perfect_required_bits[group_idx]) {
			return false;
		}
		shift -= contract.perfect_required_bits[group_idx];
		SljitPerfectHashGroupPlan plan;
		if (!TryGetSljitPerfectHashGroupIntegerKind(group.type, plan.integer_kind) ||
		    !TryGetSljitPerfectHashGroupMinimum(group.type, contract.perfect_group_minima[group_idx], plan.minimum)) {
			return false;
		}
		plan.shift = shift;
		result.push_back(plan);
	}
	return true;
}

static void EmitLoadFusedAggregateGroupSourceIndex(struct sljit_compiler *compiler, idx_t group_idx,
                                                   sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_sel_array));
	auto no_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	auto no_group_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_group_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_array, use_logical_index);
	sljit_set_label(no_group_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
	sljit_set_label(have_group_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitFusedAggregateJumpIfGroupValidityNull(struct sljit_compiler *compiler, idx_t group_idx,
                                                                    sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_validity_array));
	auto source_all_valid_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto all_valid = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid_array, all_valid);
	sljit_set_label(source_all_valid, all_valid);
	return source_is_null;
}

static void EmitLoadFusedAggregateGroupIntegerData(struct sljit_compiler *compiler, idx_t group_idx,
                                                   SljitNativeIntegerKind kind, sljit_s32 index_reg,
                                                   sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, group_data_array));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(kind), target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg),
	               NativeIntegerDataScale(kind));
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePerfectHashGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<ExecutionRegionGroupInput> &groups, const ExecutionRegionAggregateContract &contract,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	vector<SljitPerfectHashGroupPlan> group_plans;
	if (!TryBuildSljitPerfectHashGroupPlans(groups, contract, group_plans) || group_plans.empty() ||
	    !contract.grouped_state_layout_ready || payloads.size() != aggregates.size() || payloads.empty()) {
		error = "unsupported fused perfect-hash aggregate payload shape";
		return nullptr;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx],
		                                                         contract)) {
			error = "unsupported fused perfect-hash aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	vector<sljit_jump *> group_out_of_range;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	for (idx_t group_idx = 0; group_idx < group_plans.size(); group_idx++) {
		auto &group = group_plans[group_idx];
		EmitLoadFusedAggregateGroupSourceIndex(compiler, group_idx, SLJIT_R1);
		auto group_is_null = EmitFusedAggregateJumpIfGroupValidityNull(compiler, group_idx, SLJIT_R1);
		EmitLoadFusedAggregateGroupIntegerData(compiler, group_idx, group.integer_kind, SLJIT_R1, SLJIT_R2);
		if (group.minimum != 0) {
			sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(group.minimum));
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
		if (group.shift != 0) {
			sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(group.shift));
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R2, 0);
		auto group_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto group_null_label = sljit_emit_label(compiler);
		sljit_set_label(group_is_null, group_null_label);
		sljit_set_label(group_done, sljit_emit_label(compiler));
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_total_groups));
	group_out_of_range.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S4, 0, SLJIT_R0, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_group_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_tuple_size));
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_state_data));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R1, 0);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 aggregate.primitive_update_state_value_offset);
			continue;
		}
		auto &payload = payloads[payload_idx];
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1);
		auto source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1);
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array), payload_idx,
		                                  payload.integer_kind, SLJIT_R1, SLJIT_R2);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                  aggregate.primitive_update_state_value_offset,
			                                                  aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		} else {
			EmitSljitGroupedAggregateAccumulateHugeintImmediate(
			    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		}
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		sljit_set_label(source_is_null, payload_invalid);
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}
	EmitSljitAggregateLoopStep(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	auto range_error_label = sljit_emit_label(compiler);
	for (auto jump : group_out_of_range) {
		sljit_set_label(jump, range_error_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeRuntimeError));
	sljit_emit_return_void(compiler);

	return FinishSljitNativeAggregateUpdateCode(compiler, function, error);
}

} // namespace duckdb
