#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "sljitLir.h"

#include <algorithm>
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

static void EmitSljitAggregateAddRowCount(struct sljit_compiler *compiler, sljit_s32 count_reg) {
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
	EmitInitSljitNativeVectorLoop(compiler);
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
	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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
	EmitInitSljitNativeVectorLoop(compiler);
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
	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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

	EmitSljitAggregateAddRowCount(compiler, SLJIT_S1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_int64_value));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedCountReference(SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	EmitSljitAggregateAddRowCount(compiler, SLJIT_S2);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity));
	auto source_has_null = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S2, 0);
	auto commit = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_has_null, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S4);
	auto source_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S4);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_is_null, sljit_emit_label(compiler));
	sljit_set_label(next, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_set_label(commit, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_int64_value));
	auto no_count_state = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_count_state, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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
	EmitInitSljitNativeVectorLoop(compiler);

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
	EmitNextSljitNativeVectorLoop(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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
	EmitInitSljitNativeVectorLoop(compiler);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	EmitSljitGroupedAggregateIncrementInt64(compiler, SLJIT_S4);
	EmitNextSljitNativeVectorLoop(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeGroupedCountReference(SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity));
	auto source_has_null = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	auto all_valid_loop = sljit_emit_label(compiler);
	auto all_valid_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	EmitSljitGroupedAggregateIncrementInt64(compiler, SLJIT_S4);
	EmitNextSljitNativeVectorLoop(compiler, all_valid_loop);
	auto finish = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_has_null, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadLogicalIndex(compiler, SLJIT_R1);
	EmitSljitGroupedAggregateStatePointer(compiler, SLJIT_R1, SLJIT_S4);
	EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
	auto source_is_null = EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
	EmitSljitGroupedAggregateIncrementInt64(compiler, SLJIT_S4);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_is_null, sljit_emit_label(compiler));
	sljit_set_label(next, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto finish_label = sljit_emit_label(compiler);
	sljit_set_label(all_valid_done, finish_label);
	sljit_set_label(done, finish_label);
	sljit_set_label(finish, finish_label);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, bool constant_on_left,
    SljitNativeAggregateUpdateFunction &function, string &error, bool check_arithmetic_overflow,
    bool check_result_range, int64_t result_min, int64_t result_max) {
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
	EmitInitSljitNativeVectorLoop(compiler);
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
	auto emit_binary_op = check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op;
	if (constant_on_left) {
		sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	}
	struct sljit_jump *overflow = nullptr;
	if (check_arithmetic_overflow) {
		overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	}
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
	EmitNextSljitNativeVectorLoop(compiler, loop);

	struct sljit_jump *helper_done = nullptr;
	if (check_arithmetic_overflow || check_result_range) {
		auto overflow_label = sljit_emit_label(compiler);
		if (check_arithmetic_overflow) {
			sljit_set_label(overflow, overflow_label);
		}
		if (check_result_range) {
			sljit_set_label(range_too_small, overflow_label);
			sljit_set_label(range_too_large, overflow_label);
		}
		EmitSljitAggregateExpressionTreeOverflowCall(compiler, op);
		helper_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	}

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	if (helper_done) {
		sljit_set_label(helper_done, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, SljitNativeAggregateUpdateFunction &function,
    string &error, bool check_arithmetic_overflow, bool check_result_range, int64_t result_min, int64_t result_max) {
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
	EmitInitSljitNativeVectorLoop(compiler);
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
	sljit_emit_op2(compiler, check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op, SLJIT_R2, 0,
	               SLJIT_R2, 0, SLJIT_R3, 0);
	struct sljit_jump *overflow = nullptr;
	if (check_arithmetic_overflow) {
		overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	}
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
	EmitNextSljitNativeVectorLoop(compiler, loop);

	struct sljit_jump *helper_done = nullptr;
	if (check_arithmetic_overflow || check_result_range) {
		auto overflow_label = sljit_emit_label(compiler);
		if (check_arithmetic_overflow) {
			sljit_set_label(overflow, overflow_label);
		}
		if (check_result_range) {
			sljit_set_label(range_too_small, overflow_label);
			sljit_set_label(range_too_large, overflow_label);
		}
		EmitSljitAggregateExpressionTreeOverflowCall(compiler, op);
		helper_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	}

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, local_sum_offset, saw_value_offset);
	if (helper_done) {
		sljit_set_label(helper_done, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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
	EmitInitSljitNativeVectorLoop(compiler);
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
	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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
	EmitInitSljitNativeVectorLoop(compiler);
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
	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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

static void EmitLoadFusedAggregateExecuteIndex(struct sljit_compiler *compiler, bool direct_logical_index = false) {
	if (direct_logical_index) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
		return;
	}
	EmitLoadLogicalIndex(compiler, SLJIT_S3);
}

static void EmitLoadFusedAggregateSourceIndex(struct sljit_compiler *compiler, sljit_sw source_sel_array_offset,
                                              idx_t lane_idx, sljit_s32 target_reg,
                                              bool use_common_source_selection = false) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_sel_array_offset);
	auto no_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	if (use_common_source_selection) {
		auto use_common_source_sel = sljit_emit_label(compiler);
		sljit_set_label(no_array, use_common_source_sel);
		sljit_set_label(no_source_sel, use_common_source_sel);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_common_sel));
		auto no_common_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
		auto have_common_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(no_common_source_sel, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
		auto done = sljit_emit_label(compiler);
		sljit_set_label(have_source_index, done);
		sljit_set_label(have_common_source_index, done);
		return;
	}
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_array, use_logical_index);
	sljit_set_label(no_source_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static void EmitLoadGroupedAggregateStateAddress(struct sljit_compiler *compiler, sljit_s32 target_reg,
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

static bool TryGetSljitAggregateDataPointerHoist(const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists,
                                                 idx_t source_index, sljit_s32 &data_reg);

static sljit_jump *EmitLoadFusedTypedAggregateReferenceValue(
    struct sljit_compiler *compiler, const SljitNativeRegionExpressionPlan &payload, bool use_source_selection,
    bool check_validity, sljit_s32 direct_index_reg,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr) {
	if (!use_source_selection && !check_validity) {
		sljit_s32 data_reg;
		if (TryGetSljitAggregateDataPointerHoist(data_hoists, payload.source_index, data_reg)) {
			sljit_emit_op1(compiler, NativeIntegerLoadOp(payload.integer_kind), SLJIT_R2, 0,
			               SLJIT_MEM2(data_reg, direct_index_reg), NativeIntegerDataScale(payload.integer_kind));
			return nullptr;
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(payload.source_index));
		}
		sljit_emit_op1(compiler, NativeIntegerLoadOp(payload.integer_kind), SLJIT_R2, 0,
		               SLJIT_MEM2(SLJIT_R0, direct_index_reg), NativeIntegerDataScale(payload.integer_kind));
		return nullptr;
	}
	if (use_source_selection) {
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array),
		                                  payload.source_index, SLJIT_R1, true);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, direct_index_reg, 0);
	}
	sljit_jump *source_is_null = nullptr;
	if (check_validity) {
		source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload.source_index, SLJIT_R1);
	}
	sljit_s32 data_reg;
	if (TryGetSljitAggregateDataPointerHoist(data_hoists, payload.source_index, data_reg)) {
		sljit_emit_op1(compiler, NativeIntegerLoadOp(payload.integer_kind), SLJIT_R2, 0, SLJIT_MEM2(data_reg, SLJIT_R1),
		               NativeIntegerDataScale(payload.integer_kind));
	} else {
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array),
		                                  payload.source_index, payload.integer_kind, SLJIT_R1, SLJIT_R2);
	}
	return source_is_null;
}

static void EmitLoadFusedAggregatePointer(struct sljit_compiler *compiler, sljit_sw pointer_array_offset,
                                          idx_t lane_idx, sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(SLJIT_S0), pointer_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(target_reg), SljitPointerArrayOffset(lane_idx));
}

static void EmitFusedAggregateAddRowCount(struct sljit_compiler *compiler, idx_t lane_idx, sljit_s32 count_reg) {
	EmitLoadFusedAggregatePointer(compiler, offsetof(SljitNativeVectorInput, aggregate_row_counts), lane_idx, SLJIT_R0);
	auto no_row_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, count_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_set_label(no_row_count, sljit_emit_label(compiler));
}

static void EmitFusedAggregateCommitCountStar(struct sljit_compiler *compiler, idx_t lane_idx) {
	EmitFusedAggregateAddRowCount(compiler, lane_idx, SLJIT_S2);
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
	EmitFusedAggregateAddRowCount(compiler, lane_idx, SLJIT_S2);
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
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), local_count_offset);
	EmitFusedAggregateAddRowCount(compiler, lane_idx, SLJIT_R2);
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
		                                  SLJIT_R1, true);
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
				               payload.check_arithmetic_overflow
				                   ? NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW
				                   : NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op),
				               SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
			} else {
				sljit_emit_op2(compiler,
				               payload.check_arithmetic_overflow
				                   ? NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW
				                   : NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op),
				               SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			}
			if (payload.check_arithmetic_overflow) {
				AddSljitExpressionOverflowJump(overflows, payload.binary_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
			}
		} else if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, right_source_data_array),
			                                  payload_idx, payload.integer_kind, SLJIT_S4, SLJIT_R3);
			sljit_emit_op2(compiler,
			               payload.check_arithmetic_overflow
			                   ? NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW
			                   : NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op),
			               SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			if (payload.check_arithmetic_overflow) {
				AddSljitExpressionOverflowJump(overflows, payload.binary_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
			}
		}
		if (payload.check_result_range) {
			AddSljitExpressionOverflowJump(overflows, payload.binary_op,
			                               sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
			                                              NumericCast<sljit_sw>(payload.result_min)));
			AddSljitExpressionOverflowJump(overflows, payload.binary_op,
			                               sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
			                                              NumericCast<sljit_sw>(payload.result_max)));
		}
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offsets[payload_idx], saw_value_offsets[payload_idx],
		                                  SLJIT_R2);
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		for (auto invalid_jump : invalid_jumps) {
			sljit_set_label(invalid_jump, payload_invalid);
		}
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}

	EmitNextSljitNativeVectorLoop(compiler, loop);

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

	return FinishSljitCode(compiler, function, error);
}

struct SljitUngroupedFusedTypedAggregateCodegenPlan {
	vector<SljitTypedExpressionTreePlan> payloads;
	idx_t tree_node_count = 0;
	bool fast_path_supported = false;
	bool conditional_shared_payload = false;
	idx_t conditional_lane = 0;
	idx_t shared_lane = 0;
	const ExecutionExpressionIR *conditional_predicate = nullptr;
	const ExecutionExpressionIR *shared_value = nullptr;
	bool binary_shared_payload = false;
	idx_t binary_base_lane = 0;
	idx_t binary_dependent_lane = 0;
	bool binary_base_on_left = true;
	const ExecutionExpressionIR *binary_root = nullptr;
	const ExecutionExpressionIR *binary_other_value = nullptr;
};

static bool TryGetSljitAggregatePayloadIntegerKind(const LogicalType &payload_type, SljitNativeIntegerKind &kind) {
	switch (payload_type.InternalType()) {
	case PhysicalType::INT32:
		kind = SljitNativeIntegerKind::INT32;
		return true;
	case PhysicalType::INT64:
		kind = payload_type.id() == LogicalTypeId::DECIMAL ? SljitNativeIntegerKind::DECIMAL64
		                                                   : SljitNativeIntegerKind::INT64;
		return true;
	default:
		return false;
	}
}

static bool SljitAggregateTypedPayloadPlanSupported(const SljitTypedExpressionTreePlan &payload_plan,
                                                    const ExecutionRegionAggregateInput &aggregate) {
	if (!payload_plan.supported || aggregate.child_types.size() != 1) {
		return false;
	}
	SljitNativeIntegerKind aggregate_payload_kind;
	return TryGetSljitAggregatePayloadIntegerKind(aggregate.child_types[0], aggregate_payload_kind) &&
	       payload_plan.result_kind == aggregate_payload_kind;
}

static bool BuildSljitUngroupedFusedTypedAggregatePayloadPlan(const SljitNativeRegionExpressionPlan &payload,
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
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		payload_plan.supported = true;
		payload_plan.result_kind = payload.integer_kind;
		payload_plan.result_is_int64 = true;
		payload_plan.fast_path.fast_path_supported = true;
		return true;
	}
	if (payload.kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE || !payload.expression_tree) {
		return false;
	}
	payload_plan = BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false);
	return SljitAggregateTypedPayloadPlanSupported(payload_plan, aggregate);
}

static bool SljitExpressionIRStructurallyEqual(const ExecutionExpressionIR &left, const ExecutionExpressionIR &right) {
	return DescribeExecutionExpressionIR(left) == DescribeExecutionExpressionIR(right);
}

static bool SljitExpressionIRIsNonNullZero(const ExecutionExpressionIR &node) {
	return node.kind == ExecutionExpressionIRKind::CONSTANT && !node.constant.IsNull() && node.constant == int64_t(0);
}

static bool TryBuildSljitConditionalSharedAggregatePlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                        const vector<ExecutionRegionAggregateInput> &aggregates,
                                                        SljitUngroupedFusedTypedAggregateCodegenPlan &codegen_plan) {
	if (payloads.size() != 2 || aggregates.size() != 2) {
		return false;
	}
	if (aggregates[0].primitive_update_kind != aggregates[1].primitive_update_kind ||
	    (aggregates[0].primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	     aggregates[0].primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT)) {
		return false;
	}
	if (!payloads[0].expression_tree || !payloads[1].expression_tree ||
	    payloads[0].return_type != payloads[1].return_type) {
		return false;
	}
	for (idx_t conditional_lane = 0; conditional_lane < 2; conditional_lane++) {
		auto shared_lane = 1 - conditional_lane;
		auto &conditional = *payloads[conditional_lane].expression_tree;
		auto &shared = *payloads[shared_lane].expression_tree;
		if (conditional.kind != ExecutionExpressionIRKind::CASE || conditional.children.size() != 2 ||
		    !conditional.children[0] || !conditional.children[1] || !conditional.else_node ||
		    !SljitExpressionIRIsNonNullZero(*conditional.else_node) ||
		    !SljitExpressionIRStructurallyEqual(*conditional.children[1], shared)) {
			continue;
		}
		auto predicate_plan = BuildSljitTypedExpressionTreePlan(*conditional.children[0], false);
		auto value_plan = BuildSljitTypedExpressionTreePlan(shared, false);
		if (!predicate_plan.supported || !predicate_plan.result_is_bool || !value_plan.supported ||
		    !value_plan.result_is_int64) {
			return false;
		}
		codegen_plan.conditional_shared_payload = true;
		codegen_plan.conditional_lane = conditional_lane;
		codegen_plan.shared_lane = shared_lane;
		codegen_plan.conditional_predicate = conditional.children[0].get();
		codegen_plan.shared_value = &shared;
		codegen_plan.tree_node_count = predicate_plan.node_count + value_plan.node_count;
		codegen_plan.fast_path_supported =
		    predicate_plan.fast_path.fast_path_supported && value_plan.fast_path.fast_path_supported;
		return true;
	}
	return false;
}

static bool TryBuildSljitBinarySharedAggregatePlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                   const vector<ExecutionRegionAggregateInput> &aggregates,
                                                   SljitUngroupedFusedTypedAggregateCodegenPlan &codegen_plan) {
	if (payloads.size() != aggregates.size() || payloads.size() < 2) {
		return false;
	}
	for (idx_t base_lane = 0; base_lane < payloads.size(); base_lane++) {
		if (aggregates[base_lane].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
		    payloads[base_lane].kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
		    !payloads[base_lane].expression_tree) {
			continue;
		}
		auto &base = *payloads[base_lane].expression_tree;
		for (idx_t dependent_lane = base_lane + 1; dependent_lane < payloads.size(); dependent_lane++) {
			if (aggregates[dependent_lane].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR ||
			    payloads[dependent_lane].kind != SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE ||
			    !payloads[dependent_lane].expression_tree) {
				continue;
			}
			auto &dependent = *payloads[dependent_lane].expression_tree;
			if (dependent.kind != ExecutionExpressionIRKind::BINARY || dependent.arithmetic_overflow_check ||
			    !dependent.left || !dependent.right ||
			    SljitTypedExpressionTreeComparisonSupported(dependent.binary_op)) {
				continue;
			}
			SljitNativeIntegerBinaryOp native_op;
			if (!TryGetSljitExpressionTreeBinaryOp(dependent.binary_op, native_op)) {
				continue;
			}
			bool base_on_left = true;
			const ExecutionExpressionIR *other_value = nullptr;
			if (SljitExpressionIRStructurallyEqual(base, *dependent.left)) {
				other_value = dependent.right.get();
			} else if (SljitExpressionIRStructurallyEqual(base, *dependent.right)) {
				base_on_left = false;
				other_value = dependent.left.get();
			} else {
				continue;
			}
			auto other_plan = BuildSljitTypedExpressionTreePlan(*other_value, false);
			if (!other_plan.supported || !other_plan.result_is_int64 || !other_plan.fast_path.fast_path_supported) {
				continue;
			}
			codegen_plan.binary_shared_payload = true;
			codegen_plan.binary_base_lane = base_lane;
			codegen_plan.binary_dependent_lane = dependent_lane;
			codegen_plan.binary_base_on_left = base_on_left;
			codegen_plan.binary_root = &dependent;
			codegen_plan.binary_other_value = other_value;
			return true;
		}
	}
	return false;
}

static void AddSljitAggregateSourceUse(vector<pair<idx_t, idx_t>> &source_uses, idx_t source_index) {
	for (auto &entry : source_uses) {
		if (entry.first == source_index) {
			entry.second++;
			return;
		}
	}
	source_uses.emplace_back(source_index, 1);
}

static void CountSljitAggregateExpressionSourceUses(const ExecutionExpressionIR &node,
                                                    vector<pair<idx_t, idx_t>> &source_uses) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE) {
		AddSljitAggregateSourceUse(source_uses, node.ref_index);
		return;
	}
	if (node.left) {
		CountSljitAggregateExpressionSourceUses(*node.left, source_uses);
	}
	if (node.right) {
		CountSljitAggregateExpressionSourceUses(*node.right, source_uses);
	}
	if (node.else_node) {
		CountSljitAggregateExpressionSourceUses(*node.else_node, source_uses);
	}
	for (auto &child : node.children) {
		if (child) {
			CountSljitAggregateExpressionSourceUses(*child, source_uses);
		}
	}
}

static sljit_s32 SljitUngroupedAggregateSourceDataPointerReg(idx_t hoist_idx, bool include_fast_spare_reg) {
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	switch (hoist_idx) {
	case 0:
		return SLJIT_S8;
	case 1:
		return SLJIT_S9;
	case 2:
		if (include_fast_spare_reg) {
			return SLJIT_S6;
		}
		break;
	default:
		break;
	}
#endif
	throw InternalException("SLJIT ungrouped aggregate source data register is out of range");
}

static vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitUngroupedAggregateSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                    idx_t max_hoists, bool include_fast_spare_reg,
                                                    idx_t min_use_count) {
	vector<SljitTypedExpressionTreeDataPointerHoist> result;
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	vector<pair<idx_t, idx_t>> source_uses;
	for (auto &payload : payloads) {
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			AddSljitAggregateSourceUse(source_uses, payload.source_index);
		} else if (payload.expression_tree) {
			CountSljitAggregateExpressionSourceUses(*payload.expression_tree, source_uses);
		}
	}
	std::sort(source_uses.begin(), source_uses.end(),
	          [](const pair<idx_t, idx_t> &left, const pair<idx_t, idx_t> &right) {
		          if (left.second != right.second) {
			          return left.second > right.second;
		          }
		          return left.first < right.first;
	          });
	for (auto &entry : source_uses) {
		if (entry.second < min_use_count || result.size() >= max_hoists) {
			break;
		}
		SljitTypedExpressionTreeDataPointerHoist hoist;
		hoist.source_index = entry.first;
		hoist.data_reg = SljitUngroupedAggregateSourceDataPointerReg(result.size(), include_fast_spare_reg);
		result.push_back(hoist);
	}
#endif
	return result;
}

static bool
BuildSljitUngroupedFusedTypedAggregateCodegenPlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                  const vector<ExecutionRegionAggregateInput> &aggregates,
                                                  SljitUngroupedFusedTypedAggregateCodegenPlan &codegen_plan) {
	if (payloads.size() != aggregates.size() || payloads.size() < 2) {
		return false;
	}
	codegen_plan = SljitUngroupedFusedTypedAggregateCodegenPlan();
	codegen_plan.payloads.resize(payloads.size());
	codegen_plan.fast_path_supported = true;
	bool has_typed_payload = false;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!BuildSljitUngroupedFusedTypedAggregatePayloadPlan(payloads[payload_idx], aggregates[payload_idx],
		                                                       codegen_plan.payloads[payload_idx])) {
			return false;
		}
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		has_typed_payload =
		    has_typed_payload || payloads[payload_idx].kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE;
		codegen_plan.tree_node_count += codegen_plan.payloads[payload_idx].node_count;
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported && codegen_plan.payloads[payload_idx].fast_path.fast_path_supported;
	}
	if (has_typed_payload) {
		TryBuildSljitConditionalSharedAggregatePlan(payloads, aggregates, codegen_plan);
		if (!codegen_plan.conditional_shared_payload) {
			TryBuildSljitBinarySharedAggregatePlan(payloads, aggregates, codegen_plan);
		}
	}
	return has_typed_payload;
}

static void
EmitSljitBinarySharedPayloadValueReg(struct sljit_compiler *compiler,
                                     const SljitUngroupedFusedTypedAggregateCodegenPlan &codegen_plan,
                                     sljit_sw shared_value_offset, bool fast_path, bool no_source_selection,
                                     vector<SljitExpressionTreeOverflowJumps> &overflows,
                                     const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr) {
	D_ASSERT(codegen_plan.binary_shared_payload);
	D_ASSERT(codegen_plan.binary_root);
	D_ASSERT(codegen_plan.binary_other_value);
	idx_t spill_index = 0;
	if (fast_path) {
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *codegen_plan.binary_other_value, spill_index, overflows,
		                                         data_hoists);
	} else if (no_source_selection) {
		EmitSljitTypedExpressionTreeLogicalFastValueReg(compiler, *codegen_plan.binary_other_value, spill_index,
		                                                overflows, data_hoists);
	} else {
		EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *codegen_plan.binary_other_value, spill_index,
		                                                 overflows, data_hoists);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_SP), shared_value_offset);
	SljitNativeIntegerBinaryOp native_op;
	if (!TryGetSljitExpressionTreeBinaryOp(codegen_plan.binary_root->binary_op, native_op)) {
		throw InternalException("Unsupported SLJIT shared aggregate binary operator");
	}
	auto binary_kind = SljitTypedExpressionTreeIntegerKind(*codegen_plan.binary_root);
	auto binary_op = NativeIntegerBinaryOp(binary_kind, native_op);
	if (codegen_plan.binary_base_on_left) {
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	} else {
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R4, 0);
	}
	if (binary_kind == SljitNativeIntegerKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R2, 0, SLJIT_R2, 0);
	}
}

static void EmitFusedTypedAggregateAccumulate(struct sljit_compiler *compiler, AggregatePrimitiveUpdateKind kind,
                                              sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                              sljit_sw saw_value_offset, sljit_s32 value_reg) {
	if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		EmitSljitAggregateAccumulateHugeintInt64(compiler, local_lower_offset, local_upper_offset, saw_value_offset,
		                                         value_reg);
	} else {
		EmitSljitAggregateAccumulateInt64(compiler, local_lower_offset, saw_value_offset, value_reg);
	}
}

static void EmitFusedTypedConditionalSharedSawElseZero(struct sljit_compiler *compiler, sljit_sw saw_value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 14
static constexpr bool SLJIT_HAS_UNGROUPED_CONDITIONAL_HUGEINT_SUM_REGS = true;
static constexpr sljit_s32 SLJIT_UNGROUPED_SHARED_LOWER_REG = SLJIT_S10;
static constexpr sljit_s32 SLJIT_UNGROUPED_SHARED_UPPER_REG = SLJIT_S11;
static constexpr sljit_s32 SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG = SLJIT_S12;
static constexpr sljit_s32 SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG = SLJIT_S13;
#else
static constexpr bool SLJIT_HAS_UNGROUPED_CONDITIONAL_HUGEINT_SUM_REGS = false;
static constexpr sljit_s32 SLJIT_UNGROUPED_SHARED_LOWER_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_UNGROUPED_SHARED_UPPER_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG = SLJIT_R0;
#endif

static void EmitSljitAggregateAccumulateHugeintInt64Regs(struct sljit_compiler *compiler, sljit_s32 lower_reg,
                                                         sljit_s32 upper_reg, sljit_s32 value_reg) {
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, lower_reg, 0, lower_reg, 0, value_reg, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_CARRY);
	sljit_emit_op2(compiler, SLJIT_ADD, upper_reg, 0, upper_reg, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, upper_reg, 0, upper_reg, 0, SLJIT_R3, 0);
}

static void EmitFusedAggregateCommitSumHugeint(struct sljit_compiler *compiler, idx_t lane_idx,
                                               sljit_sw local_lower_offset, sljit_sw local_upper_offset,
                                               sljit_sw saw_value_offset) {
	EmitFusedAggregateAddRowCount(compiler, lane_idx, SLJIT_S2);
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

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedFusedTypedExpressionAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	SljitUngroupedFusedTypedAggregateCodegenPlan codegen_plan;
	if (!BuildSljitUngroupedFusedTypedAggregateCodegenPlan(payloads, aggregates, codegen_plan)) {
		error = "unsupported fused typed aggregate payload shape";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto tree_local_size = NumericCast<sljit_sw>(codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	vector<sljit_sw> local_sum_offsets(payloads.size(), -1);
	vector<sljit_sw> local_sum_upper_offsets(payloads.size(), -1);
	vector<sljit_sw> saw_value_offsets(payloads.size(), -1);
	sljit_sw local_size = tree_local_size;
	sljit_sw shared_fast_value_offset = -1;
	if (codegen_plan.conditional_shared_payload) {
		shared_fast_value_offset = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	const bool use_conditional_hugeint_register_accumulators =
	    SLJIT_HAS_UNGROUPED_CONDITIONAL_HUGEINT_SUM_REGS && codegen_plan.conditional_shared_payload &&
	    aggregates[codegen_plan.shared_lane].primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT &&
	    aggregates[codegen_plan.conditional_lane].primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT;
	sljit_sw register_accumulator_used_offset = -1;
	if (use_conditional_hugeint_register_accumulators) {
		register_accumulator_used_offset = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
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

	auto source_data_hoists = BuildSljitUngroupedAggregateSourceDataPointerHoists(payloads, 2, false, 2);
	auto fast_source_data_hoists = codegen_plan.fast_path_supported
	                                   ? BuildSljitUngroupedAggregateSourceDataPointerHoists(payloads, 3, true, 1)
	                                   : source_data_hoists;
	if (fast_source_data_hoists.size() < source_data_hoists.size()) {
		fast_source_data_hoists = source_data_hoists;
	}
	const bool hoist_source_data_pointers = !source_data_hoists.empty();
	const bool hoist_fast_source_data_pointers = !fast_source_data_hoists.empty();
	const auto fast_data_hoists = hoist_fast_source_data_pointers ? &fast_source_data_hoists : nullptr;
	auto saved_register_count = hoist_source_data_pointers || hoist_fast_source_data_pointers
	                                ? NumericCast<sljit_s32>(10)
	                                : NumericCast<sljit_s32>(7);
	if (use_conditional_hugeint_register_accumulators) {
		saved_register_count = NumericCast<sljit_s32>(14);
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, saved_register_count, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (local_sum_offsets[payload_idx] < 0) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_IMM, 0);
		if (local_sum_upper_offsets[payload_idx] >= 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[payload_idx], SLJIT_IMM,
			               0);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
	}
	if (use_conditional_hugeint_register_accumulators) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), register_accumulator_used_offset, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_UNGROUPED_SHARED_LOWER_REG, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_UNGROUPED_SHARED_UPPER_REG, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG, 0, SLJIT_IMM, 0);
	}
	EmitInitSljitNativeVectorSourceArrays(compiler);
	if (hoist_source_data_pointers) {
		for (auto &hoist : source_data_hoists) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.data_reg, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(hoist.source_index));
		}
	}
	auto emit_fast_source_data_hoists = [&]() {
		if (!hoist_fast_source_data_pointers) {
			return;
		}
		for (idx_t hoist_idx = source_data_hoists.size(); hoist_idx < fast_source_data_hoists.size(); hoist_idx++) {
			auto &hoist = fast_source_data_hoists[hoist_idx];
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.data_reg, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(hoist.source_index));
		}
	};

	vector<SljitExpressionTreeOverflowJumps> overflows;
	struct sljit_jump *fast_done = nullptr;
	struct sljit_jump *selected_fast_done = nullptr;
	struct sljit_jump *done = nullptr;
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

		emit_fast_source_data_hoists();
		if (use_conditional_hugeint_register_accumulators) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), register_accumulator_used_offset, SLJIT_IMM, 1);
		}
		auto fast_loop = sljit_emit_label(compiler);
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		if (codegen_plan.conditional_shared_payload) {
			auto shared_lane = codegen_plan.shared_lane;
			auto conditional_lane = codegen_plan.conditional_lane;
			auto kind = aggregates[shared_lane].primitive_update_kind;
			idx_t shared_spill_index = 0;
			EmitSljitTypedExpressionTreeFastValueReg(compiler, *codegen_plan.shared_value, shared_spill_index,
			                                         overflows, fast_data_hoists);
			if (use_conditional_hugeint_register_accumulators) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S7, 0, SLJIT_R2, 0);
				EmitSljitAggregateAccumulateHugeintInt64Regs(compiler, SLJIT_UNGROUPED_SHARED_LOWER_REG,
				                                             SLJIT_UNGROUPED_SHARED_UPPER_REG, SLJIT_S7);
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), shared_fast_value_offset, SLJIT_R2, 0);
				EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[shared_lane],
				                                  local_sum_upper_offsets[shared_lane], saw_value_offsets[shared_lane],
				                                  SLJIT_R2);
			}

			idx_t predicate_spill_index = 0;
			EmitSljitTypedExpressionTreeFastValueReg(compiler, *codegen_plan.conditional_predicate,
			                                         predicate_spill_index, overflows, fast_data_hoists);
			auto condition_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			if (use_conditional_hugeint_register_accumulators) {
				EmitSljitAggregateAccumulateHugeintInt64Regs(compiler, SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG,
				                                             SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG, SLJIT_S7);
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), shared_fast_value_offset);
				EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[conditional_lane],
				                                  local_sum_upper_offsets[conditional_lane],
				                                  saw_value_offsets[conditional_lane], SLJIT_R2);
			}
			auto conditional_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(condition_false, sljit_emit_label(compiler));
			if (!use_conditional_hugeint_register_accumulators) {
				EmitFusedTypedConditionalSharedSawElseZero(compiler, saw_value_offsets[conditional_lane]);
			}
			sljit_set_label(conditional_done, sljit_emit_label(compiler));
		} else {
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				auto kind = aggregates[payload_idx].primitive_update_kind;
				if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					continue;
				}
				if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
					EmitLoadFusedTypedAggregateReferenceValue(compiler, payloads[payload_idx], false, false, SLJIT_S1);
				} else {
					idx_t payload_spill_index = 0;
					EmitSljitTypedExpressionTreeFastValueReg(compiler, *payloads[payload_idx].expression_tree,
					                                         payload_spill_index, overflows, fast_data_hoists);
				}
				EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
				                                  local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
				                                  SLJIT_R2);
			}
		}
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		emit_fast_source_data_hoists();
		if (use_conditional_hugeint_register_accumulators) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), register_accumulator_used_offset, SLJIT_IMM, 1);
		}
		auto selected_fast_loop = sljit_emit_label(compiler);
		selected_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitExpressionTreeLogicalIndex(compiler);
		if (codegen_plan.conditional_shared_payload) {
			auto shared_lane = codegen_plan.shared_lane;
			auto conditional_lane = codegen_plan.conditional_lane;
			auto kind = aggregates[shared_lane].primitive_update_kind;
			idx_t shared_spill_index = 0;
			EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *codegen_plan.shared_value, shared_spill_index,
			                                                 overflows, fast_data_hoists);
			if (use_conditional_hugeint_register_accumulators) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S7, 0, SLJIT_R2, 0);
				EmitSljitAggregateAccumulateHugeintInt64Regs(compiler, SLJIT_UNGROUPED_SHARED_LOWER_REG,
				                                             SLJIT_UNGROUPED_SHARED_UPPER_REG, SLJIT_S7);
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), shared_fast_value_offset, SLJIT_R2, 0);
				EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[shared_lane],
				                                  local_sum_upper_offsets[shared_lane], saw_value_offsets[shared_lane],
				                                  SLJIT_R2);
			}

			idx_t predicate_spill_index = 0;
			EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *codegen_plan.conditional_predicate,
			                                                 predicate_spill_index, overflows, fast_data_hoists);
			auto condition_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			if (use_conditional_hugeint_register_accumulators) {
				EmitSljitAggregateAccumulateHugeintInt64Regs(compiler, SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG,
				                                             SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG, SLJIT_S7);
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), shared_fast_value_offset);
				EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[conditional_lane],
				                                  local_sum_upper_offsets[conditional_lane],
				                                  saw_value_offsets[conditional_lane], SLJIT_R2);
			}
			auto conditional_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(condition_false, sljit_emit_label(compiler));
			if (!use_conditional_hugeint_register_accumulators) {
				EmitFusedTypedConditionalSharedSawElseZero(compiler, saw_value_offsets[conditional_lane]);
			}
			sljit_set_label(conditional_done, sljit_emit_label(compiler));
		} else {
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				auto kind = aggregates[payload_idx].primitive_update_kind;
				if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					continue;
				}
				if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
					EmitLoadFusedTypedAggregateReferenceValue(compiler, payloads[payload_idx], true, false, SLJIT_S3);
				} else {
					idx_t payload_spill_index = 0;
					EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *payloads[payload_idx].expression_tree,
					                                                 payload_spill_index, overflows, fast_data_hoists);
				}
				EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
				                                  local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
				                                  SLJIT_R2);
			}
		}
		EmitNextSljitNativeVectorLoop(compiler, selected_fast_loop);

		sljit_set_label(use_generic_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	if (codegen_plan.conditional_shared_payload) {
		auto shared_lane = codegen_plan.shared_lane;
		auto conditional_lane = codegen_plan.conditional_lane;
		auto kind = aggregates[shared_lane].primitive_update_kind;
		idx_t slot_index = 0;
		auto shared_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *codegen_plan.shared_value, slot_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), shared_slot.valid_offset);
		auto shared_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), shared_slot.value_offset);
		EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[shared_lane],
		                                  local_sum_upper_offsets[shared_lane], saw_value_offsets[shared_lane],
		                                  SLJIT_R2);
		auto shared_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto shared_done_label = sljit_emit_label(compiler);
		sljit_set_label(shared_invalid, shared_done_label);
		sljit_set_label(shared_done, shared_done_label);

		auto predicate_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *codegen_plan.conditional_predicate, slot_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.valid_offset);
		auto condition_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.value_offset);
		auto condition_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), shared_slot.valid_offset);
		auto conditional_value_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), shared_slot.value_offset);
		EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[conditional_lane],
		                                  local_sum_upper_offsets[conditional_lane],
		                                  saw_value_offsets[conditional_lane], SLJIT_R2);
		auto conditional_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto condition_not_true = sljit_emit_label(compiler);
		sljit_set_label(condition_null, condition_not_true);
		sljit_set_label(condition_false, condition_not_true);
		EmitFusedTypedConditionalSharedSawElseZero(compiler, saw_value_offsets[conditional_lane]);
		auto conditional_done_label = sljit_emit_label(compiler);
		sljit_set_label(conditional_value_null, conditional_done_label);
		sljit_set_label(conditional_done, conditional_done_label);
	} else {
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto kind = aggregates[payload_idx].primitive_update_kind;
			if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				continue;
			}
			vector<sljit_jump *> payload_skip_jumps;
			if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
				auto source_is_null =
				    EmitLoadFusedTypedAggregateReferenceValue(compiler, payloads[payload_idx], true, true, SLJIT_S3);
				if (source_is_null) {
					payload_skip_jumps.push_back(source_is_null);
				}
			} else {
				idx_t payload_slot_index = 0;
				auto payload_slot = EmitSljitTypedExpressionTreeValue(compiler, *payloads[payload_idx].expression_tree,
				                                                      payload_slot_index, overflows);
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.valid_offset);
				payload_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
			}
			EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
			                                  local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                  SLJIT_R2);
			auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto payload_skip_label = sljit_emit_label(compiler);
			for (auto payload_skip : payload_skip_jumps) {
				sljit_set_label(payload_skip, payload_skip_label);
			}
			sljit_set_label(payload_done, sljit_emit_label(compiler));
		}
	}
	EmitNextSljitNativeVectorLoop(compiler, loop);

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
	if (selected_fast_done) {
		sljit_set_label(selected_fast_done, done_label);
	}
	sljit_set_label(done, done_label);
	if (use_conditional_hugeint_register_accumulators) {
		auto shared_lane = codegen_plan.shared_lane;
		auto conditional_lane = codegen_plan.conditional_lane;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), register_accumulator_used_offset);
		auto no_register_accumulator = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[shared_lane],
		               SLJIT_UNGROUPED_SHARED_LOWER_REG, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[shared_lane],
		               SLJIT_UNGROUPED_SHARED_UPPER_REG, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[conditional_lane],
		               SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[conditional_lane],
		               SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, 0);
		auto empty_input = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, 1);
		sljit_set_label(empty_input, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[shared_lane], SLJIT_R0, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[conditional_lane], SLJIT_R0, 0);
		sljit_set_label(no_register_accumulator, sljit_emit_label(compiler));
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = aggregates[payload_idx].primitive_update_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitFusedAggregateCommitCountStar(compiler, payload_idx);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			EmitFusedAggregateCommitSumHugeint(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                   local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx]);
		} else {
			EmitFusedAggregateCommitSumInt64(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                 saw_value_offsets[payload_idx]);
		}
	}
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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
	return SljitAggregateTypedPayloadPlanSupported(payload_plan, aggregate);
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
	EmitInitSljitNativeVectorLoop(compiler);
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
	struct sljit_jump *selected_fast_done = nullptr;
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
			EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
			                                  local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                  SLJIT_R2);
			EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
		}
		sljit_set_label(predicate_false, sljit_emit_label(compiler));
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto selected_fast_loop = sljit_emit_label(compiler);
		selected_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitExpressionTreeLogicalIndex(compiler);
		idx_t selected_predicate_spill_index = 0;
		EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, predicate, selected_predicate_spill_index,
		                                                 overflows);
		auto selected_predicate_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto kind = aggregates[payload_idx].primitive_update_kind;
			if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
				continue;
			}
			idx_t payload_spill_index = 0;
			EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *payloads[payload_idx].expression_tree,
			                                                 payload_spill_index, overflows);
			EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
			                                  local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                  SLJIT_R2);
			EmitSljitAggregateIncrementLocalCount(compiler, local_count_offsets[payload_idx]);
		}
		sljit_set_label(selected_predicate_false, sljit_emit_label(compiler));
		EmitNextSljitNativeVectorLoop(compiler, selected_fast_loop);

		sljit_set_label(use_generic_selected_loop, sljit_emit_label(compiler));
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
		EmitFusedTypedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
		                                  local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
		                                  SLJIT_R2);
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
	EmitNextSljitNativeVectorLoop(compiler, loop);

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
	if (selected_fast_done) {
		sljit_set_label(selected_fast_done, done_label);
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

	return FinishSljitCode(compiler, function, error);
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
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT) {
		return aggregate.child_types.size() == 1 &&
		       payload.return_type.InternalType() == aggregate.child_types[0].InternalType() &&
		       payload.kind == SljitNativeRegionExpressionKind::REFERENCE;
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

static bool SljitFusedGroupedTypedAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
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
	return payload.kind == SljitNativeRegionExpressionKind::REFERENCE ||
	       (payload.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE &&
	        payload.expression_tree != nullptr);
}

struct SljitGroupedFusedTypedAggregateCodegenPlan {
	vector<SljitTypedExpressionTreePlan> payloads;
	idx_t tree_node_count = 0;
	bool has_typed_payload = false;
	bool fast_path_supported = false;
};

static bool BuildSljitGroupedFusedTypedAggregateCodegenPlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                            const vector<ExecutionRegionAggregateInput> &aggregates,
                                                            const ExecutionRegionAggregateContract &contract,
                                                            SljitGroupedFusedTypedAggregateCodegenPlan &codegen_plan) {
	if (!contract.grouped_state_layout_ready || payloads.size() != aggregates.size() || payloads.empty()) {
		return false;
	}
	codegen_plan = SljitGroupedFusedTypedAggregateCodegenPlan();
	codegen_plan.payloads.resize(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &payload = payloads[payload_idx];
		if (!SljitFusedGroupedTypedAggregatePayloadSupported(payload, aggregate, contract)) {
			return false;
		}
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		auto &payload_plan = codegen_plan.payloads[payload_idx];
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			payload_plan.supported = true;
			payload_plan.result_kind = payload.integer_kind;
			payload_plan.result_is_int64 = true;
			payload_plan.fast_path.fast_path_supported = true;
			continue;
		}
		payload_plan = BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false);
		if (!SljitAggregateTypedPayloadPlanSupported(payload_plan, aggregate)) {
			return false;
		}
		codegen_plan.has_typed_payload = true;
		codegen_plan.tree_node_count += payload_plan.node_count;
	}
	codegen_plan.fast_path_supported = true;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported && codegen_plan.payloads[payload_idx].fast_path.fast_path_supported;
	}
	return codegen_plan.has_typed_payload;
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

static void EmitSljitGroupedAggregateAccumulateInt64ImmediateNoStateSet(struct sljit_compiler *compiler,
                                                                        sljit_s32 base_reg, idx_t state_offset,
                                                                        idx_t value_offset, sljit_s32 value_reg) {
	const auto offset = NumericCast<sljit_sw>(state_offset + value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(base_reg), offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), offset, SLJIT_R3, 0);
}

static void EmitSljitGroupedAggregateIncrementInt64Immediate(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                             idx_t state_offset, idx_t value_offset) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, base_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
}

static void EmitSljitGroupedAggregateIncrementInt64ImmediateDirect(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                                   idx_t state_offset, idx_t value_offset) {
	const auto offset = NumericCast<sljit_sw>(state_offset + value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(base_reg), offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), offset, SLJIT_R3, 0);
}

static bool SljitGroupedFusedCountOnlyFastPathSupported(const vector<ExecutionRegionAggregateInput> &aggregates) {
	for (auto &aggregate : aggregates) {
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::COUNT) {
			return false;
		}
	}
	return true;
}

static void EmitSljitGroupedFusedCountOnlyFastPathGuards(struct sljit_compiler *compiler, bool has_count_payload,
                                                         vector<sljit_jump *> &use_generic_loop) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_address_sel));
	use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	if (!has_count_payload) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));
	use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
}

static void EmitSljitAccumulateHugeintUpperIfNeeded(struct sljit_compiler *compiler, sljit_s32 base_reg,
                                                    sljit_sw upper_offset, sljit_s32 upper_value_reg,
                                                    sljit_s32 carry_reg) {
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R2, 0, upper_value_reg, 0, carry_reg, 0);
	auto no_upper_update = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(base_reg), upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, upper_value_reg, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, carry_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), upper_offset, SLJIT_R3, 0);
	sljit_set_label(no_upper_update, sljit_emit_label(compiler));
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
	EmitSljitAccumulateHugeintUpperIfNeeded(compiler, SLJIT_R0, offsetof(hugeint_t, upper), SLJIT_R4, SLJIT_R1);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, base_reg, state_offset, state_is_set_offset);
}

static void EmitSljitGroupedAggregateAccumulateHugeintImmediateNoStateSet(struct sljit_compiler *compiler,
                                                                          sljit_s32 base_reg, idx_t state_offset,
                                                                          idx_t value_offset, sljit_s32 value_reg) {
	const auto lower_offset = NumericCast<sljit_sw>(state_offset + value_offset + offsetof(hugeint_t, lower));
	const auto upper_offset = NumericCast<sljit_sw>(state_offset + value_offset + offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(base_reg), lower_offset);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), lower_offset, SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	EmitSljitAccumulateHugeintUpperIfNeeded(compiler, base_reg, upper_offset, SLJIT_R4, SLJIT_R1);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error) {
	if (!contract.grouped_state_layout_ready || payloads.size() != aggregates.size() || payloads.empty()) {
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
	EmitInitSljitNativeVectorLoop(compiler);

	struct sljit_jump *fast_count_done = nullptr;
	if (SljitGroupedFusedCountOnlyFastPathSupported(aggregates)) {
		bool has_count_payload = false;
		for (auto &aggregate : aggregates) {
			has_count_payload =
			    has_count_payload || aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT;
		}

		vector<sljit_jump *> use_generic_loop;
		EmitSljitGroupedFusedCountOnlyFastPathGuards(compiler, has_count_payload, use_generic_loop);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
		auto fast_loop = sljit_emit_label(compiler);
		fast_count_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM2(SLJIT_S3, SLJIT_S1), 3);
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
			EmitSljitGroupedAggregateIncrementInt64ImmediateDirect(compiler, SLJIT_S4, state_offset,
			                                                       aggregate.primitive_update_state_value_offset);
		}
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		auto generic_loop = sljit_emit_label(compiler);
		for (auto jump : use_generic_loop) {
			sljit_set_label(jump, generic_loop);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_S4, SLJIT_S3);

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
		                                  SLJIT_R1, true);
		auto source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 aggregate.primitive_update_state_value_offset);
		} else if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array),
			                                  payload_idx, payload.integer_kind, SLJIT_R1, SLJIT_R2);
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                  aggregate.primitive_update_state_value_offset,
			                                                  aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		} else {
			EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array),
			                                  payload_idx, payload.integer_kind, SLJIT_R1, SLJIT_R2);
			EmitSljitGroupedAggregateAccumulateHugeintImmediate(
			    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		}
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		sljit_set_label(source_is_null, payload_invalid);
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}

	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	if (fast_count_done) {
		sljit_set_label(fast_count_done, done_label);
	}
	sljit_set_label(done, done_label);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedFusedTypedExpressionAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error) {
	SljitGroupedFusedTypedAggregateCodegenPlan codegen_plan;
	if (!BuildSljitGroupedFusedTypedAggregateCodegenPlan(payloads, aggregates, contract, codegen_plan)) {
		error = "unsupported fused grouped typed aggregate payload shape";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto state_ptr_offset = NumericCast<sljit_sw>(codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	const auto logical_index_offset = state_ptr_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = logical_index_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	vector<SljitExpressionTreeOverflowJumps> overflows;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	EmitInitSljitNativeVectorSourceArrays(compiler);

	struct sljit_jump *fast_done = nullptr;
	struct sljit_jump *use_generic_loop = nullptr;
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

		auto fast_loop = sljit_emit_label(compiler);
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_S4, SLJIT_S1);

		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
				                                                 aggregate.primitive_update_state_value_offset);
				continue;
			}

			auto &payload = payloads[payload_idx];
			if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				EmitLoadFusedTypedAggregateReferenceValue(compiler, payload, false, false, SLJIT_S1);
			} else {
				idx_t payload_spill_index = 0;
				EmitSljitTypedExpressionTreeFastValueReg(compiler, *payload.expression_tree, payload_spill_index,
				                                         overflows);
			}
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
				EmitSljitGroupedAggregateAccumulateInt64Immediate(
				    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
				    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
			} else {
				EmitSljitGroupedAggregateAccumulateHugeintImmediate(
				    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
				    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
			}
		}
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), logical_index_offset, SLJIT_S3, 0);
	EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_R0, SLJIT_S3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), state_ptr_offset, SLJIT_R0, 0);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];

		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_R0, state_offset,
			                                                 aggregate.primitive_update_state_value_offset);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
			continue;
		}

		auto &payload = payloads[payload_idx];
		vector<sljit_jump *> payload_skip_jumps;
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			auto source_is_null = EmitLoadFusedTypedAggregateReferenceValue(compiler, payload, true, true, SLJIT_S3);
			if (source_is_null) {
				payload_skip_jumps.push_back(source_is_null);
			}
		} else {
			idx_t payload_slot_index = 0;
			auto payload_slot =
			    EmitSljitTypedExpressionTreeValue(compiler, *payload.expression_tree, payload_slot_index, overflows);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.valid_offset);
			payload_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
		}

		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S3, state_offset,
			                                                  aggregate.primitive_update_state_value_offset,
			                                                  aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		} else {
			EmitSljitGroupedAggregateAccumulateHugeintImmediate(
			    compiler, SLJIT_S3, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		}
		if (!payload_skip_jumps.empty()) {
			auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto payload_skip_label = sljit_emit_label(compiler);
			for (auto payload_skip : payload_skip_jumps) {
				sljit_set_label(payload_skip, payload_skip_label);
			}
			sljit_set_label(payload_done, sljit_emit_label(compiler));
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
	}
	EmitNextSljitNativeVectorLoop(compiler, loop);

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
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

struct SljitPerfectHashGroupPlan {
	SljitNativeRegionExpressionKind expression_kind = SljitNativeRegionExpressionKind::REFERENCE;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	idx_t source_index = DConstants::INVALID_INDEX;
	idx_t string_compress_target_size = 0;
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

static bool SljitPerfectHashGroupExpressionSupported(const SljitNativeRegionExpressionPlan &expr,
                                                     const ExecutionRegionGroupInput &group) {
	if (expr.return_type.InternalType() != group.type.InternalType()) {
		return false;
	}
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return true;
	case SljitNativeRegionExpressionKind::STRING_COMPRESS:
		return group.type.InternalType() == PhysicalType::UINT8 && expr.string_compress_target_size == sizeof(uint8_t);
	default:
		return false;
	}
}

static bool TryBuildSljitPerfectHashGroupPlans(const vector<ExecutionRegionGroupInput> &groups,
                                               const vector<SljitNativeRegionExpressionPlan> &group_expressions,
                                               const ExecutionRegionAggregateContract &contract,
                                               vector<SljitPerfectHashGroupPlan> &result) {
	if (contract.kind != ExecutionRegionAggregateOperatorKind::PERFECT_HASH ||
	    contract.perfect_required_bits.size() != groups.size() ||
	    contract.perfect_group_minima.size() != groups.size() ||
	    (!group_expressions.empty() && group_expressions.size() != groups.size())) {
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
		if (group_expressions.empty()) {
			plan.expression_kind = SljitNativeRegionExpressionKind::REFERENCE;
			plan.source_index = group.input_index;
		} else {
			auto &group_expression = group_expressions[group_idx];
			if (!SljitPerfectHashGroupExpressionSupported(group_expression, group)) {
				return false;
			}
			plan.expression_kind = group_expression.kind;
			plan.source_index = group_expression.source_index;
			plan.string_compress_target_size = group_expression.string_compress_target_size;
		}
		plan.shift = shift;
		result.push_back(plan);
	}
	return true;
}

static bool SljitCanPrecomputePerfectHashStringGroupOffset(const vector<SljitPerfectHashGroupPlan> &groups) {
	if (string_t::PREFIX_LENGTH == 0) {
		return false;
	}
	idx_t string_group_count = 0;
	for (auto &group : groups) {
		if (group.expression_kind == SljitNativeRegionExpressionKind::STRING_COMPRESS) {
			string_group_count++;
		}
	}
	return string_group_count > 1;
}

enum class SljitFusedAggregateGroupIndexMode : uint8_t { LOGICAL, SELECTED_NULLABLE, SELECTED_PRESENT };

static void EmitLoadFusedAggregateGroupSourceIndex(
    struct sljit_compiler *compiler, idx_t group_idx, sljit_s32 target_reg,
    SljitFusedAggregateGroupIndexMode mode = SljitFusedAggregateGroupIndexMode::SELECTED_NULLABLE,
    sljit_s32 group_sel_array_base_reg = 0) {
	if (mode == SljitFusedAggregateGroupIndexMode::LOGICAL) {
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
		return;
	}
	if (group_sel_array_base_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, group_sel_array_base_reg, 0);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_sel_array));
	}
	if (mode == SljitFusedAggregateGroupIndexMode::SELECTED_PRESENT) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
		return;
	}
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
                                                   sljit_s32 target_reg, bool use_hoisted_group_data,
                                                   sljit_s32 group_data_reg, sljit_s32 group_data_array_base_reg) {
	if (use_hoisted_group_data) {
		if (group_data_reg != SLJIT_R0) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, group_data_reg, 0);
		}
	} else if (group_data_array_base_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(group_data_array_base_reg),
		               SljitPointerArrayOffset(group_idx));
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_data_array));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	}
	sljit_emit_op1(compiler, NativeIntegerLoadOp(kind), target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg),
	               NativeIntegerDataScale(kind));
}

static constexpr sljit_sw SLJIT_STRING_T_SHIFT = 4;

static void EmitLoadFusedAggregateGroupMiniStringCompressData(struct sljit_compiler *compiler, idx_t group_idx,
                                                              sljit_s32 index_reg, sljit_s32 target_reg,
                                                              bool use_hoisted_group_data, sljit_s32 group_data_reg,
                                                              bool may_be_empty, bool use_precomputed_string_offset,
                                                              sljit_s32 group_data_array_base_reg) {
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;

	if (use_hoisted_group_data) {
		if (group_data_reg != SLJIT_R0) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, group_data_reg, 0);
		}
	} else if (group_data_array_base_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(group_data_array_base_reg),
		               SljitPointerArrayOffset(group_idx));
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_data_array));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
	}
	if (!use_precomputed_string_offset) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, index_reg, 0, SLJIT_IMM, SLJIT_STRING_T_SHIFT);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);

	if (string_t::PREFIX_LENGTH > 0) {
		if (!may_be_empty) {
			// UTINYINT string compression only appears for one-byte strings here; nonzero minima rule out empty keys.
			sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM1(SLJIT_R0), STRING_INLINE_PREFIX_OFFSET);
			sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, target_reg, 0, SLJIT_IMM, 1);
			return;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM1(SLJIT_R0), STRING_INLINE_PREFIX_OFFSET);
		sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, SLJIT_R3, 0, target_reg, 0);
		auto not_empty = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_IMM, 0);
		sljit_set_label(not_empty, sljit_emit_label(compiler));
		return;
	}

	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
	auto empty_string = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	auto non_inlined =
	    sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R3, 0, SLJIT_IMM, NumericCast<sljit_sw>(string_t::INLINE_LENGTH));

	sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM1(SLJIT_R0), STRING_INLINE_PREFIX_OFFSET);
	auto have_first_byte = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), STRING_POINTER_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM1(SLJIT_R4), 0);
	sljit_set_label(have_first_byte, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, SLJIT_R3, 0, target_reg, 0);
	auto have_result = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(empty_string, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_IMM, 0);

	sljit_set_label(have_result, sljit_emit_label(compiler));
}

static void EmitLoadFusedAggregateGroupData(struct sljit_compiler *compiler, idx_t group_idx,
                                            const SljitPerfectHashGroupPlan &group, sljit_s32 index_reg,
                                            sljit_s32 target_reg, bool use_hoisted_group_data, sljit_s32 group_data_reg,
                                            bool use_precomputed_string_offset = false,
                                            sljit_s32 group_data_array_base_reg = 0) {
	if (group.expression_kind == SljitNativeRegionExpressionKind::STRING_COMPRESS) {
		EmitLoadFusedAggregateGroupMiniStringCompressData(compiler, group_idx, index_reg, target_reg,
		                                                  use_hoisted_group_data, group_data_reg, group.minimum == 0,
		                                                  use_precomputed_string_offset, group_data_array_base_reg);
		return;
	}
	EmitLoadFusedAggregateGroupIntegerData(compiler, group_idx, group.integer_kind, index_reg, target_reg,
	                                       use_hoisted_group_data, group_data_reg, group_data_array_base_reg);
}

static constexpr idx_t SLJIT_LOCAL_PERFECT_HASH_MAX_GROUPS = 16;
static constexpr idx_t SLJIT_EAGER_ZERO_SPARSE_LOCAL_MAX_GROUPS = 64;
static constexpr idx_t SLJIT_SPARSE_LOCAL_PERFECT_HASH_MAX_GROUPS = 1024;
static constexpr idx_t SLJIT_DEFERRED_PERFECT_HASH_FLAG_MAX_GROUPS = 1024;

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 8
static constexpr bool SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG = true;
static constexpr sljit_s32 SLJIT_PERFECT_HASH_STATE_REG = SLJIT_S7;
static constexpr sljit_s32 SLJIT_PERFECT_HASH_SAVED_REG_COUNT = 8;
#else
static constexpr bool SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG = false;
static constexpr sljit_s32 SLJIT_PERFECT_HASH_STATE_REG = SLJIT_S4;
static constexpr sljit_s32 SLJIT_PERFECT_HASH_SAVED_REG_COUNT = 7;
#endif

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
static constexpr bool SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS = true;
static constexpr sljit_s32 SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT = 10;
#else
static constexpr bool SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS = false;
static constexpr sljit_s32 SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT = SLJIT_PERFECT_HASH_SAVED_REG_COUNT;
#endif

#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
static constexpr bool SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS = true;
static constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER0_REG = SLJIT_S6;
static constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER1_REG = SLJIT_S8;
static constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER2_REG = SLJIT_S9;
static constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_SAVED_REG_COUNT = 10;
#else
static constexpr bool SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS = false;
static constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER0_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER1_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_LOWER2_REG = SLJIT_R0;
static constexpr sljit_s32 SLJIT_SPARSE_LOCAL_RUN_SAVED_REG_COUNT = SLJIT_PERFECT_HASH_SAVED_REG_COUNT;
#endif

static sljit_s32 SljitPerfectHashGroupDataPointerReg(idx_t group_idx) {
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	switch (group_idx) {
	case 0:
		return SLJIT_S8;
	case 1:
		return SLJIT_S9;
	default:
		break;
	}
#endif
	throw InternalException("SLJIT perfect-hash group data register is out of range");
}

static sljit_s32 SljitPerfectHashSourceDataPointerReg(idx_t hoist_idx, bool include_fast_validity_reg) {
	if (hoist_idx < 2) {
		return SljitPerfectHashGroupDataPointerReg(hoist_idx);
	}
	if (include_fast_validity_reg && hoist_idx == 2) {
		return SLJIT_S6;
	}
	throw InternalException("SLJIT perfect-hash source data register is out of range");
}

static bool TryGetSljitAggregateDataPointerHoist(const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists,
                                                 idx_t source_index, sljit_s32 &data_reg) {
	if (!data_hoists) {
		return false;
	}
	for (auto &hoist : *data_hoists) {
		if (hoist.source_index == source_index) {
			data_reg = hoist.data_reg;
			return true;
		}
	}
	return false;
}

static vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitPerfectHashSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                             idx_t max_hoists = 2, bool include_fast_validity_reg = false) {
	vector<SljitTypedExpressionTreeDataPointerHoist> result;
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	vector<pair<idx_t, idx_t>> source_uses;
	for (auto &payload : payloads) {
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			AddSljitAggregateSourceUse(source_uses, payload.source_index);
		} else if (payload.expression_tree) {
			CountSljitAggregateExpressionSourceUses(*payload.expression_tree, source_uses);
		}
	}
	std::sort(source_uses.begin(), source_uses.end(),
	          [](const pair<idx_t, idx_t> &left, const pair<idx_t, idx_t> &right) {
		          if (left.second != right.second) {
			          return left.second > right.second;
		          }
		          return left.first < right.first;
	          });
	for (auto &entry : source_uses) {
		if (entry.second < 2 || result.size() >= max_hoists) {
			break;
		}
		SljitTypedExpressionTreeDataPointerHoist hoist;
		hoist.source_index = entry.first;
		hoist.data_reg = SljitPerfectHashSourceDataPointerReg(result.size(), include_fast_validity_reg);
		result.push_back(hoist);
	}
#endif
	return result;
}

static vector<SljitTypedExpressionTreeDataPointerHoist>
BuildSljitPerfectHashSpareFastSourceDataPointerHoists(const vector<SljitNativeRegionExpressionPlan> &payloads) {
	vector<SljitTypedExpressionTreeDataPointerHoist> result;
#if defined(SLJIT_NUMBER_OF_SAVED_REGISTERS) && SLJIT_NUMBER_OF_SAVED_REGISTERS >= 10
	vector<pair<idx_t, idx_t>> source_uses;
	for (auto &payload : payloads) {
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			AddSljitAggregateSourceUse(source_uses, payload.source_index);
		} else if (payload.expression_tree) {
			CountSljitAggregateExpressionSourceUses(*payload.expression_tree, source_uses);
		}
	}
	std::sort(source_uses.begin(), source_uses.end(),
	          [](const pair<idx_t, idx_t> &left, const pair<idx_t, idx_t> &right) {
		          if (left.second != right.second) {
			          return left.second > right.second;
		          }
		          return left.first < right.first;
	          });
	if (!source_uses.empty() && source_uses[0].second >= 2) {
		SljitTypedExpressionTreeDataPointerHoist hoist;
		hoist.source_index = source_uses[0].first;
		hoist.data_reg = SLJIT_S6;
		result.push_back(hoist);
	}
#endif
	return result;
}

struct SljitLocalPerfectHashAggregateLane {
	sljit_sw lower_offset = -1;
	sljit_sw upper_offset = -1;
	sljit_sw saw_offset = -1;
	sljit_sw count_offset = -1;
	bool value_always_seen = false;
	bool local_lower_never_overflows = false;
};

struct SljitLocalPerfectHashAggregatePlan {
	bool enabled = false;
	bool sparse = false;
	bool sparse_eager_zero = false;
	bool group_seen_is_byte = false;
	idx_t group_count = 0;
	idx_t sparse_count_seen_lane = DConstants::INVALID_INDEX;
	sljit_sw group_seen_offset = -1;
	sljit_sw active_groups_offset = -1;
	sljit_sw active_count_offset = -1;
	sljit_sw group_payload_offset = -1;
	sljit_sw group_payload_stride = 0;
	vector<SljitLocalPerfectHashAggregateLane> lanes;
};

struct SljitSparseLocalRunCachedLane {
	idx_t payload_idx = DConstants::INVALID_INDEX;
	sljit_s32 lower_reg = 0;
};

struct SljitDeferredPerfectHashFlagPlan {
	bool enabled = false;
	idx_t group_count = 0;
	sljit_sw group_seen_offset = -1;
};

static sljit_sw AllocateSljitLocalPerfectHashArray(sljit_sw &local_size, idx_t group_count) {
	auto result = local_size;
	local_size += NumericCast<sljit_sw>(group_count * sizeof(sljit_sw));
	return result;
}

static sljit_sw AlignSljitLocalSize(sljit_sw local_size, sljit_sw alignment) {
	D_ASSERT(alignment > 0);
	return (local_size + alignment - 1) & ~(alignment - 1);
}

static sljit_sw AllocateSljitLocalPerfectHashByteArray(sljit_sw &local_size, idx_t group_count) {
	auto result = local_size;
	local_size += NumericCast<sljit_sw>(group_count);
	local_size = AlignSljitLocalSize(local_size, NumericCast<sljit_sw>(sizeof(sljit_sw)));
	return result;
}

static bool TryBuildSljitLocalPerfectHashAggregatePlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                                       const ExecutionRegionAggregateContract &contract,
                                                       const vector<bool> &payloads_not_null, sljit_sw &local_size,
                                                       SljitLocalPerfectHashAggregatePlan &result) {
	if (contract.perfect_required_bits_total >= 8 * sizeof(idx_t)) {
		return false;
	}
	const idx_t group_count = idx_t(1) << contract.perfect_required_bits_total;
	if (group_count == 0 || group_count > SLJIT_SPARSE_LOCAL_PERFECT_HASH_MAX_GROUPS) {
		return false;
	}
	result.enabled = true;
	result.sparse = group_count > SLJIT_LOCAL_PERFECT_HASH_MAX_GROUPS;
	if (result.sparse && !SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG) {
		return false;
	}
	result.group_count = group_count;
	idx_t count_star_count = 0;
	idx_t count_star_lane = DConstants::INVALID_INDEX;
	if (result.sparse) {
		for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
			if (aggregates[aggregate_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				count_star_count++;
				count_star_lane = aggregate_idx;
			}
		}
		if (count_star_count == 1) {
			result.sparse_count_seen_lane = count_star_lane;
		}
	}
	if (result.sparse) {
		if (result.sparse_count_seen_lane == DConstants::INVALID_INDEX) {
			result.group_seen_is_byte = true;
			result.group_seen_offset = AllocateSljitLocalPerfectHashByteArray(local_size, group_count);
		}
		result.active_groups_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
		result.active_count_offset = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	} else {
		result.group_seen_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
	}
	result.lanes.resize(aggregates.size());
	if (result.sparse) {
		for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
			auto &lane = result.lanes[aggregate_idx];
			lane.value_always_seen = aggregate_idx < payloads_not_null.size() && payloads_not_null[aggregate_idx];
			switch (aggregates[aggregate_idx].primitive_update_kind) {
			case AggregatePrimitiveUpdateKind::COUNT_STAR:
				lane.count_offset = result.group_payload_stride;
				result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				break;
			case AggregatePrimitiveUpdateKind::SUM_INT64:
				lane.lower_offset = result.group_payload_stride;
				result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				if (!lane.value_always_seen) {
					lane.saw_offset = result.group_payload_stride;
					result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				}
				break;
			case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
				lane.lower_offset = result.group_payload_stride;
				result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				lane.upper_offset = result.group_payload_stride;
				result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				if (!lane.value_always_seen) {
					lane.saw_offset = result.group_payload_stride;
					result.group_payload_stride += NumericCast<sljit_sw>(sizeof(sljit_sw));
				}
				break;
			default:
				return false;
			}
		}
		sljit_sw padded_stride = 1;
		while (padded_stride < result.group_payload_stride) {
			padded_stride <<= 1;
		}
		result.group_payload_stride = padded_stride;
		result.group_payload_offset = local_size;
		local_size += NumericCast<sljit_sw>(group_count) * result.group_payload_stride;
		bool payloads_always_seen = result.sparse_count_seen_lane != DConstants::INVALID_INDEX;
		for (auto &lane : result.lanes) {
			payloads_always_seen = payloads_always_seen && lane.saw_offset < 0;
		}
		result.sparse_eager_zero = payloads_always_seen && group_count <= SLJIT_EAGER_ZERO_SPARSE_LOCAL_MAX_GROUPS;
		return true;
	}
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregates.size(); aggregate_idx++) {
		auto &lane = result.lanes[aggregate_idx];
		lane.value_always_seen = aggregate_idx < payloads_not_null.size() && payloads_not_null[aggregate_idx];
		switch (aggregates[aggregate_idx].primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
			lane.count_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			break;
		case AggregatePrimitiveUpdateKind::SUM_INT64:
			lane.lower_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			if (!lane.value_always_seen) {
				lane.saw_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			}
			break;
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			lane.lower_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			lane.upper_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			if (!lane.value_always_seen) {
				lane.saw_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
			}
			break;
		default:
			return false;
		}
	}
	return true;
}

static bool TryBuildSljitDeferredPerfectHashFlagPlan(const vector<ExecutionRegionAggregateInput> &aggregates,
                                                     const ExecutionRegionAggregateContract &contract,
                                                     sljit_sw &local_size, SljitDeferredPerfectHashFlagPlan &result) {
	if (contract.perfect_required_bits_total >= 8 * sizeof(idx_t)) {
		return false;
	}
	const idx_t group_count = idx_t(1) << contract.perfect_required_bits_total;
	if (group_count == 0 || group_count > SLJIT_DEFERRED_PERFECT_HASH_FLAG_MAX_GROUPS) {
		return false;
	}
	for (auto &aggregate : aggregates) {
		switch (aggregate.primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::COUNT_STAR:
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			break;
		default:
			return false;
		}
	}
	result.enabled = true;
	result.group_count = group_count;
	result.group_seen_offset = AllocateSljitLocalPerfectHashArray(local_size, group_count);
	return true;
}

static void EmitLoadSljitLocalArrayValue(struct sljit_compiler *compiler, sljit_sw array_offset, sljit_s32 index_reg,
                                         sljit_s32 target_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
}

static void EmitStoreSljitLocalArrayValue(struct sljit_compiler *compiler, sljit_sw array_offset, sljit_s32 index_reg,
                                          sljit_s32 value_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, index_reg), 3, value_reg, 0);
}

static void EmitStoreSljitLocalArrayImmediate(struct sljit_compiler *compiler, sljit_sw array_offset,
                                              sljit_s32 index_reg, sljit_sw value) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, index_reg), 3, SLJIT_IMM, value);
}

static sljit_jump *EmitJumpIfSljitLocalArrayZero(struct sljit_compiler *compiler, sljit_sw array_offset,
                                                 sljit_s32 index_reg) {
	EmitLoadSljitLocalArrayValue(compiler, array_offset, index_reg, SLJIT_R2);
	return sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
}

static sljit_jump *EmitJumpIfSljitLocalArrayNonZero(struct sljit_compiler *compiler, sljit_sw array_offset,
                                                    sljit_s32 index_reg) {
	EmitLoadSljitLocalArrayValue(compiler, array_offset, index_reg, SLJIT_R2);
	return sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
}

static void EmitLoadSljitLocalByteArrayValue(struct sljit_compiler *compiler, sljit_sw array_offset,
                                             sljit_s32 index_reg, sljit_s32 target_reg) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 0);
}

static void EmitStoreSljitLocalByteArrayImmediate(struct sljit_compiler *compiler, sljit_sw array_offset,
                                                  sljit_s32 index_reg, sljit_sw value) {
	sljit_get_local_base(compiler, SLJIT_R0, 0, array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, index_reg), 0, SLJIT_IMM, value);
}

static sljit_jump *EmitJumpIfSljitLocalByteArrayNonZero(struct sljit_compiler *compiler, sljit_sw array_offset,
                                                        sljit_s32 index_reg) {
	EmitLoadSljitLocalByteArrayValue(compiler, array_offset, index_reg, SLJIT_R2);
	return sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
}

static void EmitStoreSljitLocalGroupSeenImmediate(struct sljit_compiler *compiler,
                                                  const SljitLocalPerfectHashAggregatePlan &plan, sljit_s32 index_reg,
                                                  sljit_sw value) {
	if (plan.group_seen_is_byte) {
		EmitStoreSljitLocalByteArrayImmediate(compiler, plan.group_seen_offset, index_reg, value);
	} else {
		EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, index_reg, value);
	}
}

static sljit_jump *EmitJumpIfSljitLocalGroupSeenNonZero(struct sljit_compiler *compiler,
                                                        const SljitLocalPerfectHashAggregatePlan &plan,
                                                        sljit_s32 index_reg) {
	if (plan.group_seen_is_byte) {
		return EmitJumpIfSljitLocalByteArrayNonZero(compiler, plan.group_seen_offset, index_reg);
	}
	return EmitJumpIfSljitLocalArrayNonZero(compiler, plan.group_seen_offset, index_reg);
}

static bool SljitLocalStrideIsPowerOfTwo(sljit_sw stride) {
	return stride > 0 && (stride & (stride - 1)) == 0;
}

static sljit_sw SljitLocalStrideShift(sljit_sw stride) {
	sljit_sw shift = 0;
	while (stride > 1) {
		stride >>= 1;
		shift++;
	}
	return shift;
}

static bool SljitSparseLocalUsesCountSeen(const SljitLocalPerfectHashAggregatePlan &plan) {
	return plan.sparse_count_seen_lane != DConstants::INVALID_INDEX;
}

static idx_t CountSljitSparseLocalRunCacheableLanes(const SljitLocalPerfectHashAggregatePlan &plan,
                                                    const vector<ExecutionRegionAggregateInput> &aggregates) {
	if (!SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS || !plan.enabled || !plan.sparse ||
	    !SljitSparseLocalUsesCountSeen(plan)) {
		return 0;
	}
	idx_t result = 0;
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		if (payload_idx >= plan.lanes.size() || payload_idx == plan.sparse_count_seen_lane) {
			continue;
		}
		auto &lane = plan.lanes[payload_idx];
		if (lane.lower_offset < 0 || !lane.value_always_seen) {
			continue;
		}
		switch (aggregates[payload_idx].primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			break;
		default:
			continue;
		}
		result++;
	}
	return result;
}

static vector<SljitSparseLocalRunCachedLane>
BuildSljitSparseLocalRunCachedLanes(const SljitLocalPerfectHashAggregatePlan &plan,
                                    const vector<ExecutionRegionAggregateInput> &aggregates,
                                    const vector<sljit_s32> &lower_regs) {
	vector<SljitSparseLocalRunCachedLane> result;
	if (!SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS || !plan.enabled || !plan.sparse ||
	    !SljitSparseLocalUsesCountSeen(plan)) {
		return result;
	}
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		if (result.size() >= lower_regs.size()) {
			break;
		}
		if (payload_idx >= plan.lanes.size() || payload_idx == plan.sparse_count_seen_lane) {
			continue;
		}
		auto &lane = plan.lanes[payload_idx];
		if (lane.lower_offset < 0 || !lane.value_always_seen) {
			continue;
		}
		switch (aggregates[payload_idx].primitive_update_kind) {
		case AggregatePrimitiveUpdateKind::SUM_INT64:
		case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
			break;
		default:
			continue;
		}
		SljitSparseLocalRunCachedLane cached_lane;
		cached_lane.payload_idx = payload_idx;
		cached_lane.lower_reg = lower_regs[result.size()];
		result.push_back(cached_lane);
	}
	return result;
}

static const SljitSparseLocalRunCachedLane *
FindSljitSparseLocalRunCachedLane(const vector<SljitSparseLocalRunCachedLane> &cached_lanes, idx_t payload_idx) {
	for (auto &cached_lane : cached_lanes) {
		if (cached_lane.payload_idx == payload_idx) {
			return &cached_lane;
		}
	}
	return nullptr;
}

static bool SljitExpressionTreeResultNotNull(const ExecutionExpressionIR &node, const vector<bool> &source_not_null) {
	switch (node.validity) {
	case ExecutionExpressionValidityKind::CONSTANT_NULL:
		return false;
	case ExecutionExpressionValidityKind::CONSTANT_VALID:
	case ExecutionExpressionValidityKind::NOT_NULL:
		return true;
	case ExecutionExpressionValidityKind::SOURCE:
		return node.kind == ExecutionExpressionIRKind::REFERENCE && node.ref_index < source_not_null.size() &&
		       source_not_null[node.ref_index];
	case ExecutionExpressionValidityKind::CHILD:
		return node.left && SljitExpressionTreeResultNotNull(*node.left, source_not_null);
	case ExecutionExpressionValidityKind::CHILDREN_NULL_PROPAGATING:
		if (node.left && !SljitExpressionTreeResultNotNull(*node.left, source_not_null)) {
			return false;
		}
		if (node.right && !SljitExpressionTreeResultNotNull(*node.right, source_not_null)) {
			return false;
		}
		if (node.else_node && !SljitExpressionTreeResultNotNull(*node.else_node, source_not_null)) {
			return false;
		}
		for (auto &child : node.children) {
			if (child && !SljitExpressionTreeResultNotNull(*child, source_not_null)) {
				return false;
			}
		}
		return true;
	default:
		return false;
	}
}

static bool SljitAggregatePayloadResultNotNull(const SljitNativeRegionExpressionPlan &payload,
                                               const vector<bool> &source_not_null) {
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return !payload.constant_value.IsNull();
	case SljitNativeRegionExpressionKind::REFERENCE:
		return payload.source_index < source_not_null.size() && source_not_null[payload.source_index];
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		return payload.expression_tree && SljitExpressionTreeResultNotNull(*payload.expression_tree, source_not_null);
	default:
		return false;
	}
}

static vector<bool> BuildSljitAggregatePayloadNotNull(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                      const vector<ExecutionRegionAggregateInput> &aggregates,
                                                      const vector<bool> &source_not_null) {
	vector<bool> result;
	result.reserve(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			result.push_back(true);
			continue;
		}
		result.push_back(SljitAggregatePayloadResultNotNull(payloads[payload_idx], source_not_null));
	}
	return result;
}

struct SljitInt128Range {
	hugeint_t min;
	hugeint_t max;
};

static bool SljitValueToHugeint(const Value &value, const LogicalType &expected_type, hugeint_t &result) {
	if (value.IsNull() || value.type() != expected_type) {
		return false;
	}
	if (value.type().id() == LogicalTypeId::DATE) {
		result = Hugeint::Convert(value.GetValueUnsafe<date_t>().days);
		return true;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::BOOL:
		result = Hugeint::Convert(value.GetValueUnsafe<bool>() ? 1 : 0);
		return true;
	case PhysicalType::INT8:
		result = Hugeint::Convert(value.GetValueUnsafe<int8_t>());
		return true;
	case PhysicalType::INT16:
		result = Hugeint::Convert(value.GetValueUnsafe<int16_t>());
		return true;
	case PhysicalType::INT32:
		result = Hugeint::Convert(value.GetValueUnsafe<int32_t>());
		return true;
	case PhysicalType::INT64:
		result = Hugeint::Convert(value.GetValueUnsafe<int64_t>());
		return true;
	case PhysicalType::UINT8:
		result = Hugeint::Convert(value.GetValueUnsafe<uint8_t>());
		return true;
	case PhysicalType::UINT16:
		result = Hugeint::Convert(value.GetValueUnsafe<uint16_t>());
		return true;
	case PhysicalType::UINT32:
		result = Hugeint::Convert(value.GetValueUnsafe<uint32_t>());
		return true;
	case PhysicalType::UINT64:
		result = Hugeint::Convert(value.GetValueUnsafe<uint64_t>());
		return true;
	case PhysicalType::INT128:
		result = value.GetValueUnsafe<hugeint_t>();
		return true;
	default:
		return false;
	}
}

static bool SljitRangeFromSource(idx_t source_index, const LogicalType &expected_type,
                                 const vector<Value> &source_min_values, const vector<Value> &source_max_values,
                                 SljitInt128Range &result) {
	if (source_index >= source_min_values.size() || source_index >= source_max_values.size()) {
		return false;
	}
	return SljitValueToHugeint(source_min_values[source_index], expected_type, result.min) &&
	       SljitValueToHugeint(source_max_values[source_index], expected_type, result.max) && result.min <= result.max;
}

static bool SljitRangeAdd(const SljitInt128Range &left, const SljitInt128Range &right, SljitInt128Range &result) {
	result.min = left.min;
	result.max = left.max;
	return Hugeint::TryAddInPlace(result.min, right.min) && Hugeint::TryAddInPlace(result.max, right.max);
}

static bool SljitRangeSubtract(const SljitInt128Range &left, const SljitInt128Range &right, SljitInt128Range &result) {
	result.min = left.min;
	result.max = left.max;
	return Hugeint::TrySubtractInPlace(result.min, right.max) && Hugeint::TrySubtractInPlace(result.max, right.min);
}

static bool SljitRangeMultiply(const SljitInt128Range &left, const SljitInt128Range &right, SljitInt128Range &result) {
	hugeint_t values[4];
	if (!Hugeint::TryMultiply(left.min, right.min, values[0]) ||
	    !Hugeint::TryMultiply(left.min, right.max, values[1]) ||
	    !Hugeint::TryMultiply(left.max, right.min, values[2]) ||
	    !Hugeint::TryMultiply(left.max, right.max, values[3])) {
		return false;
	}
	result.min = values[0];
	result.max = values[0];
	for (idx_t value_idx = 1; value_idx < 4; value_idx++) {
		result.min = MinValue(result.min, values[value_idx]);
		result.max = MaxValue(result.max, values[value_idx]);
	}
	return true;
}

static bool SljitDecimal64BinaryHasRawSemantics(const ExecutionExpressionIR &node) {
	if (!node.left || !node.right) {
		return false;
	}
	const auto left_decimal = node.left->return_type.id() == LogicalTypeId::DECIMAL &&
	                          node.left->return_type.InternalType() == PhysicalType::INT64;
	const auto right_decimal = node.right->return_type.id() == LogicalTypeId::DECIMAL &&
	                           node.right->return_type.InternalType() == PhysicalType::INT64;
	if (!left_decimal && !right_decimal) {
		return true;
	}
	if (!left_decimal || !right_decimal || node.return_type.id() != LogicalTypeId::DECIMAL ||
	    node.return_type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	switch (node.binary_op) {
	case ExecutionExpressionBinaryOp::ADD:
	case ExecutionExpressionBinaryOp::SUBTRACT:
		return DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.left->return_type) &&
		       DecimalType::GetScale(node.return_type) == DecimalType::GetScale(node.right->return_type);
	case ExecutionExpressionBinaryOp::MULTIPLY:
		return DecimalType::GetScale(node.return_type) ==
		       DecimalType::GetScale(node.left->return_type) + DecimalType::GetScale(node.right->return_type);
	default:
		return false;
	}
}

static bool SljitRangeScaleByPowerOfTen(SljitInt128Range &range, uint8_t scale_delta) {
	if (scale_delta == 0) {
		return true;
	}
	if (scale_delta >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	auto scale = Hugeint::Convert(NumericHelper::POWERS_OF_TEN[scale_delta]);
	return Hugeint::TryMultiply(range.min, scale, range.min) && Hugeint::TryMultiply(range.max, scale, range.max);
}

static bool SljitRangeCast(const ExecutionExpressionIR &node, const SljitInt128Range &child, SljitInt128Range &result) {
	if (!node.left) {
		return false;
	}
	result = child;
	if (node.return_type.InternalType() == node.left->return_type.InternalType()) {
		if (node.return_type.id() == LogicalTypeId::DECIMAL || node.left->return_type.id() == LogicalTypeId::DECIMAL) {
			if (node.return_type.id() != LogicalTypeId::DECIMAL ||
			    node.left->return_type.id() != LogicalTypeId::DECIMAL) {
				return false;
			}
			auto source_scale = DecimalType::GetScale(node.left->return_type);
			auto target_scale = DecimalType::GetScale(node.return_type);
			if (target_scale < source_scale) {
				return false;
			}
			return SljitRangeScaleByPowerOfTen(result, target_scale - source_scale);
		}
		return true;
	}
	if (node.return_type.id() == LogicalTypeId::DECIMAL && node.return_type.InternalType() == PhysicalType::INT64 &&
	    node.left->return_type.IsIntegral()) {
		return SljitRangeScaleByPowerOfTen(result, DecimalType::GetScale(node.return_type));
	}
	if (node.return_type.IsIntegral() && node.left->return_type.IsIntegral()) {
		return true;
	}
	return false;
}

static bool SljitExpressionTreeInt128Range(const ExecutionExpressionIR &node, const vector<Value> &source_min_values,
                                           const vector<Value> &source_max_values, SljitInt128Range &result) {
	switch (node.kind) {
	case ExecutionExpressionIRKind::CONSTANT:
		return SljitValueToHugeint(node.constant, node.return_type, result.min) &&
		       SljitValueToHugeint(node.constant, node.return_type, result.max);
	case ExecutionExpressionIRKind::REFERENCE:
		return SljitRangeFromSource(node.ref_index, node.return_type, source_min_values, source_max_values, result);
	case ExecutionExpressionIRKind::CAST: {
		if (!node.left) {
			return false;
		}
		SljitInt128Range child;
		return SljitExpressionTreeInt128Range(*node.left, source_min_values, source_max_values, child) &&
		       SljitRangeCast(node, child, result);
	}
	case ExecutionExpressionIRKind::BINARY: {
		if (!node.left || !node.right || !SljitDecimal64BinaryHasRawSemantics(node)) {
			return false;
		}
		SljitInt128Range left;
		SljitInt128Range right;
		if (!SljitExpressionTreeInt128Range(*node.left, source_min_values, source_max_values, left) ||
		    !SljitExpressionTreeInt128Range(*node.right, source_min_values, source_max_values, right)) {
			return false;
		}
		switch (node.binary_op) {
		case ExecutionExpressionBinaryOp::ADD:
			return SljitRangeAdd(left, right, result);
		case ExecutionExpressionBinaryOp::SUBTRACT:
			return SljitRangeSubtract(left, right, result);
		case ExecutionExpressionBinaryOp::MULTIPLY:
			return SljitRangeMultiply(left, right, result);
		default:
			return false;
		}
	}
	default:
		return false;
	}
}

static bool SljitRangeAbsFitsLocalInt64Sum(const SljitInt128Range &range) {
	auto max_abs = range.max;
	if (max_abs < 0) {
		if (!Hugeint::TryNegate(max_abs, max_abs)) {
			return false;
		}
	}
	auto min_abs = range.min;
	if (min_abs < 0) {
		if (!Hugeint::TryNegate(min_abs, min_abs)) {
			return false;
		}
	}
	max_abs = MaxValue(max_abs, min_abs);
	auto limit = Hugeint::Convert(NumericLimits<int64_t>::Maximum() / NumericCast<int64_t>(STANDARD_VECTOR_SIZE));
	return max_abs <= limit;
}

static bool SljitPayloadRangeLocalSumLowerNeverOverflows(const SljitNativeRegionExpressionPlan &payload,
                                                         const vector<Value> &source_min_values,
                                                         const vector<Value> &source_max_values) {
	SljitInt128Range range;
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		if (!SljitValueToHugeint(payload.constant_value, payload.return_type, range.min) ||
		    !SljitValueToHugeint(payload.constant_value, payload.return_type, range.max)) {
			return false;
		}
		break;
	case SljitNativeRegionExpressionKind::REFERENCE:
		if (!SljitRangeFromSource(payload.source_index, payload.return_type, source_min_values, source_max_values,
		                          range)) {
			return false;
		}
		break;
	case SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE:
		if (!payload.expression_tree ||
		    !SljitExpressionTreeInt128Range(*payload.expression_tree, source_min_values, source_max_values, range)) {
			return false;
		}
		break;
	default:
		return false;
	}
	return SljitRangeAbsFitsLocalInt64Sum(range);
}

static bool SljitLocalSumLowerNeverOverflows(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		return true;
	default:
		break;
	}
	if (type.id() != LogicalTypeId::DECIMAL || type.InternalType() != PhysicalType::INT64) {
		return false;
	}
	const auto width = DecimalType::GetWidth(type);
	if (width == 0 || width >= NumericHelper::CACHED_POWERS_OF_TEN) {
		return false;
	}
	const auto max_abs = NumericHelper::POWERS_OF_TEN[width] - 1;
	return max_abs <= NumericLimits<int64_t>::Maximum() / NumericCast<int64_t>(STANDARD_VECTOR_SIZE);
}

static void AnnotateSljitLocalPerfectHashAggregatePlan(SljitLocalPerfectHashAggregatePlan &plan,
                                                       const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                       const vector<ExecutionRegionAggregateInput> &aggregates,
                                                       const vector<Value> &source_min_values,
                                                       const vector<Value> &source_max_values) {
	if (!plan.enabled) {
		return;
	}
	for (idx_t payload_idx = 0;
	     payload_idx < payloads.size() && payload_idx < aggregates.size() && payload_idx < plan.lanes.size();
	     payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			continue;
		}
		plan.lanes[payload_idx].local_lower_never_overflows =
		    SljitLocalSumLowerNeverOverflows(payloads[payload_idx].return_type) ||
		    SljitPayloadRangeLocalSumLowerNeverOverflows(payloads[payload_idx], source_min_values, source_max_values);
	}
}

static void EmitSljitSparseLocalPerfectHashGroupPointer(struct sljit_compiler *compiler,
                                                        const SljitLocalPerfectHashAggregatePlan &plan,
                                                        sljit_s32 group_index_reg, sljit_s32 target_reg) {
	sljit_get_local_base(compiler, target_reg, 0, plan.group_payload_offset);
	if (SljitLocalStrideIsPowerOfTwo(plan.group_payload_stride)) {
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R1, 0, group_index_reg, 0, SLJIT_IMM,
		               SljitLocalStrideShift(plan.group_payload_stride));
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, plan.group_payload_stride);
		sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R1, 0, SLJIT_R1, 0, group_index_reg, 0);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, target_reg, 0, SLJIT_R1, 0);
}

static void EmitZeroSljitSparseLocalPerfectHashLane(struct sljit_compiler *compiler,
                                                    const SljitLocalPerfectHashAggregateLane &lane,
                                                    sljit_s32 group_pointer_reg) {
	if (lane.count_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.count_offset, SLJIT_IMM, 0);
	}
	if (lane.lower_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, SLJIT_IMM, 0);
	}
	if (lane.upper_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.upper_offset, SLJIT_IMM, 0);
	}
	if (lane.saw_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 0);
	}
}

static void EmitZeroSljitSparseLocalPerfectHashPayloads(struct sljit_compiler *compiler,
                                                        const SljitLocalPerfectHashAggregatePlan &plan) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(plan.group_count));
	EmitSljitSparseLocalPerfectHashGroupPointer(compiler, plan, SLJIT_S1, SLJIT_PERFECT_HASH_STATE_REG);
	for (auto &lane : plan.lanes) {
		EmitZeroSljitSparseLocalPerfectHashLane(compiler, lane, SLJIT_PERFECT_HASH_STATE_REG);
	}
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static void EmitZeroSljitSparseLocalPerfectHashCountSentinel(struct sljit_compiler *compiler,
                                                             const SljitLocalPerfectHashAggregatePlan &plan) {
	D_ASSERT(SljitSparseLocalUsesCountSeen(plan));
	const auto &lane = plan.lanes[plan.sparse_count_seen_lane];
	D_ASSERT(lane.count_offset >= 0);
	sljit_get_local_base(compiler, SLJIT_R0, 0, plan.group_payload_offset + lane.count_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(plan.group_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_IMM, plan.group_payload_stride);
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static void EmitZeroSljitLocalPerfectHashAggregateArrays(struct sljit_compiler *compiler,
                                                         const SljitLocalPerfectHashAggregatePlan &plan) {
	if (!plan.enabled) {
		return;
	}
	if (plan.sparse) {
		if (plan.sparse_eager_zero) {
			EmitZeroSljitSparseLocalPerfectHashPayloads(compiler, plan);
			return;
		}
		if (SljitSparseLocalUsesCountSeen(plan)) {
			EmitZeroSljitSparseLocalPerfectHashCountSentinel(compiler, plan);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
			auto loop = sljit_emit_label(compiler);
			auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM,
			                           NumericCast<sljit_sw>(plan.group_count));
			EmitStoreSljitLocalGroupSeenImmediate(compiler, plan, SLJIT_S1, 0);
			EmitNextSljitNativeVectorLoop(compiler, loop);
			sljit_set_label(done, sljit_emit_label(compiler));
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), plan.active_count_offset, SLJIT_IMM, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(plan.group_count));
	EmitStoreSljitLocalGroupSeenImmediate(compiler, plan, SLJIT_S1, 0);
	for (auto &lane : plan.lanes) {
		if (lane.count_offset >= 0) {
			EmitStoreSljitLocalArrayImmediate(compiler, lane.count_offset, SLJIT_S1, 0);
		}
		if (lane.lower_offset >= 0) {
			EmitStoreSljitLocalArrayImmediate(compiler, lane.lower_offset, SLJIT_S1, 0);
		}
		if (lane.upper_offset >= 0) {
			EmitStoreSljitLocalArrayImmediate(compiler, lane.upper_offset, SLJIT_S1, 0);
		}
		if (lane.saw_offset >= 0) {
			EmitStoreSljitLocalArrayImmediate(compiler, lane.saw_offset, SLJIT_S1, 0);
		}
	}
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static void EmitZeroSljitDeferredPerfectHashFlagArray(struct sljit_compiler *compiler,
                                                      const SljitDeferredPerfectHashFlagPlan &plan) {
	if (!plan.enabled) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done =
	    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM, NumericCast<sljit_sw>(plan.group_count));
	EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, SLJIT_S1, 0);
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static void EmitMarkSljitLocalPerfectHashGroupSeen(struct sljit_compiler *compiler,
                                                   const SljitLocalPerfectHashAggregatePlan &plan,
                                                   sljit_s32 group_index_reg, sljit_s32 group_pointer_reg,
                                                   bool mark_payloads_seen = false, bool increment_count_seen = true) {
	if (!plan.enabled) {
		return;
	}
	if (!plan.sparse) {
		EmitStoreSljitLocalGroupSeenImmediate(compiler, plan, group_index_reg, 1);
		return;
	}
	EmitSljitSparseLocalPerfectHashGroupPointer(compiler, plan, group_index_reg, group_pointer_reg);
	if (plan.sparse_eager_zero) {
		if (SljitSparseLocalUsesCountSeen(plan) && increment_count_seen) {
			auto &count_seen_lane = plan.lanes[plan.sparse_count_seen_lane];
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg),
			               count_seen_lane.count_offset);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), count_seen_lane.count_offset, SLJIT_R3,
			               0);
		}
		return;
	}
	const auto use_count_seen = SljitSparseLocalUsesCountSeen(plan);
	const auto &count_seen_lane = use_count_seen ? plan.lanes[plan.sparse_count_seen_lane] : plan.lanes[0];
	sljit_jump *group_seen;
	if (use_count_seen) {
		D_ASSERT(count_seen_lane.count_offset >= 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), count_seen_lane.count_offset);
		group_seen = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	} else {
		group_seen = EmitJumpIfSljitLocalGroupSeenNonZero(compiler, plan, group_index_reg);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), plan.active_count_offset);
	EmitStoreSljitLocalArrayValue(compiler, plan.active_groups_offset, SLJIT_R2, group_index_reg);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), plan.active_count_offset, SLJIT_R2, 0);
	if (!use_count_seen) {
		EmitStoreSljitLocalGroupSeenImmediate(compiler, plan, group_index_reg, 1);
	}
	for (auto &lane : plan.lanes) {
		EmitZeroSljitSparseLocalPerfectHashLane(compiler, lane, group_pointer_reg);
	}
	if (mark_payloads_seen) {
		for (auto &lane : plan.lanes) {
			if (lane.saw_offset >= 0) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 1);
			}
		}
	}
	sljit_set_label(group_seen, sljit_emit_label(compiler));
	if (use_count_seen && increment_count_seen) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), count_seen_lane.count_offset, SLJIT_R3, 0);
	}
}

static void EmitMarkSljitDeferredPerfectHashGroupSeen(struct sljit_compiler *compiler,
                                                      const SljitDeferredPerfectHashFlagPlan &plan,
                                                      sljit_s32 group_index_reg) {
	if (plan.enabled) {
		EmitStoreSljitLocalArrayImmediate(compiler, plan.group_seen_offset, group_index_reg, 1);
	}
}

static void EmitSljitLocalPerfectHashIncrementCount(struct sljit_compiler *compiler,
                                                    const SljitLocalPerfectHashAggregateLane &lane,
                                                    sljit_s32 group_index_reg) {
	EmitLoadSljitLocalArrayValue(compiler, lane.count_offset, group_index_reg, SLJIT_R3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	EmitStoreSljitLocalArrayValue(compiler, lane.count_offset, group_index_reg, SLJIT_R3);
}

static void EmitSljitLocalPerfectHashAccumulateInt64(struct sljit_compiler *compiler,
                                                     const SljitLocalPerfectHashAggregateLane &lane,
                                                     sljit_s32 group_index_reg, sljit_s32 value_reg) {
	EmitLoadSljitLocalArrayValue(compiler, lane.lower_offset, group_index_reg, SLJIT_R3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	EmitStoreSljitLocalArrayValue(compiler, lane.lower_offset, group_index_reg, SLJIT_R3);
	if (lane.saw_offset >= 0) {
		EmitStoreSljitLocalArrayImmediate(compiler, lane.saw_offset, group_index_reg, 1);
	}
}

static void EmitSljitLocalPerfectHashAccumulateHugeint(struct sljit_compiler *compiler,
                                                       const SljitLocalPerfectHashAggregateLane &lane,
                                                       sljit_s32 group_index_reg, sljit_s32 value_reg) {
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R4, 0, value_reg, 0, SLJIT_IMM, 63);
	EmitLoadSljitLocalArrayValue(compiler, lane.lower_offset, group_index_reg, SLJIT_R3);
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	EmitStoreSljitLocalArrayValue(compiler, lane.lower_offset, group_index_reg, SLJIT_R3);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R2, 0, SLJIT_R4, 0, SLJIT_R1, 0);
	auto no_upper_update = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitLoadSljitLocalArrayValue(compiler, lane.upper_offset, group_index_reg, SLJIT_R3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R1, 0);
	EmitStoreSljitLocalArrayValue(compiler, lane.upper_offset, group_index_reg, SLJIT_R3);
	sljit_set_label(no_upper_update, sljit_emit_label(compiler));
	if (lane.saw_offset >= 0) {
		EmitStoreSljitLocalArrayImmediate(compiler, lane.saw_offset, group_index_reg, 1);
	}
}

static void EmitSljitLocalPerfectHashAccumulate(struct sljit_compiler *compiler,
                                                const SljitLocalPerfectHashAggregateLane &lane,
                                                AggregatePrimitiveUpdateKind kind, sljit_s32 group_index_reg,
                                                sljit_s32 value_reg) {
	if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		EmitSljitLocalPerfectHashAccumulateHugeint(compiler, lane, group_index_reg, value_reg);
	} else {
		EmitSljitLocalPerfectHashAccumulateInt64(compiler, lane, group_index_reg, value_reg);
	}
}

static void EmitSljitSparseLocalPerfectHashIncrementCount(struct sljit_compiler *compiler,
                                                          const SljitLocalPerfectHashAggregateLane &lane,
                                                          sljit_s32 group_pointer_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.count_offset, SLJIT_R3, 0);
}

static void EmitSljitSparseLocalPerfectHashAccumulateInt64(struct sljit_compiler *compiler,
                                                           const SljitLocalPerfectHashAggregateLane &lane,
                                                           sljit_s32 group_pointer_reg, sljit_s32 value_reg,
                                                           bool store_saw) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.lower_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, SLJIT_R3, 0);
	if (store_saw && lane.saw_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 1);
	}
}

static void EmitSljitSparseLocalPerfectHashAccumulateHugeint(struct sljit_compiler *compiler,
                                                             const SljitLocalPerfectHashAggregateLane &lane,
                                                             sljit_s32 group_pointer_reg, sljit_s32 value_reg,
                                                             bool store_saw) {
	// Sparse local hugeint lanes keep a wrapped int64 lower word and signed-overflow correction count.
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.lower_offset);
	if (lane.local_lower_never_overflows) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, SLJIT_R3, 0);
		if (store_saw && lane.saw_offset >= 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 1);
		}
		return;
	}
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_OVERFLOW, SLJIT_R3, 0, SLJIT_R3, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, SLJIT_R3, 0);
	auto no_overflow = sljit_emit_jump(compiler, SLJIT_NOT_OVERFLOW);
	auto negative_overflow = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, value_reg, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.upper_offset, SLJIT_R3, 0);
	auto overflow_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(negative_overflow, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, -1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.upper_offset, SLJIT_R3, 0);
	sljit_set_label(overflow_done, sljit_emit_label(compiler));
	sljit_set_label(no_overflow, sljit_emit_label(compiler));
	if (store_saw && lane.saw_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.saw_offset, SLJIT_IMM, 1);
	}
}

static void EmitSljitSparseLocalPerfectHashAccumulate(struct sljit_compiler *compiler,
                                                      const SljitLocalPerfectHashAggregateLane &lane,
                                                      AggregatePrimitiveUpdateKind kind, sljit_s32 group_pointer_reg,
                                                      sljit_s32 value_reg, bool store_saw = true) {
	if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		EmitSljitSparseLocalPerfectHashAccumulateHugeint(compiler, lane, group_pointer_reg, value_reg, store_saw);
	} else {
		EmitSljitSparseLocalPerfectHashAccumulateInt64(compiler, lane, group_pointer_reg, value_reg, store_saw);
	}
}

static void EmitSljitSparseLocalRunCacheFlush(struct sljit_compiler *compiler,
                                              const SljitLocalPerfectHashAggregatePlan &plan,
                                              const vector<SljitSparseLocalRunCachedLane> &cached_lanes,
                                              sljit_s32 group_pointer_reg, sljit_sw cached_group_offset,
                                              sljit_sw cached_start_offset, sljit_s32 current_index_reg,
                                              sljit_sw cached_count_offset = -1) {
	if (!SljitSparseLocalUsesCountSeen(plan)) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), cached_group_offset);
	auto no_cached_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, -1);
	auto &count_lane = plan.lanes[plan.sparse_count_seen_lane];
	if (cached_count_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), cached_count_offset);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), cached_start_offset);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R3, 0, current_index_reg, 0, SLJIT_R3, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(group_pointer_reg), count_lane.count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), count_lane.count_offset, SLJIT_R4, 0);
	for (auto &cached_lane : cached_lanes) {
		auto &lane = plan.lanes[cached_lane.payload_idx];
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.lower_offset, cached_lane.lower_reg, 0);
	}
	sljit_set_label(no_cached_group, sljit_emit_label(compiler));
}

static void EmitSljitSparseLocalRunCacheLoadCurrent(struct sljit_compiler *compiler,
                                                    const SljitLocalPerfectHashAggregatePlan &plan,
                                                    const vector<SljitSparseLocalRunCachedLane> &cached_lanes,
                                                    sljit_s32 group_pointer_reg, sljit_sw cached_start_offset,
                                                    sljit_s32 current_index_reg, sljit_sw cached_count_offset = -1) {
	D_ASSERT(SljitSparseLocalUsesCountSeen(plan));
	if (cached_count_offset >= 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), cached_count_offset, SLJIT_IMM, 1);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), cached_start_offset, current_index_reg, 0);
	}
	for (auto &cached_lane : cached_lanes) {
		auto &lane = plan.lanes[cached_lane.payload_idx];
		sljit_emit_op1(compiler, SLJIT_MOV, cached_lane.lower_reg, 0, SLJIT_MEM1(group_pointer_reg), lane.lower_offset);
	}
}

static void EmitSljitSparseLocalRunCacheIncrementCount(struct sljit_compiler *compiler, sljit_sw cached_count_offset) {
	if (cached_count_offset < 0) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), cached_count_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), cached_count_offset, SLJIT_R3, 0);
}

static void EmitSljitSparseLocalRunCacheAccumulate(struct sljit_compiler *compiler,
                                                   const SljitLocalPerfectHashAggregateLane &lane,
                                                   AggregatePrimitiveUpdateKind kind, sljit_s32 lower_reg,
                                                   sljit_s32 group_pointer_reg, sljit_s32 value_reg) {
	if (kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_ADD, lower_reg, 0, lower_reg, 0, value_reg, 0);
		return;
	}
	if (lane.local_lower_never_overflows) {
		sljit_emit_op2(compiler, SLJIT_ADD, lower_reg, 0, lower_reg, 0, value_reg, 0);
		return;
	}
	// Keep the lower word live across a same-group run; upper only changes on signed 64-bit overflow.
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_OVERFLOW, lower_reg, 0, lower_reg, 0, value_reg, 0);
	auto no_overflow = sljit_emit_jump(compiler, SLJIT_NOT_OVERFLOW);
	auto negative_overflow = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, value_reg, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.upper_offset, SLJIT_R3, 0);
	auto overflow_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(negative_overflow, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(group_pointer_reg), lane.upper_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, -1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(group_pointer_reg), lane.upper_offset, SLJIT_R3, 0);
	sljit_set_label(overflow_done, sljit_emit_label(compiler));
	sljit_set_label(no_overflow, sljit_emit_label(compiler));
}

static void EmitSljitLocalPerfectHashStatePointer(struct sljit_compiler *compiler, sljit_s32 group_index_reg,
                                                  sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_tuple_size));
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R1, 0, SLJIT_R1, 0, group_index_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_state_data));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, target_reg, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, target_reg, 0, SLJIT_R1, 0);
}

static void EmitSljitLocalPerfectHashCommitCount(struct sljit_compiler *compiler,
                                                 const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 state_reg,
                                                 sljit_s32 group_index_reg, idx_t state_offset, idx_t value_offset,
                                                 bool count_known_nonzero) {
	EmitLoadSljitLocalArrayValue(compiler, lane.count_offset, group_index_reg, SLJIT_R2);
	sljit_jump *no_count = nullptr;
	if (!count_known_nonzero) {
		no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	}
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, state_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	if (no_count) {
		sljit_set_label(no_count, sljit_emit_label(compiler));
	}
}

static void EmitSljitLocalPerfectHashCommitInt64Value(struct sljit_compiler *compiler,
                                                      const SljitLocalPerfectHashAggregateLane &lane,
                                                      sljit_s32 state_reg, sljit_s32 group_index_reg,
                                                      idx_t state_offset, idx_t value_offset,
                                                      idx_t state_is_set_offset) {
	EmitLoadSljitLocalArrayValue(compiler, lane.lower_offset, group_index_reg, SLJIT_R2);
	EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, state_reg, state_offset, value_offset,
	                                                  state_is_set_offset, SLJIT_R2);
}

static void EmitSljitLocalPerfectHashCommitInt64(struct sljit_compiler *compiler,
                                                 const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 state_reg,
                                                 sljit_s32 group_index_reg, idx_t state_offset, idx_t value_offset,
                                                 idx_t state_is_set_offset, bool value_known_seen) {
	sljit_jump *no_value = nullptr;
	if (!value_known_seen) {
		no_value = EmitJumpIfSljitLocalArrayZero(compiler, lane.saw_offset, group_index_reg);
	}
	EmitSljitLocalPerfectHashCommitInt64Value(compiler, lane, state_reg, group_index_reg, state_offset, value_offset,
	                                          state_is_set_offset);
	if (no_value) {
		sljit_set_label(no_value, sljit_emit_label(compiler));
	}
}

static void EmitSljitLocalPerfectHashCommitHugeintValue(struct sljit_compiler *compiler,
                                                        const SljitLocalPerfectHashAggregateLane &lane,
                                                        sljit_s32 state_reg, sljit_s32 group_index_reg,
                                                        idx_t state_offset, idx_t value_offset,
                                                        idx_t state_is_set_offset) {
	EmitLoadSljitLocalArrayValue(compiler, lane.lower_offset, group_index_reg, SLJIT_R2);
	EmitLoadSljitLocalArrayValue(compiler, lane.upper_offset, group_index_reg, SLJIT_R4);
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, state_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	EmitSljitAccumulateHugeintUpperIfNeeded(compiler, SLJIT_R0, offsetof(hugeint_t, upper), SLJIT_R4, SLJIT_R1);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, state_reg, state_offset, state_is_set_offset);
}

static void EmitSljitLocalPerfectHashCommitHugeint(struct sljit_compiler *compiler,
                                                   const SljitLocalPerfectHashAggregateLane &lane, sljit_s32 state_reg,
                                                   sljit_s32 group_index_reg, idx_t state_offset, idx_t value_offset,
                                                   idx_t state_is_set_offset, bool value_known_seen) {
	sljit_jump *no_value = nullptr;
	if (!value_known_seen) {
		no_value = EmitJumpIfSljitLocalArrayZero(compiler, lane.saw_offset, group_index_reg);
	}
	EmitSljitLocalPerfectHashCommitHugeintValue(compiler, lane, state_reg, group_index_reg, state_offset, value_offset,
	                                            state_is_set_offset);
	if (no_value) {
		sljit_set_label(no_value, sljit_emit_label(compiler));
	}
}

static void EmitSljitSparseLocalPerfectHashCommitCount(struct sljit_compiler *compiler,
                                                       const SljitLocalPerfectHashAggregateLane &lane,
                                                       sljit_s32 state_reg, sljit_s32 group_pointer_reg,
                                                       idx_t state_offset, idx_t value_offset,
                                                       bool count_known_nonzero) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(group_pointer_reg), lane.count_offset);
	sljit_jump *no_count = nullptr;
	if (!count_known_nonzero) {
		no_count = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	}
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, state_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	if (no_count) {
		sljit_set_label(no_count, sljit_emit_label(compiler));
	}
}

static void EmitSljitSparseLocalPerfectHashCommitInt64Value(struct sljit_compiler *compiler,
                                                            const SljitLocalPerfectHashAggregateLane &lane,
                                                            sljit_s32 state_reg, sljit_s32 group_pointer_reg,
                                                            idx_t state_offset, idx_t value_offset,
                                                            idx_t state_is_set_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(group_pointer_reg), lane.lower_offset);
	EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, state_reg, state_offset, value_offset,
	                                                  state_is_set_offset, SLJIT_R2);
}

static void EmitSljitSparseLocalPerfectHashCommitInt64(struct sljit_compiler *compiler,
                                                       const SljitLocalPerfectHashAggregateLane &lane,
                                                       sljit_s32 state_reg, sljit_s32 group_pointer_reg,
                                                       idx_t state_offset, idx_t value_offset,
                                                       idx_t state_is_set_offset, bool value_known_seen) {
	sljit_jump *no_value = nullptr;
	if (!value_known_seen) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(group_pointer_reg), lane.saw_offset);
		no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	}
	EmitSljitSparseLocalPerfectHashCommitInt64Value(compiler, lane, state_reg, group_pointer_reg, state_offset,
	                                                value_offset, state_is_set_offset);
	if (no_value) {
		sljit_set_label(no_value, sljit_emit_label(compiler));
	}
}

static void EmitSljitSparseLocalPerfectHashCommitHugeintValue(struct sljit_compiler *compiler,
                                                              const SljitLocalPerfectHashAggregateLane &lane,
                                                              sljit_s32 state_reg, sljit_s32 group_pointer_reg,
                                                              idx_t state_offset, idx_t value_offset,
                                                              idx_t state_is_set_offset) {
	EmitSljitGroupedAggregateValuePointerImmediate(compiler, state_reg, state_offset, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(group_pointer_reg), lane.lower_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD | SLJIT_SET_CARRY, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower), SLJIT_R3, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_CARRY);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(group_pointer_reg), lane.upper_offset);
	// Reconstruct the local hugeint upper word from overflow corrections plus final lower sign.
	sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R2, 0);
	EmitSljitAccumulateHugeintUpperIfNeeded(compiler, SLJIT_R0, offsetof(hugeint_t, upper), SLJIT_R4, SLJIT_R1);
	EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, state_reg, state_offset, state_is_set_offset);
}

static void EmitSljitSparseLocalPerfectHashCommitHugeint(struct sljit_compiler *compiler,
                                                         const SljitLocalPerfectHashAggregateLane &lane,
                                                         sljit_s32 state_reg, sljit_s32 group_pointer_reg,
                                                         idx_t state_offset, idx_t value_offset,
                                                         idx_t state_is_set_offset, bool value_known_seen) {
	sljit_jump *no_value = nullptr;
	if (!value_known_seen) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(group_pointer_reg), lane.saw_offset);
		no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	}
	EmitSljitSparseLocalPerfectHashCommitHugeintValue(compiler, lane, state_reg, group_pointer_reg, state_offset,
	                                                  value_offset, state_is_set_offset);
	if (no_value) {
		sljit_set_label(no_value, sljit_emit_label(compiler));
	}
}

static void EmitSljitLocalPerfectHashCommit(struct sljit_compiler *compiler,
                                            const SljitLocalPerfectHashAggregatePlan &local_plan,
                                            const vector<ExecutionRegionAggregateInput> &aggregates,
                                            const ExecutionRegionAggregateContract &contract,
                                            bool local_payloads_known_seen = false) {
	if (!local_plan.enabled) {
		return;
	}
	if (local_plan.sparse) {
		if (local_plan.sparse_eager_zero) {
			auto &count_lane = local_plan.lanes[local_plan.sparse_count_seen_lane];
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
			auto loop = sljit_emit_label(compiler);
			auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM,
			                           NumericCast<sljit_sw>(local_plan.group_count));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
			EmitSljitSparseLocalPerfectHashGroupPointer(compiler, local_plan, SLJIT_S3, SLJIT_PERFECT_HASH_STATE_REG);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_PERFECT_HASH_STATE_REG),
			               count_lane.count_offset);
			auto no_group = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, perfect_hash_group_is_set));
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 0, SLJIT_IMM, 1);
			EmitSljitLocalPerfectHashStatePointer(compiler, SLJIT_S3, SLJIT_S4);
			for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
				auto &aggregate = aggregates[payload_idx];
				auto &lane = local_plan.lanes[payload_idx];
				const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
				if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					EmitSljitSparseLocalPerfectHashCommitCount(compiler, lane, SLJIT_S4, SLJIT_PERFECT_HASH_STATE_REG,
					                                           state_offset,
					                                           aggregate.primitive_update_state_value_offset, true);
				} else if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
					EmitSljitSparseLocalPerfectHashCommitInt64(compiler, lane, SLJIT_S4, SLJIT_PERFECT_HASH_STATE_REG,
					                                           state_offset,
					                                           aggregate.primitive_update_state_value_offset,
					                                           aggregate.primitive_update_state_is_set_offset, true);
				} else {
					EmitSljitSparseLocalPerfectHashCommitHugeint(compiler, lane, SLJIT_S4, SLJIT_PERFECT_HASH_STATE_REG,
					                                             state_offset,
					                                             aggregate.primitive_update_state_value_offset,
					                                             aggregate.primitive_update_state_is_set_offset, true);
				}
			}
			sljit_set_label(no_group, sljit_emit_label(compiler));
			EmitNextSljitNativeVectorLoop(compiler, loop);
			sljit_set_label(done, sljit_emit_label(compiler));
			return;
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_SP), local_plan.active_count_offset);
		auto loop = sljit_emit_label(compiler);
		auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitLocalArrayValue(compiler, local_plan.active_groups_offset, SLJIT_S1, SLJIT_S3);
		EmitSljitSparseLocalPerfectHashGroupPointer(compiler, local_plan, SLJIT_S3, SLJIT_PERFECT_HASH_STATE_REG);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_group_is_set));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 0, SLJIT_IMM, 1);
		EmitSljitLocalPerfectHashStatePointer(compiler, SLJIT_S3, SLJIT_S4);
		for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			auto &lane = local_plan.lanes[payload_idx];
			const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				EmitSljitSparseLocalPerfectHashCommitCount(compiler, lane, SLJIT_S4, SLJIT_PERFECT_HASH_STATE_REG,
				                                           state_offset, aggregate.primitive_update_state_value_offset,
				                                           true);
			} else if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
				EmitSljitSparseLocalPerfectHashCommitInt64(compiler, lane, SLJIT_S4, SLJIT_PERFECT_HASH_STATE_REG,
				                                           state_offset, aggregate.primitive_update_state_value_offset,
				                                           aggregate.primitive_update_state_is_set_offset,
				                                           local_payloads_known_seen || lane.value_always_seen);
			} else {
				EmitSljitSparseLocalPerfectHashCommitHugeint(
				    compiler, lane, SLJIT_S4, SLJIT_PERFECT_HASH_STATE_REG, state_offset,
				    aggregate.primitive_update_state_value_offset, aggregate.primitive_update_state_is_set_offset,
				    local_payloads_known_seen || lane.value_always_seen);
			}
		}
		EmitNextSljitNativeVectorLoop(compiler, loop);
		sljit_set_label(done, sljit_emit_label(compiler));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM,
	                           NumericCast<sljit_sw>(local_plan.group_count));
	auto group_not_seen = EmitJumpIfSljitLocalArrayZero(compiler, local_plan.group_seen_offset, SLJIT_S1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_group_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 1);
	EmitSljitLocalPerfectHashStatePointer(compiler, SLJIT_S1, SLJIT_S4);
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &lane = local_plan.lanes[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitLocalPerfectHashCommitCount(compiler, lane, SLJIT_S4, SLJIT_S1, state_offset,
			                                     aggregate.primitive_update_state_value_offset, true);
		} else if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitSljitLocalPerfectHashCommitInt64(
			    compiler, lane, SLJIT_S4, SLJIT_S1, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, local_payloads_known_seen || lane.value_always_seen);
		} else {
			EmitSljitLocalPerfectHashCommitHugeint(
			    compiler, lane, SLJIT_S4, SLJIT_S1, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, local_payloads_known_seen || lane.value_always_seen);
		}
	}
	sljit_set_label(group_not_seen, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

static void EmitSljitDeferredPerfectHashFlagsCommit(struct sljit_compiler *compiler,
                                                    const SljitDeferredPerfectHashFlagPlan &deferred_plan,
                                                    const vector<ExecutionRegionAggregateInput> &aggregates,
                                                    const ExecutionRegionAggregateContract &contract) {
	if (!deferred_plan.enabled) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_IMM,
	                           NumericCast<sljit_sw>(deferred_plan.group_count));
	auto group_not_seen = EmitJumpIfSljitLocalArrayZero(compiler, deferred_plan.group_seen_offset, SLJIT_S1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, perfect_hash_group_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 1);
	EmitSljitLocalPerfectHashStatePointer(compiler, SLJIT_S1, SLJIT_S4);
	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
		EmitSljitGroupedAggregateSetStateIsSetImmediate(compiler, SLJIT_S4, state_offset,
		                                                aggregate.primitive_update_state_is_set_offset);
	}
	sljit_set_label(group_not_seen, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePerfectHashGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<ExecutionRegionGroupInput> &groups, const vector<SljitNativeRegionExpressionPlan> &group_expressions,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error) {
	vector<SljitPerfectHashGroupPlan> group_plans;
	if (!TryBuildSljitPerfectHashGroupPlans(groups, group_expressions, contract, group_plans) || group_plans.empty() ||
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
	EmitInitSljitNativeVectorLoop(compiler);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	for (idx_t group_idx = 0; group_idx < group_plans.size(); group_idx++) {
		auto &group = group_plans[group_idx];
		EmitLoadFusedAggregateGroupSourceIndex(compiler, group_idx, SLJIT_R1);
		auto group_is_null = EmitFusedAggregateJumpIfGroupValidityNull(compiler, group_idx, SLJIT_R1);
		EmitLoadFusedAggregateGroupData(compiler, group_idx, group, SLJIT_R1, SLJIT_R2, false, SLJIT_R0);
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
		                                  SLJIT_R1, true);
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
	EmitNextSljitNativeVectorLoop(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	auto range_error_label = sljit_emit_label(compiler);
	for (auto jump : group_out_of_range) {
		sljit_set_label(jump, range_error_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeRuntimeError));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

static unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdateInternal(
    const ExecutionExpressionIR *predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<ExecutionRegionGroupInput> &groups,
    const vector<SljitNativeRegionExpressionPlan> &group_expressions, const ExecutionRegionAggregateContract &contract,
    const vector<bool> &source_not_null, const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	vector<SljitPerfectHashGroupPlan> group_plans;
	SljitUngroupedFusedTypedAggregateCodegenPlan codegen_plan;
	if (!TryBuildSljitPerfectHashGroupPlans(groups, group_expressions, contract, group_plans) || group_plans.empty() ||
	    !contract.grouped_state_layout_ready ||
	    !BuildSljitUngroupedFusedTypedAggregateCodegenPlan(payloads, aggregates, codegen_plan)) {
		error = "unsupported fused perfect-hash typed aggregate payload shape";
		return nullptr;
	}
	SljitTypedExpressionTreePlan predicate_plan;
	if (predicate) {
		predicate_plan = BuildSljitTypedExpressionTreePlan(*predicate, false);
		if (!predicate_plan.supported || !predicate_plan.result_is_bool) {
			error = "unsupported filtered fused perfect-hash aggregate predicate shape";
			return nullptr;
		}
		codegen_plan.tree_node_count += predicate_plan.node_count;
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported && predicate_plan.fast_path.fast_path_supported;
	}
	if (contract.perfect_required_bits_total >= 8 * sizeof(idx_t)) {
		error = "unsupported fused perfect-hash typed aggregate domain size";
		return nullptr;
	}
	const auto perfect_hash_group_count = idx_t(1) << contract.perfect_required_bits_total;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedTypedAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx],
		                                                     contract)) {
			error = "unsupported fused perfect-hash typed aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto tree_local_size = NumericCast<sljit_sw>(codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	const auto state_pointer_offset = tree_local_size;
	const auto group_index_offset = state_pointer_offset + NumericCast<sljit_sw>(sizeof(uintptr_t));
	const auto binary_shared_value_offset = group_index_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	sljit_sw local_size = binary_shared_value_offset;
	if (codegen_plan.binary_shared_payload) {
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	auto payloads_not_null = BuildSljitAggregatePayloadNotNull(payloads, aggregates, source_not_null);
	SljitLocalPerfectHashAggregatePlan local_aggregate_plan;
	TryBuildSljitLocalPerfectHashAggregatePlan(aggregates, contract, payloads_not_null, local_size,
	                                           local_aggregate_plan);
	AnnotateSljitLocalPerfectHashAggregatePlan(local_aggregate_plan, payloads, aggregates, source_min_values,
	                                           source_max_values);
	vector<SljitSparseLocalRunCachedLane> sparse_run_cached_lanes;
	const bool sparse_run_cache_enabled = codegen_plan.fast_path_supported && SLJIT_HAS_SPARSE_LOCAL_RUN_CACHE_REGS &&
	                                      local_aggregate_plan.enabled && local_aggregate_plan.sparse &&
	                                      !local_aggregate_plan.sparse_eager_zero &&
	                                      SljitSparseLocalUsesCountSeen(local_aggregate_plan);
	const bool sparse_run_cache_count_accepted_rows = sparse_run_cache_enabled && predicate;
	const auto sparse_run_cacheable_lane_count =
	    sparse_run_cache_enabled ? CountSljitSparseLocalRunCacheableLanes(local_aggregate_plan, aggregates) : idx_t(0);
	auto source_data_hoists = BuildSljitPerfectHashSourceDataPointerHoists(payloads);
	const bool hoist_source_data_pointers = SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS &&
	                                        source_data_hoists.size() >= group_plans.size() &&
	                                        !source_data_hoists.empty();
	const bool group_data_pointer_hoist_candidate =
	    !hoist_source_data_pointers && SLJIT_HAS_PERFECT_HASH_GROUP_DATA_REGS && group_plans.size() <= 2;
	const bool sparse_run_cache_payload_register_mode =
	    sparse_run_cache_enabled && sparse_run_cacheable_lane_count >= 3 && group_plans.size() <= 2 &&
	    !hoist_source_data_pointers && !group_data_pointer_hoist_candidate;
	if (sparse_run_cache_enabled) {
		vector<sljit_s32> sparse_run_cache_lower_regs;
		sparse_run_cache_lower_regs.push_back(SLJIT_SPARSE_LOCAL_RUN_LOWER0_REG);
		if (sparse_run_cache_payload_register_mode) {
			sparse_run_cache_lower_regs.push_back(SLJIT_SPARSE_LOCAL_RUN_LOWER1_REG);
			sparse_run_cache_lower_regs.push_back(SLJIT_SPARSE_LOCAL_RUN_LOWER2_REG);
		}
		sparse_run_cached_lanes =
		    BuildSljitSparseLocalRunCachedLanes(local_aggregate_plan, aggregates, sparse_run_cache_lower_regs);
	}
	sljit_sw sparse_run_cached_group_offset = -1;
	sljit_sw sparse_run_cached_pointer_offset = -1;
	sljit_sw sparse_run_cached_start_offset = -1;
	sljit_sw sparse_run_cached_count_offset = -1;
	if (sparse_run_cache_enabled) {
		sparse_run_cached_group_offset = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		sparse_run_cached_pointer_offset = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(uintptr_t));
		sparse_run_cached_start_offset = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		if (sparse_run_cache_count_accepted_rows) {
			sparse_run_cached_count_offset = local_size;
			local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		}
	}
	SljitDeferredPerfectHashFlagPlan deferred_flag_plan;
	if (!local_aggregate_plan.enabled) {
		TryBuildSljitDeferredPerfectHashFlagPlan(aggregates, contract, local_size, deferred_flag_plan);
	}
	auto fast_source_data_hoists =
	    sparse_run_cache_enabled
	        ? (hoist_source_data_pointers ? source_data_hoists : vector<SljitTypedExpressionTreeDataPointerHoist>())
	        : (codegen_plan.fast_path_supported
	               ? (hoist_source_data_pointers ? BuildSljitPerfectHashSourceDataPointerHoists(payloads, 3, true)
	                                             : BuildSljitPerfectHashSpareFastSourceDataPointerHoists(payloads))
	               : source_data_hoists);
	if (!hoist_source_data_pointers) {
		source_data_hoists.clear();
	} else if (fast_source_data_hoists.size() < source_data_hoists.size()) {
		fast_source_data_hoists = source_data_hoists;
	}
	const auto data_hoists = hoist_source_data_pointers ? &source_data_hoists : nullptr;
	const bool hoist_group_data_pointers =
	    !sparse_run_cache_payload_register_mode && group_data_pointer_hoist_candidate;
	const bool hoist_fast_source_data_pointers =
	    !fast_source_data_hoists.empty() &&
	    (!hoist_source_data_pointers || fast_source_data_hoists.size() > source_data_hoists.size());
	const auto fast_data_hoists = hoist_fast_source_data_pointers ? &fast_source_data_hoists : data_hoists;
	const bool hoist_fast_group_data_array_base = hoist_source_data_pointers && codegen_plan.fast_path_supported &&
	                                              local_aggregate_plan.sparse &&
	                                              SljitCanPrecomputePerfectHashStringGroupOffset(group_plans);
	const bool dedicated_state_register = SLJIT_HAS_DEDICATED_PERFECT_HASH_STATE_REG && !local_aggregate_plan.enabled;
	const sljit_s32 state_pointer_reg = dedicated_state_register ? SLJIT_PERFECT_HASH_STATE_REG : SLJIT_S4;
	sljit_s32 saved_register_count = (dedicated_state_register || local_aggregate_plan.sparse)
	                                     ? SLJIT_PERFECT_HASH_SAVED_REG_COUNT
	                                     : NumericCast<sljit_s32>(7);
	if (hoist_group_data_pointers || hoist_source_data_pointers) {
		saved_register_count = SLJIT_PERFECT_HASH_GROUP_DATA_SAVED_REG_COUNT;
	}
	if (sparse_run_cache_enabled && saved_register_count < SLJIT_SPARSE_LOCAL_RUN_SAVED_REG_COUNT) {
		saved_register_count = SLJIT_SPARSE_LOCAL_RUN_SAVED_REG_COUNT;
	}

	vector<SljitExpressionTreeOverflowJumps> overflows;
	vector<sljit_jump *> group_out_of_range;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, saved_register_count, local_size);
	EmitZeroSljitLocalPerfectHashAggregateArrays(compiler, local_aggregate_plan);
	EmitZeroSljitDeferredPerfectHashFlagArray(compiler, deferred_flag_plan);
	if (sparse_run_cache_enabled) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_group_offset, SLJIT_IMM, -1);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_pointer_offset, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_start_offset, SLJIT_IMM, 0);
		if (sparse_run_cache_count_accepted_rows) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_count_offset, SLJIT_IMM, 0);
		}
	}
	EmitInitSljitNativeVectorLoop(compiler);
	EmitInitSljitNativeVectorSourceArrays(compiler);
	if (hoist_source_data_pointers) {
		for (auto &hoist : source_data_hoists) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.data_reg, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(hoist.source_index));
		}
	}
	if (hoist_group_data_pointers) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_data_array));
		for (idx_t group_idx = 0; group_idx < group_plans.size(); group_idx++) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SljitPerfectHashGroupDataPointerReg(group_idx), 0,
			               SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(group_idx));
		}
	}
	auto emit_fast_source_data_hoists = [&]() {
		if (!hoist_fast_source_data_pointers) {
			return;
		}
		for (idx_t hoist_idx = source_data_hoists.size(); hoist_idx < fast_source_data_hoists.size(); hoist_idx++) {
			auto &hoist = fast_source_data_hoists[hoist_idx];
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.data_reg, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(hoist.source_index));
		}
	};
	auto emit_fast_group_data_array_base = [&]() {
		if (!hoist_fast_group_data_array_base) {
			return;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_PERFECT_HASH_STATE_REG, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_data_array));
	};
	auto emit_predicate_skip_jumps =
	    [&](bool fast_path, bool all_valid, bool no_source_selection,
	        const vector<SljitTypedExpressionTreeDataPointerHoist> *predicate_data_hoists) {
		    vector<sljit_jump *> result;
		    if (!predicate) {
			    return result;
		    }
		    if (fast_path) {
			    idx_t predicate_spill_index = 0;
			    EmitSljitTypedExpressionTreeFastValueReg(compiler, *predicate, predicate_spill_index, overflows,
			                                             predicate_data_hoists);
			    result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
			    return result;
		    }
		    if (all_valid && no_source_selection) {
			    idx_t predicate_spill_index = 0;
			    EmitSljitTypedExpressionTreeLogicalFastValueReg(compiler, *predicate, predicate_spill_index, overflows,
			                                                    predicate_data_hoists);
			    result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
			    return result;
		    }
		    sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
		                   offsetof(SljitNativeVectorInput, source_sel_array));
		    if (all_valid) {
			    idx_t predicate_spill_index = 0;
			    EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *predicate, predicate_spill_index, overflows,
			                                                     predicate_data_hoists);
			    result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
			    return result;
		    }
		    idx_t predicate_slot_index = 0;
		    auto predicate_slot =
		        EmitSljitTypedExpressionTreeValue(compiler, *predicate, predicate_slot_index, overflows);
		    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.valid_offset);
		    result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
		    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.value_offset);
		    result.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		    return result;
	    };
	auto emit_predicate_skip_label = [&](const vector<sljit_jump *> &predicate_skip_jumps) {
		if (predicate_skip_jumps.empty()) {
			return;
		}
		auto predicate_skip_label = sljit_emit_label(compiler);
		for (auto jump : predicate_skip_jumps) {
			sljit_set_label(jump, predicate_skip_label);
		}
	};

	auto emit_group_lookup = [&](bool check_group_validity, bool materialize_state_pointer, bool defer_flags,
	                             bool direct_group_index, bool mark_local_payloads_seen,
	                             bool use_fast_group_data_array_base, bool mark_local_group = true,
	                             bool increment_count_seen = true, bool group_selection_all_present = false,
	                             sljit_s32 group_sel_array_base_reg = 0,
	                             sljit_s32 group_data_array_base_reg_override = 0) {
		bool group_index_initialized = false;
		const auto group_index_mode =
		    direct_group_index ? SljitFusedAggregateGroupIndexMode::LOGICAL
		                       : (group_selection_all_present ? SljitFusedAggregateGroupIndexMode::SELECTED_PRESENT
		                                                      : SljitFusedAggregateGroupIndexMode::SELECTED_NULLABLE);
		const bool use_precomputed_string_offset =
		    direct_group_index && !check_group_validity && SljitCanPrecomputePerfectHashStringGroupOffset(group_plans);
		if (use_precomputed_string_offset) {
			sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_S3, 0, SLJIT_IMM, SLJIT_STRING_T_SHIFT);
		}
		if (check_group_validity) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
			group_index_initialized = true;
		}
		for (idx_t group_idx = 0; group_idx < group_plans.size(); group_idx++) {
			auto &group = group_plans[group_idx];
			EmitLoadFusedAggregateGroupSourceIndex(compiler, group_idx, SLJIT_R1, group_index_mode,
			                                       group_sel_array_base_reg);
			sljit_jump *group_is_null = nullptr;
			if (check_group_validity) {
				group_is_null = EmitFusedAggregateJumpIfGroupValidityNull(compiler, group_idx, SLJIT_R1);
			}
			const auto group_data_reg =
			    hoist_group_data_pointers ? SljitPerfectHashGroupDataPointerReg(group_idx) : SLJIT_R0;
			const auto group_data_array_base_reg =
			    use_fast_group_data_array_base ? SLJIT_PERFECT_HASH_STATE_REG : group_data_array_base_reg_override;
			EmitLoadFusedAggregateGroupData(compiler, group_idx, group, SLJIT_R1, SLJIT_R2, hoist_group_data_pointers,
			                                group_data_reg, use_precomputed_string_offset, group_data_array_base_reg);
			const auto group_offset = 1 - group.minimum;
			if (group_offset != 0) {
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
				               NumericCast<sljit_sw>(group_offset));
			}
			if (group.shift != 0) {
				sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
				               NumericCast<sljit_sw>(group.shift));
			}
			if (group_index_initialized) {
				sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R2, 0);
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_R2, 0);
				group_index_initialized = true;
			}
			if (group_is_null) {
				auto group_done = sljit_emit_jump(compiler, SLJIT_JUMP);
				auto group_null_label = sljit_emit_label(compiler);
				sljit_set_label(group_is_null, group_null_label);
				sljit_set_label(group_done, sljit_emit_label(compiler));
			}
		}
		group_out_of_range.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S4, 0, SLJIT_IMM,
		                                            NumericCast<sljit_sw>(perfect_hash_group_count)));
		if (!materialize_state_pointer) {
			if (!mark_local_group) {
				return;
			}
			if (!local_aggregate_plan.sparse) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), group_index_offset, SLJIT_S4, 0);
			}
			EmitMarkSljitLocalPerfectHashGroupSeen(compiler, local_aggregate_plan, SLJIT_S4,
			                                       SLJIT_PERFECT_HASH_STATE_REG, mark_local_payloads_seen,
			                                       increment_count_seen);
			return;
		}
		if (defer_flags) {
			EmitMarkSljitDeferredPerfectHashGroupSeen(compiler, deferred_flag_plan, SLJIT_S4);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, perfect_hash_group_is_set));
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), 0, SLJIT_IMM, 1);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_tuple_size));
		sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S4, 0);
		const auto computed_state_reg = dedicated_state_register ? state_pointer_reg : SLJIT_S4;
		sljit_emit_op1(compiler, SLJIT_MOV_P, computed_state_reg, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_state_data));
		sljit_emit_op2(compiler, SLJIT_ADD, computed_state_reg, 0, computed_state_reg, 0, SLJIT_R1, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, perfect_hash_aggregate_state_offset));
		sljit_emit_op2(compiler, SLJIT_ADD, computed_state_reg, 0, computed_state_reg, 0, SLJIT_R1, 0);
		if (!dedicated_state_register) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), state_pointer_offset, SLJIT_S4, 0);
		}
	};

	auto emit_payload_updates = [&](bool fast_path, bool all_valid, bool no_source_selection,
	                                const vector<SljitTypedExpressionTreeDataPointerHoist> *payload_data_hoists,
	                                const vector<SljitSparseLocalRunCachedLane> *run_cached_lanes = nullptr) {
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				if (local_aggregate_plan.enabled) {
					if (local_aggregate_plan.sparse && payload_idx == local_aggregate_plan.sparse_count_seen_lane) {
						continue;
					}
					if (local_aggregate_plan.sparse) {
						EmitSljitSparseLocalPerfectHashIncrementCount(compiler, local_aggregate_plan.lanes[payload_idx],
						                                              SLJIT_PERFECT_HASH_STATE_REG);
					} else {
						sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), group_index_offset);
						EmitSljitLocalPerfectHashIncrementCount(compiler, local_aggregate_plan.lanes[payload_idx],
						                                        SLJIT_S4);
					}
					continue;
				}
				if (!dedicated_state_register) {
					sljit_emit_op1(compiler, SLJIT_MOV_P, state_pointer_reg, 0, SLJIT_MEM1(SLJIT_SP),
					               state_pointer_offset);
				}
				if (deferred_flag_plan.enabled && all_valid) {
					EmitSljitGroupedAggregateIncrementInt64ImmediateDirect(
					    compiler, state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset);
				} else {
					EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, state_pointer_reg, state_offset,
					                                                 aggregate.primitive_update_state_value_offset);
				}
				continue;
			}

			sljit_jump *payload_invalid = nullptr;
			if (codegen_plan.binary_shared_payload && all_valid && payload_idx == codegen_plan.binary_dependent_lane) {
				EmitSljitBinarySharedPayloadValueReg(compiler, codegen_plan, binary_shared_value_offset, fast_path,
				                                     no_source_selection, overflows, payload_data_hoists);
			} else if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
				payload_invalid = EmitLoadFusedTypedAggregateReferenceValue(compiler, payloads[payload_idx],
				                                                            !fast_path && !no_source_selection,
				                                                            !all_valid, SLJIT_S3, payload_data_hoists);
			} else if (fast_path) {
				idx_t payload_spill_index = 0;
				EmitSljitTypedExpressionTreeFastValueReg(compiler, *payloads[payload_idx].expression_tree,
				                                         payload_spill_index, overflows, payload_data_hoists);
			} else if (all_valid && no_source_selection) {
				idx_t payload_spill_index = 0;
				EmitSljitTypedExpressionTreeLogicalFastValueReg(compiler, *payloads[payload_idx].expression_tree,
				                                                payload_spill_index, overflows, payload_data_hoists);
			} else if (all_valid) {
				idx_t payload_spill_index = 0;
				sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
				               offsetof(SljitNativeVectorInput, source_sel_array));
				EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *payloads[payload_idx].expression_tree,
				                                                 payload_spill_index, overflows, payload_data_hoists);
			} else {
				idx_t payload_slot_index = 0;
				sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
				               offsetof(SljitNativeVectorInput, source_sel_array));
				auto payload_slot = EmitSljitTypedExpressionTreeValue(compiler, *payloads[payload_idx].expression_tree,
				                                                      payload_slot_index, overflows);
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.valid_offset);
				payload_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
			}
			if (codegen_plan.binary_shared_payload && all_valid && payload_idx == codegen_plan.binary_base_lane) {
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), binary_shared_value_offset, SLJIT_R2, 0);
			}

			if (local_aggregate_plan.enabled) {
				if (local_aggregate_plan.sparse) {
					auto cached_lane = run_cached_lanes && all_valid
					                       ? FindSljitSparseLocalRunCachedLane(*run_cached_lanes, payload_idx)
					                       : nullptr;
					if (cached_lane) {
						EmitSljitSparseLocalRunCacheAccumulate(compiler, local_aggregate_plan.lanes[payload_idx],
						                                       aggregate.primitive_update_kind, cached_lane->lower_reg,
						                                       SLJIT_PERFECT_HASH_STATE_REG, SLJIT_R2);
					} else {
						EmitSljitSparseLocalPerfectHashAccumulate(compiler, local_aggregate_plan.lanes[payload_idx],
						                                          aggregate.primitive_update_kind,
						                                          SLJIT_PERFECT_HASH_STATE_REG, SLJIT_R2, !all_valid);
					}
				} else {
					sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_SP), group_index_offset);
					EmitSljitLocalPerfectHashAccumulate(compiler, local_aggregate_plan.lanes[payload_idx],
					                                    aggregate.primitive_update_kind, SLJIT_S4, SLJIT_R2);
				}
				if (payload_invalid) {
					auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
					sljit_set_label(payload_invalid, sljit_emit_label(compiler));
					sljit_set_label(payload_done, sljit_emit_label(compiler));
				}
				continue;
			}

			if (!dedicated_state_register) {
				sljit_emit_op1(compiler, SLJIT_MOV_P, state_pointer_reg, 0, SLJIT_MEM1(SLJIT_SP), state_pointer_offset);
			}
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
				if (deferred_flag_plan.enabled && all_valid) {
					EmitSljitGroupedAggregateAccumulateInt64ImmediateNoStateSet(
					    compiler, state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset,
					    SLJIT_R2);
				} else {
					EmitSljitGroupedAggregateAccumulateInt64Immediate(
					    compiler, state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset,
					    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
				}
			} else {
				if (deferred_flag_plan.enabled && all_valid) {
					EmitSljitGroupedAggregateAccumulateHugeintImmediateNoStateSet(
					    compiler, state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset,
					    SLJIT_R2);
				} else {
					EmitSljitGroupedAggregateAccumulateHugeintImmediate(
					    compiler, state_pointer_reg, state_offset, aggregate.primitive_update_state_value_offset,
					    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
				}
			}
			if (payload_invalid) {
				auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
				sljit_set_label(payload_invalid, sljit_emit_label(compiler));
				sljit_set_label(payload_done, sljit_emit_label(compiler));
			}
		}
	};

	struct sljit_jump *fast_done = nullptr;
	struct sljit_jump *logical_fast_done = nullptr;
	struct sljit_jump *common_selected_group_present_fast_done = nullptr;
	struct sljit_jump *common_selected_fast_done = nullptr;
	struct sljit_jump *selected_fast_done = nullptr;
	struct sljit_jump *done = nullptr;
	const bool can_use_common_selected_group_data_base_reg = dedicated_state_register || local_aggregate_plan.sparse;
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		emit_fast_source_data_hoists();

		auto fast_loop = sljit_emit_label(compiler);
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadFusedAggregateExecuteIndex(compiler, true);
		auto predicate_skip_jumps = emit_predicate_skip_jumps(true, true, false, fast_data_hoists);
		if (sparse_run_cache_enabled) {
			emit_fast_group_data_array_base();
			emit_group_lookup(false, false, deferred_flag_plan.enabled, true, false, hoist_fast_group_data_array_base,
			                  false);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_group_offset);
			auto group_changed = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_S4, 0);
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_PERFECT_HASH_STATE_REG, 0, SLJIT_MEM1(SLJIT_SP),
			               sparse_run_cached_pointer_offset);
			EmitSljitSparseLocalRunCacheIncrementCount(compiler, sparse_run_cached_count_offset);
			emit_payload_updates(true, true, false, fast_data_hoists, &sparse_run_cached_lanes);
			emit_predicate_skip_label(predicate_skip_jumps);
			EmitNextSljitNativeVectorLoop(compiler, fast_loop);

			sljit_set_label(group_changed, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_PERFECT_HASH_STATE_REG, 0, SLJIT_MEM1(SLJIT_SP),
			               sparse_run_cached_pointer_offset);
			EmitSljitSparseLocalRunCacheFlush(compiler, local_aggregate_plan, sparse_run_cached_lanes,
			                                  SLJIT_PERFECT_HASH_STATE_REG, sparse_run_cached_group_offset,
			                                  sparse_run_cached_start_offset, SLJIT_S1, sparse_run_cached_count_offset);
			EmitMarkSljitLocalPerfectHashGroupSeen(compiler, local_aggregate_plan, SLJIT_S4,
			                                       SLJIT_PERFECT_HASH_STATE_REG, true, false);
			EmitSljitSparseLocalRunCacheLoadCurrent(compiler, local_aggregate_plan, sparse_run_cached_lanes,
			                                        SLJIT_PERFECT_HASH_STATE_REG, sparse_run_cached_start_offset,
			                                        SLJIT_S1, sparse_run_cached_count_offset);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_group_offset, SLJIT_S4, 0);
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), sparse_run_cached_pointer_offset,
			               SLJIT_PERFECT_HASH_STATE_REG, 0);
			emit_payload_updates(true, true, false, fast_data_hoists, &sparse_run_cached_lanes);
			EmitNextSljitNativeVectorLoop(compiler, fast_loop);
		} else {
			// Sparse local lookup rewrites S7 as the group payload pointer, so reload this base every row.
			emit_fast_group_data_array_base();
			emit_group_lookup(false, !local_aggregate_plan.enabled, deferred_flag_plan.enabled, true,
			                  local_aggregate_plan.sparse, hoist_fast_group_data_array_base);
			emit_payload_updates(true, true, false, fast_data_hoists);
			emit_predicate_skip_label(predicate_skip_jumps);
			EmitNextSljitNativeVectorLoop(compiler, fast_loop);
		}

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		emit_fast_source_data_hoists();
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		auto use_source_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto logical_fast_loop = sljit_emit_label(compiler);
		logical_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadFusedAggregateExecuteIndex(compiler);
		predicate_skip_jumps = emit_predicate_skip_jumps(false, true, true, fast_data_hoists);
		emit_group_lookup(false, !local_aggregate_plan.enabled, deferred_flag_plan.enabled, true,
		                  local_aggregate_plan.sparse, false);
		emit_payload_updates(false, true, true, fast_data_hoists);
		emit_predicate_skip_label(predicate_skip_jumps);
		EmitNextSljitNativeVectorLoop(compiler, logical_fast_loop);

		sljit_set_label(use_source_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_common_sel));
		auto use_per_source_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_selection_all_present));
		auto use_nullable_common_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		auto emit_load_common_source_index = [&]() {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, source_common_sel));
			sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
		};
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, group_sel_array));
		auto common_selected_group_present_fast_loop = sljit_emit_label(compiler);
		common_selected_group_present_fast_done =
		    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadFusedAggregateExecuteIndex(compiler, true);
		predicate_skip_jumps = emit_predicate_skip_jumps(false, true, false, data_hoists);
		if (can_use_common_selected_group_data_base_reg) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_PERFECT_HASH_STATE_REG, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, group_data_array));
		}
		emit_group_lookup(false, !local_aggregate_plan.enabled, deferred_flag_plan.enabled, false,
		                  local_aggregate_plan.sparse, false, true, true, true, SLJIT_S6,
		                  can_use_common_selected_group_data_base_reg ? SLJIT_PERFECT_HASH_STATE_REG : 0);
		emit_load_common_source_index();
		emit_payload_updates(false, true, true, data_hoists);
		emit_predicate_skip_label(predicate_skip_jumps);
		EmitNextSljitNativeVectorLoop(compiler, common_selected_group_present_fast_loop);

		sljit_set_label(use_nullable_common_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto common_selected_fast_loop = sljit_emit_label(compiler);
		common_selected_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadFusedAggregateExecuteIndex(compiler, true);
		predicate_skip_jumps = emit_predicate_skip_jumps(false, true, false, fast_data_hoists);
		emit_group_lookup(false, !local_aggregate_plan.enabled, deferred_flag_plan.enabled, false,
		                  local_aggregate_plan.sparse, false);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_common_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
		emit_payload_updates(false, true, true, fast_data_hoists);
		emit_predicate_skip_label(predicate_skip_jumps);
		EmitNextSljitNativeVectorLoop(compiler, common_selected_fast_loop);

		sljit_set_label(use_per_source_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto selected_fast_loop = sljit_emit_label(compiler);
		selected_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadFusedAggregateExecuteIndex(compiler);
		predicate_skip_jumps = emit_predicate_skip_jumps(false, true, false, fast_data_hoists);
		emit_group_lookup(false, !local_aggregate_plan.enabled, deferred_flag_plan.enabled, false,
		                  local_aggregate_plan.sparse, false);
		emit_payload_updates(false, true, false, fast_data_hoists);
		emit_predicate_skip_label(predicate_skip_jumps);
		EmitNextSljitNativeVectorLoop(compiler, selected_fast_loop);

		sljit_set_label(use_generic_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	auto predicate_skip_jumps = emit_predicate_skip_jumps(false, false, false, data_hoists);
	emit_group_lookup(true, !local_aggregate_plan.enabled, false, false, false, false);
	emit_payload_updates(false, false, false, data_hoists);
	emit_predicate_skip_label(predicate_skip_jumps);
	EmitNextSljitNativeVectorLoop(compiler, loop);

	sljit_jump *fast_run_cache_flushed = nullptr;
	if (sparse_run_cache_enabled && fast_done) {
		sljit_set_label(fast_done, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_PERFECT_HASH_STATE_REG, 0, SLJIT_MEM1(SLJIT_SP),
		               sparse_run_cached_pointer_offset);
		EmitSljitSparseLocalRunCacheFlush(compiler, local_aggregate_plan, sparse_run_cached_lanes,
		                                  SLJIT_PERFECT_HASH_STATE_REG, sparse_run_cached_group_offset,
		                                  sparse_run_cached_start_offset, SLJIT_S1, sparse_run_cached_count_offset);
		fast_run_cache_flushed = sljit_emit_jump(compiler, SLJIT_JUMP);
	}
	auto done_label = sljit_emit_label(compiler);
	if (fast_done && !sparse_run_cache_enabled) {
		sljit_set_label(fast_done, done_label);
	}
	if (fast_run_cache_flushed) {
		sljit_set_label(fast_run_cache_flushed, done_label);
	}
	if (logical_fast_done) {
		sljit_set_label(logical_fast_done, done_label);
	}
	if (common_selected_group_present_fast_done) {
		sljit_set_label(common_selected_group_present_fast_done, done_label);
	}
	if (common_selected_fast_done) {
		sljit_set_label(common_selected_fast_done, done_label);
	}
	if (selected_fast_done) {
		sljit_set_label(selected_fast_done, done_label);
	}
	sljit_set_label(done, done_label);
	EmitSljitLocalPerfectHashCommit(compiler, local_aggregate_plan, aggregates, contract, false);
	EmitSljitDeferredPerfectHashFlagsCommit(compiler, deferred_flag_plan, aggregates, contract);
	sljit_emit_return_void(compiler);

	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitAggregateExpressionTreeOverflowCall(compiler, overflow.op);
		sljit_emit_return_void(compiler);
	}

	auto range_error_label = sljit_emit_label(compiler);
	for (auto jump : group_out_of_range) {
		sljit_set_label(jump, range_error_label);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeRuntimeError));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const vector<ExecutionRegionGroupInput> &groups, const vector<SljitNativeRegionExpressionPlan> &group_expressions,
    const ExecutionRegionAggregateContract &contract, const vector<bool> &source_not_null,
    const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdateInternal(
	    nullptr, payloads, aggregates, groups, group_expressions, contract, source_not_null, source_min_values,
	    source_max_values, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeFilteredPerfectHashGroupedFusedTypedExpressionAggregateUpdate(
    const ExecutionExpressionIR &predicate, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<ExecutionRegionGroupInput> &groups,
    const vector<SljitNativeRegionExpressionPlan> &group_expressions, const ExecutionRegionAggregateContract &contract,
    const vector<bool> &source_not_null, const vector<Value> &source_min_values, const vector<Value> &source_max_values,
    SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativePerfectHashGroupedFusedTypedExpressionAggregateUpdateInternal(
	    &predicate, payloads, aggregates, groups, group_expressions, contract, source_not_null, source_min_values,
	    source_max_values, function, error);
}

} // namespace duckdb
