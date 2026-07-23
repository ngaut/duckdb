#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "sljitLir.h"

#include <vector>

namespace duckdb {

struct SljitAggregateIntegerBinaryOverflowJumps {
	sljit_jump *overflow = nullptr;
	sljit_jump *range_too_small = nullptr;
	sljit_jump *range_too_large = nullptr;
};

static void EmitSljitAggregateIntegerBinaryOverflowChecks(struct sljit_compiler *compiler, sljit_s32 value_reg,
                                                          bool check_arithmetic_overflow, bool check_result_range,
                                                          int64_t result_min, int64_t result_max,
                                                          SljitAggregateIntegerBinaryOverflowJumps &jumps) {
	if (check_arithmetic_overflow) {
		jumps.overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	}
	if (check_result_range) {
		jumps.range_too_small =
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, value_reg, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
		jumps.range_too_large =
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, value_reg, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
	}
}

static sljit_jump *
EmitSljitAggregateIntegerBinaryOverflowHandler(struct sljit_compiler *compiler, SljitNativeIntegerBinaryOp op,
                                               bool check_arithmetic_overflow, bool check_result_range,
                                               const SljitAggregateIntegerBinaryOverflowJumps &jumps) {
	if (!check_arithmetic_overflow && !check_result_range) {
		return nullptr;
	}
	auto overflow_label = sljit_emit_label(compiler);
	if (check_arithmetic_overflow) {
		sljit_set_label(jumps.overflow, overflow_label);
	}
	if (check_result_range) {
		sljit_set_label(jumps.range_too_small, overflow_label);
		sljit_set_label(jumps.range_too_large, overflow_label);
	}
	EmitSljitAggregateExpressionTreeOverflowCall(compiler, op);
	return sljit_emit_jump(compiler, SLJIT_JUMP);
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

	SljitAggregateIntegerBinaryOverflowJumps overflow_jumps;
	auto done = EmitSljitAggregateSelectedSourceLoop(compiler, [&]() {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1, load_op,
		                                   data_scale, SLJIT_R2);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, constant));
		auto emit_binary_op = check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op;
		if (constant_on_left) {
			sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		}
		EmitSljitAggregateIntegerBinaryOverflowChecks(compiler, SLJIT_R2, check_arithmetic_overflow, check_result_range,
		                                              result_min, result_max, overflow_jumps);
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	});

	auto helper_done = EmitSljitAggregateIntegerBinaryOverflowHandler(compiler, op, check_arithmetic_overflow,
	                                                                  check_result_range, overflow_jumps);

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

	SljitAggregateIntegerBinaryOverflowJumps overflow_jumps;
	sljit_jump *fast_commit_done = nullptr;
	if (!check_arithmetic_overflow && !check_result_range) {
		vector<sljit_jump *> generic_loop_jumps;
		auto jump_to_generic_if_present = [&](sljit_sw input_offset) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), input_offset);
			generic_loop_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
		};
		jump_to_generic_if_present(offsetof(SljitNativeVectorInput, execute_sel));
		jump_to_generic_if_present(offsetof(SljitNativeVectorInput, source_sel));
		jump_to_generic_if_present(offsetof(SljitNativeVectorInput, right_source_sel));
		jump_to_generic_if_present(offsetof(SljitNativeVectorInput, source_validity));
		jump_to_generic_if_present(offsetof(SljitNativeVectorInput, right_source_validity));

		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_data));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, right_source_data));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_IMM, 0);

		auto fast_loop = sljit_emit_label(compiler);
		auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_S3, SLJIT_S1), data_scale);
		sljit_emit_op1(compiler, load_op, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_S4, SLJIT_S1), data_scale);
		sljit_emit_op2(compiler, binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R2, 0);
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		sljit_set_label(fast_done, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
		auto no_rows = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 1);
		sljit_set_label(no_rows, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_R1, 0);
		fast_commit_done = sljit_emit_jump(compiler, SLJIT_JUMP);

		auto generic_loop_label = sljit_emit_label(compiler);
		for (auto jump : generic_loop_jumps) {
			sljit_set_label(jump, generic_loop_label);
		}
	}
	auto done = EmitSljitAggregateTwoSourceLoop(compiler, [&]() {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3, load_op,
		                                   data_scale, SLJIT_R2);
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                                   load_op, data_scale, SLJIT_R3);
		sljit_emit_op2(compiler, check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op, SLJIT_R2, 0,
		               SLJIT_R2, 0, SLJIT_R3, 0);
		EmitSljitAggregateIntegerBinaryOverflowChecks(compiler, SLJIT_R2, check_arithmetic_overflow, check_result_range,
		                                              result_min, result_max, overflow_jumps);
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offset, saw_value_offset, SLJIT_R2);
	});

	auto helper_done = EmitSljitAggregateIntegerBinaryOverflowHandler(compiler, op, check_arithmetic_overflow,
	                                                                  check_result_range, overflow_jumps);

	auto done_label = sljit_emit_label(compiler);
	if (fast_commit_done) {
		sljit_set_label(fast_commit_done, done_label);
	}
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

	auto done = EmitSljitAggregateSelectedSourceLoop(compiler, [&]() {
		EmitLoadNativeDoubleOperand(compiler, source_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_FR0);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_S0),
		                offsetof(SljitNativeVectorInput, double_constant));
		if (constant_on_left) {
			sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR1, 0, SLJIT_FR0, 0);
		} else {
			sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
		}
		EmitSljitAggregateAccumulateDouble(compiler, local_sum_offset, saw_value_offset, SLJIT_FR0);
	});

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

	auto done = EmitSljitAggregateTwoSourceLoop(compiler, [&]() {
		if (needs_helper_spill) {
			EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
			                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), left_spill_offset);
			EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data),
			                            SLJIT_S4, offsetof(SljitNativeVectorInput, right_source_double_scale),
			                            SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), right_spill_offset);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR0, SLJIT_MEM1(SLJIT_SP),
			                left_spill_offset);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_SP),
			                right_spill_offset);
		} else {
			EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
			                            offsetof(SljitNativeVectorInput, source_double_scale), SLJIT_FR0);
			EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data),
			                            SLJIT_S4, offsetof(SljitNativeVectorInput, right_source_double_scale),
			                            SLJIT_FR1);
		}
		sljit_emit_fop2(compiler, binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, SLJIT_FR1, 0);
		EmitSljitAggregateAccumulateDouble(compiler, local_sum_offset, saw_value_offset, SLJIT_FR0);
	});

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitDouble(compiler, local_sum_offset, saw_value_offset);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
