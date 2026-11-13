#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "sljitLir.h"

#include <vector>

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64IntegerBinaryConstant(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op,
                                                       bool constant_on_left, const SljitIntegerOverflowChecks &checks,
                                                       SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	const auto sum = SljitOperand::Register(SLJIT_S3);
	const auto saw_value = SljitOperand::Register(SLJIT_S4);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SLJIT_ALL_VALID_SOURCE_LOOP_SCRATCHES, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, sum.operand, sum.operand_w, SLJIT_IMM, 0);

	vector<SljitArithmeticOverflowSite> overflows;
	// The all-valid loop keeps the constant where the saw-value flag lives: every row of the source contributes
	sljit_emit_op1(compiler, SLJIT_MOV, saw_value.operand, saw_value.operand_w, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, constant));
	auto has_validity = EmitSljitAggregateAllValidSelectedSourceLoop(compiler, [&]() {
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R7, SLJIT_R1), data_scale);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, saw_value.operand, saw_value.operand_w);
		EmitSljitCheckedIntegerBinaryOp(compiler, kind, op, SLJIT_R4, constant_on_left ? SLJIT_R3 : SLJIT_R2,
		                                constant_on_left ? SLJIT_R2 : SLJIT_R3, checks, overflows);
		sljit_emit_op2(compiler, SLJIT_ADD, sum.operand, sum.operand_w, sum.operand, sum.operand_w, SLJIT_R4, 0);
	});
	sljit_emit_op2u(compiler, SLJIT_SUB | SLJIT_SET_Z, SLJIT_S2, 0, SLJIT_IMM, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, saw_value.operand, saw_value.operand_w, SLJIT_NOT_EQUAL);
	auto all_valid_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(has_validity, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, saw_value.operand, saw_value.operand_w, SLJIT_IMM, 0);
	auto done = EmitSljitAggregateSelectedSourceLoop(compiler, [&]() {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1, load_op,
		                                   data_scale, SLJIT_R2);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, constant));
		EmitSljitCheckedIntegerBinaryOp(compiler, kind, op, SLJIT_R4, constant_on_left ? SLJIT_R3 : SLJIT_R2,
		                                constant_on_left ? SLJIT_R2 : SLJIT_R3, checks, overflows);
		EmitSljitAggregateAccumulateInt64(compiler, sum, saw_value, SLJIT_R4);
	});

	// an overflowing chunk records its error and skips the commit
	auto overflow_done = EmitSljitArithmeticOverflowStubs(compiler, overflows);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	sljit_set_label(all_valid_done, done_label);
	EmitSljitAggregateCommitInt64(compiler, sum, saw_value);
	SetSljitJumpLabels(overflow_done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64IntegerBinaryReferences(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op,
                                                         const SljitIntegerOverflowChecks &checks,
                                                         SljitNativeAggregateUpdateFunction &function, string &error) {
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

	vector<SljitArithmeticOverflowSite> overflows;
	sljit_jump *fast_commit_done = nullptr;
	if (!checks.overflow && !checks.range) {
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
		// R1 is free in this loop, so a product leaves both operands for the overflow stub
		EmitSljitCheckedIntegerBinaryOp(compiler, kind, op, SLJIT_R1, SLJIT_R2, SLJIT_R3, checks, overflows);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R1, 0);
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
		EmitSljitCheckedIntegerBinaryOp(compiler, kind, op, SLJIT_R4, SLJIT_R2, SLJIT_R3, checks, overflows);
		EmitSljitAggregateAccumulateInt64(compiler, SljitOperand::Stack(local_sum_offset),
		                                  SljitOperand::Stack(saw_value_offset), SLJIT_R4);
	});

	// an overflowing chunk records its error and skips the commit
	auto overflow_done = EmitSljitArithmeticOverflowStubs(compiler, overflows);

	auto done_label = sljit_emit_label(compiler);
	if (fast_commit_done) {
		sljit_set_label(fast_commit_done, done_label);
	}
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitInt64(compiler, SljitOperand::Stack(local_sum_offset),
	                              SljitOperand::Stack(saw_value_offset));
	SetSljitJumpLabels(overflow_done, sljit_emit_label(compiler));
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
	EmitSljitLoadDoubleStateLocal(compiler, local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto done = EmitSljitAggregateSelectedSourceLoop(compiler, [&]() {
		EmitLoadNativeDoubleOperand(compiler, source_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                            offsetof(SljitNativeVectorInput, source_decimal), SLJIT_FR0);
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
	EmitSljitLoadDoubleStateLocal(compiler, local_sum_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	auto done = EmitSljitAggregateTwoSourceLoop(compiler, [&]() {
		if (needs_helper_spill) {
			EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
			                            offsetof(SljitNativeVectorInput, source_decimal), SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), left_spill_offset);
			EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data),
			                            SLJIT_S4, offsetof(SljitNativeVectorInput, right_source_decimal), SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), right_spill_offset);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR0, SLJIT_MEM1(SLJIT_SP),
			                left_spill_offset);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_SP),
			                right_spill_offset);
		} else {
			EmitLoadNativeDoubleOperand(compiler, left_kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
			                            offsetof(SljitNativeVectorInput, source_decimal), SLJIT_FR0);
			EmitLoadNativeDoubleOperand(compiler, right_kind, offsetof(SljitNativeVectorInput, right_source_data),
			                            SLJIT_S4, offsetof(SljitNativeVectorInput, right_source_decimal), SLJIT_FR1);
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
