#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "sljitLir.h"

namespace duckdb {

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
	const auto sum = SljitOperand::Register(SLJIT_S3);
	const auto saw_value = SljitOperand::Register(SLJIT_S4);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SLJIT_ALL_VALID_SOURCE_LOOP_SCRATCHES, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, sum.operand, sum.operand_w, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, saw_value.operand, saw_value.operand_w, SLJIT_IMM, 0);

	auto has_validity = EmitSljitAggregateAllValidSelectedSourceLoop(compiler, [&]() {
		sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R7, SLJIT_R1), data_scale);
		sljit_emit_op2(compiler, SLJIT_ADD, sum.operand, sum.operand_w, sum.operand, sum.operand_w, SLJIT_R2, 0);
	});
	// every row of an all-valid source contributes a value
	sljit_emit_op2u(compiler, SLJIT_SUB | SLJIT_SET_Z, SLJIT_S2, 0, SLJIT_IMM, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, saw_value.operand, saw_value.operand_w, SLJIT_NOT_EQUAL);
	auto all_valid_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(has_validity, sljit_emit_label(compiler));

	auto done = EmitSljitAggregateSelectedSourceLoop(compiler, [&]() {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1, load_op,
		                                   data_scale, SLJIT_R2);
		EmitSljitAggregateAccumulateInt64(compiler, sum, saw_value, SLJIT_R2);
	});

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	sljit_set_label(all_valid_done, done_label);
	EmitSljitAggregateCommitInt64(compiler, sum, saw_value);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintReference(SljitNativeIntegerKind kind, SljitNativeAggregateUpdateFunction &function,
                                             string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);
	EmitSljitAggregateAddRowCount(compiler, SLJIT_S2);

	auto done = EmitSljitAggregateSelectedSourceLoop(compiler, [&]() {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1, load_op,
		                                   data_scale, SLJIT_R2);
		sljit_emit_op2(compiler, SLJIT_ASHR, SLJIT_R3, 0, SLJIT_R2, 0, SLJIT_IMM, 63);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_hugeint_value));
		EmitSljitAccumulateHugeintWords(compiler, SLJIT_R0, offsetof(hugeint_t, lower), offsetof(hugeint_t, upper),
		                                SLJIT_R2, SLJIT_R3);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	});

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeint128Reference(SljitNativeAggregateUpdateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);
	EmitSljitAggregateAddRowCount(compiler, SLJIT_S2);

	auto done = EmitSljitAggregateSelectedSourceLoop(compiler, [&]() {
		EmitLoadSljitHugeintReference(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1, SLJIT_R2,
		                              SLJIT_R3);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_hugeint_value));
		EmitSljitAccumulateHugeintWords(compiler, SLJIT_R0, offsetof(hugeint_t, lower), offsetof(hugeint_t, upper),
		                                SLJIT_R2, SLJIT_R3);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	});

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumDouble(const SljitUngroupedDoubleSumValue &value,
                                                                         SljitNativeAggregateUpdateFunction &function,
                                                                         string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	// A helper call clobbers every scratch register: a source that converts through one keeps its running sum on the
	// stack and runs only the selected row loop. Any other source keeps the sum in FR2 and the constant in FR3, and
	// runs all-valid chunks in the hoisted loops.
	const bool calls_helper = NativeDoubleSourceUsesHelper(value.kind);
	const auto spilled_sum_offset = NumericCast<sljit_sw>(0);
	const auto saw_value = SLJIT_S4;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SLJIT_ALL_VALID_SOURCE_LOOP_SCRATCHES | SLJIT_ENTER_FLOAT(4), 5,
	                 calls_helper ? NumericCast<sljit_sw>(sizeof(double)) : 0);
	EmitInitSljitNativeVectorLoop(compiler);
	// the running sum starts from the state, so rows add to it in source order, as DuckDB's sum does
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_double_value));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR2, SLJIT_MEM1(SLJIT_R0), 0);
	if (calls_helper) {
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR2,
		                SLJIT_MEM1(SLJIT_SP), spilled_sum_offset);
	} else if (value.combines_constant) {
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR3, SLJIT_MEM1(SLJIT_S0),
		                offsetof(SljitNativeVectorInput, double_constant));
	}
	sljit_emit_op1(compiler, SLJIT_MOV, saw_value, 0, SLJIT_IMM, 0);

	// leaves the row's value, at source index R1, in FR0
	auto emit_value = [&](bool hoisted_data) {
		if (hoisted_data) {
			EmitLoadNativeDoubleOperandAt(compiler, value.kind, SLJIT_R7, SLJIT_R1, SLJIT_FR0);
		} else {
			EmitLoadNativeDoubleOperand(compiler, value.kind, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
			                            offsetof(SljitNativeVectorInput, source_decimal), SLJIT_FR0);
		}
		if (!value.combines_constant) {
			return;
		}
		auto constant = SLJIT_FR3;
		if (calls_helper) {
			constant = SLJIT_FR1;
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, constant, SLJIT_MEM1(SLJIT_S0),
			                offsetof(SljitNativeVectorInput, double_constant));
		}
		if (value.constant_on_left) {
			sljit_emit_fop2(compiler, value.binary_op, SLJIT_FR0, 0, constant, 0, SLJIT_FR0, 0);
		} else {
			sljit_emit_fop2(compiler, value.binary_op, SLJIT_FR0, 0, SLJIT_FR0, 0, constant, 0);
		}
	};
	auto emit_accumulate = [&]() {
		if (!calls_helper) {
			sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_FR2, 0, SLJIT_FR2, 0, SLJIT_FR0, 0);
			return;
		}
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_SP),
		                spilled_sum_offset);
		sljit_emit_fop2(compiler, SLJIT_ADD_F64, SLJIT_FR1, 0, SLJIT_FR1, 0, SLJIT_FR0, 0);
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR1,
		                SLJIT_MEM1(SLJIT_SP), spilled_sum_offset);
	};

	vector<sljit_jump *> done_jumps;
	if (!calls_helper) {
		auto has_validity = EmitSljitAggregateAllValidSelectedSourceLoop(compiler, [&]() {
			emit_value(true);
			emit_accumulate();
		});
		// every row of an all-valid source adds a value
		sljit_emit_op2u(compiler, SLJIT_SUB | SLJIT_SET_Z, SLJIT_S2, 0, SLJIT_IMM, 0);
		sljit_emit_op_flags(compiler, SLJIT_MOV, saw_value, 0, SLJIT_NOT_EQUAL);
		done_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		sljit_set_label(has_validity, sljit_emit_label(compiler));
	}
	done_jumps.push_back(EmitSljitAggregateSelectedSourceLoop(compiler, [&]() {
		emit_value(false);
		emit_accumulate();
		sljit_emit_op1(compiler, SLJIT_MOV, saw_value, 0, SLJIT_IMM, 1);
	}));
	auto done_label = sljit_emit_label(compiler);
	for (auto jump : done_jumps) {
		sljit_set_label(jump, done_label);
	}

	EmitSljitAggregateAddRowCount(compiler, SLJIT_S2);
	auto no_value = sljit_emit_cmp(compiler, SLJIT_EQUAL, saw_value, 0, SLJIT_IMM, 0);
	if (calls_helper) {
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR2, SLJIT_MEM1(SLJIT_SP),
		                spilled_sum_offset);
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_double_value));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR2, SLJIT_MEM1(SLJIT_R0),
	                0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
	sljit_set_label(no_value, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumDoubleReference(SljitNativeDoubleSourceKind kind,
                                            SljitNativeAggregateUpdateFunction &function, string &error) {
	SljitUngroupedDoubleSumValue value;
	value.kind = kind;
	return BuildSljitNativeUngroupedSumDouble(value, function, error);
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

} // namespace duckdb
