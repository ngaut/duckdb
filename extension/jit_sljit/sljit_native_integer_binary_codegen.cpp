#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "duckdb/common/exception.hpp"

#include "sljitLir.h"

namespace duckdb {

// The result lands in R4 so both operands stay in R2/R3 for the overflow stub; a date operand is always on the left.
template <class EMIT_ROW>
static void EmitSljitNativeIntegerBinaryRow(struct sljit_compiler *compiler, SljitNativeIntegerKind kind,
                                            SljitNativeIntegerBinaryOp op, sljit_s32 left_reg, sljit_s32 right_reg,
                                            const SljitIntegerOverflowChecks &checks,
                                            vector<SljitArithmeticOverflowSite> &overflows, EMIT_ROW &&store_result) {
	struct sljit_jump *date_is_negative_infinity = nullptr;
	struct sljit_jump *date_is_positive_infinity = nullptr;
	if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
		date_is_negative_infinity =
		    sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, left_reg, 0, SLJIT_IMM, SLJIT_DATE_NEGATIVE_INFINITY_DAYS);
		date_is_positive_infinity =
		    sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, left_reg, 0, SLJIT_IMM, SLJIT_DATE_POSITIVE_INFINITY_DAYS);
	}
	EmitSljitCheckedIntegerBinaryOp(compiler, kind, op, SLJIT_R4, left_reg, right_reg, checks, overflows);
	if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
		auto arithmetic_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto date_infinity_label = sljit_emit_label(compiler);
		sljit_set_label(date_is_negative_infinity, date_infinity_label);
		sljit_set_label(date_is_positive_infinity, date_infinity_label);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, left_reg, 0);
		sljit_set_label(arithmetic_done, sljit_emit_label(compiler));
	}
	store_result(SLJIT_R4);
}

static unique_ptr<ExecutionRegionCodeHandle>
FinishSljitNativeIntegerBinary(struct sljit_compiler *compiler, sljit_jump *done,
                               const vector<SljitArithmeticOverflowSite> &overflows,
                               SljitNativeVectorFunction &function, string &error) {
	auto overflow_done = EmitSljitArithmeticOverflowStubs(compiler, overflows);
	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	SetSljitJumpLabels(overflow_done, done_label);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerBinaryConstant(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, bool constant_on_left,
                                      const SljitIntegerOverflowChecks &checks, SljitNativeVectorFunction &function,
                                      string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	vector<SljitArithmeticOverflowSite> overflows;
	auto done = EmitSljitSelectedSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1, load_op,
		                                   data_scale, SLJIT_R2);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, constant));
		EmitSljitNativeIntegerBinaryRow(
		    compiler, kind, op, constant_on_left ? SLJIT_R3 : SLJIT_R2, constant_on_left ? SLJIT_R2 : SLJIT_R3, checks,
		    overflows, [&](sljit_s32 result_reg) {
			    EmitStoreSljitNativeFixedWidthResult(compiler, store_op, data_scale, result_reg);
		    });
	});
	return FinishSljitNativeIntegerBinary(compiler, done, overflows, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBinaryReferences(SljitNativeIntegerKind kind,
                                                                              SljitNativeIntegerBinaryOp op,
                                                                              const SljitIntegerOverflowChecks &checks,
                                                                              SljitNativeVectorFunction &function,
                                                                              string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	vector<SljitArithmeticOverflowSite> overflows;
	auto done = EmitSljitTwoSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3, load_op,
		                                   data_scale, SLJIT_R2);
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                                   load_op, data_scale, SLJIT_R3);
		EmitSljitNativeIntegerBinaryRow(
		    compiler, kind, op, SLJIT_R2, SLJIT_R3, checks, overflows, [&](sljit_s32 result_reg) {
			    EmitStoreSljitNativeFixedWidthResult(compiler, store_op, data_scale, result_reg);
		    });
	});
	return FinishSljitNativeIntegerBinary(compiler, done, overflows, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeDecimal128WideningMultiply(SljitNativeSignedIntegerWidth left_width,
                                           SljitNativeSignedIntegerWidth right_width,
                                           SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto left_load_op = NativeSignedIntegerLoadOp(left_width);
	const auto left_data_scale = NativeSignedIntegerDataScale(left_width);
	const auto right_load_op = NativeSignedIntegerLoadOp(right_width);
	const auto right_data_scale = NativeSignedIntegerDataScale(right_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitTwoSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                                   left_load_op, left_data_scale, SLJIT_R2);
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                                   right_load_op, right_data_scale, SLJIT_R1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R2, 0);
		sljit_emit_op0(compiler, SLJIT_LMUL_SW);

		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, result_data));
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R3, 0, SLJIT_S1, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R2), offsetof(hugeint_t, lower), SLJIT_R0, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R2), offsetof(hugeint_t, upper), SLJIT_R1, 0);
	});

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
