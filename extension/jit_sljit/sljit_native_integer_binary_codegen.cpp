#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "duckdb/common/exception.hpp"

#include "sljitLir.h"

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerBinaryConstant(SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, bool constant_on_left,
                                      SljitNativeVectorFunction &function, string &error,
                                      bool check_arithmetic_overflow, bool check_result_range, int64_t result_min,
                                      int64_t result_max) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	struct sljit_jump *overflow = nullptr;
	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	auto done = EmitSljitSelectedSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1, load_op,
		                                   data_scale, SLJIT_R2);
		struct sljit_jump *date_is_negative_infinity = nullptr;
		struct sljit_jump *date_is_positive_infinity = nullptr;
		if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
			date_is_negative_infinity = sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, SLJIT_R2, 0, SLJIT_IMM,
			                                           SLJIT_DATE_NEGATIVE_INFINITY_DAYS);
			date_is_positive_infinity = sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, SLJIT_R2, 0, SLJIT_IMM,
			                                           SLJIT_DATE_POSITIVE_INFINITY_DAYS);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, constant));
		auto emit_binary_op = check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op;
		switch (op) {
		case SljitNativeIntegerBinaryOp::ADD:
			sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			break;
		case SljitNativeIntegerBinaryOp::SUBTRACT:
			if (constant_on_left) {
				sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
			} else {
				sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			}
			break;
		case SljitNativeIntegerBinaryOp::MULTIPLY:
			sljit_emit_op2(compiler, emit_binary_op, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			break;
		default:
			throw InternalException("Unknown SLJIT native integer binary operator");
		}
		if (check_arithmetic_overflow) {
			overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
		}
		if (check_result_range) {
			range_too_small =
			    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
			range_too_large =
			    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
		}
		if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
			auto store_date_result = sljit_emit_label(compiler);
			sljit_set_label(date_is_negative_infinity, store_date_result);
			sljit_set_label(date_is_positive_infinity, store_date_result);
		}
		EmitStoreSljitNativeFixedWidthResult(compiler, store_op, data_scale, SLJIT_R2);
	});

	if (check_arithmetic_overflow || check_result_range) {
		auto overflow_label = sljit_emit_label(compiler);
		if (check_arithmetic_overflow) {
			sljit_set_label(overflow, overflow_label);
		}
		if (check_result_range) {
			sljit_set_label(range_too_small, overflow_label);
			sljit_set_label(range_too_large, overflow_label);
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeIntegerOverflow));
	}
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBinaryReferences(
    SljitNativeIntegerKind kind, SljitNativeIntegerBinaryOp op, SljitNativeVectorFunction &function, string &error,
    bool check_arithmetic_overflow, bool check_result_range, int64_t result_min, int64_t result_max) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto binary_op = NativeIntegerBinaryOp(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	struct sljit_jump *overflow = nullptr;
	struct sljit_jump *range_too_small = nullptr;
	struct sljit_jump *range_too_large = nullptr;
	auto done = EmitSljitTwoSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3, load_op,
		                                   data_scale, SLJIT_R2);
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                                   load_op, data_scale, SLJIT_R3);
		struct sljit_jump *date_is_negative_infinity = nullptr;
		struct sljit_jump *date_is_positive_infinity = nullptr;
		if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
			date_is_negative_infinity = sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, SLJIT_R2, 0, SLJIT_IMM,
			                                           SLJIT_DATE_NEGATIVE_INFINITY_DAYS);
			date_is_positive_infinity = sljit_emit_cmp(compiler, SLJIT_EQUAL | SLJIT_32, SLJIT_R2, 0, SLJIT_IMM,
			                                           SLJIT_DATE_POSITIVE_INFINITY_DAYS);
		}

		sljit_emit_op2(compiler, check_arithmetic_overflow ? binary_op | SLJIT_SET_OVERFLOW : binary_op, SLJIT_R2, 0,
		               SLJIT_R2, 0, SLJIT_R3, 0);
		if (check_arithmetic_overflow) {
			overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
		}
		if (check_result_range) {
			range_too_small =
			    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_min));
			range_too_large =
			    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(result_max));
		}
		if (SljitNativeIntegerKindPreservesSourceDateInfinity(kind)) {
			auto store_date_result = sljit_emit_label(compiler);
			sljit_set_label(date_is_negative_infinity, store_date_result);
			sljit_set_label(date_is_positive_infinity, store_date_result);
		}
		EmitStoreSljitNativeFixedWidthResult(compiler, store_op, data_scale, SLJIT_R2);
	});

	if (check_arithmetic_overflow || check_result_range) {
		auto overflow_label = sljit_emit_label(compiler);
		if (check_arithmetic_overflow) {
			sljit_set_label(overflow, overflow_label);
		}
		if (check_result_range) {
			sljit_set_label(range_too_small, overflow_label);
			sljit_set_label(range_too_large, overflow_label);
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeIntegerOverflow));
	}

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
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
