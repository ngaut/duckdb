#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

static void EmitStoreBoolResult(struct sljit_compiler *compiler, bool value) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, value ? 1 : 0);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeNullCheck(SljitNativeNullCheckOp op,
                                                                SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadSelectedIndex(compiler);
	auto source_is_null = EmitSkipInvalidSourceRow(compiler);

	EmitStoreBoolResult(compiler, op != SljitNativeNullCheckOp::IS_NULL);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(source_is_null, sljit_emit_label(compiler));
	EmitStoreBoolResult(compiler, op == SljitNativeNullCheckOp::IS_NULL);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	EmitNextSljitNativeVectorLoop(compiler, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerCompareConstant(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op,
                                       bool constant_on_left, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto compare_type = NativeIntegerCompareJumpType(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitSelectedSourceInvalidResultBranchLoop(
	    compiler, [&](vector<sljit_jump *> &, vector<sljit_jump *> &next_jumps) {
		    EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                       load_op, data_scale, SLJIT_R2);
		    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		                   offsetof(SljitNativeVectorInput, constant));
		    struct sljit_jump *comparison_true;
		    if (constant_on_left) {
			    comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
		    } else {
			    comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
		    }

		    EmitStoreBoolResult(compiler, false);
		    next_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		    sljit_set_label(comparison_true, sljit_emit_label(compiler));
		    EmitStoreBoolResult(compiler, true);
	    });
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerCompareReferences(SljitNativeIntegerKind kind,
                                                                               SljitNativeIntegerCompareOp op,
                                                                               SljitNativeVectorFunction &function,
                                                                               string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto compare_type = NativeIntegerCompareJumpType(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitTwoSourceInvalidResultBranchLoop(
	    compiler, [&](vector<sljit_jump *> &, vector<sljit_jump *> &next_jumps) {
		    EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                                       load_op, data_scale, SLJIT_R2);
		    EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                                       load_op, data_scale, SLJIT_R3);

		    auto comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
		    EmitStoreBoolResult(compiler, false);
		    next_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));

		    sljit_set_label(comparison_true, sljit_emit_label(compiler));
		    EmitStoreBoolResult(compiler, true);
	    });
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerInList(SljitNativeIntegerKind kind, idx_t constant_count,
                                                                    bool list_has_null, bool not_in,
                                                                    SljitNativeVectorFunction &function,
                                                                    string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeIntegerDataScale(kind);
	auto source_load_op = NativeIntegerLoadOp(kind);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 4, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitSelectedSourceInvalidResultBranchLoop(
	    compiler, [&](vector<sljit_jump *> &, vector<sljit_jump *> &next_jumps) {
		    EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                       source_load_op, source_data_scale, SLJIT_R2);
		    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);

		    auto constants_loop = sljit_emit_label(compiler);
		    auto no_match = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S3, 0, SLJIT_IMM,
		                                   NumericCast<sljit_sw>(constant_count));
		    sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		                   offsetof(SljitNativeVectorInput, constants));
		    sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 3);
		    auto match = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_R3, 0);
		    sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
		    auto repeat_constants = sljit_emit_jump(compiler, SLJIT_JUMP);
		    sljit_set_label(repeat_constants, constants_loop);

		    sljit_set_label(no_match, sljit_emit_label(compiler));
		    if (list_has_null) {
			    EmitSetResultRowInvalid(compiler);
		    } else {
			    EmitStoreBoolResult(compiler, not_in);
		    }
		    next_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));

		    sljit_set_label(match, sljit_emit_label(compiler));
		    EmitStoreBoolResult(compiler, !not_in);
	    });
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

sljit_s32 NativeIntegerLowerBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive) {
	auto result = inclusive ? SLJIT_SIG_LESS : SLJIT_SIG_LESS_EQUAL;
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
}

sljit_s32 NativeIntegerUpperBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive) {
	auto result = inclusive ? SLJIT_SIG_GREATER : SLJIT_SIG_GREATER_EQUAL;
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBetween(SljitNativeIntegerKind kind, int64_t lower,
                                                                     int64_t upper, bool lower_inclusive,
                                                                     bool upper_inclusive, bool not_between,
                                                                     SljitNativeVectorFunction &function,
                                                                     string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeIntegerDataScale(kind);
	auto source_load_op = NativeIntegerLoadOp(kind);
	auto lower_failure = NativeIntegerLowerBoundFailureJump(kind, lower_inclusive);
	auto upper_failure = NativeIntegerUpperBoundFailureJump(kind, upper_inclusive);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitSelectedSourceInvalidResultBranchLoop(
	    compiler, [&](vector<sljit_jump *> &, vector<sljit_jump *> &next_jumps) {
		    EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                       source_load_op, source_data_scale, SLJIT_R2);
		    auto lower_failed =
		        sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(lower));
		    auto upper_failed =
		        sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(upper));

		    EmitStoreBoolResult(compiler, !not_between);
		    next_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));

		    auto failed_label = sljit_emit_label(compiler);
		    sljit_set_label(lower_failed, failed_label);
		    sljit_set_label(upper_failed, failed_label);
		    EmitStoreBoolResult(compiler, not_between);
	    });
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
