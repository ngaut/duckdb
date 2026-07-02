#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"
#include "sljit_native_predicate_select_codegen.hpp"

#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

#include <cstddef>

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerSelectConstant(SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp op,
                                      bool constant_on_left, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto compare_type = NativeIntegerCompareJumpType(kind, op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, SLJIT_SELECT_LOCAL_SIZE);
	EmitSljitNativeSelectInitLoop(compiler);

	vector<sljit_jump *> generic_path;
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, false_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_data));
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_S1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeVectorInput, constant));
	struct sljit_jump *fast_comparison_true;
	if (constant_on_left) {
		fast_comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		fast_comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	}
	auto fast_next = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_comparison_true, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, true_sel));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), 2, SLJIT_S1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_R1, 0);
	auto fast_next_label = sljit_emit_label(compiler);
	sljit_set_label(fast_next, fast_next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(fast_done, sljit_emit_label(compiler));
	EmitSljitNativeSelectFinishLoop(compiler);

	EmitSljitNativeSelectRowLoop(compiler, [&](sljit_label *loop) {
		for (auto &jump : generic_path) {
			sljit_set_label(jump, loop);
		}

		EmitSljitNativeSelectLoadResultAndSourceIndex(compiler);
		auto source_is_null = EmitSkipInvalidSourceRow(compiler);

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

		auto comparison_false = sljit_emit_label(compiler);
		sljit_set_label(source_is_null, comparison_false);
		EmitSljitNativeSelectStoreFalse(compiler);
		auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

		sljit_set_label(comparison_true, sljit_emit_label(compiler));
		EmitSljitNativeSelectStoreTrue(compiler);

		auto next_label = sljit_emit_label(compiler);
		sljit_set_label(next, next_label);
	});

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerSelectReferences(SljitNativeIntegerKind kind,
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

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, SLJIT_SELECT_LOCAL_SIZE);
	EmitSljitNativeSelectInitLoop(compiler);

	EmitSljitNativeSelectRowLoop(compiler, [&](sljit_label *) {
		EmitSljitNativeSelectLoadResultIndex(compiler);
		EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
		EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);
		auto left_is_null =
		    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3);
		auto right_is_null =
		    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);

		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_S3,
		                                   load_op, data_scale, SLJIT_R2);
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                                   load_op, data_scale, SLJIT_R3);

		auto comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
		auto comparison_false = sljit_emit_label(compiler);
		sljit_set_label(left_is_null, comparison_false);
		sljit_set_label(right_is_null, comparison_false);
		EmitSljitNativeSelectStoreFalse(compiler);
		auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

		sljit_set_label(comparison_true, sljit_emit_label(compiler));
		EmitSljitNativeSelectStoreTrue(compiler);

		auto next_label = sljit_emit_label(compiler);
		sljit_set_label(next, next_label);
	});

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeNullCheckSelect(SljitNativeNullCheckOp op, SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, SLJIT_SELECT_LOCAL_SIZE);
	EmitSljitNativeSelectInitLoop(compiler);

	EmitSljitNativeSelectRowLoop(compiler, [&](sljit_label *) {
		EmitSljitNativeSelectLoadResultAndSourceIndex(compiler);
		auto source_is_null = EmitSkipInvalidSourceRow(compiler);

		EmitSljitNativeSelectStoreResult(compiler, op != SljitNativeNullCheckOp::IS_NULL);
		auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

		sljit_set_label(source_is_null, sljit_emit_label(compiler));
		EmitSljitNativeSelectStoreResult(compiler, op == SljitNativeNullCheckOp::IS_NULL);

		auto next_label = sljit_emit_label(compiler);
		sljit_set_label(next, next_label);
	});

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeIntegerInListSelect(SljitNativeIntegerKind kind, idx_t constant_count, bool list_has_null, bool not_in,
                                    SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeIntegerDataScale(kind);
	auto source_load_op = NativeIntegerLoadOp(kind);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 4, SLJIT_SELECT_LOCAL_SIZE);
	EmitSljitNativeSelectInitLoop(compiler);

	EmitSljitNativeSelectRowLoop(compiler, [&](sljit_label *) {
		EmitSljitNativeSelectLoadResultAndSourceIndex(compiler);
		auto source_is_null = EmitSkipInvalidSourceRow(compiler);

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
		EmitSljitNativeSelectStoreResult(compiler, !list_has_null && not_in);
		auto next_after_no_match = sljit_emit_jump(compiler, SLJIT_JUMP);

		sljit_set_label(match, sljit_emit_label(compiler));
		EmitSljitNativeSelectStoreResult(compiler, !not_in);
		auto next_after_match = sljit_emit_jump(compiler, SLJIT_JUMP);

		sljit_set_label(source_is_null, sljit_emit_label(compiler));
		EmitSljitNativeSelectStoreFalse(compiler);

		auto next_label = sljit_emit_label(compiler);
		sljit_set_label(next_after_no_match, next_label);
		sljit_set_label(next_after_match, next_label);
	});

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegerBetweenSelect(SljitNativeIntegerKind kind, int64_t lower,
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

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, SLJIT_SELECT_LOCAL_SIZE);
	EmitSljitNativeSelectInitLoop(compiler);

	EmitSljitNativeSelectRowLoop(compiler, [&](sljit_label *) {
		EmitSljitNativeSelectLoadResultAndSourceIndex(compiler);
		auto source_is_null = EmitSkipInvalidSourceRow(compiler);

		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                   source_load_op, source_data_scale, SLJIT_R2);
		auto lower_failed =
		    sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(lower));
		auto upper_failed =
		    sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(upper));

		EmitSljitNativeSelectStoreResult(compiler, !not_between);
		auto next_after_success = sljit_emit_jump(compiler, SLJIT_JUMP);

		auto failed_label = sljit_emit_label(compiler);
		sljit_set_label(lower_failed, failed_label);
		sljit_set_label(upper_failed, failed_label);
		EmitSljitNativeSelectStoreResult(compiler, not_between);
		auto next_after_failure = sljit_emit_jump(compiler, SLJIT_JUMP);

		sljit_set_label(source_is_null, sljit_emit_label(compiler));
		EmitSljitNativeSelectStoreFalse(compiler);

		auto next_label = sljit_emit_label(compiler);
		sljit_set_label(next_after_success, next_label);
		sljit_set_label(next_after_failure, next_label);
	});

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
