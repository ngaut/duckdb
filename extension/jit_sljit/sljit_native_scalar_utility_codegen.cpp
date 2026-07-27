#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_native_fixed_width_codegen.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/date.hpp"

#include "sljitLir.h"

namespace duckdb {

void EmitSljitDateYearFromDays(struct sljit_compiler *compiler, sljit_s32 days_reg, sljit_s32 target_reg) {
	static_assert(sizeof(date_t) == sizeof(int32_t), "SLJIT date-year expects DuckDB date_t ABI size");

	if (days_reg != SLJIT_R3) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, days_reg, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_IMM, Date::EPOCH_YEAR);

	auto normalize_negative_loop = sljit_emit_label(compiler);
	auto not_negative = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, Date::DAYS_PER_YEAR_INTERVAL);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, Date::YEAR_INTERVAL);
	auto repeat_negative = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_negative, normalize_negative_loop);
	sljit_set_label(not_negative, sljit_emit_label(compiler));

	auto normalize_positive_loop = sljit_emit_label(compiler);
	auto in_current_interval =
	    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R3, 0, SLJIT_IMM, Date::DAYS_PER_YEAR_INTERVAL);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, Date::DAYS_PER_YEAR_INTERVAL);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, Date::YEAR_INTERVAL);
	auto repeat_positive = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_positive, normalize_positive_loop);
	sljit_set_label(in_current_interval, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 365);
	sljit_emit_op0(compiler, SLJIT_DIV_SW);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_IMM,
	               reinterpret_cast<sljit_sw>(&Date::CUMULATIVE_YEAR_DAYS[0]));

	auto year_offset_loop = sljit_emit_label(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_S32, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_R1, SLJIT_R2), 2);
	auto have_year_offset = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER_EQUAL, SLJIT_R3, 0, SLJIT_R0, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	auto repeat_year_offset = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_year_offset, year_offset_loop);
	sljit_set_label(have_year_offset, sljit_emit_label(compiler));

	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, SLJIT_R4, 0, SLJIT_R2, 0);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegralCompress(SljitNativeSignedIntegerWidth source_width,
                                                                       SljitNativeUnsignedIntegerWidth target_width,
                                                                       SljitNativeVectorFunction &function,
                                                                       string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeSignedIntegerDataScale(source_width);
	auto source_load_op = NativeSignedIntegerLoadOp(source_width);
	auto target_data_scale = NativeUnsignedIntegerDataScale(target_width);
	auto target_store_op = NativeUnsignedIntegerStoreOp(target_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitSelectedSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                   source_load_op, source_data_scale, SLJIT_R2);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, constant));
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);

		EmitStoreSljitNativeFixedWidthResult(compiler, target_store_op, target_data_scale, SLJIT_R2);
	});
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeIntegralDecompress(SljitNativeUnsignedIntegerWidth source_width,
                                                                         SljitNativeSignedIntegerWidth target_width,
                                                                         SljitNativeVectorFunction &function,
                                                                         string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto source_data_scale = NativeUnsignedIntegerDataScale(source_width);
	auto source_load_op = NativeUnsignedIntegerLoadOp(source_width);
	auto target_data_scale = NativeSignedIntegerDataScale(target_width);
	auto target_store_op = NativeSignedIntegerStoreOp(target_width);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitSelectedSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                   source_load_op, source_data_scale, SLJIT_R2);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, constant));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);

		EmitStoreSljitNativeFixedWidthResult(compiler, target_store_op, target_data_scale, SLJIT_R2);
	});
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeDateYear(SljitNativeVectorFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	auto done = EmitSljitSelectedSourceInvalidResultLoop(compiler, [&](vector<sljit_jump *> &invalid_jumps) {
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, source_data), SLJIT_R1,
		                                   SLJIT_MOV_S32, 2, SLJIT_S3);

		invalid_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM,
		                                       NumericCast<sljit_sw>(date_t::infinity().days)));
		invalid_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM,
		                                       NumericCast<sljit_sw>(date_t::ninfinity().days)));

		EmitSljitDateYearFromDays(compiler, SLJIT_S3, SLJIT_S4);
		EmitStoreSljitNativeFixedWidthResult(compiler, SLJIT_MOV, 3, SLJIT_S4);
	});
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeErrorGuardedReference(idx_t value_size, SljitNativeIntegerCompareOp guard_compare_op,
                                      bool guard_constant_on_left, SljitNativeVectorFunction &function, string &error) {
	if (!IsSljitNativeFixedByteStorageSize(value_size)) {
		error = "SLJIT error-guarded reference has unsupported value size " + std::to_string(value_size);
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto compare_type = NativeIntegerCompareJumpType(SljitNativeIntegerKind::INT64, guard_compare_op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	struct sljit_jump *guard_is_true = nullptr;
	auto done = EmitSljitInvalidResultLoop(compiler, [&](vector<sljit_jump *> &invalid_jumps, vector<sljit_jump *> &) {
		EmitLoadLogicalIndex(compiler, SLJIT_R1);
		EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel), SLJIT_R1, SLJIT_S3);
		EmitLoadSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel), SLJIT_R1, SLJIT_S4);

		auto guard_is_null =
		    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, right_source_validity), SLJIT_S4);
		EmitLoadSljitNativeFixedWidthValue(compiler, offsetof(SljitNativeVectorInput, right_source_data), SLJIT_S4,
		                                   SLJIT_MOV, 3, SLJIT_R2);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, constant));
		if (guard_constant_on_left) {
			guard_is_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			guard_is_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
		}

		auto copy_label = sljit_emit_label(compiler);
		sljit_set_label(guard_is_null, copy_label);
		invalid_jumps.push_back(
		    EmitJumpIfValidityNull(compiler, offsetof(SljitNativeVectorInput, source_validity), SLJIT_S3));
		EmitCopySljitNativeFixedWidthSourceToResult(compiler, value_size, SLJIT_S3);
	});

	auto error_label = sljit_emit_label(compiler);
	sljit_set_label(guard_is_true, error_label);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(SljitNativeRuntimeError));
	auto helper_error = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	sljit_set_label(helper_error, done_label);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
