#include "sljit_join_probe_codegen.hpp"

#include "sljit_codegen_util.hpp"
#include "sljit_hash_join_probe_codegen_validation.hpp"
#include "sljit_hash_join_probe_key_codegen.hpp"
#include "sljit_hash_join_probe_perfect_input_codegen.hpp"

#include "sljitLir.h"

#include "duckdb/common/numeric_utils.hpp"

#include <cstddef>

namespace duckdb {

static void EmitPausePerfectHashJoinProbe(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, input_offset), SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, finished), SLJIT_IMM, 0);
}

static void EmitFinishPerfectHashJoinProbe(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, input_offset), SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, finished), SLJIT_IMM, 1);
}

static unique_ptr<ExecutionRegionCodeHandle>
FinishSljitPerfectHashJoinProbeCode(struct sljit_compiler *compiler, SljitNativePerfectHashJoinProbeFunction &function,
                                    string &error, sljit_s32 selected_count_src) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, selected_count), selected_count_src, 0);
	EmitFinishPerfectHashJoinProbe(compiler);
	sljit_emit_return_void(compiler);
	return FinishSljitCode(compiler, function, error);
}

static void EmitAbortPerfectHashJoinProbeWithCastError(struct sljit_compiler *compiler, sljit_s32 value_reg) {
	EmitCallHashJoinInt64ToInt32CastError(compiler, offsetof(SljitNativePerfectHashJoinProbeInput, error), value_reg);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, selected_count), SLJIT_S3, 0);
	EmitPausePerfectHashJoinProbe(compiler);
	sljit_emit_return_void(compiler);
}

static void EmitCheckedPerfectHashJoinInt64ToInt32Range(struct sljit_compiler *compiler, sljit_s32 value_reg,
                                                        sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, source_key0_int64_to_int32_unchecked));
	auto unchecked = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	auto below_range = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, value_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(NumericLimits<int32_t>::Minimum()));
	auto above_range = sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, value_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(NumericLimits<int32_t>::Maximum()));
	auto in_range = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto range_error = sljit_emit_label(compiler);
	sljit_set_label(below_range, range_error);
	sljit_set_label(above_range, range_error);
	EmitAbortPerfectHashJoinProbeWithCastError(compiler, value_reg);

	auto done = sljit_emit_label(compiler);
	sljit_set_label(unchecked, done);
	sljit_set_label(in_range, done);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitPerfectHashJoinProbe(const SljitNativeHashJoinProbePlan &plan,
                                                                     SljitNativePerfectHashJoinProbeFunction &function,
                                                                     string &error) {
	if (!SljitValidatePerfectHashJoinProbePlan(plan, error)) {
		return nullptr;
	}
	auto &key = plan.keys[0];
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto signed_compare = SljitHashJoinKeyKindIsSigned(key.key_kind);
	const auto less_than_min = signed_compare ? SLJIT_SIG_LESS : SLJIT_LESS;
	const auto greater_than_max = signed_compare ? SLJIT_SIG_GREATER : SLJIT_GREATER;

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_R0, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadPerfectHashJoinSourceIndex(compiler, SLJIT_R1, SLJIT_R0);
	auto source_is_null = EmitJumpIfPerfectHashJoinSourceNull(compiler, SLJIT_R1, SLJIT_R2, SLJIT_R4);
	if (key.key_kind == SljitNativeHashJoinKeyKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, source_key0_int64_to_int32));
		auto load_int32 = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_S4, SLJIT_R1), 3);
		EmitCheckedPerfectHashJoinInt64ToInt32Range(compiler, SLJIT_R0, SLJIT_R2);
		auto loaded_key = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(load_int32, sljit_emit_label(compiler));
		EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R0, SLJIT_S4, SLJIT_R1, 0);
		sljit_set_label(loaded_key, sljit_emit_label(compiler));
	} else {
		EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R0, SLJIT_S4, SLJIT_R1, 0);
	}

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_min));
	auto below_range = sljit_emit_cmp(compiler, less_than_min, SLJIT_R0, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_max));
	auto above_range = sljit_emit_cmp(compiler, greater_than_max, SLJIT_R0, 0, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R2, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_validity));
	auto all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R3, 0, SLJIT_R0, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R2, SLJIT_R3), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto value_missing = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(all_valid, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, build_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R2, SLJIT_S3), 2, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, match_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R2, SLJIT_S3), 2, SLJIT_S1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);

	auto skip_row = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, skip_row);
	sljit_set_label(below_range, skip_row);
	sljit_set_label(above_range, skip_row);
	sljit_set_label(value_missing, skip_row);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	return FinishSljitPerfectHashJoinProbeCode(compiler, function, error, SLJIT_S3);
}

} // namespace duckdb
