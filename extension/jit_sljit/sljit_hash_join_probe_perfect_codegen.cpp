#include "sljit_join_probe_codegen.hpp"

#include "sljit_codegen_util.hpp"
#include "sljit_hash_join_probe_codegen_validation.hpp"
#include "sljit_hash_join_probe_key_codegen.hpp"
#include "sljit_hash_join_probe_perfect_input_codegen.hpp"
#include "sljit_hash_join_runtime.hpp"
#include "sljit_register_layout.hpp"

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

static void EmitLoadHoistedPerfectHashJoinSourceIndex(struct sljit_compiler *compiler, sljit_s32 source_sel_reg,
                                                      sljit_s32 target) {
	auto no_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, source_sel_reg, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(source_sel_reg, SLJIT_S1), 2);
	auto have_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfHoistedPerfectHashJoinSourceNull(struct sljit_compiler *compiler,
                                                                     sljit_s32 source_validity_reg,
                                                                     sljit_s32 source_index, sljit_s32 scratch,
                                                                     sljit_s32 scratch2) {
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, source_validity_reg, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch2, 0, source_index, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch2, 0, SLJIT_MEM2(source_validity_reg, scratch2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, scratch, 0, source_index, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, SLJIT_IMM, 1, scratch, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, scratch, 0, scratch, 0, scratch2, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static sljit_sw SljitWidePerfectHashJoinBoundOffset(bool unsigned_key, bool maximum, sljit_sw word_offset) {
	if (unsigned_key) {
		return (maximum ? offsetof(SljitNativePerfectHashJoinProbeInput, perfect_max_u128)
		                : offsetof(SljitNativePerfectHashJoinProbeInput, perfect_min_u128)) +
		       word_offset;
	}
	return (maximum ? offsetof(SljitNativePerfectHashJoinProbeInput, perfect_max_128)
	                : offsetof(SljitNativePerfectHashJoinProbeInput, perfect_min_128)) +
	       word_offset;
}

static void EmitLoadWidePerfectHashJoinInvariantBounds(struct sljit_compiler *compiler, sljit_s32 min_lower,
                                                       sljit_s32 min_upper, sljit_s32 max_lower, bool unsigned_key) {
	const auto lower_offset = NumericCast<sljit_sw>(offsetof(hugeint_t, lower));
	const auto upper_offset = NumericCast<sljit_sw>(offsetof(hugeint_t, upper));
	sljit_emit_op1(compiler, SLJIT_MOV, min_lower, 0, SLJIT_MEM1(SLJIT_S0),
	               SljitWidePerfectHashJoinBoundOffset(unsigned_key, false, lower_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, min_upper, 0, SLJIT_MEM1(SLJIT_S0),
	               SljitWidePerfectHashJoinBoundOffset(unsigned_key, false, upper_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, max_lower, 0, SLJIT_MEM1(SLJIT_S0),
	               SljitWidePerfectHashJoinBoundOffset(unsigned_key, true, lower_offset));
}

static void EmitLoadWidePerfectHashJoinKey(struct sljit_compiler *compiler, sljit_s32 source_data,
                                           sljit_s32 source_index, sljit_s32 lower, sljit_s32 upper,
                                           sljit_s32 scratch) {
	EmitLoadHashJoinKeyWord(compiler, lower, source_data, source_index, offsetof(hugeint_t, lower), scratch);
	EmitLoadHashJoinKeyWord(compiler, upper, source_data, source_index, offsetof(hugeint_t, upper), scratch);
}

static void EmitWidePerfectHashJoinRangeChecks(struct sljit_compiler *compiler, bool unsigned_key, sljit_s32 key_lower,
                                               sljit_s32 key_upper, sljit_s32 min_lower, sljit_s32 min_upper,
                                               sljit_s32 max_lower, sljit_s32 max_upper,
                                               vector<struct sljit_jump *> &range_failures) {
	const auto less_than = unsigned_key ? SLJIT_LESS : SLJIT_SIG_LESS;
	const auto greater_than = unsigned_key ? SLJIT_GREATER : SLJIT_SIG_GREATER;

	// Compare the high word first. The low word is only relevant when the high
	// words are equal, preserving full signed/unsigned 128-bit ordering without
	// routing every probe through a C++ helper.
	auto upper_below_min = sljit_emit_cmp(compiler, less_than, key_upper, 0, min_upper, 0);
	auto upper_equal_min = sljit_emit_cmp(compiler, SLJIT_EQUAL, key_upper, 0, min_upper, 0);
	auto after_min_low = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto min_low_check = sljit_emit_label(compiler);
	auto lower_below_min = sljit_emit_cmp(compiler, SLJIT_LESS, key_lower, 0, min_lower, 0);
	auto after_min = sljit_emit_label(compiler);
	sljit_set_label(upper_equal_min, min_low_check);
	sljit_set_label(after_min_low, after_min);

	auto upper_above_max = sljit_emit_cmp(compiler, greater_than, key_upper, 0, max_upper, 0);
	auto upper_equal_max = sljit_emit_cmp(compiler, SLJIT_EQUAL, key_upper, 0, max_upper, 0);
	auto after_max_low = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto max_low_check = sljit_emit_label(compiler);
	auto lower_above_max = sljit_emit_cmp(compiler, SLJIT_GREATER, key_lower, 0, max_lower, 0);
	auto after_max = sljit_emit_label(compiler);
	sljit_set_label(upper_equal_max, max_low_check);
	sljit_set_label(after_max_low, after_max);

	range_failures.push_back(upper_below_min);
	range_failures.push_back(lower_below_min);
	range_failures.push_back(upper_above_max);
	range_failures.push_back(lower_above_max);
}

static unique_ptr<ExecutionRegionCodeHandle>
BuildSljitWidePerfectHashJoinProbe(struct sljit_compiler *compiler, bool unsigned_key,
                                   const SljitPerfectHashJoinProbeCodegenConfig &config,
                                   SljitNativePerfectHashJoinProbeFunction &function, string &error) {
	const auto &registers = GetSljitPerfectHashProbeRegisterLayout();
	D_ASSERT(registers.has_invariant_hoists);
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, registers.saved_register_count, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, registers.source_data, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, registers.source_selection, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, source_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_P, registers.source_validity, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, source_validity));
	EmitLoadWidePerfectHashJoinInvariantBounds(compiler, registers.minimum_lower, registers.minimum_upper,
	                                           registers.maximum_lower, unsigned_key);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadHoistedPerfectHashJoinSourceIndex(compiler, registers.source_selection, SLJIT_R2);
	auto source_is_null =
	    EmitJumpIfHoistedPerfectHashJoinSourceNull(compiler, registers.source_validity, SLJIT_R2, SLJIT_R3, SLJIT_R1);
	EmitLoadWidePerfectHashJoinKey(compiler, registers.source_data, SLJIT_R2, SLJIT_R0, SLJIT_R1, SLJIT_R3);
	// The target has only three saved registers after the loop state and source
	// metadata. Keep the fourth bound out of scratch-register lifetime: R4 is
	// reused below for bitmap indexing and must be reloaded before each compare.
	sljit_emit_op1(
	    compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	    SljitWidePerfectHashJoinBoundOffset(unsigned_key, true, NumericCast<sljit_sw>(offsetof(hugeint_t, upper))));
	vector<struct sljit_jump *> range_failures;
	EmitWidePerfectHashJoinRangeChecks(compiler, unsigned_key, SLJIT_R0, SLJIT_R1, registers.minimum_lower,
	                                   registers.minimum_upper, registers.maximum_lower, SLJIT_R4, range_failures);

	// Subtract the full-width minimum with the carry flag as the low-word
	// borrow. The perfect-hash range is bounded to the selection-vector domain,
	// so the resulting high word is zero and the low word is the table offset.
	sljit_emit_op2(compiler, SLJIT_SUB | SLJIT_SET_CARRY, SLJIT_R0, 0, SLJIT_R0, 0, registers.minimum_lower, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_CARRY);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R1, 0, SLJIT_R1, 0, registers.minimum_upper, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R3, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_validity));
	auto all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(SLJIT_R3, SLJIT_R4), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R3, 0, SLJIT_R0, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R3, 0, SLJIT_IMM, 1, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	auto value_missing = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(all_valid, sljit_emit_label(compiler));

	if (config.emit_build_selection) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, build_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R2, SLJIT_S3), 2, SLJIT_R0, 0);
	}
	if (config.emit_match_selection) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, match_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R2, SLJIT_S3), 2, SLJIT_S1, 0);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);

	auto skip_row = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, skip_row);
	for (auto failure : range_failures) {
		sljit_set_label(failure, skip_row);
	}
	sljit_set_label(value_missing, skip_row);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	return FinishSljitPerfectHashJoinProbeCode(compiler, function, error, SLJIT_S3);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitPerfectHashJoinProbe(const SljitNativeHashJoinProbePlan &plan,
                               SljitNativePerfectHashJoinProbeFunction &function, string &error,
                               const SljitPerfectHashJoinProbeCodegenConfig &config) {
	if (!SljitValidatePerfectHashJoinProbePlan(plan, error)) {
		return nullptr;
	}
	auto &key = plan.keys[0];
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	if (SljitHashJoinKeyKindIs128(key.key_kind)) {
		// Wide perfect-hash keys retain their full bounds in the runtime ABI. Use
		// the generated word-pair loop when the target exposes the saved-register
		// file needed to keep the bounds hot; the helper remains the portable
		// fallback for smaller register files.
		if (GetSljitPerfectHashProbeRegisterLayout().has_invariant_hoists) {
			return BuildSljitWidePerfectHashJoinProbe(compiler, key.key_kind == SljitNativeHashJoinKeyKind::UINT128,
			                                          config, function, error);
		}
		sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 4, 1, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, config.emit_match_selection ? 1 : 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, config.emit_build_selection ? 1 : 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM,
		               key.key_kind == SljitNativeHashJoinKeyKind::UINT128 ? 1 : 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS4V(P, W, W, W), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitPopulateWidePerfectHashJoinSelections));
		sljit_emit_return_void(compiler);
		return FinishSljitCode(compiler, function, error);
	}

	const auto signed_compare = SljitHashJoinKeyKindIsSigned(key.key_kind);
	const auto less_than_min = signed_compare ? SLJIT_SIG_LESS : SLJIT_LESS;
	const auto greater_than_max = signed_compare ? SLJIT_SIG_GREATER : SLJIT_GREATER;
	const auto &registers = GetSljitPerfectHashProbeRegisterLayout();

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, registers.saved_register_count, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePerfectHashJoinProbeInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, registers.source_data, 0, SLJIT_R0, 0);
	if (registers.has_invariant_hoists) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.source_selection, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, source_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.source_validity, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, source_validity));
		sljit_emit_op1(compiler, SLJIT_MOV, registers.minimum, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_min));
		sljit_emit_op1(compiler, SLJIT_MOV, registers.maximum, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_max));
		sljit_emit_op1(compiler, SLJIT_MOV_P, registers.perfect_validity, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_validity));
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	struct sljit_jump *source_is_null;
	if (registers.has_invariant_hoists) {
		EmitLoadHoistedPerfectHashJoinSourceIndex(compiler, registers.source_selection, SLJIT_R1);
		source_is_null = EmitJumpIfHoistedPerfectHashJoinSourceNull(compiler, registers.source_validity, SLJIT_R1,
		                                                            SLJIT_R2, SLJIT_R4);
	} else {
		EmitLoadPerfectHashJoinSourceIndex(compiler, SLJIT_R1, SLJIT_R0);
		source_is_null = EmitJumpIfPerfectHashJoinSourceNull(compiler, SLJIT_R1, SLJIT_R2, SLJIT_R4);
	}
	if (key.key_kind == SljitNativeHashJoinKeyKind::INT32) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, source_key0_int64_to_int32));
		auto load_int32 = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(registers.source_data, SLJIT_R1), 3);
		EmitCheckedPerfectHashJoinInt64ToInt32Range(compiler, SLJIT_R0, SLJIT_R2);
		auto loaded_key = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(load_int32, sljit_emit_label(compiler));
		EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R0, registers.source_data, SLJIT_R1, 0);
		sljit_set_label(loaded_key, sljit_emit_label(compiler));
	} else {
		EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R0, registers.source_data, SLJIT_R1, 0);
	}

	struct sljit_jump *below_range;
	struct sljit_jump *above_range;
	struct sljit_jump *all_valid;
	if (registers.has_invariant_hoists) {
		below_range = sljit_emit_cmp(compiler, less_than_min, SLJIT_R0, 0, registers.minimum, 0);
		above_range = sljit_emit_cmp(compiler, greater_than_max, SLJIT_R0, 0, registers.maximum, 0);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R0, 0, SLJIT_R0, 0, registers.minimum, 0);

		all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, registers.perfect_validity, 0, SLJIT_IMM, 0);
		sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R3, 0, SLJIT_R0, 0, SLJIT_IMM, 6);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(registers.perfect_validity, SLJIT_R3), 3);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_min));
		below_range = sljit_emit_cmp(compiler, less_than_min, SLJIT_R0, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_max));
		above_range = sljit_emit_cmp(compiler, greater_than_max, SLJIT_R0, 0, SLJIT_R3, 0);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R2, 0);

		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, perfect_validity));
		all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R3, 0, SLJIT_R0, 0, SLJIT_IMM, 6);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R2, SLJIT_R3), 3);
	}
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto value_missing = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(all_valid, sljit_emit_label(compiler));

	if (config.emit_build_selection) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, build_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R2, SLJIT_S3), 2, SLJIT_R0, 0);
	}
	if (config.emit_match_selection) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePerfectHashJoinProbeInput, match_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R2, SLJIT_S3), 2, SLJIT_S1, 0);
	}
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
