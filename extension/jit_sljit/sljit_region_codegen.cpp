#include "sljit_region_codegen.hpp"

#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "sljitLir.h"

#include <cstddef>
#include <exception>

namespace duckdb {

static void SLJIT_FUNC SljitFusedFilterProjectionOverflow(SljitFusedFilterProjectionInput *input) {
	try {
		throw OutOfRangeException("%s", input->overflow_message);
	} catch (...) {
		input->error = std::current_exception();
	}
}

static unique_ptr<JitCodeHandle> FinishSljitFusedFilterProjectionCode(
    struct sljit_compiler *compiler, SljitFusedFilterProjectionFunction &function, string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitFusedFilterProjectionFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static unique_ptr<JitCodeHandle> FinishSljitUngroupedAggregateUpdateCode(
    struct sljit_compiler *compiler, SljitNativeUngroupedAggregateFunction &function, string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativeUngroupedAggregateFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static unique_ptr<JitCodeHandle> FinishSljitGroupedAggregateUpdateCode(
    struct sljit_compiler *compiler, SljitNativeGroupedAggregateFunction &function, string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativeGroupedAggregateFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static unique_ptr<JitCodeHandle> FinishSljitHashJoinProbeCode(struct sljit_compiler *compiler,
                                                              SljitNativeHashJoinProbeFunction &function,
                                                              string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativeHashJoinProbeFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static unique_ptr<JitCodeHandle> FinishSljitFusedUngroupedAggregateCode(
    struct sljit_compiler *compiler, SljitFusedUngroupedAggregateFunction &function, string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitFusedUngroupedAggregateFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static void SLJIT_FUNC SljitFusedUngroupedAggregateOverflow(SljitFusedUngroupedAggregateInput *input) {
	try {
		throw OutOfRangeException("%s", input->overflow_message);
	} catch (...) {
		input->error = std::current_exception();
	}
}

static void EmitAddUngroupedStateCount(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, state_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, count));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
}

static void EmitLoadUngroupedStateValueAddress(struct sljit_compiler *compiler, sljit_s32 target,
                                               sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, state));
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, scratch, 0);
}

static void EmitSetUngroupedStateIsSet(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, state));
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, state_is_set_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, scratch, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(target), 0, SLJIT_IMM, 1);
}

static void EmitLoadUngroupedSourceIndex(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, source_sel));
	auto no_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfUngroupedSourceNull(struct sljit_compiler *compiler, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, source_validity));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static void EmitAddUngroupedInt64SourceValue(struct sljit_compiler *compiler, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_IMM, 1);
}

static void EmitAddHugeintInt64ToStateValue(struct sljit_compiler *compiler, sljit_s32 state_addr_reg,
                                            sljit_s32 value_reg, sljit_s32 lower_reg, sljit_s32 upper_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, lower_reg, 0, SLJIT_MEM1(state_addr_reg), offsetof(hugeint_t, lower));
	sljit_emit_op2(compiler, SLJIT_ADD, lower_reg, 0, lower_reg, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(state_addr_reg), offsetof(hugeint_t, lower), lower_reg, 0);

	auto negative_value = sljit_emit_cmp(compiler, SLJIT_SIG_LESS, value_reg, 0, SLJIT_IMM, 0);

	auto positive_no_overflow = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, lower_reg, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, upper_reg, 0, SLJIT_MEM1(state_addr_reg), offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_ADD, upper_reg, 0, upper_reg, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(state_addr_reg), offsetof(hugeint_t, upper), upper_reg, 0);
	auto adjusted_positive = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto negative_label = sljit_emit_label(compiler);
	sljit_set_label(negative_value, negative_label);
	auto negative_no_adjust = sljit_emit_cmp(compiler, SLJIT_LESS, lower_reg, 0, value_reg, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, upper_reg, 0, SLJIT_MEM1(state_addr_reg), offsetof(hugeint_t, upper));
	sljit_emit_op2(compiler, SLJIT_SUB, upper_reg, 0, upper_reg, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(state_addr_reg), offsetof(hugeint_t, upper), upper_reg, 0);
	auto adjusted_negative = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto done = sljit_emit_label(compiler);
	sljit_set_label(positive_no_overflow, done);
	sljit_set_label(adjusted_positive, done);
	sljit_set_label(negative_no_adjust, done);
	sljit_set_label(adjusted_negative, done);
}

static void EmitAddUngroupedHugeintInt64SourceValue(struct sljit_compiler *compiler, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
	EmitLoadUngroupedStateValueAddress(compiler, SLJIT_R0, SLJIT_R4);
	EmitAddHugeintInt64ToStateValue(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R3, SLJIT_R4);
	EmitSetUngroupedStateIsSet(compiler, SLJIT_R0, SLJIT_R3);
}

static void EmitLoadGroupedStateValueAddress(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM2(target, SLJIT_S1), 3);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, scratch, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, scratch, 0);
}

static void EmitSetGroupedStateIsSet(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, state_addresses));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM2(target, SLJIT_S1), 3);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, aggregate_state_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, scratch, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, state_is_set_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, scratch, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(target), 0, SLJIT_IMM, 1);
}

static void EmitLoadGroupedSourceIndex(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, source_sel));
	auto no_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfGroupedSourceNull(struct sljit_compiler *compiler, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, source_validity));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static void EmitAddGroupedInt64SourceValue(struct sljit_compiler *compiler, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
	EmitLoadGroupedStateValueAddress(compiler, SLJIT_R0, SLJIT_R4);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R3, 0);
	EmitSetGroupedStateIsSet(compiler, SLJIT_R0, SLJIT_R3);
}

static void EmitAddGroupedHugeintInt64SourceValue(struct sljit_compiler *compiler, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
	EmitLoadGroupedStateValueAddress(compiler, SLJIT_R0, SLJIT_R4);
	EmitAddHugeintInt64ToStateValue(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R3, SLJIT_R4);
	EmitSetGroupedStateIsSet(compiler, SLJIT_R0, SLJIT_R3);
}

static sljit_s32 SljitHashJoinKeyLoadOp(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
		return SLJIT_MOV_S8;
	case SljitNativeHashJoinKeyKind::INT16:
		return SLJIT_MOV_S16;
	case SljitNativeHashJoinKeyKind::INT32:
		return SLJIT_MOV_S32;
	case SljitNativeHashJoinKeyKind::INT64:
	case SljitNativeHashJoinKeyKind::UINT64:
		return SLJIT_MOV;
	case SljitNativeHashJoinKeyKind::UINT8:
		return SLJIT_MOV_U8;
	case SljitNativeHashJoinKeyKind::UINT16:
		return SLJIT_MOV_U16;
	case SljitNativeHashJoinKeyKind::UINT32:
		return SLJIT_MOV_U32;
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
}

static sljit_sw SljitHashJoinKeyScale(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
	case SljitNativeHashJoinKeyKind::UINT8:
		return 0;
	case SljitNativeHashJoinKeyKind::INT16:
	case SljitNativeHashJoinKeyKind::UINT16:
		return 1;
	case SljitNativeHashJoinKeyKind::INT32:
	case SljitNativeHashJoinKeyKind::UINT32:
		return 2;
	case SljitNativeHashJoinKeyKind::INT64:
	case SljitNativeHashJoinKeyKind::UINT64:
		return 3;
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
}

static bool SljitHashJoinKeyHashesAsUInt32(SljitNativeHashJoinKeyKind kind) {
	return kind != SljitNativeHashJoinKeyKind::INT64 && kind != SljitNativeHashJoinKeyKind::UINT64;
}

static bool SljitHashJoinKeyKindIsSigned(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
	case SljitNativeHashJoinKeyKind::INT16:
	case SljitNativeHashJoinKeyKind::INT32:
	case SljitNativeHashJoinKeyKind::INT64:
		return true;
	default:
		return false;
	}
}

static sljit_s32 SljitHashJoinPredicateMismatchComparison(ExpressionType comparison_type,
                                                          SljitNativeHashJoinKeyKind key_kind) {
	const bool signed_compare = SljitHashJoinKeyKindIsSigned(key_kind);
	switch (comparison_type) {
	case ExpressionType::COMPARE_NOTEQUAL:
		return SLJIT_EQUAL;
	case ExpressionType::COMPARE_LESSTHAN:
		return signed_compare ? SLJIT_SIG_GREATER_EQUAL : SLJIT_GREATER_EQUAL;
	case ExpressionType::COMPARE_GREATERTHAN:
		return signed_compare ? SLJIT_SIG_LESS_EQUAL : SLJIT_LESS_EQUAL;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return signed_compare ? SLJIT_SIG_GREATER : SLJIT_GREATER;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return signed_compare ? SLJIT_SIG_LESS : SLJIT_LESS;
	default:
		throw InternalException("Unknown SLJIT hash join match predicate");
	}
}

static sljit_sw DuckDBMurmurHashMultiplierImmediate() {
	return static_cast<sljit_sw>(0xd6e8feb86659fd93ULL);
}

static sljit_sw DuckDBNullHashImmediate() {
	return static_cast<sljit_sw>(0xbf58476d1ce4e5b9ULL);
}

static void EmitDuckDBMurmurHash64(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_IMM, DuckDBMurmurHashMultiplierImmediate());
	sljit_emit_op2(compiler, SLJIT_MUL, target, 0, target, 0, scratch, 0);

	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_IMM, DuckDBMurmurHashMultiplierImmediate());
	sljit_emit_op2(compiler, SLJIT_MUL, target, 0, target, 0, scratch, 0);

	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
}

static void EmitLoadHashJoinSourceIndex(struct sljit_compiler *compiler, idx_t key_idx, sljit_s32 target,
                                        sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, source_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(scratch),
	               NumericCast<sljit_sw>(key_idx * sizeof(sel_t *)));
	auto no_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(scratch, SLJIT_S1), 2);
	auto have_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfHashJoinSourceNull(struct sljit_compiler *compiler, idx_t key_idx,
                                                       sljit_s32 source_index, sljit_s32 scratch,
                                                       sljit_s32 scratch2) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, source_validity));
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(scratch),
	               NumericCast<sljit_sw>(key_idx * sizeof(validity_t *)));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch2, 0, source_index, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch2, 0, SLJIT_MEM2(scratch, scratch2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, scratch, 0, source_index, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, SLJIT_IMM, 1, scratch, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, scratch, 0, scratch, 0, scratch2, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static struct sljit_jump *EmitJumpIfHashJoinRhsKeyNull(struct sljit_compiler *compiler, idx_t key_idx,
                                                       sljit_s32 row_pointer, sljit_s32 scratch,
                                                       sljit_s32 scratch2) {
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, rhs_keys_have_validity));
	auto rhs_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(row_pointer),
	               NumericCast<sljit_sw>(key_idx / 8));
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, scratch2, 0, scratch, 0,
	               SLJIT_IMM, 1ULL << (key_idx % 8));
	auto rhs_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(rhs_all_valid, sljit_emit_label(compiler));
	return rhs_is_null;
}

static void EmitLoadHashJoinSourceData(struct sljit_compiler *compiler, idx_t key_idx, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(target),
	               NumericCast<sljit_sw>(key_idx * sizeof(const_data_ptr_t)));
}

static void EmitLoadHashJoinKey(struct sljit_compiler *compiler, SljitNativeHashJoinKeyKind kind, sljit_s32 target,
                                sljit_s32 base, sljit_s32 index, sljit_sw offset) {
	auto load_op = SljitHashJoinKeyLoadOp(kind);
	if (index == SLJIT_IMM) {
		sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM1(base), offset);
		return;
	}
	if (offset == 0) {
		sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(base, index), SljitHashJoinKeyScale(kind));
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, index, 0, SLJIT_IMM, SljitHashJoinKeyScale(kind));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, offset);
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(base, SLJIT_R4), 0);
}

static void EmitHashJoinKeyHash(struct sljit_compiler *compiler, SljitNativeHashJoinKeyKind key_kind,
                                sljit_s32 hash_reg, sljit_s32 scratch) {
	if (SljitHashJoinKeyHashesAsUInt32(key_kind)) {
		sljit_emit_op2(compiler, SLJIT_AND, hash_reg, 0, hash_reg, 0, SLJIT_IMM, 0xffffffffULL);
	}
	EmitDuckDBMurmurHash64(compiler, hash_reg, scratch);
}

static void EmitDuckDBCombineHashScalar(struct sljit_compiler *compiler, sljit_s32 current_hash,
                                        sljit_s32 other_hash, sljit_s32 scratch) {
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, current_hash, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, current_hash, 0, current_hash, 0, scratch, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_IMM, DuckDBMurmurHashMultiplierImmediate());
	sljit_emit_op2(compiler, SLJIT_MUL, current_hash, 0, current_hash, 0, scratch, 0);
	sljit_emit_op2(compiler, SLJIT_XOR, current_hash, 0, current_hash, 0, other_hash, 0);
}

static constexpr sljit_sw SLJIT_HASH_JOIN_PROBE_OFFSET_LOCAL = 0;
static constexpr sljit_s32 SLJIT_HASH_JOIN_PROBE_LOCAL_SIZE = static_cast<sljit_s32>(sizeof(sljit_sw));

static void EmitSaveHashJoinProbeOffset(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_HASH_JOIN_PROBE_OFFSET_LOCAL, SLJIT_R1, 0);
}

static void EmitRestoreHashJoinProbeOffset(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_HASH_JOIN_PROBE_OFFSET_LOCAL);
}

static void EmitStoreHashJoinMarkProbeFlag(struct sljit_compiler *compiler, sljit_sw value) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, match_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R4, SLJIT_S1), 2, SLJIT_IMM, value);
}

static void EmitFinishHashJoinProbe(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, input_offset), SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, resume_row_pointer), SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, finished), SLJIT_IMM, 1);
}

static void EmitMarkHashJoinBuildChain(struct sljit_compiler *compiler, sljit_s32 row_pointer, sljit_s32 scratch,
                                       sljit_s32 next_pointer, idx_t found_match_offset, idx_t pointer_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(row_pointer),
	               NumericCast<sljit_sw>(found_match_offset));
	auto already_marked = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, scratch, 0, SLJIT_IMM, 0);

	auto mark_loop = sljit_emit_label(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(row_pointer), NumericCast<sljit_sw>(found_match_offset),
	               SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, chains_longer_than_one));
	auto chain_done = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, dictionary_emission));
	auto direct_pointer = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_U32, scratch, 0, SLJIT_MEM1(row_pointer),
	               NumericCast<sljit_sw>(pointer_offset));
	sljit_emit_op1(compiler, SLJIT_MOV_P, next_pointer, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, aux_next_ptrs));
	sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM2(next_pointer, scratch), 3);
	auto next_loaded = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(direct_pointer, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM1(row_pointer),
	               NumericCast<sljit_sw>(pointer_offset));

	sljit_set_label(next_loaded, sljit_emit_label(compiler));
	auto no_next = sljit_emit_cmp(compiler, SLJIT_EQUAL, row_pointer, 0, SLJIT_IMM, 0);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, mark_loop);

	auto done = sljit_emit_label(compiler);
	sljit_set_label(already_marked, done);
	sljit_set_label(chain_done, done);
	sljit_set_label(no_next, done);
}

unique_ptr<JitCodeHandle> BuildSljitHashJoinProbe(const vector<SljitNativeHashJoinProbeKeyPlan> &keys,
                                                   idx_t equality_key_count, bool mark_build_match,
                                                   idx_t found_match_offset, idx_t pointer_offset,
                                                   JitRegionHashJoinProbeOutputMode output_mode,
                                                   SljitNativeHashJoinProbeFunction &function, string &error) {
	if (keys.empty()) {
		error = "SLJIT hash join probe requires at least one key";
		return nullptr;
	}
	if (equality_key_count == 0 || equality_key_count > keys.size()) {
		error = "SLJIT hash join probe requires an equality-key prefix";
		return nullptr;
	}
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		if ((key_idx < equality_key_count) != keys[key_idx].equality_key) {
			error = "SLJIT hash join probe key plan is not an equality-key prefix";
			return nullptr;
		}
	}
	if (output_mode == JitRegionHashJoinProbeOutputMode::NONE) {
		error = "SLJIT hash join probe requires an output mode";
		return nullptr;
	}
	const bool mark_build_only = output_mode == JitRegionHashJoinProbeOutputMode::MARK_BUILD_ONLY;
	const bool mark_probe = output_mode == JitRegionHashJoinProbeOutputMode::MARK_PROBE;
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, SLJIT_HASH_JOIN_PROBE_LOCAL_SIZE);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, entries));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	if (mark_probe) {
		EmitStoreHashJoinMarkProbeFlag(compiler, 0);
	}
	vector<struct sljit_jump *> source_is_null;
	for (idx_t key_idx = 0; key_idx < equality_key_count; key_idx++) {
		auto &key = keys[key_idx];
		EmitLoadHashJoinSourceIndex(compiler, key_idx, SLJIT_R1, SLJIT_R0);
		auto source_null = EmitJumpIfHashJoinSourceNull(compiler, key_idx, SLJIT_R1, SLJIT_R0, SLJIT_R4);
		if (!key.null_equal) {
			source_is_null.push_back(source_null);
			EmitLoadHashJoinSourceData(compiler, key_idx, SLJIT_R0);
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R2, SLJIT_R0, SLJIT_R1, 0);
			EmitHashJoinKeyHash(compiler, key.key_kind, SLJIT_R2, SLJIT_R4);
		} else {
			EmitLoadHashJoinSourceData(compiler, key_idx, SLJIT_R0);
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R2, SLJIT_R0, SLJIT_R1, 0);
			EmitHashJoinKeyHash(compiler, key.key_kind, SLJIT_R2, SLJIT_R4);
			auto source_hash_ready = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(source_null, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, DuckDBNullHashImmediate());
			sljit_set_label(source_hash_ready, sljit_emit_label(compiler));
		}
		if (key_idx == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			EmitDuckDBCombineHashScalar(compiler, SLJIT_R3, SLJIT_R2, SLJIT_R4);
		}
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_R3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, pointer_mask));
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, bitmask));
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);

	auto probe_loop = sljit_emit_label(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_S4, SLJIT_R1), 3);
	auto empty_slot = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, use_salt));
	auto skip_salt = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, pointer_mask));
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	auto salt_mismatch = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R3, 0);
	sljit_set_label(skip_salt, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, pointer_mask));
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	EmitSaveHashJoinProbeOffset(compiler);
	vector<struct sljit_jump *> key_mismatches;
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		auto &key = keys[key_idx];
		EmitLoadHashJoinSourceIndex(compiler, key_idx, SLJIT_R1, SLJIT_R4);
		if (key_idx >= equality_key_count) {
			key_mismatches.push_back(EmitJumpIfHashJoinSourceNull(compiler, key_idx, SLJIT_R1, SLJIT_R2, SLJIT_R4));
			key_mismatches.push_back(EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4));
			EmitLoadHashJoinSourceData(compiler, key_idx, SLJIT_R4);
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R2, SLJIT_R4, SLJIT_R1, 0);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(key.key_layout_offset));
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R4, SLJIT_R4, SLJIT_IMM, 0);
			key_mismatches.push_back(sljit_emit_cmp(
			    compiler, SljitHashJoinPredicateMismatchComparison(key.comparison_type, key.key_kind),
			    SLJIT_R2, 0, SLJIT_R4, 0));
		} else if (key.null_equal) {
			auto source_null = EmitJumpIfHashJoinSourceNull(compiler, key_idx, SLJIT_R1, SLJIT_R2, SLJIT_R4);
			key_mismatches.push_back(EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4));
			EmitLoadHashJoinSourceData(compiler, key_idx, SLJIT_R4);
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R2, SLJIT_R4, SLJIT_R1, 0);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(key.key_layout_offset));
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R4, SLJIT_R4, SLJIT_IMM, 0);
			key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));
			auto key_done = sljit_emit_jump(compiler, SLJIT_JUMP);

			sljit_set_label(source_null, sljit_emit_label(compiler));
			auto rhs_null_match = EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4);
			key_mismatches.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			auto nulls_match = sljit_emit_label(compiler);
			sljit_set_label(rhs_null_match, nulls_match);
			sljit_set_label(key_done, nulls_match);
		} else {
			key_mismatches.push_back(EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4));
			EmitLoadHashJoinSourceData(compiler, key_idx, SLJIT_R4);
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R2, SLJIT_R4, SLJIT_R1, 0);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(key.key_layout_offset));
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R4, SLJIT_R4, SLJIT_IMM, 0);
			key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));
		}
	}
	if (mark_build_match) {
		if (mark_build_only) {
			EmitMarkHashJoinBuildChain(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, found_match_offset,
			                           pointer_offset);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), NumericCast<sljit_sw>(found_match_offset),
			               SLJIT_IMM, 1);
		}
	}
	if (mark_build_only) {
		auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto restore_hash_offset = sljit_emit_label(compiler);
		for (auto &key_mismatch : key_mismatches) {
			sljit_set_label(key_mismatch, restore_hash_offset);
		}
		EmitRestoreHashJoinProbeOffset(compiler);
		auto next_slot = sljit_emit_label(compiler);
		sljit_set_label(salt_mismatch, next_slot);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, bitmask));
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);
		auto repeat_probe = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_probe, probe_loop);

		auto skip_row = sljit_emit_label(compiler);
		sljit_set_label(empty_slot, skip_row);
		for (auto &null_jump : source_is_null) {
			sljit_set_label(null_jump, skip_row);
		}
		sljit_set_label(advance_row, skip_row);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto repeat_rows = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_rows, loop);

		sljit_set_label(done, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, selected_count), SLJIT_IMM, 0);
		EmitFinishHashJoinProbe(compiler);
		sljit_emit_return_void(compiler);
		return FinishSljitHashJoinProbeCode(compiler, function, error);
	}

	if (mark_probe) {
		EmitStoreHashJoinMarkProbeFlag(compiler, 1);
		auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);

		auto restore_hash_offset = sljit_emit_label(compiler);
		for (auto &key_mismatch : key_mismatches) {
			sljit_set_label(key_mismatch, restore_hash_offset);
		}
		EmitRestoreHashJoinProbeOffset(compiler);
		auto next_slot = sljit_emit_label(compiler);
		sljit_set_label(salt_mismatch, next_slot);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, bitmask));
		sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);
		auto repeat_probe = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_probe, probe_loop);

		auto skip_row = sljit_emit_label(compiler);
		sljit_set_label(empty_slot, skip_row);
		for (auto &null_jump : source_is_null) {
			sljit_set_label(null_jump, skip_row);
		}
		sljit_set_label(advance_row, skip_row);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto repeat_rows = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_rows, loop);

		sljit_set_label(done, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, selected_count), SLJIT_S2, 0);
		EmitFinishHashJoinProbe(compiler);
		sljit_emit_return_void(compiler);
		return FinishSljitHashJoinProbeCode(compiler, function, error);
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, row_pointers));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM2(SLJIT_R4, SLJIT_S3), 3, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, match_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R4, SLJIT_S3), 2, SLJIT_S1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto restore_hash_offset = sljit_emit_label(compiler);
	for (auto &key_mismatch : key_mismatches) {
		sljit_set_label(key_mismatch, restore_hash_offset);
	}
	EmitRestoreHashJoinProbeOffset(compiler);
	auto next_slot = sljit_emit_label(compiler);
	sljit_set_label(salt_mismatch, next_slot);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, bitmask));
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R4, 0);
	auto repeat_probe = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_probe, probe_loop);

	auto skip_row = sljit_emit_label(compiler);
	sljit_set_label(empty_slot, skip_row);
	for (auto &null_jump : source_is_null) {
		sljit_set_label(null_jump, skip_row);
	}
	sljit_set_label(advance_row, skip_row);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat_rows = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_rows, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, selected_count), SLJIT_S3, 0);
	EmitFinishHashJoinProbe(compiler);
	sljit_emit_return_void(compiler);
	return FinishSljitHashJoinProbeCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitUngroupedCountStarUpdate(SljitNativeUngroupedAggregateFunction &function,
                                                             string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 3, 3, 0);
	EmitAddUngroupedStateCount(compiler);
	EmitLoadUngroupedStateValueAddress(compiler, SLJIT_R0, SLJIT_R2);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, count));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_return_void(compiler);
	return FinishSljitUngroupedAggregateUpdateCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitGroupedCountStarUpdate(SljitNativeGroupedAggregateFunction &function,
                                                           string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 4, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadGroupedStateValueAddress(compiler, SLJIT_R0, SLJIT_R2);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);
	return FinishSljitGroupedAggregateUpdateCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitGroupedCountUpdate(SljitNativeGroupedAggregateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadGroupedSourceIndex(compiler, SLJIT_R1);
	auto source_is_null = EmitJumpIfGroupedSourceNull(compiler, SLJIT_R1);
	EmitLoadGroupedStateValueAddress(compiler, SLJIT_R0, SLJIT_R3);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R2, 0);
	auto next = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, next);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);
	return FinishSljitGroupedAggregateUpdateCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitGroupedSumInt64Update(SljitNativeGroupedAggregateFunction &function,
                                                          string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadGroupedSourceIndex(compiler, SLJIT_R1);
	auto source_is_null = EmitJumpIfGroupedSourceNull(compiler, SLJIT_R1);
	EmitAddGroupedInt64SourceValue(compiler, SLJIT_R1);
	auto next = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, next);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);
	return FinishSljitGroupedAggregateUpdateCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitGroupedSumHugeintInt64Update(SljitNativeGroupedAggregateFunction &function,
                                                                 string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeGroupedAggregateInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadGroupedSourceIndex(compiler, SLJIT_R1);
	auto source_is_null = EmitJumpIfGroupedSourceNull(compiler, SLJIT_R1);
	EmitAddGroupedHugeintInt64SourceValue(compiler, SLJIT_R1);
	auto next = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, next);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);
	return FinishSljitGroupedAggregateUpdateCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitUngroupedCountUpdate(SljitNativeUngroupedAggregateFunction &function,
                                                         string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitAddUngroupedStateCount(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, source_validity));
	auto all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadUngroupedSourceIndex(compiler, SLJIT_S4);
	auto source_is_null = EmitJumpIfUngroupedSourceNull(compiler, SLJIT_S4);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);
	auto next = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, next);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	auto all_valid_label = sljit_emit_label(compiler);
	sljit_set_label(all_valid, all_valid_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S2, 0);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	EmitLoadUngroupedStateValueAddress(compiler, SLJIT_R0, SLJIT_R2);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_return_void(compiler);
	return FinishSljitUngroupedAggregateUpdateCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitUngroupedSumInt64Update(SljitNativeUngroupedAggregateFunction &function,
                                                            string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	EmitAddUngroupedStateCount(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, source_validity));
	auto all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	auto nullable_loop = sljit_emit_label(compiler);
	auto nullable_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadUngroupedSourceIndex(compiler, SLJIT_R1);
	auto source_is_null = EmitJumpIfUngroupedSourceNull(compiler, SLJIT_R1);
	EmitAddUngroupedInt64SourceValue(compiler, SLJIT_R1);
	auto nullable_next = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, nullable_next);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto nullable_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(nullable_repeat, nullable_loop);

	auto all_valid_loop = sljit_emit_label(compiler);
	sljit_set_label(all_valid, all_valid_loop);
	auto all_valid_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadUngroupedSourceIndex(compiler, SLJIT_R1);
	EmitAddUngroupedInt64SourceValue(compiler, SLJIT_R1);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto all_valid_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(all_valid_repeat, all_valid_loop);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(nullable_done, done_label);
	sljit_set_label(all_valid_done, done_label);
	auto no_values = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitLoadUngroupedStateValueAddress(compiler, SLJIT_R0, SLJIT_R2);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S3, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	EmitSetUngroupedStateIsSet(compiler, SLJIT_R0, SLJIT_R2);
	auto return_label = sljit_emit_label(compiler);
	sljit_set_label(no_values, return_label);
	sljit_emit_return_void(compiler);
	return FinishSljitUngroupedAggregateUpdateCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitUngroupedSumHugeintInt64Update(SljitNativeUngroupedAggregateFunction &function,
                                                                   string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 3, 0);
	EmitAddUngroupedStateCount(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeUngroupedAggregateInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadUngroupedSourceIndex(compiler, SLJIT_R1);
	auto source_is_null = EmitJumpIfUngroupedSourceNull(compiler, SLJIT_R1);
	EmitAddUngroupedHugeintInt64SourceValue(compiler, SLJIT_R1);
	auto next = sljit_emit_label(compiler);
	sljit_set_label(source_is_null, next);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);
	return FinishSljitUngroupedAggregateUpdateCode(compiler, function, error);
}

struct SljitFusedAggregatePredicateBranches {
	vector<sljit_jump *> true_jumps;
	vector<sljit_jump *> false_jumps;
	vector<sljit_jump *> null_jumps;
};

static constexpr sljit_sw SLJIT_FUSED_AGG_SUM_OFFSET = 0;
static constexpr sljit_sw SLJIT_FUSED_AGG_FILTERED_COUNT_OFFSET = sizeof(sljit_sw);
static constexpr sljit_sw SLJIT_FUSED_AGG_VALID_COUNT_OFFSET = 2 * sizeof(sljit_sw);
static constexpr sljit_sw SLJIT_FUSED_AGG_SOURCE_DATA_BASE_OFFSET = 3 * sizeof(sljit_sw);

static sljit_sw SljitFusedAggregatePointerArrayOffset(idx_t index) {
	return NumericCast<sljit_sw>(index * sizeof(data_ptr_t));
}

static sljit_sw SljitFusedAggregateSourceDescriptorOffset(idx_t index) {
	return SLJIT_FUSED_AGG_SOURCE_DATA_BASE_OFFSET +
	       NumericCast<sljit_sw>(index * (sizeof(const_data_ptr_t) + sizeof(const sel_t *) +
	                                      sizeof(const validity_t *)));
}

static sljit_sw SljitFusedAggregateSourceDataOffset(idx_t index) {
	return SljitFusedAggregateSourceDescriptorOffset(index);
}

static sljit_sw SljitFusedAggregateSourceSelOffset(idx_t index) {
	return SljitFusedAggregateSourceDescriptorOffset(index) + NumericCast<sljit_sw>(sizeof(const_data_ptr_t));
}

static sljit_sw SljitFusedAggregateSourceValidityOffset(idx_t index) {
	return SljitFusedAggregateSourceSelOffset(index) + NumericCast<sljit_sw>(sizeof(const sel_t *));
}

static sljit_sw SljitFusedAggregateLocalSize(idx_t source_count) {
	return SljitFusedAggregateSourceDescriptorOffset(source_count);
}

static void AppendFusedAggregatePredicateJumps(vector<sljit_jump *> &target, vector<sljit_jump *> source) {
	target.reserve(target.size() + source.size());
	for (auto &jump : source) {
		target.push_back(jump);
	}
}

static void AppendFusedAggregatePredicateBranches(SljitFusedAggregatePredicateBranches &target,
                                                  SljitFusedAggregatePredicateBranches source) {
	AppendFusedAggregatePredicateJumps(target.true_jumps, std::move(source.true_jumps));
	AppendFusedAggregatePredicateJumps(target.false_jumps, std::move(source.false_jumps));
	AppendFusedAggregatePredicateJumps(target.null_jumps, std::move(source.null_jumps));
}

static void SetFusedAggregatePredicateJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label) {
	for (auto &jump : jumps) {
		sljit_set_label(jump, label);
	}
}

static void RegisterFusedAggregateSourceIndex(idx_t source_index, idx_t &source_count) {
	if (source_index >= source_count) {
		source_count = source_index + 1;
	}
}

static void RegisterFusedAggregatePredicateSources(const SljitNativePredicate &predicate, idx_t &source_count) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		return;
	case SljitNativePredicateKind::REFERENCE:
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativePredicateKind::INTEGER_IN_LIST:
	case SljitNativePredicateKind::INTEGER_BETWEEN:
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT:
	case SljitNativePredicateKind::NULL_CHECK:
		RegisterFusedAggregateSourceIndex(predicate.source_index, source_count);
		return;
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES:
		RegisterFusedAggregateSourceIndex(predicate.source_index, source_count);
		RegisterFusedAggregateSourceIndex(predicate.right_source_index, source_count);
		return;
	case SljitNativePredicateKind::NOT:
		RegisterFusedAggregatePredicateSources(*predicate.child, source_count);
		return;
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			RegisterFusedAggregatePredicateSources(*child, source_count);
		}
		return;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		for (auto source_index : predicate.guard_source_indices) {
			RegisterFusedAggregateSourceIndex(source_index, source_count);
		}
		RegisterFusedAggregatePredicateSources(*predicate.child, source_count);
		return;
	default:
		throw InternalException("Unknown SLJIT fused aggregate predicate kind");
	}
}

static void RegisterFusedAggregateExpressionSources(const SljitNativeRegionExpressionPlan &expr, idx_t &source_count) {
	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::CONSTANT:
		return;
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_CAST:
	case SljitNativeRegionExpressionKind::SIGNED_TO_UNSIGNED_INTEGER_CAST:
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
	case SljitNativeRegionExpressionKind::INTEGRAL_COMPRESS:
	case SljitNativeRegionExpressionKind::INTEGRAL_DECOMPRESS:
	case SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8:
	case SljitNativeRegionExpressionKind::DATE_YEAR:
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		RegisterFusedAggregateSourceIndex(expr.source_index, source_count);
		return;
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::DOUBLE_BINARY_REFERENCES:
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		RegisterFusedAggregateSourceIndex(expr.source_index, source_count);
		RegisterFusedAggregateSourceIndex(expr.right_source_index, source_count);
		return;
	case SljitNativeRegionExpressionKind::INTEGER_COALESCE:
		RegisterFusedAggregateSourceIndex(expr.source_index, source_count);
		if (expr.coalesce_rhs_kind == SljitNativeCoalesceRhsKind::REFERENCE) {
			RegisterFusedAggregateSourceIndex(expr.right_source_index, source_count);
		}
		return;
	case SljitNativeRegionExpressionKind::CONSTANT_OR_NULL:
		for (auto source_index : expr.constant_or_null.guard_source_indices) {
			RegisterFusedAggregateSourceIndex(source_index, source_count);
		}
		return;
	case SljitNativeRegionExpressionKind::PREDICATE:
		RegisterFusedAggregatePredicateSources(*expr.predicate, source_count);
		return;
	default:
		throw InternalException("Unknown SLJIT fused aggregate expression kind");
	}
}

static idx_t FusedAggregateSourceCount(const SljitNativeRegionExpressionPlan *filter,
                                       const SljitNativeRegionExpressionPlan &projection) {
	idx_t source_count = 0;
	if (filter) {
		RegisterFusedAggregateExpressionSources(*filter, source_count);
	}
	RegisterFusedAggregateExpressionSources(projection, source_count);
	return source_count;
}

static void EmitStoreFusedAggregateSourcePointers(struct sljit_compiler *compiler, idx_t source_count) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, source_data));
	for (idx_t source_idx = 0; source_idx < source_count; source_idx++) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0),
		               SljitFusedAggregatePointerArrayOffset(source_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP),
		               SljitFusedAggregateSourceDataOffset(source_idx), SLJIT_R1, 0);
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, source_sel));
	for (idx_t source_idx = 0; source_idx < source_count; source_idx++) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0),
		               SljitFusedAggregatePointerArrayOffset(source_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP),
		               SljitFusedAggregateSourceSelOffset(source_idx), SLJIT_R1, 0);
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, source_validity));
	for (idx_t source_idx = 0; source_idx < source_count; source_idx++) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0),
		               SljitFusedAggregatePointerArrayOffset(source_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP),
		               SljitFusedAggregateSourceValidityOffset(source_idx), SLJIT_R1, 0);
	}
}

static void EmitLoadFusedAggregateSourceIndex(struct sljit_compiler *compiler, idx_t source_index,
                                              sljit_s32 logical_index, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedAggregateSourceSelOffset(source_index));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, logical_index, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfFusedAggregateSourceNull(struct sljit_compiler *compiler, idx_t source_index,
                                                            sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedAggregateSourceValidityOffset(source_index));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static void EmitLoadFusedAggregateSourceData(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                            sljit_s32 index_reg, sljit_sw data_scale, sljit_s32 load_op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedAggregateSourceDataOffset(source_index));
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(SLJIT_R0, index_reg), data_scale);
}

static void EmitLoadFusedAggregateFastSourceData(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                                sljit_sw data_scale, sljit_s32 load_op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedAggregateSourceDataOffset(source_index));
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale);
}

static void EmitLoadFusedAggregateDenseSourceData(struct sljit_compiler *compiler, idx_t source_index,
                                                  sljit_s32 target, sljit_s32 row_index_reg, sljit_sw data_scale,
                                                  sljit_s32 load_op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedAggregateSourceDataOffset(source_index));
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(SLJIT_R0, row_index_reg), data_scale);
}

static void EmitLoadFusedAggregateStringPointer(struct sljit_compiler *compiler, idx_t source_index,
                                                sljit_s32 target, sljit_s32 row_index_reg) {
	static_assert(sizeof(string_t) == 16, "SLJIT fused string match expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_T_SHIFT = 4;
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedAggregateSourceDataOffset(source_index));
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, row_index_reg, 0, SLJIT_IMM, STRING_T_SHIFT);
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_R4, 0);
}

static void EmitLoadFusedAggregateStringDataPointer(struct sljit_compiler *compiler, sljit_s32 string_pointer_reg,
                                                    sljit_s32 string_length_reg, sljit_s32 target_reg) {
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;
	auto non_inlined = sljit_emit_cmp(compiler, SLJIT_GREATER, string_length_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, string_pointer_reg, 0, SLJIT_IMM,
	               STRING_INLINE_PREFIX_OFFSET);
	auto have_data = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(string_pointer_reg), STRING_POINTER_OFFSET);
	sljit_set_label(have_data, sljit_emit_label(compiler));
}

static void EmitLoadFusedAggregateStringForMatch(struct sljit_compiler *compiler, idx_t source_index,
                                                 sljit_s32 row_index_reg,
                                                 SljitFusedAggregatePredicateBranches &result, bool check_null) {
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	sljit_s32 source_row_reg = row_index_reg;
	if (check_null) {
		EmitLoadFusedAggregateSourceIndex(compiler, source_index, row_index_reg, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, source_index, SLJIT_R1));
		source_row_reg = SLJIT_R1;
	}
	EmitLoadFusedAggregateStringPointer(compiler, source_index, SLJIT_R0, source_row_reg);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
	EmitLoadFusedAggregateStringDataPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4);
}

static void EmitFusedStringEqualsAtPosition(struct sljit_compiler *compiler, const string &constant,
                                            sljit_s32 data_reg, sljit_s32 position_reg,
                                            vector<sljit_jump *> &mismatch_jumps) {
	for (idx_t byte_idx = 0; byte_idx < constant.size(); byte_idx++) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, position_reg, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(byte_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM2(data_reg, SLJIT_R0), 0);
		mismatch_jumps.push_back(sljit_emit_cmp(
		    compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		    NumericCast<sljit_sw>(static_cast<uint8_t>(constant[byte_idx]))));
	}
}

static void EmitFusedStringPrefixBranches(struct sljit_compiler *compiler, const string &prefix,
                                          SljitFusedAggregatePredicateBranches &result) {
	const auto prefix_length = prefix.size();
	result.false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM,
	                                             NumericCast<sljit_sw>(prefix_length)));
	if (prefix_length == 0) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitFusedStringEqualsAtPosition(compiler, prefix, SLJIT_R4, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitFusedStringSuffixBranches(struct sljit_compiler *compiler, const string &suffix,
                                          SljitFusedAggregatePredicateBranches &result) {
	const auto suffix_length = suffix.size();
	result.false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM,
	                                             NumericCast<sljit_sw>(suffix_length)));
	if (suffix_length == 0) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S4, 0, SLJIT_R2, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(suffix_length));
	EmitFusedStringEqualsAtPosition(compiler, suffix, SLJIT_R4, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitFusedStringFindFromCurrentPosition(struct sljit_compiler *compiler, const string &needle,
                                                   sljit_s32 data_reg, sljit_s32 length_reg, sljit_s32 position_reg,
                                                   vector<sljit_jump *> &false_jumps) {
	const auto needle_length = needle.size();
	if (needle_length == 0) {
		return;
	}
	false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_LESS, length_reg, 0, SLJIT_IMM,
	                                     NumericCast<sljit_sw>(needle_length)));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R1, 0, length_reg, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(needle_length));
	false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER, position_reg, 0, SLJIT_R1, 0));
	auto loop = sljit_emit_label(compiler);
	vector<sljit_jump *> mismatch_jumps;
	EmitFusedStringEqualsAtPosition(compiler, needle, data_reg, position_reg, mismatch_jumps);
	auto found = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto next_position = sljit_emit_label(compiler);
	SetFusedAggregatePredicateJumpLabels(mismatch_jumps, next_position);
	sljit_emit_op2(compiler, SLJIT_ADD, position_reg, 0, position_reg, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, position_reg, 0, SLJIT_R1, 0);
	false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	auto found_label = sljit_emit_label(compiler);
	sljit_set_label(repeat, loop);
	sljit_set_label(found, found_label);
	sljit_emit_op2(compiler, SLJIT_ADD, position_reg, 0, position_reg, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(needle_length));
}

static void EmitFusedStringContainsBranches(struct sljit_compiler *compiler, const string &needle,
                                            SljitFusedAggregatePredicateBranches &result) {
	if (needle.empty()) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitFusedStringFindFromCurrentPosition(compiler, needle, SLJIT_R4, SLJIT_R2, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitFusedStringLikeBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                        SljitFusedAggregatePredicateBranches &result) {
	if (predicate.string_constants.empty()) {
		if (predicate.string_anchor_start && predicate.string_anchor_end) {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	idx_t fragment_idx = 0;
	if (predicate.string_anchor_start) {
		auto &prefix = predicate.string_constants[0];
		result.false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM,
		                                             NumericCast<sljit_sw>(prefix.size())));
		EmitFusedStringEqualsAtPosition(compiler, prefix, SLJIT_R4, SLJIT_S4, result.false_jumps);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, NumericCast<sljit_sw>(prefix.size()));
		fragment_idx = 1;
	}
	for (; fragment_idx < predicate.string_constants.size(); fragment_idx++) {
		auto &fragment = predicate.string_constants[fragment_idx];
		const bool is_last = fragment_idx + 1 == predicate.string_constants.size();
		if (is_last && predicate.string_anchor_end) {
			result.false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM,
			                                             NumericCast<sljit_sw>(fragment.size())));
			sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R1, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(fragment.size()));
			result.false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R1, 0, SLJIT_S4, 0));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_R1, 0);
			EmitFusedStringEqualsAtPosition(compiler, fragment, SLJIT_R4, SLJIT_S4, result.false_jumps);
		} else {
			EmitFusedStringFindFromCurrentPosition(compiler, fragment, SLJIT_R4, SLJIT_R2, SLJIT_S4,
			                                       result.false_jumps);
		}
	}
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static sljit_s32 FusedNativeIntegerLowerBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive) {
	auto result = inclusive ? SLJIT_SIG_LESS : SLJIT_SIG_LESS_EQUAL;
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
}

static sljit_s32 FusedNativeIntegerUpperBoundFailureJump(SljitNativeIntegerKind kind, bool inclusive) {
	auto result = inclusive ? SLJIT_SIG_GREATER : SLJIT_SIG_GREATER_EQUAL;
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
}

static sljit_s32 FusedNativeIntegerCompareFailureJump(SljitNativeIntegerKind kind,
                                                      SljitNativeIntegerCompareOp op) {
	sljit_s32 result;
	switch (op) {
	case SljitNativeIntegerCompareOp::EQUAL:
		result = SLJIT_NOT_EQUAL;
		break;
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		result = SLJIT_EQUAL;
		break;
	case SljitNativeIntegerCompareOp::LESS_THAN:
		result = SLJIT_SIG_GREATER_EQUAL;
		break;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		result = SLJIT_SIG_LESS_EQUAL;
		break;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		result = SLJIT_SIG_GREATER;
		break;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		result = SLJIT_SIG_LESS;
		break;
	default:
		throw InternalException("Unknown SLJIT native integer comparison operator");
	}
	if (kind == SljitNativeIntegerKind::INT32) {
		result |= SLJIT_32;
	}
	return result;
}

static SljitFusedAggregatePredicateBranches
EmitFusedAggregatePredicateBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate);

static SljitFusedAggregatePredicateBranches EmitFusedAggregateConjunctionBranches(
    struct sljit_compiler *compiler, const SljitNativePredicate &predicate, idx_t child_index, bool null_pending) {
	SljitFusedAggregatePredicateBranches result;
	if (child_index >= predicate.children.size()) {
		if (null_pending) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.conjunction_op == JitExpressionConjunctionOp::AND) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}

	auto child = EmitFusedAggregatePredicateBranches(compiler, *predicate.children[child_index]);
	if (predicate.conjunction_op == JitExpressionConjunctionOp::AND) {
		auto true_label = sljit_emit_label(compiler);
		SetFusedAggregatePredicateJumpLabels(child.true_jumps, true_label);
		auto true_rest = EmitFusedAggregateConjunctionBranches(compiler, predicate, child_index + 1, null_pending);
		AppendFusedAggregatePredicateBranches(result, std::move(true_rest));

		auto null_label = sljit_emit_label(compiler);
		SetFusedAggregatePredicateJumpLabels(child.null_jumps, null_label);
		auto null_rest = EmitFusedAggregateConjunctionBranches(compiler, predicate, child_index + 1, true);
		AppendFusedAggregatePredicateBranches(result, std::move(null_rest));

		AppendFusedAggregatePredicateJumps(result.false_jumps, std::move(child.false_jumps));
		return result;
	}

	auto false_label = sljit_emit_label(compiler);
	SetFusedAggregatePredicateJumpLabels(child.false_jumps, false_label);
	auto false_rest = EmitFusedAggregateConjunctionBranches(compiler, predicate, child_index + 1, null_pending);
	AppendFusedAggregatePredicateBranches(result, std::move(false_rest));

	auto null_label = sljit_emit_label(compiler);
	SetFusedAggregatePredicateJumpLabels(child.null_jumps, null_label);
	auto null_rest = EmitFusedAggregateConjunctionBranches(compiler, predicate, child_index + 1, true);
	AppendFusedAggregatePredicateBranches(result, std::move(null_rest));

	AppendFusedAggregatePredicateJumps(result.true_jumps, std::move(child.true_jumps));
	return result;
}

static SljitFusedAggregatePredicateBranches
EmitFusedAggregatePredicateBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate) {
	SljitFusedAggregatePredicateBranches result;
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		if (predicate.constant_is_null) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.constant_value) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	case SljitNativePredicateKind::REFERENCE:
		EmitLoadFusedAggregateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadFusedAggregateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, 0, SLJIT_MOV_U8);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	case SljitNativePredicateKind::NOT: {
		auto child = EmitFusedAggregatePredicateBranches(compiler, *predicate.child);
		result.true_jumps = std::move(child.false_jumps);
		result.false_jumps = std::move(child.true_jumps);
		result.null_jumps = std::move(child.null_jumps);
		return result;
	}
	case SljitNativePredicateKind::CONJUNCTION:
		return EmitFusedAggregateConjunctionBranches(compiler, predicate, 0, false);
	case SljitNativePredicateKind::CONSTANT_OR_NULL: {
		if (predicate.guard_has_null_constant) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return result;
		}
		for (auto source_index : predicate.guard_source_indices) {
			EmitLoadFusedAggregateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
			result.null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, source_index, SLJIT_R1));
		}
		auto child = EmitFusedAggregatePredicateBranches(compiler, *predicate.child);
		AppendFusedAggregatePredicateBranches(result, std::move(child));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadFusedAggregateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadFusedAggregateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		if (predicate.constant_on_left) {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_IMM,
			                                           NumericCast<sljit_sw>(predicate.constant), SLJIT_R2, 0));
		} else {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_IMM,
			                                           NumericCast<sljit_sw>(predicate.constant)));
		}
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadFusedAggregateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadFusedAggregateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		result.null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		result.null_jumps.push_back(
		    EmitJumpIfFusedAggregateSourceNull(compiler, predicate.right_source_index, SLJIT_S4));
		EmitLoadFusedAggregateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		EmitLoadFusedAggregateSourceData(compiler, predicate.right_source_index, SLJIT_R3, SLJIT_S4, data_scale,
		                                 load_op);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_IN_LIST: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		EmitLoadFusedAggregateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadFusedAggregateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		for (auto constant : predicate.constants) {
			auto match = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM,
			                            NumericCast<sljit_sw>(constant));
			if (predicate.not_in) {
				result.false_jumps.push_back(match);
			} else {
				result.true_jumps.push_back(match);
			}
		}
		if (predicate.list_has_null) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.not_in) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}
	case SljitNativePredicateKind::INTEGER_BETWEEN: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto lower_failure =
		    FusedNativeIntegerLowerBoundFailureJump(predicate.integer_kind, predicate.lower_inclusive);
		auto upper_failure =
		    FusedNativeIntegerUpperBoundFailureJump(predicate.integer_kind, predicate.upper_inclusive);
		EmitLoadFusedAggregateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadFusedAggregateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		auto lower_failed = sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM,
		                                   NumericCast<sljit_sw>(predicate.lower));
		auto upper_failed = sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM,
		                                   NumericCast<sljit_sw>(predicate.upper));
		if (predicate.not_between) {
			result.true_jumps.push_back(lower_failed);
			result.true_jumps.push_back(upper_failed);
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(lower_failed);
			result.false_jumps.push_back(upper_failed);
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
		EmitLoadFusedAggregateStringForMatch(compiler, predicate.source_index, SLJIT_S3, result, true);
		EmitFusedStringPrefixBranches(compiler, predicate.string_constant, result);
		return result;
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
		EmitLoadFusedAggregateStringForMatch(compiler, predicate.source_index, SLJIT_S3, result, true);
		EmitFusedStringSuffixBranches(compiler, predicate.string_constant, result);
		return result;
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
		EmitLoadFusedAggregateStringForMatch(compiler, predicate.source_index, SLJIT_S3, result, true);
		EmitFusedStringContainsBranches(compiler, predicate.string_constant, result);
		return result;
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
		EmitLoadFusedAggregateStringForMatch(compiler, predicate.source_index, SLJIT_S3, result, true);
		EmitFusedStringLikeBranches(compiler, predicate, result);
		return result;
	case SljitNativePredicateKind::NULL_CHECK:
		EmitLoadFusedAggregateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		if (predicate.null_check_op == SljitNativeNullCheckOp::IS_NULL) {
			result.true_jumps.push_back(
			    EmitJumpIfFusedAggregateSourceNull(compiler, predicate.source_index, SLJIT_R1));
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(
			    EmitJumpIfFusedAggregateSourceNull(compiler, predicate.source_index, SLJIT_R1));
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	default:
		throw InternalException("Unknown SLJIT fused aggregate predicate kind");
	}
}

static SljitFusedAggregatePredicateBranches
EmitFusedAggregateFilterBranches(struct sljit_compiler *compiler, const SljitNativeRegionExpressionPlan &filter) {
	if (filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		return EmitFusedAggregatePredicateBranches(compiler, *filter.predicate);
	}

	SljitNativePredicate predicate;
	switch (filter.kind) {
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		predicate.kind = SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		predicate.kind = SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES;
		predicate.right_source_index = filter.right_source_index;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		predicate.kind = SljitNativePredicateKind::INTEGER_IN_LIST;
		predicate.constants = filter.constants;
		predicate.list_has_null = filter.list_has_null;
		predicate.not_in = filter.not_in;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		predicate.kind = SljitNativePredicateKind::INTEGER_BETWEEN;
		predicate.lower = filter.lower;
		predicate.upper = filter.upper;
		predicate.lower_inclusive = filter.lower_inclusive;
		predicate.upper_inclusive = filter.upper_inclusive;
		predicate.not_between = filter.not_between;
		break;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		predicate.kind = SljitNativePredicateKind::NULL_CHECK;
		predicate.null_check_op = filter.null_check_op;
		break;
	default:
		throw InternalException("Unsupported SLJIT fused aggregate filter expression kind");
	}
	predicate.source_index = filter.source_index;
	predicate.constant = filter.constant;
	predicate.constant_on_left = filter.constant_on_left;
	predicate.integer_kind = filter.integer_kind;
	predicate.compare_op = filter.compare_op;
	return EmitFusedAggregatePredicateBranches(compiler, predicate);
}

static SljitFusedAggregatePredicateBranches EmitFusedAggregateDensePredicateBranches(
    struct sljit_compiler *compiler, const SljitNativePredicate &predicate, sljit_s32 row_index_reg);

static SljitFusedAggregatePredicateBranches EmitFusedAggregateDenseConjunctionBranches(
    struct sljit_compiler *compiler, const SljitNativePredicate &predicate, idx_t child_index, bool null_pending,
    sljit_s32 row_index_reg) {
	SljitFusedAggregatePredicateBranches result;
	if (child_index >= predicate.children.size()) {
		if (null_pending) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.conjunction_op == JitExpressionConjunctionOp::AND) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}

	auto child = EmitFusedAggregateDensePredicateBranches(compiler, *predicate.children[child_index], row_index_reg);
	if (predicate.conjunction_op == JitExpressionConjunctionOp::AND) {
		auto true_label = sljit_emit_label(compiler);
		SetFusedAggregatePredicateJumpLabels(child.true_jumps, true_label);
		auto true_rest =
		    EmitFusedAggregateDenseConjunctionBranches(compiler, predicate, child_index + 1, null_pending,
		                                               row_index_reg);
		AppendFusedAggregatePredicateBranches(result, std::move(true_rest));

		auto null_label = sljit_emit_label(compiler);
		SetFusedAggregatePredicateJumpLabels(child.null_jumps, null_label);
		auto null_rest =
		    EmitFusedAggregateDenseConjunctionBranches(compiler, predicate, child_index + 1, true, row_index_reg);
		AppendFusedAggregatePredicateBranches(result, std::move(null_rest));

		AppendFusedAggregatePredicateJumps(result.false_jumps, std::move(child.false_jumps));
		return result;
	}

	auto false_label = sljit_emit_label(compiler);
	SetFusedAggregatePredicateJumpLabels(child.false_jumps, false_label);
	auto false_rest =
	    EmitFusedAggregateDenseConjunctionBranches(compiler, predicate, child_index + 1, null_pending, row_index_reg);
	AppendFusedAggregatePredicateBranches(result, std::move(false_rest));

	auto null_label = sljit_emit_label(compiler);
	SetFusedAggregatePredicateJumpLabels(child.null_jumps, null_label);
	auto null_rest =
	    EmitFusedAggregateDenseConjunctionBranches(compiler, predicate, child_index + 1, true, row_index_reg);
	AppendFusedAggregatePredicateBranches(result, std::move(null_rest));

	AppendFusedAggregatePredicateJumps(result.true_jumps, std::move(child.true_jumps));
	return result;
}

static SljitFusedAggregatePredicateBranches EmitFusedAggregateDensePredicateBranches(
    struct sljit_compiler *compiler, const SljitNativePredicate &predicate, sljit_s32 row_index_reg) {
	SljitFusedAggregatePredicateBranches result;
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		if (predicate.constant_is_null) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.constant_value) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	case SljitNativePredicateKind::REFERENCE:
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, 0,
		                                      SLJIT_MOV_U8);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	case SljitNativePredicateKind::NOT: {
		auto child = EmitFusedAggregateDensePredicateBranches(compiler, *predicate.child, row_index_reg);
		result.true_jumps = std::move(child.false_jumps);
		result.false_jumps = std::move(child.true_jumps);
		result.null_jumps = std::move(child.null_jumps);
		return result;
	}
	case SljitNativePredicateKind::CONJUNCTION:
		return EmitFusedAggregateDenseConjunctionBranches(compiler, predicate, 0, false, row_index_reg);
	case SljitNativePredicateKind::CONSTANT_OR_NULL: {
		if (predicate.guard_has_null_constant) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return result;
		}
		auto child = EmitFusedAggregateDensePredicateBranches(compiler, *predicate.child, row_index_reg);
		AppendFusedAggregatePredicateBranches(result, std::move(child));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, data_scale,
		                                      load_op);
		if (predicate.constant_on_left) {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_IMM,
			                                           NumericCast<sljit_sw>(predicate.constant), SLJIT_R2, 0));
		} else {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_IMM,
			                                           NumericCast<sljit_sw>(predicate.constant)));
		}
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, data_scale,
		                                      load_op);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.right_source_index, SLJIT_R3, row_index_reg,
		                                      data_scale, load_op);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_IN_LIST: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, data_scale,
		                                      load_op);
		for (auto constant : predicate.constants) {
			auto match = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM,
			                            NumericCast<sljit_sw>(constant));
			if (predicate.not_in) {
				result.false_jumps.push_back(match);
			} else {
				result.true_jumps.push_back(match);
			}
		}
		if (predicate.list_has_null) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.not_in) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}
	case SljitNativePredicateKind::INTEGER_BETWEEN: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto lower_failure =
		    FusedNativeIntegerLowerBoundFailureJump(predicate.integer_kind, predicate.lower_inclusive);
		auto upper_failure =
		    FusedNativeIntegerUpperBoundFailureJump(predicate.integer_kind, predicate.upper_inclusive);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, data_scale,
		                                      load_op);
		auto lower_failed = sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM,
		                                   NumericCast<sljit_sw>(predicate.lower));
		auto upper_failed = sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM,
		                                   NumericCast<sljit_sw>(predicate.upper));
		if (predicate.not_between) {
			result.true_jumps.push_back(lower_failed);
			result.true_jumps.push_back(upper_failed);
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(lower_failed);
			result.false_jumps.push_back(upper_failed);
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
		EmitLoadFusedAggregateStringForMatch(compiler, predicate.source_index, row_index_reg, result, false);
		EmitFusedStringPrefixBranches(compiler, predicate.string_constant, result);
		return result;
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
		EmitLoadFusedAggregateStringForMatch(compiler, predicate.source_index, row_index_reg, result, false);
		EmitFusedStringSuffixBranches(compiler, predicate.string_constant, result);
		return result;
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
		EmitLoadFusedAggregateStringForMatch(compiler, predicate.source_index, row_index_reg, result, false);
		EmitFusedStringContainsBranches(compiler, predicate.string_constant, result);
		return result;
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT:
		EmitLoadFusedAggregateStringForMatch(compiler, predicate.source_index, row_index_reg, result, false);
		EmitFusedStringLikeBranches(compiler, predicate, result);
		return result;
	case SljitNativePredicateKind::NULL_CHECK:
		if (predicate.null_check_op == SljitNativeNullCheckOp::IS_NULL) {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	default:
		throw InternalException("Unknown SLJIT fused aggregate fast predicate kind");
	}
}

static SljitFusedAggregatePredicateBranches EmitFusedAggregateDenseFilterBranches(
    struct sljit_compiler *compiler, const SljitNativeRegionExpressionPlan &filter, sljit_s32 row_index_reg) {
	if (filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		return EmitFusedAggregateDensePredicateBranches(compiler, *filter.predicate, row_index_reg);
	}

	SljitNativePredicate predicate;
	switch (filter.kind) {
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		predicate.kind = SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		predicate.kind = SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES;
		predicate.right_source_index = filter.right_source_index;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_IN_LIST:
		predicate.kind = SljitNativePredicateKind::INTEGER_IN_LIST;
		predicate.constants = filter.constants;
		predicate.list_has_null = filter.list_has_null;
		predicate.not_in = filter.not_in;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		predicate.kind = SljitNativePredicateKind::INTEGER_BETWEEN;
		predicate.lower = filter.lower;
		predicate.upper = filter.upper;
		predicate.lower_inclusive = filter.lower_inclusive;
		predicate.upper_inclusive = filter.upper_inclusive;
		predicate.not_between = filter.not_between;
		break;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		predicate.kind = SljitNativePredicateKind::NULL_CHECK;
		predicate.null_check_op = filter.null_check_op;
		break;
	default:
		throw InternalException("Unsupported SLJIT fused aggregate fast filter expression kind");
	}
	predicate.source_index = filter.source_index;
	predicate.constant = filter.constant;
	predicate.constant_on_left = filter.constant_on_left;
	predicate.integer_kind = filter.integer_kind;
	predicate.compare_op = filter.compare_op;
	return EmitFusedAggregateDensePredicateBranches(compiler, predicate, row_index_reg);
}

static bool EmitFusedAggregateDensePredicateFailureJumps(struct sljit_compiler *compiler,
                                                        const SljitNativePredicate &predicate,
                                                        sljit_s32 row_index_reg,
                                                        vector<sljit_jump *> &failure_jumps) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		if (predicate.constant_is_null || !predicate.constant_value) {
			failure_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return true;
	case SljitNativePredicateKind::REFERENCE:
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, 0,
		                                      SLJIT_MOV_U8);
		failure_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		return true;
	case SljitNativePredicateKind::CONJUNCTION:
		if (predicate.conjunction_op != JitExpressionConjunctionOp::AND) {
			return false;
		}
		for (auto &child : predicate.children) {
			if (!child ||
			    !EmitFusedAggregateDensePredicateFailureJumps(compiler, *child, row_index_reg, failure_jumps)) {
				return false;
			}
		}
		return true;
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		if (predicate.guard_has_null_constant) {
			failure_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return true;
		}
		return predicate.child &&
		       EmitFusedAggregateDensePredicateFailureJumps(compiler, *predicate.child, row_index_reg,
		                                                   failure_jumps);
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto failure_type = FusedNativeIntegerCompareFailureJump(predicate.integer_kind, predicate.compare_op);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, data_scale,
		                                      load_op);
		if (predicate.constant_on_left) {
			failure_jumps.push_back(sljit_emit_cmp(compiler, failure_type, SLJIT_IMM,
			                                       NumericCast<sljit_sw>(predicate.constant), SLJIT_R2, 0));
		} else {
			failure_jumps.push_back(sljit_emit_cmp(compiler, failure_type, SLJIT_R2, 0, SLJIT_IMM,
			                                       NumericCast<sljit_sw>(predicate.constant)));
		}
		return true;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto failure_type = FusedNativeIntegerCompareFailureJump(predicate.integer_kind, predicate.compare_op);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, data_scale,
		                                      load_op);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.right_source_index, SLJIT_R3, row_index_reg,
		                                      data_scale, load_op);
		failure_jumps.push_back(sljit_emit_cmp(compiler, failure_type, SLJIT_R2, 0, SLJIT_R3, 0));
		return true;
	}
	case SljitNativePredicateKind::INTEGER_BETWEEN: {
		if (predicate.not_between) {
			return false;
		}
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto lower_failure =
		    FusedNativeIntegerLowerBoundFailureJump(predicate.integer_kind, predicate.lower_inclusive);
		auto upper_failure =
		    FusedNativeIntegerUpperBoundFailureJump(predicate.integer_kind, predicate.upper_inclusive);
		EmitLoadFusedAggregateDenseSourceData(compiler, predicate.source_index, SLJIT_R2, row_index_reg, data_scale,
		                                      load_op);
		failure_jumps.push_back(sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM,
		                                       NumericCast<sljit_sw>(predicate.lower)));
		failure_jumps.push_back(sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM,
		                                       NumericCast<sljit_sw>(predicate.upper)));
		return true;
	}
	case SljitNativePredicateKind::NULL_CHECK:
		if (predicate.null_check_op == SljitNativeNullCheckOp::IS_NULL) {
			failure_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return true;
	default:
		return false;
	}
}

static bool EmitFusedAggregateDenseFilterFailureJumps(struct sljit_compiler *compiler,
                                                      const SljitNativeRegionExpressionPlan &filter,
                                                      sljit_s32 row_index_reg,
                                                      vector<sljit_jump *> &failure_jumps) {
	if (filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
		return EmitFusedAggregateDensePredicateFailureJumps(compiler, *filter.predicate, row_index_reg,
		                                                   failure_jumps);
	}

	SljitNativePredicate predicate;
	switch (filter.kind) {
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT:
		predicate.kind = SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES:
		predicate.kind = SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES;
		predicate.right_source_index = filter.right_source_index;
		break;
	case SljitNativeRegionExpressionKind::INTEGER_BETWEEN:
		predicate.kind = SljitNativePredicateKind::INTEGER_BETWEEN;
		predicate.lower = filter.lower;
		predicate.upper = filter.upper;
		predicate.lower_inclusive = filter.lower_inclusive;
		predicate.upper_inclusive = filter.upper_inclusive;
		predicate.not_between = filter.not_between;
		break;
	case SljitNativeRegionExpressionKind::NULL_CHECK:
		predicate.kind = SljitNativePredicateKind::NULL_CHECK;
		predicate.null_check_op = filter.null_check_op;
		break;
	default:
		return false;
	}
	predicate.source_index = filter.source_index;
	predicate.constant = filter.constant;
	predicate.constant_on_left = filter.constant_on_left;
	predicate.integer_kind = filter.integer_kind;
	predicate.compare_op = filter.compare_op;
	return EmitFusedAggregateDensePredicateFailureJumps(compiler, predicate, row_index_reg, failure_jumps);
}

static SljitFusedAggregatePredicateBranches
EmitFusedAggregateFastFilterBranches(struct sljit_compiler *compiler, const SljitNativeRegionExpressionPlan &filter) {
	return EmitFusedAggregateDenseFilterBranches(compiler, filter, SLJIT_S1);
}

static SljitFusedAggregatePredicateBranches EmitFusedAggregateSharedSelectionFilterBranches(
    struct sljit_compiler *compiler, const SljitNativeRegionExpressionPlan &filter) {
	return EmitFusedAggregateDenseFilterBranches(compiler, filter, SLJIT_S3);
}

static void EmitIncrementFusedAggregateLocal(struct sljit_compiler *compiler, sljit_sw offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), offset);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), offset, SLJIT_R3, 0);
}

static void EmitAddFusedAggregateProjectedValue(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_FUSED_AGG_SUM_OFFSET);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_FUSED_AGG_SUM_OFFSET, SLJIT_R3, 0);
	EmitIncrementFusedAggregateLocal(compiler, SLJIT_FUSED_AGG_VALID_COUNT_OFFSET);
}

static void EmitAddFusedAggregateStateCount(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, state_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP),
	               SLJIT_FUSED_AGG_FILTERED_COUNT_OFFSET);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, selected_count), SLJIT_R2, 0);
}

static void EmitLoadFusedAggregateStateValueAddress(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, state));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, state_value_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_R2, 0);
}

static void EmitSetFusedAggregateStateIsSet(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, state));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, state_is_set_offset));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_IMM, 1);
}

static void EmitCommitFusedAggregateSum(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_FUSED_AGG_VALID_COUNT_OFFSET);
	auto no_values = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	EmitLoadFusedAggregateStateValueAddress(compiler, SLJIT_R0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0), 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_FUSED_AGG_SUM_OFFSET);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_R0), 0, SLJIT_R1, 0);
	EmitSetFusedAggregateStateIsSet(compiler);
	sljit_set_label(no_values, sljit_emit_label(compiler));
}

static vector<sljit_jump *> EmitFusedAggregateProjectionValue(struct sljit_compiler *compiler,
                                                             const SljitNativeRegionExpressionPlan &projection,
                                                             vector<sljit_jump *> &overflow_jumps) {
	vector<sljit_jump *> null_jumps;
	auto data_scale = NativeIntegerDataScale(projection.integer_kind);
	auto load_op = NativeIntegerLoadOp(projection.integer_kind);
	if (projection.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		EmitLoadFusedAggregateSourceIndex(compiler, projection.source_index, SLJIT_S3, SLJIT_R1);
		null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, projection.source_index, SLJIT_R1));
		EmitLoadFusedAggregateSourceData(compiler, projection.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		return null_jumps;
	}

	D_ASSERT(projection.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES);
	auto binary_op = NativeIntegerBinaryOp(projection.integer_kind, projection.binary_op);
	EmitLoadFusedAggregateSourceIndex(compiler, projection.source_index, SLJIT_S3, SLJIT_R1);
	EmitLoadFusedAggregateSourceIndex(compiler, projection.right_source_index, SLJIT_S3, SLJIT_S4);
	null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, projection.source_index, SLJIT_R1));
	null_jumps.push_back(EmitJumpIfFusedAggregateSourceNull(compiler, projection.right_source_index, SLJIT_S4));
	EmitLoadFusedAggregateSourceData(compiler, projection.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
	EmitLoadFusedAggregateSourceData(compiler, projection.right_source_index, SLJIT_R3, SLJIT_S4, data_scale, load_op);
	sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	overflow_jumps.push_back(sljit_emit_jump(compiler, SLJIT_OVERFLOW));
	if (projection.check_result_range) {
		overflow_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
		                                        NumericCast<sljit_sw>(projection.result_min)));
		overflow_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
		                                        NumericCast<sljit_sw>(projection.result_max)));
	}
	return null_jumps;
}

static void EmitFusedAggregateFastProjectionValue(struct sljit_compiler *compiler,
                                                 const SljitNativeRegionExpressionPlan &projection,
                                                 vector<sljit_jump *> &overflow_jumps) {
	auto data_scale = NativeIntegerDataScale(projection.integer_kind);
	auto load_op = NativeIntegerLoadOp(projection.integer_kind);
	if (projection.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		EmitLoadFusedAggregateFastSourceData(compiler, projection.source_index, SLJIT_R2, data_scale, load_op);
		return;
	}

	D_ASSERT(projection.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES);
	auto binary_op = NativeIntegerBinaryOp(projection.integer_kind, projection.binary_op);
	EmitLoadFusedAggregateFastSourceData(compiler, projection.source_index, SLJIT_R2, data_scale, load_op);
	EmitLoadFusedAggregateFastSourceData(compiler, projection.right_source_index, SLJIT_R3, data_scale, load_op);
	sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	overflow_jumps.push_back(sljit_emit_jump(compiler, SLJIT_OVERFLOW));
	if (projection.check_result_range) {
		overflow_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
		                                        NumericCast<sljit_sw>(projection.result_min)));
		overflow_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
		                                        NumericCast<sljit_sw>(projection.result_max)));
	}
}

static void EmitFusedAggregateSharedSelectionProjectionValue(struct sljit_compiler *compiler,
                                                            const SljitNativeRegionExpressionPlan &projection,
                                                            vector<sljit_jump *> &overflow_jumps) {
	auto data_scale = NativeIntegerDataScale(projection.integer_kind);
	auto load_op = NativeIntegerLoadOp(projection.integer_kind);
	if (projection.kind == SljitNativeRegionExpressionKind::REFERENCE) {
		EmitLoadFusedAggregateSourceData(compiler, projection.source_index, SLJIT_R2, SLJIT_S3, data_scale, load_op);
		return;
	}

	D_ASSERT(projection.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES);
	auto binary_op = NativeIntegerBinaryOp(projection.integer_kind, projection.binary_op);
	EmitLoadFusedAggregateSourceData(compiler, projection.source_index, SLJIT_R2, SLJIT_S3, data_scale, load_op);
	EmitLoadFusedAggregateSourceData(compiler, projection.right_source_index, SLJIT_R3, SLJIT_S3, data_scale, load_op);
	sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
	overflow_jumps.push_back(sljit_emit_jump(compiler, SLJIT_OVERFLOW));
	if (projection.check_result_range) {
		overflow_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
		                                        NumericCast<sljit_sw>(projection.result_min)));
		overflow_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
		                                        NumericCast<sljit_sw>(projection.result_max)));
	}
}

static unique_ptr<JitCodeHandle> BuildSljitFusedUngroupedSum(
    const SljitNativeRegionExpressionPlan *filter, const SljitNativeRegionExpressionPlan &projection,
    const SljitNativeUngroupedAggregateUpdatePlan &, SljitFusedUngroupedAggregateFunction &function, string &error) {
	auto source_count = FusedAggregateSourceCount(filter, projection);
	if (source_count == 0) {
		error = "SLJIT fused ungrouped aggregate requires at least one source vector";
		return nullptr;
	}
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, SljitFusedAggregateLocalSize(source_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_FUSED_AGG_SUM_OFFSET, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_FUSED_AGG_FILTERED_COUNT_OFFSET, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_FUSED_AGG_VALID_COUNT_OFFSET, SLJIT_IMM, 0);
	EmitStoreFusedAggregateSourcePointers(compiler, source_count);

	vector<sljit_jump *> overflow_jumps;

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, flat_all_valid));
	auto use_non_flat_path = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	SljitFusedAggregatePredicateBranches fast_branches;
	vector<sljit_jump *> fast_failure_jumps;
	bool fast_filter_fallthrough = false;
	if (filter) {
		fast_filter_fallthrough =
		    EmitFusedAggregateDenseFilterFailureJumps(compiler, *filter, SLJIT_S1, fast_failure_jumps);
		if (!fast_filter_fallthrough) {
			fast_branches = EmitFusedAggregateFastFilterBranches(compiler, *filter);
			auto fast_true_label = sljit_emit_label(compiler);
			SetFusedAggregatePredicateJumpLabels(fast_branches.true_jumps, fast_true_label);
		}
	}
	EmitIncrementFusedAggregateLocal(compiler, SLJIT_FUSED_AGG_FILTERED_COUNT_OFFSET);
	EmitFusedAggregateFastProjectionValue(compiler, projection, overflow_jumps);
	EmitAddFusedAggregateProjectedValue(compiler);

	auto fast_next_row = sljit_emit_label(compiler);
	if (filter) {
		if (fast_filter_fallthrough) {
			SetFusedAggregatePredicateJumpLabels(fast_failure_jumps, fast_next_row);
		} else {
			SetFusedAggregatePredicateJumpLabels(fast_branches.false_jumps, fast_next_row);
			SetFusedAggregatePredicateJumpLabels(fast_branches.null_jumps, fast_next_row);
		}
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	auto non_flat_path = sljit_emit_label(compiler);
	sljit_set_label(use_non_flat_path, non_flat_path);
	sljit_jump *use_generic_path = nullptr;
	sljit_jump *shared_done = nullptr;
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedUngroupedAggregateInput, shared_selection_all_valid));
	use_generic_path = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	auto shared_loop = sljit_emit_label(compiler);
	shared_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedAggregateSourceSelOffset(0));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_S3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);

	SljitFusedAggregatePredicateBranches shared_branches;
	vector<sljit_jump *> shared_failure_jumps;
	bool shared_filter_fallthrough = false;
	if (filter) {
		shared_filter_fallthrough =
		    EmitFusedAggregateDenseFilterFailureJumps(compiler, *filter, SLJIT_S3, shared_failure_jumps);
		if (!shared_filter_fallthrough) {
			shared_branches = EmitFusedAggregateSharedSelectionFilterBranches(compiler, *filter);
			auto shared_true_label = sljit_emit_label(compiler);
			SetFusedAggregatePredicateJumpLabels(shared_branches.true_jumps, shared_true_label);
		}
	}
	EmitIncrementFusedAggregateLocal(compiler, SLJIT_FUSED_AGG_FILTERED_COUNT_OFFSET);
	EmitFusedAggregateSharedSelectionProjectionValue(compiler, projection, overflow_jumps);
	EmitAddFusedAggregateProjectedValue(compiler);

	auto shared_next_row = sljit_emit_label(compiler);
	if (filter) {
		if (shared_filter_fallthrough) {
			SetFusedAggregatePredicateJumpLabels(shared_failure_jumps, shared_next_row);
		} else {
			SetFusedAggregatePredicateJumpLabels(shared_branches.false_jumps, shared_next_row);
			SetFusedAggregatePredicateJumpLabels(shared_branches.null_jumps, shared_next_row);
		}
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto shared_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(shared_repeat, shared_loop);

	auto loop = sljit_emit_label(compiler);
	sljit_set_label(use_generic_path, loop);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);

	SljitFusedAggregatePredicateBranches branches;
	if (filter) {
		branches = EmitFusedAggregateFilterBranches(compiler, *filter);
		auto true_label = sljit_emit_label(compiler);
		SetFusedAggregatePredicateJumpLabels(branches.true_jumps, true_label);
	}
	EmitIncrementFusedAggregateLocal(compiler, SLJIT_FUSED_AGG_FILTERED_COUNT_OFFSET);
	auto projection_null_jumps = EmitFusedAggregateProjectionValue(compiler, projection, overflow_jumps);
	EmitAddFusedAggregateProjectedValue(compiler);
	auto next_after_value = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto projection_null_label = sljit_emit_label(compiler);
	SetFusedAggregatePredicateJumpLabels(projection_null_jumps, projection_null_label);

	auto next_after_projection = sljit_emit_label(compiler);
	sljit_set_label(next_after_value, next_after_projection);
	if (filter) {
		SetFusedAggregatePredicateJumpLabels(branches.false_jumps, next_after_projection);
		SetFusedAggregatePredicateJumpLabels(branches.null_jumps, next_after_projection);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	auto overflow_label = sljit_emit_label(compiler);
	for (auto &overflow : overflow_jumps) {
		sljit_set_label(overflow, overflow_label);
	}
	if (!overflow_jumps.empty()) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitFusedUngroupedAggregateOverflow));
		sljit_emit_return_void(compiler);
	}

	auto commit_label = sljit_emit_label(compiler);
	sljit_set_label(fast_done, commit_label);
	if (shared_done) {
		sljit_set_label(shared_done, commit_label);
	}
	sljit_set_label(done, commit_label);
	EmitAddFusedAggregateStateCount(compiler);
	EmitCommitFusedAggregateSum(compiler);
	sljit_emit_return_void(compiler);
	return FinishSljitFusedUngroupedAggregateCode(compiler, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitFusedFilterProjectionUngroupedSum(
    const SljitNativeRegionExpressionPlan &filter, const SljitNativeRegionExpressionPlan &projection,
    const SljitNativeUngroupedAggregateUpdatePlan &update, SljitFusedUngroupedAggregateFunction &function,
    string &error) {
	return BuildSljitFusedUngroupedSum(&filter, projection, update, function, error);
}

unique_ptr<JitCodeHandle> BuildSljitFusedProjectionUngroupedSum(
    const SljitNativeRegionExpressionPlan &projection, const SljitNativeUngroupedAggregateUpdatePlan &update,
    SljitFusedUngroupedAggregateFunction &function, string &error) {
	return BuildSljitFusedUngroupedSum(nullptr, projection, update, function, error);
}

static void SLJIT_FUNC SljitFusedPerfectHashAggregateOverflow(SljitFusedPerfectHashAggregateInput *input) {
	try {
		throw OutOfRangeException("%s", input->overflow_message);
	} catch (...) {
		input->error = std::current_exception();
	}
}

static void SLJIT_FUNC SljitFusedPerfectHashAggregateInvalidGroup(SljitFusedPerfectHashAggregateInput *input) {
	try {
		throw InvalidInputException("Perfect hash aggregate: aggregate group %llu exceeded total groups %llu. This "
		                            "likely means that the statistics in your data source are corrupt.\n* PRAGMA "
		                            "disable_optimizer to disable optimizations that rely on correct statistics",
		                            static_cast<unsigned long long>(input->invalid_group_id),
		                            static_cast<unsigned long long>(input->perfect_hash_total_groups));
	} catch (...) {
		input->error = std::current_exception();
	}
}

static unique_ptr<JitCodeHandle> FinishSljitFusedPerfectHashAggregateCode(
    struct sljit_compiler *compiler, SljitFusedPerfectHashAggregateFunction &function, string &error) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = sljit_generate_code(compiler, 0, nullptr);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitFusedPerfectHashAggregateFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

enum class SljitFusedPerfectHashExprKind : uint8_t {
	SOURCE_REF,
	STRING_COMPRESS_UINT8,
	INTEGER_CAST,
	INTEGER_BINARY_CONSTANT,
	INTEGER_BINARY_REFERENCES,
	INTEGER_COMPARE_CONSTANT,
	INTEGER_COMPARE_REFERENCES
};

struct SljitFusedPerfectHashExpr {
	SljitFusedPerfectHashExprKind kind = SljitFusedPerfectHashExprKind::SOURCE_REF;
	LogicalType return_type;
	SljitNativeIntegerKind integer_kind = SljitNativeIntegerKind::INT64;
	idx_t source_index = 0;
	idx_t left = DConstants::INVALID_INDEX;
	idx_t right = DConstants::INVALID_INDEX;
	int64_t constant = 0;
	int64_t result_min = 0;
	int64_t result_max = 0;
	bool constant_on_left = false;
	bool check_result_range = false;
	SljitNativeIntegerBinaryOp binary_op = SljitNativeIntegerBinaryOp::ADD;
	SljitNativeIntegerCompareOp compare_op = SljitNativeIntegerCompareOp::EQUAL;
	SljitNativeSignedIntegerWidth cast_source_width = SljitNativeSignedIntegerWidth::INT32;
	SljitNativeSignedIntegerWidth cast_target_width = SljitNativeSignedIntegerWidth::INT32;
	bool try_cast = false;
};

struct SljitFusedPerfectHashAggregatePlan {
	idx_t source_count = 0;
	vector<SljitFusedPerfectHashExpr> expressions;
	vector<idx_t> filters;
	vector<idx_t> groups;
	vector<idx_t> aggregate_payloads;
	vector<SljitNativeGroupedAggregateUpdatePlan> updates;
	vector<idx_t> required_bits;
	vector<int64_t> group_minima;
	idx_t total_required_bits = 0;
};

enum class SljitFusedPerfectHashVectorPath : uint8_t {
	GENERIC,
	SELECTION_ALL_VALID,
	ALL_SELECTED_ALL_VALID,
	FLAT_ALL_VALID
};

static sljit_sw SljitFusedPerfectHashPointerArrayOffset(idx_t index) {
	return NumericCast<sljit_sw>(index * sizeof(data_ptr_t));
}

static sljit_sw SljitFusedPerfectHashValueOffset(idx_t index) {
	return NumericCast<sljit_sw>(index * sizeof(sljit_sw) * 2);
}

static sljit_sw SljitFusedPerfectHashNullOffset(idx_t index) {
	return SljitFusedPerfectHashValueOffset(index) + NumericCast<sljit_sw>(sizeof(sljit_sw));
}

static sljit_sw SljitFusedPerfectHashAggregateBaseOffset(const SljitFusedPerfectHashAggregatePlan &plan) {
	return NumericCast<sljit_sw>(plan.expressions.size() * sizeof(sljit_sw) * 2);
}

static sljit_sw SljitFusedPerfectHashSourceDataOffset(const SljitFusedPerfectHashAggregatePlan &plan, idx_t index) {
	return SljitFusedPerfectHashAggregateBaseOffset(plan) + NumericCast<sljit_sw>(sizeof(sljit_sw)) +
	       NumericCast<sljit_sw>(index * sizeof(const_data_ptr_t));
}

static sljit_sw SljitFusedPerfectHashSourceSelBaseOffset(const SljitFusedPerfectHashAggregatePlan &plan) {
	return SljitFusedPerfectHashSourceDataOffset(plan, 0) +
	       NumericCast<sljit_sw>(plan.source_count * sizeof(const_data_ptr_t));
}

static sljit_sw SljitFusedPerfectHashSourceSelOffset(const SljitFusedPerfectHashAggregatePlan &plan, idx_t index) {
	return SljitFusedPerfectHashSourceSelBaseOffset(plan) +
	       NumericCast<sljit_sw>(index * sizeof(const sel_t *));
}

static sljit_sw SljitFusedPerfectHashSourceValidityBaseOffset(const SljitFusedPerfectHashAggregatePlan &plan) {
	return SljitFusedPerfectHashSourceSelBaseOffset(plan) +
	       NumericCast<sljit_sw>(plan.source_count * sizeof(const sel_t *));
}

static sljit_sw SljitFusedPerfectHashSourceValidityOffset(const SljitFusedPerfectHashAggregatePlan &plan,
                                                          idx_t index) {
	return SljitFusedPerfectHashSourceValidityBaseOffset(plan) +
	       NumericCast<sljit_sw>(index * sizeof(const validity_t *));
}

static sljit_sw SljitFusedPerfectHashStateDataOffset(const SljitFusedPerfectHashAggregatePlan &plan) {
	return SljitFusedPerfectHashSourceValidityBaseOffset(plan) +
	       NumericCast<sljit_sw>(plan.source_count * sizeof(const validity_t *));
}

static sljit_sw SljitFusedPerfectHashGroupIsSetOffset(const SljitFusedPerfectHashAggregatePlan &plan) {
	return SljitFusedPerfectHashStateDataOffset(plan) + NumericCast<sljit_sw>(sizeof(data_ptr_t));
}

static sljit_sw SljitFusedPerfectHashTotalGroupsOffset(const SljitFusedPerfectHashAggregatePlan &plan) {
	return SljitFusedPerfectHashGroupIsSetOffset(plan) + NumericCast<sljit_sw>(sizeof(bool *));
}

static sljit_sw SljitFusedPerfectHashTupleSizeOffset(const SljitFusedPerfectHashAggregatePlan &plan) {
	return SljitFusedPerfectHashTotalGroupsOffset(plan) + NumericCast<sljit_sw>(sizeof(idx_t));
}

static sljit_sw SljitFusedPerfectHashAggregateStateOffsetOffset(const SljitFusedPerfectHashAggregatePlan &plan) {
	return SljitFusedPerfectHashTupleSizeOffset(plan) + NumericCast<sljit_sw>(sizeof(idx_t));
}

static sljit_sw SljitFusedPerfectHashLocalSize(const SljitFusedPerfectHashAggregatePlan &plan) {
	return SljitFusedPerfectHashAggregateStateOffsetOffset(plan) + NumericCast<sljit_sw>(sizeof(idx_t));
}

static void EmitStoreFusedPerfectHashSourcePointerArray(struct sljit_compiler *compiler, sljit_sw input_offset,
                                                        sljit_sw (*local_offset)(const SljitFusedPerfectHashAggregatePlan &,
                                                                                 idx_t),
                                                        const SljitFusedPerfectHashAggregatePlan &plan) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), input_offset);
	for (idx_t source_idx = 0; source_idx < plan.source_count; source_idx++) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R0),
		               SljitFusedPerfectHashPointerArrayOffset(source_idx));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), local_offset(plan, source_idx), SLJIT_R1, 0);
	}
}

static void EmitStoreFusedPerfectHashSourcePointers(struct sljit_compiler *compiler,
                                                    const SljitFusedPerfectHashAggregatePlan &plan) {
	EmitStoreFusedPerfectHashSourcePointerArray(compiler, offsetof(SljitFusedPerfectHashAggregateInput, source_data),
	                                            SljitFusedPerfectHashSourceDataOffset, plan);
	EmitStoreFusedPerfectHashSourcePointerArray(compiler, offsetof(SljitFusedPerfectHashAggregateInput, source_sel),
	                                            SljitFusedPerfectHashSourceSelOffset, plan);
	EmitStoreFusedPerfectHashSourcePointerArray(compiler, offsetof(SljitFusedPerfectHashAggregateInput, source_validity),
	                                            SljitFusedPerfectHashSourceValidityOffset, plan);
}

static void EmitStoreFusedPerfectHashStateLayout(struct sljit_compiler *compiler,
                                                 const SljitFusedPerfectHashAggregatePlan &plan) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, perfect_hash_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), SljitFusedPerfectHashStateDataOffset(plan), SLJIT_R0,
	               0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, perfect_hash_group_is_set));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), SljitFusedPerfectHashGroupIsSetOffset(plan), SLJIT_R0,
	               0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, perfect_hash_total_groups));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SljitFusedPerfectHashTotalGroupsOffset(plan), SLJIT_R0,
	               0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, perfect_hash_tuple_size));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SljitFusedPerfectHashTupleSizeOffset(plan), SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, perfect_hash_aggregate_state_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashAggregateStateOffsetOffset(plan), SLJIT_R0, 0);
}

static bool TryGetFusedPerfectHashIntegerKind(const LogicalType &type, SljitNativeIntegerKind &kind) {
	switch (type.InternalType()) {
	case PhysicalType::UINT8:
		kind = SljitNativeIntegerKind::UINT8;
		return true;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
		kind = SljitNativeIntegerKind::INT32;
		return true;
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
		kind = type.id() == LogicalTypeId::DECIMAL ? SljitNativeIntegerKind::DECIMAL64
		                                           : SljitNativeIntegerKind::INT64;
		return true;
	default:
		return false;
	}
}

static bool TryReadFusedPerfectHashMinimum(const Value &value, int64_t &result) {
	if (value.IsNull()) {
		return false;
	}
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		result = value.GetValueUnsafe<int8_t>();
		return true;
	case PhysicalType::INT16:
		result = value.GetValueUnsafe<int16_t>();
		return true;
	case PhysicalType::INT32:
		result = value.GetValueUnsafe<int32_t>();
		return true;
	case PhysicalType::INT64:
		result = value.GetValueUnsafe<int64_t>();
		return true;
	case PhysicalType::UINT8:
		result = value.GetValueUnsafe<uint8_t>();
		return true;
	case PhysicalType::UINT16:
		result = value.GetValueUnsafe<uint16_t>();
		return true;
	case PhysicalType::UINT32:
		result = value.GetValueUnsafe<uint32_t>();
		return true;
	default:
		return false;
	}
}

static idx_t AddFusedPerfectHashSourceExpression(SljitFusedPerfectHashAggregatePlan &plan, idx_t source_index,
                                                 const LogicalType &type, string &error) {
	SljitNativeIntegerKind integer_kind;
	if (type.InternalType() != PhysicalType::VARCHAR && !TryGetFusedPerfectHashIntegerKind(type, integer_kind)) {
		error = "SLJIT fused perfect hash source expression has unsupported type " + type.ToString();
		return DConstants::INVALID_INDEX;
	}
	SljitFusedPerfectHashExpr expr;
	expr.kind = SljitFusedPerfectHashExprKind::SOURCE_REF;
	expr.return_type = type;
	expr.integer_kind = type.InternalType() == PhysicalType::VARCHAR ? SljitNativeIntegerKind::INT64 : integer_kind;
	expr.source_index = source_index;
	plan.expressions.push_back(std::move(expr));
	return plan.expressions.size() - 1;
}

static idx_t AddFusedPerfectHashExpression(SljitFusedPerfectHashAggregatePlan &plan,
                                           SljitFusedPerfectHashExpr expr) {
	plan.expressions.push_back(std::move(expr));
	return plan.expressions.size() - 1;
}

static idx_t BuildFusedPerfectHashExpression(const SljitNativeRegionExpressionPlan &expr,
                                             const vector<idx_t> &current_columns,
                                             SljitFusedPerfectHashAggregatePlan &plan, string &error) {
	auto resolve_current = [&](idx_t source_index) -> idx_t {
		if (source_index >= current_columns.size()) {
			error = "SLJIT fused perfect hash expression references column outside current projection layout";
			return DConstants::INVALID_INDEX;
		}
		return current_columns[source_index];
	};

	switch (expr.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
		return resolve_current(expr.source_index);
	case SljitNativeRegionExpressionKind::STRING_COMPRESS_UINT8: {
		auto child = resolve_current(expr.source_index);
		if (child == DConstants::INVALID_INDEX) {
			return child;
		}
		SljitFusedPerfectHashExpr fused;
		fused.kind = SljitFusedPerfectHashExprKind::STRING_COMPRESS_UINT8;
		fused.return_type = expr.return_type;
		fused.integer_kind = SljitNativeIntegerKind::INT32;
		fused.left = child;
		return AddFusedPerfectHashExpression(plan, std::move(fused));
	}
	case SljitNativeRegionExpressionKind::INTEGER_CAST: {
		auto child = resolve_current(expr.source_index);
		if (child == DConstants::INVALID_INDEX) {
			return child;
		}
		SljitNativeIntegerKind integer_kind;
		if (!TryGetFusedPerfectHashIntegerKind(expr.return_type, integer_kind)) {
			error = "SLJIT fused perfect hash cast has unsupported result type " + expr.return_type.ToString();
			return DConstants::INVALID_INDEX;
		}
		SljitFusedPerfectHashExpr fused;
		fused.kind = SljitFusedPerfectHashExprKind::INTEGER_CAST;
		fused.return_type = expr.return_type;
		fused.integer_kind = integer_kind;
		fused.left = child;
		fused.cast_source_width = expr.cast_source_width;
		fused.cast_target_width = expr.cast_target_width;
		fused.try_cast = expr.try_cast;
		return AddFusedPerfectHashExpression(plan, std::move(fused));
	}
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT: {
		auto child = resolve_current(expr.source_index);
		if (child == DConstants::INVALID_INDEX) {
			return child;
		}
		SljitFusedPerfectHashExpr fused;
		fused.kind = SljitFusedPerfectHashExprKind::INTEGER_BINARY_CONSTANT;
		fused.return_type = expr.return_type;
		fused.integer_kind = expr.integer_kind;
		fused.left = child;
		fused.constant = expr.constant;
		fused.constant_on_left = expr.constant_on_left;
		fused.check_result_range = expr.check_result_range;
		fused.result_min = expr.result_min;
		fused.result_max = expr.result_max;
		fused.binary_op = expr.binary_op;
		return AddFusedPerfectHashExpression(plan, std::move(fused));
	}
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES: {
		auto left = resolve_current(expr.source_index);
		auto right = resolve_current(expr.right_source_index);
		if (left == DConstants::INVALID_INDEX || right == DConstants::INVALID_INDEX) {
			return DConstants::INVALID_INDEX;
		}
		SljitFusedPerfectHashExpr fused;
		fused.kind = SljitFusedPerfectHashExprKind::INTEGER_BINARY_REFERENCES;
		fused.return_type = expr.return_type;
		fused.integer_kind = expr.integer_kind;
		fused.left = left;
		fused.right = right;
		fused.check_result_range = expr.check_result_range;
		fused.result_min = expr.result_min;
		fused.result_max = expr.result_max;
		fused.binary_op = expr.binary_op;
		return AddFusedPerfectHashExpression(plan, std::move(fused));
	}
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT: {
		auto child = resolve_current(expr.source_index);
		if (child == DConstants::INVALID_INDEX) {
			return child;
		}
		SljitFusedPerfectHashExpr fused;
		fused.kind = SljitFusedPerfectHashExprKind::INTEGER_COMPARE_CONSTANT;
		fused.return_type = LogicalType::BOOLEAN;
		fused.integer_kind = expr.integer_kind;
		fused.left = child;
		fused.constant = expr.constant;
		fused.constant_on_left = expr.constant_on_left;
		fused.compare_op = expr.compare_op;
		return AddFusedPerfectHashExpression(plan, std::move(fused));
	}
	case SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES: {
		auto left = resolve_current(expr.source_index);
		auto right = resolve_current(expr.right_source_index);
		if (left == DConstants::INVALID_INDEX || right == DConstants::INVALID_INDEX) {
			return DConstants::INVALID_INDEX;
		}
		SljitFusedPerfectHashExpr fused;
		fused.kind = SljitFusedPerfectHashExprKind::INTEGER_COMPARE_REFERENCES;
		fused.return_type = LogicalType::BOOLEAN;
		fused.integer_kind = expr.integer_kind;
		fused.left = left;
		fused.right = right;
		fused.compare_op = expr.compare_op;
		return AddFusedPerfectHashExpression(plan, std::move(fused));
	}
	default:
		error = "SLJIT fused perfect hash expression does not support this expression kind";
		return DConstants::INVALID_INDEX;
	}
}

static idx_t BuildFusedPerfectHashPredicate(const SljitNativePredicate &predicate,
                                            const vector<idx_t> &current_columns,
                                            SljitFusedPerfectHashAggregatePlan &plan, string &error) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT: {
		SljitNativeRegionExpressionPlan expr;
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_COMPARE_CONSTANT;
		expr.integer_kind = predicate.integer_kind;
		expr.source_index = predicate.source_index;
		expr.constant = predicate.constant;
		expr.constant_on_left = predicate.constant_on_left;
		expr.compare_op = predicate.compare_op;
		return BuildFusedPerfectHashExpression(expr, current_columns, plan, error);
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES: {
		SljitNativeRegionExpressionPlan expr;
		expr.kind = SljitNativeRegionExpressionKind::INTEGER_COMPARE_REFERENCES;
		expr.integer_kind = predicate.integer_kind;
		expr.source_index = predicate.source_index;
		expr.right_source_index = predicate.right_source_index;
		expr.compare_op = predicate.compare_op;
		return BuildFusedPerfectHashExpression(expr, current_columns, plan, error);
	}
	default:
		error = "SLJIT fused perfect hash predicate is outside generated predicate coverage";
		return DConstants::INVALID_INDEX;
	}
}

static bool BuildFusedPerfectHashPredicateFilters(const SljitNativePredicate &predicate,
                                                  const vector<idx_t> &current_columns,
                                                  SljitFusedPerfectHashAggregatePlan &plan, string &error) {
	if (predicate.kind == SljitNativePredicateKind::CONJUNCTION) {
		if (predicate.conjunction_op != JitExpressionConjunctionOp::AND) {
			error = "SLJIT fused perfect hash predicate supports AND conjunctions only";
			return false;
		}
		for (auto &child : predicate.children) {
			if (!child || !BuildFusedPerfectHashPredicateFilters(*child, current_columns, plan, error)) {
				return false;
			}
		}
		return true;
	}
	auto filter_idx = BuildFusedPerfectHashPredicate(predicate, current_columns, plan, error);
	if (filter_idx == DConstants::INVALID_INDEX) {
		return false;
	}
	plan.filters.push_back(filter_idx);
	return true;
}

static bool BuildFusedPerfectHashPlan(const SljitNativeRegionPlan &region,
                                      SljitFusedPerfectHashAggregatePlan &plan, string &error) {
	if (!CanFuseNativePerfectHashAggregateRegion(region)) {
		error = "SLJIT fused perfect hash requires a prepared-input/projection/perfect-hash aggregate region";
		return false;
	}
	auto &first_op = region.ops.front();
	if (first_op.output_types.empty()) {
		error = "SLJIT fused perfect hash requires source input types from the first native op";
		return false;
	}
	plan.source_count = first_op.output_types.size();
	vector<idx_t> current_columns;
	current_columns.reserve(first_op.output_types.size());
	for (idx_t source_idx = 0; source_idx < first_op.output_types.size(); source_idx++) {
		auto expr_idx = AddFusedPerfectHashSourceExpression(plan, source_idx, first_op.output_types[source_idx], error);
		if (expr_idx == DConstants::INVALID_INDEX) {
			return false;
		}
		current_columns.push_back(expr_idx);
	}

	for (idx_t op_idx = 0; op_idx + 1 < region.ops.size(); op_idx++) {
		auto &op = region.ops[op_idx];
		if (op.kind == SljitNativeRegionOpKind::FILTER) {
			if (op.filter.kind == SljitNativeRegionExpressionKind::PREDICATE) {
				if (!op.filter.predicate) {
					error = "SLJIT fused perfect hash filter has no predicate body";
					return false;
				}
				if (!BuildFusedPerfectHashPredicateFilters(*op.filter.predicate, current_columns, plan, error)) {
					return false;
				}
			} else {
				auto filter_idx = BuildFusedPerfectHashExpression(op.filter, current_columns, plan, error);
				if (filter_idx == DConstants::INVALID_INDEX) {
					return false;
				}
				plan.filters.push_back(filter_idx);
			}
			continue;
		}
		if (op.kind == SljitNativeRegionOpKind::PROJECTION) {
			vector<idx_t> next_columns;
			next_columns.reserve(op.projections.size());
			for (auto &projection : op.projections) {
				auto expr_idx = BuildFusedPerfectHashExpression(projection, current_columns, plan, error);
				if (expr_idx == DConstants::INVALID_INDEX) {
					return false;
				}
				next_columns.push_back(expr_idx);
			}
			current_columns = std::move(next_columns);
			continue;
		}
		error = "SLJIT fused perfect hash region has unsupported operator before sink";
		return false;
	}

	auto &sink = region.ops.back();
	for (auto &binding : sink.grouped_aggregate_groups) {
		if (binding.input_index >= current_columns.size()) {
			error = "SLJIT fused perfect hash group binding references column outside fused layout";
			return false;
		}
		plan.groups.push_back(current_columns[binding.input_index]);
	}
	plan.aggregate_payloads.resize(sink.native_grouped_aggregate_updates.size(), DConstants::INVALID_INDEX);
	for (idx_t update_idx = 0; update_idx < sink.native_grouped_aggregate_updates.size(); update_idx++) {
		auto &update = sink.native_grouped_aggregate_updates[update_idx];
		if (update.update_kind != JitAggregateUpdateKind::COUNT_STAR) {
			if (update.payload_index >= current_columns.size()) {
				error = "SLJIT fused perfect hash aggregate payload references column outside fused layout";
				return false;
			}
			plan.aggregate_payloads[update_idx] = current_columns[update.payload_index];
		}
		plan.updates.push_back(update);
	}
	plan.required_bits = sink.perfect_hash_required_bits;
	plan.total_required_bits = 0;
	for (auto bits : plan.required_bits) {
		plan.total_required_bits += bits;
	}
	if (plan.total_required_bits >= sizeof(idx_t) * 8) {
		error = "SLJIT fused perfect hash group space exceeds idx_t";
		return false;
	}
	for (auto &minimum : sink.perfect_hash_group_minima) {
		int64_t value;
		if (!TryReadFusedPerfectHashMinimum(minimum, value)) {
			error = "SLJIT fused perfect hash group minimum has unsupported type " + minimum.type().ToString();
			return false;
		}
		plan.group_minima.push_back(value);
	}
	return true;
}

static void EmitStoreFusedPerfectHashExprNull(struct sljit_compiler *compiler, idx_t expr_idx, sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SljitFusedPerfectHashNullOffset(expr_idx), value_reg, 0);
}

static void EmitStoreFusedPerfectHashExprValue(struct sljit_compiler *compiler, idx_t expr_idx, sljit_s32 value_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SljitFusedPerfectHashValueOffset(expr_idx), value_reg, 0);
}

static void EmitLoadFusedPerfectHashExprNull(struct sljit_compiler *compiler, idx_t expr_idx, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM1(SLJIT_SP), SljitFusedPerfectHashNullOffset(expr_idx));
}

static void EmitLoadFusedPerfectHashExprValue(struct sljit_compiler *compiler, idx_t expr_idx, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM1(SLJIT_SP), SljitFusedPerfectHashValueOffset(expr_idx));
}

static bool FusedPerfectHashExprNonNullWithAllValidSources(const SljitFusedPerfectHashAggregatePlan &plan,
                                                           idx_t expr_idx) {
	if (expr_idx == DConstants::INVALID_INDEX || expr_idx >= plan.expressions.size()) {
		return false;
	}
	auto &expr = plan.expressions[expr_idx];
	switch (expr.kind) {
	case SljitFusedPerfectHashExprKind::SOURCE_REF:
		return true;
	case SljitFusedPerfectHashExprKind::STRING_COMPRESS_UINT8:
		return FusedPerfectHashExprNonNullWithAllValidSources(plan, expr.left);
	case SljitFusedPerfectHashExprKind::INTEGER_CAST:
		return !expr.try_cast && FusedPerfectHashExprNonNullWithAllValidSources(plan, expr.left);
	case SljitFusedPerfectHashExprKind::INTEGER_BINARY_CONSTANT:
	case SljitFusedPerfectHashExprKind::INTEGER_COMPARE_CONSTANT:
		return FusedPerfectHashExprNonNullWithAllValidSources(plan, expr.left);
	case SljitFusedPerfectHashExprKind::INTEGER_BINARY_REFERENCES:
	case SljitFusedPerfectHashExprKind::INTEGER_COMPARE_REFERENCES:
		return FusedPerfectHashExprNonNullWithAllValidSources(plan, expr.left) &&
		       FusedPerfectHashExprNonNullWithAllValidSources(plan, expr.right);
	default:
		return false;
	}
}

static bool FusedPerfectHashExprNonNullOnPath(const SljitFusedPerfectHashAggregatePlan &plan, idx_t expr_idx,
                                              SljitFusedPerfectHashVectorPath path) {
	return path != SljitFusedPerfectHashVectorPath::GENERIC &&
	       FusedPerfectHashExprNonNullWithAllValidSources(plan, expr_idx);
}

static void EmitLoadFusedPerfectHashSourceIndex(struct sljit_compiler *compiler, idx_t source_index,
                                                sljit_s32 logical_index, sljit_s32 target,
                                                SljitFusedPerfectHashVectorPath path,
                                                const SljitFusedPerfectHashAggregatePlan &plan) {
	if (path == SljitFusedPerfectHashVectorPath::FLAT_ALL_VALID) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, logical_index, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashSourceSelOffset(plan, source_index));
	if (path == SljitFusedPerfectHashVectorPath::ALL_SELECTED_ALL_VALID) {
		sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 2);
		return;
	}
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_source_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, logical_index, 0);
	auto done = sljit_emit_label(compiler);
	sljit_set_label(have_source_index, done);
}

static struct sljit_jump *EmitJumpIfFusedPerfectHashSourceNull(struct sljit_compiler *compiler, idx_t source_index,
                                                               sljit_s32 index_reg,
                                                               SljitFusedPerfectHashVectorPath path,
                                                               const SljitFusedPerfectHashAggregatePlan &plan) {
	if (path != SljitFusedPerfectHashVectorPath::GENERIC) {
		return nullptr;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashSourceValidityOffset(plan, source_index));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto done = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid, done);
	return source_is_null;
}

static void EmitLoadFusedPerfectHashSourceDataPointer(struct sljit_compiler *compiler, idx_t source_index,
                                                      sljit_s32 target,
                                                      const SljitFusedPerfectHashAggregatePlan &plan) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashSourceDataOffset(plan, source_index));
}

static void EmitLoadFusedPerfectHashSourceInteger(struct sljit_compiler *compiler, idx_t source_index,
                                                  sljit_s32 row_index_reg, sljit_s32 target,
                                                  SljitNativeIntegerKind integer_kind,
                                                  const SljitFusedPerfectHashAggregatePlan &plan) {
	auto data_scale = NativeIntegerDataScale(integer_kind);
	auto load_op = NativeIntegerLoadOp(integer_kind);
	EmitLoadFusedPerfectHashSourceDataPointer(compiler, source_index, SLJIT_R0, plan);
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(SLJIT_R0, row_index_reg), data_scale);
}

static void EmitFusedPerfectHashExpression(struct sljit_compiler *compiler,
                                           const SljitFusedPerfectHashAggregatePlan &plan, idx_t expr_idx,
                                           sljit_s32 row_index_reg, vector<sljit_jump *> &overflow_jumps,
                                           SljitFusedPerfectHashVectorPath path) {
	auto &expr = plan.expressions[expr_idx];
	switch (expr.kind) {
	case SljitFusedPerfectHashExprKind::SOURCE_REF: {
		auto known_valid = FusedPerfectHashExprNonNullOnPath(plan, expr_idx, path);
		EmitLoadFusedPerfectHashSourceIndex(compiler, expr.source_index, row_index_reg, SLJIT_R1, path, plan);
		auto is_null = EmitJumpIfFusedPerfectHashSourceNull(compiler, expr.source_index, SLJIT_R1, path, plan);
		if (expr.return_type.InternalType() == PhysicalType::VARCHAR) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		} else {
			EmitLoadFusedPerfectHashSourceInteger(compiler, expr.source_index, SLJIT_R1, SLJIT_R2, expr.integer_kind,
			                                      plan);
		}
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		if (!known_valid) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
			EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R3);
		}
		if (is_null) {
			auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto null_label = sljit_emit_label(compiler);
			sljit_set_label(is_null, null_label);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
			EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R3);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
			sljit_set_label(done, sljit_emit_label(compiler));
		}
		return;
	}
	case SljitFusedPerfectHashExprKind::STRING_COMPRESS_UINT8: {
		auto known_valid = FusedPerfectHashExprNonNullOnPath(plan, expr_idx, path);
		auto &child = plan.expressions[expr.left];
		if (child.kind != SljitFusedPerfectHashExprKind::SOURCE_REF ||
		    child.return_type.InternalType() != PhysicalType::VARCHAR) {
			throw InternalException("SLJIT fused perfect hash string compression requires a source VARCHAR reference");
		}
		static_assert(sizeof(string_t) == 16, "SLJIT string compression expects DuckDB string_t ABI size");
		static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
		static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
		static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;
		static constexpr sljit_sw STRING_T_SHIFT = 4;
		EmitLoadFusedPerfectHashSourceIndex(compiler, child.source_index, row_index_reg, SLJIT_R1, path, plan);
		auto is_null = EmitJumpIfFusedPerfectHashSourceNull(compiler, child.source_index, SLJIT_R1, path, plan);
		EmitLoadFusedPerfectHashSourceDataPointer(compiler, child.source_index, SLJIT_R0, plan);
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_R1, 0, SLJIT_IMM, STRING_T_SHIFT);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
		auto empty_string = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		auto non_inlined = sljit_emit_cmp(compiler, SLJIT_GREATER, SLJIT_R2, 0, SLJIT_IMM,
		                                  NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0), STRING_INLINE_PREFIX_OFFSET);
		auto have_first_byte = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(non_inlined, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_R0), STRING_POINTER_OFFSET);
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R4), 0);
		sljit_set_label(have_first_byte, sljit_emit_label(compiler));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		auto have_result = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(empty_string, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_set_label(have_result, sljit_emit_label(compiler));
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		if (!known_valid) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 0);
			EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R3);
		}
		if (is_null) {
			auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto null_label = sljit_emit_label(compiler);
			sljit_set_label(is_null, null_label);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, 1);
			EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R3);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
			EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
			sljit_set_label(done, sljit_emit_label(compiler));
		}
		return;
	}
	case SljitFusedPerfectHashExprKind::INTEGER_CAST: {
		vector<sljit_jump *> null_jumps;
		if (!FusedPerfectHashExprNonNullOnPath(plan, expr.left, path)) {
			EmitLoadFusedPerfectHashExprNull(compiler, expr.left, SLJIT_R1);
			null_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
		}
		EmitLoadFusedPerfectHashExprValue(compiler, expr.left, SLJIT_R2);
		if (NativeSignedIntegerCastNeedsRangeCheck(expr.cast_source_width, expr.cast_target_width)) {
			auto range_too_small =
			    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
			                   NumericCast<sljit_sw>(NativeSignedIntegerMin(expr.cast_target_width)));
			auto range_too_large =
			    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
			                   NumericCast<sljit_sw>(NativeSignedIntegerMax(expr.cast_target_width)));
			if (expr.try_cast) {
				null_jumps.push_back(range_too_small);
				null_jumps.push_back(range_too_large);
			} else {
				overflow_jumps.push_back(range_too_small);
				overflow_jumps.push_back(range_too_large);
			}
		}
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		if (!FusedPerfectHashExprNonNullOnPath(plan, expr_idx, path)) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
			EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R1);
		}
		if (null_jumps.empty()) {
			return;
		}
		auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto null_label = sljit_emit_label(compiler);
		for (auto &jump : null_jumps) {
			sljit_set_label(jump, null_label);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 1);
		EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		sljit_set_label(done, sljit_emit_label(compiler));
		return;
	}
	case SljitFusedPerfectHashExprKind::INTEGER_BINARY_CONSTANT:
	case SljitFusedPerfectHashExprKind::INTEGER_BINARY_REFERENCES: {
		vector<sljit_jump *> null_jumps;
		if (!FusedPerfectHashExprNonNullOnPath(plan, expr.left, path)) {
			EmitLoadFusedPerfectHashExprNull(compiler, expr.left, SLJIT_R1);
			null_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
		}
		if (expr.kind == SljitFusedPerfectHashExprKind::INTEGER_BINARY_REFERENCES) {
			if (!FusedPerfectHashExprNonNullOnPath(plan, expr.right, path)) {
				EmitLoadFusedPerfectHashExprNull(compiler, expr.right, SLJIT_R1);
				null_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
			}
		}
		EmitLoadFusedPerfectHashExprValue(compiler, expr.left, SLJIT_R2);
		if (expr.kind == SljitFusedPerfectHashExprKind::INTEGER_BINARY_CONSTANT) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, NumericCast<sljit_sw>(expr.constant));
		} else {
			EmitLoadFusedPerfectHashExprValue(compiler, expr.right, SLJIT_R3);
		}
		auto binary_op = NativeIntegerBinaryOp(expr.integer_kind, expr.binary_op);
		if (expr.binary_op == SljitNativeIntegerBinaryOp::SUBTRACT && expr.constant_on_left) {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		}
		overflow_jumps.push_back(sljit_emit_jump(compiler, SLJIT_OVERFLOW));
		if (expr.check_result_range) {
			overflow_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
			                                        NumericCast<sljit_sw>(expr.result_min)));
			overflow_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
			                                        NumericCast<sljit_sw>(expr.result_max)));
		}
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		if (!FusedPerfectHashExprNonNullOnPath(plan, expr_idx, path)) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
			EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R1);
		}
		if (null_jumps.empty()) {
			return;
		}
		auto done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto null_label = sljit_emit_label(compiler);
		for (auto &jump : null_jumps) {
			sljit_set_label(jump, null_label);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 1);
		EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		sljit_set_label(done, sljit_emit_label(compiler));
		return;
	}
	case SljitFusedPerfectHashExprKind::INTEGER_COMPARE_CONSTANT:
	case SljitFusedPerfectHashExprKind::INTEGER_COMPARE_REFERENCES: {
		vector<sljit_jump *> null_jumps;
		if (!FusedPerfectHashExprNonNullOnPath(plan, expr.left, path)) {
			EmitLoadFusedPerfectHashExprNull(compiler, expr.left, SLJIT_R1);
			null_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
		}
		if (expr.kind == SljitFusedPerfectHashExprKind::INTEGER_COMPARE_REFERENCES) {
			if (!FusedPerfectHashExprNonNullOnPath(plan, expr.right, path)) {
				EmitLoadFusedPerfectHashExprNull(compiler, expr.right, SLJIT_R1);
				null_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
			}
		}
		EmitLoadFusedPerfectHashExprValue(compiler, expr.left, SLJIT_R2);
		if (expr.kind == SljitFusedPerfectHashExprKind::INTEGER_COMPARE_CONSTANT) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_IMM, NumericCast<sljit_sw>(expr.constant));
		} else {
			EmitLoadFusedPerfectHashExprValue(compiler, expr.right, SLJIT_R3);
		}
		auto compare_type = NativeIntegerCompareJumpType(expr.integer_kind, expr.compare_op);
		struct sljit_jump *comparison_true;
		if (expr.constant_on_left && expr.kind == SljitFusedPerfectHashExprKind::INTEGER_COMPARE_CONSTANT) {
			comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		auto known_valid = FusedPerfectHashExprNonNullOnPath(plan, expr_idx, path);
		if (!known_valid) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
			EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R1);
		}
		auto done_false = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto true_label = sljit_emit_label(compiler);
		sljit_set_label(comparison_true, true_label);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 1);
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		if (!known_valid) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 0);
			EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R1);
		}
		auto done_true = sljit_emit_jump(compiler, SLJIT_JUMP);
		if (null_jumps.empty()) {
			auto done_label = sljit_emit_label(compiler);
			sljit_set_label(done_false, done_label);
			sljit_set_label(done_true, done_label);
			return;
		}
		auto null_label = sljit_emit_label(compiler);
		for (auto &jump : null_jumps) {
			sljit_set_label(jump, null_label);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_IMM, 1);
		EmitStoreFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R1);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, 0);
		EmitStoreFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		auto done_label = sljit_emit_label(compiler);
		sljit_set_label(done_false, done_label);
		sljit_set_label(done_true, done_label);
		return;
	}
	default:
		throw InternalException("Unknown SLJIT fused perfect hash expression kind");
	}
}

static void MarkFusedPerfectHashRequiredExpression(const SljitFusedPerfectHashAggregatePlan &plan, idx_t expr_idx,
                                                   vector<bool> &required) {
	if (expr_idx == DConstants::INVALID_INDEX || expr_idx >= plan.expressions.size() || required[expr_idx]) {
		return;
	}
	required[expr_idx] = true;
	auto &expr = plan.expressions[expr_idx];
	if (expr.kind == SljitFusedPerfectHashExprKind::STRING_COMPRESS_UINT8) {
		// String compression reads the source VARCHAR vector directly; its SOURCE_REF child only records the source
		// binding and does not produce a reusable value slot for the generated code.
		return;
	}
	MarkFusedPerfectHashRequiredExpression(plan, expr.left, required);
	MarkFusedPerfectHashRequiredExpression(plan, expr.right, required);
}

static void EmitMarkFusedPerfectHashGroup(struct sljit_compiler *compiler,
                                          const SljitFusedPerfectHashAggregatePlan &plan) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashGroupIsSetOffset(plan));
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S4), 0, SLJIT_IMM, 1);
}

static void EmitStoreFusedPerfectHashAggregateBase(struct sljit_compiler *compiler,
                                                   const SljitFusedPerfectHashAggregatePlan &plan) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashStateDataOffset(plan));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashTupleSizeOffset(plan));
	sljit_emit_op2(compiler, SLJIT_MUL, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_S4, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashAggregateStateOffsetOffset(plan));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashAggregateBaseOffset(plan), SLJIT_R0, 0);
}

static sljit_sw FusedPerfectHashStateValueOffset(const SljitNativeGroupedAggregateUpdatePlan &update) {
	return NumericCast<sljit_sw>(update.aggregate_state_offset + update.state_value_offset);
}

static sljit_sw FusedPerfectHashStateIsSetOffset(const SljitNativeGroupedAggregateUpdatePlan &update) {
	if (update.state_is_set_offset < update.state_value_offset) {
		throw InternalException("SLJIT fused perfect hash sum has invalid optional state layout");
	}
	return NumericCast<sljit_sw>(update.aggregate_state_offset + update.state_is_set_offset);
}

static void EmitLoadFusedPerfectHashAggregateBase(struct sljit_compiler *compiler,
                                                  const SljitFusedPerfectHashAggregatePlan &plan,
                                                  sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashAggregateBaseOffset(plan));
}

static void EmitAddFusedPerfectHashStateInt64AtBase(struct sljit_compiler *compiler,
                                                    const SljitNativeGroupedAggregateUpdatePlan &update,
                                                    sljit_s32 base_reg, sljit_s32 scratch_reg, sljit_s32 value_reg,
                                                    sljit_sw value_offset) {
	auto state_value_offset = FusedPerfectHashStateValueOffset(update);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch_reg, 0, SLJIT_MEM1(base_reg), state_value_offset);
	sljit_emit_op2(compiler, SLJIT_ADD, scratch_reg, 0, scratch_reg, 0, value_reg, value_offset);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(base_reg), state_value_offset, scratch_reg, 0);
}

static void EmitSetFusedPerfectHashStateIsSetAtBase(struct sljit_compiler *compiler,
                                                    const SljitNativeGroupedAggregateUpdatePlan &update,
                                                    sljit_s32 base_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(base_reg), FusedPerfectHashStateIsSetOffset(update), SLJIT_IMM,
	               1);
}

static void EmitAddFusedPerfectHashStateCount(struct sljit_compiler *compiler,
                                              const SljitNativeGroupedAggregateUpdatePlan &update,
                                              sljit_s32 base_reg) {
	if (update.state_type != LogicalType::BIGINT) {
		throw InternalException("SLJIT fused perfect hash count has unsupported state type");
	}
	EmitAddFusedPerfectHashStateInt64AtBase(compiler, update, base_reg, SLJIT_R4, SLJIT_IMM, 1);
}

static void EmitAddFusedPerfectHashStateSum(struct sljit_compiler *compiler,
                                            const SljitNativeGroupedAggregateUpdatePlan &update, sljit_s32 base_reg,
                                            sljit_s32 value_reg) {
	if (update.state_type == LogicalType::BIGINT) {
		EmitAddFusedPerfectHashStateInt64AtBase(compiler, update, base_reg, SLJIT_R4, value_reg, 0);
		EmitSetFusedPerfectHashStateIsSetAtBase(compiler, update, base_reg);
		return;
	}
	if (update.state_type == LogicalType::HUGEINT) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, base_reg, 0, SLJIT_IMM,
		               FusedPerfectHashStateValueOffset(update));
		EmitAddHugeintInt64ToStateValue(compiler, SLJIT_R4, value_reg, SLJIT_R2, SLJIT_R3);
		EmitSetFusedPerfectHashStateIsSetAtBase(compiler, update, base_reg);
		return;
	}
	throw InternalException("SLJIT fused perfect hash sum has unsupported state type");
}

static sljit_jump *EmitFusedDirectPerfectHashLoop(struct sljit_compiler *compiler,
                                                  const SljitFusedPerfectHashAggregatePlan &plan,
                                                  vector<sljit_jump *> &overflow_jumps,
                                                  vector<sljit_jump *> &invalid_group_jumps,
                                                  SljitFusedPerfectHashVectorPath path) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	vector<bool> required_expressions(plan.expressions.size(), false);
	for (auto filter_idx : plan.filters) {
		MarkFusedPerfectHashRequiredExpression(plan, filter_idx, required_expressions);
	}
	for (auto group_idx : plan.groups) {
		MarkFusedPerfectHashRequiredExpression(plan, group_idx, required_expressions);
	}
	vector<bool> evaluated_expressions(plan.expressions.size(), false);
	for (idx_t expr_idx = 0; expr_idx < plan.expressions.size(); expr_idx++) {
		if (required_expressions[expr_idx]) {
			EmitFusedPerfectHashExpression(compiler, plan, expr_idx, SLJIT_S1, overflow_jumps, path);
			evaluated_expressions[expr_idx] = true;
		}
	}

	vector<sljit_jump *> filter_failed;
	for (auto filter_idx : plan.filters) {
		if (!FusedPerfectHashExprNonNullOnPath(plan, filter_idx, path)) {
			EmitLoadFusedPerfectHashExprNull(compiler, filter_idx, SLJIT_R1);
			filter_failed.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
		}
		EmitLoadFusedPerfectHashExprValue(compiler, filter_idx, SLJIT_R1);
		filter_failed.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0));
	}

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	idx_t current_shift = plan.total_required_bits;
	for (idx_t group_idx = 0; group_idx < plan.groups.size(); group_idx++) {
		current_shift -= plan.required_bits[group_idx];
		auto expr_idx = plan.groups[group_idx];
		sljit_jump *group_is_null = nullptr;
		if (!FusedPerfectHashExprNonNullOnPath(plan, expr_idx, path)) {
			EmitLoadFusedPerfectHashExprNull(compiler, expr_idx, SLJIT_R1);
			group_is_null = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0);
		}
		EmitLoadFusedPerfectHashExprValue(compiler, expr_idx, SLJIT_R2);
		sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
		               NumericCast<sljit_sw>(plan.group_minima[group_idx]));
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
		if (current_shift > 0) {
			sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(current_shift));
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S4, 0, SLJIT_S4, 0, SLJIT_R2, 0);
		if (group_is_null) {
			sljit_set_label(group_is_null, sljit_emit_label(compiler));
		}
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP),
	               SljitFusedPerfectHashTotalGroupsOffset(plan));
	invalid_group_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S4, 0, SLJIT_R0, 0));
	EmitMarkFusedPerfectHashGroup(compiler, plan);
	EmitStoreFusedPerfectHashAggregateBase(compiler, plan);

	vector<bool> payload_expressions(plan.expressions.size(), false);
	for (idx_t update_idx = 0; update_idx < plan.updates.size(); update_idx++) {
		if (plan.updates[update_idx].update_kind == JitAggregateUpdateKind::COUNT ||
		    plan.updates[update_idx].update_kind == JitAggregateUpdateKind::SUM) {
			MarkFusedPerfectHashRequiredExpression(plan, plan.aggregate_payloads[update_idx], payload_expressions);
		}
	}
	for (idx_t expr_idx = 0; expr_idx < plan.expressions.size(); expr_idx++) {
		if (payload_expressions[expr_idx] && !evaluated_expressions[expr_idx]) {
			EmitFusedPerfectHashExpression(compiler, plan, expr_idx, SLJIT_S1, overflow_jumps, path);
			evaluated_expressions[expr_idx] = true;
		}
	}
	EmitLoadFusedPerfectHashAggregateBase(compiler, plan, SLJIT_R0);
	for (idx_t update_idx = 0; update_idx < plan.updates.size(); update_idx++) {
		auto &update = plan.updates[update_idx];
		if (update.update_kind == JitAggregateUpdateKind::COUNT_STAR) {
			EmitAddFusedPerfectHashStateCount(compiler, update, SLJIT_R0);
			continue;
		}
		if (update.update_kind != JitAggregateUpdateKind::COUNT && update.update_kind != JitAggregateUpdateKind::SUM) {
			throw InternalException("SLJIT fused perfect hash direct update supports count/count-star/sum only");
		}
		const auto payload_expr = plan.aggregate_payloads[update_idx];
		if (payload_expr == DConstants::INVALID_INDEX) {
			throw InternalException("SLJIT fused perfect hash payload binding is missing");
		}
		sljit_jump *payload_is_null = nullptr;
		if (!FusedPerfectHashExprNonNullOnPath(plan, payload_expr, path)) {
			EmitLoadFusedPerfectHashExprNull(compiler, payload_expr, SLJIT_R1);
			payload_is_null = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R1, 0, SLJIT_IMM, 0);
		}
		if (update.update_kind == JitAggregateUpdateKind::COUNT) {
			EmitAddFusedPerfectHashStateCount(compiler, update, SLJIT_R0);
		} else {
			EmitLoadFusedPerfectHashExprValue(compiler, payload_expr, SLJIT_R1);
			EmitAddFusedPerfectHashStateSum(compiler, update, SLJIT_R0, SLJIT_R1);
		}
		if (payload_is_null) {
			sljit_set_label(payload_is_null, sljit_emit_label(compiler));
		}
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);

	auto next = sljit_emit_label(compiler);
	for (auto &jump : filter_failed) {
		sljit_set_label(jump, next);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);
	return done;
}

static unique_ptr<JitCodeHandle> BuildSljitFusedDirectPerfectHashCode(
    const SljitFusedPerfectHashAggregatePlan &plan, SljitFusedPerfectHashAggregateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto local_size = SljitFusedPerfectHashLocalSize(plan);
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	EmitStoreFusedPerfectHashSourcePointers(compiler, plan);
	EmitStoreFusedPerfectHashStateLayout(compiler, plan);

	vector<sljit_jump *> overflow_jumps;
	vector<sljit_jump *> invalid_group_jumps;
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, flat_all_valid));
	auto use_all_valid_or_generic_path = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto fast_done = EmitFusedDirectPerfectHashLoop(compiler, plan, overflow_jumps, invalid_group_jumps,
	                                                SljitFusedPerfectHashVectorPath::FLAT_ALL_VALID);
	auto all_valid_or_generic_path = sljit_emit_label(compiler);
	sljit_set_label(use_all_valid_or_generic_path, all_valid_or_generic_path);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, all_selected));
	auto use_all_valid_or_generic_after_selected = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto all_selected_done = EmitFusedDirectPerfectHashLoop(compiler, plan, overflow_jumps, invalid_group_jumps,
	                                                        SljitFusedPerfectHashVectorPath::ALL_SELECTED_ALL_VALID);
	auto all_valid_or_generic_after_selected = sljit_emit_label(compiler);
	sljit_set_label(use_all_valid_or_generic_after_selected, all_valid_or_generic_after_selected);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, all_valid));
	auto use_generic_path = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto all_valid_done = EmitFusedDirectPerfectHashLoop(compiler, plan, overflow_jumps, invalid_group_jumps,
	                                                     SljitFusedPerfectHashVectorPath::SELECTION_ALL_VALID);
	auto generic_path = sljit_emit_label(compiler);
	sljit_set_label(use_generic_path, generic_path);
	auto generic_done = EmitFusedDirectPerfectHashLoop(compiler, plan, overflow_jumps, invalid_group_jumps,
	                                                   SljitFusedPerfectHashVectorPath::GENERIC);

	auto invalid_group_label = sljit_emit_label(compiler);
	for (auto &invalid_group : invalid_group_jumps) {
		sljit_set_label(invalid_group, invalid_group_label);
	}
	if (!invalid_group_jumps.empty()) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitFusedPerfectHashAggregateInput, invalid_group_id), SLJIT_S4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitFusedPerfectHashAggregateInvalidGroup));
		sljit_emit_return_void(compiler);
	}

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(fast_done, done_label);
	sljit_set_label(all_selected_done, done_label);
	sljit_set_label(all_valid_done, done_label);
	sljit_set_label(generic_done, done_label);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedPerfectHashAggregateInput, selected_count), SLJIT_S3, 0);
	sljit_emit_return_void(compiler);

	auto overflow_label = sljit_emit_label(compiler);
	for (auto &overflow : overflow_jumps) {
		sljit_set_label(overflow, overflow_label);
	}
	if (!overflow_jumps.empty()) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitFusedPerfectHashAggregateOverflow));
		sljit_emit_return_void(compiler);
	}
	return FinishSljitFusedPerfectHashAggregateCode(compiler, function, error);
}

bool BuildSljitFusedDirectPerfectHashAggregate(const SljitNativeRegionPlan &region, unique_ptr<JitCodeHandle> &code,
                                               SljitFusedPerfectHashAggregateFunction &function,
                                               string &overflow_message, string &error) {
	SljitFusedPerfectHashAggregatePlan plan;
	if (!BuildFusedPerfectHashPlan(region, plan, error)) {
		return false;
	}
	overflow_message = "Overflow in fused perfect hash aggregate expression";
	code = BuildSljitFusedDirectPerfectHashCode(plan, function, error);
	return code && function;
}

static void EmitLoadFusedSourceIndex(struct sljit_compiler *compiler, sljit_sw sel_offset, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), sel_offset);
	auto no_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfFusedSourceNull(struct sljit_compiler *compiler, sljit_sw validity_offset,
                                                    sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), validity_offset);
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R3, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R4, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R3), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R3, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R3, 0, SLJIT_IMM, 1, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(source_all_valid, sljit_emit_label(compiler));
	return source_is_null;
}

static void EmitSetFusedResultRowInvalid(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, result_validity));
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_S3, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_S3, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_XOR, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, -1);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3, SLJIT_R3, 0);
}

unique_ptr<JitCodeHandle> BuildSljitFusedIntegerFilterProjection(
    SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp compare_op, bool compare_constant_on_left,
    SljitNativeIntegerBinaryOp projection_op, bool projection_constant_on_left,
    SljitFusedFilterProjectionFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}
	auto data_scale = NativeIntegerDataScale(kind);
	auto load_op = NativeIntegerLoadOp(kind);
	auto store_op = NativeIntegerStoreOp(kind);
	auto compare_type = NativeIntegerCompareJumpType(kind, compare_op);
	auto binary_op = NativeIntegerBinaryOp(kind, projection_op);

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, input_count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);

	vector<sljit_jump *> generic_path;
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, filter_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, projection_sel));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, filter_validity));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, projection_validity));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, result_validity));
	generic_path.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, filter_data));
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R4, SLJIT_S1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, filter_constant));
	struct sljit_jump *fast_comparison_true;
	if (compare_constant_on_left) {
		fast_comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		fast_comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	}
	auto fast_next_input = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(fast_comparison_true, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, projection_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, projection_constant));
	switch (projection_op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		break;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		if (projection_constant_on_left) {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		}
		break;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		break;
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
	auto fast_overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, result_data));
	sljit_emit_op1(compiler, store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);

	auto fast_next_input_label = sljit_emit_label(compiler);
	sljit_set_label(fast_next_input, fast_next_input_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(fast_done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitFusedFilterProjectionInput, output_count),
	               SLJIT_S3, 0);
	sljit_emit_return_void(compiler);

	sljit_set_label(fast_overflow, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
	                 SLJIT_FUNC_ADDR(SljitFusedFilterProjectionOverflow));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitFusedFilterProjectionInput, output_count),
	               SLJIT_S3, 0);
	sljit_emit_return_void(compiler);

	auto loop = sljit_emit_label(compiler);
	for (auto &jump : generic_path) {
		sljit_set_label(jump, loop);
	}
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadFusedSourceIndex(compiler, offsetof(SljitFusedFilterProjectionInput, filter_sel), SLJIT_R1);
	auto filter_is_null =
	    EmitJumpIfFusedSourceNull(compiler, offsetof(SljitFusedFilterProjectionInput, filter_validity), SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, filter_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, filter_constant));
	struct sljit_jump *comparison_true;
	if (compare_constant_on_left) {
		comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R3, 0, SLJIT_R2, 0);
	} else {
		comparison_true = sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0);
	}
	auto next_input = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(comparison_true, sljit_emit_label(compiler));
	EmitLoadFusedSourceIndex(compiler, offsetof(SljitFusedFilterProjectionInput, projection_sel), SLJIT_R1);
	auto projection_is_null =
	    EmitJumpIfFusedSourceNull(compiler, offsetof(SljitFusedFilterProjectionInput, projection_validity), SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, projection_data));
	sljit_emit_op1(compiler, load_op, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R1), data_scale);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, projection_constant));
	switch (projection_op) {
	case SljitNativeIntegerBinaryOp::ADD:
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		break;
	case SljitNativeIntegerBinaryOp::SUBTRACT:
		if (projection_constant_on_left) {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		}
		break;
	case SljitNativeIntegerBinaryOp::MULTIPLY:
		sljit_emit_op2(compiler, binary_op | SLJIT_SET_OVERFLOW, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
		break;
	default:
		throw InternalException("Unknown SLJIT native integer binary operator");
	}
	auto overflow = sljit_emit_jump(compiler, SLJIT_OVERFLOW);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitFusedFilterProjectionInput, result_data));
	sljit_emit_op1(compiler, store_op, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), data_scale, SLJIT_R2, 0);
	auto increment_output = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(projection_is_null, sljit_emit_label(compiler));
	EmitSetFusedResultRowInvalid(compiler);

	auto increment_output_label = sljit_emit_label(compiler);
	sljit_set_label(increment_output, increment_output_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S3, 0, SLJIT_S3, 0, SLJIT_IMM, 1);

	auto next_input_label = sljit_emit_label(compiler);
	sljit_set_label(next_input, next_input_label);
	sljit_set_label(filter_is_null, next_input_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	auto overflow_label = sljit_emit_label(compiler);
	sljit_set_label(overflow, overflow_label);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM,
	                 SLJIT_FUNC_ADDR(SljitFusedFilterProjectionOverflow));

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitFusedFilterProjectionInput, output_count),
	               SLJIT_S3, 0);
	sljit_emit_return_void(compiler);

	return FinishSljitFusedFilterProjectionCode(compiler, function, error);
}

} // namespace duckdb
