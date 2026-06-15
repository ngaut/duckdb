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

} // namespace duckdb
