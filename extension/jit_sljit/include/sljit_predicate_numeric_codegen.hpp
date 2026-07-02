//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_numeric_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {

struct SljitPredicateBranches;

static void EmitLoadPredicateSourceData(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                        sljit_s32 index_reg, sljit_sw data_scale, sljit_s32 load_op);

static void EmitScalePredicateDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                            double scale, sljit_s32 target) {
	if (!NativeDoubleSourceHasDecimalScale(kind)) {
		return;
	}
	sljit_emit_fset64(compiler, SLJIT_FR2, scale);
	sljit_emit_fop2(compiler, SLJIT_DIV_F64, target, 0, target, 0, SLJIT_FR2, 0);
}

static bool PredicateDoubleSourceIsSinglePrecision(SljitNativeDoubleSourceKind kind) {
	return kind == SljitNativeDoubleSourceKind::FLOAT;
}

static void EmitLoadPredicateDoubleOperand(struct sljit_compiler *compiler, SljitNativeDoubleSourceKind kind,
                                           idx_t source_index, sljit_s32 index_reg, double scale, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	switch (kind) {
	case SljitNativeDoubleSourceKind::FLOAT:
		sljit_emit_fmem(compiler, SLJIT_MOV_F32 | SLJIT_MEM_ALIGNED_16, target, SLJIT_MEM2(SLJIT_R0, index_reg), 2);
		return;
	case SljitNativeDoubleSourceKind::DOUBLE:
		sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, target, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		return;
	case SljitNativeDoubleSourceKind::INT64_TO_DOUBLE:
	case SljitNativeDoubleSourceKind::DECIMAL64_TO_DOUBLE:
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
		sljit_emit_fop1(compiler, SLJIT_CONV_F64_FROM_SW, target, 0, SLJIT_R2, 0);
		EmitScalePredicateDoubleOperand(compiler, kind, scale, target);
		return;
	case SljitNativeDoubleSourceKind::INT128_TO_DOUBLE:
	case SljitNativeDoubleSourceKind::DECIMAL128_TO_DOUBLE:
		sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 4);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R0, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R2),
		               NumericCast<sljit_sw>(offsetof(hugeint_t, lower)));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_MEM1(SLJIT_R2),
		               NumericCast<sljit_sw>(offsetof(hugeint_t, upper)));
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS2(F64, W, W), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeHugeintToDouble));
		sljit_emit_fop1(compiler, SLJIT_MOV_F64, target, 0, SLJIT_RETURN_FREG, 0);
		EmitScalePredicateDoubleOperand(compiler, kind, scale, target);
		return;
	default:
		throw InternalException("Unknown SLJIT native predicate double source kind");
	}
}

static sljit_sw SljitUInt64Immediate(uint64_t value) {
	return static_cast<sljit_sw>(value);
}

static SljitNativeIntegerCompareOp FlipNativeIntegerCompareOp(SljitNativeIntegerCompareOp op) {
	switch (op) {
	case SljitNativeIntegerCompareOp::LESS_THAN:
		return SljitNativeIntegerCompareOp::GREATER_THAN;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		return SljitNativeIntegerCompareOp::LESS_THAN;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		return SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		return SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL;
	default:
		return op;
	}
}

static sljit_s32 NativeDoubleCompareJumpType(SljitNativeIntegerCompareOp op) {
	switch (op) {
	case SljitNativeIntegerCompareOp::EQUAL:
		return SLJIT_ORDERED_EQUAL;
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		return SLJIT_ORDERED_NOT_EQUAL;
	case SljitNativeIntegerCompareOp::LESS_THAN:
		return SLJIT_ORDERED_LESS;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		return SLJIT_ORDERED_GREATER;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		return SLJIT_ORDERED_LESS_EQUAL;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		return SLJIT_ORDERED_GREATER_EQUAL;
	default:
		throw InternalException("Unknown SLJIT native double predicate comparison operator");
	}
}

static void EmitLoadPredicateInt128SourceWord(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                              sljit_s32 index_reg, sljit_sw word_offset, sljit_s32 scratch) {
	static_assert(sizeof(hugeint_t) == 16, "SLJIT INT128 predicate lowering expects DuckDB hugeint_t ABI size");
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, index_reg, 0, SLJIT_IMM, 4);
	if (word_offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, scratch, 0, scratch, 0, SLJIT_IMM, word_offset);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM2(SLJIT_R0, scratch), 0);
}

static void EmitSljitInt128CompareBranches(struct sljit_compiler *compiler, SljitNativeIntegerCompareOp op,
                                           sljit_s32 left_lower, sljit_sw left_lowerw, sljit_s32 left_upper,
                                           sljit_sw left_upperw, sljit_s32 right_lower, sljit_sw right_lowerw,
                                           sljit_s32 right_upper, sljit_sw right_upperw,
                                           SljitPredicateBranches &result) {
	switch (op) {
	case SljitNativeIntegerCompareOp::EQUAL:
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_EQUAL, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::NOT_EQUAL:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::LESS_THAN:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, left_upper, left_upperw, right_upper, right_upperw));
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_LESS, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::GREATER_THAN:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, left_upper, left_upperw, right_upper, right_upperw));
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_GREATER, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::LESS_THAN_OR_EQUAL:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, left_upper, left_upperw, right_upper, right_upperw));
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	case SljitNativeIntegerCompareOp::GREATER_THAN_OR_EQUAL:
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, left_upper, left_upperw, right_upper, right_upperw));
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_SIG_LESS, left_upper, left_upperw, right_upper, right_upperw));
		result.true_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, left_lower, left_lowerw, right_lower, right_lowerw));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	default:
		throw InternalException("Unknown SLJIT INT128 predicate comparison operator");
	}
}

} // namespace duckdb
