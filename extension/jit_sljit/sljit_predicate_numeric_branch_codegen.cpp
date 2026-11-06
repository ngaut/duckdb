//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_numeric_branch_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_predicate_codegen_helpers.hpp"

#include "sljit_predicate_numeric_codegen.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/value.hpp"

#include <cstddef>

namespace duckdb {

//! Branches on `FR0 op FR1` in DuckDB's order (see SljitDoubleCompareRanks). When the right operand is known not to be
//! NaN one comparison decides; otherwise an unordered pair compares the operands' ranks. Clobbers R2 and R3
static void EmitSljitDoubleCompareBranches(struct sljit_compiler *compiler, SljitNativeIntegerCompareOp op,
                                           bool single_precision, bool right_is_number,
                                           SljitPredicateBranches &result) {
	const sljit_s32 precision = single_precision ? SLJIT_32 : 0;
	if (right_is_number) {
		result.true_jumps.push_back(sljit_emit_fcmp(compiler, NativeDoubleCompareAgainstNumberJumpType(op) | precision,
		                                            SLJIT_FR0, 0, SLJIT_FR1, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	auto ordered = sljit_emit_fcmp(compiler, SLJIT_ORDERED | precision, SLJIT_FR0, 0, SLJIT_FR1, 0);
	sljit_emit_fop1(compiler, SLJIT_CMP_F64 | precision | SLJIT_SET_UNORDERED, SLJIT_FR0, 0, SLJIT_FR0, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_UNORDERED);
	sljit_emit_fop1(compiler, SLJIT_CMP_F64 | precision | SLJIT_SET_UNORDERED, SLJIT_FR1, 0, SLJIT_FR1, 0);
	sljit_emit_op_flags(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_UNORDERED);
	result.true_jumps.push_back(sljit_emit_cmp(
	    compiler, NativeIntegerCompareJumpType(SljitNativeIntegerKind::INT64, op), SLJIT_R2, 0, SLJIT_R3, 0));
	result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	sljit_set_label(ordered, sljit_emit_label(compiler));
	result.true_jumps.push_back(
	    sljit_emit_fcmp(compiler, NativeDoubleOrderedCompareJumpType(op) | precision, SLJIT_FR0, 0, SLJIT_FR1, 0));
	result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

bool TryEmitSljitNumericPredicateBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                          const vector<bool> &source_not_null, bool sources_all_valid,
                                          SljitPredicateBranches &result) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		if (predicate.constant_on_left) {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_IMM,
			                                           NumericCast<sljit_sw>(predicate.constant), SLJIT_R2, 0));
		} else {
			result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_IMM,
			                                           NumericCast<sljit_sw>(predicate.constant)));
		}
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return true;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_REFERENCES: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		AppendPredicateSourceNullJump(compiler, result, predicate.right_source_index, SLJIT_S4, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		EmitLoadPredicateSourceData(compiler, predicate.right_source_index, SLJIT_R3, SLJIT_S4, data_scale, load_op);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return true;
	}
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT: {
		auto single_precision = PredicateDoubleSourceIsSinglePrecision(predicate.double_source_kind);
		// the value is the left operand
		auto compare_op =
		    predicate.constant_on_left ? FlipNativeIntegerCompareOp(predicate.compare_op) : predicate.compare_op;
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
		                               predicate.double_source_decimal_scale, SLJIT_FR0);
		if (single_precision) {
			sljit_emit_fset32(compiler, SLJIT_FR1, static_cast<sljit_f32>(predicate.double_constant));
		} else {
			sljit_emit_fset64(compiler, SLJIT_FR1, predicate.double_constant);
		}
		EmitSljitDoubleCompareBranches(compiler, compare_op, single_precision, !Value::IsNan(predicate.double_constant),
		                               result);
		return true;
	}
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES: {
		static constexpr sljit_sw LEFT_SPILL_OFFSET = SLJIT_SELECT_LOCAL_SIZE;
		static constexpr sljit_sw RIGHT_SPILL_OFFSET = SLJIT_SELECT_LOCAL_SIZE + sizeof(double);
		auto single_precision = PredicateDoubleSourceIsSinglePrecision(predicate.double_source_kind);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		AppendPredicateSourceNullJump(compiler, result, predicate.right_source_index, SLJIT_S4, source_not_null,
		                              sources_all_valid);
		if (NativeDoubleSourceUsesHelper(predicate.double_source_kind) ||
		    NativeDoubleSourceUsesHelper(predicate.double_right_source_kind)) {
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
			                               predicate.double_source_decimal_scale, SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), LEFT_SPILL_OFFSET);
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_right_source_kind, predicate.right_source_index,
			                               SLJIT_S4, predicate.double_right_source_decimal_scale, SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), RIGHT_SPILL_OFFSET);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR0, SLJIT_MEM1(SLJIT_SP),
			                LEFT_SPILL_OFFSET);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_SP),
			                RIGHT_SPILL_OFFSET);
		} else {
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
			                               predicate.double_source_decimal_scale, SLJIT_FR0);
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_right_source_kind, predicate.right_source_index,
			                               SLJIT_S4, predicate.double_right_source_decimal_scale, SLJIT_FR1);
		}
		EmitSljitDoubleCompareBranches(compiler, predicate.compare_op, single_precision, false, result);
		return true;
	}
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT: {
		auto compare_op =
		    predicate.constant_on_left ? FlipNativeIntegerCompareOp(predicate.compare_op) : predicate.compare_op;
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1,
		                                  offsetof(hugeint_t, lower), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R3, SLJIT_R1,
		                                  offsetof(hugeint_t, upper), SLJIT_R4);
		EmitSljitInt128CompareBranches(compiler, compare_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_IMM,
		                               SljitUInt64Immediate(predicate.int128_constant_lower), SLJIT_IMM,
		                               NumericCast<sljit_sw>(predicate.int128_constant_upper), result);
		return true;
	}
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES:
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		AppendPredicateSourceNullJump(compiler, result, predicate.right_source_index, SLJIT_S4, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1,
		                                  offsetof(hugeint_t, lower), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R3, SLJIT_R1,
		                                  offsetof(hugeint_t, upper), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.right_source_index, SLJIT_R4, SLJIT_S4,
		                                  offsetof(hugeint_t, lower), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.right_source_index, SLJIT_S4, SLJIT_S4,
		                                  offsetof(hugeint_t, upper), SLJIT_R1);
		EmitSljitInt128CompareBranches(compiler, predicate.compare_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R4, 0, SLJIT_S4,
		                               0, result);
		return true;
	case SljitNativePredicateKind::INTEGER_IN_LIST: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		for (auto constant : predicate.constants) {
			auto match = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant));
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
		return true;
	}
	case SljitNativePredicateKind::INTEGER_BETWEEN: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto lower_failure = NativeIntegerLowerBoundFailureJump(predicate.integer_kind, predicate.lower_inclusive);
		auto upper_failure = NativeIntegerUpperBoundFailureJump(predicate.integer_kind, predicate.upper_inclusive);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		auto lower_failed =
		    sljit_emit_cmp(compiler, lower_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(predicate.lower));
		auto upper_failed =
		    sljit_emit_cmp(compiler, upper_failure, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(predicate.upper));
		if (predicate.not_between) {
			result.true_jumps.push_back(lower_failed);
			result.true_jumps.push_back(upper_failed);
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(lower_failed);
			result.false_jumps.push_back(upper_failed);
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return true;
	}
	default:
		return false;
	}
}

} // namespace duckdb
