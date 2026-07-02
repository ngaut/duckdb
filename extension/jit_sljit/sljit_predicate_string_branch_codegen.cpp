//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_string_branch_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_predicate_codegen_helpers.hpp"

#include "sljit_predicate_numeric_codegen.hpp"
#include "sljit_predicate_string_codegen.hpp"

#include "duckdb/common/helper.hpp"

#include <algorithm>

namespace duckdb {

bool TryEmitSljitStringPredicateBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                         vector<shared_ptr<void>> &owned_data, const vector<bool> &source_not_null,
                                         bool sources_all_valid, SljitPredicateBranches &result) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT:
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result, source_not_null, sources_all_valid);
		EmitStringEqualBranches(compiler, predicate.string_constant, result);
		return true;
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT:
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result, source_not_null, sources_all_valid);
		EmitStringInListBranches(compiler, predicate, result, owned_data);
		return true;
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT:
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result, source_not_null, sources_all_valid);
		EmitStringPrefixBranches(compiler, predicate.string_constant, result);
		return true;
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT:
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result, source_not_null, sources_all_valid);
		EmitStringSuffixBranches(compiler, predicate.string_constant, result);
		return true;
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT:
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result, source_not_null, sources_all_valid);
		EmitStringContainsBranches(compiler, predicate.string_constant, result);
		return true;
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result, source_not_null, sources_all_valid);
		auto pattern = AddStringConstantCodeData(owned_data, predicate.string_constant);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_IMM, CastPointerToValue(pattern));
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS3(W, P, W, P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeStringLikePercentOnly));
		result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_RETURN_REG, 0, SLJIT_IMM, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return true;
	}
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT: {
		static_assert(sizeof(string_t) == 16, "SLJIT string substring expects DuckDB string_t ABI size");
		static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
		static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
		const auto substring_length = predicate.substring_length;
		const auto inline_length = std::min<idx_t>(substring_length, string_t::PREFIX_LENGTH);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateStringPointer(compiler, predicate.source_index, SLJIT_R0, SLJIT_R1);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(substring_length)));
		if (substring_length == 0) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return true;
		}
		if (substring_length > string_t::PREFIX_LENGTH) {
			EmitLoadPredicateStringDataPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4);
		}
		for (auto &constant : predicate.string_constants) {
			vector<sljit_jump *> mismatch_jumps;
			if (inline_length > 0) {
				EmitStringBytesEqualAtFixedOffset(compiler, constant, 0, inline_length, SLJIT_R0,
				                                  STRING_INLINE_PREFIX_OFFSET, mismatch_jumps);
			}
			if (substring_length > inline_length) {
				EmitStringBytesEqualAtFixedOffset(compiler, constant, inline_length, substring_length - inline_length,
				                                  SLJIT_R4, NumericCast<sljit_sw>(inline_length), mismatch_jumps);
			}
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			SetPredicateJumpLabels(mismatch_jumps, sljit_emit_label(compiler));
		}
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return true;
	}
	default:
		return false;
	}
}

} // namespace duckdb
