//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_string_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_internal.hpp"
#include "sljit_native_codegen.hpp"
#include "sljit_predicate_string_runtime.hpp"
#include "sljit_string_chunk_codegen.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

struct SljitPredicateBranches;

static void SetPredicateJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label);
static void EmitLoadPredicateSourceIndex(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 logical_index,
                                         sljit_s32 target);
static void AppendPredicateSourceNullJump(struct sljit_compiler *compiler, SljitPredicateBranches &result,
                                          idx_t source_index, sljit_s32 index_reg, const vector<bool> &source_not_null,
                                          bool sources_all_valid);

static void EmitLoadPredicateStringPointer(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                           sljit_s32 index_reg) {
	static_assert(sizeof(string_t) == 16, "SLJIT string prefix expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_T_SHIFT = 4;
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(target), SljitPointerArrayOffset(source_index));
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, index_reg, 0, SLJIT_IMM, STRING_T_SHIFT);
	sljit_emit_op2(compiler, SLJIT_ADD, target, 0, target, 0, SLJIT_R4, 0);
}

static void EmitLoadPredicateStringDataPointer(struct sljit_compiler *compiler, sljit_s32 string_pointer_reg,
                                               sljit_s32 string_length_reg, sljit_s32 target_reg) {
	static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
	static constexpr sljit_sw STRING_POINTER_OFFSET = sizeof(uint32_t) + string_t::PREFIX_BYTES;
	auto non_inlined = sljit_emit_cmp(compiler, SLJIT_GREATER, string_length_reg, 0, SLJIT_IMM,
	                                  NumericCast<sljit_sw>(string_t::INLINE_LENGTH));
	sljit_emit_op2(compiler, SLJIT_ADD, target_reg, 0, string_pointer_reg, 0, SLJIT_IMM, STRING_INLINE_PREFIX_OFFSET);
	auto have_data = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(non_inlined, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target_reg, 0, SLJIT_MEM1(string_pointer_reg), STRING_POINTER_OFFSET);
	sljit_set_label(have_data, sljit_emit_label(compiler));
}

static void EmitLoadPredicateStringForMatch(struct sljit_compiler *compiler, idx_t source_index,
                                            SljitPredicateBranches &result, const vector<bool> &source_not_null,
                                            bool sources_all_valid) {
	static_assert(sizeof(string_t) == 16, "SLJIT string match expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	EmitLoadPredicateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
	AppendPredicateSourceNullJump(compiler, result, source_index, SLJIT_R1, source_not_null, sources_all_valid);
	EmitLoadPredicateStringPointer(compiler, source_index, SLJIT_R0, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
	EmitLoadPredicateStringDataPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4);
}

static void EmitStringBytesEqualAtPosition(struct sljit_compiler *compiler, const string &constant,
                                           idx_t constant_offset, idx_t compare_length, sljit_s32 data_reg,
                                           sljit_sw data_offset, sljit_s32 position_reg,
                                           vector<sljit_jump *> &mismatch_jumps) {
	if (data_reg == SLJIT_R0) {
		throw InternalException("SLJIT variable-position string comparison cannot use R0 as its data base");
	}
	for (idx_t byte_idx = 0; byte_idx < compare_length;) {
		const auto chunk_size = SljitStringCompareChunkSize(compare_length - byte_idx);
		const auto source_offset = data_offset + NumericCast<sljit_sw>(byte_idx);
		if (source_offset == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, position_reg, 0);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, position_reg, 0, SLJIT_IMM, source_offset);
		}
		sljit_emit_mem(compiler, SljitStringChunkLoadOp(chunk_size) | SLJIT_MEM_UNALIGNED, SLJIT_R3,
		               SLJIT_MEM2(data_reg, SLJIT_R0), 0);
		mismatch_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                   SljitStringChunkImmediate(constant, constant_offset + byte_idx, chunk_size)));
		byte_idx += chunk_size;
	}
}

static void EmitStringBytesEqualAtFixedOffset(struct sljit_compiler *compiler, const string &constant,
                                              idx_t constant_offset, idx_t compare_length, sljit_s32 data_reg,
                                              sljit_sw data_offset, vector<sljit_jump *> &mismatch_jumps) {
	for (idx_t byte_idx = 0; byte_idx < compare_length;) {
		const auto chunk_size = SljitStringCompareChunkSize(compare_length - byte_idx);
		sljit_emit_mem(compiler, SljitStringChunkLoadOp(chunk_size) | SLJIT_MEM_UNALIGNED, SLJIT_R3,
		               SLJIT_MEM1(data_reg), data_offset + NumericCast<sljit_sw>(byte_idx));
		mismatch_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R3, 0, SLJIT_IMM,
		                   SljitStringChunkImmediate(constant, constant_offset + byte_idx, chunk_size)));
		byte_idx += chunk_size;
	}
}

static void EmitStringEqualsAtPosition(struct sljit_compiler *compiler, const string &constant, sljit_s32 data_reg,
                                       sljit_s32 position_reg, vector<sljit_jump *> &mismatch_jumps) {
	EmitStringBytesEqualAtPosition(compiler, constant, 0, constant.size(), data_reg, 0, position_reg, mismatch_jumps);
}

static void EmitStringPrefixBranches(struct sljit_compiler *compiler, const string &prefix,
                                     SljitPredicateBranches &result) {
	const auto prefix_length = prefix.size();
	result.false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(prefix_length)));
	if (prefix_length == 0) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitStringEqualsAtPosition(compiler, prefix, SLJIT_R4, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitStringEqualBranches(struct sljit_compiler *compiler, const string &constant,
                                    SljitPredicateBranches &result) {
	const auto constant_length = constant.size();
	result.false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant_length)));
	if (constant_length == 0) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitStringEqualsAtPosition(compiler, constant, SLJIT_R4, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static const SljitNativeStringConstant *AddStringConstantCodeData(vector<shared_ptr<void>> &owned_data,
                                                                  const string &value) {
	auto data = make_shared_ptr<SljitNativeStringConstant>(value);
	auto result = data.get();
	owned_data.push_back(std::move(data));
	return result;
}

static const SljitNativeStringConstantList *AddStringConstantListCodeData(vector<shared_ptr<void>> &owned_data,
                                                                          const vector<string> &values) {
	auto data = make_shared_ptr<SljitNativeStringConstantList>(values);
	auto result = data.get();
	owned_data.push_back(std::move(data));
	return result;
}

static bool StringInListShouldUseHelper(const SljitNativePredicate &predicate) {
	static constexpr idx_t INLINE_STRING_IN_LIST_LIMIT = 2;
	return predicate.string_constants.size() > INLINE_STRING_IN_LIST_LIMIT;
}

static void EmitStringInListInlineBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                           SljitPredicateBranches &result) {
	for (auto &constant : predicate.string_constants) {
		vector<sljit_jump *> next_candidate_jumps;
		next_candidate_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(constant.size())));
		if (!constant.empty()) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
			EmitStringEqualsAtPosition(compiler, constant, SLJIT_R4, SLJIT_S4, next_candidate_jumps);
		}
		if (predicate.not_in) {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		SetPredicateJumpLabels(next_candidate_jumps, sljit_emit_label(compiler));
	}
	if (predicate.list_has_null) {
		result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	} else if (predicate.not_in) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	} else {
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}
}

static void EmitStringInListHelperBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                           SljitPredicateBranches &result, vector<shared_ptr<void>> &owned_data) {
	auto constants = AddStringConstantListCodeData(owned_data, predicate.string_constants);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_IMM, CastPointerToValue(constants));
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS3(W, P, W, P), SLJIT_IMM,
	                 SLJIT_FUNC_ADDR(SljitNativeStringInListConstant));
	auto match = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_RETURN_REG, 0, SLJIT_IMM, 0);
	if (predicate.not_in) {
		result.false_jumps.push_back(match);
	} else {
		result.true_jumps.push_back(match);
	}
	if (predicate.list_has_null) {
		result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	} else if (predicate.not_in) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	} else {
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}
}

static void EmitStringInListBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                     SljitPredicateBranches &result, vector<shared_ptr<void>> &owned_data) {
	if (StringInListShouldUseHelper(predicate)) {
		EmitStringInListHelperBranches(compiler, predicate, result, owned_data);
		return;
	}
	EmitStringInListInlineBranches(compiler, predicate, result);
}

static void EmitStringSuffixBranches(struct sljit_compiler *compiler, const string &suffix,
                                     SljitPredicateBranches &result) {
	const auto suffix_length = suffix.size();
	result.false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(suffix_length)));
	if (suffix_length == 0) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_S4, 0, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(suffix_length));
	EmitStringEqualsAtPosition(compiler, suffix, SLJIT_R4, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

static void EmitStringFindFromCurrentPosition(struct sljit_compiler *compiler, const string &needle, sljit_s32 data_reg,
                                              sljit_s32 length_reg, sljit_s32 position_reg,
                                              vector<sljit_jump *> &false_jumps) {
	const auto needle_length = needle.size();
	if (needle_length == 0) {
		return;
	}
	false_jumps.push_back(
	    sljit_emit_cmp(compiler, SLJIT_LESS, length_reg, 0, SLJIT_IMM, NumericCast<sljit_sw>(needle_length)));
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R1, 0, length_reg, 0, SLJIT_IMM, NumericCast<sljit_sw>(needle_length));
	false_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_GREATER, position_reg, 0, SLJIT_R1, 0));
	auto loop = sljit_emit_label(compiler);
	vector<sljit_jump *> mismatch_jumps;
	EmitStringEqualsAtPosition(compiler, needle, data_reg, position_reg, mismatch_jumps);
	auto found = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto next_position = sljit_emit_label(compiler);
	SetPredicateJumpLabels(mismatch_jumps, next_position);
	sljit_emit_op2(compiler, SLJIT_ADD, position_reg, 0, position_reg, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_cmp(compiler, SLJIT_LESS_EQUAL, position_reg, 0, SLJIT_R1, 0);
	false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	auto found_label = sljit_emit_label(compiler);
	sljit_set_label(repeat, loop);
	sljit_set_label(found, found_label);
	sljit_emit_op2(compiler, SLJIT_ADD, position_reg, 0, position_reg, 0, SLJIT_IMM,
	               NumericCast<sljit_sw>(needle_length));
}

static void EmitStringContainsBranches(struct sljit_compiler *compiler, const string &needle,
                                       SljitPredicateBranches &result) {
	if (needle.empty()) {
		result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S4, 0, SLJIT_IMM, 0);
	EmitStringFindFromCurrentPosition(compiler, needle, SLJIT_R4, SLJIT_R2, SLJIT_S4, result.false_jumps);
	result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
}

} // namespace duckdb
