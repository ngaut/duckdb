//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>

namespace duckdb {

struct SljitNativeStringConstant {
	explicit SljitNativeStringConstant(string value_p) : value(std::move(value_p)) {
	}

	string value;
};

struct SljitNativeStringConstantList {
	explicit SljitNativeStringConstantList(vector<string> values_p) : values(std::move(values_p)) {
	}

	vector<string> values;
};

static sljit_sw SLJIT_FUNC SljitNativeStringLikePercentOnly(const char *sdata, idx_t slen,
                                                            const SljitNativeStringConstant *pattern) {
	const auto &pattern_value = pattern->value;
	const auto pdata = pattern_value.data();
	const auto plen = pattern_value.size();
	const bool anchor_start = plen == 0 || pdata[0] != '%';
	const bool anchor_end = plen == 0 || pdata[plen - 1] != '%';
	idx_t position = 0;
	idx_t pattern_idx = 0;
	bool saw_fragment = false;

	while (pattern_idx < plen) {
		while (pattern_idx < plen && pdata[pattern_idx] == '%') {
			pattern_idx++;
		}
		auto fragment_start = pattern_idx;
		while (pattern_idx < plen && pdata[pattern_idx] != '%') {
			pattern_idx++;
		}
		auto fragment_length = pattern_idx - fragment_start;
		if (fragment_length == 0) {
			continue;
		}
		const bool first_fragment = !saw_fragment;
		const bool last_fragment = pattern_idx == plen;
		saw_fragment = true;

		if (first_fragment && anchor_start) {
			if (slen < fragment_length || memcmp(sdata, pdata + fragment_start, fragment_length) != 0) {
				return false;
			}
			position = fragment_length;
			if (last_fragment && anchor_end) {
				return position == slen;
			}
			continue;
		}
		if (last_fragment && anchor_end) {
			if (slen < fragment_length) {
				return false;
			}
			auto suffix_position = slen - fragment_length;
			return suffix_position >= position &&
			       memcmp(sdata + suffix_position, pdata + fragment_start, fragment_length) == 0;
		}

		bool found = false;
		while (position + fragment_length <= slen) {
			if (memcmp(sdata + position, pdata + fragment_start, fragment_length) == 0) {
				position += fragment_length;
				found = true;
				break;
			}
			position++;
		}
		if (!found) {
			return false;
		}
	}
	return saw_fragment || !anchor_start || !anchor_end || slen == 0;
}

static sljit_sw SLJIT_FUNC SljitNativeStringInListConstant(const char *sdata, idx_t slen,
                                                           const SljitNativeStringConstantList *list) {
	for (auto &constant : list->values) {
		if (constant.empty() && slen == 0) {
			return true;
		}
		if (constant.size() == slen && memcmp(sdata, constant.data(), slen) == 0) {
			return true;
		}
	}
	return false;
}

static unique_ptr<ExecutionRegionCodeHandle> FinishSljitNativePredicateCode(struct sljit_compiler *compiler,
                                                                            SljitNativePredicateFunction &function,
                                                                            string &error,
                                                                            vector<shared_ptr<void>> owned_data = {}) {
	auto compiler_error = sljit_get_compiler_error(compiler);
	if (compiler_error != SLJIT_SUCCESS) {
		error = "SLJIT compiler failed with error code " + std::to_string(compiler_error);
		sljit_free_compiler(compiler);
		return nullptr;
	}

	auto code = GenerateSljitCode(compiler);
	auto code_size = LossyNumericCast<idx_t>(sljit_get_generated_code_size(compiler));
	sljit_free_compiler(compiler);
	if (!code) {
		error = "SLJIT executable code generation failed";
		return nullptr;
	}

	function = reinterpret_cast<SljitNativePredicateFunction>(code);
	return MakeSljitCodeHandle(code, code_size, std::move(owned_data));
}

struct SljitPredicateBranches {
	vector<sljit_jump *> true_jumps;
	vector<sljit_jump *> false_jumps;
	vector<sljit_jump *> null_jumps;
};

static void AppendPredicateJumps(vector<sljit_jump *> &target, vector<sljit_jump *> source) {
	for (auto &jump : source) {
		target.push_back(jump);
	}
}

static void AppendPredicateBranches(SljitPredicateBranches &target, SljitPredicateBranches source) {
	AppendPredicateJumps(target.true_jumps, std::move(source.true_jumps));
	AppendPredicateJumps(target.false_jumps, std::move(source.false_jumps));
	AppendPredicateJumps(target.null_jumps, std::move(source.null_jumps));
}

static void SetPredicateJumpLabels(const vector<sljit_jump *> &jumps, sljit_label *label) {
	for (auto &jump : jumps) {
		sljit_set_label(jump, label);
	}
}

static void EmitLoadPredicateLogicalIndex(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_logical_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_logical_index, sljit_emit_label(compiler));
}

static void EmitLoadPredicateResultAndLogicalIndexForSelect(struct sljit_compiler *compiler) {
	EmitLoadPredicateLogicalIndex(compiler, SLJIT_S3);
}

static void EmitLoadPredicateResultIndex(struct sljit_compiler *compiler, sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, execute_sel));
	auto no_execute_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	auto have_result_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_execute_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_result_index, sljit_emit_label(compiler));
}

static void EmitLoadPredicateSourceIndex(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 logical_index,
                                         sljit_s32 target) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(SLJIT_R0, logical_index), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_source_sel, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, logical_index, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfPredicateSourceNull(struct sljit_compiler *compiler, idx_t source_index,
                                                        sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_validity));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
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

static void EmitLoadPredicateSourceData(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                        sljit_s32 index_reg, sljit_sw data_scale, sljit_s32 load_op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(SLJIT_R0, index_reg), data_scale);
}

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
                                            SljitPredicateBranches &result) {
	static_assert(sizeof(string_t) == 16, "SLJIT string match expects DuckDB string_t ABI size");
	static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
	EmitLoadPredicateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
	result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, source_index, SLJIT_R1));
	EmitLoadPredicateStringPointer(compiler, source_index, SLJIT_R0, SLJIT_R1);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
	EmitLoadPredicateStringDataPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4);
}

static idx_t SljitStringCompareChunkSize(idx_t remaining) {
	if (remaining >= sizeof(sljit_sw)) {
		return sizeof(sljit_sw);
	}
	if (remaining >= sizeof(uint32_t)) {
		return sizeof(uint32_t);
	}
	if (remaining >= sizeof(uint16_t)) {
		return sizeof(uint16_t);
	}
	return sizeof(uint8_t);
}

static sljit_s32 SljitStringChunkLoadOp(idx_t chunk_size) {
	if (chunk_size == sizeof(uint8_t)) {
		return SLJIT_MOV_U8;
	}
	if (chunk_size == sizeof(uint16_t)) {
		return SLJIT_MOV_U16;
	}
	if (chunk_size == sizeof(uint32_t)) {
		return SLJIT_MOV_U32;
	}
	if (chunk_size == sizeof(sljit_sw)) {
		return SLJIT_MOV;
	}
	throw InternalException("Unsupported SLJIT packed string comparison width");
}

static sljit_sw SljitStringChunkImmediate(const string &constant, idx_t constant_offset, idx_t chunk_size) {
	uint64_t value = 0;
	memcpy(&value, constant.data() + constant_offset, chunk_size);
	if (chunk_size == sizeof(sljit_sw)) {
		return SljitUInt64Immediate(value);
	}
	return NumericCast<sljit_sw>(value);
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
	return predicate.string_constants.size() > 2;
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

static void EmitPredicateStoreTrueSelection(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	EmitLoadPredicateResultIndex(compiler, SLJIT_R3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, true_sel));
	auto no_true_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 2, SLJIT_R3, 0);
	sljit_set_label(no_true_sel, sljit_emit_label(compiler));
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_R2, 0);
}

static void EmitPredicateStoreFalseSelection(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_R2, 0);
	EmitLoadPredicateResultIndex(compiler, SLJIT_R3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, false_sel));
	auto no_false_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 2, SLJIT_R3, 0);
	sljit_set_label(no_false_sel, sljit_emit_label(compiler));
}

static void EmitStorePredicateResultInvalid(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, result_validity));
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, SLJIT_S1, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_S1, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_XOR, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_IMM, -1);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3, SLJIT_R3, 0);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeConstantOrNull(const vector<idx_t> &guard_source_indices,
                                                                     SljitNativePredicateFunction &function,
                                                                     string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, count));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadPredicateLogicalIndex(compiler, SLJIT_S3);

	vector<sljit_jump *> invalid_jumps;
	for (auto source_index : guard_source_indices) {
		EmitLoadPredicateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
		invalid_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, source_index, SLJIT_R1));
	}

	auto next_valid = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto invalid_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(invalid_jumps, invalid_label);
	EmitStorePredicateResultInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_valid, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_return_void(compiler);

	return FinishSljitNativePredicateCode(compiler, function, error);
}

static SljitPredicateBranches EmitSljitPredicateBranches(struct sljit_compiler *compiler,
                                                         const SljitNativePredicate &predicate,
                                                         vector<shared_ptr<void>> &owned_data);

static bool PredicateDoubleCompareUsesHelper(const SljitNativePredicate &predicate) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT:
		return NativeDoubleSourceUsesHelper(predicate.double_source_kind);
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES:
		return NativeDoubleSourceUsesHelper(predicate.double_source_kind) ||
		       NativeDoubleSourceUsesHelper(predicate.double_right_source_kind);
	case SljitNativePredicateKind::NOT:
	case SljitNativePredicateKind::CONSTANT_OR_NULL:
		return predicate.child && PredicateDoubleCompareUsesHelper(*predicate.child);
	case SljitNativePredicateKind::CONJUNCTION:
		for (auto &child : predicate.children) {
			if (child && PredicateDoubleCompareUsesHelper(*child)) {
				return true;
			}
		}
		return false;
	default:
		return false;
	}
}

static SljitPredicateBranches EmitSljitConjunctionBranches(struct sljit_compiler *compiler,
                                                           const SljitNativePredicate &predicate, idx_t child_index,
                                                           bool null_pending, vector<shared_ptr<void>> &owned_data) {
	SljitPredicateBranches result;
	if (child_index >= predicate.children.size()) {
		if (null_pending) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}

	auto child = EmitSljitPredicateBranches(compiler, *predicate.children[child_index], owned_data);
	if (predicate.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		auto true_label = sljit_emit_label(compiler);
		SetPredicateJumpLabels(child.true_jumps, true_label);
		auto true_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, null_pending, owned_data);
		AppendPredicateBranches(result, std::move(true_rest));

		auto null_label = sljit_emit_label(compiler);
		SetPredicateJumpLabels(child.null_jumps, null_label);
		auto null_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, true, owned_data);
		AppendPredicateBranches(result, std::move(null_rest));

		AppendPredicateJumps(result.false_jumps, std::move(child.false_jumps));
		return result;
	}

	auto false_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(child.false_jumps, false_label);
	auto false_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, null_pending, owned_data);
	AppendPredicateBranches(result, std::move(false_rest));

	auto null_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(child.null_jumps, null_label);
	auto null_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, true, owned_data);
	AppendPredicateBranches(result, std::move(null_rest));

	AppendPredicateJumps(result.true_jumps, std::move(child.true_jumps));
	return result;
}

static SljitPredicateBranches EmitSljitPredicateBranches(struct sljit_compiler *compiler,
                                                         const SljitNativePredicate &predicate,
                                                         vector<shared_ptr<void>> &owned_data) {
	SljitPredicateBranches result;
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
	case SljitNativePredicateKind::REFERENCE: {
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, 0, SLJIT_MOV_U8);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::NOT: {
		auto child = EmitSljitPredicateBranches(compiler, *predicate.child, owned_data);
		result.true_jumps = std::move(child.false_jumps);
		result.false_jumps = std::move(child.true_jumps);
		result.null_jumps = std::move(child.null_jumps);
		return result;
	}
	case SljitNativePredicateKind::CONJUNCTION:
		return EmitSljitConjunctionBranches(compiler, predicate, 0, false, owned_data);
	case SljitNativePredicateKind::CONSTANT_OR_NULL: {
		if (predicate.guard_has_null_constant) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return result;
		}
		for (auto source_index : predicate.guard_source_indices) {
			EmitLoadPredicateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
			result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, source_index, SLJIT_R1));
		}
		auto child = EmitSljitPredicateBranches(compiler, *predicate.child, owned_data);
		AppendPredicateBranches(result, std::move(child));
		return result;
	}
	case SljitNativePredicateKind::INTEGER_COMPARE_CONSTANT: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto compare_type = NativeIntegerCompareJumpType(predicate.integer_kind, predicate.compare_op);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
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
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.right_source_index, SLJIT_S4));
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, data_scale, load_op);
		EmitLoadPredicateSourceData(compiler, predicate.right_source_index, SLJIT_R3, SLJIT_S4, data_scale, load_op);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, compare_type, SLJIT_R2, 0, SLJIT_R3, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::DOUBLE_COMPARE_CONSTANT: {
		auto single_precision = PredicateDoubleSourceIsSinglePrecision(predicate.double_source_kind);
		auto compare_type = NativeDoubleCompareJumpType(predicate.compare_op) | (single_precision ? SLJIT_32 : 0);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
		                               predicate.double_source_scale, SLJIT_FR0);
		if (single_precision) {
			sljit_emit_fset32(compiler, SLJIT_FR1, static_cast<sljit_f32>(predicate.double_constant));
		} else {
			sljit_emit_fset64(compiler, SLJIT_FR1, predicate.double_constant);
		}
		if (predicate.constant_on_left) {
			result.true_jumps.push_back(sljit_emit_fcmp(compiler, compare_type, SLJIT_FR1, 0, SLJIT_FR0, 0));
		} else {
			result.true_jumps.push_back(sljit_emit_fcmp(compiler, compare_type, SLJIT_FR0, 0, SLJIT_FR1, 0));
		}
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::DOUBLE_COMPARE_REFERENCES: {
		static constexpr sljit_sw LEFT_SPILL_OFFSET = SLJIT_SELECT_LOCAL_SIZE;
		static constexpr sljit_sw RIGHT_SPILL_OFFSET = SLJIT_SELECT_LOCAL_SIZE + sizeof(double);
		auto single_precision = PredicateDoubleSourceIsSinglePrecision(predicate.double_source_kind);
		auto compare_type = NativeDoubleCompareJumpType(predicate.compare_op) | (single_precision ? SLJIT_32 : 0);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.right_source_index, SLJIT_S4));
		if (NativeDoubleSourceUsesHelper(predicate.double_source_kind) ||
		    NativeDoubleSourceUsesHelper(predicate.double_right_source_kind)) {
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
			                               predicate.double_source_scale, SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), LEFT_SPILL_OFFSET);
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_right_source_kind, predicate.right_source_index,
			                               SLJIT_S4, predicate.double_right_source_scale, SLJIT_FR0);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_STORE | SLJIT_MEM_ALIGNED_32, SLJIT_FR0,
			                SLJIT_MEM1(SLJIT_SP), RIGHT_SPILL_OFFSET);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR0, SLJIT_MEM1(SLJIT_SP),
			                LEFT_SPILL_OFFSET);
			sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, SLJIT_FR1, SLJIT_MEM1(SLJIT_SP),
			                RIGHT_SPILL_OFFSET);
		} else {
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_source_kind, predicate.source_index, SLJIT_R1,
			                               predicate.double_source_scale, SLJIT_FR0);
			EmitLoadPredicateDoubleOperand(compiler, predicate.double_right_source_kind, predicate.right_source_index,
			                               SLJIT_S4, predicate.double_right_source_scale, SLJIT_FR1);
		}
		result.true_jumps.push_back(sljit_emit_fcmp(compiler, compare_type, SLJIT_FR0, 0, SLJIT_FR1, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::INT128_COMPARE_CONSTANT: {
		auto compare_op =
		    predicate.constant_on_left ? FlipNativeIntegerCompareOp(predicate.compare_op) : predicate.compare_op;
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1,
		                                  offsetof(hugeint_t, lower), SLJIT_R4);
		EmitLoadPredicateInt128SourceWord(compiler, predicate.source_index, SLJIT_R3, SLJIT_R1,
		                                  offsetof(hugeint_t, upper), SLJIT_R4);
		EmitSljitInt128CompareBranches(compiler, compare_op, SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_IMM,
		                               SljitUInt64Immediate(predicate.int128_constant_lower), SLJIT_IMM,
		                               NumericCast<sljit_sw>(predicate.int128_constant_upper), result);
		return result;
	}
	case SljitNativePredicateKind::INT128_COMPARE_REFERENCES: {
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		EmitLoadPredicateSourceIndex(compiler, predicate.right_source_index, SLJIT_S3, SLJIT_S4);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.right_source_index, SLJIT_S4));
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
		return result;
	}
	case SljitNativePredicateKind::INTEGER_IN_LIST: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
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
		return result;
	}
	case SljitNativePredicateKind::INTEGER_BETWEEN: {
		auto data_scale = NativeIntegerDataScale(predicate.integer_kind);
		auto load_op = NativeIntegerLoadOp(predicate.integer_kind);
		auto lower_failure = NativeIntegerLowerBoundFailureJump(predicate.integer_kind, predicate.lower_inclusive);
		auto upper_failure = NativeIntegerUpperBoundFailureJump(predicate.integer_kind, predicate.upper_inclusive);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
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
		return result;
	}
	case SljitNativePredicateKind::STRING_EQUAL_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringEqualBranches(compiler, predicate.string_constant, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_IN_LIST_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringInListBranches(compiler, predicate, result, owned_data);
		return result;
	}
	case SljitNativePredicateKind::STRING_PREFIX_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringPrefixBranches(compiler, predicate.string_constant, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_SUFFIX_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringSuffixBranches(compiler, predicate.string_constant, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_CONTAINS_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		EmitStringContainsBranches(compiler, predicate.string_constant, result);
		return result;
	}
	case SljitNativePredicateKind::STRING_LIKE_CONSTANT: {
		EmitLoadPredicateStringForMatch(compiler, predicate.source_index, result);
		auto pattern = AddStringConstantCodeData(owned_data, predicate.string_constant);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_R4, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_R2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_IMM, CastPointerToValue(pattern));
		sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS3(W, P, W, P), SLJIT_IMM,
		                 SLJIT_FUNC_ADDR(SljitNativeStringLikePercentOnly));
		result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_RETURN_REG, 0, SLJIT_IMM, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return result;
	}
	case SljitNativePredicateKind::STRING_SUBSTRING_IN_LIST_CONSTANT: {
		static_assert(sizeof(string_t) == 16, "SLJIT string substring expects DuckDB string_t ABI size");
		static constexpr sljit_sw STRING_LENGTH_OFFSET = 0;
		static constexpr sljit_sw STRING_INLINE_PREFIX_OFFSET = sizeof(uint32_t);
		const auto substring_length = predicate.substring_length;
		const auto inline_length = std::min<idx_t>(substring_length, string_t::PREFIX_LENGTH);
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
		EmitLoadPredicateStringPointer(compiler, predicate.source_index, SLJIT_R0, SLJIT_R1);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_R0), STRING_LENGTH_OFFSET);
		result.false_jumps.push_back(
		    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_R2, 0, SLJIT_IMM, NumericCast<sljit_sw>(substring_length)));
		if (substring_length == 0) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return result;
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
		return result;
	}
	case SljitNativePredicateKind::NULL_CHECK: {
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		auto source_is_null = EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1);
		if (predicate.null_check_op == SljitNativeNullCheckOp::IS_NULL) {
			result.true_jumps.push_back(source_is_null);
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(source_is_null);
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return result;
	}
	default:
		throw InternalException("Unknown SLJIT native predicate kind");
	}
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativePredicate(const SljitNativePredicate &predicate,
                                                                bool materialize_result,
                                                                SljitNativePredicateFunction &function, string &error) {
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto local_size = materialize_result ? 0 : SLJIT_SELECT_LOCAL_SIZE;
	if (PredicateDoubleCompareUsesHelper(predicate)) {
		local_size = SLJIT_SELECT_LOCAL_SIZE + sizeof(double) * 2;
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(3), 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, count));
	if (!materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	if (materialize_result) {
		EmitLoadPredicateLogicalIndex(compiler, SLJIT_S3);
	} else {
		EmitLoadPredicateResultAndLogicalIndexForSelect(compiler);
	}

	vector<shared_ptr<void>> owned_data;
	auto branches = EmitSljitPredicateBranches(compiler, predicate, owned_data);
	auto false_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.false_jumps, false_label);
	if (materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 0);
	} else {
		EmitPredicateStoreFalseSelection(compiler);
	}
	auto next_after_false = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto true_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.true_jumps, true_label);
	if (materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 1);
	} else {
		EmitPredicateStoreTrueSelection(compiler);
	}
	auto next_after_true = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto null_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(branches.null_jumps, null_label);
	if (materialize_result) {
		EmitStorePredicateResultInvalid(compiler);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, result_data));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 0, SLJIT_IMM, 0);
	} else {
		EmitPredicateStoreFalseSelection(compiler);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next_after_false, next_label);
	sljit_set_label(next_after_true, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	sljit_set_label(done, sljit_emit_label(compiler));
	if (!materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, selected_count),
		               SLJIT_R0, 0);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitNativePredicateCode(compiler, function, error, std::move(owned_data));
}

} // namespace duckdb
