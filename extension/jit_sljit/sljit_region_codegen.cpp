#include "sljit_region_codegen.hpp"

#include "sljit_codegen_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/string_type.hpp"

#include "sljitLir.h"

#include <cstddef>
#include <exception>

namespace duckdb {

static unique_ptr<ExecutionRegionCodeHandle> FinishSljitHashJoinProbeCode(struct sljit_compiler *compiler,
                                                                          SljitNativeHashJoinProbeFunction &function,
                                                                          string &error) {
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

	function = reinterpret_cast<SljitNativeHashJoinProbeFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static unique_ptr<ExecutionRegionCodeHandle>
FinishSljitNestedLoopJoinProbeCode(struct sljit_compiler *compiler, SljitNativeNestedLoopJoinProbeFunction &function,
                                   string &error) {
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

	function = reinterpret_cast<SljitNativeNestedLoopJoinProbeFunction>(code);
	return MakeSljitCodeHandle(code, code_size);
}

static inline bool SljitNestedLoopJoinValueIsValid(const validity_t *validity, idx_t row_idx) {
	if (!validity) {
		return true;
	}
	const auto entry_idx = row_idx / ValidityMask::BITS_PER_VALUE;
	const auto idx_in_entry = row_idx % ValidityMask::BITS_PER_VALUE;
	return ValidityMask::RowIsValid(validity[entry_idx], idx_in_entry);
}

template <class T>
static inline bool SljitNestedLoopJoinCompareValue(T left, T right, ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::EQUAL:
		return left == right;
	case ExecutionRegionComparisonType::NOT_EQUAL:
	case ExecutionRegionComparisonType::DISTINCT_FROM:
		return left != right;
	case ExecutionRegionComparisonType::LESS_THAN:
		return left < right;
	case ExecutionRegionComparisonType::GREATER_THAN:
		return left > right;
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return left <= right;
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return left >= right;
	case ExecutionRegionComparisonType::NOT_DISTINCT_FROM:
		return left == right;
	default:
		throw InternalException("Unsupported SLJIT nested loop join comparison");
	}
}

template <class T, ExecutionRegionComparisonType COMPARISON>
static void SljitNestedLoopJoinProbeTypedCompare(SljitNativeNestedLoopJoinProbeInput *input) {
	auto left_data = reinterpret_cast<const T *>(input->left_data);
	auto right_data = reinterpret_cast<const T *>(input->right_data);
	auto left_idx = input->left_offset;
	auto right_idx = input->right_offset;
	idx_t out_idx = 0;

	while (left_idx < input->left_count) {
		auto left_source_idx = input->left_sel ? input->left_sel[left_idx] : left_idx;
		if (!SljitNestedLoopJoinValueIsValid(input->left_validity, left_source_idx)) {
			left_idx++;
			right_idx = 0;
			continue;
		}
		auto left_value = left_data[left_source_idx];
		while (right_idx < input->right_count) {
			auto right_source_idx = input->right_sel ? input->right_sel[right_idx] : right_idx;
			if (SljitNestedLoopJoinValueIsValid(input->right_validity, right_source_idx) &&
			    SljitNestedLoopJoinCompareValue<T>(left_value, right_data[right_source_idx], COMPARISON)) {
				input->left_match_sel[out_idx] = UnsafeNumericCast<sel_t>(left_idx);
				input->right_match_sel[out_idx] = UnsafeNumericCast<sel_t>(right_idx);
				out_idx++;
				right_idx++;
				if (out_idx == input->output_capacity) {
					input->left_offset = left_idx;
					input->right_offset = right_idx;
					input->selected_count = out_idx;
					input->right_chunk_finished = false;
					return;
				}
				continue;
			}
			right_idx++;
		}
		left_idx++;
		right_idx = 0;
	}

	input->left_offset = left_idx;
	input->right_offset = 0;
	input->selected_count = out_idx;
	input->right_chunk_finished = true;
}

using SljitNestedLoopJoinProbeHelper = void (*)(SljitNativeNestedLoopJoinProbeInput *);

template <class T>
static SljitNestedLoopJoinProbeHelper
SelectSljitNestedLoopJoinProbeComparisonHelper(ExecutionRegionComparisonType comparison_type) {
	switch (comparison_type) {
	case ExecutionRegionComparisonType::EQUAL:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::EQUAL>;
	case ExecutionRegionComparisonType::NOT_EQUAL:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::NOT_EQUAL>;
	case ExecutionRegionComparisonType::LESS_THAN:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::LESS_THAN>;
	case ExecutionRegionComparisonType::GREATER_THAN:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::GREATER_THAN>;
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL>;
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL>;
	case ExecutionRegionComparisonType::NOT_DISTINCT_FROM:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::NOT_DISTINCT_FROM>;
	case ExecutionRegionComparisonType::DISTINCT_FROM:
		return SljitNestedLoopJoinProbeTypedCompare<T, ExecutionRegionComparisonType::DISTINCT_FROM>;
	default:
		return nullptr;
	}
}

static SljitNestedLoopJoinProbeHelper
SelectSljitNestedLoopJoinProbeHelper(const SljitNativeNestedLoopJoinProbePlan &plan, string &error) {
	if (plan.conditions.size() != 1) {
		error = "SLJIT native nested loop join probe requires one comparison condition";
		return nullptr;
	}
	auto &condition = plan.conditions[0];
	switch (condition.value_kind) {
	case SljitNativeNestedLoopJoinValueKind::INT32:
		return SelectSljitNestedLoopJoinProbeComparisonHelper<int32_t>(condition.comparison_type);
	case SljitNativeNestedLoopJoinValueKind::INT64:
		return SelectSljitNestedLoopJoinProbeComparisonHelper<int64_t>(condition.comparison_type);
	case SljitNativeNestedLoopJoinValueKind::INT128:
		return SelectSljitNestedLoopJoinProbeComparisonHelper<hugeint_t>(condition.comparison_type);
	case SljitNativeNestedLoopJoinValueKind::DOUBLE:
		return SelectSljitNestedLoopJoinProbeComparisonHelper<double>(condition.comparison_type);
	default:
		error = "SLJIT native nested loop join probe has unsupported value kind";
		return nullptr;
	}
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
	case SljitNativeHashJoinKeyKind::INT128:
	case SljitNativeHashJoinKeyKind::UINT128:
		throw InternalException("128-bit SLJIT native hash join keys are loaded by word");
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
	case SljitNativeHashJoinKeyKind::INT128:
	case SljitNativeHashJoinKeyKind::UINT128:
		throw InternalException("128-bit SLJIT native hash join keys are indexed by generated address arithmetic");
	default:
		throw InternalException("Unknown SLJIT native hash join key kind");
	}
}

static bool SljitHashJoinKeyKindIs128(SljitNativeHashJoinKeyKind kind) {
	return kind == SljitNativeHashJoinKeyKind::INT128 || kind == SljitNativeHashJoinKeyKind::UINT128;
}

static bool SljitHashJoinKeyHashesAsUInt32(SljitNativeHashJoinKeyKind kind) {
	return kind != SljitNativeHashJoinKeyKind::INT64 && kind != SljitNativeHashJoinKeyKind::UINT64 &&
	       !SljitHashJoinKeyKindIs128(kind);
}

static bool SljitHashJoinKeyKindIsSigned(SljitNativeHashJoinKeyKind kind) {
	switch (kind) {
	case SljitNativeHashJoinKeyKind::INT8:
	case SljitNativeHashJoinKeyKind::INT16:
	case SljitNativeHashJoinKeyKind::INT32:
	case SljitNativeHashJoinKeyKind::INT64:
	case SljitNativeHashJoinKeyKind::INT128:
		return true;
	default:
		return false;
	}
}

static sljit_s32 SljitHashJoinPredicateMismatchComparison(ExecutionRegionComparisonType comparison_type,
                                                          SljitNativeHashJoinKeyKind key_kind) {
	const bool signed_compare = SljitHashJoinKeyKindIsSigned(key_kind);
	switch (comparison_type) {
	case ExecutionRegionComparisonType::NOT_EQUAL:
		return SLJIT_EQUAL;
	case ExecutionRegionComparisonType::LESS_THAN:
		return signed_compare ? SLJIT_SIG_GREATER_EQUAL : SLJIT_GREATER_EQUAL;
	case ExecutionRegionComparisonType::GREATER_THAN:
		return signed_compare ? SLJIT_SIG_LESS_EQUAL : SLJIT_LESS_EQUAL;
	case ExecutionRegionComparisonType::LESS_THAN_OR_EQUAL:
		return signed_compare ? SLJIT_SIG_GREATER : SLJIT_GREATER;
	case ExecutionRegionComparisonType::GREATER_THAN_OR_EQUAL:
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

static void EmitDuckDBMurmurMultiply(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch,
                                     sljit_s32 multiplier_reg) {
	if (multiplier_reg != 0) {
		sljit_emit_op2(compiler, SLJIT_MUL, target, 0, target, 0, multiplier_reg, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_IMM, DuckDBMurmurHashMultiplierImmediate());
	sljit_emit_op2(compiler, SLJIT_MUL, target, 0, target, 0, scratch, 0);
}

static void EmitDuckDBMurmurHash64(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch,
                                   sljit_s32 multiplier_reg = 0) {
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
	EmitDuckDBMurmurMultiply(compiler, target, scratch, multiplier_reg);

	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
	EmitDuckDBMurmurMultiply(compiler, target, scratch, multiplier_reg);

	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, target, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, target, 0, target, 0, scratch, 0);
}

static void EmitLoadHashJoinSourceIndex(struct sljit_compiler *compiler, idx_t key_idx, sljit_s32 target,
                                        sljit_s32 scratch, bool assume_identity_selection = false) {
	if (assume_identity_selection) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, source_sel));
	auto no_sel_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(scratch),
	               NumericCast<sljit_sw>(key_idx * sizeof(sel_t *)));
	auto no_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target, 0, SLJIT_MEM2(scratch, SLJIT_S1), 2);
	auto have_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_sel_array, use_logical_index);
	sljit_set_label(no_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_S1, 0);
	sljit_set_label(have_index, sljit_emit_label(compiler));
}

static struct sljit_jump *EmitJumpIfHashJoinSourceNull(struct sljit_compiler *compiler, idx_t key_idx,
                                                       sljit_s32 source_index, sljit_s32 scratch, sljit_s32 scratch2,
                                                       bool assume_source_all_valid = false) {
	if (assume_source_all_valid) {
		return nullptr;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, source_validity));
	auto source_all_valid_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, scratch, 0, SLJIT_MEM1(scratch),
	               NumericCast<sljit_sw>(key_idx * sizeof(validity_t *)));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch2, 0, source_index, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, scratch2, 0, SLJIT_MEM2(scratch, scratch2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, scratch, 0, source_index, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, SLJIT_IMM, 1, scratch, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, scratch, 0, scratch, 0, scratch2, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto all_valid = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid_array, all_valid);
	sljit_set_label(source_all_valid, all_valid);
	return source_is_null;
}

static struct sljit_jump *EmitJumpIfHashJoinRhsKeyNull(struct sljit_compiler *compiler, idx_t key_idx,
                                                       sljit_s32 row_pointer, sljit_s32 scratch, sljit_s32 scratch2,
                                                       bool assume_rhs_all_valid = false) {
	if (assume_rhs_all_valid) {
		return nullptr;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, rhs_keys_have_validity));
	auto rhs_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(row_pointer), NumericCast<sljit_sw>(key_idx / 8));
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, scratch2, 0, scratch, 0, SLJIT_IMM, 1ULL << (key_idx % 8));
	auto rhs_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(rhs_all_valid, sljit_emit_label(compiler));
	return rhs_is_null;
}

static void EmitLoadHashJoinSourceData(struct sljit_compiler *compiler, idx_t key_idx, sljit_s32 target,
                                       const vector<sljit_s32> &source_data_regs) {
	if (key_idx < source_data_regs.size() && source_data_regs[key_idx] != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, source_data_regs[key_idx], 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, target, 0, SLJIT_MEM1(target),
	               NumericCast<sljit_sw>(key_idx * sizeof(const_data_ptr_t)));
}

static sljit_s32 EmitPrepareHashJoinSourceData(struct sljit_compiler *compiler, idx_t key_idx, sljit_s32 target,
                                               const vector<sljit_s32> &source_data_regs) {
	if (key_idx < source_data_regs.size() && source_data_regs[key_idx] != 0) {
		return source_data_regs[key_idx];
	}
	EmitLoadHashJoinSourceData(compiler, key_idx, target, source_data_regs);
	return target;
}

static void EmitLoadHashJoinKey(struct sljit_compiler *compiler, SljitNativeHashJoinKeyKind kind, sljit_s32 target,
                                sljit_s32 base, sljit_s32 index, sljit_sw offset) {
	if (SljitHashJoinKeyKindIs128(kind)) {
		throw InternalException("128-bit SLJIT native hash join keys are loaded by word");
	}
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

static void EmitLoadHashJoinKeyWord(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 base, sljit_s32 index,
                                    sljit_sw offset, sljit_s32 scratch) {
	if (index == SLJIT_IMM) {
		sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM1(base), offset);
		return;
	}
	sljit_emit_op2(compiler, SLJIT_SHL, scratch, 0, index, 0, SLJIT_IMM, 4);
	if (offset != 0) {
		sljit_emit_op2(compiler, SLJIT_ADD, scratch, 0, scratch, 0, SLJIT_IMM, offset);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, target, 0, SLJIT_MEM2(base, scratch), 0);
}

static void EmitHashJoinKeyHash(struct sljit_compiler *compiler, SljitNativeHashJoinKeyKind key_kind,
                                sljit_s32 hash_reg, sljit_s32 scratch, sljit_s32 multiplier_reg = 0) {
	if (SljitHashJoinKeyHashesAsUInt32(key_kind)) {
		sljit_emit_op2(compiler, SLJIT_AND, hash_reg, 0, hash_reg, 0, SLJIT_IMM, 0xffffffffULL);
	}
	EmitDuckDBMurmurHash64(compiler, hash_reg, scratch, multiplier_reg);
}

static void EmitHashJoinKeyHashFromSourceData(struct sljit_compiler *compiler, SljitNativeHashJoinKeyKind key_kind,
                                              sljit_s32 hash_reg, sljit_s32 source_data, sljit_s32 source_index,
                                              sljit_s32 scratch, sljit_s32 multiplier_reg = 0) {
	if (!SljitHashJoinKeyKindIs128(key_kind)) {
		EmitLoadHashJoinKey(compiler, key_kind, hash_reg, source_data, source_index, 0);
		EmitHashJoinKeyHash(compiler, key_kind, hash_reg, scratch, multiplier_reg);
		return;
	}

	EmitLoadHashJoinKeyWord(compiler, hash_reg, source_data, source_index, offsetof(hugeint_t, lower), scratch);
	EmitDuckDBMurmurHash64(compiler, hash_reg, scratch, multiplier_reg);
	EmitLoadHashJoinKeyWord(compiler, scratch, source_data, source_index, offsetof(hugeint_t, upper), scratch);
	EmitDuckDBMurmurHash64(compiler, scratch, source_data, multiplier_reg);
	sljit_emit_op2(compiler, SLJIT_XOR, hash_reg, 0, hash_reg, 0, scratch, 0);
}

static void EmitHashJoinEqualityKeyMismatch(struct sljit_compiler *compiler, idx_t key_idx,
                                            SljitNativeHashJoinKeyKind key_kind, sljit_sw key_layout_offset,
                                            vector<struct sljit_jump *> &equality_key_mismatches,
                                            const vector<sljit_s32> &source_data_regs, sljit_s32 source_index_reg) {
	auto source_data_reg = EmitPrepareHashJoinSourceData(compiler, key_idx, SLJIT_R4, source_data_regs);
	if (!SljitHashJoinKeyKindIs128(key_kind)) {
		EmitLoadHashJoinKey(compiler, key_kind, SLJIT_R2, source_data_reg, source_index_reg, 0);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, key_layout_offset);
		EmitLoadHashJoinKey(compiler, key_kind, SLJIT_R4, SLJIT_R4, SLJIT_IMM, 0);
		equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));
		return;
	}

	EmitLoadHashJoinKeyWord(compiler, SLJIT_R2, source_data_reg, source_index_reg, offsetof(hugeint_t, lower),
	                        SLJIT_R2);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R4, SLJIT_R0, SLJIT_IMM,
	                        key_layout_offset + NumericCast<sljit_sw>(offsetof(hugeint_t, lower)), SLJIT_R4);
	equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));

	source_data_reg = EmitPrepareHashJoinSourceData(compiler, key_idx, SLJIT_R4, source_data_regs);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R2, source_data_reg, source_index_reg, offsetof(hugeint_t, upper),
	                        SLJIT_R2);
	EmitLoadHashJoinKeyWord(compiler, SLJIT_R4, SLJIT_R0, SLJIT_IMM,
	                        key_layout_offset + NumericCast<sljit_sw>(offsetof(hugeint_t, upper)), SLJIT_R4);
	equality_key_mismatches.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R2, 0));
}

static void EmitDuckDBCombineHashScalar(struct sljit_compiler *compiler, sljit_s32 current_hash, sljit_s32 other_hash,
                                        sljit_s32 scratch, sljit_s32 multiplier_reg = 0) {
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, current_hash, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, scratch, 0, scratch, 0, SLJIT_IMM, 32);
	sljit_emit_op2(compiler, SLJIT_XOR, current_hash, 0, current_hash, 0, scratch, 0);
	EmitDuckDBMurmurMultiply(compiler, current_hash, scratch, multiplier_reg);
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

static void EmitApplyHashJoinBitmask(struct sljit_compiler *compiler, sljit_s32 target, sljit_s32 scratch,
                                     bool bitmask_reg_available) {
	if (bitmask_reg_available) {
		sljit_emit_op2(compiler, SLJIT_AND, target, 0, target, 0, SLJIT_S6, 0);
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, bitmask));
	sljit_emit_op2(compiler, SLJIT_AND, target, 0, target, 0, scratch, 0);
}

static void EmitStoreHashJoinMarkProbeFlag(struct sljit_compiler *compiler, sljit_sw value) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, match_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R4, SLJIT_S1), 2, SLJIT_IMM, value);
}

static void EmitFinishHashJoinProbe(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeHashJoinProbeInput, input_offset),
	               SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, resume_row_pointer), SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeHashJoinProbeInput, finished),
	               SLJIT_IMM, 1);
}

static void EmitPauseHashJoinProbe(struct sljit_compiler *compiler, sljit_s32 resume_row_pointer) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeHashJoinProbeInput, input_offset),
	               SLJIT_S1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, resume_row_pointer), resume_row_pointer, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeHashJoinProbeInput, finished),
	               SLJIT_IMM, 0);
}

static void EmitLoadHashJoinNextPointer(struct sljit_compiler *compiler, sljit_s32 row_pointer, sljit_s32 scratch,
                                        sljit_s32 next_pointer, idx_t pointer_offset,
                                        bool chain_layout_is_constant = false,
                                        bool constant_chains_longer_than_one = false,
                                        bool constant_dictionary_emission = false,
                                        sljit_s32 aux_next_ptrs_reg = 0) {
	if (chain_layout_is_constant) {
		if (!constant_chains_longer_than_one) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_IMM, 0);
			return;
		}
		if (constant_dictionary_emission) {
			sljit_emit_op1(compiler, SLJIT_MOV_U32, scratch, 0, SLJIT_MEM1(row_pointer),
			               NumericCast<sljit_sw>(pointer_offset));
			if (aux_next_ptrs_reg != 0) {
				sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM2(aux_next_ptrs_reg, scratch), 3);
			} else {
				sljit_emit_op1(compiler, SLJIT_MOV_P, next_pointer, 0, SLJIT_MEM1(SLJIT_S0),
				               offsetof(SljitNativeHashJoinProbeInput, aux_next_ptrs));
				sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM2(next_pointer, scratch), 3);
			}
			return;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM1(row_pointer),
		               NumericCast<sljit_sw>(pointer_offset));
		return;
	}

	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, chains_longer_than_one));
	auto no_chain = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_U8, scratch, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, dictionary_emission));
	auto direct_pointer = sljit_emit_cmp(compiler, SLJIT_EQUAL, scratch, 0, SLJIT_IMM, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_U32, scratch, 0, SLJIT_MEM1(row_pointer), NumericCast<sljit_sw>(pointer_offset));
	sljit_emit_op1(compiler, SLJIT_MOV_P, next_pointer, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, aux_next_ptrs));
	sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM2(next_pointer, scratch), 3);
	auto done = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(direct_pointer, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_MEM1(row_pointer),
	               NumericCast<sljit_sw>(pointer_offset));
	auto done_direct = sljit_emit_jump(compiler, SLJIT_JUMP);

	sljit_set_label(no_chain, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV_P, row_pointer, 0, SLJIT_IMM, 0);

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(done, done_label);
	sljit_set_label(done_direct, done_label);
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

	sljit_emit_op1(compiler, SLJIT_MOV_U32, scratch, 0, SLJIT_MEM1(row_pointer), NumericCast<sljit_sw>(pointer_offset));
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

unique_ptr<ExecutionRegionCodeHandle> BuildSljitPerfectHashJoinProbe(const SljitNativeHashJoinProbeKeyPlan &key,
                                                                     ExecutionHashJoinProbeOutputMode output_mode,
                                                                     SljitNativeHashJoinProbeFunction &function,
                                                                     string &error) {
	if (!key.equality_key) {
		error = "SLJIT perfect hash join probe requires an equality key";
		return nullptr;
	}
	if (key.key_kind == SljitNativeHashJoinKeyKind::INT128 || key.key_kind == SljitNativeHashJoinKeyKind::UINT128) {
		error = "SLJIT perfect hash join probe does not support 128-bit keys";
		return nullptr;
	}
	if (output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_AND_BUILD &&
	    output_mode != ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY) {
		error = "SLJIT perfect hash join probe requires an inner output mode";
		return nullptr;
	}
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
	               offsetof(SljitNativeHashJoinProbeInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_R0), 0);

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);

	EmitLoadHashJoinSourceIndex(compiler, 0, SLJIT_R1, SLJIT_R0);
	auto source_is_null = EmitJumpIfHashJoinSourceNull(compiler, 0, SLJIT_R1, SLJIT_R2, SLJIT_R4);
	EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R0, SLJIT_S4, SLJIT_R1, 0);

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, perfect_min));
	auto below_range = sljit_emit_cmp(compiler, less_than_min, SLJIT_R0, 0, SLJIT_R2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, perfect_max));
	auto above_range = sljit_emit_cmp(compiler, greater_than_max, SLJIT_R0, 0, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_SUB, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R2, 0);

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, perfect_validity));
	auto all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R3, 0, SLJIT_R0, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R2, SLJIT_R3), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R4, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto value_missing = sljit_emit_jump(compiler, SLJIT_EQUAL);
	sljit_set_label(all_valid, sljit_emit_label(compiler));

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, build_sel));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R2, SLJIT_S3), 2, SLJIT_R0, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, match_sel));
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
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeHashJoinProbeInput, selected_count),
	               SLJIT_S3, 0);
	EmitFinishHashJoinProbe(compiler);
	sljit_emit_return_void(compiler);
	return FinishSljitHashJoinProbeCode(compiler, function, error);
}

bool ValidateSljitHashJoinProbe(const vector<SljitNativeHashJoinProbeKeyPlan> &keys, idx_t equality_key_count,
                                ExecutionHashJoinProbeOutputMode output_mode, string &error) {
	if (keys.empty()) {
		error = "SLJIT hash join probe requires at least one key";
		return false;
	}
	if (equality_key_count == 0 || equality_key_count > keys.size()) {
		error = "SLJIT hash join probe requires an equality-key prefix";
		return false;
	}
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		if ((key_idx < equality_key_count) != keys[key_idx].equality_key) {
			error = "SLJIT hash join probe key plan is not an equality-key prefix";
			return false;
		}
	}
	if (output_mode == ExecutionHashJoinProbeOutputMode::NONE) {
		error = "SLJIT hash join probe requires an output mode";
		return false;
	}
	return true;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitHashJoinProbe(const vector<SljitNativeHashJoinProbeKeyPlan> &keys,
                                                              idx_t equality_key_count, bool mark_build_match,
                                                              idx_t found_match_offset, idx_t pointer_offset,
                                                              ExecutionHashJoinProbeOutputMode output_mode,
                                                              SljitNativeHashJoinProbeFunction &function,
                                                              string &error, bool assume_flat_all_valid,
                                                              bool use_salt_is_constant, bool constant_use_salt,
                                                              bool chain_layout_is_constant,
                                                              bool constant_chains_longer_than_one,
                                                              bool constant_dictionary_emission,
                                                              bool assume_common_selection_all_valid) {
	if (!ValidateSljitHashJoinProbe(keys, equality_key_count, output_mode, error)) {
		return nullptr;
	}
	const bool mark_build_only = output_mode == ExecutionHashJoinProbeOutputMode::MARK_BUILD_ONLY;
	const bool mark_probe = output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE;
	const bool matched_probe_only = output_mode == ExecutionHashJoinProbeOutputMode::MATCHED_PROBE_ONLY;
	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	static constexpr bool BITMASK_REG_AVAILABLE = SLJIT_NUMBER_OF_SAVED_REGISTERS >= 7;
	static constexpr sljit_s32 BASE_SAVED_REG_COUNT = BITMASK_REG_AVAILABLE ? 7 : 6;
	const bool assume_all_keys_valid = assume_flat_all_valid || assume_common_selection_all_valid;
	sljit_s32 saved_reg_count = BASE_SAVED_REG_COUNT;
	sljit_s32 hash_multiplier_reg = 0;
	sljit_s32 common_source_index_reg = 0;
	vector<sljit_s32> source_data_regs(keys.size(), 0);
	sljit_s32 aux_next_ptrs_reg = 0;
	auto allocate_saved_reg = [&]() -> sljit_s32 {
		if (saved_reg_count >= SLJIT_NUMBER_OF_SAVED_REGISTERS) {
			return 0;
		}
		return SLJIT_S(saved_reg_count++);
	};
	hash_multiplier_reg = allocate_saved_reg();
	if (assume_common_selection_all_valid) {
		common_source_index_reg = allocate_saved_reg();
		if (common_source_index_reg == 0) {
			error = "not enough saved registers for selected all-valid hash join probe";
			sljit_free_compiler(compiler);
			return nullptr;
		}
	}
	if (assume_all_keys_valid) {
		for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
			auto reg = allocate_saved_reg();
			if (reg == 0) {
				break;
			}
			source_data_regs[key_idx] = reg;
		}
	}
	if (chain_layout_is_constant && constant_chains_longer_than_one && constant_dictionary_emission) {
		aux_next_ptrs_reg = allocate_saved_reg();
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, saved_reg_count, SLJIT_HASH_JOIN_PROBE_LOCAL_SIZE);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, input_offset));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, count));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, entries));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, pointer_mask));
	if (BITMASK_REG_AVAILABLE) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S6, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, bitmask));
	}
	if (hash_multiplier_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV, hash_multiplier_reg, 0, SLJIT_IMM,
		               DuckDBMurmurHashMultiplierImmediate());
	}
	for (idx_t key_idx = 0; key_idx < source_data_regs.size(); key_idx++) {
		auto reg = source_data_regs[key_idx];
		if (reg == 0) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV_P, reg, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, source_data));
		sljit_emit_op1(compiler, SLJIT_MOV_P, reg, 0, SLJIT_MEM1(reg),
		               NumericCast<sljit_sw>(key_idx * sizeof(const_data_ptr_t)));
	}
	if (aux_next_ptrs_reg != 0) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, aux_next_ptrs_reg, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, aux_next_ptrs));
	}

	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeHashJoinProbeInput, resume_row_pointer));
	auto no_resume_row_pointer = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	auto resume_row_pointer_ready = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(no_resume_row_pointer, sljit_emit_label(compiler));

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	if (mark_probe) {
		EmitStoreHashJoinMarkProbeFlag(compiler, 0);
	}
	if (assume_common_selection_all_valid) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, source_sel));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, common_source_index_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S1), 2);
	}
	vector<struct sljit_jump *> source_is_null;
	for (idx_t key_idx = 0; key_idx < equality_key_count; key_idx++) {
		auto &key = keys[key_idx];
		const auto source_index_reg =
		    assume_flat_all_valid ? SLJIT_S1 : (assume_common_selection_all_valid ? common_source_index_reg : SLJIT_R1);
		if (!assume_all_keys_valid) {
			EmitLoadHashJoinSourceIndex(compiler, key_idx, SLJIT_R1, SLJIT_R0);
		}
		auto source_null =
		    EmitJumpIfHashJoinSourceNull(compiler, key_idx, source_index_reg, SLJIT_R0, SLJIT_R4,
		                                 assume_all_keys_valid);
		if (!key.null_equal || assume_all_keys_valid) {
			if (source_null) {
				source_is_null.push_back(source_null);
			}
			auto source_data_reg = EmitPrepareHashJoinSourceData(compiler, key_idx, SLJIT_R0, source_data_regs);
			EmitHashJoinKeyHashFromSourceData(compiler, key.key_kind, SLJIT_R2, source_data_reg, source_index_reg,
			                                  SLJIT_R4, hash_multiplier_reg);
		} else {
			auto source_data_reg = EmitPrepareHashJoinSourceData(compiler, key_idx, SLJIT_R0, source_data_regs);
			EmitHashJoinKeyHashFromSourceData(compiler, key.key_kind, SLJIT_R2, source_data_reg, source_index_reg,
			                                  SLJIT_R4, hash_multiplier_reg);
			auto source_hash_ready = sljit_emit_jump(compiler, SLJIT_JUMP);
			sljit_set_label(source_null, sljit_emit_label(compiler));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_IMM, DuckDBNullHashImmediate());
			sljit_set_label(source_hash_ready, sljit_emit_label(compiler));
		}
		if (key_idx == 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_R2, 0);
		} else {
			EmitDuckDBCombineHashScalar(compiler, SLJIT_R3, SLJIT_R2, SLJIT_R4, hash_multiplier_reg);
		}
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, SLJIT_R3, 0);
	sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R3, 0, SLJIT_R3, 0, SLJIT_S5, 0);
	EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, BITMASK_REG_AVAILABLE);

	auto probe_loop = sljit_emit_label(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM2(SLJIT_S4, SLJIT_R1), 3);
	auto empty_slot = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	struct sljit_jump *salt_mismatch = nullptr;
	if (!use_salt_is_constant || constant_use_salt) {
		struct sljit_jump *skip_salt = nullptr;
		if (!use_salt_is_constant) {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R4, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeHashJoinProbeInput, use_salt));
			skip_salt = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R4, 0, SLJIT_IMM, 0);
		}
		sljit_emit_op2(compiler, SLJIT_OR, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_S5, 0);
		salt_mismatch = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R4, 0, SLJIT_R3, 0);
		if (skip_salt) {
			sljit_set_label(skip_salt, sljit_emit_label(compiler));
		}
	}

	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_S5, 0);
	if (equality_key_count > 0) {
		sljit_emit_op_src(compiler, SLJIT_PREFETCH_L1, SLJIT_MEM1(SLJIT_R0),
		                  NumericCast<sljit_sw>(keys[0].key_layout_offset));
	}
	if (!assume_all_keys_valid) {
		EmitSaveHashJoinProbeOffset(compiler);
	}
	auto row_pointer_ready = sljit_emit_label(compiler);
	sljit_set_label(resume_row_pointer_ready, row_pointer_ready);
	vector<struct sljit_jump *> equality_key_mismatches;
	vector<struct sljit_jump *> predicate_key_mismatches;
	for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
		auto &key = keys[key_idx];
		const auto source_index_reg =
		    assume_flat_all_valid ? SLJIT_S1 : (assume_common_selection_all_valid ? common_source_index_reg : SLJIT_R1);
		if (!assume_all_keys_valid) {
			EmitLoadHashJoinSourceIndex(compiler, key_idx, SLJIT_R1, SLJIT_R4);
		}
		if (key_idx >= equality_key_count) {
			auto source_null =
			    EmitJumpIfHashJoinSourceNull(compiler, key_idx, source_index_reg, SLJIT_R2, SLJIT_R4,
			                                 assume_all_keys_valid);
			if (source_null) {
				predicate_key_mismatches.push_back(source_null);
			}
			auto rhs_null =
			    EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4, assume_all_keys_valid);
			if (rhs_null) {
				predicate_key_mismatches.push_back(rhs_null);
			}
			auto source_data_reg = EmitPrepareHashJoinSourceData(compiler, key_idx, SLJIT_R4, source_data_regs);
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R2, source_data_reg, source_index_reg, 0);
			sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R4, 0, SLJIT_R0, 0, SLJIT_IMM,
			               NumericCast<sljit_sw>(key.key_layout_offset));
			EmitLoadHashJoinKey(compiler, key.key_kind, SLJIT_R4, SLJIT_R4, SLJIT_IMM, 0);
			predicate_key_mismatches.push_back(
			    sljit_emit_cmp(compiler, SljitHashJoinPredicateMismatchComparison(key.comparison_type, key.key_kind),
			                   SLJIT_R2, 0, SLJIT_R4, 0));
		} else if (key.null_equal && !assume_all_keys_valid) {
			auto source_null = EmitJumpIfHashJoinSourceNull(compiler, key_idx, SLJIT_R1, SLJIT_R2, SLJIT_R4);
			equality_key_mismatches.push_back(
			    EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4));
			EmitHashJoinEqualityKeyMismatch(compiler, key_idx, key.key_kind,
			                                NumericCast<sljit_sw>(key.key_layout_offset), equality_key_mismatches,
			                                source_data_regs, source_index_reg);
			auto key_done = sljit_emit_jump(compiler, SLJIT_JUMP);

			sljit_set_label(source_null, sljit_emit_label(compiler));
			auto rhs_null_match = EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4);
			equality_key_mismatches.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			auto nulls_match = sljit_emit_label(compiler);
			sljit_set_label(rhs_null_match, nulls_match);
			sljit_set_label(key_done, nulls_match);
		} else {
			auto rhs_null =
			    EmitJumpIfHashJoinRhsKeyNull(compiler, key_idx, SLJIT_R0, SLJIT_R2, SLJIT_R4, assume_all_keys_valid);
			if (rhs_null) {
				equality_key_mismatches.push_back(rhs_null);
			}
			EmitHashJoinEqualityKeyMismatch(compiler, key_idx, key.key_kind,
			                                NumericCast<sljit_sw>(key.key_layout_offset), equality_key_mismatches,
			                                source_data_regs, source_index_reg);
		}
	}
	if (mark_build_match) {
		if (mark_build_only) {
			EmitMarkHashJoinBuildChain(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, found_match_offset, pointer_offset);
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_MEM1(SLJIT_R0), NumericCast<sljit_sw>(found_match_offset),
			               SLJIT_IMM, 1);
		}
	}
	if (mark_build_only) {
		auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto restore_hash_offset = sljit_emit_label(compiler);
		for (auto &key_mismatch : equality_key_mismatches) {
			sljit_set_label(key_mismatch, restore_hash_offset);
		}
		for (auto &key_mismatch : predicate_key_mismatches) {
			sljit_set_label(key_mismatch, restore_hash_offset);
		}
		if (!assume_all_keys_valid) {
			EmitRestoreHashJoinProbeOffset(compiler);
		}
		auto next_slot = sljit_emit_label(compiler);
		if (salt_mismatch) {
			sljit_set_label(salt_mismatch, next_slot);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
		EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, BITMASK_REG_AVAILABLE);
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
		for (auto &key_mismatch : equality_key_mismatches) {
			sljit_set_label(key_mismatch, restore_hash_offset);
		}
		for (auto &key_mismatch : predicate_key_mismatches) {
			sljit_set_label(key_mismatch, restore_hash_offset);
		}
		if (!assume_all_keys_valid) {
			EmitRestoreHashJoinProbeOffset(compiler);
		}
		auto next_slot = sljit_emit_label(compiler);
		if (salt_mismatch) {
			sljit_set_label(salt_mismatch, next_slot);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
		EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, BITMASK_REG_AVAILABLE);
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
	if (matched_probe_only) {
		auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto predicate_mismatch = sljit_emit_label(compiler);
		for (auto &key_mismatch : predicate_key_mismatches) {
			sljit_set_label(key_mismatch, predicate_mismatch);
		}
		EmitLoadHashJoinNextPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, pointer_offset,
		                            chain_layout_is_constant, constant_chains_longer_than_one,
		                            constant_dictionary_emission, aux_next_ptrs_reg);
		auto continue_chain = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		auto advance_after_predicate = sljit_emit_jump(compiler, SLJIT_JUMP);

		auto restore_hash_offset = sljit_emit_label(compiler);
		for (auto &key_mismatch : equality_key_mismatches) {
			sljit_set_label(key_mismatch, restore_hash_offset);
		}
		if (!assume_all_keys_valid) {
			EmitRestoreHashJoinProbeOffset(compiler);
		}
		auto next_slot = sljit_emit_label(compiler);
		if (salt_mismatch) {
			sljit_set_label(salt_mismatch, next_slot);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
		EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, BITMASK_REG_AVAILABLE);
		auto repeat_probe = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_probe, probe_loop);

		auto skip_row = sljit_emit_label(compiler);
		sljit_set_label(empty_slot, skip_row);
		for (auto &null_jump : source_is_null) {
			sljit_set_label(null_jump, skip_row);
		}
		sljit_set_label(advance_row, skip_row);
		sljit_set_label(advance_after_predicate, skip_row);
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto repeat_rows = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_rows, loop);
		sljit_set_label(continue_chain, row_pointer_ready);

		sljit_set_label(done, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeHashJoinProbeInput, selected_count), SLJIT_S3, 0);
		EmitFinishHashJoinProbe(compiler);
		sljit_emit_return_void(compiler);
		return FinishSljitHashJoinProbeCode(compiler, function, error);
	}

	const bool no_chain_specialized = chain_layout_is_constant && !constant_chains_longer_than_one;
	if (no_chain_specialized) {
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
		auto repeat_after_match = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_after_match, loop);

		auto predicate_mismatch = sljit_emit_label(compiler);
		for (auto &key_mismatch : predicate_key_mismatches) {
			sljit_set_label(key_mismatch, predicate_mismatch);
		}
		auto advance_after_predicate = sljit_emit_jump(compiler, SLJIT_JUMP);

		auto restore_hash_offset = sljit_emit_label(compiler);
		for (auto &key_mismatch : equality_key_mismatches) {
			sljit_set_label(key_mismatch, restore_hash_offset);
		}
		if (!assume_all_keys_valid) {
			EmitRestoreHashJoinProbeOffset(compiler);
		}
		auto next_slot = sljit_emit_label(compiler);
		if (salt_mismatch) {
			sljit_set_label(salt_mismatch, next_slot);
		}
		sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
		EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, BITMASK_REG_AVAILABLE);
		auto repeat_probe = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(repeat_probe, probe_loop);

		auto skip_row = sljit_emit_label(compiler);
		sljit_set_label(empty_slot, skip_row);
		for (auto &null_jump : source_is_null) {
			sljit_set_label(null_jump, skip_row);
		}
		sljit_set_label(advance_after_predicate, skip_row);
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
	if (!no_chain_specialized) {
		EmitLoadHashJoinNextPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, pointer_offset,
		                            chain_layout_is_constant, constant_chains_longer_than_one,
		                            constant_dictionary_emission, aux_next_ptrs_reg);
	}
	auto output_has_capacity =
	    sljit_emit_cmp(compiler, SLJIT_LESS, SLJIT_S3, 0, SLJIT_IMM, STANDARD_VECTOR_SIZE);
	struct sljit_jump *pause_same_row = nullptr;
	if (no_chain_specialized) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_IMM, 0);
	} else {
		pause_same_row = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto pause_output = sljit_emit_label(compiler);
	if (pause_same_row) {
		sljit_set_label(pause_same_row, pause_output);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeHashJoinProbeInput, selected_count),
	               SLJIT_S3, 0);
	EmitPauseHashJoinProbe(compiler, SLJIT_R0);
	sljit_emit_return_void(compiler);

	auto output_has_capacity_label = sljit_emit_label(compiler);
	sljit_set_label(output_has_capacity, output_has_capacity_label);
	struct sljit_jump *continue_chain_after_match = nullptr;
	if (!no_chain_specialized) {
		continue_chain_after_match = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	}
	auto advance_row = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto predicate_mismatch = sljit_emit_label(compiler);
	for (auto &key_mismatch : predicate_key_mismatches) {
		sljit_set_label(key_mismatch, predicate_mismatch);
	}
	struct sljit_jump *continue_chain_after_predicate = nullptr;
	if (!no_chain_specialized) {
		EmitLoadHashJoinNextPointer(compiler, SLJIT_R0, SLJIT_R2, SLJIT_R4, pointer_offset,
		                            chain_layout_is_constant, constant_chains_longer_than_one,
		                            constant_dictionary_emission, aux_next_ptrs_reg);
		continue_chain_after_predicate = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	}
	auto advance_after_predicate = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto restore_hash_offset = sljit_emit_label(compiler);
	for (auto &key_mismatch : equality_key_mismatches) {
		sljit_set_label(key_mismatch, restore_hash_offset);
	}
	if (!assume_all_keys_valid) {
		EmitRestoreHashJoinProbeOffset(compiler);
	}
	auto next_slot = sljit_emit_label(compiler);
	if (salt_mismatch) {
		sljit_set_label(salt_mismatch, next_slot);
	}
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R1, 0, SLJIT_R1, 0, SLJIT_IMM, 1);
	EmitApplyHashJoinBitmask(compiler, SLJIT_R1, SLJIT_R4, BITMASK_REG_AVAILABLE);
	auto repeat_probe = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_probe, probe_loop);

	auto skip_row = sljit_emit_label(compiler);
	sljit_set_label(empty_slot, skip_row);
	for (auto &null_jump : source_is_null) {
		sljit_set_label(null_jump, skip_row);
	}
	sljit_set_label(advance_row, skip_row);
	sljit_set_label(advance_after_predicate, skip_row);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat_rows = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat_rows, loop);
	if (continue_chain_after_match) {
		sljit_set_label(continue_chain_after_match, row_pointer_ready);
	}
	if (continue_chain_after_predicate) {
		sljit_set_label(continue_chain_after_predicate, row_pointer_ready);
	}

	sljit_set_label(done, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativeHashJoinProbeInput, selected_count),
	               SLJIT_S3, 0);
	EmitFinishHashJoinProbe(compiler);
	sljit_emit_return_void(compiler);
	return FinishSljitHashJoinProbeCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNestedLoopJoinProbe(const SljitNativeNestedLoopJoinProbePlan &plan,
                                                                    SljitNativeNestedLoopJoinProbeFunction &function,
                                                                    string &error) {
	auto helper = SelectSljitNestedLoopJoinProbeHelper(plan, error);
	if (!helper) {
		if (error.empty()) {
			error = "SLJIT native nested loop join probe has no typed comparison helper";
		}
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 1, 1, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_S0, 0);
	sljit_emit_icall(compiler, SLJIT_CALL, SLJIT_ARGS1V(P), SLJIT_IMM, SLJIT_FUNC_ADDR(helper));
	sljit_emit_return_void(compiler);
	return FinishSljitNestedLoopJoinProbeCode(compiler, function, error);
}

} // namespace duckdb
