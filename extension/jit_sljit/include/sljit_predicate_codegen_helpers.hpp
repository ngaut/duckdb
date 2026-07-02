//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_codegen_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"

namespace duckdb {

struct SljitPredicateBranches {
	vector<sljit_jump *> true_jumps;
	vector<sljit_jump *> false_jumps;
	vector<sljit_jump *> null_jumps;
};

SljitPredicateBranches EmitSljitPredicateBranches(struct sljit_compiler *compiler,
                                                  const SljitNativePredicate &predicate,
                                                  vector<shared_ptr<void>> &owned_data,
                                                  const vector<bool> &source_not_null, bool sources_all_valid);
bool TryEmitSljitStructuralPredicateBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                             vector<shared_ptr<void>> &owned_data, const vector<bool> &source_not_null,
                                             bool sources_all_valid, SljitPredicateBranches &result);
bool TryEmitSljitNumericPredicateBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                          const vector<bool> &source_not_null, bool sources_all_valid,
                                          SljitPredicateBranches &result);
bool TryEmitSljitStringPredicateBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                         vector<shared_ptr<void>> &owned_data, const vector<bool> &source_not_null,
                                         bool sources_all_valid, SljitPredicateBranches &result);
bool SljitPredicateDoubleCompareUsesHelper(const SljitNativePredicate &predicate);
bool SljitPredicateUsesSourceValidity(const SljitNativePredicate &predicate, const vector<bool> &source_not_null);

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

static void AppendPredicateSourceNullJump(struct sljit_compiler *compiler, SljitPredicateBranches &result,
                                          idx_t source_index, sljit_s32 index_reg, const vector<bool> &source_not_null,
                                          bool sources_all_valid);

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

static bool PredicateSourceCanHaveNull(idx_t source_index, const vector<bool> &source_not_null) {
	return source_index >= source_not_null.size() || !source_not_null[source_index];
}

static void AppendPredicateSourceNullJump(struct sljit_compiler *compiler, SljitPredicateBranches &result,
                                          idx_t source_index, sljit_s32 index_reg, const vector<bool> &source_not_null,
                                          bool sources_all_valid) {
	if (sources_all_valid || !PredicateSourceCanHaveNull(source_index, source_not_null)) {
		return;
	}
	result.null_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, source_index, index_reg));
}

static void EmitLoadPredicateSourceData(struct sljit_compiler *compiler, idx_t source_index, sljit_s32 target,
                                        sljit_s32 index_reg, sljit_sw data_scale, sljit_s32 load_op) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativePredicateInput, source_data));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(source_index));
	sljit_emit_op1(compiler, load_op, target, 0, SLJIT_MEM2(SLJIT_R0, index_reg), data_scale);
}

static void EmitPredicateStoreTrueSelection(struct sljit_compiler *compiler) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
	EmitLoadPredicateLogicalIndex(compiler, SLJIT_R3);
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
	EmitLoadPredicateLogicalIndex(compiler, SLJIT_R3);
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

} // namespace duckdb
