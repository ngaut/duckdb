//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_structural_branch_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_predicate_codegen_helpers.hpp"

namespace duckdb {

static SljitPredicateBranches EmitSljitConjunctionBranches(struct sljit_compiler *compiler,
                                                           const SljitNativePredicate &predicate, idx_t child_index,
                                                           bool null_pending, vector<shared_ptr<void>> &owned_data,
                                                           const vector<bool> &source_not_null,
                                                           bool sources_all_valid) {
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

	auto child = EmitSljitPredicateBranches(compiler, *predicate.children[child_index], owned_data, source_not_null,
	                                        sources_all_valid);
	if (predicate.conjunction_op == ExecutionExpressionConjunctionOp::AND) {
		auto true_label = sljit_emit_label(compiler);
		SetPredicateJumpLabels(child.true_jumps, true_label);
		auto true_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, null_pending, owned_data,
		                                              source_not_null, sources_all_valid);
		AppendPredicateBranches(result, std::move(true_rest));

		auto null_label = sljit_emit_label(compiler);
		SetPredicateJumpLabels(child.null_jumps, null_label);
		auto null_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, true, owned_data,
		                                              source_not_null, sources_all_valid);
		AppendPredicateBranches(result, std::move(null_rest));

		AppendPredicateJumps(result.false_jumps, std::move(child.false_jumps));
		return result;
	}

	auto false_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(child.false_jumps, false_label);
	auto false_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, null_pending, owned_data,
	                                               source_not_null, sources_all_valid);
	AppendPredicateBranches(result, std::move(false_rest));

	auto null_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(child.null_jumps, null_label);
	auto null_rest = EmitSljitConjunctionBranches(compiler, predicate, child_index + 1, true, owned_data,
	                                              source_not_null, sources_all_valid);
	AppendPredicateBranches(result, std::move(null_rest));

	AppendPredicateJumps(result.true_jumps, std::move(child.true_jumps));
	return result;
}

bool TryEmitSljitStructuralPredicateBranches(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                             vector<shared_ptr<void>> &owned_data, const vector<bool> &source_not_null,
                                             bool sources_all_valid, SljitPredicateBranches &result) {
	switch (predicate.kind) {
	case SljitNativePredicateKind::CONSTANT:
		if (predicate.constant_is_null) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else if (predicate.constant_value) {
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return true;
	case SljitNativePredicateKind::REFERENCE:
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		AppendPredicateSourceNullJump(compiler, result, predicate.source_index, SLJIT_R1, source_not_null,
		                              sources_all_valid);
		EmitLoadPredicateSourceData(compiler, predicate.source_index, SLJIT_R2, SLJIT_R1, 0, SLJIT_MOV_U8);
		result.true_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0));
		result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		return true;
	case SljitNativePredicateKind::NOT: {
		auto child =
		    EmitSljitPredicateBranches(compiler, *predicate.child, owned_data, source_not_null, sources_all_valid);
		result.true_jumps = std::move(child.false_jumps);
		result.false_jumps = std::move(child.true_jumps);
		result.null_jumps = std::move(child.null_jumps);
		return true;
	}
	case SljitNativePredicateKind::CONJUNCTION:
		result =
		    EmitSljitConjunctionBranches(compiler, predicate, 0, false, owned_data, source_not_null, sources_all_valid);
		return true;
	case SljitNativePredicateKind::CONSTANT_OR_NULL: {
		if (predicate.guard_has_null_constant) {
			result.null_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			return true;
		}
		for (auto source_index : predicate.guard_source_indices) {
			EmitLoadPredicateSourceIndex(compiler, source_index, SLJIT_S3, SLJIT_R1);
			AppendPredicateSourceNullJump(compiler, result, source_index, SLJIT_R1, source_not_null, sources_all_valid);
		}
		auto child =
		    EmitSljitPredicateBranches(compiler, *predicate.child, owned_data, source_not_null, sources_all_valid);
		AppendPredicateBranches(result, std::move(child));
		return true;
	}
	case SljitNativePredicateKind::NULL_CHECK:
		if (sources_all_valid || !PredicateSourceCanHaveNull(predicate.source_index, source_not_null)) {
			if (predicate.null_check_op == SljitNativeNullCheckOp::IS_NULL) {
				result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			} else {
				result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
			}
			return true;
		}
		EmitLoadPredicateSourceIndex(compiler, predicate.source_index, SLJIT_S3, SLJIT_R1);
		if (predicate.null_check_op == SljitNativeNullCheckOp::IS_NULL) {
			result.true_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
			result.false_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		} else {
			result.false_jumps.push_back(EmitJumpIfPredicateSourceNull(compiler, predicate.source_index, SLJIT_R1));
			result.true_jumps.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
		}
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
