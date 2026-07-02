//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_predicate_codegen_helpers.hpp"

#include <cstddef>

namespace duckdb {

static void EmitResetPredicateLoopState(struct sljit_compiler *compiler, bool materialize_result) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	if (!materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET, SLJIT_IMM, 0);
	}
}

static sljit_jump *EmitSljitNativePredicateLoop(struct sljit_compiler *compiler, const SljitNativePredicate &predicate,
                                                bool materialize_result, vector<shared_ptr<void>> &owned_data,
                                                bool sources_all_valid) {
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadPredicateLogicalIndex(compiler, SLJIT_S3);

	auto branches =
	    EmitSljitPredicateBranches(compiler, predicate, owned_data, predicate.source_not_null, sources_all_valid);
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
	return done;
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
	if (SljitPredicateDoubleCompareUsesHelper(predicate)) {
		local_size = SLJIT_SELECT_LOCAL_SIZE + sizeof(double) * 2;
	}
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(3), 5, local_size);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S2, 0, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, count));

	vector<shared_ptr<void>> owned_data;
	vector<sljit_jump *> done_jumps;
	if (SljitPredicateUsesSourceValidity(predicate, predicate.source_not_null)) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativePredicateInput, sources_all_valid));
		auto generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		EmitResetPredicateLoopState(compiler, materialize_result);
		done_jumps.push_back(EmitSljitNativePredicateLoop(compiler, predicate, materialize_result, owned_data, true));
		sljit_set_label(generic_loop, sljit_emit_label(compiler));
		EmitResetPredicateLoopState(compiler, materialize_result);
		done_jumps.push_back(EmitSljitNativePredicateLoop(compiler, predicate, materialize_result, owned_data, false));
	} else {
		EmitResetPredicateLoopState(compiler, materialize_result);
		done_jumps.push_back(EmitSljitNativePredicateLoop(compiler, predicate, materialize_result, owned_data, false));
	}

	auto done_label = sljit_emit_label(compiler);
	SetPredicateJumpLabels(done_jumps, done_label);
	if (!materialize_result) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), SLJIT_SELECT_TRUE_COUNT_OFFSET);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0), offsetof(SljitNativePredicateInput, selected_count),
		               SLJIT_R0, 0);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error, std::move(owned_data));
}

} // namespace duckdb
