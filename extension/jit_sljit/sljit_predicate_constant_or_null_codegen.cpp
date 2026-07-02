//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_predicate_constant_or_null_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_predicate_codegen_helpers.hpp"

namespace duckdb {

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

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
