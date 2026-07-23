#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeExpressionTree(const ExecutionExpressionIR &root, SljitNativeVectorFunction &function, string &error) {
	if (!SljitExpressionTreeIsSupported(root)) {
		error = "SLJIT expression-tree codegen only supports checked DECIMAL64 arithmetic trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	auto local_size = NumericCast<sljit_sw>(CountSljitExpressionTreeSpills(root) * sizeof(sljit_sw));
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, SLJIT_NATIVE_VECTOR_SAVED_REG_COUNT, local_size);
	// Hoist loop-invariant vector-format arrays so source indexing and references do not reload them
	// for every expression node in the fused tree.
	EmitInitSljitNativeExpressionVectorLoop(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	idx_t fast_spill_index = 0;
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, fast_spill_index, overflows, true);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, NativeIntegerStoreOp(SljitNativeIntegerKind::DECIMAL64), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64), SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto fast_repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(fast_repeat, fast_loop);

	sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<idx_t> source_refs;
	CollectSljitExpressionTreeSourceRefs(root, source_refs);
	vector<sljit_jump *> source_null_jumps;
	for (auto source_index : source_refs) {
		source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	}

	idx_t spill_index = 0;
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, spill_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, result_data));
	sljit_emit_op1(compiler, NativeIntegerStoreOp(SljitNativeIntegerKind::DECIMAL64), SLJIT_MEM2(SLJIT_R0, SLJIT_S1),
	               NativeIntegerDataScale(SljitNativeIntegerKind::DECIMAL64), SLJIT_R2, 0);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	for (auto source_null_jump : source_null_jumps) {
		sljit_set_label(source_null_jump, invalid_label);
	}
	EmitSetResultRowInvalid(compiler);

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	vector<sljit_jump *> helper_done;
	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitExpressionTreeOverflowCall(compiler, overflow.op);
		helper_done.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(fast_done, done_label);
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, done_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

static unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumExpressionTree(const ExecutionExpressionIR &root,
                                           SljitNativeAggregateUpdateFunction &function, string &error,
                                           SljitNativeAggregateSumStateKind state_kind) {
	if (!SljitExpressionTreeIsSupported(root)) {
		error = "SLJIT aggregate reducer only supports checked DECIMAL64 expression trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto spill_size = NumericCast<sljit_sw>(CountSljitExpressionTreeSpills(root) * sizeof(sljit_sw));
	const bool hugeint_state = state_kind == SljitNativeAggregateSumStateKind::HUGEINT;
	const auto local_sum_offset = spill_size;
	const auto local_sum_upper_offset = local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto saw_value_offset = hugeint_state ? local_sum_upper_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))
	                                            : local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, SLJIT_NATIVE_VECTOR_SAVED_REG_COUNT, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (hugeint_state) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offset, SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);
	// Hoist loop-invariant vector-format arrays (see projection tree).
	EmitInitSljitNativeVectorSourceArrays(compiler);

	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
	auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	idx_t fast_spill_index = 0;
	auto fast_loop = sljit_emit_label(compiler);
	auto fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, fast_spill_index, overflows, true);
	EmitSljitAggregateAccumulateSumState(compiler, state_kind, local_sum_offset, local_sum_upper_offset,
	                                     saw_value_offset, SLJIT_R2);
	EmitNextSljitNativeVectorLoop(compiler, fast_loop);

	sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	vector<idx_t> source_refs;
	CollectSljitExpressionTreeSourceRefs(root, source_refs);
	vector<sljit_jump *> source_null_jumps;
	for (auto source_index : source_refs) {
		source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeSourceNull(compiler, source_index));
	}

	idx_t spill_index = 0;
	EmitSljitExpressionTreeValue(compiler, root, SLJIT_R2, spill_index, overflows);
	EmitSljitAggregateAccumulateSumState(compiler, state_kind, local_sum_offset, local_sum_upper_offset,
	                                     saw_value_offset, SLJIT_R2);
	auto next = sljit_emit_jump(compiler, SLJIT_JUMP);

	auto invalid_label = sljit_emit_label(compiler);
	for (auto source_null_jump : source_null_jumps) {
		sljit_set_label(source_null_jump, invalid_label);
	}

	auto next_label = sljit_emit_label(compiler);
	sljit_set_label(next, next_label);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_S1, 0, SLJIT_S1, 0, SLJIT_IMM, 1);
	auto repeat = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(repeat, loop);

	vector<sljit_jump *> helper_done;
	for (auto &overflow : overflows) {
		auto overflow_label = sljit_emit_label(compiler);
		for (auto jump : overflow.jumps) {
			sljit_set_label(jump, overflow_label);
		}
		EmitSljitAggregateExpressionTreeOverflowCall(compiler, overflow.op);
		helper_done.push_back(sljit_emit_jump(compiler, SLJIT_JUMP));
	}

	auto done_label = sljit_emit_label(compiler);
	sljit_set_label(fast_done, done_label);
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitSumState(compiler, state_kind, local_sum_offset, local_sum_upper_offset, saw_value_offset);

	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64ExpressionTree(const ExecutionExpressionIR &root,
                                                SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativeUngroupedSumExpressionTree(root, function, error, SljitNativeAggregateSumStateKind::INT64);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintExpressionTree(const ExecutionExpressionIR &root,
                                                  SljitNativeAggregateUpdateFunction &function, string &error) {
	return BuildSljitNativeUngroupedSumExpressionTree(root, function, error, SljitNativeAggregateSumStateKind::HUGEINT);
}

} // namespace duckdb
