#include "sljit_native_codegen.hpp"

#include "sljit_codegen_internal.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"

#include "sljitLir.h"

namespace duckdb {

static unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeUngroupedSumTypedExpressionTree(
    const ExecutionExpressionIR &root, SljitNativeAggregateUpdateFunction &function, string &error,
    SljitNativeAggregateSumStateKind state_kind, bool emit_flat_nullable_fast_path) {
	const auto tree_plan = BuildSljitTypedExpressionTreePlan(root, emit_flat_nullable_fast_path);
	if (!tree_plan.supported || !tree_plan.result_is_int64) {
		error = "SLJIT aggregate typed expression-tree reducer only supports INT64/DECIMAL64 expression trees";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto tree_local_size = NumericCast<sljit_sw>(tree_plan.node_count * sizeof(sljit_sw) * 3);
	const bool hugeint_state = state_kind == SljitNativeAggregateSumStateKind::HUGEINT;
	const auto local_sum_offset = tree_local_size;
	const auto local_sum_upper_offset = local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto saw_value_offset = hugeint_state ? local_sum_upper_offset + NumericCast<sljit_sw>(sizeof(sljit_sw))
	                                            : local_sum_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = saw_value_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, GetSljitNativeVectorRegisterLayout().saved_register_count,
	                 local_size);
	EmitInitSljitNativeExpressionVectorLoop(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offset, SLJIT_IMM, 0);
	if (hugeint_state) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offset, SLJIT_IMM, 0);
	}
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 0);

	vector<SljitExpressionTreeOverflowJumps> overflows;
	const auto &fast_path = tree_plan.fast_path;
	struct sljit_jump *fast_done = nullptr;
	if (fast_path.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_slow_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		auto fast_loop = sljit_emit_label(compiler);
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		idx_t fast_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, root, fast_spill_index, overflows);
		EmitSljitAggregateAccumulateSumState(compiler, state_kind, local_sum_offset, local_sum_upper_offset,
		                                     saw_value_offset, SLJIT_R2);
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);
		sljit_set_label(use_slow_loop, sljit_emit_label(compiler));
	}
	struct sljit_jump *flat_nullable_done = nullptr;
	struct sljit_jump *use_generic_loop = nullptr;
	if (fast_path.precheck_nulls_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		auto flat_nullable_loop = sljit_emit_label(compiler);
		flat_nullable_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		vector<sljit_jump *> source_null_jumps;
		for (auto source_index : fast_path.source_refs) {
			source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
		}
		idx_t flat_nullable_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, root, flat_nullable_spill_index, overflows);
		EmitSljitAggregateAccumulateSumState(compiler, state_kind, local_sum_offset, local_sum_upper_offset,
		                                     saw_value_offset, SLJIT_R2);
		auto flat_nullable_next = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto flat_nullable_invalid = sljit_emit_label(compiler);
		for (auto source_null_jump : source_null_jumps) {
			sljit_set_label(source_null_jump, flat_nullable_invalid);
		}
		sljit_set_label(flat_nullable_next, sljit_emit_label(compiler));
		EmitNextSljitNativeVectorLoop(compiler, flat_nullable_loop);
		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
	}

	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);
	idx_t slot_index = 0;
	auto root_slot = EmitSljitTypedExpressionTreeValue(compiler, root, slot_index, overflows);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), root_slot.valid_offset);
	auto root_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), root_slot.value_offset);
	EmitSljitAggregateAccumulateSumState(compiler, state_kind, local_sum_offset, local_sum_upper_offset,
	                                     saw_value_offset, SLJIT_R2);
	auto row_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(root_invalid, sljit_emit_label(compiler));
	sljit_set_label(row_done, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, loop);

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
	if (fast_done) {
		sljit_set_label(fast_done, done_label);
	}
	if (flat_nullable_done) {
		sljit_set_label(flat_nullable_done, done_label);
	}
	sljit_set_label(done, done_label);
	EmitSljitAggregateCommitSumState(compiler, state_kind, local_sum_offset, local_sum_upper_offset, saw_value_offset);
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumInt64TypedExpressionTree(const ExecutionExpressionIR &root,
                                                     SljitNativeAggregateUpdateFunction &function, string &error,
                                                     bool emit_flat_nullable_fast_path) {
	return BuildSljitNativeUngroupedSumTypedExpressionTree(
	    root, function, error, SljitNativeAggregateSumStateKind::INT64, emit_flat_nullable_fast_path);
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedSumHugeintTypedExpressionTree(const ExecutionExpressionIR &root,
                                                       SljitNativeAggregateUpdateFunction &function, string &error,
                                                       bool emit_flat_nullable_fast_path) {
	return BuildSljitNativeUngroupedSumTypedExpressionTree(
	    root, function, error, SljitNativeAggregateSumStateKind::HUGEINT, emit_flat_nullable_fast_path);
}

} // namespace duckdb
