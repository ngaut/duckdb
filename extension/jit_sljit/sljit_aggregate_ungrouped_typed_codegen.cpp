//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_ungrouped_typed_codegen.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_ungrouped_shared_codegen.hpp"
#include "sljit_aggregate_ungrouped_typed_codegen.hpp"
#include "sljit_aggregate_ungrouped_typed_payload_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "sljitLir.h"

namespace duckdb {

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedFusedAggregateUpdate(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                              const vector<ExecutionRegionAggregateInput> &aggregates,
                                              SljitNativeAggregateUpdateFunction &function, string &error) {
	SljitUngroupedFusedAggregateUpdatePlan update_plan;
	if (!TryBuildSljitUngroupedFusedAggregateUpdatePlan(payloads, aggregates, update_plan, error)) {
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto &codegen_plan = update_plan.codegen_plan;
	const auto &local_sum_offsets = update_plan.local_sum_offsets;
	const auto &local_sum_upper_offsets = update_plan.local_sum_upper_offsets;
	const auto &saw_value_offsets = update_plan.saw_value_offsets;
	const auto &source_data_hoists = update_plan.source_data_hoists;
	const auto &fast_source_data_hoists = update_plan.fast_source_data_hoists;
	const auto local_size = update_plan.local_size;
	const auto shared_fast_value_offset = update_plan.shared_fast_value_offset;
	const auto register_accumulator_used_offset = update_plan.register_accumulator_used_offset;
	const auto use_conditional_hugeint_register_accumulators =
	    update_plan.use_conditional_hugeint_register_accumulators;
	const auto hoist_source_data_pointers = update_plan.hoist_source_data_pointers;
	const auto hoist_fast_source_data_pointers = update_plan.hoist_fast_source_data_pointers;
	const auto fast_data_hoists = hoist_fast_source_data_pointers ? &fast_source_data_hoists : nullptr;
	const auto hybrid_data_hoists = hoist_source_data_pointers ? &source_data_hoists : nullptr;
	const auto saved_register_count = update_plan.saved_register_count;

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, saved_register_count, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (local_sum_offsets[payload_idx] < 0) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_IMM, 0);
		if (local_sum_upper_offsets[payload_idx] >= 0) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[payload_idx], SLJIT_IMM,
			               0);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
	}
	if (use_conditional_hugeint_register_accumulators) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), register_accumulator_used_offset, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_UNGROUPED_SHARED_LOWER_REG, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_UNGROUPED_SHARED_UPPER_REG, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG, 0, SLJIT_IMM, 0);
	}
	EmitInitSljitNativeVectorSourceArrays(compiler);
	if (hoist_source_data_pointers) {
		for (auto &hoist : source_data_hoists) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.data_reg, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(hoist.source_index));
		}
	}
	auto emit_fast_source_data_hoists = [&]() {
		if (!hoist_fast_source_data_pointers) {
			return;
		}
		for (idx_t hoist_idx = source_data_hoists.size(); hoist_idx < fast_source_data_hoists.size(); hoist_idx++) {
			auto &hoist = fast_source_data_hoists[hoist_idx];
			sljit_emit_op1(compiler, SLJIT_MOV_P, hoist.data_reg, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(hoist.source_index));
		}
	};

	vector<SljitExpressionTreeOverflowJumps> overflows;
	struct sljit_jump *fast_done = nullptr;
	struct sljit_jump *selected_fast_done = nullptr;
	struct sljit_jump *hybrid_done = nullptr;
	struct sljit_jump *done = nullptr;
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

		emit_fast_source_data_hoists();
		if (use_conditional_hugeint_register_accumulators) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), register_accumulator_used_offset, SLJIT_IMM, 1);
		}
		auto fast_loop = sljit_emit_label(compiler);
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		if (codegen_plan.conditional_shared_payload) {
			EmitSljitUngroupedTypedConditionalSharedFastPayload(
			    compiler, codegen_plan, aggregates, local_sum_offsets, local_sum_upper_offsets, saw_value_offsets,
			    shared_fast_value_offset, use_conditional_hugeint_register_accumulators, false, overflows,
			    fast_data_hoists);
		} else {
			EmitSljitUngroupedTypedFastPayloads(compiler, payloads, aggregates, local_sum_offsets,
			                                    local_sum_upper_offsets, saw_value_offsets, false, overflows,
			                                    fast_data_hoists);
		}
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		auto use_generic_selected_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		emit_fast_source_data_hoists();
		if (use_conditional_hugeint_register_accumulators) {
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), register_accumulator_used_offset, SLJIT_IMM, 1);
		}
		auto selected_fast_loop = sljit_emit_label(compiler);
		selected_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitExpressionTreeLogicalIndex(compiler);
		if (codegen_plan.conditional_shared_payload) {
			EmitSljitUngroupedTypedConditionalSharedFastPayload(
			    compiler, codegen_plan, aggregates, local_sum_offsets, local_sum_upper_offsets, saw_value_offsets,
			    shared_fast_value_offset, use_conditional_hugeint_register_accumulators, true, overflows,
			    fast_data_hoists);
		} else {
			EmitSljitUngroupedTypedFastPayloads(compiler, payloads, aggregates, local_sum_offsets,
			                                    local_sum_upper_offsets, saw_value_offsets, true, overflows,
			                                    fast_data_hoists);
		}
		EmitNextSljitNativeVectorLoop(compiler, selected_fast_loop);

		sljit_set_label(use_generic_selected_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	const bool use_hybrid_nullable_loop = !codegen_plan.conditional_shared_payload && payloads.size() == 1 &&
	                                      payloads[0].expression_tree &&
	                                      codegen_plan.payloads[0].fast_path.hybrid_nulls_supported;
	if (use_hybrid_nullable_loop) {
		auto &payload = payloads[0];
		auto &fast_path = codegen_plan.payloads[0].fast_path;
		auto kind = aggregates[0].primitive_update_kind;
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_no_selection));
		auto use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
		// The hybrid loop can enter generic nullable evaluation on any row. Keep
		// S6 owned by the validity-pointer array instead of borrowing it for the
		// third all-valid-only data hoist.
		auto hybrid_loop = sljit_emit_label(compiler);
		hybrid_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		vector<sljit_jump *> source_null_jumps;
		for (auto source_index : fast_path.source_refs) {
			source_null_jumps.push_back(EmitJumpIfSljitExpressionTreeFlatSourceNull(compiler, source_index));
		}
		idx_t fast_spill_index = 0;
		EmitSljitTypedExpressionTreeFastValueReg(compiler, *payload.expression_tree, fast_spill_index, overflows,
		                                         hybrid_data_hoists);
		EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[0], local_sum_upper_offsets[0],
		                                 saw_value_offsets[0], SLJIT_R2);
		auto fast_row_done = sljit_emit_jump(compiler, SLJIT_JUMP);

		auto generic_row = sljit_emit_label(compiler);
		for (auto source_null_jump : source_null_jumps) {
			sljit_set_label(source_null_jump, generic_row);
		}
		EmitLoadSljitExpressionTreeLogicalIndex(compiler);
		idx_t slot_index = 0;
		auto payload_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *payload.expression_tree, slot_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.valid_offset);
		auto generic_row_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
		EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[0], local_sum_upper_offsets[0],
		                                 saw_value_offsets[0], SLJIT_R2);
		auto next_row = sljit_emit_label(compiler);
		sljit_set_label(fast_row_done, next_row);
		sljit_set_label(generic_row_invalid, next_row);
		EmitNextSljitNativeVectorLoop(compiler, hybrid_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);

	if (codegen_plan.conditional_shared_payload) {
		auto shared_lane = codegen_plan.shared_lane;
		auto conditional_lane = codegen_plan.conditional_lane;
		auto kind = aggregates[shared_lane].primitive_update_kind;
		idx_t slot_index = 0;
		auto shared_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *codegen_plan.shared_value, slot_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), shared_slot.valid_offset);
		auto shared_invalid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), shared_slot.value_offset);
		EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[shared_lane],
		                                 local_sum_upper_offsets[shared_lane], saw_value_offsets[shared_lane],
		                                 SLJIT_R2);
		auto shared_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto shared_done_label = sljit_emit_label(compiler);
		sljit_set_label(shared_invalid, shared_done_label);
		sljit_set_label(shared_done, shared_done_label);

		auto predicate_slot =
		    EmitSljitTypedExpressionTreeValue(compiler, *codegen_plan.conditional_predicate, slot_index, overflows);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.valid_offset);
		auto condition_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), predicate_slot.value_offset);
		auto condition_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), shared_slot.valid_offset);
		auto conditional_value_null = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), shared_slot.value_offset);
		EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[conditional_lane],
		                                 local_sum_upper_offsets[conditional_lane], saw_value_offsets[conditional_lane],
		                                 SLJIT_R2);
		auto conditional_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto condition_not_true = sljit_emit_label(compiler);
		sljit_set_label(condition_null, condition_not_true);
		sljit_set_label(condition_false, condition_not_true);
		EmitFusedTypedConditionalSharedSawElseZero(compiler, saw_value_offsets[conditional_lane]);
		auto conditional_done_label = sljit_emit_label(compiler);
		sljit_set_label(conditional_value_null, conditional_done_label);
		sljit_set_label(conditional_done, conditional_done_label);
	} else {
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto kind = aggregates[payload_idx].primitive_update_kind;
			if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				continue;
			}
			vector<sljit_jump *> payload_skip_jumps;
			if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
				auto source_is_null =
				    EmitLoadFusedAggregateReferenceValue(compiler, payloads[payload_idx], true, true, SLJIT_S3);
				if (source_is_null) {
					payload_skip_jumps.push_back(source_is_null);
				}
			} else {
				idx_t payload_slot_index = 0;
				auto payload_slot = EmitSljitTypedExpressionTreeValue(compiler, *payloads[payload_idx].expression_tree,
				                                                      payload_slot_index, overflows);
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.valid_offset);
				payload_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
			}
			EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
			                                 local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                 SLJIT_R2);
			auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto payload_skip_label = sljit_emit_label(compiler);
			for (auto payload_skip : payload_skip_jumps) {
				sljit_set_label(payload_skip, payload_skip_label);
			}
			sljit_set_label(payload_done, sljit_emit_label(compiler));
		}
	}
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
	if (selected_fast_done) {
		sljit_set_label(selected_fast_done, done_label);
	}
	if (hybrid_done) {
		sljit_set_label(hybrid_done, done_label);
	}
	sljit_set_label(done, done_label);
	if (use_conditional_hugeint_register_accumulators) {
		auto shared_lane = codegen_plan.shared_lane;
		auto conditional_lane = codegen_plan.conditional_lane;
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), register_accumulator_used_offset);
		auto no_register_accumulator = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[shared_lane],
		               SLJIT_UNGROUPED_SHARED_LOWER_REG, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[shared_lane],
		               SLJIT_UNGROUPED_SHARED_UPPER_REG, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[conditional_lane],
		               SLJIT_UNGROUPED_CONDITIONAL_LOWER_REG, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_upper_offsets[conditional_lane],
		               SLJIT_UNGROUPED_CONDITIONAL_UPPER_REG, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, 0);
		auto empty_input = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S2, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R0, 0, SLJIT_IMM, 1);
		sljit_set_label(empty_input, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[shared_lane], SLJIT_R0, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[conditional_lane], SLJIT_R0, 0);
		sljit_set_label(no_register_accumulator, sljit_emit_label(compiler));
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = aggregates[payload_idx].primitive_update_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitUngroupedAggregateCommitCountStar(compiler, payload_idx);
		} else if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			EmitUngroupedAggregateCommitSumHugeint(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                       local_sum_upper_offsets[payload_idx],
			                                       saw_value_offsets[payload_idx]);
		} else {
			EmitUngroupedAggregateCommitSumInt64(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                     saw_value_offsets[payload_idx]);
		}
	}
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
