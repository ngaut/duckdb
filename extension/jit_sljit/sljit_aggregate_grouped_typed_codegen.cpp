#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "sljitLir.h"

namespace duckdb {

struct SljitGroupedFusedTypedAggregateCodegenPlan {
	vector<SljitTypedExpressionTreePlan> payloads;
	idx_t tree_node_count = 0;
	bool has_typed_payload = false;
	bool fast_path_supported = false;
};

static bool BuildSljitGroupedFusedTypedAggregateCodegenPlan(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                            const vector<ExecutionRegionAggregateInput> &aggregates,
                                                            const ExecutionRegionAggregateContract &contract,
                                                            SljitGroupedFusedTypedAggregateCodegenPlan &codegen_plan) {
	if (!contract.grouped_state_layout_ready || payloads.size() != aggregates.size() || payloads.empty()) {
		return false;
	}
	codegen_plan = SljitGroupedFusedTypedAggregateCodegenPlan();
	codegen_plan.payloads.resize(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &payload = payloads[payload_idx];
		if (!SljitFusedGroupedTypedAggregatePayloadSupported(payload, aggregate, contract)) {
			return false;
		}
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		auto &payload_plan = codegen_plan.payloads[payload_idx];
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			payload_plan.supported = true;
			payload_plan.result_kind = payload.integer_kind;
			payload_plan.result_is_int64 = true;
			payload_plan.fast_path.fast_path_supported = true;
			continue;
		}
		payload_plan = BuildSljitTypedExpressionTreePlan(*payload.expression_tree, false);
		if (!SljitAggregateTypedPayloadPlanSupported(payload_plan, aggregate)) {
			return false;
		}
		codegen_plan.has_typed_payload = true;
		codegen_plan.tree_node_count += payload_plan.node_count;
	}
	codegen_plan.fast_path_supported = true;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported && codegen_plan.payloads[payload_idx].fast_path.fast_path_supported;
	}
	return codegen_plan.has_typed_payload;
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedFusedTypedExpressionAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error) {
	SljitGroupedFusedTypedAggregateCodegenPlan codegen_plan;
	if (!BuildSljitGroupedFusedTypedAggregateCodegenPlan(payloads, aggregates, contract, codegen_plan)) {
		error = "unsupported fused grouped typed aggregate payload shape";
		return nullptr;
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	const auto state_ptr_offset = NumericCast<sljit_sw>(codegen_plan.tree_node_count * sizeof(sljit_sw) * 3);
	const auto logical_index_offset = state_ptr_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size = logical_index_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	vector<SljitExpressionTreeOverflowJumps> overflows;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 7, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	EmitInitSljitNativeVectorSourceArrays(compiler);

	struct sljit_jump *fast_done = nullptr;
	struct sljit_jump *use_generic_loop = nullptr;
	struct sljit_jump *selected_fast_done = nullptr;
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

		auto fast_loop = sljit_emit_label(compiler);
		fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_S4, SLJIT_S1);

		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
				                                                 aggregate.primitive_update_state_value_offset);
				continue;
			}

			auto &payload = payloads[payload_idx];
			if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				EmitLoadFusedTypedAggregateReferenceValue(compiler, payload, false, false, SLJIT_S1);
			} else {
				idx_t payload_spill_index = 0;
				EmitSljitTypedExpressionTreeFastValueReg(compiler, *payload.expression_tree, payload_spill_index,
				                                         overflows);
			}
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
				EmitSljitGroupedAggregateAccumulateInt64Immediate(
				    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
				    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
			} else {
				EmitSljitGroupedAggregateAccumulateHugeintImmediate(
				    compiler, SLJIT_S4, state_offset, aggregate.primitive_update_state_value_offset,
				    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
			}
		}
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);

		auto selected_fast_loop = sljit_emit_label(compiler);
		selected_fast_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		EmitLoadSljitExpressionTreeLogicalIndex(compiler);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), logical_index_offset, SLJIT_S3, 0);
		EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_R0, SLJIT_S3);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), state_ptr_offset, SLJIT_R0, 0);

		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &aggregate = aggregates[payload_idx];
			const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
				sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
				EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_R0, state_offset,
				                                                 aggregate.primitive_update_state_value_offset);
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
				continue;
			}

			auto &payload = payloads[payload_idx];
			if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
				EmitLoadFusedTypedAggregateReferenceValue(compiler, payload, true, false, SLJIT_S3);
			} else {
				idx_t payload_spill_index = 0;
				EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *payload.expression_tree,
				                                                 payload_spill_index, overflows);
			}

			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
			if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
				EmitSljitGroupedAggregateAccumulateInt64Immediate(
				    compiler, SLJIT_S3, state_offset, aggregate.primitive_update_state_value_offset,
				    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
			} else {
				EmitSljitGroupedAggregateAccumulateHugeintImmediate(
				    compiler, SLJIT_S3, state_offset, aggregate.primitive_update_state_value_offset,
				    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
			}
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
		}
		EmitNextSljitNativeVectorLoop(compiler, selected_fast_loop);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), logical_index_offset, SLJIT_S3, 0);
	EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_R0, SLJIT_S3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), state_ptr_offset, SLJIT_R0, 0);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[aggregate.aggregate_index];

		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_R0, state_offset,
			                                                 aggregate.primitive_update_state_value_offset);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
			continue;
		}

		auto &payload = payloads[payload_idx];
		vector<sljit_jump *> payload_skip_jumps;
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			auto source_is_null = EmitLoadFusedTypedAggregateReferenceValue(compiler, payload, true, true, SLJIT_S3);
			if (source_is_null) {
				payload_skip_jumps.push_back(source_is_null);
			}
		} else {
			idx_t payload_slot_index = 0;
			auto payload_slot =
			    EmitSljitTypedExpressionTreeValue(compiler, *payload.expression_tree, payload_slot_index, overflows);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.valid_offset);
			payload_skip_jumps.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R3, 0, SLJIT_IMM, 0));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), payload_slot.value_offset);
		}

		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S3, state_offset,
			                                                  aggregate.primitive_update_state_value_offset,
			                                                  aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		} else {
			EmitSljitGroupedAggregateAccumulateHugeintImmediate(
			    compiler, SLJIT_S3, state_offset, aggregate.primitive_update_state_value_offset,
			    aggregate.primitive_update_state_is_set_offset, SLJIT_R2);
		}
		if (!payload_skip_jumps.empty()) {
			auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
			auto payload_skip_label = sljit_emit_label(compiler);
			for (auto payload_skip : payload_skip_jumps) {
				sljit_set_label(payload_skip, payload_skip_label);
			}
			sljit_set_label(payload_done, sljit_emit_label(compiler));
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
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
	sljit_set_label(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
