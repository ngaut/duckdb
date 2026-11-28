#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_aggregate_typed_payload_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"
#include "sljit_typed_expression_plan.hpp"

#include "sljitLir.h"

namespace duckdb {

struct SljitGroupedFusedTypedAggregateCodegenPlan {
	vector<SljitTypedExpressionTreePlan> payloads;
	vector<SljitAggregatePayloadDescriptor> payload_descriptors;
	SljitSharedBinaryPayloadPlan shared_binary;
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
	codegen_plan.payload_descriptors.resize(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &payload = payloads[payload_idx];
		auto &descriptor = codegen_plan.payload_descriptors[payload_idx];
		if (!SljitFusedGroupedAggregatePayloadSupported(payload, aggregate, contract, &descriptor)) {
			return false;
		}
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
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
		if (!SljitAggregateTypedPayloadPlanSupported(payload_plan, descriptor)) {
			return false;
		}
		codegen_plan.has_typed_payload = true;
		codegen_plan.tree_node_count += payload_plan.node_count;
	}
	codegen_plan.fast_path_supported = true;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (codegen_plan.payload_descriptors[payload_idx].primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		codegen_plan.fast_path_supported =
		    codegen_plan.fast_path_supported && codegen_plan.payloads[payload_idx].fast_path.fast_path_supported;
	}
	if (codegen_plan.fast_path_supported) {
		TryBuildSljitSharedBinaryPayloadPlan(payloads, codegen_plan.payload_descriptors, codegen_plan.shared_binary);
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
	const auto shared_value_offset = logical_index_offset + NumericCast<sljit_sw>(sizeof(sljit_sw));
	const auto local_size =
	    shared_value_offset + (codegen_plan.shared_binary.Enabled() ? NumericCast<sljit_sw>(sizeof(sljit_sw)) : 0);
	vector<SljitArithmeticOverflowSite> overflows;
	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), SljitTypedExpressionTreeEnterScratches(5),
	                 GetSljitNativeVectorRegisterLayout().saved_register_count, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	EmitInitSljitNativeVectorSourceArrays(compiler);

	vector<sljit_jump *> fast_done;
	vector<sljit_jump *> fast_direct_done;
	struct sljit_jump *use_generic_loop = nullptr;
	vector<sljit_jump *> selected_fast_done;
	vector<sljit_jump *> selected_by_loop_fast_done;
	if (codegen_plan.fast_path_supported) {
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_flat_all_valid));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);

		auto emit_flat_fast_payloads = [&]() {
			if (codegen_plan.shared_binary.Enabled()) {
				EmitSljitSharedBinaryPayloadBase(compiler, codegen_plan.shared_binary, 0, shared_value_offset,
				                                 SljitAggregateExpressionIndexMode::FLAT, overflows);
			}
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				auto &descriptor = codegen_plan.payload_descriptors[payload_idx];
				const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
				if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
					                                                 descriptor.state_value_offset);
					continue;
				}

				auto &payload = payloads[payload_idx];
				if (codegen_plan.shared_binary.Enabled() && codegen_plan.shared_binary.lanes[payload_idx].matched) {
					EmitSljitSharedBinaryPayloadLane(compiler, codegen_plan.shared_binary.lanes[payload_idx], 0,
					                                 shared_value_offset, SljitAggregateExpressionIndexMode::FLAT,
					                                 overflows);
				} else if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
					EmitLoadFusedAggregateReferenceValue(compiler, payload, false, false, SLJIT_S1);
				} else {
					idx_t payload_spill_index = 0;
					EmitSljitTypedExpressionTreeFastValueReg(compiler, *payload.expression_tree, payload_spill_index,
					                                         overflows);
				}
				EmitSljitGroupedAggregateAccumulateSumImmediate(compiler, descriptor.primitive_kind, SLJIT_S4,
				                                                state_offset, descriptor.state_value_offset,
				                                                descriptor.state_is_set_offset, SLJIT_R2);
			}
		};

		vector<sljit_jump *> use_generic_address_fast_loop;
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_address_sel));
		use_generic_address_fast_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));

		auto fast_direct_loop = EmitSljitRowLoopHead(compiler, fast_direct_done);
		EmitLoadGroupedAggregateStateAddressByLoopIndex(compiler, SLJIT_S4);
		emit_flat_fast_payloads();
		EmitSljitRowLoopLatch(compiler, fast_direct_loop, fast_direct_done);

		SetSljitJumpLabels(use_generic_address_fast_loop, sljit_emit_label(compiler));
		auto fast_loop = EmitSljitRowLoopHead(compiler, fast_done);
		EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_S4, SLJIT_S1);
		emit_flat_fast_payloads();
		EmitSljitRowLoopLatch(compiler, fast_loop, fast_done);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, expression_tree_all_valid));
		use_generic_loop = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);

		auto emit_selected_fast_payloads = [&]() {
			if (codegen_plan.shared_binary.Enabled()) {
				EmitSljitSharedBinaryPayloadBase(compiler, codegen_plan.shared_binary, 0, shared_value_offset,
				                                 SljitAggregateExpressionIndexMode::SELECTED, overflows);
			}
			for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
				auto &descriptor = codegen_plan.payload_descriptors[payload_idx];
				const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
				if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
					sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
					EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_R0, state_offset,
					                                                 descriptor.state_value_offset);
					sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
					continue;
				}

				auto &payload = payloads[payload_idx];
				if (codegen_plan.shared_binary.Enabled() && codegen_plan.shared_binary.lanes[payload_idx].matched) {
					EmitSljitSharedBinaryPayloadLane(compiler, codegen_plan.shared_binary.lanes[payload_idx], 0,
					                                 shared_value_offset, SljitAggregateExpressionIndexMode::SELECTED,
					                                 overflows);
				} else if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
					EmitLoadFusedAggregateReferenceValue(compiler, payload, true, false, SLJIT_S3);
				} else {
					idx_t payload_spill_index = 0;
					EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, *payload.expression_tree,
					                                                 payload_spill_index, overflows);
				}

				sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
				EmitSljitGroupedAggregateAccumulateSumImmediate(compiler, descriptor.primitive_kind, SLJIT_S3,
				                                                state_offset, descriptor.state_value_offset,
				                                                descriptor.state_is_set_offset, SLJIT_R2);
				sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
			}
		};

		vector<sljit_jump *> use_indexed_selected_fast_loop;
		sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_addresses_by_loop_index));
		use_indexed_selected_fast_loop.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_address_sel));
		use_indexed_selected_fast_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));

		auto selected_by_loop_fast_loop = EmitSljitRowLoopHead(compiler, selected_by_loop_fast_done);
		EmitLoadSljitExpressionTreeLogicalIndex(compiler);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), logical_index_offset, SLJIT_S3, 0);
		EmitLoadGroupedAggregateStateAddressByLoopIndex(compiler, SLJIT_R0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), state_ptr_offset, SLJIT_R0, 0);
		emit_selected_fast_payloads();
		EmitSljitRowLoopLatch(compiler, selected_by_loop_fast_loop, selected_by_loop_fast_done);

		SetSljitJumpLabels(use_indexed_selected_fast_loop, sljit_emit_label(compiler));
		auto selected_fast_loop = EmitSljitRowLoopHead(compiler, selected_fast_done);
		EmitLoadSljitExpressionTreeLogicalIndex(compiler);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), logical_index_offset, SLJIT_S3, 0);
		EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_R0, SLJIT_S3);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), state_ptr_offset, SLJIT_R0, 0);
		emit_selected_fast_payloads();
		EmitSljitRowLoopLatch(compiler, selected_fast_loop, selected_fast_done);

		sljit_set_label(use_generic_loop, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	vector<sljit_jump *> done;
	auto loop = EmitSljitRowLoopHead(compiler, done);
	EmitLoadSljitExpressionTreeLogicalIndex(compiler);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), logical_index_offset, SLJIT_S3, 0);
	EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_R0, SLJIT_S3);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_MEM1(SLJIT_SP), state_ptr_offset, SLJIT_R0, 0);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &descriptor = codegen_plan.payload_descriptors[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];

		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_SP), state_ptr_offset);
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_R0, state_offset,
			                                                 descriptor.state_value_offset);
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_SP), logical_index_offset);
			continue;
		}

		auto &payload = payloads[payload_idx];
		vector<sljit_jump *> payload_skip_jumps;
		if (payload.kind == SljitNativeRegionExpressionKind::REFERENCE) {
			auto source_is_null = EmitLoadFusedAggregateReferenceValue(compiler, payload, true, true, SLJIT_S3);
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
		EmitSljitGroupedAggregateAccumulateSumImmediate(compiler, descriptor.primitive_kind, SLJIT_S3, state_offset,
		                                                descriptor.state_value_offset, descriptor.state_is_set_offset,
		                                                SLJIT_R2);
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
	EmitSljitRowLoopLatch(compiler, loop, done);

	auto helper_done = EmitSljitArithmeticOverflowStubs(compiler, overflows);

	auto done_label = sljit_emit_label(compiler);
	SetSljitJumpLabels(fast_done, done_label);
	SetSljitJumpLabels(fast_direct_done, done_label);
	SetSljitJumpLabels(selected_fast_done, done_label);
	SetSljitJumpLabels(selected_by_loop_fast_done, done_label);
	SetSljitJumpLabels(done, done_label);
	for (auto jump : helper_done) {
		sljit_set_label(jump, sljit_emit_label(compiler));
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
