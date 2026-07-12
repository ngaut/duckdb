#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "sljitLir.h"

namespace duckdb {

static bool SljitGroupedFusedCountOnlyFastPathSupported(const vector<SljitAggregatePayloadDescriptor> &descriptors) {
	for (auto &descriptor : descriptors) {
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (descriptor.primitive_kind != AggregatePrimitiveUpdateKind::COUNT) {
			return false;
		}
	}
	return true;
}

static void EmitSljitGroupedFusedCountOnlyFastPathGuards(struct sljit_compiler *compiler, bool has_count_payload,
                                                         vector<sljit_jump *> &use_generic_loop) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, execute_sel));
	use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_address_sel));
	use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	if (!has_count_payload) {
		return;
	}
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, source_validity_array));
	use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
}

unique_ptr<ExecutionRegionCodeHandle> BuildSljitNativeGroupedFusedPrimitiveAggregateUpdate(
    const vector<SljitNativeRegionExpressionPlan> &payloads, const vector<ExecutionRegionAggregateInput> &aggregates,
    const ExecutionRegionAggregateContract &contract, SljitNativeAggregateUpdateFunction &function, string &error) {
	if (!contract.grouped_state_layout_ready || payloads.size() != aggregates.size() || payloads.empty()) {
		error = "unsupported fused grouped aggregate payload shape";
		return nullptr;
	}
	vector<SljitAggregatePayloadDescriptor> descriptors(payloads.size());
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedGroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx],
		                                                         contract) ||
		    !SljitTryBindAggregatePayloadDescriptor(payloads[payload_idx], aggregates[payload_idx],
		                                            descriptors[payload_idx])) {
			error = "unsupported fused grouped aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 5, 0);
	EmitInitSljitNativeVectorLoop(compiler);

	struct sljit_jump *fast_count_done = nullptr;
	if (SljitGroupedFusedCountOnlyFastPathSupported(descriptors)) {
		bool has_count_payload = false;
		for (auto &descriptor : descriptors) {
			has_count_payload = has_count_payload || descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT;
		}

		vector<sljit_jump *> use_generic_loop;
		EmitSljitGroupedFusedCountOnlyFastPathGuards(compiler, has_count_payload, use_generic_loop);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
		auto fast_loop = sljit_emit_label(compiler);
		fast_count_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM2(SLJIT_S3, SLJIT_S1), 3);
		for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
			auto &descriptor = descriptors[payload_idx];
			const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
			EmitSljitGroupedAggregateIncrementInt64ImmediateDirect(compiler, SLJIT_S4, state_offset,
			                                                       descriptor.state_value_offset);
		}
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		auto generic_loop = sljit_emit_label(compiler);
		for (auto jump : use_generic_loop) {
			sljit_set_label(jump, generic_loop);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_S4, SLJIT_S3);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &descriptor = descriptors[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 descriptor.state_value_offset);
			continue;
		}

		auto &payload = payloads[payload_idx];
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1, true);
		auto source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1);
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 descriptor.state_value_offset);
		} else if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array),
			                                  payload_idx, payload.integer_kind, SLJIT_R1, SLJIT_R2);
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                  descriptor.state_value_offset,
			                                                  descriptor.state_is_set_offset, SLJIT_R2);
		} else if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			if (descriptor.IsDoubleWord()) {
				EmitLoadFusedAggregateHugeintData(compiler, offsetof(SljitNativeVectorInput, source_data_array),
				                                  payload_idx, SLJIT_R1, SLJIT_R2, SLJIT_R3);
				EmitSljitGroupedAggregateAccumulateHugeintValueImmediate(
				    compiler, SLJIT_S4, state_offset, descriptor.state_value_offset, descriptor.state_is_set_offset,
				    SLJIT_R2, SLJIT_R3);
			} else {
				EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array),
				                                  payload_idx, payload.integer_kind, SLJIT_R1, SLJIT_R2);
				EmitSljitGroupedAggregateAccumulateHugeintImmediate(compiler, SLJIT_S4, state_offset,
				                                                    descriptor.state_value_offset,
				                                                    descriptor.state_is_set_offset, SLJIT_R2);
			}
		} else {
			EmitLoadFusedAggregateDoubleData(compiler, offsetof(SljitNativeVectorInput, source_data_array), payload_idx,
			                                 SLJIT_R1, SLJIT_TMP_FR0);
			EmitSljitGroupedAggregateAccumulateDoubleImmediate(compiler, SLJIT_S4, state_offset,
			                                                   descriptor.state_value_offset,
			                                                   descriptor.state_is_set_offset, SLJIT_TMP_FR0);
		}
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		sljit_set_label(source_is_null, payload_invalid);
		sljit_set_label(payload_done, sljit_emit_label(compiler));
	}

	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	if (fast_count_done) {
		sljit_set_label(fast_count_done, done_label);
	}
	sljit_set_label(done, done_label);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
