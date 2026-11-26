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

//! A row whose state address in S4 is null (its group is not resolved yet) is appended to the deferred rows by its
//! input index in row_index_reg; the returned jump skips the row
static sljit_jump *EmitSljitDeferRowWithUnresolvedState(struct sljit_compiler *compiler, sljit_s32 row_index_reg) {
	auto resolved = sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_S4, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_deferred_rows));
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_deferred_row_count));
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 2, row_index_reg, 0);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_IMM, 1);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_deferred_row_count), SLJIT_R2, 0);
	auto skip = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(resolved, sljit_emit_label(compiler));
	return skip;
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

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5 | SLJIT_ENTER_FLOAT(2), 6, 0);
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
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 descriptor.state_value_offset);
		}
		EmitNextSljitNativeVectorLoop(compiler, fast_loop);

		auto generic_loop = sljit_emit_label(compiler);
		for (auto jump : use_generic_loop) {
			sljit_set_label(jump, generic_loop);
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S1, 0, SLJIT_IMM, 0);
	}

	// a valid payload value of the row at index_reg updates the states at S4
	auto emit_valid_payload_update = [&](idx_t payload_idx, sljit_s32 index_reg) {
		auto &descriptor = descriptors[payload_idx];
		auto &payload = payloads[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
		const auto source_data_array = offsetof(SljitNativeVectorInput, source_data_array);
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 descriptor.state_value_offset);
		} else if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_INT64) {
			EmitLoadFusedAggregateIntegerData(compiler, source_data_array, payload_idx, payload.integer_kind, index_reg,
			                                  SLJIT_R2);
			EmitSljitGroupedAggregateAccumulateInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                  descriptor.state_value_offset,
			                                                  descriptor.state_is_set_offset, SLJIT_R2);
		} else if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			if (descriptor.IsDoubleWord()) {
				EmitLoadFusedAggregateHugeintData(compiler, source_data_array, payload_idx, index_reg, SLJIT_R2,
				                                  SLJIT_R3);
				EmitSljitGroupedAggregateAccumulateHugeintValueImmediate(
				    compiler, SLJIT_S4, state_offset, descriptor.state_value_offset, descriptor.state_is_set_offset,
				    SLJIT_R2, SLJIT_R3);
			} else {
				EmitLoadFusedAggregateIntegerData(compiler, source_data_array, payload_idx, payload.integer_kind,
				                                  index_reg, SLJIT_R2);
				EmitSljitGroupedAggregateAccumulateHugeintImmediate(compiler, SLJIT_S4, state_offset,
				                                                    descriptor.state_value_offset,
				                                                    descriptor.state_is_set_offset, SLJIT_R2);
			}
		} else {
			EmitLoadFusedAggregateDoubleData(compiler, source_data_array, payload_idx, index_reg, SLJIT_FR0);
			EmitSljitGroupedAggregateAccumulateDoubleImmediate(compiler, SLJIT_S4, state_offset,
			                                                   descriptor.state_value_offset,
			                                                   descriptor.state_is_set_offset, SLJIT_FR0);
		}
	};

	// Rows that reach their states through a selection of addresses (one address per distinct group, as a storage
	// dictionary's groups resolve), with flat payloads and no NULLs: the bases stay in registers and nothing is
	// checked per row
	vector<sljit_jump *> use_generic_loop;
	for (auto offset :
	     {offsetof(SljitNativeVectorInput, execute_sel), offsetof(SljitNativeVectorInput, source_sel_array),
	      offsetof(SljitNativeVectorInput, source_validity_array)}) {
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), NumericCast<sljit_sw>(offset));
		use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	}
	sljit_emit_op1(compiler, SLJIT_MOV_U8, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses_by_loop_index));
	use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_NOT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S3, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_address_sel));
	use_generic_loop.push_back(sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_S3, 0, SLJIT_IMM, 0));
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S5, 0, SLJIT_MEM1(SLJIT_S0),
	               offsetof(SljitNativeVectorInput, aggregate_state_addresses));
	auto selected_address_loop = sljit_emit_label(compiler);
	auto selected_address_done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, SLJIT_R1, 0, SLJIT_MEM2(SLJIT_S3, SLJIT_S1), 2);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_S4, 0, SLJIT_MEM2(SLJIT_S5, SLJIT_R1), 3);
	auto skip_selected_address_row = EmitSljitDeferRowWithUnresolvedState(compiler, SLJIT_S1);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &descriptor = descriptors[payload_idx];
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4,
			                                                 contract.grouped_state_offsets[descriptor.aggregate_index],
			                                                 descriptor.state_value_offset);
			continue;
		}
		emit_valid_payload_update(payload_idx, SLJIT_S1);
	}
	sljit_set_label(skip_selected_address_row, sljit_emit_label(compiler));
	EmitNextSljitNativeVectorLoop(compiler, selected_address_loop);
	auto generic_loop_start = sljit_emit_label(compiler);
	for (auto jump : use_generic_loop) {
		sljit_set_label(jump, generic_loop_start);
	}

	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);
	EmitLoadGroupedAggregateStateAddress(compiler, SLJIT_S4, SLJIT_S3);
	auto skip_row = EmitSljitDeferRowWithUnresolvedState(compiler, SLJIT_S3);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto &descriptor = descriptors[payload_idx];
		const auto state_offset = contract.grouped_state_offsets[descriptor.aggregate_index];
		if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitSljitGroupedAggregateIncrementInt64Immediate(compiler, SLJIT_S4, state_offset,
			                                                 descriptor.state_value_offset);
			continue;
		}
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1, true);
		auto source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1);
		emit_valid_payload_update(payload_idx, SLJIT_R1);
		sljit_set_label(source_is_null, sljit_emit_label(compiler));
	}
	sljit_set_label(skip_row, sljit_emit_label(compiler));

	EmitNextSljitNativeVectorLoop(compiler, loop);

	auto done_label = sljit_emit_label(compiler);
	if (fast_count_done) {
		sljit_set_label(fast_count_done, done_label);
	}
	sljit_set_label(selected_address_done, done_label);
	sljit_set_label(done, done_label);
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
