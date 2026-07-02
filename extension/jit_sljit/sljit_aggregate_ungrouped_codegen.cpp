#include "sljit_native_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_primitive_codegen.hpp"
#include "sljit_aggregate_ungrouped_shared_codegen.hpp"
#include "sljit_codegen_internal.hpp"
#include "sljit_codegen_util.hpp"

#include "sljitLir.h"

namespace duckdb {

static bool SljitFusedUngroupedPrimitiveAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                                  const ExecutionRegionAggregateInput &aggregate) {
	if (!aggregate.primitive_update_ready) {
		return false;
	}
	if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
		return aggregate.child_count == 0 && aggregate.child_types.empty();
	}
	if (aggregate.primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64 ||
	    aggregate.child_types.size() != 1 ||
	    aggregate.primitive_update_input_type != aggregate.child_types[0].InternalType() ||
	    payload.return_type.InternalType() != aggregate.child_types[0].InternalType()) {
		return false;
	}
	switch (payload.kind) {
	case SljitNativeRegionExpressionKind::REFERENCE:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT:
	case SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES:
		return true;
	default:
		return false;
	}
}

unique_ptr<ExecutionRegionCodeHandle>
BuildSljitNativeUngroupedFusedPrimitiveAggregateUpdate(const vector<SljitNativeRegionExpressionPlan> &payloads,
                                                       const vector<ExecutionRegionAggregateInput> &aggregates,
                                                       SljitNativeAggregateUpdateFunction &function, string &error) {
	if (payloads.size() != aggregates.size() || payloads.size() < 2) {
		error = "unsupported fused aggregate payload shape";
		return nullptr;
	}
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (!SljitFusedUngroupedPrimitiveAggregatePayloadSupported(payloads[payload_idx], aggregates[payload_idx])) {
			error = "unsupported fused aggregate payload shape";
			return nullptr;
		}
	}

	auto compiler = sljit_create_compiler(nullptr);
	if (!compiler) {
		error = "failed to create SLJIT compiler";
		return nullptr;
	}

	vector<sljit_sw> local_sum_offsets(payloads.size(), -1);
	vector<sljit_sw> saw_value_offsets(payloads.size(), -1);
	bool has_sum_lane = false;
	sljit_sw local_size = 0;
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind != AggregatePrimitiveUpdateKind::SUM_INT64) {
			continue;
		}
		has_sum_lane = true;
		local_sum_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
		saw_value_offsets[payload_idx] = local_size;
		local_size += NumericCast<sljit_sw>(sizeof(sljit_sw));
	}
	if (!has_sum_lane) {
		sljit_free_compiler(compiler);
		error = "unsupported fused aggregate payload shape";
		return nullptr;
	}

	sljit_emit_enter(compiler, 0, SLJIT_ARGS1V(P), 5, 5, local_size);
	EmitInitSljitNativeVectorLoop(compiler);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (local_sum_offsets[payload_idx] < 0) {
			continue;
		}
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), local_sum_offsets[payload_idx], SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offsets[payload_idx], SLJIT_IMM, 0);
	}

	vector<SljitExpressionTreeOverflowJumps> overflows;
	auto loop = sljit_emit_label(compiler);
	auto done = sljit_emit_cmp(compiler, SLJIT_GREATER_EQUAL, SLJIT_S1, 0, SLJIT_S2, 0);
	EmitLoadFusedAggregateExecuteIndex(compiler);

	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		auto &payload = payloads[payload_idx];
		vector<sljit_jump *> invalid_jumps;
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array), payload_idx,
		                                  SLJIT_R1, true);
		invalid_jumps.push_back(EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload_idx, SLJIT_R1));
		if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, right_source_sel_array),
			                                  payload_idx, SLJIT_S4);
			invalid_jumps.push_back(EmitFusedAggregateJumpIfValidityNull(
			    compiler, offsetof(SljitNativeVectorInput, right_source_validity_array), payload_idx, SLJIT_S4));
		}
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array), payload_idx,
		                                  payload.integer_kind, SLJIT_R1, SLJIT_R2);
		if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_CONSTANT) {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
			               offsetof(SljitNativeVectorInput, constants));
			sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM1(SLJIT_R0),
			               NumericCast<sljit_sw>(payload_idx * sizeof(int64_t)));
			if (payload.constant_on_left) {
				sljit_emit_op2(compiler,
				               payload.check_arithmetic_overflow
				                   ? NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW
				                   : NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op),
				               SLJIT_R2, 0, SLJIT_R3, 0, SLJIT_R2, 0);
			} else {
				sljit_emit_op2(compiler,
				               payload.check_arithmetic_overflow
				                   ? NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW
				                   : NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op),
				               SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			}
			if (payload.check_arithmetic_overflow) {
				AddSljitExpressionOverflowJump(overflows, payload.binary_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
			}
		} else if (payload.kind == SljitNativeRegionExpressionKind::INTEGER_BINARY_REFERENCES) {
			EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, right_source_data_array),
			                                  payload_idx, payload.integer_kind, SLJIT_S4, SLJIT_R3);
			sljit_emit_op2(compiler,
			               payload.check_arithmetic_overflow
			                   ? NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op) | SLJIT_SET_OVERFLOW
			                   : NativeIntegerBinaryOp(payload.integer_kind, payload.binary_op),
			               SLJIT_R2, 0, SLJIT_R2, 0, SLJIT_R3, 0);
			if (payload.check_arithmetic_overflow) {
				AddSljitExpressionOverflowJump(overflows, payload.binary_op, sljit_emit_jump(compiler, SLJIT_OVERFLOW));
			}
		}
		if (payload.check_result_range) {
			AddSljitExpressionOverflowJump(overflows, payload.binary_op,
			                               sljit_emit_cmp(compiler, SLJIT_SIG_LESS, SLJIT_R2, 0, SLJIT_IMM,
			                                              NumericCast<sljit_sw>(payload.result_min)));
			AddSljitExpressionOverflowJump(overflows, payload.binary_op,
			                               sljit_emit_cmp(compiler, SLJIT_SIG_GREATER, SLJIT_R2, 0, SLJIT_IMM,
			                                              NumericCast<sljit_sw>(payload.result_max)));
		}
		EmitSljitAggregateAccumulateInt64(compiler, local_sum_offsets[payload_idx], saw_value_offsets[payload_idx],
		                                  SLJIT_R2);
		auto payload_done = sljit_emit_jump(compiler, SLJIT_JUMP);
		auto payload_invalid = sljit_emit_label(compiler);
		for (auto invalid_jump : invalid_jumps) {
			sljit_set_label(invalid_jump, payload_invalid);
		}
		sljit_set_label(payload_done, sljit_emit_label(compiler));
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
	sljit_set_label(done, done_label);
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		if (aggregates[payload_idx].primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			EmitUngroupedAggregateCommitCountStar(compiler, payload_idx);
		} else {
			EmitUngroupedAggregateCommitSumInt64(compiler, payload_idx, local_sum_offsets[payload_idx],
			                                     saw_value_offsets[payload_idx]);
		}
	}
	auto return_label = sljit_emit_label(compiler);
	for (auto jump : helper_done) {
		sljit_set_label(jump, return_label);
	}
	sljit_emit_return_void(compiler);

	return FinishSljitCode(compiler, function, error);
}

} // namespace duckdb
