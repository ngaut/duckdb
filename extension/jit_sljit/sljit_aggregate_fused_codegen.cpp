#include "sljit_aggregate_fused_codegen.hpp"

#include "sljit_aggregate_payload_descriptor.hpp"
#include "sljit_codegen_util.hpp"

namespace duckdb {

void EmitLoadFusedAggregateExecuteIndex(struct sljit_compiler *compiler, bool direct_logical_index) {
	if (direct_logical_index) {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_S3, 0, SLJIT_S1, 0);
		return;
	}
	EmitLoadLogicalIndex(compiler, SLJIT_S3);
}

void EmitLoadFusedAggregateSourceIndex(struct sljit_compiler *compiler, sljit_sw source_sel_array_offset,
                                       idx_t lane_idx, sljit_s32 target_reg, bool use_common_source_selection) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_sel_array_offset);
	auto no_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	auto no_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
	auto have_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
	if (use_common_source_selection) {
		auto use_common_source_sel = sljit_emit_label(compiler);
		sljit_set_label(no_array, use_common_source_sel);
		sljit_set_label(no_source_sel, use_common_source_sel);
		sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0),
		               offsetof(SljitNativeVectorInput, source_common_sel));
		auto no_common_source_sel = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
		sljit_emit_op1(compiler, SLJIT_MOV_U32, target_reg, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_S3), 2);
		auto have_common_source_index = sljit_emit_jump(compiler, SLJIT_JUMP);
		sljit_set_label(no_common_source_sel, sljit_emit_label(compiler));
		sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
		auto done = sljit_emit_label(compiler);
		sljit_set_label(have_source_index, done);
		sljit_set_label(have_common_source_index, done);
		return;
	}
	auto use_logical_index = sljit_emit_label(compiler);
	sljit_set_label(no_array, use_logical_index);
	sljit_set_label(no_source_sel, use_logical_index);
	sljit_emit_op1(compiler, SLJIT_MOV, target_reg, 0, SLJIT_S3, 0);
	sljit_set_label(have_source_index, sljit_emit_label(compiler));
}

struct sljit_jump *EmitFusedAggregateJumpIfValidityNull(struct sljit_compiler *compiler, sljit_sw validity_array_offset,
                                                        idx_t lane_idx, sljit_s32 index_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), validity_array_offset);
	auto source_all_valid_array = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	auto source_all_valid = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R0, 0, SLJIT_IMM, 0);
	sljit_emit_op2(compiler, SLJIT_LSHR, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 6);
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R3, 0, SLJIT_MEM2(SLJIT_R0, SLJIT_R2), 3);
	sljit_emit_op2(compiler, SLJIT_AND, SLJIT_R2, 0, index_reg, 0, SLJIT_IMM, 63);
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, SLJIT_IMM, 1, SLJIT_R2, 0);
	sljit_emit_op2(compiler, SLJIT_AND | SLJIT_SET_Z, SLJIT_R4, 0, SLJIT_R4, 0, SLJIT_R3, 0);
	auto source_is_null = sljit_emit_jump(compiler, SLJIT_EQUAL);
	auto all_valid = sljit_emit_label(compiler);
	sljit_set_label(source_all_valid_array, all_valid);
	sljit_set_label(source_all_valid, all_valid);
	return source_is_null;
}

void EmitLoadFusedAggregateIntegerData(struct sljit_compiler *compiler, sljit_sw source_data_array_offset,
                                       idx_t lane_idx, SljitNativeIntegerKind kind, sljit_s32 index_reg,
                                       sljit_s32 target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_data_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	sljit_emit_op1(compiler, NativeIntegerLoadOp(kind), target_reg, 0, SLJIT_MEM2(SLJIT_R0, index_reg),
	               NativeIntegerDataScale(kind));
}

void EmitLoadFusedAggregateHugeintData(struct sljit_compiler *compiler, sljit_sw source_data_array_offset,
                                       idx_t lane_idx, sljit_s32 index_reg, sljit_s32 lower_target_reg,
                                       sljit_s32 upper_target_reg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_data_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	sljit_emit_op2(compiler, SLJIT_SHL, SLJIT_R4, 0, index_reg, 0, SLJIT_IMM, 4);
	sljit_emit_op2(compiler, SLJIT_ADD, SLJIT_R0, 0, SLJIT_R0, 0, SLJIT_R4, 0);
	sljit_emit_op1(compiler, SLJIT_MOV, lower_target_reg, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, lower));
	sljit_emit_op1(compiler, SLJIT_MOV, upper_target_reg, 0, SLJIT_MEM1(SLJIT_R0), offsetof(hugeint_t, upper));
}

void EmitLoadFusedAggregateDoubleData(struct sljit_compiler *compiler, sljit_sw source_data_array_offset,
                                      idx_t lane_idx, sljit_s32 index_reg, sljit_s32 target_freg) {
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S0), source_data_array_offset);
	sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_R0), SljitPointerArrayOffset(lane_idx));
	sljit_emit_fmem(compiler, SLJIT_MOV_F64 | SLJIT_MEM_ALIGNED_32, target_freg, SLJIT_MEM2(SLJIT_R0, index_reg), 3);
}

sljit_jump *
EmitLoadFusedTypedAggregateReferenceValue(struct sljit_compiler *compiler,
                                          const SljitNativeRegionExpressionPlan &payload, bool use_source_selection,
                                          bool check_validity, sljit_s32 direct_index_reg,
                                          const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists) {
	if (!use_source_selection && !check_validity) {
		sljit_s32 data_reg;
		if (TryGetSljitTypedExpressionTreeDataPointerHoist(data_hoists, payload.source_index, data_reg)) {
			sljit_emit_op1(compiler, NativeIntegerLoadOp(payload.integer_kind), SLJIT_R2, 0,
			               SLJIT_MEM2(data_reg, direct_index_reg), NativeIntegerDataScale(payload.integer_kind));
			return nullptr;
		} else {
			sljit_emit_op1(compiler, SLJIT_MOV_P, SLJIT_R0, 0, SLJIT_MEM1(SLJIT_S5),
			               SljitPointerArrayOffset(payload.source_index));
		}
		sljit_emit_op1(compiler, NativeIntegerLoadOp(payload.integer_kind), SLJIT_R2, 0,
		               SLJIT_MEM2(SLJIT_R0, direct_index_reg), NativeIntegerDataScale(payload.integer_kind));
		return nullptr;
	}
	if (use_source_selection) {
		EmitLoadFusedAggregateSourceIndex(compiler, offsetof(SljitNativeVectorInput, source_sel_array),
		                                  payload.source_index, SLJIT_R1, true);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R1, 0, direct_index_reg, 0);
	}
	sljit_jump *source_is_null = nullptr;
	if (check_validity) {
		source_is_null = EmitFusedAggregateJumpIfValidityNull(
		    compiler, offsetof(SljitNativeVectorInput, source_validity_array), payload.source_index, SLJIT_R1);
	}
	sljit_s32 data_reg;
	if (TryGetSljitTypedExpressionTreeDataPointerHoist(data_hoists, payload.source_index, data_reg)) {
		sljit_emit_op1(compiler, NativeIntegerLoadOp(payload.integer_kind), SLJIT_R2, 0, SLJIT_MEM2(data_reg, SLJIT_R1),
		               NativeIntegerDataScale(payload.integer_kind));
	} else {
		EmitLoadFusedAggregateIntegerData(compiler, offsetof(SljitNativeVectorInput, source_data_array),
		                                  payload.source_index, payload.integer_kind, SLJIT_R1, SLJIT_R2);
	}
	return source_is_null;
}

bool SljitAggregateTypedPayloadPlanSupported(const SljitTypedExpressionTreePlan &payload_plan,
                                             const SljitAggregatePayloadDescriptor &descriptor) {
	if (!payload_plan.supported) {
		return false;
	}
	SljitNativeIntegerKind aggregate_payload_kind;
	return descriptor.TryGetTypedIntegerKind(aggregate_payload_kind) &&
	       payload_plan.result_kind == aggregate_payload_kind;
}

bool SljitFusedGroupedPrimitiveAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                         const ExecutionRegionAggregateInput &aggregate,
                                                         const ExecutionRegionAggregateContract &contract) {
	if (aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
		return false;
	}
	SljitAggregatePayloadDescriptor descriptor;
	if (!SljitTryBindAggregatePayloadDescriptor(payload, aggregate, descriptor)) {
		return false;
	}
	if (!descriptor.has_payload) {
		return true;
	}
	if (descriptor.value_abi == SljitAggregatePayloadValueABI::VALIDITY_ONLY) {
		return payload.kind == SljitNativeRegionExpressionKind::REFERENCE;
	}
	if (descriptor.primitive_kind == AggregatePrimitiveUpdateKind::SUM_DOUBLE) {
		return payload.kind == SljitNativeRegionExpressionKind::REFERENCE;
	}
	if (descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	    descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
		return false;
	}
	return payload.kind == SljitNativeRegionExpressionKind::REFERENCE;
}

bool SljitFusedGroupedTypedAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                     const ExecutionRegionAggregateInput &aggregate,
                                                     const ExecutionRegionAggregateContract &contract,
                                                     SljitAggregatePayloadDescriptor *bound_descriptor) {
	if (aggregate.aggregate_index >= contract.grouped_state_offsets.size()) {
		return false;
	}
	SljitAggregatePayloadDescriptor descriptor;
	if (!SljitTryBindAggregatePayloadDescriptor(payload, aggregate, descriptor)) {
		return false;
	}
	if (bound_descriptor) {
		*bound_descriptor = descriptor;
	}
	if (!descriptor.has_payload) {
		return true;
	}
	if (!descriptor.IsMachineWord() || (descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_INT64 &&
	                                    descriptor.primitive_kind != AggregatePrimitiveUpdateKind::SUM_HUGEINT)) {
		return false;
	}
	return payload.kind == SljitNativeRegionExpressionKind::REFERENCE ||
	       (payload.kind == SljitNativeRegionExpressionKind::TYPED_EXPRESSION_TREE &&
	        payload.expression_tree != nullptr);
}

} // namespace duckdb
