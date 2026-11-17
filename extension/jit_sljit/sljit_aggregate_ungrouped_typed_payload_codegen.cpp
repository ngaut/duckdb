#include "sljit_aggregate_ungrouped_typed_payload_codegen.hpp"

#include "sljit_aggregate_fused_codegen.hpp"
#include "sljit_aggregate_ungrouped_shared_codegen.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

void EmitFusedTypedConditionalSharedSawElseZero(struct sljit_compiler *compiler, sljit_sw saw_value_offset) {
	sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), saw_value_offset, SLJIT_IMM, 1);
}

static void EmitSljitUngroupedTypedFastPayloadValueReg(struct sljit_compiler *compiler,
                                                       const ExecutionExpressionIR &expression,
                                                       SljitAggregateExpressionIndexMode index_mode,
                                                       vector<SljitArithmeticOverflowSite> &overflows,
                                                       const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	idx_t payload_spill_index = 0;
	switch (index_mode) {
	case SljitAggregateExpressionIndexMode::FLAT:
		EmitSljitTypedExpressionTreeFastValueReg(compiler, expression, payload_spill_index, overflows, data_hoists);
		return;
	case SljitAggregateExpressionIndexMode::LOGICAL:
		EmitSljitTypedExpressionTreeLogicalFastValueReg(compiler, expression, payload_spill_index, overflows,
		                                                data_hoists);
		return;
	case SljitAggregateExpressionIndexMode::SELECTED:
		EmitSljitTypedExpressionTreeSelectedFastValueReg(compiler, expression, payload_spill_index, overflows,
		                                                 data_hoists);
		return;
	}
	throw InternalException("Unsupported SLJIT ungrouped aggregate expression index mode");
}

void EmitSljitUngroupedTypedConditionalSharedFastPayload(
    struct sljit_compiler *compiler, const SljitFusedAggregateCodegenPlan &codegen_plan,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<sljit_sw> &local_sum_offsets,
    const vector<sljit_sw> &local_sum_upper_offsets, const vector<sljit_sw> &saw_value_offsets,
    sljit_sw shared_fast_value_offset, bool use_conditional_hugeint_register_accumulators,
    SljitAggregateExpressionIndexMode index_mode, vector<SljitArithmeticOverflowSite> &overflows,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	auto shared_lane = codegen_plan.shared_lane;
	auto conditional_lane = codegen_plan.conditional_lane;
	auto kind = aggregates[shared_lane].primitive_update_kind;
	EmitSljitUngroupedTypedFastPayloadValueReg(compiler, *codegen_plan.shared_value, index_mode, overflows,
	                                           data_hoists);
	if (use_conditional_hugeint_register_accumulators) {
		const auto &registers = GetSljitUngroupedAggregateRegisterLayout();
		sljit_emit_op1(compiler, SLJIT_MOV, registers.conditional_payload, 0, SLJIT_R2, 0);
		EmitUngroupedAggregateAccumulateHugeintInt64Regs(compiler, registers.shared_lower, registers.shared_upper,
		                                                 registers.conditional_payload);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_MEM1(SLJIT_SP), shared_fast_value_offset, SLJIT_R2, 0);
		EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[shared_lane],
		                                 local_sum_upper_offsets[shared_lane], saw_value_offsets[shared_lane],
		                                 SLJIT_R2);
	}

	EmitSljitUngroupedTypedFastPayloadValueReg(compiler, *codegen_plan.conditional_predicate, index_mode, overflows,
	                                           data_hoists);
	auto condition_false = sljit_emit_cmp(compiler, SLJIT_EQUAL, SLJIT_R2, 0, SLJIT_IMM, 0);
	if (use_conditional_hugeint_register_accumulators) {
		const auto &registers = GetSljitUngroupedAggregateRegisterLayout();
		EmitUngroupedAggregateAccumulateHugeintInt64Regs(compiler, registers.conditional_lower,
		                                                 registers.conditional_upper, registers.conditional_payload);
	} else {
		sljit_emit_op1(compiler, SLJIT_MOV, SLJIT_R2, 0, SLJIT_MEM1(SLJIT_SP), shared_fast_value_offset);
		EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[conditional_lane],
		                                 local_sum_upper_offsets[conditional_lane], saw_value_offsets[conditional_lane],
		                                 SLJIT_R2);
	}
	auto conditional_done = sljit_emit_jump(compiler, SLJIT_JUMP);
	sljit_set_label(condition_false, sljit_emit_label(compiler));
	if (!use_conditional_hugeint_register_accumulators) {
		EmitFusedTypedConditionalSharedSawElseZero(compiler, saw_value_offsets[conditional_lane]);
	}
	sljit_set_label(conditional_done, sljit_emit_label(compiler));
}

void GetSljitUngroupedAggregateAccumulatorPair(idx_t pair, sljit_s32 &lower, sljit_s32 &upper) {
	const auto &registers = GetSljitUngroupedAggregateRegisterLayout();
	D_ASSERT(registers.has_register_accumulators && pair < 2);
	lower = pair == 0 ? registers.shared_lower : registers.conditional_lower;
	upper = pair == 0 ? registers.shared_upper : registers.conditional_upper;
}

void EmitSljitUngroupedTypedFastPayloads(
    struct sljit_compiler *compiler, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<sljit_sw> &local_sum_offsets,
    const vector<sljit_sw> &local_sum_upper_offsets, const vector<sljit_sw> &saw_value_offsets,
    const vector<idx_t> &register_accumulator_pairs, SljitAggregateExpressionIndexMode index_mode,
    vector<SljitArithmeticOverflowSite> &overflows, const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists) {
	for (idx_t payload_idx = 0; payload_idx < payloads.size(); payload_idx++) {
		auto kind = aggregates[payload_idx].primitive_update_kind;
		if (kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			continue;
		}
		if (payloads[payload_idx].kind == SljitNativeRegionExpressionKind::REFERENCE) {
			EmitLoadFusedAggregateReferenceValue(
			    compiler, payloads[payload_idx], index_mode == SljitAggregateExpressionIndexMode::SELECTED, false,
			    index_mode == SljitAggregateExpressionIndexMode::FLAT ? SLJIT_S1 : SLJIT_S3, data_hoists);
		} else {
			EmitSljitUngroupedTypedFastPayloadValueReg(compiler, *payloads[payload_idx].expression_tree, index_mode,
			                                           overflows, data_hoists);
		}
		const auto pair = register_accumulator_pairs[payload_idx];
		if (pair == DConstants::INVALID_INDEX) {
			EmitUngroupedAggregateAccumulate(compiler, kind, local_sum_offsets[payload_idx],
			                                 local_sum_upper_offsets[payload_idx], saw_value_offsets[payload_idx],
			                                 SLJIT_R2);
			continue;
		}
		sljit_s32 lower;
		sljit_s32 upper;
		GetSljitUngroupedAggregateAccumulatorPair(pair, lower, upper);
		if (kind == AggregatePrimitiveUpdateKind::SUM_HUGEINT) {
			EmitUngroupedAggregateAccumulateHugeintInt64Regs(compiler, lower, upper, SLJIT_R2);
		} else {
			sljit_emit_op2(compiler, SLJIT_ADD, lower, 0, lower, 0, SLJIT_R2, 0);
		}
	}
}

} // namespace duckdb
