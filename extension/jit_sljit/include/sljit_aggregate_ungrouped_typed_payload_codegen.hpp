#pragma once

#include "sljit_aggregate_typed_payload_codegen.hpp"
#include "sljit_aggregate_ungrouped_typed_codegen.hpp"

namespace duckdb {

void EmitFusedTypedConditionalSharedSawElseZero(struct sljit_compiler *compiler, sljit_sw saw_value_offset);
void EmitSljitUngroupedTypedConditionalSharedFastPayload(
    struct sljit_compiler *compiler, const SljitFusedAggregateCodegenPlan &codegen_plan,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<sljit_sw> &local_sum_offsets,
    const vector<sljit_sw> &local_sum_upper_offsets, const vector<sljit_sw> &saw_value_offsets,
    sljit_sw shared_fast_value_offset, bool use_conditional_hugeint_register_accumulators,
    SljitAggregateExpressionIndexMode index_mode, vector<SljitArithmeticOverflowSite> &overflows,
    const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists);
//! The registers of an accumulator pair: the shared and the conditional lane's
void GetSljitUngroupedAggregateAccumulatorPair(idx_t pair, sljit_s32 &lower, sljit_s32 &upper);
void EmitSljitUngroupedTypedFastPayloads(
    struct sljit_compiler *compiler, const vector<SljitNativeRegionExpressionPlan> &payloads,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<sljit_sw> &local_sum_offsets,
    const vector<sljit_sw> &local_sum_upper_offsets, const vector<sljit_sw> &saw_value_offsets,
    const vector<idx_t> &register_accumulator_pairs, SljitAggregateExpressionIndexMode index_mode,
    vector<SljitArithmeticOverflowSite> &overflows, const vector<SljitTypedExpressionTreeLoopHoist> *data_hoists);

} // namespace duckdb
