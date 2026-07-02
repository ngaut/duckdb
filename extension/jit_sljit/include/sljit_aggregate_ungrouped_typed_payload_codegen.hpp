#pragma once

#include "sljit_aggregate_ungrouped_typed_codegen.hpp"

namespace duckdb {

void EmitFusedTypedConditionalSharedSawElseZero(struct sljit_compiler *compiler, sljit_sw saw_value_offset);
void EmitSljitUngroupedTypedConditionalSharedFastPayload(
    struct sljit_compiler *compiler, const SljitFusedTypedAggregateCodegenPlan &codegen_plan,
    const vector<ExecutionRegionAggregateInput> &aggregates, const vector<sljit_sw> &local_sum_offsets,
    const vector<sljit_sw> &local_sum_upper_offsets, const vector<sljit_sw> &saw_value_offsets,
    sljit_sw shared_fast_value_offset, bool use_conditional_hugeint_register_accumulators, bool selected,
    vector<SljitExpressionTreeOverflowJumps> &overflows,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists);
void EmitSljitUngroupedTypedFastPayloads(struct sljit_compiler *compiler,
                                         const vector<SljitNativeRegionExpressionPlan> &payloads,
                                         const vector<ExecutionRegionAggregateInput> &aggregates,
                                         const vector<sljit_sw> &local_sum_offsets,
                                         const vector<sljit_sw> &local_sum_upper_offsets,
                                         const vector<sljit_sw> &saw_value_offsets, bool selected,
                                         vector<SljitExpressionTreeOverflowJumps> &overflows,
                                         const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists);

} // namespace duckdb
