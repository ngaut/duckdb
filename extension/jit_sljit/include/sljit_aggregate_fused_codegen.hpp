//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_aggregate_fused_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_codegen_internal.hpp"
#include "sljit_region_plan.hpp"
#include "sljit_typed_expression_plan.hpp"

namespace duckdb {

void EmitLoadFusedAggregateExecuteIndex(struct sljit_compiler *compiler, bool direct_logical_index = false);
void EmitLoadFusedAggregateSourceIndex(struct sljit_compiler *compiler, sljit_sw source_sel_array_offset,
                                       idx_t lane_idx, sljit_s32 target_reg, bool use_common_source_selection = false);
struct sljit_jump *EmitFusedAggregateJumpIfValidityNull(struct sljit_compiler *compiler, sljit_sw validity_array_offset,
                                                        idx_t lane_idx, sljit_s32 index_reg);
void EmitLoadFusedAggregateIntegerData(struct sljit_compiler *compiler, sljit_sw source_data_array_offset,
                                       idx_t lane_idx, SljitNativeIntegerKind kind, sljit_s32 index_reg,
                                       sljit_s32 target_reg);
void EmitLoadFusedAggregateDoubleData(struct sljit_compiler *compiler, sljit_sw source_data_array_offset,
                                      idx_t lane_idx, sljit_s32 index_reg, sljit_s32 target_freg);
sljit_jump *EmitLoadFusedTypedAggregateReferenceValue(
    struct sljit_compiler *compiler, const SljitNativeRegionExpressionPlan &payload, bool use_source_selection,
    bool check_validity, sljit_s32 direct_index_reg,
    const vector<SljitTypedExpressionTreeDataPointerHoist> *data_hoists = nullptr);

bool SljitAggregateTypedPayloadPlanSupported(const SljitTypedExpressionTreePlan &payload_plan,
                                             const ExecutionRegionAggregateInput &aggregate);
bool SljitFusedGroupedPrimitiveAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                         const ExecutionRegionAggregateInput &aggregate,
                                                         const ExecutionRegionAggregateContract &contract);
bool SljitFusedGroupedTypedAggregatePayloadSupported(const SljitNativeRegionExpressionPlan &payload,
                                                     const ExecutionRegionAggregateInput &aggregate,
                                                     const ExecutionRegionAggregateContract &contract);

} // namespace duckdb
