//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_codegen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_function_types.hpp"
#include "sljit_region_plan.hpp"

#include "duckdb/execution/jit/runtime.hpp"

namespace duckdb {

unique_ptr<JitCodeHandle> BuildSljitFusedIntegerFilterProjection(
    SljitNativeIntegerKind kind, SljitNativeIntegerCompareOp compare_op, bool compare_constant_on_left,
    SljitNativeIntegerBinaryOp projection_op, bool projection_constant_on_left,
    SljitFusedFilterProjectionFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitUngroupedCountStarUpdate(SljitNativeUngroupedAggregateFunction &function,
                                                             string &error);
unique_ptr<JitCodeHandle> BuildSljitUngroupedCountUpdate(SljitNativeUngroupedAggregateFunction &function,
                                                         string &error);
unique_ptr<JitCodeHandle> BuildSljitUngroupedSumInt64Update(SljitNativeUngroupedAggregateFunction &function,
                                                            string &error);
unique_ptr<JitCodeHandle> BuildSljitUngroupedSumHugeintInt64Update(SljitNativeUngroupedAggregateFunction &function,
                                                                   string &error);
unique_ptr<JitCodeHandle> BuildSljitGroupedCountStarUpdate(SljitNativeGroupedAggregateFunction &function,
                                                           string &error);
unique_ptr<JitCodeHandle> BuildSljitGroupedCountUpdate(SljitNativeGroupedAggregateFunction &function, string &error);
unique_ptr<JitCodeHandle> BuildSljitGroupedSumInt64Update(SljitNativeGroupedAggregateFunction &function,
                                                          string &error);
unique_ptr<JitCodeHandle> BuildSljitGroupedSumHugeintInt64Update(SljitNativeGroupedAggregateFunction &function,
                                                                 string &error);
unique_ptr<JitCodeHandle> BuildSljitFusedFilterProjectionUngroupedSum(
    const SljitNativeRegionExpressionPlan &filter, const SljitNativeRegionExpressionPlan &projection,
    const SljitNativeUngroupedAggregateUpdatePlan &update, SljitFusedUngroupedAggregateFunction &function,
    string &error);
unique_ptr<JitCodeHandle> BuildSljitFusedProjectionUngroupedSum(
    const SljitNativeRegionExpressionPlan &projection, const SljitNativeUngroupedAggregateUpdatePlan &update,
    SljitFusedUngroupedAggregateFunction &function, string &error);
bool BuildSljitFusedDirectPerfectHashAggregate(const SljitNativeRegionPlan &region, unique_ptr<JitCodeHandle> &code,
                                               SljitFusedPerfectHashAggregateFunction &function,
                                               string &overflow_message, string &error);
unique_ptr<JitCodeHandle> BuildSljitHashJoinProbe(const vector<SljitNativeHashJoinProbeKeyPlan> &keys,
                                                   idx_t equality_key_count, bool mark_build_match,
                                                   idx_t found_match_offset, idx_t pointer_offset,
                                                   JitRegionHashJoinProbeOutputMode output_mode,
                                                   SljitNativeHashJoinProbeFunction &function, string &error);

} // namespace duckdb
