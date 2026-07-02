//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_aggregate_projection_fusion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

bool TryFuseNativeProjectionIntoPrimitiveAggregateUpdate(const vector<LogicalType> &input_types,
                                                         SljitNativeRegionOpPlan &projection,
                                                         SljitNativeRegionOpPlan &aggregate_update,
                                                         bool render_diagnostics);
bool TryFuseNativeProjectionIntoPerfectHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                           SljitNativeRegionOpPlan &projection,
                                                           SljitNativeRegionOpPlan &aggregate_update,
                                                           bool render_diagnostics);
bool TryComposePrimitiveAggregatePayloadsThroughProjection(const vector<LogicalType> &input_types,
                                                           const SljitNativeRegionOpPlan &projection,
                                                           SljitNativeRegionOpPlan &aggregate_update,
                                                           bool render_diagnostics);

} // namespace duckdb
