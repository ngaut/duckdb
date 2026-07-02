//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_region_aggregate_partial_fusion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan_internal.hpp"

namespace duckdb {

bool TryPartiallyFuseNativeProjectionIntoPerfectHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                                    SljitNativeRegionOpPlan &projection,
                                                                    SljitNativeRegionOpPlan &aggregate_update,
                                                                    bool render_diagnostics);
bool TryPartiallyFuseNativeProjectionIntoRegularHashAggregateUpdate(const vector<LogicalType> &input_types,
                                                                    SljitNativeRegionOpPlan &projection,
                                                                    SljitNativeRegionOpPlan &aggregate_update,
                                                                    bool render_diagnostics);

} // namespace duckdb
