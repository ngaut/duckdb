//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_composition.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_executable.hpp"

namespace duckdb {

bool TryComposeNativeProjection(const vector<SljitNativeRegionExpressionPlan> &input_projection,
                                const SljitNativeRegionExpressionPlan &expr, SljitNativeRegionExpressionPlan &result,
                                bool render_diagnostics);

} // namespace duckdb
