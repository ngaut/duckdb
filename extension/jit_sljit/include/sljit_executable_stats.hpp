//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_executable_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_region_plan.hpp"

namespace duckdb {

void SljitUpdateExecutableCurrentNotNull(const SljitNativeRegionOpPlan &op, vector<bool> &current_not_null);
void SljitUpdateExecutableCurrentRanges(const SljitNativeRegionOpPlan &op, vector<Value> &current_min_values,
                                        vector<Value> &current_max_values);

} // namespace duckdb
