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
vector<bool> SljitBuildExecutableOutputNotNull(const SljitNativeRegionOpPlan &op, const vector<bool> &input_not_null);
void SljitUpdateExecutableCurrentDistinctCounts(const SljitNativeRegionOpPlan &op,
                                                vector<idx_t> &current_distinct_counts,
                                                const vector<Value> &current_min_values,
                                                const vector<Value> &current_max_values);
void SljitUpdateExecutableCurrentRanges(const SljitNativeRegionOpPlan &op, vector<Value> &current_min_values,
                                        vector<Value> &current_max_values);

} // namespace duckdb
