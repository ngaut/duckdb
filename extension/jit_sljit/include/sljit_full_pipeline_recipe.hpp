//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_region_executable.hpp"

namespace duckdb {

SljitFullPipelineRecipePlan BuildSljitFullPipelineRecipePlan(const vector<SljitExecutableRegionOp> &ops,
                                                             const SljitNativeAggregateStateSourcePlan &source,
                                                             const vector<LogicalType> &source_output_types,
                                                             const vector<Value> &source_min_values,
                                                             const vector<Value> &source_max_values);

} // namespace duckdb
