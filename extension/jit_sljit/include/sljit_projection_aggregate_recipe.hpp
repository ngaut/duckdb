//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"

namespace duckdb {

bool SljitTryBuildProjectionAggregateRecipe(const vector<SljitExecutableRegionOp> &ops,
                                            const SljitFullPipelineRecipeBinding &binding,
                                            SljitFullPipelineRecipe &recipe,
                                            const SljitProjectionAggregatePlanFacts &plan);

} // namespace duckdb
