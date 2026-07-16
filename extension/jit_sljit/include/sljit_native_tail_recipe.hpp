//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_native_tail_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"

namespace duckdb {

bool SljitTryBuildNativeTailRecipe(const vector<SljitExecutableRegionOp> &ops,
                                   const SljitFullPipelineRecipeBinding &binding, SljitFullPipelineRecipe &recipe);

} // namespace duckdb
