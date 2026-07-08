//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_hash_join_delim_join_sink_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"

namespace duckdb {

bool SljitTryBuildHashJoinDelimJoinSinkRecipe(const vector<SljitExecutableRegionOp> &ops,
                                              const SljitFullPipelineRecipeBinding &binding,
                                              SljitFullPipelineRecipe &recipe,
                                              const SljitHashJoinDelimJoinSinkFacts &facts);

} // namespace duckdb
