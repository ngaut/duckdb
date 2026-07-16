//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_recipe.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_projection_aggregate_recipe.hpp"

namespace duckdb {

bool SljitTryBuildProjectionAggregateRecipe(const SljitFullPipelineRecipeBinding &binding,
                                            SljitFullPipelineRecipe &recipe,
                                            const SljitProjectionAggregatePlanFacts &plan) {
	auto &recipes = binding.ProjectionAggregateRecipes();
	switch (plan.prefix.Kind()) {
	case SljitProjectionAggregatePrefixKind::SOURCE:
		return recipes.TryMakeSourceProjectionAggregateTailRecipe(plan.shape, recipe);
	case SljitProjectionAggregatePrefixKind::JOIN_PREFIX:
		return recipes.TryMakeMarkFilterProjectionAggregateRecipe(plan.shape, plan.prefix, recipe) ||
		       recipes.TryMakeMarkFilterNativeTailRecipe(plan.prefix, recipe) ||
		       recipes.TryMakeJoinDirectProjectionAggregateRecipe(plan.shape, plan.prefix, recipe) ||
		       recipes.TryMakeJoinProjectionAggregateTailRecipe(plan.shape, plan.prefix, recipe);
	case SljitProjectionAggregatePrefixKind::INVALID:
		return false;
	}
	return false;
}

} // namespace duckdb
