//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_facts.hpp"
#include "sljit_full_pipeline_recipe_sequence_builder.hpp"
#include "sljit_projection_aggregate_recipe_binding.hpp"

namespace duckdb {

class SljitFullPipelineRecipeBinding : private SljitFullPipelineRecipeSequenceBuilder {
public:
	SljitFullPipelineRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               bool uses_extended_source_fetch_budget_p);

	SljitFullPipelineRecipePlan MakeNativeOnlyPlan() const;

	SljitFullPipelineRecipePlan MakePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) const;

	bool TryMakeProjectionFilterProjectionNativeTailRecipe(const SljitProjectionFilterProjectionNativeTailFacts &facts,
	                                                       SljitFullPipelineRecipe &recipe) const;

	SljitFullPipelineRecipe MakeSourceUngroupedAggregateRecipe(const SljitSourceUngroupedAggregateFacts &facts) const;

	bool TryMakeSourceFilterAggregateRecipe(const SljitSourceFilterAggregateFacts &facts,
	                                        SljitFullPipelineRecipe &recipe) const;

	bool TryMakeJoinFilterAggregateRecipe(const SljitJoinFilterAggregateFacts &facts,
	                                      SljitFullPipelineRecipe &recipe) const;

	bool TryMakeGeneratedFilterProjectionNativeTailRecipe(const SljitGeneratedFilterProjectionNativeTailFacts &facts,
	                                                      SljitFullPipelineRecipe &recipe) const;
	bool TryMakeSourceHashJoinBuildSinkRecipe(const SljitSourceHashJoinBuildSinkFacts &facts,
	                                          SljitFullPipelineRecipe &recipe) const;

	bool TryMakeHashJoinDelimJoinSinkRecipe(const SljitHashJoinDelimJoinSinkFacts &facts,
	                                        SljitFullPipelineRecipe &recipe) const;

	bool TryMakeHashJoinAppendSinkRecipe(const SljitHashJoinAppendSinkFacts &facts,
	                                     SljitFullPipelineRecipe &recipe) const;

	bool TryMakeHashJoinBuildSinkRecipe(const SljitHashJoinBuildSinkFacts &facts,
	                                    SljitFullPipelineRecipe &recipe) const;

	const SljitProjectionAggregateRecipeBinding &ProjectionAggregateRecipes() const {
		return projection_aggregate_recipes;
	}

private:
	SljitProjectionAggregateRecipeBinding projection_aggregate_recipes;
};

} // namespace duckdb
