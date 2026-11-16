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
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p);

	//! Generated steps run the region's ops from the source on as far as they bind, into the sink when generated code
	//! updates it; otherwise DuckDB's operators take the rows over after them
	bool TryMakeOperatorHandOverRecipe(SljitFullPipelineRecipe &recipe) const;

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

	bool TryMakeHashJoinBuildSinkRecipe(const SljitHashJoinBuildSinkFacts &facts,
	                                    SljitFullPipelineRecipe &recipe) const;

	const SljitProjectionAggregateRecipeBinding &ProjectionAggregateRecipes() const {
		return projection_aggregate_recipes;
	}

private:
	//! Binds the source prefix [0, prefix_end): each FILTER as a generated-filter step, each run of PROJECTIONs as one
	//! projection-chain step.
	bool TryAddSourcePrefixSteps(SljitFullPipelinePrimitiveSequence &sequence, idx_t prefix_end) const;

private:
	SljitProjectionAggregateRecipeBinding projection_aggregate_recipes;
};

} // namespace duckdb
