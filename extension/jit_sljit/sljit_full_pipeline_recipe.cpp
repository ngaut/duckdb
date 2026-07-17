//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_recipe.hpp"

#include "duckdb/common/constants.hpp"

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"

#include <utility>

namespace duckdb {

class SljitFullPipelineRecipeBuilder {
public:
	SljitFullPipelineRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
	    : SljitFullPipelineRecipeBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
	                                     SljitAnalyzeFullPipelineScheduleFacts(ops_p)) {
	}

	SljitFullPipelineRecipePlan Build() const {
		SljitFullPipelineRecipe recipe;
		idx_t count;
		auto registry = RecipeRegistry(count);
		for (idx_t entry_idx = 0; entry_idx < count; entry_idx++) {
			if ((this->*registry[entry_idx].try_build)(recipe)) {
				return binding.MakePrimitiveRecipePlan(std::move(recipe));
			}
		}
		return binding.MakeNativeOnlyPlan();
	}

private:
	using TryBuildRecipeFunction = bool (SljitFullPipelineRecipeBuilder::*)(SljitFullPipelineRecipe &) const;

	struct SljitFullPipelineRecipeRegistryEntry {
		TryBuildRecipeFunction try_build;
	};

	static const SljitFullPipelineRecipeRegistryEntry *RecipeRegistry(idx_t &count) {
		static const SljitFullPipelineRecipeRegistryEntry registry[] = {
		    {&SljitFullPipelineRecipeBuilder::TryBuildSourceUngroupedAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildSourceFilterAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildJoinFilterAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildSourceHashJoinBuildSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildHashJoinDelimJoinSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildHashJoinAppendSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildHashJoinBuildSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildNativeTailRecipe}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSourceUngroupedAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSourceUngroupedAggregateFacts facts;
		if (!SljitTryAnalyzeSourceUngroupedAggregate(ops, facts)) {
			return false;
		}
		recipe = binding.MakeSourceUngroupedAggregateRecipe(facts);
		return true;
	}

	bool TryBuildSourceFilterAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSourceFilterAggregateFacts facts;
		return SljitTryAnalyzeSourceFilterAggregate(ops, facts) &&
		       binding.TryMakeSourceFilterAggregateRecipe(facts, recipe);
	}

	bool TryBuildJoinFilterAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitJoinFilterAggregateFacts facts;
		if (!SljitTryAnalyzeJoinFilterAggregate(ops, facts) ||
		    !binding.TryMakeJoinFilterAggregateRecipe(facts, recipe)) {
			return false;
		}
		return true;
	}

	bool TryBuildSourceHashJoinBuildSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSourceHashJoinBuildSinkFacts facts;
		return SljitTryAnalyzeSourceHashJoinBuildSink(ops, facts) &&
		       binding.TryMakeSourceHashJoinBuildSinkRecipe(facts, recipe);
	}

	bool TryBuildHashJoinDelimJoinSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinDelimJoinSinkFacts facts;
		return SljitTryAnalyzeHashJoinDelimJoinSink(ops, facts) &&
		       binding.TryMakeHashJoinDelimJoinSinkRecipe(facts, recipe);
	}

	bool TryBuildHashJoinAppendSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinAppendSinkFacts facts;
		return SljitTryAnalyzeHashJoinAppendSink(ops, facts) && binding.TryMakeHashJoinAppendSinkRecipe(facts, recipe);
	}

	bool TryBuildHashJoinBuildSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinBuildSinkFacts facts;
		return SljitTryAnalyzeHashJoinBuildSink(ops, facts) && binding.TryMakeHashJoinBuildSinkRecipe(facts, recipe);
	}

	bool TryBuildProjectionAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitProjectionAggregatePlanFacts plan;
		if (!SljitTryAnalyzeProjectionAggregatePlan(ops, plan)) {
			return false;
		}
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

	bool TryBuildNativeTailRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitMarkFilterProjectionNativeTailFacts mark_filter;
		if (SljitTryAnalyzeMarkFilterProjectionNativeTail(ops, mark_filter) &&
		    binding.ProjectionAggregateRecipes().TryMakeMarkFilterProjectionNativeTailRecipe(mark_filter, recipe)) {
			return true;
		}
		SljitGeneratedFilterProjectionNativeTailFacts generated_filter;
		if (SljitTryAnalyzeGeneratedFilterProjectionNativeTail(ops, generated_filter) &&
		    binding.TryMakeGeneratedFilterProjectionNativeTailRecipe(generated_filter, recipe)) {
			return true;
		}
		SljitProjectionFilterProjectionNativeTailFacts projection_filter;
		return SljitTryAnalyzeProjectionFilterProjectionNativeTail(ops, projection_filter) &&
		       binding.TryMakeProjectionFilterProjectionNativeTailRecipe(projection_filter, recipe);
	}

private:
	SljitFullPipelineRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               const SljitFullPipelineScheduleFacts &schedule_facts)
	    : ops(ops_p), binding(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
	                          schedule_facts.uses_extended_source_fetch_budget) {
	}

	const vector<SljitExecutableRegionOp> &ops;
	SljitFullPipelineRecipeBinding binding;
};

SljitFullPipelineRecipePlan BuildSljitFullPipelineRecipePlan(const vector<SljitExecutableRegionOp> &ops,
                                                             const vector<LogicalType> &source_output_types,
                                                             const vector<Value> &source_min_values,
                                                             const vector<Value> &source_max_values) {
	return SljitFullPipelineRecipeBuilder(ops, source_output_types, source_min_values, source_max_values).Build();
}

} // namespace duckdb
