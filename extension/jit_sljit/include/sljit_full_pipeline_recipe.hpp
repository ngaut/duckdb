//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"
#include "sljit_hash_join_delim_join_sink_recipe.hpp"
#include "sljit_native_tail_recipe.hpp"
#include "sljit_projection_aggregate_recipe.hpp"
#include "sljit_selected_join_aggregate_recipe.hpp"

#include <utility>

namespace duckdb {

class SljitFullPipelineRecipeBuilder {
public:
	SljitFullPipelineRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
	    : ops(ops_p), schedule_facts(SljitAnalyzeFullPipelineScheduleFacts(ops_p)),
	      binding(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
	              schedule_facts.uses_extended_source_fetch_budget) {
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
		    {&SljitFullPipelineRecipeBuilder::TryBuildSelectedJoinAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildHashJoinDelimJoinSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildNativeTailRecipe}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSelectedJoinAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSelectedJoinAggregateFacts facts;
		if (!SljitTryAnalyzeSelectedJoinAggregate(ops, facts)) {
			return false;
		}
		return SljitSelectedJoinAggregateRecipeBuilder(ops, binding).Build(recipe, facts);
	}

	bool TryBuildHashJoinDelimJoinSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinDelimJoinSinkFacts facts;
		if (!SljitTryAnalyzeHashJoinDelimJoinSink(ops, facts)) {
			return false;
		}
		return SljitHashJoinDelimJoinSinkRecipeBuilder(ops, binding).Build(recipe, facts);
	}

	bool TryBuildProjectionAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitProjectionAggregatePlanFacts plan;
		if (!SljitTryAnalyzeProjectionAggregatePlan(ops, plan)) {
			return false;
		}
		return SljitProjectionAggregateRecipeBuilder(ops, binding).Build(recipe, plan);
	}

	bool TryBuildNativeTailRecipe(SljitFullPipelineRecipe &recipe) const {
		return SljitNativeTailRecipeBuilder(ops, schedule_facts, binding).Build(recipe);
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	SljitFullPipelineScheduleFacts schedule_facts;
	SljitFullPipelineRecipeBinding binding;
};

static SljitFullPipelineRecipePlan BuildSljitFullPipelineRecipePlan(const vector<SljitExecutableRegionOp> &ops,
                                                                    const vector<LogicalType> &source_output_types,
                                                                    const vector<Value> &source_min_values,
                                                                    const vector<Value> &source_max_values) {
	return SljitFullPipelineRecipeBuilder(ops, source_output_types, source_min_values, source_max_values).Build();
}

} // namespace duckdb
