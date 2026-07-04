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
#include "sljit_native_tail_recipe.hpp"
#include "sljit_projection_aggregate_recipe.hpp"

#include <utility>

namespace duckdb {

class SljitFullPipelineRecipeBuilder {
public:
	SljitFullPipelineRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               bool uses_scan_filters_p)
	    : ops(ops_p), uses_scan_filters(uses_scan_filters_p),
	      schedule_facts(SljitAnalyzeFullPipelineScheduleFacts(ops_p, uses_scan_filters_p)),
	      binding(ops_p, source_min_values_p, source_max_values_p,
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
		const char *name;
		TryBuildRecipeFunction try_build;
	};

	static const SljitFullPipelineRecipeRegistryEntry *RecipeRegistry(idx_t &count) {
		static const SljitFullPipelineRecipeRegistryEntry registry[] = {
		    {"selected_join_aggregate", &SljitFullPipelineRecipeBuilder::TryBuildSelectedJoinAggregateRecipe},
		    {"hash_join_delim_join_sink", &SljitFullPipelineRecipeBuilder::TryBuildHashJoinDelimJoinSinkRecipe},
		    {"projection_aggregate", &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateRecipe},
		    {"native_tail", &SljitFullPipelineRecipeBuilder::TryBuildNativeTailRecipe}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSelectedJoinAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSelectedJoinAggregateFacts facts;
		if (!SljitTryAnalyzeSelectedJoinAggregate(ops, facts)) {
			return false;
		}
		if (facts.HasSecondHashJoin()) {
			if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx) ||
			    !SljitAggregateUpdateCanUseSelectedJoinPerfectHashBackend(ops[facts.aggregate_idx])) {
				return false;
			}
			recipe = binding.MakeTwoJoinSelectedAggregateRecipe(facts.first_hash_join_idx, facts.second_hash_join_idx,
			                                                    facts.aggregate_idx);
			return true;
		}
		if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
		    !SljitAggregateUpdateCanUseSelectedJoinPerfectHashBackend(ops[facts.aggregate_idx])) {
			return false;
		}
		recipe = binding.MakeSelectedJoinAggregateRecipe(facts.first_hash_join_idx, facts.aggregate_idx);
		return true;
	}

	bool TryBuildHashJoinDelimJoinSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinDelimJoinSinkFacts facts;
		if (!SljitTryAnalyzeHashJoinDelimJoinSink(ops, facts)) {
			return false;
		}
		if (facts.sink_idx + 3 > SLJIT_FULL_PIPELINE_MAX_PRIMITIVES ||
		    !SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive(ops, facts.final_hash_join_idx, facts.sink_idx)) {
			return false;
		}
		for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx <= facts.final_hash_join_idx;
		     hash_join_idx++) {
			if (hash_join_idx == facts.final_hash_join_idx) {
				if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
					return false;
				}
				continue;
			}
			if (!SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx)) {
				return false;
			}
		}
		recipe =
		    binding.MakeHashJoinDelimJoinSinkRecipe(facts.first_hash_join_idx, facts.final_hash_join_idx, facts.sink_idx);
		return true;
	}

	bool TryBuildProjectionAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitProjectionAggregatePlanFacts plan;
		if (!SljitTryAnalyzeProjectionAggregatePlan(ops, plan)) {
			return false;
		}
		return SljitProjectionAggregateRecipeBuilder(ops, binding).Build(recipe, plan);
	}

	bool TryBuildNativeTailRecipe(SljitFullPipelineRecipe &recipe) const {
		return SljitNativeTailRecipeBuilder(ops, uses_scan_filters, binding).Build(recipe);
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	bool uses_scan_filters;
	SljitFullPipelineScheduleFacts schedule_facts;
	SljitFullPipelineRecipeBinding binding;
};

static SljitFullPipelineRecipePlan BuildSljitFullPipelineRecipePlan(const vector<SljitExecutableRegionOp> &ops,
                                                                    const vector<Value> &source_min_values,
                                                                    const vector<Value> &source_max_values,
                                                                    bool uses_scan_filters) {
	return SljitFullPipelineRecipeBuilder(ops, source_min_values, source_max_values, uses_scan_filters).Build();
}

} // namespace duckdb
