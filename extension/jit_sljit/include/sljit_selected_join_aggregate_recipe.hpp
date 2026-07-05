//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_selected_join_aggregate_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"

namespace duckdb {

class SljitSelectedJoinAggregateRecipeBuilder {
public:
	SljitSelectedJoinAggregateRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                                        const SljitFullPipelineRecipeBinding &binding_p)
	    : ops(ops_p), binding(binding_p) {
	}

	bool Build(SljitFullPipelineRecipe &recipe, const SljitSelectedJoinAggregateFacts &facts) const {
		idx_t count;
		auto registry = RecipeRegistry(count);
		for (idx_t entry_idx = 0; entry_idx < count; entry_idx++) {
			if ((this->*registry[entry_idx].try_build)(recipe, facts)) {
				return true;
			}
		}
		return false;
	}

private:
	using TryBuildSelectedJoinAggregateRecipeFunction = bool (SljitSelectedJoinAggregateRecipeBuilder::*)(
	    SljitFullPipelineRecipe &, const SljitSelectedJoinAggregateFacts &) const;

	struct RegistryEntry {
		TryBuildSelectedJoinAggregateRecipeFunction try_build;
	};

	static const RegistryEntry *RecipeRegistry(idx_t &count) {
		static const RegistryEntry registry[] = {
		    {&SljitSelectedJoinAggregateRecipeBuilder::TryBuildTwoJoinSelectedAggregate},
		    {&SljitSelectedJoinAggregateRecipeBuilder::TryBuildPreProjectionSelectedAggregate},
		    {&SljitSelectedJoinAggregateRecipeBuilder::TryBuildSingleJoinSelectedAggregate}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildTwoJoinSelectedAggregate(SljitFullPipelineRecipe &recipe,
	                                      const SljitSelectedJoinAggregateFacts &facts) const {
		if (!facts.HasSecondHashJoin() || !CanBindAggregateTerminal(facts.aggregate_idx) ||
		    !CanBindHashJoinSelection(facts.first_hash_join_idx) ||
		    !CanBindHashJoinSelection(facts.second_hash_join_idx)) {
			return false;
		}
		recipe = binding.MakeTwoJoinSelectedAggregateRecipe(facts.first_hash_join_idx, facts.second_hash_join_idx,
		                                                    facts.aggregate_idx);
		return true;
	}

	bool TryBuildPreProjectionSelectedAggregate(SljitFullPipelineRecipe &recipe,
	                                            const SljitSelectedJoinAggregateFacts &facts) const {
		if (facts.HasSecondHashJoin() || !facts.HasPreJoinProjection() ||
		    !CanBindAggregateTerminal(facts.aggregate_idx) || !CanBindHashJoinSelection(facts.first_hash_join_idx) ||
		    !SljitCanBindProjectionChainPrimitive(ops, facts.pre_join_projection_idx)) {
			return false;
		}
		recipe = binding.MakePreProjectionSelectedJoinAggregateRecipe(
		    facts.pre_join_projection_idx, facts.first_hash_join_idx, facts.aggregate_idx);
		return true;
	}

	bool TryBuildSingleJoinSelectedAggregate(SljitFullPipelineRecipe &recipe,
	                                         const SljitSelectedJoinAggregateFacts &facts) const {
		if (facts.HasSecondHashJoin() || facts.HasPreJoinProjection() ||
		    !CanBindAggregateTerminal(facts.aggregate_idx) || !CanBindHashJoinSelection(facts.first_hash_join_idx)) {
			return false;
		}
		recipe = binding.MakeSelectedJoinAggregateRecipe(facts.first_hash_join_idx, facts.aggregate_idx);
		return true;
	}

	bool CanBindAggregateTerminal(idx_t aggregate_idx) const {
		return SljitCanBindDirectJoinOutputAggregatePrimitive(ops, aggregate_idx);
	}

	bool CanBindHashJoinSelection(idx_t hash_join_idx) const {
		return SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx);
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	const SljitFullPipelineRecipeBinding &binding;
};

} // namespace duckdb
