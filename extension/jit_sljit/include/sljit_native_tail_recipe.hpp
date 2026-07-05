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

class SljitNativeTailRecipeBuilder {
public:
	SljitNativeTailRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                             const SljitFullPipelineScheduleFacts &schedule_facts_p,
	                             const SljitFullPipelineRecipeBinding &binding_p)
	    : ops(ops_p), schedule_facts(schedule_facts_p), binding(binding_p) {
	}

	bool Build(SljitFullPipelineRecipe &recipe) const {
		idx_t count;
		auto registry = RecipeRegistry(count);
		for (idx_t entry_idx = 0; entry_idx < count; entry_idx++) {
			if ((this->*registry[entry_idx].try_build)(recipe)) {
				return true;
			}
		}
		return false;
	}

private:
	using TryBuildRecipeFunction = bool (SljitNativeTailRecipeBuilder::*)(SljitFullPipelineRecipe &) const;

	struct RegistryEntry {
		TryBuildRecipeFunction try_build;
	};

	static const RegistryEntry *RecipeRegistry(idx_t &count) {
		static const RegistryEntry registry[] = {
		    {&SljitNativeTailRecipeBuilder::TryBuildFactsRecipe<SljitMarkFilterProjectionNativeTailFacts>},
		    {&SljitNativeTailRecipeBuilder::TryBuildFactsRecipe<SljitGeneratedFilterProjectionNativeTailFacts>},
		    {&SljitNativeTailRecipeBuilder::TryBuildFactsRecipe<SljitProjectionFilterProjectionNativeTailFacts>},
		    {&SljitNativeTailRecipeBuilder::TryBuildFactsRecipe<SljitSourceBatchNativeTailFacts>}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	template <class FACTS>
	bool TryBuildFactsRecipe(SljitFullPipelineRecipe &recipe) const {
		FACTS facts;
		if (!AnalyzeFacts(facts)) {
			return false;
		}
		if (!binding.CanMakeNativeTailRecipe(facts.tail_start_idx)) {
			return false;
		}
		recipe = MakeFactsRecipe(facts);
		return true;
	}

	bool AnalyzeFacts(SljitMarkFilterProjectionNativeTailFacts &facts) const {
		return SljitTryAnalyzeMarkFilterProjectionNativeTail(ops, facts);
	}

	bool AnalyzeFacts(SljitGeneratedFilterProjectionNativeTailFacts &facts) const {
		return SljitTryAnalyzeGeneratedFilterProjectionNativeTail(ops, facts);
	}

	bool AnalyzeFacts(SljitProjectionFilterProjectionNativeTailFacts &facts) const {
		return SljitTryAnalyzeProjectionFilterProjectionNativeTail(ops, facts);
	}

	bool AnalyzeFacts(SljitSourceBatchNativeTailFacts &facts) const {
		return SljitTryAnalyzeSourceBatchNativeTail(schedule_facts, facts);
	}

	SljitFullPipelineRecipe MakeFactsRecipe(const SljitMarkFilterProjectionNativeTailFacts &facts) const {
		return binding.MakeMarkFilterProjectionNativeTailRecipe(facts);
	}

	SljitFullPipelineRecipe MakeFactsRecipe(const SljitGeneratedFilterProjectionNativeTailFacts &facts) const {
		return binding.MakeGeneratedFilterProjectionNativeTailRecipe(facts);
	}

	SljitFullPipelineRecipe MakeFactsRecipe(const SljitProjectionFilterProjectionNativeTailFacts &facts) const {
		return binding.MakeProjectionFilterProjectionNativeTailRecipe(facts);
	}

	SljitFullPipelineRecipe MakeFactsRecipe(const SljitSourceBatchNativeTailFacts &facts) const {
		return binding.MakeSourceBatchNativeTailRecipe(facts);
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	const SljitFullPipelineScheduleFacts &schedule_facts;
	const SljitFullPipelineRecipeBinding &binding;
};

} // namespace duckdb
