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
	SljitNativeTailRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p, bool uses_scan_filters_p,
	                             const SljitFullPipelineRecipeBinding &binding_p)
	    : ops(ops_p), uses_scan_filters(uses_scan_filters_p), binding(binding_p) {
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
		const char *name;
		TryBuildRecipeFunction try_build;
	};

	static const RegistryEntry *RecipeRegistry(idx_t &count) {
		static const RegistryEntry registry[] = {
		    {"mark_filter_projection_native_tail", &SljitNativeTailRecipeBuilder::TryBuildMarkFilterProjection},
		    {"generated_filter_projection_native_tail",
		     &SljitNativeTailRecipeBuilder::TryBuildGeneratedFilterProjection},
		    {"projection_filter_projection_native_tail",
		     &SljitNativeTailRecipeBuilder::TryBuildProjectionFilterProjection},
		    {"source_batch_native_tail", &SljitNativeTailRecipeBuilder::TryBuildSourceBatch}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildMarkFilterProjection(SljitFullPipelineRecipe &recipe) const {
		SljitMarkFilterProjectionNativeTailFacts facts;
		if (!SljitTryAnalyzeMarkFilterProjectionNativeTail(ops, facts)) {
			return false;
		}
		recipe = binding.MakeMarkFilterProjectionNativeTailRecipe(facts);
		return true;
	}

	bool TryBuildGeneratedFilterProjection(SljitFullPipelineRecipe &recipe) const {
		SljitGeneratedFilterProjectionNativeTailFacts facts;
		if (!SljitTryAnalyzeGeneratedFilterProjectionNativeTail(ops, facts)) {
			return false;
		}
		recipe = binding.MakeGeneratedFilterProjectionNativeTailRecipe(facts);
		return true;
	}

	bool TryBuildProjectionFilterProjection(SljitFullPipelineRecipe &recipe) const {
		SljitProjectionFilterProjectionNativeTailFacts facts;
		if (!SljitTryAnalyzeProjectionFilterProjectionNativeTail(ops, facts)) {
			return false;
		}
		recipe = binding.MakeProjectionFilterProjectionNativeTailRecipe(facts);
		return true;
	}

	bool TryBuildSourceBatch(SljitFullPipelineRecipe &recipe) const {
		SljitSourceBatchNativeTailFacts facts;
		if (!SljitTryAnalyzeSourceBatchNativeTail(ops, uses_scan_filters, facts) ||
		    !SljitCanBindNativeTailHandoffPrimitive(ops, facts.tail_start_idx)) {
			return false;
		}
		recipe = binding.MakeSourceBatchNativeTailRecipe(facts);
		return true;
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	bool uses_scan_filters;
	const SljitFullPipelineRecipeBinding &binding;
};

} // namespace duckdb
