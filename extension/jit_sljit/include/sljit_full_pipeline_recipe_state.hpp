//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_primitive_sequence.hpp"

#include <utility>

namespace duckdb {

struct SljitFullPipelineRecipe {
	SljitFullPipelinePrimitiveSequence primitive_sequence;
	bool uses_extended_source_fetch_budget = false;
};

struct SljitFullPipelineRecipePlan {
	bool has_recipe = false;
	SljitFullPipelineRecipe recipe;
};

static SljitFullPipelineRecipe
SljitMakeFullPipelinePrimitiveRecipe(bool uses_extended_source_fetch_budget,
                                     SljitFullPipelinePrimitiveSequence primitive_sequence) {
	if (primitive_sequence.Count() == 0) {
		throw InternalException("SLJIT full-pipeline primitive recipe cannot be empty");
	}
	SljitFullPipelineRecipe recipe;
	recipe.uses_extended_source_fetch_budget = uses_extended_source_fetch_budget;
	recipe.primitive_sequence = std::move(primitive_sequence);
	return recipe;
}

static SljitFullPipelineRecipePlan SljitMakeFullPipelinePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) {
	if (recipe.primitive_sequence.Count() == 0) {
		throw InternalException("SLJIT full-pipeline primitive recipe cannot be empty");
	}
	SljitFullPipelineRecipePlan plan;
	plan.has_recipe = true;
	plan.recipe = std::move(recipe);
	return plan;
}

static SljitFullPipelineRecipePlan SljitMakeFullPipelineNativeOnlyPlan() {
	SljitFullPipelineRecipePlan plan;
	return plan;
}

} // namespace duckdb
