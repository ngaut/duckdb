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

struct SljitHashJoinDirectAggregateConsumerContract {
	idx_t probe_step_idx = DConstants::INVALID_INDEX;
	idx_t terminal_step_idx = DConstants::INVALID_INDEX;
	idx_t probe_input_filter_idx = DConstants::INVALID_INDEX;

	bool IsBound() const {
		return probe_step_idx != DConstants::INVALID_INDEX && terminal_step_idx != DConstants::INVALID_INDEX;
	}
};

static SljitHashJoinDirectAggregateConsumerContract SljitBindHashJoinDirectAggregateConsumerContract(
	const SljitFullPipelinePrimitiveSequence &primitive_sequence) {
	SljitHashJoinDirectAggregateConsumerContract contract;
	if (primitive_sequence.Count() < 2) {
		return contract;
	}

	const auto terminal_step_idx = primitive_sequence.Count() - 1;
	if (primitive_sequence.Step(terminal_step_idx).kind !=
	    SljitFullPipelinePrimitiveKind::POST_JOIN_PROJECTION_AGGREGATE_UPDATE) {
		return contract;
	}

	for (idx_t probe_step_idx = 1; probe_step_idx < terminal_step_idx; probe_step_idx++) {
		if (primitive_sequence.Step(probe_step_idx).kind !=
		    SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION) {
			continue;
		}

		idx_t probe_input_filter_idx = DConstants::INVALID_INDEX;
		const auto next_step_idx = probe_step_idx + 1;
		if (next_step_idx != terminal_step_idx) {
			if (next_step_idx + 1 != terminal_step_idx ||
			    primitive_sequence.Step(next_step_idx).kind != SljitFullPipelinePrimitiveKind::GENERATED_FILTER) {
				continue;
			}
			probe_input_filter_idx = primitive_sequence.Step(next_step_idx).generated_filter.filter_idx;
		}

		contract.probe_step_idx = probe_step_idx;
		contract.terminal_step_idx = terminal_step_idx;
		contract.probe_input_filter_idx = probe_input_filter_idx;
		return contract;
	}
	return contract;
}

struct SljitFullPipelineRecipe {
	SljitFullPipelinePrimitiveSequence primitive_sequence;
	SljitHashJoinDirectAggregateConsumerContract direct_aggregate_consumer;
	bool uses_extended_source_fetch_budget = false;
};

struct SljitFullPipelineRecipePlan {
	bool has_recipe = false;
	SljitFullPipelineRecipe recipe;
	string native_only_runtime_path;
};

static SljitFullPipelineRecipe
SljitMakeFullPipelinePrimitiveRecipe(bool uses_extended_source_fetch_budget,
                                     SljitFullPipelinePrimitiveSequence primitive_sequence) {
	if (primitive_sequence.Count() == 0) {
		throw InternalException("SLJIT full-pipeline primitive recipe cannot be empty");
	}
	SljitFullPipelineRecipe recipe;
	recipe.direct_aggregate_consumer =
	    SljitBindHashJoinDirectAggregateConsumerContract(primitive_sequence);
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

static SljitFullPipelineRecipePlan SljitMakeFullPipelineNativeOnlyPlan(string runtime_path) {
	SljitFullPipelineRecipePlan plan;
	plan.native_only_runtime_path = std::move(runtime_path);
	return plan;
}

} // namespace duckdb
