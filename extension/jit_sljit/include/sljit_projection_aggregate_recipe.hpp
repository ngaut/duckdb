//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"

namespace duckdb {

class SljitProjectionAggregateRecipeBuilder {
public:
	SljitProjectionAggregateRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                                      const SljitFullPipelineRecipeBinding &binding_p)
	    : ops(ops_p), binding(binding_p) {
	}

	bool Build(SljitFullPipelineRecipe &recipe, const SljitProjectionAggregatePlanFacts &plan) const {
		idx_t count;
		auto registry = RecipeRegistry(count);
		for (idx_t entry_idx = 0; entry_idx < count; entry_idx++) {
			if ((this->*registry[entry_idx].try_build)(recipe, plan)) {
				return true;
			}
		}
		return false;
	}

private:
	using TryBuildProjectionAggregateRecipeFunction = bool (SljitProjectionAggregateRecipeBuilder::*)(
	    SljitFullPipelineRecipe &, const SljitProjectionAggregatePlanFacts &) const;

	struct RegistryEntry {
		const char *name;
		TryBuildProjectionAggregateRecipeFunction try_build;
	};

	static const RegistryEntry *RecipeRegistry(idx_t &count) {
		static const RegistryEntry registry[] = {
		    {"source", &SljitProjectionAggregateRecipeBuilder::TryBuildSourcePrefix},
		    {"single_join", &SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinPrefix},
		    {"two_join", &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinPrefix}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	enum class SljitProjectionAggregateSingleJoinStrategy {
		INVALID,
		MARK_FILTER_BOUNDARY,
		SOURCE_FILTER_PROJECTION,
		PRE_JOIN_PROJECTION,
		DIRECT_PROJECTION,
		PROJECTION_CHAIN
	};

	enum class SljitProjectionAggregateTwoJoinStrategy {
		INVALID,
		MARK_FILTER_BOUNDARY,
		SOURCE_FILTER_PROJECTION,
		PRE_JOIN_PROJECTION,
		BETWEEN_PROJECTION,
		DIRECT_PROJECTION,
		PROJECTION_CHAIN
	};

	bool TryBuildSourcePrefix(SljitFullPipelineRecipe &recipe, const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		if (!SourcePrefixHasCountStarGroupedBackend(plan)) {
			return false;
		}
		recipe = binding.MakeSourceProjectionGroupedAggregateRecipe(shape);
		return true;
	}

	bool TryBuildSingleJoinPrefix(SljitFullPipelineRecipe &recipe,
	                              const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		switch (SelectSingleJoinStrategy(plan)) {
		case SljitProjectionAggregateSingleJoinStrategy::MARK_FILTER_BOUNDARY:
			if (!binding.SelectedProjectionAggregateHasDedicatedBackend(shape)) {
				recipe = binding.MakeMarkFilterNativeTailRecipe(facts.first_hash_join_idx, facts.mark_filter_idx);
				return true;
			}
			recipe = binding.MakeMarkFilterProjectionAggregateRecipe(shape, facts.first_hash_join_idx,
			                                                         facts.mark_filter_idx);
			return true;
		case SljitProjectionAggregateSingleJoinStrategy::SOURCE_FILTER_PROJECTION:
			recipe = binding.MakeFilterProjectionJoinProjectionAggregateRecipe(
			    shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx,
			    SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
			return true;
		case SljitProjectionAggregateSingleJoinStrategy::PRE_JOIN_PROJECTION:
			recipe = binding.MakePreProjectionJoinProjectionAggregateRecipe(
			    shape, facts.first_hash_join_idx,
			    SljitDirectJoinOutputAggregateUpdateSchedule::IMMEDIATE_ROW_POINTER_UPDATE);
			return true;
		case SljitProjectionAggregateSingleJoinStrategy::DIRECT_PROJECTION:
			recipe = binding.MakeJoinProjectionAggregateRecipe(
			    shape, facts.first_hash_join_idx,
			    SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
			return true;
		case SljitProjectionAggregateSingleJoinStrategy::PROJECTION_CHAIN:
			recipe = binding.MakeJoinProjectionAggregateRecipe(
			    shape, facts.first_hash_join_idx,
			    SljitDirectJoinOutputAggregateUpdateSchedule::IMMEDIATE_ROW_POINTER_UPDATE);
			return true;
		case SljitProjectionAggregateSingleJoinStrategy::INVALID:
			return false;
		}
		return false;
	}

	bool TryBuildTwoJoinPrefix(SljitFullPipelineRecipe &recipe, const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		switch (SelectTwoJoinStrategy(plan)) {
		case SljitProjectionAggregateTwoJoinStrategy::MARK_FILTER_BOUNDARY:
			if (shape.ProjectionCount() != 0 && binding.SelectedProjectionAggregateHasDedicatedBackend(shape)) {
				recipe = binding.MakeTwoJoinMarkFilterProjectionAggregateRecipe(
				    shape, facts.first_hash_join_idx, facts.second_hash_join_idx, facts.mark_filter_idx);
				return true;
			}
			recipe = binding.MakeTwoJoinMarkFilterNativeTailRecipe(shape, facts.first_hash_join_idx,
			                                                       facts.second_hash_join_idx, facts.mark_filter_idx);
			return true;
		case SljitProjectionAggregateTwoJoinStrategy::SOURCE_FILTER_PROJECTION:
			recipe = binding.MakeFilterProjectionTwoJoinProjectionAggregateRecipe(
			    shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx,
			    facts.second_hash_join_idx);
			return true;
		case SljitProjectionAggregateTwoJoinStrategy::PRE_JOIN_PROJECTION:
			recipe = binding.MakePreProjectionTwoJoinProjectionAggregateRecipe(
			    shape, facts.pre_join_projection_idx, facts.first_hash_join_idx, facts.between_projection_idx,
			    facts.second_hash_join_idx);
			return true;
		case SljitProjectionAggregateTwoJoinStrategy::BETWEEN_PROJECTION:
			if (DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
				recipe = binding.MakeBetweenProjectionTwoJoinDirectAggregateRecipe(
				    shape, facts.first_hash_join_idx, facts.between_projection_idx, facts.second_hash_join_idx);
				return true;
			}
			recipe = binding.MakeBetweenProjectionTwoJoinProjectionAggregateRecipe(
			    shape, facts.first_hash_join_idx, facts.between_projection_idx, facts.second_hash_join_idx);
			return true;
		case SljitProjectionAggregateTwoJoinStrategy::DIRECT_PROJECTION:
			recipe = binding.MakeTwoJoinDirectProjectionAggregateRecipe(shape, facts.first_hash_join_idx);
			return true;
		case SljitProjectionAggregateTwoJoinStrategy::PROJECTION_CHAIN:
			recipe = binding.MakeTwoJoinProjectionChainAggregateRecipe(shape, facts.first_hash_join_idx,
			                                                           facts.second_hash_join_idx);
			return true;
		case SljitProjectionAggregateTwoJoinStrategy::INVALID:
			return false;
		}
		return false;
	}

	bool SourcePrefixHasCountStarGroupedBackend(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		return plan.prefix.Kind() == SljitProjectionAggregatePrefixKind::SOURCE && plan.ProjectionCount() == 1 &&
		       plan.HasFixedFinalProjection() &&
		       SljitGroupedAggregateUpdateHasDedicatedBackend(ops, shape.aggregate_idx) &&
		       SljitChooseGroupedAggregateUpdateStrategy(ops[shape.aggregate_idx]) ==
		           SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
	}

	SljitProjectionAggregateSingleJoinStrategy
	SelectSingleJoinStrategy(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (facts.Kind() != SljitProjectionAggregatePrefixKind::SINGLE_JOIN) {
			return SljitProjectionAggregateSingleJoinStrategy::INVALID;
		}
		if (SingleJoinHasMarkFilterBoundary(plan)) {
			return SljitProjectionAggregateSingleJoinStrategy::MARK_FILTER_BOUNDARY;
		}
		if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
		    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			return SljitProjectionAggregateSingleJoinStrategy::INVALID;
		}
		if (facts.HasSourceFilterProjection() && !facts.HasPreJoinProjection() && !facts.HasMarkFilter() &&
		    plan.ProjectionCount() == 1) {
			return SljitProjectionAggregateSingleJoinStrategy::SOURCE_FILTER_PROJECTION;
		}
		if (facts.HasPreJoinProjection() && !facts.HasSourceFilterProjection() && !facts.HasMarkFilter()) {
			return SljitProjectionAggregateSingleJoinStrategy::PRE_JOIN_PROJECTION;
		}
		if (facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() || facts.HasMarkFilter()) {
			return SljitProjectionAggregateSingleJoinStrategy::INVALID;
		}
		if (plan.ProjectionCount() == 1) {
			return SljitProjectionAggregateSingleJoinStrategy::DIRECT_PROJECTION;
		}
		if (plan.ProjectionCount() == 2 && plan.HasFixedFinalProjection()) {
			return SljitProjectionAggregateSingleJoinStrategy::PROJECTION_CHAIN;
		}
		return SljitProjectionAggregateSingleJoinStrategy::INVALID;
	}

	SljitProjectionAggregateTwoJoinStrategy SelectTwoJoinStrategy(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (facts.Kind() != SljitProjectionAggregatePrefixKind::TWO_JOIN) {
			return SljitProjectionAggregateTwoJoinStrategy::INVALID;
		}
		if (TwoJoinHasMarkFilterBoundary(plan)) {
			return SljitProjectionAggregateTwoJoinStrategy::MARK_FILTER_BOUNDARY;
		}
		if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx)) {
			return SljitProjectionAggregateTwoJoinStrategy::INVALID;
		}
		if (facts.HasSourceFilterProjection() && !facts.HasPreJoinProjection() && !facts.HasBetweenProjection() &&
		    !facts.HasMarkFilter() && shape.ProjectionCount() == 2 && plan.HasFixedFinalProjection() &&
		    CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx)) {
			return SljitProjectionAggregateTwoJoinStrategy::SOURCE_FILTER_PROJECTION;
		}
		if (facts.HasPreJoinProjection() && facts.HasBetweenProjection() && shape.ProjectionCount() == 2 &&
		    !facts.HasMarkFilter() && plan.HasFixedFinalProjection() &&
		    CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx)) {
			return SljitProjectionAggregateTwoJoinStrategy::PRE_JOIN_PROJECTION;
		}
		if (facts.HasBetweenProjection() && !facts.HasMarkFilter() && shape.ProjectionCount() != 0 &&
		    plan.HasFixedFinalProjection() && CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx)) {
			return SljitProjectionAggregateTwoJoinStrategy::BETWEEN_PROJECTION;
		}
		if (facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() || facts.HasBetweenProjection() ||
		    facts.HasMarkFilter()) {
			return SljitProjectionAggregateTwoJoinStrategy::INVALID;
		}
		if (shape.ProjectionCount() == 1 && plan.HasFixedFinalProjection() &&
		    SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.first_hash_join_idx) &&
		    SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.second_hash_join_idx) &&
		    DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			return SljitProjectionAggregateTwoJoinStrategy::DIRECT_PROJECTION;
		}
		if (shape.ProjectionCount() == 2 && plan.HasFixedFinalProjection() &&
		    SljitCanBindHashJoinProbeMaterializePrimitive(ops, facts.first_hash_join_idx)) {
			return SljitProjectionAggregateTwoJoinStrategy::PROJECTION_CHAIN;
		}
		return SljitProjectionAggregateTwoJoinStrategy::INVALID;
	}

	bool SingleJoinHasMarkFilterBoundary(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		return facts.HasMarkFilter() && !facts.HasSourceFilterProjection() && !facts.HasPreJoinProjection() &&
		       facts.mark_filter_idx == facts.first_hash_join_idx + 1 && shape.ProjectionCount() == 1 &&
		       plan.HasFixedFinalProjection() &&
		       ops[facts.first_hash_join_idx].hash_join_probe.plan.output_mode ==
		           ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
		       SljitIsMarkProbeMarkerFilter(ops[facts.first_hash_join_idx], ops[facts.mark_filter_idx]);
	}

	bool TwoJoinHasMarkFilterBoundary(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		return facts.HasMarkFilter() && !facts.HasSourceFilterProjection() && !facts.HasPreJoinProjection() &&
		       !facts.HasBetweenProjection() && facts.mark_filter_idx == facts.second_hash_join_idx + 1 &&
		       ops[facts.second_hash_join_idx].hash_join_probe.plan.output_mode ==
		           ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
		       SljitIsMarkProbeMarkerFilter(ops[facts.second_hash_join_idx], ops[facts.mark_filter_idx]);
	}

	bool
	DirectJoinProjectionAggregateHasDedicatedBackend(const SljitFullPipelineProjectionAggregateShape &shape) const {
		return SljitCanBindDirectJoinOutputAggregatePrimitive(ops, shape.aggregate_idx);
	}

	bool CanBindHashJoinProbeProjectionInput(idx_t hash_join_idx) const {
		return SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx) ||
		       SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx);
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	const SljitFullPipelineRecipeBinding &binding;
};

} // namespace duckdb
