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
		TryBuildProjectionAggregateRecipeFunction try_build;
	};

	static const RegistryEntry *RecipeRegistry(idx_t &count) {
		static const RegistryEntry registry[] = {
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildSourceCountStarGrouped},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinMarkFilter},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinSourceFilterProjection},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinPreProjection},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinDirectProjection},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinProjectionChain},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinMarkFilter},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinSourceFilterProjection},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinPreProjection},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinBetweenProjection},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinDirectProjection},
		    {&SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinProjectionChain}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSourceCountStarGrouped(SljitFullPipelineRecipe &recipe,
	                                    const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		if (!SourcePrefixHasCountStarGroupedBackend(plan)) {
			return false;
		}
		recipe = binding.MakeSourceProjectionGroupedAggregateRecipe(shape);
		return true;
	}

	bool TryBuildSingleJoinMarkFilter(SljitFullPipelineRecipe &recipe,
	                                  const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SingleJoinHasMarkFilterBoundary(plan)) {
			return false;
		}
		if (!binding.SelectedProjectionAggregateHasDedicatedBackend(shape)) {
			recipe = binding.MakeMarkFilterNativeTailRecipe(facts.first_hash_join_idx, facts.mark_filter_idx);
			return true;
		}
		recipe =
		    binding.MakeMarkFilterProjectionAggregateRecipe(shape, facts.first_hash_join_idx, facts.mark_filter_idx);
		return true;
	}

	bool TryBuildSingleJoinSourceFilterProjection(SljitFullPipelineRecipe &recipe,
	                                              const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SingleJoinCanUseDirectAggregate(plan) || !facts.HasSourceFilterProjection() ||
		    facts.HasPreJoinProjection() || facts.HasMarkFilter() || plan.ProjectionCount() != 1) {
			return false;
		}
		recipe = binding.MakeFilterProjectionJoinProjectionAggregateRecipe(
		    shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx,
		    SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
		return true;
	}

	bool TryBuildSingleJoinPreProjection(SljitFullPipelineRecipe &recipe,
	                                     const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SingleJoinCanUseDirectAggregate(plan) || !facts.HasPreJoinProjection() ||
		    facts.HasSourceFilterProjection() || facts.HasMarkFilter()) {
			return false;
		}
		recipe = binding.MakePreProjectionJoinProjectionAggregateRecipe(
		    shape, facts.first_hash_join_idx,
		    SljitDirectJoinOutputAggregateUpdateSchedule::IMMEDIATE_ROW_POINTER_UPDATE);
		return true;
	}

	bool TryBuildSingleJoinDirectProjection(SljitFullPipelineRecipe &recipe,
	                                        const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SingleJoinCanUseDirectAggregate(plan) || SingleJoinHasPrefixOperator(facts) ||
		    plan.ProjectionCount() != 1) {
			return false;
		}
		recipe = binding.MakeJoinProjectionAggregateRecipe(
		    shape, facts.first_hash_join_idx, SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
		return true;
	}

	bool TryBuildSingleJoinProjectionChain(SljitFullPipelineRecipe &recipe,
	                                       const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SingleJoinCanUseDirectAggregate(plan) || SingleJoinHasPrefixOperator(facts) ||
		    plan.ProjectionCount() != 2 || !plan.HasFixedFinalProjection()) {
			return false;
		}
		recipe = binding.MakeJoinProjectionAggregateRecipe(
		    shape, facts.first_hash_join_idx,
		    SljitDirectJoinOutputAggregateUpdateSchedule::IMMEDIATE_ROW_POINTER_UPDATE);
		return true;
	}

	bool TryBuildTwoJoinMarkFilter(SljitFullPipelineRecipe &recipe,
	                               const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!TwoJoinHasMarkFilterBoundary(plan)) {
			return false;
		}
		if (shape.ProjectionCount() != 0 && binding.SelectedProjectionAggregateHasDedicatedBackend(shape)) {
			recipe = binding.MakeTwoJoinMarkFilterProjectionAggregateRecipe(
			    shape, facts.first_hash_join_idx, facts.second_hash_join_idx, facts.mark_filter_idx);
			return true;
		}
		recipe = binding.MakeTwoJoinMarkFilterNativeTailRecipe(shape, facts.first_hash_join_idx,
		                                                       facts.second_hash_join_idx, facts.mark_filter_idx);
		return true;
	}

	bool TryBuildTwoJoinSourceFilterProjection(SljitFullPipelineRecipe &recipe,
	                                           const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!TwoJoinCanUseSelection(plan) || !facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() ||
		    facts.HasBetweenProjection() || facts.HasMarkFilter() || shape.ProjectionCount() != 2 ||
		    !plan.HasFixedFinalProjection() || !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx)) {
			return false;
		}
		recipe = binding.MakeFilterProjectionTwoJoinProjectionAggregateRecipe(
		    shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx,
		    facts.second_hash_join_idx);
		return true;
	}

	bool TryBuildTwoJoinPreProjection(SljitFullPipelineRecipe &recipe,
	                                  const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!TwoJoinCanUseSelection(plan) || !facts.HasPreJoinProjection() || !facts.HasBetweenProjection() ||
		    facts.HasMarkFilter() || shape.ProjectionCount() != 2 || !plan.HasFixedFinalProjection() ||
		    !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx)) {
			return false;
		}
		recipe = binding.MakePreProjectionTwoJoinProjectionAggregateRecipe(
		    shape, facts.pre_join_projection_idx, facts.first_hash_join_idx, facts.between_projection_idx,
		    facts.second_hash_join_idx);
		return true;
	}

	bool TryBuildTwoJoinBetweenProjection(SljitFullPipelineRecipe &recipe,
	                                      const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!TwoJoinCanUseSelection(plan) || !facts.HasBetweenProjection() || facts.HasMarkFilter() ||
		    shape.ProjectionCount() == 0 || !plan.HasFixedFinalProjection() ||
		    !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx)) {
			return false;
		}
		if (DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			recipe = binding.MakeBetweenProjectionTwoJoinDirectAggregateRecipe(
			    shape, facts.first_hash_join_idx, facts.between_projection_idx, facts.second_hash_join_idx);
			return true;
		}
		recipe = binding.MakeBetweenProjectionTwoJoinProjectionAggregateRecipe(
		    shape, facts.first_hash_join_idx, facts.between_projection_idx, facts.second_hash_join_idx);
		return true;
	}

	bool TryBuildTwoJoinDirectProjection(SljitFullPipelineRecipe &recipe,
	                                     const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!TwoJoinCanUseSelection(plan) || TwoJoinHasPrefixOperator(facts) || shape.ProjectionCount() != 1 ||
		    !plan.HasFixedFinalProjection() ||
		    !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.first_hash_join_idx) ||
		    !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.second_hash_join_idx) ||
		    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			return false;
		}
		recipe = binding.MakeTwoJoinDirectProjectionAggregateRecipe(shape, facts.first_hash_join_idx);
		return true;
	}

	bool TryBuildTwoJoinProjectionChain(SljitFullPipelineRecipe &recipe,
	                                    const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!TwoJoinCanUseSelection(plan) || TwoJoinHasPrefixOperator(facts) || shape.ProjectionCount() != 2 ||
		    !plan.HasFixedFinalProjection() ||
		    !SljitCanBindHashJoinProbeMaterializePrimitive(ops, facts.first_hash_join_idx)) {
			return false;
		}
		recipe = binding.MakeTwoJoinProjectionChainAggregateRecipe(shape, facts.first_hash_join_idx,
		                                                           facts.second_hash_join_idx);
		return true;
	}

	bool SourcePrefixHasCountStarGroupedBackend(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		return plan.prefix.Kind() == SljitProjectionAggregatePrefixKind::SOURCE && plan.ProjectionCount() == 1 &&
		       plan.HasFixedFinalProjection() &&
		       SljitGroupedAggregateUpdateHasDedicatedBackend(ops, shape.aggregate_idx) &&
		       SljitChooseGroupedAggregateUpdateStrategy(ops[shape.aggregate_idx]) ==
		           SljitGroupedAggregateUpdateStrategyKind::COUNT_STAR_PREAGGREGATION;
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

	bool SingleJoinCanUseDirectAggregate(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		return facts.Kind() == SljitProjectionAggregatePrefixKind::SINGLE_JOIN &&
		       SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) &&
		       DirectJoinProjectionAggregateHasDedicatedBackend(shape);
	}

	bool TwoJoinCanUseSelection(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		return facts.Kind() == SljitProjectionAggregatePrefixKind::TWO_JOIN &&
		       SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx);
	}

	static bool SingleJoinHasPrefixOperator(const SljitProjectionAggregatePrefixFacts &facts) {
		return facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() || facts.HasMarkFilter();
	}

	static bool TwoJoinHasPrefixOperator(const SljitProjectionAggregatePrefixFacts &facts) {
		return facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() || facts.HasBetweenProjection() ||
		       facts.HasMarkFilter();
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
