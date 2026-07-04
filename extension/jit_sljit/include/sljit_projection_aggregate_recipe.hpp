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
		    {"source_grouped_aggregate", &SljitProjectionAggregateRecipeBuilder::TryBuildSourceGrouped},
		    {"single_join_mark_filter", &SljitProjectionAggregateRecipeBuilder::TryBuildSingleMarkFilter},
		    {"single_join_source_filter_projection",
		     &SljitProjectionAggregateRecipeBuilder::TryBuildSingleSourceFilterProjection},
		    {"single_join_pre_projection", &SljitProjectionAggregateRecipeBuilder::TryBuildSinglePreProjection},
		    {"single_join_direct_projection", &SljitProjectionAggregateRecipeBuilder::TryBuildSingleDirectProjection},
		    {"single_join_projection_chain", &SljitProjectionAggregateRecipeBuilder::TryBuildSingleProjectionChain},
		    {"two_join_mark_filter", &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinMarkFilter},
		    {"two_join_filtered_projection_prefix",
		     &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinSourceFilterProjection},
		    {"two_join_pre_projection", &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinPreProjection},
		    {"two_join_between_projection", &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinBetweenProjection},
		    {"two_join_direct_projection", &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinDirectProjection},
		    {"two_join_projection_chain", &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinProjectionChain}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSourceGrouped(SljitFullPipelineRecipe &recipe,
	                           const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		if (!SljitProjectionAggregateCanUseSourceCountStarGroupedAggregate(ops, plan)) {
			return false;
		}
		recipe = binding.MakeSourceProjectionGroupedAggregateRecipe(shape);
		return true;
	}

	bool TryBuildSingleMarkFilter(SljitFullPipelineRecipe &recipe,
	                              const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SljitProjectionAggregateCanUseSingleJoinMarkFilterBoundary(ops, plan)) {
			return false;
		}
		if (!binding.SelectedProjectionAggregateHasDedicatedBackend(shape)) {
			recipe = binding.MakeMarkFilterNativeTailRecipe(facts.first_hash_join_idx, facts.mark_filter_idx);
			return true;
		}
		recipe = binding.MakeMarkFilterProjectionAggregateRecipe(shape, facts.first_hash_join_idx,
		                                                         facts.mark_filter_idx);
		return true;
	}

	bool TryBuildSingleSourceFilterProjection(SljitFullPipelineRecipe &recipe,
	                                          const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SljitProjectionAggregateCanUseSingleJoinSourceFilterProjection(plan) ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
		    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			return false;
		}
		recipe = binding.MakeFilterProjectionJoinProjectionAggregateRecipe(
		    shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx,
		    SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
		return true;
	}

	bool TryBuildSinglePreProjection(SljitFullPipelineRecipe &recipe,
	                                 const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SljitProjectionAggregateCanUseSingleJoinPreJoinProjection(plan) ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
		    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			return false;
		}
		recipe = binding.MakePreProjectionJoinProjectionAggregateRecipe(
		    shape, facts.first_hash_join_idx,
		    SljitDirectJoinOutputAggregateUpdateSchedule::IMMEDIATE_ROW_POINTER_UPDATE);
		return true;
	}

	bool TryBuildSingleDirectProjection(SljitFullPipelineRecipe &recipe,
	                                    const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SljitProjectionAggregateCanUseSingleJoinSingleProjection(plan) ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
		    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			return false;
		}
		recipe = binding.MakeJoinProjectionAggregateRecipe(
		    shape, facts.first_hash_join_idx,
		    SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
		return true;
	}

	bool TryBuildSingleProjectionChain(SljitFullPipelineRecipe &recipe,
	                                   const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SljitProjectionAggregateCanUseSingleJoinProjectionChain(plan) ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
		    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
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
		if (!facts.HasSecondHashJoin() || !facts.HasMarkFilter() || facts.HasSourceFilterProjection() ||
		    facts.HasPreJoinProjection() || facts.HasBetweenProjection() ||
		    facts.mark_filter_idx != facts.second_hash_join_idx + 1 ||
		    ops[facts.second_hash_join_idx].hash_join_probe.plan.output_mode !=
		        ExecutionHashJoinProbeOutputMode::MARK_PROBE) {
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
		if (!facts.HasSecondHashJoin() || !facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() ||
		    facts.HasBetweenProjection() || shape.ProjectionCount() != 2 || !plan.HasFixedFinalProjection() ||
		    !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx)) {
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
		if (!facts.HasSecondHashJoin() || !facts.HasPreJoinProjection() || !facts.HasBetweenProjection() ||
		    shape.ProjectionCount() != 2 || !plan.HasFixedFinalProjection() ||
		    !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx)) {
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
		if (!facts.HasSecondHashJoin() || !facts.HasBetweenProjection() || facts.HasMarkFilter() ||
		    shape.ProjectionCount() == 0 || !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx) ||
		    !plan.HasFixedFinalProjection()) {
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
		if (!facts.HasSecondHashJoin() || facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() ||
		    facts.HasBetweenProjection() || facts.HasMarkFilter() || shape.ProjectionCount() != 1 ||
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
		if (!facts.HasSecondHashJoin() || shape.ProjectionCount() != 2 || !plan.HasFixedFinalProjection() ||
		    !SljitCanBindHashJoinProbeMaterializePrimitive(ops, facts.first_hash_join_idx) ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx)) {
			return false;
		}
		recipe =
		    binding.MakeTwoJoinProjectionChainAggregateRecipe(shape, facts.first_hash_join_idx, facts.second_hash_join_idx);
		return true;
	}

	bool DirectJoinProjectionAggregateHasDedicatedBackend(const SljitFullPipelineProjectionAggregateShape &shape) const {
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
