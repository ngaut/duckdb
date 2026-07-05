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
		const auto prefix_kind = plan.prefix.Kind();
		idx_t count;
		auto registry = RecipeRegistry(count);
		for (idx_t entry_idx = 0; entry_idx < count; entry_idx++) {
			if (registry[entry_idx].prefix_kind != prefix_kind) {
				continue;
			}
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
		SljitProjectionAggregatePrefixKind prefix_kind;
		TryBuildProjectionAggregateRecipeFunction try_build;
	};

	static const RegistryEntry *RecipeRegistry(idx_t &count) {
		static const RegistryEntry registry[] = {
		    {SljitProjectionAggregatePrefixKind::SOURCE,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildSourceProjectionAggregate},
		    {SljitProjectionAggregatePrefixKind::SINGLE_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildMarkBoundary},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildMarkBoundary},
		    {SljitProjectionAggregatePrefixKind::SINGLE_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinPreparedRecipe},
		    {SljitProjectionAggregatePrefixKind::SINGLE_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildSingleJoinMaterializedTail},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinSourcePrefixRecipe},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildTwoJoinProjectedPrefixRecipe},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildPlainTwoJoinDirectAggregate},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildPlainTwoJoinMaterializedTail}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSourceProjectionAggregate(SljitFullPipelineRecipe &recipe,
	                                       const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		if (plan.ProjectionCount() == 0 || !binding.SelectedProjectionAggregateHasDedicatedBackend(shape, true)) {
			return false;
		}
		recipe = binding.MakeSourceProjectionGroupedAggregateRecipe(shape);
		return true;
	}

	bool HasMarkFilterBoundary(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (!facts.HasMarkFilter() || facts.HasSourceFilterProjection() || facts.HasPreJoinProjection()) {
			return false;
		}
		switch (facts.Kind()) {
		case SljitProjectionAggregatePrefixKind::SINGLE_JOIN:
			return facts.mark_filter_idx == facts.first_hash_join_idx + 1 &&
			       ops[facts.first_hash_join_idx].hash_join_probe.plan.output_mode ==
			           ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
			       SljitIsMarkProbeMarkerFilter(ops[facts.first_hash_join_idx], ops[facts.mark_filter_idx]);
		case SljitProjectionAggregatePrefixKind::TWO_JOIN:
			return !facts.HasBetweenProjection() && facts.mark_filter_idx == facts.second_hash_join_idx + 1 &&
			       ops[facts.second_hash_join_idx].hash_join_probe.plan.output_mode ==
			           ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
			       SljitIsMarkProbeMarkerFilter(ops[facts.second_hash_join_idx], ops[facts.mark_filter_idx]);
		default:
			return false;
		}
	}

	bool TryBuildMarkBoundary(SljitFullPipelineRecipe &recipe,
	                          const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!HasMarkFilterBoundary(plan)) {
			return false;
		}
		switch (facts.Kind()) {
		case SljitProjectionAggregatePrefixKind::SINGLE_JOIN:
			if (!binding.SelectedProjectionAggregateHasDedicatedBackend(shape)) {
				if (!binding.CanMakeNativeTailRecipe(facts.mark_filter_idx + 1)) {
					return false;
				}
				recipe = binding.MakeMarkFilterNativeTailRecipe(facts.first_hash_join_idx, facts.mark_filter_idx);
				return true;
			}
			recipe =
			    binding.MakeMarkFilterProjectionAggregateRecipe(shape, facts.first_hash_join_idx, facts.mark_filter_idx);
			return true;
		case SljitProjectionAggregatePrefixKind::TWO_JOIN:
			if (shape.ProjectionCount() != 0 && binding.SelectedProjectionAggregateHasDedicatedBackend(shape)) {
				recipe = binding.MakeTwoJoinMarkFilterProjectionAggregateRecipe(
				    shape, facts.first_hash_join_idx, facts.second_hash_join_idx, facts.mark_filter_idx);
				return true;
			}
			if (!binding.CanMakeNativeTailRecipe(facts.mark_filter_idx + 1)) {
				return false;
			}
			recipe = binding.MakeTwoJoinMarkFilterNativeTailRecipe(shape, facts.first_hash_join_idx,
			                                                       facts.second_hash_join_idx, facts.mark_filter_idx);
			return true;
		default:
			return false;
		}
	}

	bool TryBuildSingleJoinPreparedRecipe(SljitFullPipelineRecipe &recipe,
	                                      const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (facts.HasMarkFilter()) {
			return false;
		}
		if (facts.HasSourceFilterProjection()) {
			if (!facts.HasPreJoinProjection() && plan.ProjectionCount() == 1 && SingleJoinCanUseDirectAggregate(plan)) {
				recipe = binding.MakeFilterProjectionJoinProjectionAggregateRecipe(
				    shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx);
				return true;
			}
			return false;
		}
		if (facts.HasPreJoinProjection() && SingleJoinCanUseDirectAggregate(plan)) {
			recipe = binding.MakePreProjectionJoinProjectionAggregateRecipe(shape, facts.first_hash_join_idx);
			return true;
		}
		if (!facts.HasPreJoinProjection() && (plan.ProjectionCount() == 1 || plan.ProjectionCount() == 2) &&
		    SingleJoinCanUseDirectAggregate(plan)) {
			recipe = binding.MakeJoinProjectionAggregateRecipe(plan.shape, facts.first_hash_join_idx);
			return true;
		}
		return false;
	}

	bool TryBuildSingleJoinMaterializedTail(SljitFullPipelineRecipe &recipe,
	                                        const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!SingleJoinCanUseProjectionAggregateTail(plan)) {
			return false;
		}
		recipe = binding.MakeSingleJoinProjectionAggregateTailRecipe(
		    shape, facts.source_filter_idx, facts.source_projection_idx, facts.pre_join_projection_idx,
		    facts.first_hash_join_idx);
		return true;
	}

	bool TryBuildTwoJoinSourcePrefixRecipe(SljitFullPipelineRecipe &recipe,
	                                       const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (facts.HasMarkFilter() || !facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() ||
		    facts.HasBetweenProjection() || !CanBindSecondHashJoinSelection(plan) ||
		    !TwoJoinCanUseProjectionAggregateTail(plan)) {
			return false;
		}
		recipe = binding.MakeFilterProjectionTwoJoinProjectionAggregateRecipe(
		    plan.shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx,
		    facts.second_hash_join_idx);
		return true;
	}

	bool TryBuildTwoJoinProjectedPrefixRecipe(SljitFullPipelineRecipe &recipe,
	                                          const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (facts.HasMarkFilter() || facts.HasSourceFilterProjection() || !facts.HasBetweenProjection() ||
		    !CanBindSecondHashJoinSelection(plan)) {
			return false;
		}
		if (facts.HasPreJoinProjection()) {
			if (!TwoJoinCanUseProjectionAggregateTail(plan)) {
				return false;
			}
			recipe = binding.MakePreProjectionTwoJoinProjectionAggregateRecipe(
			    shape, facts.pre_join_projection_idx, facts.first_hash_join_idx, facts.between_projection_idx,
			    facts.second_hash_join_idx);
			return true;
		}
		if (shape.ProjectionCount() == 0 || !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
		    (!DirectJoinProjectionAggregateHasDedicatedBackend(shape) &&
		     !binding.CanMakeProjectionAggregateTailRecipe(shape))) {
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

	bool TryBuildPlainTwoJoinDirectAggregate(SljitFullPipelineRecipe &recipe,
	                                         const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (TwoJoinHasPreparedPrefix(plan) || !CanBindSecondHashJoinSelection(plan) || shape.ProjectionCount() != 1 ||
		    !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.first_hash_join_idx) ||
		    !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.second_hash_join_idx) ||
		    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			return false;
		}
		recipe = binding.MakeTwoJoinDirectProjectionAggregateRecipe(shape, facts.first_hash_join_idx);
		return true;
	}

	bool TryBuildPlainTwoJoinMaterializedTail(SljitFullPipelineRecipe &recipe,
	                                          const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (TwoJoinHasPreparedPrefix(plan) || !CanBindSecondHashJoinSelection(plan) || shape.ProjectionCount() != 2 ||
		    !SljitCanBindHashJoinProbeMaterializePrimitive(ops, facts.first_hash_join_idx) ||
		    !binding.CanMakeProjectionAggregateTailRecipe(shape)) {
			return false;
		}
		recipe =
		    binding.MakeTwoJoinProjectionChainAggregateRecipe(shape, facts.first_hash_join_idx, facts.second_hash_join_idx);
		return true;
	}

	bool SingleJoinCanUseDirectAggregate(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		return SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) &&
		       DirectJoinProjectionAggregateHasDedicatedBackend(shape);
	}

	bool SingleJoinCanUseProjectionAggregateTail(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (facts.HasMarkFilter() || !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
		    !binding.CanMakeProjectionAggregateTailRecipe(shape)) {
			return false;
		}
		if (facts.HasSourceFilterProjection() &&
		    (!SljitCanBindGeneratedFilterPrimitive(ops, facts.source_filter_idx) ||
		     !SljitCanBindProjectionChainPrimitive(ops, facts.source_projection_idx))) {
			return false;
		}
		return !facts.HasPreJoinProjection() || SljitCanBindProjectionChainPrimitive(ops, facts.pre_join_projection_idx);
	}

	bool CanBindSecondHashJoinSelection(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		return SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx);
	}

	bool TwoJoinHasPreparedPrefix(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		return facts.HasMarkFilter() || facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() ||
		       facts.HasBetweenProjection();
	}

	bool TwoJoinCanUseProjectionAggregateTail(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		return shape.ProjectionCount() == 2 && CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) &&
		       binding.CanMakeProjectionAggregateTailRecipe(shape);
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
