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
		     &SljitProjectionAggregateRecipeBuilder::TryBuildMarkProjectionAggregate},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildMarkProjectionAggregate},
		    {SljitProjectionAggregatePrefixKind::SINGLE_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildMarkNativeTail},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildMarkNativeTail},
		    {SljitProjectionAggregatePrefixKind::SINGLE_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildDirectProjectionAggregate},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildDirectProjectionAggregate},
		    {SljitProjectionAggregatePrefixKind::SINGLE_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildProjectionAggregateTail},
		    {SljitProjectionAggregatePrefixKind::TWO_JOIN,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildProjectionAggregateTail}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSourceProjectionAggregate(SljitFullPipelineRecipe &recipe,
	                                       const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		if (plan.ProjectionCount() == 0 || !binding.ProjectionAggregateHasDedicatedBackend(shape, true)) {
			return false;
		}
		recipe = binding.MakeSourceProjectionAggregateRecipe(shape);
		return true;
	}

	bool CanBindMarkFilterBoundary(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (!facts.HasMarkFilter() || facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() ||
		    facts.HasBetweenProjection()) {
			return false;
		}
		const auto hash_join_idx = facts.MarkFilterHashJoinIdx();
		if (hash_join_idx == DConstants::INVALID_INDEX) {
			return false;
		}
		return facts.mark_filter_idx == hash_join_idx + 1 &&
		       ops[hash_join_idx].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
		       SljitIsMarkProbeMarkerFilter(ops[hash_join_idx], ops[facts.mark_filter_idx]);
	}

	bool TryBuildMarkProjectionAggregate(SljitFullPipelineRecipe &recipe,
	                                     const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!CanBindMarkFilterBoundary(plan) || !binding.ProjectionAggregateHasDedicatedBackend(shape) ||
		    (facts.HasSecondHashJoin() && shape.ProjectionCount() == 0)) {
			return false;
		}
		recipe = binding.MakeMarkFilterProjectionAggregateRecipe(shape, facts);
		return true;
	}

	bool TryBuildMarkNativeTail(SljitFullPipelineRecipe &recipe, const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (!CanBindMarkFilterBoundary(plan) || !binding.CanMakeNativeTailRecipe(facts.mark_filter_idx + 1)) {
			return false;
		}
		recipe = binding.MakeMarkFilterNativeTailRecipe(facts);
		return true;
	}

	bool TryBuildDirectProjectionAggregate(SljitFullPipelineRecipe &recipe,
	                                       const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (!CanBindDirectProjectionAggregate(plan)) {
			return false;
		}
		recipe = binding.MakeJoinDirectProjectionAggregateRecipe(plan.shape, facts);
		return true;
	}

	bool TryBuildProjectionAggregateTail(SljitFullPipelineRecipe &recipe,
	                                     const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (!CanBindProjectionAggregateTail(plan)) {
			return false;
		}
		recipe = binding.MakeJoinProjectionAggregateTailRecipe(plan.shape, facts);
		return true;
	}

	bool CanBindDirectProjectionAggregate(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
			return false;
		}
		if (!facts.HasSecondHashJoin()) {
			if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
			    (shape.ProjectionCount() == 0 &&
			     !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.first_hash_join_idx))) {
				return false;
			}
			if (facts.HasSourceFilterProjection()) {
				return !facts.HasPreJoinProjection() && plan.ProjectionCount() == 1;
			}
			return facts.HasPreJoinProjection() || plan.ProjectionCount() <= 2;
		}
		if (facts.HasMarkFilter() || facts.HasSourceFilterProjection() ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx)) {
			return false;
		}
		if (facts.HasBetweenProjection()) {
			return !facts.HasPreJoinProjection() && shape.ProjectionCount() != 0 &&
			       CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx);
		}
		if (facts.HasPreJoinProjection()) {
			return false;
		}
		const auto projection_count = shape.ProjectionCount();
		if (projection_count != 0 && projection_count != 1) {
			return false;
		}
		if (projection_count == 0) {
			return SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) &&
			       SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.second_hash_join_idx);
		}
		return SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.first_hash_join_idx) &&
		       SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.second_hash_join_idx);
	}

	bool CanBindProjectionAggregateTail(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (!facts.HasSecondHashJoin()) {
			if (facts.HasMarkFilter() || !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
			    !binding.CanMakeProjectionAggregateTailRecipe(shape)) {
				return false;
			}
			if (facts.HasSourceFilterProjection() &&
			    (!SljitCanBindGeneratedFilterPrimitive(ops, facts.source_filter_idx) ||
			     !SljitCanBindProjectionChainPrimitive(ops, facts.source_projection_idx))) {
				return false;
			}
			return !facts.HasPreJoinProjection() ||
			       SljitCanBindProjectionChainPrimitive(ops, facts.pre_join_projection_idx);
		}
		return SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx) &&
		       CanBindProjectionAggregateTailPrefix(plan) && shape.ProjectionCount() != 0 &&
		       CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) &&
		       binding.CanMakeProjectionAggregateTailRecipe(shape);
	}

	bool CanBindProjectionAggregateTailPrefix(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (facts.HasMarkFilter()) {
			return false;
		}
		if (facts.HasSourceFilterProjection()) {
			return !facts.HasPreJoinProjection() && !facts.HasBetweenProjection();
		}
		if (facts.HasBetweenProjection()) {
			return true;
		}
		return !facts.HasPreJoinProjection();
	}

	bool
	DirectJoinProjectionAggregateHasDedicatedBackend(const SljitFullPipelineProjectionAggregateShape &shape) const {
		return shape.aggregate_idx < ops.size() &&
		       SljitAggregateUpdateHasDedicatedCompiledBackend(ops[shape.aggregate_idx]);
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
