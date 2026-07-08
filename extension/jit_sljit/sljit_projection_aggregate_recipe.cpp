//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_recipe.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_projection_aggregate_recipe.hpp"

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
		    {SljitProjectionAggregatePrefixKind::JOIN_PREFIX,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildMarkProjectionAggregate},
		    {SljitProjectionAggregatePrefixKind::JOIN_PREFIX,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildMarkNativeTail},
		    {SljitProjectionAggregatePrefixKind::JOIN_PREFIX,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildDirectProjectionAggregate},
		    {SljitProjectionAggregatePrefixKind::JOIN_PREFIX,
		     &SljitProjectionAggregateRecipeBuilder::TryBuildProjectionAggregateTail}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSourceProjectionAggregate(SljitFullPipelineRecipe &recipe,
	                                       const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		if (plan.ProjectionCount() == 0 || !binding.CanMakeProjectionAggregateTailRecipe(shape)) {
			return false;
		}
		recipe = binding.MakeSourceProjectionAggregateTailRecipe(shape);
		return true;
	}

	bool CanBindMarkFilterBoundary(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (!facts.HasMarkFilter() || facts.HasSourceFilterProjection() || facts.HasAnyJoinInputProjection()) {
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
		    (facts.JoinCount() > 1 && shape.ProjectionCount() == 0)) {
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
		if (shape.aggregate_idx >= ops.size() ||
		    !SljitAggregateUpdateHasDedicatedCompiledBackend(ops[shape.aggregate_idx])) {
			return false;
		}
		if (facts.JoinCount() == 1) {
			const auto hash_join_idx = facts.HashJoinIdx(0);
			if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx) ||
			    (shape.ProjectionCount() == 0 &&
			     !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, hash_join_idx))) {
				return false;
			}
			if (facts.HasSourceFilterProjection()) {
				return !facts.HasJoinInputProjection(0) && plan.ProjectionCount() == 1;
			}
			return facts.HasJoinInputProjection(0) || plan.ProjectionCount() <= 2;
		}
		if (facts.HasMarkFilter() || facts.HasSourceFilterProjection() ||
		    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.FinalHashJoinIdx()) ||
		    !CanBindJoinPrefixInputs(facts)) {
			return false;
		}
		if (facts.HasJoinInputProjection(1)) {
			return !facts.HasJoinInputProjection(0) && shape.ProjectionCount() != 0 &&
			       CanBindHashJoinProbeProjectionInput(facts.HashJoinIdx(0));
		}
		if (facts.HasJoinInputProjection(0)) {
			return false;
		}
		const auto projection_count = shape.ProjectionCount();
		if (projection_count != 0 && projection_count != 1) {
			return false;
		}
		if (projection_count == 0) {
			return SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.HashJoinIdx(0)) &&
			       SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.FinalHashJoinIdx());
		}
		return SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.HashJoinIdx(0)) &&
		       SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.FinalHashJoinIdx());
	}

	bool CanBindProjectionAggregateTail(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		if (facts.JoinCount() == 1) {
			const auto hash_join_idx = facts.HashJoinIdx(0);
			if (facts.HasMarkFilter() || !CanBindHashJoinProbeProjectionInput(hash_join_idx) ||
			    !binding.CanMakeProjectionAggregateTailRecipe(shape)) {
				return false;
			}
			if (facts.HasSourceFilterProjection() &&
			    (!SljitCanBindGeneratedFilterPrimitive(ops, facts.source_filter_idx) ||
			     !SljitCanBindProjectionChainPrimitive(ops, facts.source_projection_idx))) {
				return false;
			}
			return !facts.HasJoinInputProjection(0) ||
			       SljitCanBindProjectionChainPrimitive(ops, facts.JoinInputProjectionIdx(0));
		}
		return SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.FinalHashJoinIdx()) &&
		       CanBindProjectionAggregateTailPrefix(plan) && shape.ProjectionCount() != 0 &&
		       CanBindJoinPrefixInputs(facts) && binding.CanMakeProjectionAggregateTailRecipe(shape);
	}

	bool CanBindProjectionAggregateTailPrefix(const SljitProjectionAggregatePlanFacts &plan) const {
		auto &facts = plan.prefix;
		if (facts.HasMarkFilter()) {
			return false;
		}
		if (facts.HasSourceFilterProjection()) {
			return facts.JoinCount() == 1 || !facts.HasJoinInputProjection(0);
		}
		return true;
	}

	bool CanBindHashJoinProbeProjectionInput(idx_t hash_join_idx) const {
		return SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx) ||
		       SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx);
	}

	bool CanBindJoinPrefixInputs(const SljitProjectionAggregatePrefixFacts &facts) const {
		if (facts.JoinCount() == 0) {
			return false;
		}
		const auto final_join_ordinal = facts.JoinCount() - 1;
		for (idx_t join_ordinal = 0; join_ordinal < final_join_ordinal; join_ordinal++) {
			if (facts.HasJoinInputProjection(join_ordinal) &&
			    !SljitCanBindProjectionChainPrimitive(ops, facts.JoinInputProjectionIdx(join_ordinal))) {
				return false;
			}
			if (!CanBindHashJoinProbeProjectionInput(facts.HashJoinIdx(join_ordinal))) {
				return false;
			}
		}
		return !facts.HasJoinInputProjection(final_join_ordinal) ||
		       SljitCanBindProjectionChainPrimitive(ops, facts.JoinInputProjectionIdx(final_join_ordinal));
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	const SljitFullPipelineRecipeBinding &binding;
};

bool SljitTryBuildProjectionAggregateRecipe(const vector<SljitExecutableRegionOp> &ops,
                                            const SljitFullPipelineRecipeBinding &binding,
                                            SljitFullPipelineRecipe &recipe,
                                            const SljitProjectionAggregatePlanFacts &plan) {
	return SljitProjectionAggregateRecipeBuilder(ops, binding).Build(recipe, plan);
}

} // namespace duckdb
