//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"
#include "sljit_projection_chain_runtime.hpp"

#include <utility>

namespace duckdb {

class SljitFullPipelineRecipeBuilder {
public:
	SljitFullPipelineRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               bool uses_scan_filters_p)
	    : ops(ops_p), uses_scan_filters(uses_scan_filters_p),
	      schedule_facts(SljitAnalyzeFullPipelineScheduleFacts(ops_p, uses_scan_filters_p)),
	      binding(ops_p, source_min_values_p, source_max_values_p,
	              schedule_facts.uses_extended_source_fetch_budget) {
	}

	SljitFullPipelineRecipePlan Build() const {
		SljitFullPipelineRecipe recipe;
		idx_t count;
		auto registry = RecipeRegistry(count);
		for (idx_t entry_idx = 0; entry_idx < count; entry_idx++) {
			if ((this->*registry[entry_idx].try_build)(recipe)) {
				return binding.MakePrimitiveRecipePlan(std::move(recipe));
			}
		}
		return binding.MakeNativeOnlyPlan();
	}

private:
	using TryBuildRecipeFunction = bool (SljitFullPipelineRecipeBuilder::*)(SljitFullPipelineRecipe &) const;

	struct SljitFullPipelineRecipeRegistryEntry {
		const char *name;
		TryBuildRecipeFunction try_build;
	};

	static const SljitFullPipelineRecipeRegistryEntry *RecipeRegistry(idx_t &count) {
		static const SljitFullPipelineRecipeRegistryEntry registry[] = {
		    {"selected_join_aggregate", &SljitFullPipelineRecipeBuilder::TryBuildSelectedJoinAggregateRecipe},
		    {"hash_join_delim_join_sink", &SljitFullPipelineRecipeBuilder::TryBuildHashJoinDelimJoinSinkRecipe},
		    {"projection_aggregate", &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateRecipe},
		    {"mark_filter_projection_native_tail",
		     &SljitFullPipelineRecipeBuilder::TryBuildMarkFilterProjectionNativeTailRecipe},
		    {"generated_filter_projection_native_tail",
		     &SljitFullPipelineRecipeBuilder::TryBuildGeneratedFilterProjectionNativeTailRecipe},
		    {"projection_filter_projection_native_tail",
		     &SljitFullPipelineRecipeBuilder::TryBuildProjectionFilterProjectionNativeTailRecipe},
		    {"source_batch_native_tail", &SljitFullPipelineRecipeBuilder::TryBuildSourceBatchNativeTailRecipe}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSelectedJoinAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSelectedJoinAggregateFacts facts;
		if (!SljitTryAnalyzeSelectedJoinAggregate(ops, facts)) {
			return false;
		}
		if (facts.HasSecondHashJoin()) {
			if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx) ||
			    !SljitAggregateUpdateCanUseSelectedJoinPerfectHashBackend(ops[facts.aggregate_idx])) {
				return false;
			}
			recipe = binding.MakeTwoJoinSelectedAggregateRecipe(facts.first_hash_join_idx, facts.second_hash_join_idx,
			                                                    facts.aggregate_idx);
			return true;
		}
		if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
		    !SljitAggregateUpdateCanUseSelectedJoinPerfectHashBackend(ops[facts.aggregate_idx])) {
			return false;
		}
		recipe = binding.MakeSelectedJoinAggregateRecipe(facts.first_hash_join_idx, facts.aggregate_idx);
		return true;
	}

	bool TryBuildGeneratedFilterProjectionNativeTailRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitGeneratedFilterProjectionNativeTailFacts facts;
		if (!SljitTryAnalyzeGeneratedFilterProjectionNativeTail(ops, facts)) {
			return false;
		}
		recipe = binding.MakeGeneratedFilterProjectionNativeTailRecipe(facts);
		return true;
	}

	bool TryBuildHashJoinDelimJoinSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinDelimJoinSinkFacts facts;
		if (!SljitTryAnalyzeHashJoinDelimJoinSink(ops, facts)) {
			return false;
		}
		if (facts.sink_idx + 3 > SLJIT_FULL_PIPELINE_MAX_PRIMITIVES ||
		    !SljitCanBindSelectedHashJoinDelimJoinSinkPrimitive(ops, facts.final_hash_join_idx, facts.sink_idx)) {
			return false;
		}
		for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx <= facts.final_hash_join_idx;
		     hash_join_idx++) {
			if (hash_join_idx == facts.final_hash_join_idx) {
				if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
					return false;
				}
				continue;
			}
			if (!SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx)) {
				return false;
			}
		}
		recipe =
		    binding.MakeHashJoinDelimJoinSinkRecipe(facts.first_hash_join_idx, facts.final_hash_join_idx, facts.sink_idx);
		return true;
	}

	bool TryBuildProjectionFilterProjectionNativeTailRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitProjectionFilterProjectionNativeTailFacts facts;
		if (!SljitTryAnalyzeProjectionFilterProjectionNativeTail(ops, facts)) {
			return false;
		}
		recipe = binding.MakeProjectionFilterProjectionNativeTailRecipe(facts);
		return true;
	}

	bool TryBuildMarkFilterProjectionNativeTailRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitMarkFilterProjectionNativeTailFacts facts;
		if (!SljitTryAnalyzeMarkFilterProjectionNativeTail(ops, facts)) {
			return false;
		}
		recipe = binding.MakeMarkFilterProjectionNativeTailRecipe(facts);
		return true;
	}

	bool TryBuildSourceBatchNativeTailRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSourceBatchNativeTailFacts facts;
		if (!SljitTryAnalyzeSourceBatchNativeTail(ops, uses_scan_filters, facts) ||
		    !SljitCanBindNativeTailHandoffPrimitive(ops, facts.tail_start_idx)) {
			return false;
		}
		recipe = binding.MakeSourceBatchNativeTailRecipe(facts);
		return true;
	}

	bool TryBuildProjectionAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitProjectionAggregatePlanFacts plan;
		if (!SljitTryAnalyzeProjectionAggregatePlan(ops, plan)) {
			return false;
		}
		idx_t count;
		auto registry = ProjectionAggregateRecipeRegistry(count);
		for (idx_t entry_idx = 0; entry_idx < count; entry_idx++) {
			if ((this->*registry[entry_idx].try_build)(recipe, plan)) {
				return true;
			}
		}
		return false;
	}

	using TryBuildProjectionAggregateRecipeFunction =
	    bool (SljitFullPipelineRecipeBuilder::*)(SljitFullPipelineRecipe &,
	                                             const SljitProjectionAggregatePlanFacts &) const;

	struct SljitProjectionAggregateRecipeRegistryEntry {
		const char *name;
		TryBuildProjectionAggregateRecipeFunction try_build;
	};

	static const SljitProjectionAggregateRecipeRegistryEntry *ProjectionAggregateRecipeRegistry(idx_t &count) {
		static const SljitProjectionAggregateRecipeRegistryEntry registry[] = {
		    {"source_grouped_aggregate", &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateSourceGrouped},
		    {"single_join_mark_filter", &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateSingleMarkFilter},
		    {"single_join_source_filter_projection",
		     &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateSingleSourceFilterProjection},
		    {"single_join_pre_projection", &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateSinglePreProjection},
		    {"single_join_direct_projection",
		     &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateSingleDirectProjection},
		    {"single_join_projection_chain",
		     &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateSingleProjectionChain},
		    {"two_join_mark_filter", &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateTwoJoinMarkFilter},
		    {"two_join_filtered_projection_prefix",
		     &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateTwoJoinSourceFilterProjection},
		    {"two_join_pre_projection", &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateTwoJoinPreProjection},
		    {"two_join_between_projection",
		     &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateTwoJoinBetweenProjection},
		    {"two_join_direct_projection",
		     &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateTwoJoinDirectProjection},
		    {"two_join_projection_chain",
		     &SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateTwoJoinProjectionChain}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildProjectionAggregateSourceGrouped(SljitFullPipelineRecipe &recipe,
	                                              const SljitProjectionAggregatePlanFacts &plan) const {
		auto &shape = plan.shape;
		if (!SljitProjectionAggregateCanUseSourceCountStarGroupedAggregate(ops, plan)) {
			return false;
		}
		recipe = binding.MakeSourceProjectionGroupedAggregateRecipe(shape);
		return true;
	}

	bool TryBuildProjectionAggregateSingleMarkFilter(SljitFullPipelineRecipe &recipe,
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

	bool TryBuildProjectionAggregateSingleSourceFilterProjection(
	    SljitFullPipelineRecipe &recipe, const SljitProjectionAggregatePlanFacts &plan) const {
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

	bool TryBuildProjectionAggregateSinglePreProjection(SljitFullPipelineRecipe &recipe,
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

	bool TryBuildProjectionAggregateSingleDirectProjection(SljitFullPipelineRecipe &recipe,
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

	bool TryBuildProjectionAggregateSingleProjectionChain(SljitFullPipelineRecipe &recipe,
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

	bool TryBuildProjectionAggregateTwoJoinMarkFilter(SljitFullPipelineRecipe &recipe,
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

	bool TryBuildProjectionAggregateTwoJoinSourceFilterProjection(
	    SljitFullPipelineRecipe &recipe, const SljitProjectionAggregatePlanFacts &plan) const {
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

	bool TryBuildProjectionAggregateTwoJoinPreProjection(SljitFullPipelineRecipe &recipe,
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

	bool TryBuildProjectionAggregateTwoJoinBetweenProjection(SljitFullPipelineRecipe &recipe,
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

	bool TryBuildProjectionAggregateTwoJoinDirectProjection(SljitFullPipelineRecipe &recipe,
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

	bool TryBuildProjectionAggregateTwoJoinProjectionChain(SljitFullPipelineRecipe &recipe,
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
	bool uses_scan_filters;
	SljitFullPipelineScheduleFacts schedule_facts;
	SljitFullPipelineRecipeBinding binding;
};

static SljitFullPipelineRecipePlan BuildSljitFullPipelineRecipePlan(const vector<SljitExecutableRegionOp> &ops,
                                                                    const vector<Value> &source_min_values,
                                                                    const vector<Value> &source_max_values,
                                                                    bool uses_scan_filters) {
	return SljitFullPipelineRecipeBuilder(ops, source_min_values, source_max_values, uses_scan_filters).Build();
}

} // namespace duckdb
