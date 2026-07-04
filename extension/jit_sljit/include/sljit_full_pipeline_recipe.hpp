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
	      binding(ops_p, source_min_values_p, source_max_values_p, uses_scan_filters_p) {
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
		    {"filtered_source_aggregate", &SljitFullPipelineRecipeBuilder::TryBuildFilteredSourceAggregateRecipe},
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

	bool TryBuildFilteredSourceAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitFilteredSourceAggregateFacts facts;
		if (!SljitTryAnalyzeFilteredSourceAggregate(ops, uses_scan_filters, facts)) {
			return false;
		}
		SljitSourceBatchNativeTailFacts source_batch_facts;
		source_batch_facts.boundary_op_idx = facts.hash_join_idx;
		source_batch_facts.tail_start_idx = facts.hash_join_idx;
		if (!SljitCanBindNativeTailHandoffPrimitive(ops, source_batch_facts.tail_start_idx)) {
			return false;
		}
		recipe = binding.MakeSourceBatchNativeTailRecipe(source_batch_facts);
		return true;
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
		auto kind = ProjectionAggregateRecipeKind(plan);
		idx_t count;
		auto registry = ProjectionAggregateRecipeRegistry(count);
		for (idx_t entry_idx = 0; entry_idx < count; entry_idx++) {
			if (registry[entry_idx].kind != kind) {
				continue;
			}
			if (TryBuildProjectionAggregateRecipeVariant(recipe, plan, registry[entry_idx].variant)) {
				return true;
			}
		}
		return false;
	}

	enum class SljitProjectionAggregateRecipeKind { SOURCE, SINGLE_JOIN, TWO_JOIN };

	enum class SljitProjectionAggregateRecipeVariant {
		SOURCE_GROUPED_AGGREGATE,
		SINGLE_JOIN_MARK_FILTER,
		SINGLE_JOIN_SOURCE_FILTER_PROJECTION,
		SINGLE_JOIN_PRE_PROJECTION,
		SINGLE_JOIN_DIRECT_PROJECTION,
		SINGLE_JOIN_PROJECTION_CHAIN,
		TWO_JOIN_MARK_FILTER,
		TWO_JOIN_SOURCE_FILTER_PROJECTION,
		TWO_JOIN_PRE_PROJECTION,
		TWO_JOIN_BETWEEN_PROJECTION,
		TWO_JOIN_DIRECT_PROJECTION,
		TWO_JOIN_PROJECTION_CHAIN
	};

	struct SljitProjectionAggregateRecipeRegistryEntry {
		SljitProjectionAggregateRecipeKind kind;
		SljitProjectionAggregateRecipeVariant variant;
		const char *name;
	};

	static SljitProjectionAggregateRecipeKind
	ProjectionAggregateRecipeKind(const SljitProjectionAggregatePlanFacts &plan) {
		if (!plan.prefix.HasFirstHashJoin()) {
			return SljitProjectionAggregateRecipeKind::SOURCE;
		}
		if (plan.prefix.HasSecondHashJoin()) {
			return SljitProjectionAggregateRecipeKind::TWO_JOIN;
		}
		return SljitProjectionAggregateRecipeKind::SINGLE_JOIN;
	}

	static const SljitProjectionAggregateRecipeRegistryEntry *ProjectionAggregateRecipeRegistry(idx_t &count) {
		static const SljitProjectionAggregateRecipeRegistryEntry registry[] = {
		    {SljitProjectionAggregateRecipeKind::SOURCE,
		     SljitProjectionAggregateRecipeVariant::SOURCE_GROUPED_AGGREGATE, "source_grouped_aggregate"},
		    {SljitProjectionAggregateRecipeKind::SINGLE_JOIN,
		     SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_MARK_FILTER, "single_join_mark_filter"},
		    {SljitProjectionAggregateRecipeKind::SINGLE_JOIN,
		     SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_SOURCE_FILTER_PROJECTION,
		     "single_join_source_filter_projection"},
		    {SljitProjectionAggregateRecipeKind::SINGLE_JOIN,
		     SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_PRE_PROJECTION, "single_join_pre_projection"},
		    {SljitProjectionAggregateRecipeKind::SINGLE_JOIN,
		     SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_DIRECT_PROJECTION, "single_join_direct_projection"},
		    {SljitProjectionAggregateRecipeKind::SINGLE_JOIN,
		     SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_PROJECTION_CHAIN, "single_join_projection_chain"},
		    {SljitProjectionAggregateRecipeKind::TWO_JOIN,
		     SljitProjectionAggregateRecipeVariant::TWO_JOIN_MARK_FILTER, "two_join_mark_filter"},
		    {SljitProjectionAggregateRecipeKind::TWO_JOIN,
		     SljitProjectionAggregateRecipeVariant::TWO_JOIN_SOURCE_FILTER_PROJECTION,
		     "two_join_filtered_projection_prefix"},
		    {SljitProjectionAggregateRecipeKind::TWO_JOIN,
		     SljitProjectionAggregateRecipeVariant::TWO_JOIN_PRE_PROJECTION, "two_join_pre_projection"},
		    {SljitProjectionAggregateRecipeKind::TWO_JOIN,
		     SljitProjectionAggregateRecipeVariant::TWO_JOIN_BETWEEN_PROJECTION, "two_join_between_projection"},
		    {SljitProjectionAggregateRecipeKind::TWO_JOIN,
		     SljitProjectionAggregateRecipeVariant::TWO_JOIN_DIRECT_PROJECTION, "two_join_direct_projection"},
		    {SljitProjectionAggregateRecipeKind::TWO_JOIN,
		     SljitProjectionAggregateRecipeVariant::TWO_JOIN_PROJECTION_CHAIN, "two_join_projection_chain"}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildProjectionAggregateRecipeVariant(SljitFullPipelineRecipe &recipe,
	                                              const SljitProjectionAggregatePlanFacts &plan,
	                                              SljitProjectionAggregateRecipeVariant variant) const {
		auto &shape = plan.shape;
		auto &facts = plan.prefix;
		switch (variant) {
		case SljitProjectionAggregateRecipeVariant::SOURCE_GROUPED_AGGREGATE:
			if (!SljitProjectionAggregateCanUseSourceCountStarGroupedAggregate(ops, plan)) {
				return false;
			}
			recipe = binding.MakeSourceProjectionGroupedAggregateRecipe(shape);
			return true;
		case SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_MARK_FILTER:
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
		case SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_SOURCE_FILTER_PROJECTION:
			if (!SljitProjectionAggregateCanUseSingleJoinSourceFilterProjection(plan) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
			    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
				return false;
			}
			recipe = binding.MakeFilterProjectionJoinProjectionAggregateRecipe(
			    shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx,
			    SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
			return true;
		case SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_PRE_PROJECTION:
			if (!SljitProjectionAggregateCanUseSingleJoinPreJoinProjection(plan) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
			    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
				return false;
			}
			recipe = binding.MakePreProjectionJoinProjectionAggregateRecipe(
			    shape, facts.first_hash_join_idx,
			    SljitDirectJoinOutputAggregateUpdateSchedule::IMMEDIATE_ROW_POINTER_UPDATE);
			return true;
		case SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_DIRECT_PROJECTION:
			if (!SljitProjectionAggregateCanUseSingleJoinSingleProjection(plan) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
			    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
				return false;
			}
			recipe = binding.MakeJoinProjectionAggregateRecipe(
			    shape, facts.first_hash_join_idx,
			    SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
			return true;
		case SljitProjectionAggregateRecipeVariant::SINGLE_JOIN_PROJECTION_CHAIN:
			if (!SljitProjectionAggregateCanUseSingleJoinProjectionChain(plan) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.first_hash_join_idx) ||
			    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
				return false;
			}
			recipe = binding.MakeJoinProjectionAggregateRecipe(
			    shape, facts.first_hash_join_idx,
			    SljitDirectJoinOutputAggregateUpdateSchedule::IMMEDIATE_ROW_POINTER_UPDATE);
			return true;
		case SljitProjectionAggregateRecipeVariant::TWO_JOIN_MARK_FILTER:
			if (!facts.HasMarkFilter() || facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() ||
			    facts.HasBetweenProjection() || facts.mark_filter_idx != facts.second_hash_join_idx + 1 ||
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
			                                                       facts.second_hash_join_idx,
			                                                       facts.mark_filter_idx);
			return true;
		case SljitProjectionAggregateRecipeVariant::TWO_JOIN_SOURCE_FILTER_PROJECTION:
			if (!facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() || facts.HasBetweenProjection() ||
			    shape.ProjectionCount() != 2 || !plan.HasFixedFinalProjection() ||
			    !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx)) {
				return false;
			}
			recipe = binding.MakeFilterProjectionTwoJoinProjectionAggregateRecipe(
			    shape, facts.source_filter_idx, facts.source_projection_idx, facts.first_hash_join_idx,
			    facts.second_hash_join_idx);
			return true;
		case SljitProjectionAggregateRecipeVariant::TWO_JOIN_PRE_PROJECTION:
			if (!facts.HasPreJoinProjection() || !facts.HasBetweenProjection() || shape.ProjectionCount() != 2 ||
			    !plan.HasFixedFinalProjection() || !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx)) {
				return false;
			}
			recipe = binding.MakePreProjectionTwoJoinProjectionAggregateRecipe(
			    shape, facts.pre_join_projection_idx, facts.first_hash_join_idx, facts.between_projection_idx,
			    facts.second_hash_join_idx);
			return true;
		case SljitProjectionAggregateRecipeVariant::TWO_JOIN_BETWEEN_PROJECTION:
			if (!facts.HasBetweenProjection() || facts.HasMarkFilter() || shape.ProjectionCount() == 0 ||
			    !CanBindHashJoinProbeProjectionInput(facts.first_hash_join_idx) ||
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
		case SljitProjectionAggregateRecipeVariant::TWO_JOIN_DIRECT_PROJECTION:
			if (facts.HasSourceFilterProjection() || facts.HasPreJoinProjection() || facts.HasBetweenProjection() ||
			    facts.HasMarkFilter() || shape.ProjectionCount() != 1 || !plan.HasFixedFinalProjection() ||
			    !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.first_hash_join_idx) ||
			    !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.second_hash_join_idx) ||
			    !DirectJoinProjectionAggregateHasDedicatedBackend(shape)) {
				return false;
			}
			recipe = binding.MakeTwoJoinDirectProjectionAggregateRecipe(shape, facts.first_hash_join_idx);
			return true;
		case SljitProjectionAggregateRecipeVariant::TWO_JOIN_PROJECTION_CHAIN:
			if (shape.ProjectionCount() != 2 || !plan.HasFixedFinalProjection() ||
			    !SljitCanBindHashJoinProbeMaterializePrimitive(ops, facts.first_hash_join_idx) ||
			    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.second_hash_join_idx)) {
				return false;
			}
			recipe = binding.MakeTwoJoinProjectionChainAggregateRecipe(shape, facts.first_hash_join_idx,
			                                                           facts.second_hash_join_idx);
			return true;
		}
		throw InternalException("SLJIT projection aggregate recipe has an unknown variant");
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
	SljitFullPipelineRecipeBinding binding;
};

static SljitFullPipelineRecipePlan BuildSljitFullPipelineRecipePlan(const vector<SljitExecutableRegionOp> &ops,
                                                                    const vector<Value> &source_min_values,
                                                                    const vector<Value> &source_max_values,
                                                                    bool uses_scan_filters) {
	return SljitFullPipelineRecipeBuilder(ops, source_min_values, source_max_values, uses_scan_filters).Build();
}

} // namespace duckdb
