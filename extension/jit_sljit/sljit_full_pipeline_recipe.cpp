//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_recipe.hpp"

#include "duckdb/common/constants.hpp"

#include "sljit_full_pipeline_recipe_binding.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"
#include "sljit_projection_source_runtime.hpp"

#include <utility>

namespace duckdb {

static constexpr idx_t SLJIT_PRIMITIVE_STATE_SOURCE_MIN_SEMANTIC_BYTES = 128;

static bool SljitPrimitiveAggregateStateKindsCanCombine(AggregatePrimitiveUpdateKind source,
                                                        AggregatePrimitiveUpdateKind target) {
	switch (target) {
	case AggregatePrimitiveUpdateKind::SUM_INT64:
		return AggregatePrimitiveUpdateUsesInt64State(source);
	case AggregatePrimitiveUpdateKind::SUM_HUGEINT:
		return AggregatePrimitiveUpdateUsesHugeintState(source) || AggregatePrimitiveUpdateUsesInt64State(source);
	case AggregatePrimitiveUpdateKind::SUM_DOUBLE:
		return source == AggregatePrimitiveUpdateKind::SUM_DOUBLE;
	default:
		return false;
	}
}

static idx_t SljitPrimitiveAggregateStateSemanticBytes(AggregatePrimitiveUpdateKind kind) {
	auto bytes = AggregatePrimitiveUpdateStateValueSize(kind);
	if (AggregatePrimitiveUpdateHasStateIsSet(kind)) {
		bytes += sizeof(uint64_t);
	}
	return bytes;
}

static bool SljitTryBindPrimitiveAggregateStateSource(const SljitNativeAggregateStateSourcePlan &source,
                                                      const vector<SljitExecutableRegionOp> &ops,
                                                      const SljitSourceUngroupedAggregateFacts &facts,
                                                      vector<ExecutionAggregateStateCombineLane> &lanes) {
	if (source.function_name != "hash_aggregate_scan" || !source.state_scan_contract.primitive_aggregate_batch_ready ||
	    facts.aggregate_idx >= ops.size()) {
		return false;
	}
	auto &aggregate_op = ops[facts.aggregate_idx];
	auto &sink_aggregates = aggregate_op.aggregate_update.plan.sink_info.aggregates;
	auto &payloads = aggregate_op.aggregate_update.payloads;
	if (sink_aggregates.empty() || sink_aggregates.size() != payloads.size()) {
		return false;
	}
	vector<ExecutionAggregateStateCombineLane> candidate;
	candidate.reserve(sink_aggregates.size());
	for (idx_t target_idx = 0; target_idx < sink_aggregates.size(); target_idx++) {
		auto &target = sink_aggregates[target_idx];
		auto &payload = payloads[target_idx];
		if (!target.primitive_update_ready) {
			return false;
		}
		ExecutionAggregateStateCombineLane lane;
		lane.target_aggregate_index = target.aggregate_index;
		if (target.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (target.child_count != 0) {
				return false;
			}
			lane.source_kind = ExecutionAggregateStateCombineSourceKind::ROW_COUNT;
			candidate.push_back(lane);
			continue;
		}
		idx_t source_output_idx;
		if (target.child_count != 1 || target.child_types.size() != 1 ||
		    !SljitTryGetExecutableReferenceInputIndex(payload, source_output_idx)) {
			return false;
		}
		optional_ptr<const ExecutionRegionPrimitiveAggregateStateLane> source_lane;
		for (auto &candidate_lane : source.state_scan_contract.primitive_aggregate_lanes) {
			if (candidate_lane.source_output_index == source_output_idx) {
				source_lane = candidate_lane;
				break;
			}
		}
		if (!source_lane) {
			return false;
		}
		if (source_lane->return_type != target.child_types[0] ||
		    !SljitPrimitiveAggregateStateKindsCanCombine(source_lane->primitive_update_kind,
		                                                 target.primitive_update_kind)) {
			return false;
		}
		lane.source_aggregate_index = source_lane->aggregate_index;
		candidate.push_back(lane);
	}
	idx_t semantic_state_bytes = 0;
	vector<idx_t> counted_source_aggregates;
	for (auto &mapping : candidate) {
		if (mapping.source_kind != ExecutionAggregateStateCombineSourceKind::AGGREGATE_STATE) {
			continue;
		}
		bool already_counted = false;
		for (auto aggregate_index : counted_source_aggregates) {
			if (aggregate_index == mapping.source_aggregate_index) {
				already_counted = true;
				break;
			}
		}
		if (already_counted) {
			continue;
		}
		for (auto &source_lane : source.state_scan_contract.primitive_aggregate_lanes) {
			if (source_lane.aggregate_index == mapping.source_aggregate_index) {
				semantic_state_bytes += SljitPrimitiveAggregateStateSemanticBytes(source_lane.primitive_update_kind);
				counted_source_aggregates.push_back(mapping.source_aggregate_index);
				break;
			}
		}
	}
	// Narrow scans are already cheaper through DuckDB's finalized vector path.
	// Elide materialization only once the mapped state row spans enough semantic
	// payload to recover at least two typical cache lines per row-major visit.
	if (semantic_state_bytes < SLJIT_PRIMITIVE_STATE_SOURCE_MIN_SEMANTIC_BYTES) {
		return false;
	}
	lanes = std::move(candidate);
	return true;
}

class SljitFullPipelineRecipeBuilder {
public:
	SljitFullPipelineRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                               const SljitNativeAggregateStateSourcePlan &source_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
	    : SljitFullPipelineRecipeBuilder(ops_p, source_p, source_output_types_p, source_min_values_p,
	                                     source_max_values_p, SljitAnalyzeFullPipelineScheduleFacts(ops_p)) {
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
		TryBuildRecipeFunction try_build;
	};

	static const SljitFullPipelineRecipeRegistryEntry *RecipeRegistry(idx_t &count) {
		static const SljitFullPipelineRecipeRegistryEntry registry[] = {
		    {&SljitFullPipelineRecipeBuilder::TryBuildSourceUngroupedAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildSourceFilterAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildJoinFilterAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildSourceHashJoinBuildSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildHashJoinDelimJoinSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildHashJoinAppendSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildHashJoinBuildSinkRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildProjectionAggregateRecipe},
		    {&SljitFullPipelineRecipeBuilder::TryBuildNativeTailRecipe}};
		count = sizeof(registry) / sizeof(registry[0]);
		return registry;
	}

	bool TryBuildSourceUngroupedAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSourceUngroupedAggregateFacts facts;
		if (!SljitTryAnalyzeSourceUngroupedAggregate(ops, facts)) {
			return false;
		}
		recipe = binding.MakeSourceUngroupedAggregateRecipe(facts);
		SljitTryBindPrimitiveAggregateStateSource(source, ops, facts, recipe.primitive_aggregate_state_source_lanes);
		return true;
	}

	bool TryBuildSourceFilterAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSourceFilterAggregateFacts facts;
		return SljitTryAnalyzeSourceFilterAggregate(ops, facts) &&
		       binding.TryMakeSourceFilterAggregateRecipe(facts, recipe);
	}

	bool TryBuildJoinFilterAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitJoinFilterAggregateFacts facts;
		if (!SljitTryAnalyzeJoinFilterAggregate(ops, facts) ||
		    !binding.TryMakeJoinFilterAggregateRecipe(facts, recipe)) {
			return false;
		}
		return true;
	}

	bool TryBuildSourceHashJoinBuildSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitSourceHashJoinBuildSinkFacts facts;
		return SljitTryAnalyzeSourceHashJoinBuildSink(ops, facts) &&
		       binding.TryMakeSourceHashJoinBuildSinkRecipe(facts, recipe);
	}

	bool TryBuildHashJoinDelimJoinSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinDelimJoinSinkFacts facts;
		return SljitTryAnalyzeHashJoinDelimJoinSink(ops, facts) &&
		       binding.TryMakeHashJoinDelimJoinSinkRecipe(facts, recipe);
	}

	bool TryBuildHashJoinAppendSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinAppendSinkFacts facts;
		return SljitTryAnalyzeHashJoinAppendSink(ops, facts) && binding.TryMakeHashJoinAppendSinkRecipe(facts, recipe);
	}

	bool TryBuildHashJoinBuildSinkRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitHashJoinBuildSinkFacts facts;
		return SljitTryAnalyzeHashJoinBuildSink(ops, facts) && binding.TryMakeHashJoinBuildSinkRecipe(facts, recipe);
	}

	bool TryBuildProjectionAggregateRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitProjectionAggregatePlanFacts plan;
		if (!SljitTryAnalyzeProjectionAggregatePlan(ops, plan)) {
			return false;
		}
		auto &recipes = binding.ProjectionAggregateRecipes();
		switch (plan.prefix.Kind()) {
		case SljitProjectionAggregatePrefixKind::SOURCE:
			return recipes.TryMakeSourceProjectionAggregateTailRecipe(plan.shape, recipe);
		case SljitProjectionAggregatePrefixKind::JOIN_PREFIX:
			return recipes.TryMakeMarkFilterProjectionAggregateRecipe(plan.shape, plan.prefix, recipe) ||
			       recipes.TryMakeMarkFilterNativeTailRecipe(plan.prefix, recipe) ||
			       recipes.TryMakeJoinDirectProjectionAggregateRecipe(plan.shape, plan.prefix, recipe) ||
			       recipes.TryMakeJoinProjectionAggregateTailRecipe(plan.shape, plan.prefix, recipe);
		case SljitProjectionAggregatePrefixKind::INVALID:
			return false;
		}
		return false;
	}

	bool TryBuildNativeTailRecipe(SljitFullPipelineRecipe &recipe) const {
		SljitMarkFilterProjectionNativeTailFacts mark_filter;
		if (SljitTryAnalyzeMarkFilterProjectionNativeTail(ops, mark_filter) &&
		    binding.ProjectionAggregateRecipes().TryMakeMarkFilterProjectionNativeTailRecipe(mark_filter, recipe)) {
			return true;
		}
		SljitGeneratedFilterProjectionNativeTailFacts generated_filter;
		if (SljitTryAnalyzeGeneratedFilterProjectionNativeTail(ops, generated_filter) &&
		    binding.TryMakeGeneratedFilterProjectionNativeTailRecipe(generated_filter, recipe)) {
			return true;
		}
		SljitProjectionFilterProjectionNativeTailFacts projection_filter;
		return SljitTryAnalyzeProjectionFilterProjectionNativeTail(ops, projection_filter) &&
		       binding.TryMakeProjectionFilterProjectionNativeTailRecipe(projection_filter, recipe);
	}

private:
	SljitFullPipelineRecipeBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                               const SljitNativeAggregateStateSourcePlan &source_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               const SljitFullPipelineScheduleFacts &schedule_facts)
	    : ops(ops_p), source(source_p), binding(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
	                                            schedule_facts.uses_extended_source_fetch_budget) {
	}

	const vector<SljitExecutableRegionOp> &ops;
	const SljitNativeAggregateStateSourcePlan &source;
	SljitFullPipelineRecipeBinding binding;
};

SljitFullPipelineRecipePlan BuildSljitFullPipelineRecipePlan(const vector<SljitExecutableRegionOp> &ops,
                                                             const SljitNativeAggregateStateSourcePlan &source,
                                                             const vector<LogicalType> &source_output_types,
                                                             const vector<Value> &source_min_values,
                                                             const vector<Value> &source_max_values) {
	return SljitFullPipelineRecipeBuilder(ops, source, source_output_types, source_min_values, source_max_values)
	    .Build();
}

} // namespace duckdb
