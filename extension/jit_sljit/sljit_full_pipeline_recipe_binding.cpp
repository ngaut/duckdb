//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_binding.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_recipe_binding.hpp"

#include "duckdb/common/constants.hpp"

#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"
#include "sljit_region_plan_internal.hpp"

#include <utility>

namespace duckdb {

SljitFullPipelineRecipeBinding::SljitFullPipelineRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
                                                               const vector<LogicalType> &source_output_types_p,
                                                               const vector<Value> &source_min_values_p,
                                                               const vector<Value> &source_max_values_p)
    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p),
      projection_aggregate_recipes(ops_p, source_output_types_p, source_min_values_p, source_max_values_p) {
}

SljitFullPipelineRecipePlan SljitFullPipelineRecipeBinding::MakeNativeOnlyPlan() const {
	string runtime_path = "full_pipeline.recipe.native_only";
	for (auto &op : ops) {
		runtime_path += ".";
		runtime_path += SljitNativeRegionOpKindName(op.kind);
	}
	return SljitMakeFullPipelineNativeOnlyPlan(std::move(runtime_path));
}

SljitFullPipelineRecipePlan
SljitFullPipelineRecipeBinding::MakePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) const {
	return SljitMakeFullPipelinePrimitiveRecipePlan(std::move(recipe));
}

bool SljitFullPipelineRecipeBinding::TryMakeProjectionFilterProjectionNativeTailRecipe(
    const SljitProjectionFilterProjectionNativeTailFacts &facts, SljitFullPipelineRecipe &recipe) const {
	SljitGeneratedFilterPrimitive generated_filter;
	SljitProjectionChainPrimitive pre_projection;
	SljitProjectionChainPrimitive projection;
	if (!SljitTryBindGeneratedFilterPrimitive(ops, facts.filter_idx, generated_filter) ||
	    !SljitTryBindProjectionChainPrimitive(ops, facts.pre_projection_idx, pre_projection) ||
	    !SljitTryBindProjectionChainPrimitive(ops, facts.projection_idx, projection)) {
		return false;
	}
	auto sequence = MakeSourceSequence();
	if (facts.filter_can_run_before_pre_projection) {
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(pre_projection));
	} else {
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(pre_projection));
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
	return TryMakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx, recipe);
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeSourceUngroupedAggregateRecipe(
    const SljitSourceUngroupedAggregateFacts &facts) const {
	auto sequence = MakeSourceSequence();
	auto aggregate_update = SljitBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
	return MakePrimitiveSequence(std::move(sequence));
}

bool SljitFullPipelineRecipeBinding::TryMakeSourceFilterAggregateRecipe(const SljitSourceFilterAggregateFacts &facts,
                                                                        SljitFullPipelineRecipe &recipe) const {
	SljitUngroupedAggregateUpdatePrimitive filtered_ungrouped;
	SljitUngroupedAggregateUpdatePrimitive ungrouped;
	SljitGroupedAggregateUpdatePrimitive filtered_grouped;
	SljitGroupedAggregateUpdatePrimitive grouped;
	SljitGeneratedFilterPrimitive generated_filter;
	const auto can_bind_filtered_ungrouped = SljitTryBindFilteredUngroupedAggregateUpdatePrimitive(
	    ops, facts.filter_idx, facts.aggregate_idx, filtered_ungrouped);
	const auto can_bind_ungrouped = SljitTryBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx, ungrouped);
	const auto can_bind_filtered_grouped = SljitTryBindFilteredGroupedAggregateUpdatePrimitive(
	    ops, facts.filter_idx, facts.aggregate_idx, filtered_grouped);
	const auto has_dedicated_grouped_backend =
	    SljitTryBindGroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx, grouped);
	const auto can_bind_generated_filter =
	    SljitTryBindGeneratedFilterPrimitive(ops, facts.filter_idx, generated_filter);
	const auto filtered_grouped_uses_fused_kernel =
	    facts.aggregate_idx < ops.size() &&
	    ops[facts.aggregate_idx].kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
	    ops[facts.aggregate_idx].aggregate_update.filtered_update.IsExecutable();
	if (!can_bind_filtered_ungrouped && !can_bind_ungrouped && !can_bind_filtered_grouped &&
	    !has_dedicated_grouped_backend) {
		return false;
	}
	if (can_bind_filtered_grouped && !filtered_grouped_uses_fused_kernel && !can_bind_generated_filter) {
		return false;
	}

	auto sequence = MakeSourceSequence();
	if (can_bind_filtered_ungrouped) {
		sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(filtered_ungrouped));
	} else if (can_bind_filtered_grouped) {
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(filtered_grouped));
	} else {
		if (!can_bind_generated_filter) {
			return false;
		}
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		if (can_bind_ungrouped) {
			sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(ungrouped));
		} else if (has_dedicated_grouped_backend) {
			sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped));
		} else {
			D_ASSERT(false);
			return false;
		}
	}
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

bool SljitFullPipelineRecipeBinding::TryMakeJoinFilterAggregateRecipe(const SljitJoinFilterAggregateFacts &facts,
                                                                      SljitFullPipelineRecipe &recipe) const {
	SljitFullPipelinePrimitiveStep probe_step;
	SljitGeneratedFilterPrimitive generated_filter;
	if (!TryMakeHashJoinProbeSelectionStep(facts.hash_join_idx, probe_step) ||
	    !SljitTryBindGeneratedFilterPrimitive(ops, facts.filter_idx, generated_filter)) {
		return false;
	}
	SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
	if (!SljitTryBindPostJoinProjectionPrimitive(ops, facts.hash_join_idx, facts.first_post_join_projection_idx,
	                                             facts.final_post_join_projection_idx,
	                                             post_join_aggregate.post_join_projection)) {
		return false;
	}
	post_join_aggregate.aggregate_idx = facts.aggregate_idx;
	if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, post_join_aggregate)) {
		return false;
	}

	auto sequence = MakeSourceSequence();
	if (!TryAddSourcePrefixSteps(sequence, facts.hash_join_idx)) {
		return false;
	}
	sequence.Add(std::move(probe_step));
	const auto probe_step_idx = sequence.Count() - 1;
	sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	sequence.Add(SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(post_join_aggregate));
	auto direct_consumer = SljitMakeHashJoinDirectAggregateConsumerContract(
	    probe_step_idx, sequence.Count() - 1, facts.hash_join_idx, facts.aggregate_idx, facts.filter_idx);
	recipe = MakePrimitiveSequence(std::move(sequence), direct_consumer);
	return true;
}

bool SljitFullPipelineRecipeBinding::TryMakeGeneratedFilterProjectionNativeTailRecipe(
    const SljitGeneratedFilterProjectionNativeTailFacts &facts, SljitFullPipelineRecipe &recipe) const {
	SljitGeneratedFilterPrimitive generated_filter;
	SljitProjectionChainPrimitive projection;
	if (!SljitTryBindGeneratedFilterPrimitive(ops, facts.filter_idx, generated_filter) ||
	    !SljitTryBindProjectionChainPrimitive(ops, facts.projection_idx, projection)) {
		return false;
	}
	auto sequence = MakeSourceSequence();
	sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
	return TryMakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx, recipe);
}

bool SljitFullPipelineRecipeBinding::TryAddSourcePrefixSteps(SljitFullPipelinePrimitiveSequence &sequence,
                                                             idx_t prefix_end) const {
	idx_t op_idx = 0;
	while (op_idx < prefix_end) {
		if (ops[op_idx].kind == SljitNativeRegionOpKind::FILTER) {
			SljitGeneratedFilterPrimitive generated_filter;
			if (!SljitTryBindGeneratedFilterPrimitive(ops, op_idx, generated_filter)) {
				return false;
			}
			sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
			op_idx++;
			continue;
		}
		// Consecutive projections run as one chain.
		auto final_projection_idx = op_idx;
		while (final_projection_idx + 1 < prefix_end &&
		       ops[final_projection_idx + 1].kind == SljitNativeRegionOpKind::PROJECTION) {
			final_projection_idx++;
		}
		SljitProjectionChainPrimitive projection;
		if (!SljitTryBindProjectionChainPrimitive(ops, op_idx, final_projection_idx, projection)) {
			return false;
		}
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
		op_idx = final_projection_idx + 1;
	}
	return true;
}

bool SljitFullPipelineRecipeBinding::TryMakeSourceHashJoinBuildSinkRecipe(
    const SljitSourceHashJoinBuildSinkFacts &facts, SljitFullPipelineRecipe &recipe) const {
	if (facts.sink_idx >= ops.size() || facts.sink_idx != ops.size() - 1) {
		return false;
	}
	auto sequence = MakeSourceSequence();
	if (!TryAddSourcePrefixSteps(sequence, facts.sink_idx)) {
		return false;
	}
	SljitHashJoinBuildSinkPrimitive sink;
	if (!SljitTryBindHashJoinBuildSinkPrimitive(ops, facts.sink_idx, DConstants::INVALID_INDEX, sink)) {
		return false;
	}
	sink.direct_source_ingress = true;
	sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinBuildSink(sink));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

bool SljitFullPipelineRecipeBinding::TryMakeHashJoinBuildSinkRecipe(const SljitHashJoinBuildSinkFacts &facts,
                                                                    SljitFullPipelineRecipe &recipe) const {
	if (facts.first_hash_join_idx > facts.final_hash_join_idx) {
		return false;
	}
	SljitHashJoinBuildSinkPrimitive sink;
	if (!SljitTryBindHashJoinBuildSinkPrimitive(ops, facts.sink_idx, facts.projection_idx, sink)) {
		return false;
	}
	auto sequence = MakeSourceSequence();
	if (!TryAddSourcePrefixSteps(sequence, facts.first_hash_join_idx)) {
		return false;
	}
	for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx < facts.final_hash_join_idx; hash_join_idx++) {
		SljitFullPipelinePrimitiveStep probe_step;
		if (!TryMakeHashJoinProbeMaterializeStep(hash_join_idx, probe_step)) {
			return false;
		}
		sequence.Add(std::move(probe_step));
	}
	SljitFullPipelinePrimitiveStep selection_step;
	if (!TryMakeHashJoinProbeSelectionStep(facts.final_hash_join_idx, selection_step)) {
		return false;
	}
	sequence.Add(std::move(selection_step));
	sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinBuildSink(sink));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

} // namespace duckdb
