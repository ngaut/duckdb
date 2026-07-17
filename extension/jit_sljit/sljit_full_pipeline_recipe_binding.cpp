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
                                                               const vector<Value> &source_max_values_p,
                                                               bool uses_extended_source_fetch_budget_p)
    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
                                             uses_extended_source_fetch_budget_p),
      projection_aggregate_recipes(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
                                   uses_extended_source_fetch_budget_p) {
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
	if (!SljitCanBindGeneratedFilterPrimitive(ops, facts.filter_idx) ||
	    !SljitCanBindProjectionChainPrimitive(ops, facts.pre_projection_idx) ||
	    !SljitCanBindProjectionChainPrimitive(ops, facts.projection_idx)) {
		return false;
	}
	auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
	auto sequence = MakeSourceSequence();
	if (facts.filter_can_run_before_pre_projection) {
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, facts.pre_projection_idx);
	} else {
		AddProjectionChainStep(sequence, facts.pre_projection_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	}
	AddProjectionChainStep(sequence, facts.projection_idx);
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
	const auto can_bind_filtered_ungrouped =
	    SljitCanBindFilteredUngroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx);
	const auto can_bind_ungrouped = SljitCanBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
	const auto can_bind_filtered_grouped =
	    SljitCanBindFilteredGroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx);
	const auto has_dedicated_grouped_backend = SljitGroupedAggregateUpdateHasDedicatedBackend(ops, facts.aggregate_idx);
	const auto can_bind_generated_filter = SljitCanBindGeneratedFilterPrimitive(ops, facts.filter_idx);
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
		auto aggregate_update =
		    SljitBindFilteredUngroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
	} else if (can_bind_filtered_grouped) {
		auto aggregate_update =
		    SljitBindFilteredGroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(aggregate_update));
	} else {
		if (!can_bind_generated_filter) {
			return false;
		}
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		if (can_bind_ungrouped) {
			auto aggregate_update = SljitBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
			sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
		} else if (has_dedicated_grouped_backend) {
			auto aggregate_update = SljitBindGroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
			sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(aggregate_update));
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
	if (facts.HasProjectionPrefix() &&
	    !SljitCanBindProjectionChainPrimitive(ops, facts.first_projection_idx, facts.final_projection_idx)) {
		return false;
	}
	if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.hash_join_idx) ||
	    !SljitCanBindGeneratedFilterPrimitive(ops, facts.filter_idx)) {
		return false;
	}
	if (!SljitCanBindPostJoinProjectionPrimitive(ops, facts.hash_join_idx, facts.first_post_join_projection_idx,
	                                             facts.final_post_join_projection_idx)) {
		return false;
	}
	SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
	post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
	    ops, facts.hash_join_idx, facts.first_post_join_projection_idx, facts.final_post_join_projection_idx);
	post_join_aggregate.aggregate_idx = facts.aggregate_idx;
	if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, post_join_aggregate)) {
		return false;
	}

	auto sequence = MakeSourceSequence();
	if (facts.HasProjectionPrefix()) {
		AddProjectionChainStep(sequence, facts.first_projection_idx, facts.final_projection_idx);
	}
	sequence.Add(MakeHashJoinProbeSelectionStep(facts.hash_join_idx));
	const auto probe_step_idx = sequence.Count() - 1;
	auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	sequence.Add(SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(post_join_aggregate));
	auto direct_consumer = SljitMakeHashJoinDirectAggregateConsumerContract(
	    probe_step_idx, sequence.Count() - 1, facts.hash_join_idx, facts.aggregate_idx, facts.filter_idx);
	recipe = MakePrimitiveSequence(std::move(sequence), direct_consumer);
	return true;
}

bool SljitFullPipelineRecipeBinding::TryMakeGeneratedFilterProjectionNativeTailRecipe(
    const SljitGeneratedFilterProjectionNativeTailFacts &facts, SljitFullPipelineRecipe &recipe) const {
	if (!SljitCanBindGeneratedFilterPrimitive(ops, facts.filter_idx) ||
	    !SljitCanBindProjectionChainPrimitive(ops, facts.projection_idx)) {
		return false;
	}
	auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
	auto sequence = MakeSourceSequence();
	sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	AddProjectionChainStep(sequence, facts.projection_idx);
	return TryMakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx, recipe);
}

bool SljitFullPipelineRecipeBinding::TryMakeSourceHashJoinBuildSinkRecipe(
    const SljitSourceHashJoinBuildSinkFacts &facts, SljitFullPipelineRecipe &recipe) const {
	if (facts.sink_idx + 1 != ops.size() || !SljitCanBindHashJoinBuildSinkPrimitive(ops, facts.sink_idx)) {
		return false;
	}
	for (idx_t op_idx = 0; op_idx < facts.sink_idx; op_idx++) {
		if (ops[op_idx].kind == SljitNativeRegionOpKind::FILTER) {
			if (!SljitCanBindGeneratedFilterPrimitive(ops, op_idx)) {
				return false;
			}
		} else if (ops[op_idx].kind == SljitNativeRegionOpKind::PROJECTION) {
			if (!SljitCanBindProjectionChainPrimitive(ops, op_idx)) {
				return false;
			}
		} else {
			return false;
		}
	}

	auto sequence = MakeSourceSequence();
	for (idx_t op_idx = 0; op_idx < facts.sink_idx; op_idx++) {
		if (ops[op_idx].kind == SljitNativeRegionOpKind::FILTER) {
			auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, op_idx);
			sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		} else if (ops[op_idx].kind == SljitNativeRegionOpKind::PROJECTION) {
			AddProjectionChainStep(sequence, op_idx);
		} else {
			D_ASSERT(false);
			return false;
		}
	}
	auto sink = SljitBindHashJoinBuildSinkPrimitive(ops, facts.sink_idx);
	sink.direct_source_ingress = true;
	sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinBuildSink(sink));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

bool SljitFullPipelineRecipeBinding::TryMakeHashJoinDelimJoinSinkRecipe(const SljitHashJoinDelimJoinSinkFacts &facts,
                                                                        SljitFullPipelineRecipe &recipe) const {
	if (facts.first_hash_join_idx > facts.final_hash_join_idx ||
	    facts.sink_idx + 3 > SLJIT_FULL_PIPELINE_MAX_PRIMITIVES) {
		return false;
	}
	auto sequence = MakeSourceSequence();
	for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx < facts.final_hash_join_idx; hash_join_idx++) {
		SljitHashJoinProbeMaterializePrimitive probe;
		const bool unchecked = hash_join_idx == 0 && SljitHashJoinSourceKey0RangeFitsInt32(
		                                                 ops, hash_join_idx, source_min_values, source_max_values);
		if (!SljitTryBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx, probe, unchecked)) {
			return false;
		}
		sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeMaterialize(probe));
	}
	SljitHashJoinProbeSelectionPrimitive selection;
	const bool unchecked =
	    facts.final_hash_join_idx == 0 &&
	    SljitHashJoinSourceKey0RangeFitsInt32(ops, facts.final_hash_join_idx, source_min_values, source_max_values);
	if (!SljitTryBindHashJoinProbeSelectionPrimitive(ops, facts.final_hash_join_idx, selection, unchecked)) {
		return false;
	}
	SljitDelimJoinSinkPrimitive delim_sink;
	if (!SljitTryBindSelectedHashJoinDelimJoinSinkPrimitive(ops, selection, facts.sink_idx, delim_sink)) {
		return false;
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(selection));
	sequence.Add(SljitFullPipelinePrimitiveStep::DelimJoinSink(delim_sink));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

bool SljitFullPipelineRecipeBinding::TryMakeHashJoinAppendSinkRecipe(const SljitHashJoinAppendSinkFacts &facts,
                                                                     SljitFullPipelineRecipe &recipe) const {
	if (facts.first_hash_join_idx > facts.final_hash_join_idx ||
	    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.final_hash_join_idx) ||
	    !SljitCanBindSelectedHashJoinAppendSinkPrimitive(ops, facts.final_hash_join_idx, facts.sink_idx)) {
		return false;
	}
	for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx < facts.final_hash_join_idx; hash_join_idx++) {
		if (!SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx)) {
			return false;
		}
	}

	auto sequence = MakeSourceSequence();
	for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx < facts.final_hash_join_idx; hash_join_idx++) {
		sequence.Add(MakeHashJoinProbeMaterializeStep(hash_join_idx));
	}
	sequence.Add(MakeHashJoinProbeSelectionStep(facts.final_hash_join_idx));
	auto sink = SljitBindSelectedHashJoinAppendSinkPrimitive(ops, facts.final_hash_join_idx, facts.sink_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::AppendSink(sink));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

bool SljitFullPipelineRecipeBinding::TryMakeHashJoinBuildSinkRecipe(const SljitHashJoinBuildSinkFacts &facts,
                                                                    SljitFullPipelineRecipe &recipe) const {
	if (facts.HasPreProjection() && !SljitCanBindProjectionChainPrimitive(ops, facts.pre_projection_idx)) {
		return false;
	}
	if (facts.HasFilterProjection() && (!SljitCanBindGeneratedFilterPrimitive(ops, facts.filter_idx) ||
	                                    !SljitCanBindProjectionChainPrimitive(ops, facts.filter_projection_idx))) {
		return false;
	}
	if (facts.first_hash_join_idx > facts.final_hash_join_idx ||
	    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.final_hash_join_idx) ||
	    !SljitCanBindHashJoinBuildSinkPrimitive(ops, facts.sink_idx, facts.projection_idx)) {
		return false;
	}
	for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx < facts.final_hash_join_idx; hash_join_idx++) {
		if (!SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx)) {
			return false;
		}
	}

	auto sequence = MakeSourceSequence();
	if (facts.HasPreProjection()) {
		AddProjectionChainStep(sequence, facts.pre_projection_idx);
	}
	if (facts.HasFilterProjection()) {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, facts.filter_projection_idx);
	}
	for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx < facts.final_hash_join_idx; hash_join_idx++) {
		sequence.Add(MakeHashJoinProbeMaterializeStep(hash_join_idx));
	}
	sequence.Add(MakeHashJoinProbeSelectionStep(facts.final_hash_join_idx));
	auto sink = SljitBindHashJoinBuildSinkPrimitive(ops, facts.sink_idx, facts.projection_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinBuildSink(sink));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

} // namespace duckdb
