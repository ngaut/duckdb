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
#include "sljit_projection_aggregate_recipe_binding.hpp"

#include <utility>

namespace duckdb {

SljitFullPipelineRecipeBinding::SljitFullPipelineRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
                                                               const vector<LogicalType> &source_output_types_p,
                                                               const vector<Value> &source_min_values_p,
                                                               const vector<Value> &source_max_values_p,
                                                               bool uses_extended_source_fetch_budget_p)
    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
                                             uses_extended_source_fetch_budget_p) {
}

SljitFullPipelineRecipePlan SljitFullPipelineRecipeBinding::MakeNativeOnlyPlan() const {
	return SljitMakeFullPipelineNativeOnlyPlan();
}

SljitFullPipelineRecipePlan
SljitFullPipelineRecipeBinding::MakePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) const {
	if (!SljitFullPipelinePrimitiveSequenceIsExecutable(ops, recipe.primitive_sequence)) {
		throw InternalException("SLJIT recipe builder accepted an invalid full-pipeline primitive sequence");
	}
	return SljitMakeFullPipelinePrimitiveRecipePlan(std::move(recipe));
}

bool SljitFullPipelineRecipeBinding::CanMakeNativeTailRecipe(idx_t tail_start_idx) const {
	return SljitFullPipelineRecipeSequenceBuilder::CanMakeNativeTailRecipe(tail_start_idx);
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeProjectionFilterProjectionNativeTailRecipe(
    const SljitProjectionFilterProjectionNativeTailFacts &facts) const {
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
	return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeMarkFilterProjectionNativeTailRecipe(
    const SljitMarkFilterProjectionNativeTailFacts &facts) const {
	return ProjectionAggregateBinding().MakeMarkFilterProjectionNativeTailRecipe(facts);
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeSourceProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape) const {
	return ProjectionAggregateBinding().MakeSourceProjectionAggregateTailRecipe(shape);
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeSourceUngroupedAggregateRecipe(
    const SljitSourceUngroupedAggregateFacts &facts) const {
	auto sequence = MakeSourceSequence();
	auto aggregate_update = SljitBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
	return MakePrimitiveSequence(std::move(sequence));
}

bool SljitFullPipelineRecipeBinding::CanMakeSourceFilterAggregateRecipe(
    const SljitSourceFilterAggregateFacts &facts) const {
	if (!SljitCanBindGeneratedFilterPrimitive(ops, facts.filter_idx)) {
		return false;
	}
	if (SljitCanBindFilteredUngroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx) ||
	    SljitCanBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx) ||
	    SljitCanBindFilteredGroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx)) {
		return true;
	}
	return SljitGroupedAggregateUpdateHasDedicatedBackend(ops, facts.aggregate_idx);
}

SljitFullPipelineRecipe
SljitFullPipelineRecipeBinding::MakeSourceFilterAggregateRecipe(const SljitSourceFilterAggregateFacts &facts) const {
	auto sequence = MakeSourceSequence();
	if (SljitCanBindFilteredUngroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx)) {
		auto aggregate_update =
		    SljitBindFilteredUngroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
		return MakePrimitiveSequence(std::move(sequence));
	}
	if (SljitCanBindFilteredGroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx)) {
		auto aggregate_update =
		    SljitBindFilteredGroupedAggregateUpdatePrimitive(ops, facts.filter_idx, facts.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(aggregate_update));
		return MakePrimitiveSequence(std::move(sequence));
	}
	auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	if (SljitCanBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx)) {
		auto aggregate_update = SljitBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
		return MakePrimitiveSequence(std::move(sequence));
	}
	if (SljitGroupedAggregateUpdateHasDedicatedBackend(ops, facts.aggregate_idx)) {
		auto aggregate_update = SljitBindGroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(aggregate_update));
		return MakePrimitiveSequence(std::move(sequence));
	}
	throw InternalException("SLJIT source filter aggregate recipe has no dedicated aggregate backend");
}

bool SljitFullPipelineRecipeBinding::CanMakeJoinFilterAggregateRecipe(
    const SljitJoinFilterAggregateFacts &facts) const {
	if (facts.HasProjectionPrefix() &&
	    !SljitCanBindProjectionChainPrimitive(ops, facts.first_projection_idx, facts.final_projection_idx)) {
		return false;
	}
	if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.hash_join_idx) ||
	    !SljitCanBindGeneratedFilterPrimitive(ops, facts.filter_idx)) {
		return false;
	}
	SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
	post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
	    ops, facts.hash_join_idx, DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
	post_join_aggregate.aggregate_idx = facts.aggregate_idx;
	return SljitCanBindPostJoinProjectionAggregatePrimitive(ops, post_join_aggregate);
}

SljitFullPipelineRecipe
SljitFullPipelineRecipeBinding::MakeJoinFilterAggregateRecipe(const SljitJoinFilterAggregateFacts &facts) const {
	auto sequence = MakeSourceSequence();
	if (facts.HasProjectionPrefix()) {
		AddProjectionChainStep(sequence, facts.first_projection_idx, facts.final_projection_idx);
	}
	sequence.Add(MakeHashJoinProbeSelectionStep(facts.hash_join_idx));
	auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
	post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
	    ops, facts.hash_join_idx, DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
	post_join_aggregate.aggregate_idx = facts.aggregate_idx;
	if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, post_join_aggregate)) {
		throw InternalException("SLJIT join filter aggregate recipe has no post-join aggregate backend");
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(post_join_aggregate));
	return MakePrimitiveSequence(std::move(sequence));
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeGeneratedFilterProjectionNativeTailRecipe(
    const SljitGeneratedFilterProjectionNativeTailFacts &facts) const {
	auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
	auto sequence = MakeSourceSequence();
	sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	AddProjectionChainStep(sequence, facts.projection_idx);
	return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeHashJoinDelimJoinSinkRecipe(idx_t first_hash_join_idx,
                                                                                        idx_t final_hash_join_idx,
                                                                                        idx_t sink_idx) const {
	if (first_hash_join_idx > final_hash_join_idx) {
		throw InternalException("SLJIT hash-join delimiter recipe has an invalid hash-join range");
	}
	auto sequence = MakeSourceSequence();
	for (idx_t hash_join_idx = first_hash_join_idx; hash_join_idx < final_hash_join_idx; hash_join_idx++) {
		sequence.Add(MakeHashJoinProbeMaterializeStep(hash_join_idx));
	}
	sequence.Add(MakeHashJoinProbeSelectionStep(final_hash_join_idx));
	auto delim_sink = SljitBindSelectedHashJoinDelimJoinSinkPrimitive(ops, final_hash_join_idx, sink_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::DelimJoinSink(delim_sink));
	return MakePrimitiveSequence(std::move(sequence));
}

bool SljitFullPipelineRecipeBinding::CanMakeHashJoinAppendSinkRecipe(const SljitHashJoinAppendSinkFacts &facts) const {
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
	return true;
}

SljitFullPipelineRecipe
SljitFullPipelineRecipeBinding::MakeHashJoinAppendSinkRecipe(const SljitHashJoinAppendSinkFacts &facts) const {
	auto sequence = MakeSourceSequence();
	for (idx_t hash_join_idx = facts.first_hash_join_idx; hash_join_idx < facts.final_hash_join_idx; hash_join_idx++) {
		sequence.Add(MakeHashJoinProbeMaterializeStep(hash_join_idx));
	}
	sequence.Add(MakeHashJoinProbeSelectionStep(facts.final_hash_join_idx));
	auto sink = SljitBindSelectedHashJoinAppendSinkPrimitive(ops, facts.final_hash_join_idx, facts.sink_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::AppendSink(sink));
	return MakePrimitiveSequence(std::move(sequence));
}

bool SljitFullPipelineRecipeBinding::CanMakeHashJoinBuildSinkRecipe(const SljitHashJoinBuildSinkFacts &facts) const {
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
	return true;
}

SljitFullPipelineRecipe
SljitFullPipelineRecipeBinding::MakeHashJoinBuildSinkRecipe(const SljitHashJoinBuildSinkFacts &facts) const {
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
	return MakePrimitiveSequence(std::move(sequence));
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeJoinDirectProjectionAggregateRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts) const {
	return ProjectionAggregateBinding().MakeJoinDirectProjectionAggregateRecipe(shape, facts);
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeJoinProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts) const {
	return ProjectionAggregateBinding().MakeJoinProjectionAggregateTailRecipe(shape, facts);
}

SljitFullPipelineRecipe SljitFullPipelineRecipeBinding::MakeMarkFilterProjectionAggregateRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts) const {
	return ProjectionAggregateBinding().MakeMarkFilterProjectionAggregateRecipe(shape, facts);
}

SljitFullPipelineRecipe
SljitFullPipelineRecipeBinding::MakeMarkFilterNativeTailRecipe(const SljitProjectionAggregatePrefixFacts &facts) const {
	return ProjectionAggregateBinding().MakeMarkFilterNativeTailRecipe(facts);
}

bool SljitFullPipelineRecipeBinding::ProjectionAggregateHasDedicatedBackend(
    const SljitFullPipelineProjectionAggregateShape &shape,
    bool allow_direct_projected_primitive_payload_update) const {
	return ProjectionAggregateBinding().ProjectionAggregateHasDedicatedBackend(
	    shape, allow_direct_projected_primitive_payload_update);
}

bool SljitFullPipelineRecipeBinding::CanMakeProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape) const {
	return ProjectionAggregateBinding().CanMakeProjectionAggregateTailRecipe(shape);
}

SljitProjectionAggregateRecipeBinding SljitFullPipelineRecipeBinding::ProjectionAggregateBinding() const {
	return SljitProjectionAggregateRecipeBinding(ops, source_output_types, source_min_values, source_max_values,
	                                             uses_extended_source_fetch_budget);
}

} // namespace duckdb
