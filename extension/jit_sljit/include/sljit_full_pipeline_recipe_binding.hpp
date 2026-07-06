//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"
#include "sljit_full_pipeline_recipe_sequence_builder.hpp"
#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_full_pipeline_shape.hpp"
#include "sljit_generated_filter_primitive.hpp"
#include "sljit_projection_aggregate_recipe_binding.hpp"
#include "sljit_projection_chain_primitive.hpp"

#include <utility>

namespace duckdb {

class SljitFullPipelineRecipeBinding : private SljitFullPipelineRecipeSequenceBuilder {
public:
	SljitFullPipelineRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               bool uses_extended_source_fetch_budget_p)
	    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
	                                             uses_extended_source_fetch_budget_p) {
	}

	SljitFullPipelineRecipePlan MakeNativeOnlyPlan() const {
		return SljitMakeFullPipelineNativeOnlyPlan(uses_extended_source_fetch_budget);
	}

	SljitFullPipelineRecipePlan MakePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) const {
		if (!SljitFullPipelinePrimitiveSequenceIsExecutable(ops, recipe.primitive_sequence)) {
			throw InternalException("SLJIT recipe builder accepted an invalid full-pipeline primitive sequence");
		}
		return SljitMakeFullPipelinePrimitiveRecipePlan(std::move(recipe));
	}

	bool CanMakeNativeTailRecipe(idx_t tail_start_idx) const {
		return SljitFullPipelineRecipeSequenceBuilder::CanMakeNativeTailRecipe(tail_start_idx);
	}

	SljitFullPipelineRecipe
	MakeProjectionFilterProjectionNativeTailRecipe(const SljitProjectionFilterProjectionNativeTailFacts &facts) const {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
		auto sequence = MakeSourceSequence();
		AddProjectionChainStep(sequence, facts.pre_projection_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, facts.projection_idx);
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionNativeTailRecipe(const SljitMarkFilterProjectionNativeTailFacts &facts) const {
		return ProjectionAggregateBinding().MakeMarkFilterProjectionNativeTailRecipe(facts);
	}

	SljitFullPipelineRecipe
	MakeSourceProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const {
		return ProjectionAggregateBinding().MakeSourceProjectionAggregateRecipe(shape);
	}

	SljitFullPipelineRecipe MakeSourceBatchNativeTailRecipe(const SljitSourceBatchNativeTailFacts &facts) const {
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(facts.boundary_op_idx));
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe MakeSourceUngroupedAggregateRecipe(const SljitSourceUngroupedAggregateFacts &facts) const {
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, facts.aggregate_idx);
		auto aggregate_update = SljitBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
		return MakePrimitiveSequence(std::move(sequence));
	}

	bool CanMakeSourceFilterAggregateRecipe(const SljitSourceFilterAggregateFacts &facts) const {
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

	SljitFullPipelineRecipe MakeSourceFilterAggregateRecipe(const SljitSourceFilterAggregateFacts &facts) const {
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, facts.aggregate_idx);
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

	SljitFullPipelineRecipe
	MakeGeneratedFilterProjectionNativeTailRecipe(const SljitGeneratedFilterProjectionNativeTailFacts &facts) const {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, facts.projection_idx);
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe MakeHashJoinDelimJoinSinkRecipe(idx_t first_hash_join_idx, idx_t final_hash_join_idx,
	                                                        idx_t sink_idx) const {
		if (first_hash_join_idx > final_hash_join_idx) {
			throw InternalException("SLJIT hash-join delimiter recipe has an invalid hash-join range");
		}
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, first_hash_join_idx);
		for (idx_t hash_join_idx = first_hash_join_idx; hash_join_idx < final_hash_join_idx; hash_join_idx++) {
			sequence.Add(MakeHashJoinProbeMaterializeStep(hash_join_idx));
		}
		sequence.Add(MakeHashJoinProbeSelectionStep(final_hash_join_idx));
		auto delim_sink = SljitBindSelectedHashJoinDelimJoinSinkPrimitive(ops, final_hash_join_idx, sink_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::DelimJoinSink(delim_sink));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakeJoinDirectProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                        const SljitProjectionAggregatePrefixFacts &facts) const {
		return ProjectionAggregateBinding().MakeJoinDirectProjectionAggregateRecipe(shape, facts);
	}

	SljitFullPipelineRecipe
	MakeJoinProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                      const SljitProjectionAggregatePrefixFacts &facts) const {
		return ProjectionAggregateBinding().MakeJoinProjectionAggregateTailRecipe(shape, facts);
	}

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                        const SljitProjectionAggregatePrefixFacts &facts) const {
		return ProjectionAggregateBinding().MakeMarkFilterProjectionAggregateRecipe(shape, facts);
	}

	SljitFullPipelineRecipe MakeMarkFilterNativeTailRecipe(const SljitProjectionAggregatePrefixFacts &facts) const {
		return ProjectionAggregateBinding().MakeMarkFilterNativeTailRecipe(facts);
	}

	bool ProjectionAggregateHasDedicatedBackend(const SljitFullPipelineProjectionAggregateShape &shape,
	                                            bool allow_direct_projected_primitive_payload_update = false) const {
		return ProjectionAggregateBinding().ProjectionAggregateHasDedicatedBackend(
		    shape, allow_direct_projected_primitive_payload_update);
	}

	bool CanMakeProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const {
		return ProjectionAggregateBinding().CanMakeProjectionAggregateTailRecipe(shape);
	}

private:
	SljitProjectionAggregateRecipeBinding ProjectionAggregateBinding() const {
		return SljitProjectionAggregateRecipeBinding(ops, source_output_types, source_min_values, source_max_values,
		                                             uses_extended_source_fetch_budget);
	}
};

} // namespace duckdb
