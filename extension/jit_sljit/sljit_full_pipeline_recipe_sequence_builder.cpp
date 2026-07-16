//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_sequence_builder.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_recipe_sequence_builder.hpp"

#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"

#include <utility>

namespace duckdb {

SljitFullPipelineRecipeSequenceBuilder::SljitFullPipelineRecipeSequenceBuilder(
    const vector<SljitExecutableRegionOp> &ops_p, const vector<LogicalType> &source_output_types_p,
    const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
    bool uses_extended_source_fetch_budget_p)
    : ops(ops_p), source_output_types(source_output_types_p), source_min_values(source_min_values_p),
      source_max_values(source_max_values_p), uses_extended_source_fetch_budget(uses_extended_source_fetch_budget_p) {
}

SljitFullPipelineRecipe SljitFullPipelineRecipeSequenceBuilder::MakePrimitiveSequence(
    SljitFullPipelinePrimitiveSequence sequence,
    SljitHashJoinDirectAggregateConsumerContract direct_aggregate_consumer) const {
	return SljitMakeFullPipelinePrimitiveRecipe(uses_extended_source_fetch_budget, std::move(sequence),
	                                            direct_aggregate_consumer);
}

SljitFullPipelinePrimitiveSequence SljitFullPipelineRecipeSequenceBuilder::MakeSourceSequence() const {
	SljitFullPipelinePrimitiveSequence sequence;
	sequence.Add(SljitFullPipelinePrimitiveStep::SourceFetch());
	return sequence;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelineRecipeSequenceBuilder::MakeProjectionChainStep(idx_t projection_idx) const {
	auto projection = SljitBindProjectionChainPrimitive(ops, projection_idx);
	return SljitFullPipelinePrimitiveStep::ProjectionChain(projection);
}

SljitFullPipelinePrimitiveStep
SljitFullPipelineRecipeSequenceBuilder::MakeProjectionChainStep(idx_t first_projection_idx,
                                                                idx_t final_projection_idx) const {
	auto projection = SljitBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx);
	return SljitFullPipelinePrimitiveStep::ProjectionChain(projection);
}

void SljitFullPipelineRecipeSequenceBuilder::AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence,
                                                                    idx_t projection_idx) const {
	sequence.Add(MakeProjectionChainStep(projection_idx));
}

void SljitFullPipelineRecipeSequenceBuilder::AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence,
                                                                    idx_t first_projection_idx,
                                                                    idx_t final_projection_idx) const {
	sequence.Add(MakeProjectionChainStep(first_projection_idx, final_projection_idx));
}

bool SljitFullPipelineRecipeSequenceBuilder::TryMakeNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
                                                                     idx_t tail_start_idx,
                                                                     SljitFullPipelineRecipe &recipe) const {
	if (tail_start_idx >= ops.size() || !SljitNativeTailCanConsumeTail(ops, tail_start_idx)) {
		return false;
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::NativeTailDelegation(tail_start_idx));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelineRecipeSequenceBuilder::MakeHashJoinProbeMaterializeStep(idx_t hash_join_idx) const {
	const auto source_key0_int64_to_int32_unchecked =
	    hash_join_idx == 0 &&
	    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
	auto primitive =
	    SljitBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx, source_key0_int64_to_int32_unchecked);
	return SljitFullPipelinePrimitiveStep::HashJoinProbeMaterialize(primitive);
}

SljitFullPipelinePrimitiveStep
SljitFullPipelineRecipeSequenceBuilder::MakeHashJoinProbeSelectionStep(idx_t hash_join_idx) const {
	const auto source_key0_int64_to_int32_unchecked =
	    hash_join_idx == 0 &&
	    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
	auto primitive = SljitBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx, source_key0_int64_to_int32_unchecked);
	return SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(primitive);
}

SljitFullPipelinePrimitiveStep
SljitFullPipelineRecipeSequenceBuilder::MakeHashJoinProbeProjectionInputStep(idx_t hash_join_idx) const {
	if (SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		return MakeHashJoinProbeSelectionStep(hash_join_idx);
	}
	return MakeHashJoinProbeMaterializeStep(hash_join_idx);
}

} // namespace duckdb
