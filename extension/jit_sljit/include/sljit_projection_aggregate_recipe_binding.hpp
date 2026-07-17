//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_recipe_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

#include "sljit_full_pipeline_recipe_facts.hpp"
#include "sljit_full_pipeline_recipe_sequence_builder.hpp"

namespace duckdb {

class SljitPreJoinProjectionRecipeBinding;
struct SljitPostJoinProjectionAggregatePrimitive;

class SljitProjectionAggregateRecipeBinding : private SljitFullPipelineRecipeSequenceBuilder {
public:
	SljitProjectionAggregateRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
	                                      const vector<LogicalType> &source_output_types_p,
	                                      const vector<Value> &source_min_values_p,
	                                      const vector<Value> &source_max_values_p,
	                                      bool uses_extended_source_fetch_budget_p);

	bool TryMakeMarkFilterProjectionNativeTailRecipe(const SljitMarkFilterProjectionNativeTailFacts &facts,
	                                                 SljitFullPipelineRecipe &recipe) const;

	bool TryMakeSourceProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                SljitFullPipelineRecipe &recipe) const;

	bool TryMakeJoinDirectProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                const SljitProjectionAggregatePrefixFacts &facts,
	                                                SljitFullPipelineRecipe &recipe) const;

	bool TryMakeJoinProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                              const SljitProjectionAggregatePrefixFacts &facts,
	                                              SljitFullPipelineRecipe &recipe) const;

	bool TryMakeMarkFilterProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                const SljitProjectionAggregatePrefixFacts &facts,
	                                                SljitFullPipelineRecipe &recipe) const;

	bool TryMakeMarkFilterNativeTailRecipe(const SljitProjectionAggregatePrefixFacts &facts,
	                                       SljitFullPipelineRecipe &recipe) const;

private:
	bool CanBindMarkFilterBoundary(const SljitProjectionAggregatePrefixFacts &facts) const;

	bool CanBindDirectProjectionAggregate(const SljitFullPipelineProjectionAggregateShape &shape,
	                                      const SljitProjectionAggregatePrefixFacts &facts) const;

	bool CanBindProjectionAggregateTail(const SljitFullPipelineProjectionAggregateShape &shape,
	                                    const SljitProjectionAggregatePrefixFacts &facts) const;

	bool CanBindProjectionAggregateTailPrefix(const SljitProjectionAggregatePrefixFacts &facts) const;

	bool CanBindHashJoinProbeProjectionInput(idx_t hash_join_idx) const;

	bool CanBindJoinPrefixInputs(const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelinePrimitiveSequence
	MakeSingleJoinDirectPrefixSequence(const SljitProjectionAggregatePrefixFacts &facts,
	                                   SljitPostJoinProjectionAggregatePrimitive &post_join_aggregate) const;

	SljitFullPipelinePrimitiveSequence
	MakeSingleJoinProjectionAggregateTailPrefixSequence(const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelinePrimitiveSequence
	MakeJoinPrefixSelectionSequence(const SljitProjectionAggregatePrefixFacts &facts) const;

	void AddProjectionAggregateSourcePrefix(SljitFullPipelinePrimitiveSequence &sequence,
	                                        const SljitProjectionAggregatePrefixFacts &facts) const;

	void AddJoinInputProjection(SljitFullPipelinePrimitiveSequence &sequence,
	                            const SljitProjectionAggregatePrefixFacts &facts, idx_t join_ordinal) const;

	void AddProjectionAggregateFirstJoinPrefix(SljitFullPipelinePrimitiveSequence &sequence,
	                                           const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelinePrimitiveStep
	MakeMarkProbeFilterBoundaryStep(idx_t hash_join_idx, idx_t filter_idx, bool apply_filter_selection,
	                                idx_t downstream_projection_idx = DConstants::INVALID_INDEX,
	                                bool materialize_filter_selection = false) const;
	bool TryMakeMarkProbeFilterBoundaryStep(idx_t hash_join_idx, idx_t filter_idx, bool apply_filter_selection,
	                                        idx_t downstream_projection_idx, bool materialize_filter_selection,
	                                        SljitFullPipelinePrimitiveStep &step) const;

	SljitFullPipelinePrimitiveSequence MakeMarkFilterPrefix(idx_t hash_join_idx, idx_t filter_idx,
	                                                        bool apply_filter_selection,
	                                                        idx_t downstream_projection_idx = DConstants::INVALID_INDEX,
	                                                        bool materialize_filter_selection = false) const;

	SljitFullPipelinePrimitiveSequence
	MakeProjectionAggregatePreMarkJoinPrefix(const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelinePrimitiveSequence
	MakeProjectionAggregateMarkFilterPrefix(const SljitProjectionAggregatePrefixFacts &facts,
	                                        bool materialize_filter_selection,
	                                        idx_t downstream_projection_idx = DConstants::INVALID_INDEX) const;

	SljitFullPipelinePrimitiveSequence MakeSourceHashJoinProjectionInputSequence(idx_t hash_join_idx) const;

	bool TryBindPostJoinProjectionAggregatePrimitive(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                 idx_t hash_join_idx,
	                                                 SljitPostJoinProjectionAggregatePrimitive &primitive) const;

	SljitPreJoinProjectionRecipeBinding PreJoinProjectionBinding() const;

	bool TryMakeProjectionAggregateRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                      const SljitFullPipelineProjectionAggregateShape &shape,
	                                      bool allow_direct_projected_primitive_payload_update,
	                                      SljitFullPipelineRecipe &recipe) const;

	bool TryMakeProjectionAggregateTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                          const SljitFullPipelineProjectionAggregateShape &shape,
	                                          SljitFullPipelineRecipe &recipe) const;

	bool TryMakeProjectionNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                       const SljitFullPipelineProjectionAggregateShape &shape,
	                                       SljitFullPipelineRecipe &recipe) const;

	bool TryMakeProjectionNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence, idx_t first_projection_idx,
	                                       idx_t final_projection_idx, idx_t tail_start_idx,
	                                       SljitFullPipelineRecipe &recipe) const;
};

} // namespace duckdb
