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

enum class SljitProjectionGroupedAggregateRecipeMode : uint8_t { INVALID, PROJECTED_INPUT, MATERIALIZED_PROJECTION };

class SljitProjectionAggregateRecipeBinding : private SljitFullPipelineRecipeSequenceBuilder {
public:
	SljitProjectionAggregateRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
	                                      const vector<LogicalType> &source_output_types_p,
	                                      const vector<Value> &source_min_values_p,
	                                      const vector<Value> &source_max_values_p,
	                                      bool uses_extended_source_fetch_budget_p);

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionNativeTailRecipe(const SljitMarkFilterProjectionNativeTailFacts &facts) const;

	SljitFullPipelineRecipe
	MakeSourceProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const;

	SljitFullPipelineRecipe
	MakeJoinDirectProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                        const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelineRecipe
	MakeJoinProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                      const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                        const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelineRecipe MakeMarkFilterNativeTailRecipe(const SljitProjectionAggregatePrefixFacts &facts) const;

	bool ProjectionAggregateHasDedicatedBackend(const SljitFullPipelineProjectionAggregateShape &shape,
	                                            bool allow_direct_projected_primitive_payload_update = false) const;

	bool CanMakeProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const;

private:
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

	SljitPostJoinProjectionAggregatePrimitive
	BindPostJoinProjectionAggregatePrimitive(const SljitFullPipelineProjectionAggregateShape &shape,
	                                         idx_t hash_join_idx) const;

	SljitProjectionGroupedAggregateRecipeMode
	ChooseProjectionGroupedAggregateRecipeMode(const SljitFullPipelineProjectionAggregateShape &shape,
	                                           bool allow_direct_projected_primitive_payload_update = false) const;

	SljitPreJoinProjectionRecipeBinding PreJoinProjectionBinding() const;

	SljitFullPipelineRecipe
	MakeProjectionAggregateRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                              const SljitFullPipelineProjectionAggregateShape &shape,
	                              bool allow_direct_projected_primitive_payload_update = false) const;

	SljitFullPipelineRecipe
	MakeProjectionGroupedAggregateRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                     const SljitFullPipelineProjectionAggregateShape &shape,
	                                     bool allow_direct_projected_primitive_payload_update = false) const;

	SljitFullPipelineRecipe
	MakeProjectionAggregateTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                  const SljitFullPipelineProjectionAggregateShape &shape) const;

	SljitFullPipelineRecipe
	MakeProjectionNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                               const SljitFullPipelineProjectionAggregateShape &shape) const;

	SljitFullPipelineRecipe MakeProjectionNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                                       idx_t first_projection_idx, idx_t final_projection_idx,
	                                                       idx_t tail_start_idx) const;
};

} // namespace duckdb
