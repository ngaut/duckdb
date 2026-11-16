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
	                                      const vector<Value> &source_max_values_p);

	bool TryMakeJoinDirectProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                const SljitProjectionAggregatePrefixFacts &facts,
	                                                SljitFullPipelineRecipe &recipe) const;

private:
	bool CanBindDirectProjectionAggregate(const SljitFullPipelineProjectionAggregateShape &shape,
	                                      const SljitProjectionAggregatePrefixFacts &facts) const;

	bool CanBindHashJoinProbeProjectionInput(idx_t hash_join_idx) const;

	bool CanBindJoinPrefixInputs(const SljitProjectionAggregatePrefixFacts &facts) const;

	SljitFullPipelinePrimitiveSequence
	MakeSingleJoinDirectPrefixSequence(const SljitProjectionAggregatePrefixFacts &facts,
	                                   SljitPostJoinProjectionAggregatePrimitive &post_join_aggregate) const;

	SljitFullPipelinePrimitiveSequence
	MakeJoinPrefixSelectionSequence(const SljitProjectionAggregatePrefixFacts &facts) const;

	void AddProjectionAggregateSourcePrefix(SljitFullPipelinePrimitiveSequence &sequence,
	                                        const SljitProjectionAggregatePrefixFacts &facts) const;

	void AddJoinInputProjection(SljitFullPipelinePrimitiveSequence &sequence,
	                            const SljitProjectionAggregatePrefixFacts &facts, idx_t join_ordinal) const;

	bool TryBindPostJoinProjectionAggregatePrimitive(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                 idx_t hash_join_idx,
	                                                 SljitPostJoinProjectionAggregatePrimitive &primitive) const;

	SljitPreJoinProjectionRecipeBinding PreJoinProjectionBinding() const;

	bool TryMakeProjectionNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                       const SljitFullPipelineProjectionAggregateShape &shape,
	                                       SljitFullPipelineRecipe &recipe) const;

	bool TryMakeProjectionNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence, idx_t first_projection_idx,
	                                       idx_t final_projection_idx, idx_t tail_start_idx,
	                                       SljitFullPipelineRecipe &recipe) const;
};

} // namespace duckdb
