//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_sequence_builder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

#include "sljit_full_pipeline_recipe_state.hpp"

namespace duckdb {

class SljitFullPipelineRecipeSequenceBuilder {
public:
	SljitFullPipelineRecipeSequenceBuilder(const vector<SljitExecutableRegionOp> &ops_p,
	                                       const vector<LogicalType> &source_output_types_p,
	                                       const vector<Value> &source_min_values_p,
	                                       const vector<Value> &source_max_values_p,
	                                       bool uses_extended_source_fetch_budget_p);

protected:
	SljitFullPipelineRecipe
	MakePrimitiveSequence(SljitFullPipelinePrimitiveSequence sequence,
	                      SljitHashJoinDirectAggregateConsumerContract direct_aggregate_consumer = {}) const;

	SljitFullPipelinePrimitiveSequence MakeSourceSequence() const;

	SljitFullPipelinePrimitiveStep MakeProjectionChainStep(idx_t projection_idx) const;

	SljitFullPipelinePrimitiveStep MakeProjectionChainStep(idx_t first_projection_idx,
	                                                       idx_t final_projection_idx) const;

	void AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence, idx_t projection_idx) const;

	void AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence, idx_t first_projection_idx,
	                            idx_t final_projection_idx) const;

	bool TryMakeNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence, idx_t tail_start_idx,
	                             SljitFullPipelineRecipe &recipe) const;

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeMaterializeStep(idx_t hash_join_idx) const;

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeSelectionStep(idx_t hash_join_idx) const;

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeProjectionInputStep(idx_t hash_join_idx) const;

protected:
	const vector<SljitExecutableRegionOp> &ops;
	const vector<LogicalType> &source_output_types;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	bool uses_extended_source_fetch_budget;
};

} // namespace duckdb
