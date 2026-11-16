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
	                                       const vector<Value> &source_max_values_p);

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

	//! The generated step that runs op `op_idx` on its own
	//! The generated step computing ops [op_idx, next_op_idx)
	bool TryMakeGeneratedOperatorStep(idx_t op_idx, SljitFullPipelinePrimitiveStep &step, idx_t &next_op_idx) const;
	bool NativeTailInputLayoutMatches(const SljitFullPipelinePrimitiveSequence &sequence, idx_t tail_start_idx) const;
	//! The generated step that updates sink op `op_idx`
	bool TryMakeGeneratedSinkStep(idx_t op_idx, SljitFullPipelinePrimitiveStep &step) const;
	//! The aggregate sink after filter `filter_idx`, evaluating the filter in the pass that updates the states
	bool TryMakeFilteredSinkStep(idx_t filter_idx, SljitFullPipelinePrimitiveStep &step) const;
	static bool StepHandsOverWholeChunks(const SljitFullPipelinePrimitiveStep &step);
	bool TryMakeSelectedBuildSinkSteps(idx_t probe_idx, SljitFullPipelinePrimitiveStep &probe_step,
	                                   SljitFullPipelinePrimitiveStep &sink_step) const;
	bool TryMakeNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence, idx_t tail_start_idx,
	                             SljitFullPipelineRecipe &recipe) const;

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeMaterializeStep(idx_t hash_join_idx) const;
	bool TryMakeHashJoinProbeMaterializeStep(idx_t hash_join_idx, SljitFullPipelinePrimitiveStep &step) const;

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeSelectionStep(idx_t hash_join_idx) const;
	bool TryMakeHashJoinProbeSelectionStep(idx_t hash_join_idx, SljitFullPipelinePrimitiveStep &step) const;

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeProjectionInputStep(idx_t hash_join_idx) const;

protected:
	const vector<SljitExecutableRegionOp> &ops;
	const vector<LogicalType> &source_output_types;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
};

} // namespace duckdb
