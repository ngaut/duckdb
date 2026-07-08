//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_pre_join_projection_recipe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_full_pipeline_recipe_sequence_builder.hpp"
#include "sljit_pre_join_projection_descriptor.hpp"

namespace duckdb {

class SljitPreJoinProjectionRecipeBinding : private SljitFullPipelineRecipeSequenceBuilder {
public:
	SljitPreJoinProjectionRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
	                                    const vector<LogicalType> &source_output_types_p,
	                                    const vector<Value> &source_min_values_p,
	                                    const vector<Value> &source_max_values_p,
	                                    bool uses_extended_source_fetch_budget_p);

	bool TryBuildView(idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                  SljitPreJoinProjectionViewDescriptor &pre_join_view) const;

	SljitFullPipelinePrimitiveSequence MakeHashJoinSelectionSequence(
	    idx_t pre_join_projection_idx, idx_t hash_join_idx,
	    optional_ptr<const SljitPreJoinProjectionViewDescriptor> pre_join_view = nullptr) const;

	bool TryAppendElidedHashJoinProbeSelection(SljitFullPipelinePrimitiveSequence &sequence,
	                                           idx_t pre_join_projection_idx, idx_t hash_join_idx) const;

	bool TryAppendElidedHashJoinProbeSelection(SljitFullPipelinePrimitiveSequence &sequence,
	                                           const SljitPreJoinProjectionViewDescriptor &pre_join_view,
	                                           idx_t pre_join_projection_idx, idx_t hash_join_idx) const;

private:
	void AddMaterializedHashJoinSelection(SljitFullPipelinePrimitiveSequence &sequence, idx_t pre_join_projection_idx,
	                                      idx_t hash_join_idx) const;

	SljitHashJoinProbeSelectionPrimitive
	BindElidedHashJoinProbeSelection(const SljitPreJoinProjectionViewDescriptor &pre_join_view,
	                                 idx_t pre_join_projection_idx, idx_t hash_join_idx) const;
};

} // namespace duckdb
