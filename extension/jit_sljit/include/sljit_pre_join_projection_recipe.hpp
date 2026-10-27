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
	                                    const vector<Value> &source_max_values_p);

	SljitFullPipelinePrimitiveSequence MakeHashJoinSelectionSequence(idx_t pre_join_projection_idx,
	                                                                 idx_t hash_join_idx) const;

	bool TryAppendElidedHashJoinProbeSelection(SljitFullPipelinePrimitiveSequence &sequence,
	                                           idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                                           bool preserve_source_ordinals = false) const;

private:
	void AddMaterializedHashJoinSelection(SljitFullPipelinePrimitiveSequence &sequence, idx_t pre_join_projection_idx,
	                                      idx_t hash_join_idx) const;
};

} // namespace duckdb
