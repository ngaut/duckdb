//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_pre_join_projection_recipe.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_pre_join_projection_recipe.hpp"

#include <utility>

namespace duckdb {

SljitPreJoinProjectionRecipeBinding::SljitPreJoinProjectionRecipeBinding(
    const vector<SljitExecutableRegionOp> &ops_p, const vector<LogicalType> &source_output_types_p,
    const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p) {
}

SljitFullPipelinePrimitiveSequence
SljitPreJoinProjectionRecipeBinding::MakeHashJoinSelectionSequence(idx_t pre_join_projection_idx,
                                                                   idx_t hash_join_idx) const {
	auto sequence = MakeSourceSequence();
	if (TryAppendElidedHashJoinProbeSelection(sequence, pre_join_projection_idx, hash_join_idx)) {
		return sequence;
	}
	AddMaterializedHashJoinSelection(sequence, pre_join_projection_idx, hash_join_idx);
	return sequence;
}

bool SljitPreJoinProjectionRecipeBinding::TryAppendElidedHashJoinProbeSelection(
    SljitFullPipelinePrimitiveSequence &sequence, idx_t pre_join_projection_idx, idx_t hash_join_idx,
    bool preserve_source_ordinals) const {
	SljitHashJoinProbeSelectionPrimitive primitive;
	if (!SljitTryBindElidedHashJoinProbeSelection(ops, pre_join_projection_idx, hash_join_idx, source_min_values,
	                                              source_max_values, primitive) ||
	    (preserve_source_ordinals && !primitive.output_layout->PreservesSourceColumnOrdinals())) {
		return false;
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(primitive));
	return true;
}

void SljitPreJoinProjectionRecipeBinding::AddMaterializedHashJoinSelection(SljitFullPipelinePrimitiveSequence &sequence,
                                                                           idx_t pre_join_projection_idx,
                                                                           idx_t hash_join_idx) const {
	AddProjectionChainStep(sequence, pre_join_projection_idx);
	sequence.Add(MakeHashJoinProbeSelectionStep(hash_join_idx));
}

} // namespace duckdb
