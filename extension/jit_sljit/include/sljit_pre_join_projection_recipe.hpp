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
	                                    bool uses_extended_source_fetch_budget_p)
	    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
	                                             uses_extended_source_fetch_budget_p) {
	}

	bool TryBuildView(idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                  SljitPreJoinProjectionViewDescriptor &pre_join_view) const {
		return SljitTryBuildPreJoinProjectionViewDescriptor(ops, pre_join_projection_idx, hash_join_idx,
		                                                    source_min_values, source_max_values, pre_join_view);
	}

	SljitFullPipelinePrimitiveSequence
	MakeHashJoinSelectionSequence(idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                              bool add_source_batch_boundary_for_elided,
	                              optional_ptr<const SljitPreJoinProjectionViewDescriptor> pre_join_view = nullptr)
	    const {
		auto sequence = MakeSourceSequence();
		if (pre_join_view && TryAppendElidedHashJoinProbeSelection(
		                         sequence, *pre_join_view, pre_join_projection_idx, hash_join_idx,
		                         add_source_batch_boundary_for_elided)) {
			return sequence;
		}
		if (!pre_join_view && TryAppendElidedHashJoinProbeSelection(
		                          sequence, pre_join_projection_idx, hash_join_idx,
		                          add_source_batch_boundary_for_elided)) {
			return sequence;
		}
		AddMaterializedHashJoinSelection(sequence, pre_join_projection_idx, hash_join_idx);
		return sequence;
	}

	bool TryAppendElidedHashJoinProbeSelection(SljitFullPipelinePrimitiveSequence &sequence,
	                                           idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                                           bool add_source_batch_boundary) const {
		SljitPreJoinProjectionViewDescriptor pre_join_view;
		if (!TryBuildView(pre_join_projection_idx, hash_join_idx, pre_join_view)) {
			return false;
		}
		return TryAppendElidedHashJoinProbeSelection(sequence, pre_join_view, pre_join_projection_idx, hash_join_idx,
		                                             add_source_batch_boundary);
	}

	bool TryAppendElidedHashJoinProbeSelection(SljitFullPipelinePrimitiveSequence &sequence,
	                                           const SljitPreJoinProjectionViewDescriptor &pre_join_view,
	                                           idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                                           bool add_source_batch_boundary) const {
		if (!pre_join_view.CanElideProjectionWithCurrentHashProbe()) {
			return false;
		}
		if (add_source_batch_boundary) {
			AddSourceBatchBoundaryIfUseful(sequence, hash_join_idx);
		}
		auto remapped_hash_join_selection =
		    BindElidedHashJoinProbeSelection(pre_join_view, pre_join_projection_idx, hash_join_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(remapped_hash_join_selection));
		return true;
	}

private:
	void AddMaterializedHashJoinSelection(SljitFullPipelinePrimitiveSequence &sequence, idx_t pre_join_projection_idx,
	                                      idx_t hash_join_idx) const {
		AddProjectionChainStep(sequence, pre_join_projection_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(hash_join_idx));
	}

	SljitHashJoinProbeSelectionPrimitive
	BindElidedHashJoinProbeSelection(const SljitPreJoinProjectionViewDescriptor &pre_join_view,
	                                 idx_t pre_join_projection_idx, idx_t hash_join_idx) const {
		if (!pre_join_view.CanElideProjectionWithCurrentHashProbe() ||
		    pre_join_view.projection_idx != pre_join_projection_idx || pre_join_view.hash_join_idx != hash_join_idx) {
			throw InternalException("SLJIT pre-join projection elision cannot bind inconsistent descriptor");
		}
		SljitHashJoinProbeInputRemap input_remap;
		if (!pre_join_view.hash_probe_key_inputs_match_source) {
			input_remap.key_input_indices = pre_join_view.hash_probe_key_source_indices;
		}
		input_remap.residual_probe_source_indices = pre_join_view.residual_probe_source_indices;
		return SljitBindHashJoinProbeSelectionPrimitive(
		    ops, hash_join_idx, pre_join_view.source_key0_int64_to_int32_unchecked, std::move(input_remap),
		    pre_join_view.projected_to_source, pre_join_projection_idx,
		    optional_ptr<const vector<LogicalType>>(&ops[pre_join_projection_idx].input_types));
	}
};

} // namespace duckdb
