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
	return SljitFinalizeFullPipelinePrimitiveRecipe(ops, uses_extended_source_fetch_budget, std::move(sequence),
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

//! The tail consumes the recipe's materialized view by bound column index, so
//! the view's layout must match the tail operator's declared input exactly. A
//! mismatch that reaches runtime references the wrong columns — loud when the
//! types differ, silent when they coincide — so the recipe is rejected here.
//! Steps whose view this walk cannot derive yet (selection views, remaps, mark
//! boundaries) skip validation: enforcement applies only where the layout is
//! exactly provable, and unproven shapes keep their existing behavior.
bool SljitFullPipelineRecipeSequenceBuilder::NativeTailInputLayoutMatches(
    const SljitFullPipelinePrimitiveSequence &sequence, idx_t tail_start_idx) const {
	const vector<LogicalType> *view_types = &source_output_types;
	for (idx_t step_idx = 1; step_idx < sequence.Count(); step_idx++) {
		auto &step = sequence.Step(step_idx);
		switch (step.kind) {
		case SljitFullPipelinePrimitiveKind::GENERATED_FILTER:
			// Selection-only: the view's column layout is unchanged.
			break;
		case SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN: {
			auto &projection = step.projection_chain;
			if (projection.HasBoundComposedProjection()) {
				view_types = &projection.bound_composed_projection->output_types;
				break;
			}
			if (projection.final_projection_idx >= ops.size()) {
				return false;
			}
			view_types = &ops[projection.final_projection_idx].output_types;
			break;
		}
		default:
			// View not derivable: skip validation for this recipe.
			return true;
		}
	}
	// Sink-kind ops declare their input inside the sink plan, not the generic
	// field; an empty declaration means the layout is not provable here.
	auto &tail = ops[tail_start_idx];
	const vector<LogicalType> *expected = &tail.input_types;
	if (expected->empty()) {
		switch (tail.kind) {
		case SljitNativeRegionOpKind::AGGREGATE_UPDATE:
			expected = &tail.aggregate_update.plan.input_types;
			break;
		case SljitNativeRegionOpKind::APPEND_SINK:
			expected = &tail.append_sink.plan.input_types;
			break;
		case SljitNativeRegionOpKind::ORDER_SINK:
			expected = &tail.order_sink.plan.input_types;
			break;
		case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
			expected = &tail.delim_join_sink.plan.input_types;
			break;
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
			expected = &tail.hash_join_build.plan.input_types;
			break;
		default:
			break;
		}
	}
	if (expected->empty()) {
		return true;
	}
	if (view_types->size() != expected->size()) {
		return false;
	}
	for (idx_t col_idx = 0; col_idx < expected->size(); col_idx++) {
		if ((*view_types)[col_idx] != (*expected)[col_idx]) {
			return false;
		}
	}
	return true;
}

bool SljitFullPipelineRecipeSequenceBuilder::TryMakeNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
                                                                     idx_t tail_start_idx,
                                                                     SljitFullPipelineRecipe &recipe) const {
	if (tail_start_idx >= ops.size() || !SljitNativeTailCanConsumeTail(ops, tail_start_idx)) {
		return false;
	}
	if (!NativeTailInputLayoutMatches(sequence, tail_start_idx)) {
		return false;
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::NativeTailDelegation(tail_start_idx));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelineRecipeSequenceBuilder::MakeHashJoinProbeMaterializeStep(idx_t hash_join_idx) const {
	SljitFullPipelinePrimitiveStep step;
	if (!TryMakeHashJoinProbeMaterializeStep(hash_join_idx, step)) {
		throw InternalException("SLJIT hash join materialize primitive cannot bind requested operator");
	}
	return step;
}

bool SljitFullPipelineRecipeSequenceBuilder::TryMakeHashJoinProbeMaterializeStep(
    idx_t hash_join_idx, SljitFullPipelinePrimitiveStep &step) const {
	const auto source_key0_int64_to_int32_unchecked =
	    hash_join_idx == 0 &&
	    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
	SljitHashJoinProbeMaterializePrimitive primitive;
	if (!SljitTryBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx, primitive,
	                                                   source_key0_int64_to_int32_unchecked)) {
		return false;
	}
	step = SljitFullPipelinePrimitiveStep::HashJoinProbeMaterialize(primitive);
	return true;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelineRecipeSequenceBuilder::MakeHashJoinProbeSelectionStep(idx_t hash_join_idx) const {
	SljitFullPipelinePrimitiveStep step;
	if (!TryMakeHashJoinProbeSelectionStep(hash_join_idx, step)) {
		throw InternalException("SLJIT hash join selection primitive cannot bind requested operator");
	}
	return step;
}

bool SljitFullPipelineRecipeSequenceBuilder::TryMakeHashJoinProbeSelectionStep(
    idx_t hash_join_idx, SljitFullPipelinePrimitiveStep &step) const {
	const auto source_key0_int64_to_int32_unchecked =
	    hash_join_idx == 0 &&
	    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
	SljitHashJoinProbeSelectionPrimitive primitive;
	if (!SljitTryBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx, primitive,
	                                                 source_key0_int64_to_int32_unchecked)) {
		return false;
	}
	step = SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(primitive);
	return true;
}

SljitFullPipelinePrimitiveStep
SljitFullPipelineRecipeSequenceBuilder::MakeHashJoinProbeProjectionInputStep(idx_t hash_join_idx) const {
	if (SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
		return MakeHashJoinProbeSelectionStep(hash_join_idx);
	}
	return MakeHashJoinProbeMaterializeStep(hash_join_idx);
}

} // namespace duckdb
