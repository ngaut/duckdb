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
    const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
    : ops(ops_p), source_output_types(source_output_types_p), source_min_values(source_min_values_p),
      source_max_values(source_max_values_p) {
}

SljitFullPipelineRecipe SljitFullPipelineRecipeSequenceBuilder::MakePrimitiveSequence(
    SljitFullPipelinePrimitiveSequence sequence,
    SljitHashJoinDirectAggregateConsumerContract direct_aggregate_consumer) const {
	return SljitFinalizeFullPipelinePrimitiveRecipe(ops, std::move(sequence), direct_aggregate_consumer);
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
	// The tail op's declared input references are the single authority for what
	// its execution dereferences; the derived view must satisfy them.
	return ops[tail_start_idx].DeclaredInputConstraints().SatisfiedBy(*view_types);
}

bool SljitFullPipelineRecipeSequenceBuilder::TryMakeGeneratedOperatorStep(idx_t op_idx,
                                                                          SljitFullPipelinePrimitiveStep &step) const {
	switch (ops[op_idx].Kind()) {
	case SljitNativeRegionOpKind::FILTER: {
		SljitGeneratedFilterPrimitive filter;
		if (!SljitTryBindGeneratedFilterPrimitive(ops, op_idx, filter)) {
			return false;
		}
		step = SljitFullPipelinePrimitiveStep::GeneratedFilter(filter);
		return true;
	}
	case SljitNativeRegionOpKind::PROJECTION: {
		SljitProjectionChainPrimitive projection;
		if (!SljitTryBindProjectionChainPrimitive(ops, op_idx, projection)) {
			return false;
		}
		step = SljitFullPipelinePrimitiveStep::ProjectionChain(projection);
		return true;
	}
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE:
		return TryMakeHashJoinProbeMaterializeStep(op_idx, step);
	default:
		return false;
	}
}

bool SljitFullPipelineRecipeSequenceBuilder::TryMakeGeneratedSinkStep(idx_t op_idx,
                                                                      SljitFullPipelinePrimitiveStep &step) const {
	auto &op = ops[op_idx];
	if (op.Kind() == SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
		SljitHashJoinBuildSinkPrimitive sink;
		if (!SljitTryBindHashJoinBuildSinkPrimitive(ops, op_idx, DConstants::INVALID_INDEX, sink)) {
			return false;
		}
		step = SljitFullPipelinePrimitiveStep::HashJoinBuildSink(sink);
		return true;
	}
	if (op.Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE || !op.AggregateUpdate().plan.UsesPrimitivePayloads()) {
		return false;
	}
	SljitUngroupedAggregateUpdatePrimitive ungrouped;
	if (SljitTryBindUngroupedAggregateUpdatePrimitive(ops, op_idx, ungrouped)) {
		step = SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(ungrouped);
		return true;
	}
	SljitGroupedAggregateUpdatePrimitive grouped;
	if (SljitTryBindGroupedAggregateUpdatePrimitive(ops, op_idx, grouped)) {
		step = SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped);
		return true;
	}
	step = SljitFullPipelinePrimitiveStep::PrimitiveAggregateUpdate(op_idx);
	return true;
}

//! A filtered sink evaluates its filter over every row of the chunk it gets: the step before it must hand over whole
//! chunks rather than a selection of their rows
bool SljitFullPipelineRecipeSequenceBuilder::StepHandsOverWholeChunks(const SljitFullPipelinePrimitiveStep &step) {
	switch (step.kind) {
	case SljitFullPipelinePrimitiveKind::GENERATED_FILTER:
	case SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION:
	case SljitFullPipelinePrimitiveKind::MARK_PROBE_FILTER_BOUNDARY:
		return false;
	default:
		return true;
	}
}

bool SljitFullPipelineRecipeSequenceBuilder::TryMakeFilteredSinkStep(idx_t filter_idx,
                                                                     SljitFullPipelinePrimitiveStep &step) const {
	const auto aggregate_idx = filter_idx + 1;
	if (aggregate_idx >= ops.size() || ops[filter_idx].Kind() != SljitNativeRegionOpKind::FILTER ||
	    ops[aggregate_idx].Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    !ops[aggregate_idx].AggregateUpdate().plan.UsesPrimitivePayloads()) {
		return false;
	}
	SljitUngroupedAggregateUpdatePrimitive ungrouped;
	if (SljitTryBindFilteredUngroupedAggregateUpdatePrimitive(ops, filter_idx, aggregate_idx, ungrouped)) {
		step = SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(ungrouped);
		return true;
	}
	SljitGroupedAggregateUpdatePrimitive grouped;
	if (SljitTryBindFilteredGroupedAggregateUpdatePrimitive(ops, filter_idx, aggregate_idx, grouped)) {
		step = SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped);
		return true;
	}
	return false;
}

//! Generated steps carry the rows from `tail_start_idx` on as far as they bind, into the sink when generated code
//! updates it; otherwise DuckDB's operators take them over at the furthest op they can take them at
bool SljitFullPipelineRecipeSequenceBuilder::TryMakeNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
                                                                     idx_t tail_start_idx,
                                                                     SljitFullPipelineRecipe &recipe) const {
	SljitFullPipelinePrimitiveSequence handed_over;
	auto hand_over_idx = DConstants::INVALID_INDEX;
	for (auto op_idx = tail_start_idx; op_idx < ops.size(); op_idx++) {
		SljitFullPipelinePrimitiveStep sink_step;
		if ((op_idx + 1 == ops.size() && TryMakeGeneratedSinkStep(op_idx, sink_step)) ||
		    (op_idx + 2 == ops.size() && StepHandsOverWholeChunks(sequence.Step(sequence.Count() - 1)) &&
		     TryMakeFilteredSinkStep(op_idx, sink_step))) {
			sequence.Add(std::move(sink_step));
			recipe = MakePrimitiveSequence(std::move(sequence));
			return true;
		}
		if (SljitNativeTailCanConsumeTail(ops, op_idx) && NativeTailInputLayoutMatches(sequence, op_idx)) {
			handed_over = sequence;
			hand_over_idx = op_idx;
		}
		SljitFullPipelinePrimitiveStep step;
		if (sequence.Count() + 1 >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES || !TryMakeGeneratedOperatorStep(op_idx, step)) {
			break;
		}
		sequence.Add(std::move(step));
	}
	if (hand_over_idx == DConstants::INVALID_INDEX) {
		return false;
	}
	handed_over.Add(SljitFullPipelinePrimitiveStep::NativeTailDelegation(hand_over_idx));
	recipe = MakePrimitiveSequence(std::move(handed_over));
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
