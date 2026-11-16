//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_sequence_builder.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_full_pipeline_recipe_sequence_builder.hpp"

#include "sljit_expression_plan_tree.hpp"
#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_projected_grouped_aggregate_update_primitive.hpp"
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
		case SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION:
			// a probe's selection references build rows only generated steps read
			if (step_idx + 1 == sequence.Count()) {
				return false;
			}
			return true;
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

bool SljitFullPipelineRecipeSequenceBuilder::TryMakeGeneratedOperatorStep(
    const SljitFullPipelinePrimitiveSequence &sequence, idx_t op_idx, SljitFullPipelinePrimitiveStep &step,
    idx_t &next_op_idx) const {
	next_op_idx = op_idx + 1;
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
		// A projection computing the keys of the probe after it folds into the probe, whose selected rows the
		// projection after the probe reads; the probe selects from the whole chunk the projection would have read
		const auto hash_join_idx = op_idx + 1;
		SljitHashJoinProbeSelectionPrimitive selection;
		if (hash_join_idx + 1 < ops.size() && StepHandsOverWholeChunks(sequence.Step(sequence.Count() - 1)) &&
		    ops[hash_join_idx].Kind() == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		    ops[hash_join_idx + 1].Kind() == SljitNativeRegionOpKind::PROJECTION &&
		    SljitTryBindElidedHashJoinProbeSelection(ops, op_idx, hash_join_idx, source_min_values, source_max_values,
		                                             selection)) {
			step = SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(selection);
			next_op_idx = hash_join_idx + 1;
			return true;
		}
		// consecutive projections run as one chain
		auto final_projection_idx = op_idx;
		while (final_projection_idx + 1 < ops.size() &&
		       ops[final_projection_idx + 1].Kind() == SljitNativeRegionOpKind::PROJECTION) {
			final_projection_idx++;
		}
		SljitProjectionChainPrimitive projection;
		if (!SljitTryBindProjectionChainPrimitive(ops, op_idx, final_projection_idx, projection)) {
			return false;
		}
		step = SljitFullPipelinePrimitiveStep::ProjectionChain(projection);
		next_op_idx = final_projection_idx + 1;
		return true;
	}
	case SljitNativeRegionOpKind::HASH_JOIN_PROBE: {
		// A filter on a MARK probe's marker selects in the probe's own pass
		const auto filter_idx = op_idx + 1;
		if (filter_idx < ops.size() && ops[filter_idx].Kind() == SljitNativeRegionOpKind::FILTER &&
		    ops[op_idx].HashJoinProbe().plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
		    SljitIsMarkProbeMarkerFilter(ops[op_idx], ops[filter_idx])) {
			const auto projection_idx =
			    filter_idx + 1 < ops.size() && ops[filter_idx + 1].Kind() == SljitNativeRegionOpKind::PROJECTION
			        ? filter_idx + 1
			        : DConstants::INVALID_INDEX;
			// without a projection to read through the kept rows' selection, the probe hands them over compacted
			const bool materialize_kept_rows = projection_idx == DConstants::INVALID_INDEX;
			SljitMarkProbeFilterBoundaryPrimitive boundary;
			if (SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, op_idx, filter_idx, true, projection_idx,
			                                                 materialize_kept_rows, boundary)) {
				step = SljitFullPipelinePrimitiveStep::MarkProbeFilterBoundary(boundary);
				next_op_idx = filter_idx + 1;
				return true;
			}
		}
		// a probe reads the previous probe's selected rows where they stand
		if (op_idx + 1 < ops.size() && ops[op_idx + 1].Kind() == SljitNativeRegionOpKind::HASH_JOIN_PROBE &&
		    TryMakeHashJoinProbeSelectionStep(op_idx, step)) {
			return true;
		}
		return TryMakeHashJoinProbeMaterializeStep(op_idx, step);
	}
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

// References, constants and casts of them cost nothing to fuse into the aggregate input.
static bool SljitExpressionIsReferenceLike(const ExecutionExpressionIR &node) {
	if (node.kind == ExecutionExpressionIRKind::REFERENCE || node.kind == ExecutionExpressionIRKind::CONSTANT) {
		return true;
	}
	return node.kind == ExecutionExpressionIRKind::CAST && node.left && !node.right &&
	       SljitExpressionIsReferenceLike(*node.left);
}

static bool SljitExpressionsAreReferenceLike(const vector<SljitExecutableRegionExpression> &expressions) {
	for (auto &expression : expressions) {
		auto tree = CopySljitExpressionPlanAsInputTree(expression.plan);
		if (!tree || !SljitExpressionIsReferenceLike(*tree)) {
			return false;
		}
	}
	return true;
}

//! A probe step that emits build columns; a MARK boundary only selects rows of its input
static bool SljitStepEmitsBuildColumns(const SljitFullPipelinePrimitiveStep &step) {
	return step.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_MATERIALIZE ||
	       step.kind == SljitFullPipelinePrimitiveKind::HASH_JOIN_PROBE_SELECTION;
}

//! The projections [first_projection_idx, aggregate) and the aggregate after them, the region's last op, as the
//! aggregate's input: an ungrouped update after one composed chain, a grouped update reading the projections where
//! it can, or the chain and a grouped update
bool SljitFullPipelineRecipeSequenceBuilder::TryMakeProjectedSinkSteps(
    idx_t first_projection_idx, const SljitFullPipelinePrimitiveSequence &sequence,
    vector<SljitFullPipelinePrimitiveStep> &steps) const {
	const auto aggregate_idx = ops.size() - 1;
	if (first_projection_idx >= aggregate_idx ||
	    ops[aggregate_idx].Kind() != SljitNativeRegionOpKind::AGGREGATE_UPDATE ||
	    !ops[aggregate_idx].AggregateUpdate().plan.UsesPrimitivePayloads()) {
		return false;
	}
	bool reference_like = SljitExpressionsAreReferenceLike(ops[aggregate_idx].AggregateUpdate().payloads);
	for (auto op_idx = first_projection_idx; op_idx < aggregate_idx; op_idx++) {
		if (ops[op_idx].Kind() != SljitNativeRegionOpKind::PROJECTION) {
			return false;
		}
		reference_like = reference_like && SljitExpressionsAreReferenceLike(ops[op_idx].Projections());
	}
	const auto final_projection_idx = aggregate_idx - 1;
	bool after_filter = false;
	bool after_probe = false;
	for (idx_t step_idx = 0; step_idx < sequence.Count(); step_idx++) {
		const auto &step = sequence.Step(step_idx);
		after_filter = after_filter || step.kind == SljitFullPipelinePrimitiveKind::GENERATED_FILTER;
		after_probe = after_probe || SljitStepEmitsBuildColumns(step);
	}
	// Behind a source filter, fusing the projections into the aggregate input only pays off while it references or
	// casts columns: computed expressions are cheaper as their own step over the selected rows
	if (after_filter && !after_probe && !reference_like) {
		return false;
	}
	SljitProjectionChainPrimitive projection;
	const bool chain =
	    SljitTryBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx, projection);
	SljitUngroupedAggregateUpdatePrimitive ungrouped;
	if (chain && SljitTryBindUngroupedAggregateUpdatePrimitive(ops, aggregate_idx, ungrouped)) {
		steps.push_back(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
		steps.push_back(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(ungrouped));
		return true;
	}
	// a probe's output reaches the aggregate as a view the direct payload update does not read
	SljitGroupedAggregateUpdatePrimitive grouped;
	if (SljitTryBindProjectedInputGroupedAggregateUpdatePrimitive(ops, first_projection_idx, final_projection_idx,
	                                                              aggregate_idx, !after_probe, grouped)) {
		steps.push_back(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped));
		return true;
	}
	if (chain && SljitTryBindGroupedAggregateUpdatePrimitive(ops, aggregate_idx, grouped)) {
		steps.push_back(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
		steps.push_back(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped));
		return true;
	}
	return false;
}

//! A probe feeding the region's hash join build, through at most one projection, hands the build its selected rows:
//! the build reads the probe's output columns where they stand instead of a materialized join chunk
bool SljitFullPipelineRecipeSequenceBuilder::TryMakeSelectedBuildSinkSteps(
    idx_t probe_idx, SljitFullPipelinePrimitiveStep &probe_step, SljitFullPipelinePrimitiveStep &sink_step) const {
	const auto build_idx = ops.size() - 1;
	if (probe_idx >= build_idx || ops[probe_idx].Kind() != SljitNativeRegionOpKind::HASH_JOIN_PROBE ||
	    ops[build_idx].Kind() != SljitNativeRegionOpKind::HASH_JOIN_BUILD) {
		return false;
	}
	auto projection_idx = DConstants::INVALID_INDEX;
	if (probe_idx + 2 == build_idx) {
		projection_idx = probe_idx + 1;
	} else if (probe_idx + 1 != build_idx) {
		return false;
	}
	SljitHashJoinBuildSinkPrimitive sink;
	if (!SljitTryBindHashJoinBuildSinkPrimitive(ops, build_idx, projection_idx, sink) ||
	    !TryMakeHashJoinProbeSelectionStep(probe_idx, probe_step)) {
		return false;
	}
	sink_step = SljitFullPipelinePrimitiveStep::HashJoinBuildSink(sink);
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
		SljitFullPipelinePrimitiveStep probe_step;
		vector<SljitFullPipelinePrimitiveStep> projected_sink_steps;
		if (sequence.Count() + 2 <= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES &&
		    TryMakeProjectedSinkSteps(op_idx, sequence, projected_sink_steps)) {
			for (auto &projected_step : projected_sink_steps) {
				sequence.Add(std::move(projected_step));
			}
			recipe = MakePrimitiveSequence(std::move(sequence));
			return true;
		}
		if (sequence.Count() + 2 <= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES &&
		    TryMakeSelectedBuildSinkSteps(op_idx, probe_step, sink_step)) {
			sequence.Add(std::move(probe_step));
			sequence.Add(std::move(sink_step));
			recipe = MakePrimitiveSequence(std::move(sequence));
			return true;
		}
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
		idx_t next_op_idx;
		if (sequence.Count() + 1 >= SLJIT_FULL_PIPELINE_MAX_PRIMITIVES ||
		    !TryMakeGeneratedOperatorStep(sequence, op_idx, step, next_op_idx)) {
			break;
		}
		sequence.Add(std::move(step));
		// a step computing several ops leaves no operator boundary between them
		op_idx = next_op_idx - 1;
	}
	if (hand_over_idx == DConstants::INVALID_INDEX) {
		return false;
	}
	// The rows a MARK boundary keeps reach DuckDB's operators through projections at most: the boundary hands them
	// over compacted rather than as a selection every later chunk is sliced by
	for (auto step_idx = handed_over.Count(); step_idx > 0; step_idx--) {
		auto &step = handed_over.Step(step_idx - 1);
		if (step.kind == SljitFullPipelinePrimitiveKind::PROJECTION_CHAIN) {
			continue;
		}
		if (step.kind == SljitFullPipelinePrimitiveKind::MARK_PROBE_FILTER_BOUNDARY) {
			auto &boundary = step.mark_probe_filter_boundary;
			const auto projection_idx = boundary.filter_idx + 1 < ops.size() && ops[boundary.filter_idx + 1].Kind() ==
			                                                                        SljitNativeRegionOpKind::PROJECTION
			                                ? boundary.filter_idx + 1
			                                : DConstants::INVALID_INDEX;
			SljitMarkProbeFilterBoundaryPrimitive compacted;
			if (SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, boundary.hash_join_idx, boundary.filter_idx, true,
			                                                 projection_idx, true, compacted)) {
				boundary = std::move(compacted);
			}
		}
		break;
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
