//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_recipe_binding.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_projection_aggregate_recipe_binding.hpp"

#include "sljit_pre_join_projection_recipe.hpp"
#include "sljit_projected_grouped_aggregate_update_primitive.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

#include <utility>

namespace duckdb {

SljitProjectionAggregateRecipeBinding::SljitProjectionAggregateRecipeBinding(
    const vector<SljitExecutableRegionOp> &ops_p, const vector<LogicalType> &source_output_types_p,
    const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
    bool uses_extended_source_fetch_budget_p)
    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p,
                                             uses_extended_source_fetch_budget_p) {
}

bool SljitProjectionAggregateRecipeBinding::TryMakeMarkFilterProjectionNativeTailRecipe(
    const SljitMarkFilterProjectionNativeTailFacts &facts, SljitFullPipelineRecipe &recipe) const {
	if (!SljitCanBindMarkProbeFilterBoundaryPrimitive(ops, facts.hash_join_idx, facts.filter_idx)) {
		return false;
	}
	auto sequence = MakeMarkFilterPrefix(facts.hash_join_idx, facts.filter_idx, true, facts.first_projection_idx);
	return TryMakeProjectionNativeTailRecipe(std::move(sequence), facts.first_projection_idx,
	                                         facts.final_projection_idx, facts.tail_start_idx, recipe);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeSourceProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, SljitFullPipelineRecipe &recipe) const {
	if (shape.ProjectionCount() == 0) {
		return false;
	}
	auto sequence = MakeSourceSequence();
	if (TryMakeProjectionAggregateRecipe(sequence, shape, true, recipe)) {
		return true;
	}
	return TryMakeProjectionNativeTailRecipe(std::move(sequence), shape, recipe);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeJoinDirectProjectionAggregateRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts,
    SljitFullPipelineRecipe &recipe) const {
	if (!CanBindDirectProjectionAggregate(shape, facts)) {
		return false;
	}
	const auto aggregate_join_idx = facts.FinalHashJoinIdx();
	SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
	if (!TryBindPostJoinProjectionAggregatePrimitive(shape, aggregate_join_idx, post_join_aggregate)) {
		return false;
	}
	SljitFullPipelinePrimitiveSequence sequence;
	if (facts.JoinCount() > 1) {
		sequence = MakeJoinPrefixSelectionSequence(facts);
	} else {
		sequence = MakeSingleJoinDirectPrefixSequence(facts, post_join_aggregate);
	}
	const auto probe_step_idx = sequence.Count() - 1;
	const auto &probe = sequence.Step(probe_step_idx).hash_join_probe_selection;
	sequence.Add(SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(post_join_aggregate));
	auto direct_consumer = SljitMakeHashJoinDirectAggregateConsumerContract(
	    probe_step_idx, sequence.Count() - 1, probe.hash_join_idx, post_join_aggregate.aggregate_idx);
	recipe = MakePrimitiveSequence(std::move(sequence), direct_consumer);
	return true;
}

bool SljitProjectionAggregateRecipeBinding::TryMakeJoinProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts,
    SljitFullPipelineRecipe &recipe) const {
	if (!CanBindProjectionAggregateTail(shape, facts)) {
		return false;
	}
	SljitFullPipelinePrimitiveSequence sequence;
	if (facts.JoinCount() > 1) {
		sequence = MakeJoinPrefixSelectionSequence(facts);
	} else {
		sequence = MakeSingleJoinProjectionAggregateTailPrefixSequence(facts);
	}
	return TryMakeProjectionAggregateTailRecipe(std::move(sequence), shape, recipe);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeMarkFilterProjectionAggregateRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts,
    SljitFullPipelineRecipe &recipe) const {
	if (!CanBindMarkFilterBoundary(facts) || (facts.JoinCount() > 1 && shape.ProjectionCount() == 0)) {
		return false;
	}
	auto sequence = MakeProjectionAggregateMarkFilterPrefix(facts, false, shape.first_projection_idx);
	return TryMakeProjectionAggregateRecipe(std::move(sequence), shape, true, recipe);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeMarkFilterNativeTailRecipe(
    const SljitProjectionAggregatePrefixFacts &facts, SljitFullPipelineRecipe &recipe) const {
	if (!CanBindMarkFilterBoundary(facts)) {
		return false;
	}
	auto sequence = MakeProjectionAggregateMarkFilterPrefix(facts, true, DConstants::INVALID_INDEX);
	return TryMakeNativeTailRecipe(std::move(sequence), facts.mark_filter_idx + 1, recipe);
}

bool SljitProjectionAggregateRecipeBinding::CanBindMarkFilterBoundary(
    const SljitProjectionAggregatePrefixFacts &facts) const {
	if (!facts.HasMarkFilter() || facts.HasSourceFilterProjection() || facts.HasAnyJoinInputProjection()) {
		return false;
	}
	const auto hash_join_idx = facts.MarkFilterHashJoinIdx();
	if (hash_join_idx >= ops.size() || facts.mark_filter_idx >= ops.size()) {
		return false;
	}
	return facts.mark_filter_idx == hash_join_idx + 1 &&
	       ops[hash_join_idx].hash_join_probe.plan.output_mode == ExecutionHashJoinProbeOutputMode::MARK_PROBE &&
	       SljitIsMarkProbeMarkerFilter(ops[hash_join_idx], ops[facts.mark_filter_idx]);
}

bool SljitProjectionAggregateRecipeBinding::CanBindDirectProjectionAggregate(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts) const {
	if (shape.aggregate_idx >= ops.size() ||
	    !SljitAggregateUpdateHasDedicatedCompiledBackend(ops[shape.aggregate_idx])) {
		return false;
	}
	if (facts.JoinCount() == 1) {
		const auto hash_join_idx = facts.HashJoinIdx(0);
		if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx) ||
		    (shape.ProjectionCount() == 0 &&
		     !SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, hash_join_idx))) {
			return false;
		}
		if (facts.HasSourceFilterProjection()) {
			return !facts.HasJoinInputProjection(0) && shape.ProjectionCount() == 1;
		}
		return facts.HasJoinInputProjection(0) || shape.ProjectionCount() <= 2;
	}
	if (facts.HasMarkFilter() || facts.HasSourceFilterProjection() ||
	    !SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.FinalHashJoinIdx()) ||
	    !CanBindJoinPrefixInputs(facts)) {
		return false;
	}
	if (facts.HasJoinInputProjection(1)) {
		return !facts.HasJoinInputProjection(0) && shape.ProjectionCount() != 0 &&
		       CanBindHashJoinProbeProjectionInput(facts.HashJoinIdx(0));
	}
	if (facts.HasJoinInputProjection(0)) {
		return false;
	}
	const auto projection_count = shape.ProjectionCount();
	if (projection_count != 0 && projection_count != 1) {
		return false;
	}
	if (projection_count == 0) {
		return SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.HashJoinIdx(0)) &&
		       SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.FinalHashJoinIdx());
	}
	return SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.HashJoinIdx(0)) &&
	       SljitFullPipelineHashJoinProbeIsMatchedProbeAndBuild(ops, facts.FinalHashJoinIdx());
}

bool SljitProjectionAggregateRecipeBinding::CanBindProjectionAggregateTail(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts) const {
	if (facts.JoinCount() == 1) {
		const auto hash_join_idx = facts.HashJoinIdx(0);
		if (facts.HasMarkFilter() || !CanBindHashJoinProbeProjectionInput(hash_join_idx)) {
			return false;
		}
		if (facts.HasSourceFilterProjection() &&
		    (!SljitCanBindGeneratedFilterPrimitive(ops, facts.source_filter_idx) ||
		     !SljitCanBindProjectionChainPrimitive(ops, facts.source_projection_idx))) {
			return false;
		}
		return !facts.HasJoinInputProjection(0) ||
		       SljitCanBindProjectionChainPrimitive(ops, facts.JoinInputProjectionIdx(0));
	}
	return SljitCanBindHashJoinProbeSelectionPrimitive(ops, facts.FinalHashJoinIdx()) &&
	       CanBindProjectionAggregateTailPrefix(facts) && shape.ProjectionCount() != 0 &&
	       CanBindJoinPrefixInputs(facts);
}

bool SljitProjectionAggregateRecipeBinding::CanBindProjectionAggregateTailPrefix(
    const SljitProjectionAggregatePrefixFacts &facts) const {
	if (facts.HasMarkFilter()) {
		return false;
	}
	if (facts.HasSourceFilterProjection()) {
		return facts.JoinCount() == 1 || !facts.HasJoinInputProjection(0);
	}
	return true;
}

bool SljitProjectionAggregateRecipeBinding::CanBindHashJoinProbeProjectionInput(idx_t hash_join_idx) const {
	return SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx) ||
	       SljitCanBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx);
}

bool SljitProjectionAggregateRecipeBinding::CanBindJoinPrefixInputs(
    const SljitProjectionAggregatePrefixFacts &facts) const {
	if (facts.JoinCount() == 0) {
		return false;
	}
	const auto final_join_ordinal = facts.JoinCount() - 1;
	for (idx_t join_ordinal = 0; join_ordinal < final_join_ordinal; join_ordinal++) {
		if (facts.HasJoinInputProjection(join_ordinal) &&
		    !SljitCanBindProjectionChainPrimitive(ops, facts.JoinInputProjectionIdx(join_ordinal))) {
			return false;
		}
		if (!CanBindHashJoinProbeProjectionInput(facts.HashJoinIdx(join_ordinal))) {
			return false;
		}
	}
	return !facts.HasJoinInputProjection(final_join_ordinal) ||
	       SljitCanBindProjectionChainPrimitive(ops, facts.JoinInputProjectionIdx(final_join_ordinal));
}

SljitFullPipelinePrimitiveSequence SljitProjectionAggregateRecipeBinding::MakeSingleJoinDirectPrefixSequence(
    const SljitProjectionAggregatePrefixFacts &facts,
    SljitPostJoinProjectionAggregatePrimitive &post_join_aggregate) const {
	const auto hash_join_idx = facts.HashJoinIdx(0);
	auto sequence = MakeSourceSequence();
	if (facts.HasSourceFilterProjection()) {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.source_filter_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, facts.source_projection_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(hash_join_idx));
	} else if (facts.HasJoinInputProjection(0)) {
		SljitPreJoinProjectionViewDescriptor pre_join_view;
		auto pre_join_projection = PreJoinProjectionBinding();
		const auto has_pre_join_view =
		    pre_join_projection.TryBuildView(facts.JoinInputProjectionIdx(0), hash_join_idx, pre_join_view);
		SljitStringSetCaseGroupedPayloadProjection string_set_case_projection;
		if (has_pre_join_view &&
		    SljitTryBuildStringSetCaseGroupedPayloadProjection(
		        ops, pre_join_view, post_join_aggregate.post_join_projection.first_projection_idx,
		        post_join_aggregate.post_join_projection.final_projection_idx, string_set_case_projection)) {
			post_join_aggregate.post_join_projection.EnableStringSetCaseGroupedPayload(string_set_case_projection);
		}
		optional_ptr<const SljitPreJoinProjectionViewDescriptor> pre_join_view_ptr;
		if (has_pre_join_view) {
			pre_join_view_ptr = pre_join_view;
		}
		sequence = pre_join_projection.MakeHashJoinSelectionSequence(facts.JoinInputProjectionIdx(0), hash_join_idx,
		                                                             pre_join_view_ptr);
	} else {
		sequence.Add(MakeHashJoinProbeSelectionStep(hash_join_idx));
	}
	return sequence;
}

SljitFullPipelinePrimitiveSequence
SljitProjectionAggregateRecipeBinding::MakeSingleJoinProjectionAggregateTailPrefixSequence(
    const SljitProjectionAggregatePrefixFacts &facts) const {
	const auto hash_join_idx = facts.HashJoinIdx(0);
	auto sequence = MakeSourceSequence();
	if (facts.HasJoinInputProjection(0) && !facts.HasSourceFilterProjection() &&
	    PreJoinProjectionBinding().TryAppendElidedHashJoinProbeSelection(sequence, facts.JoinInputProjectionIdx(0),
	                                                                     hash_join_idx)) {
		return sequence;
	}
	AddProjectionAggregateFirstJoinPrefix(sequence, facts);
	sequence.Add(MakeHashJoinProbeProjectionInputStep(hash_join_idx));
	return sequence;
}

SljitFullPipelinePrimitiveSequence SljitProjectionAggregateRecipeBinding::MakeJoinPrefixSelectionSequence(
    const SljitProjectionAggregatePrefixFacts &facts) const {
	auto sequence = MakeSourceSequence();
	AddProjectionAggregateSourcePrefix(sequence, facts);
	const auto final_join_ordinal = facts.JoinCount() - 1;
	for (idx_t join_ordinal = 0; join_ordinal < final_join_ordinal; join_ordinal++) {
		AddJoinInputProjection(sequence, facts, join_ordinal);
		sequence.Add(MakeHashJoinProbeProjectionInputStep(facts.HashJoinIdx(join_ordinal)));
	}
	AddJoinInputProjection(sequence, facts, final_join_ordinal);
	sequence.Add(MakeHashJoinProbeSelectionStep(facts.HashJoinIdx(final_join_ordinal)));
	return sequence;
}

void SljitProjectionAggregateRecipeBinding::AddProjectionAggregateSourcePrefix(
    SljitFullPipelinePrimitiveSequence &sequence, const SljitProjectionAggregatePrefixFacts &facts) const {
	if (facts.HasSourceFilterProjection()) {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.source_filter_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, facts.source_projection_idx);
	}
}

void SljitProjectionAggregateRecipeBinding::AddJoinInputProjection(SljitFullPipelinePrimitiveSequence &sequence,
                                                                   const SljitProjectionAggregatePrefixFacts &facts,
                                                                   idx_t join_ordinal) const {
	if (facts.HasJoinInputProjection(join_ordinal)) {
		AddProjectionChainStep(sequence, facts.JoinInputProjectionIdx(join_ordinal));
	}
}

void SljitProjectionAggregateRecipeBinding::AddProjectionAggregateFirstJoinPrefix(
    SljitFullPipelinePrimitiveSequence &sequence, const SljitProjectionAggregatePrefixFacts &facts) const {
	AddProjectionAggregateSourcePrefix(sequence, facts);
	AddJoinInputProjection(sequence, facts, 0);
}

SljitFullPipelinePrimitiveStep SljitProjectionAggregateRecipeBinding::MakeMarkProbeFilterBoundaryStep(
    idx_t hash_join_idx, idx_t filter_idx, bool apply_filter_selection, idx_t downstream_projection_idx,
    bool materialize_filter_selection) const {
	bool allow_marker_omission = false;
	if (apply_filter_selection && downstream_projection_idx != DConstants::INVALID_INDEX &&
	    !materialize_filter_selection) {
		auto &hash_join_op = ops[hash_join_idx];
		if (hash_join_op.output_types.empty()) {
			throw InternalException("SLJIT MARK probe filter boundary has no marker output");
		}
		const auto marker_idx = hash_join_op.output_types.size() - 1;
		allow_marker_omission = !SljitProjectionReferencesInputColumn(ops[downstream_projection_idx],
		                                                              hash_join_op.output_types.size(), marker_idx);
	}
	auto primitive = SljitBindMarkProbeFilterBoundaryPrimitive(ops, hash_join_idx, filter_idx, apply_filter_selection,
	                                                           downstream_projection_idx, allow_marker_omission,
	                                                           materialize_filter_selection);
	return SljitFullPipelinePrimitiveStep::MarkProbeFilterBoundary(primitive);
}

SljitFullPipelinePrimitiveSequence SljitProjectionAggregateRecipeBinding::MakeMarkFilterPrefix(
    idx_t hash_join_idx, idx_t filter_idx, bool apply_filter_selection, idx_t downstream_projection_idx,
    bool materialize_filter_selection) const {
	auto sequence = MakeSourceSequence();
	sequence.Add(MakeMarkProbeFilterBoundaryStep(hash_join_idx, filter_idx, apply_filter_selection,
	                                             downstream_projection_idx, materialize_filter_selection));
	return sequence;
}

SljitFullPipelinePrimitiveSequence SljitProjectionAggregateRecipeBinding::MakeProjectionAggregatePreMarkJoinPrefix(
    const SljitProjectionAggregatePrefixFacts &facts) const {
	auto sequence = MakeSourceSequence();
	AddProjectionAggregateSourcePrefix(sequence, facts);
	const auto marker_join_ordinal = facts.JoinCount() - 1;
	for (idx_t join_ordinal = 0; join_ordinal < marker_join_ordinal; join_ordinal++) {
		AddJoinInputProjection(sequence, facts, join_ordinal);
		sequence.Add(MakeHashJoinProbeProjectionInputStep(facts.HashJoinIdx(join_ordinal)));
	}
	AddJoinInputProjection(sequence, facts, marker_join_ordinal);
	return sequence;
}

SljitFullPipelinePrimitiveSequence SljitProjectionAggregateRecipeBinding::MakeProjectionAggregateMarkFilterPrefix(
    const SljitProjectionAggregatePrefixFacts &facts, bool materialize_filter_selection,
    idx_t downstream_projection_idx) const {
	if (facts.JoinCount() > 1 || facts.HasSourceFilterProjection() || facts.HasJoinInputProjection(0)) {
		auto sequence = MakeProjectionAggregatePreMarkJoinPrefix(facts);
		sequence.Add(MakeMarkProbeFilterBoundaryStep(facts.FinalHashJoinIdx(), facts.mark_filter_idx, true,
		                                             downstream_projection_idx, materialize_filter_selection));
		return sequence;
	}
	return MakeMarkFilterPrefix(facts.HashJoinIdx(0), facts.mark_filter_idx, true, downstream_projection_idx,
	                            materialize_filter_selection);
}

SljitFullPipelinePrimitiveSequence
SljitProjectionAggregateRecipeBinding::MakeSourceHashJoinProjectionInputSequence(idx_t hash_join_idx) const {
	auto sequence = MakeSourceSequence();
	sequence.Add(MakeHashJoinProbeProjectionInputStep(hash_join_idx));
	return sequence;
}

bool SljitProjectionAggregateRecipeBinding::TryBindPostJoinProjectionAggregatePrimitive(
    const SljitFullPipelineProjectionAggregateShape &shape, idx_t hash_join_idx,
    SljitPostJoinProjectionAggregatePrimitive &primitive) const {
	if (!SljitCanBindPostJoinProjectionPrimitive(ops, hash_join_idx, shape.first_projection_idx,
	                                             shape.final_projection_idx)) {
		return false;
	}
	SljitPostJoinProjectionAggregatePrimitive candidate;
	candidate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
	    ops, hash_join_idx, shape.first_projection_idx, shape.final_projection_idx);
	candidate.aggregate_idx = shape.aggregate_idx;
	if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, candidate)) {
		return false;
	}
	primitive = std::move(candidate);
	return true;
}

SljitPreJoinProjectionRecipeBinding SljitProjectionAggregateRecipeBinding::PreJoinProjectionBinding() const {
	return SljitPreJoinProjectionRecipeBinding(ops, source_output_types, source_min_values, source_max_values,
	                                           uses_extended_source_fetch_budget);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeProjectionAggregateRecipe(
    SljitFullPipelinePrimitiveSequence sequence, const SljitFullPipelineProjectionAggregateShape &shape,
    bool allow_direct_projected_primitive_payload_update, SljitFullPipelineRecipe &recipe) const {
	if (SljitCanBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx)) {
		if (!SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx)) {
			return false;
		}
		AddProjectionChainStep(sequence, shape.first_projection_idx, shape.final_projection_idx);
		auto aggregate_update = SljitBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
		recipe = MakePrimitiveSequence(std::move(sequence));
		return true;
	}

	SljitGroupedAggregateUpdatePrimitive grouped_update;
	if (SljitTryBindProjectedInputGroupedAggregateUpdatePrimitive(
	        ops, shape.first_projection_idx, shape.final_projection_idx, shape.aggregate_idx,
	        allow_direct_projected_primitive_payload_update, grouped_update)) {
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update));
		recipe = MakePrimitiveSequence(std::move(sequence));
		return true;
	}
	if (!SljitGroupedAggregateUpdateHasDedicatedBackend(ops, shape.aggregate_idx) ||
	    !SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx)) {
		return false;
	}
	AddProjectionChainStep(sequence, shape.first_projection_idx, shape.final_projection_idx);
	grouped_update = SljitBindGroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update));
	recipe = MakePrimitiveSequence(std::move(sequence));
	return true;
}

bool SljitProjectionAggregateRecipeBinding::TryMakeProjectionAggregateTailRecipe(
    SljitFullPipelinePrimitiveSequence sequence, const SljitFullPipelineProjectionAggregateShape &shape,
    SljitFullPipelineRecipe &recipe) const {
	if (TryMakeProjectionAggregateRecipe(sequence, shape, false, recipe)) {
		return true;
	}
	return TryMakeProjectionNativeTailRecipe(std::move(sequence), shape, recipe);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeProjectionNativeTailRecipe(
    SljitFullPipelinePrimitiveSequence sequence, const SljitFullPipelineProjectionAggregateShape &shape,
    SljitFullPipelineRecipe &recipe) const {
	return TryMakeProjectionNativeTailRecipe(std::move(sequence), shape.first_projection_idx,
	                                         shape.final_projection_idx, shape.aggregate_idx, recipe);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeProjectionNativeTailRecipe(
    SljitFullPipelinePrimitiveSequence sequence, idx_t first_projection_idx, idx_t final_projection_idx,
    idx_t tail_start_idx, SljitFullPipelineRecipe &recipe) const {
	SljitDelimJoinSinkPrimitive delim_sink;
	if (SljitTryBindProjectedDelimJoinSinkPrimitive(ops, first_projection_idx, final_projection_idx, tail_start_idx,
	                                                delim_sink)) {
		sequence.Add(SljitFullPipelinePrimitiveStep::DelimJoinSink(delim_sink));
		recipe = MakePrimitiveSequence(std::move(sequence));
		return true;
	}
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)) {
		return false;
	}
	AddProjectionChainStep(sequence, first_projection_idx, final_projection_idx);
	return TryMakeNativeTailRecipe(std::move(sequence), tail_start_idx, recipe);
}

} // namespace duckdb
