//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_projection_aggregate_recipe_binding.cpp
//
//
//===----------------------------------------------------------------------===//

#include "sljit_projection_aggregate_recipe_binding.hpp"

#include "sljit_hash_join_projection_source_runtime.hpp"
#include "sljit_pre_join_projection_recipe.hpp"
#include "sljit_projected_grouped_aggregate_update_primitive.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

#include <utility>

namespace duckdb {

SljitProjectionAggregateRecipeBinding::SljitProjectionAggregateRecipeBinding(
    const vector<SljitExecutableRegionOp> &ops_p, const vector<LogicalType> &source_output_types_p,
    const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p)
    : SljitFullPipelineRecipeSequenceBuilder(ops_p, source_output_types_p, source_min_values_p, source_max_values_p) {
}

bool SljitProjectionAggregateRecipeBinding::TryMakeMarkFilterProjectionNativeTailRecipe(
    const SljitMarkFilterProjectionNativeTailFacts &facts, SljitFullPipelineRecipe &recipe) const {
	SljitFullPipelinePrimitiveStep boundary_step;
	if (!TryMakeMarkProbeFilterBoundaryStep(facts.hash_join_idx, facts.filter_idx, true, facts.first_projection_idx,
	                                        false, boundary_step)) {
		return false;
	}
	auto sequence = MakeSourceSequence();
	sequence.Add(std::move(boundary_step));
	return TryMakeProjectionNativeTailRecipe(std::move(sequence), facts.first_projection_idx,
	                                         facts.final_projection_idx, facts.tail_start_idx, recipe);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeSourceProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts,
    SljitFullPipelineRecipe &recipe) const {
	if (shape.ProjectionCount() == 0) {
		return false;
	}
	auto sequence = MakeSourceSequence();
	if (facts.HasSourceFilter()) {
		SljitGeneratedFilterPrimitive source_filter;
		if (!SljitTryBindGeneratedFilterPrimitive(ops, facts.source_filter_idx, source_filter)) {
			return false;
		}
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(source_filter));
	}
	if (facts.HasSourceProjection()) {
		AddProjectionChainStep(sequence, facts.source_projection_idx);
	}
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
	SljitUngroupedAggregateUpdatePrimitive aggregate_update;
	if (shape.ProjectionCount() == 0 &&
	    SljitTryBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx, aggregate_update)) {
		// The aggregate reads the probe rows the marker filter keeps: the probe emits their selection directly
		auto sequence = MakeProjectionAggregateMarkFilterPrefix(facts, true, DConstants::INVALID_INDEX);
		sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
		recipe = MakePrimitiveSequence(std::move(sequence));
		return true;
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
	if (!facts.HasMarkFilter()) {
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
		if (facts.HasSourceFilter() && (!SljitCanBindGeneratedFilterPrimitive(ops, facts.source_filter_idx) ||
		                                (facts.HasSourceProjection() &&
		                                 !SljitCanBindProjectionChainPrimitive(ops, facts.source_projection_idx)))) {
			return false;
		}
		if (facts.HasSourceFilter()) {
			return !facts.HasJoinInputProjection(0) && shape.ProjectionCount() <= 2;
		}
		return facts.HasJoinInputProjection(0) || shape.ProjectionCount() <= 2;
	}
	if (facts.HasMarkFilter() || facts.HasSourceFilter() ||
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
		if (facts.HasSourceFilter() && !SljitCanBindGeneratedFilterPrimitive(ops, facts.source_filter_idx)) {
			return false;
		}
		if (facts.HasSourceProjection() && !SljitCanBindProjectionChainPrimitive(ops, facts.source_projection_idx)) {
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
	if (facts.HasSourceFilter()) {
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
	if (facts.HasSourceFilter()) {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.source_filter_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	}
	if (facts.HasSourceProjection()) {
		auto pre_join_projection = PreJoinProjectionBinding();
		if (pre_join_projection.TryAppendElidedHashJoinProbeSelection(sequence, facts.source_projection_idx,
		                                                              hash_join_idx, true)) {
			return sequence;
		}
		AddProjectionChainStep(sequence, facts.source_projection_idx);
	}
	if (facts.HasSourceFilter()) {
		sequence.Add(MakeHashJoinProbeSelectionStep(hash_join_idx));
	} else if (facts.HasJoinInputProjection(0)) {
		auto pre_join_projection = PreJoinProjectionBinding();
		sequence = pre_join_projection.MakeHashJoinSelectionSequence(facts.JoinInputProjectionIdx(0), hash_join_idx);
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
	if (facts.HasJoinInputProjection(0) && !facts.HasSourceFilter() &&
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
	if (facts.HasSourceFilter()) {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.source_filter_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
	}
	if (facts.HasSourceProjection()) {
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
	SljitFullPipelinePrimitiveStep step;
	if (!TryMakeMarkProbeFilterBoundaryStep(hash_join_idx, filter_idx, apply_filter_selection,
	                                        downstream_projection_idx, materialize_filter_selection, step)) {
		throw InternalException("SLJIT MARK probe filter boundary step cannot bind requested contract");
	}
	return step;
}

bool SljitTryBindMarkProbeFilterBoundaryPrimitive(const vector<SljitExecutableRegionOp> &ops, idx_t hash_join_idx,
                                                  idx_t filter_idx, bool apply_filter_selection,
                                                  idx_t downstream_projection_idx, bool materialize_filter_selection,
                                                  SljitMarkProbeFilterBoundaryPrimitive &primitive) {
	if (!SljitCanBindMarkProbeFilterBoundaryPrimitive(ops, hash_join_idx, filter_idx) ||
	    (materialize_filter_selection && !apply_filter_selection)) {
		return false;
	}
	const bool has_projection = downstream_projection_idx != DConstants::INVALID_INDEX;
	if (has_projection && (downstream_projection_idx >= ops.size() ||
	                       ops[downstream_projection_idx].kind != SljitNativeRegionOpKind::PROJECTION)) {
		return false;
	}
	const auto &join = ops[hash_join_idx];
	const auto &input_types = join.hash_join_probe.plan.input_types;
	const auto &lhs_columns = join.hash_join_probe.plan.operator_info.hash_join_contract.lhs_output_column_indices;
	if (join.output_types.size() != lhs_columns.size() + 1 || !SljitIsBooleanType(join.output_types.back())) {
		return false;
	}
	SljitMarkProbeFilterBoundaryPrimitive candidate;
	candidate.hash_join_idx = hash_join_idx;
	candidate.filter_idx = filter_idx;
	candidate.apply_filter_selection = apply_filter_selection;
	candidate.filter_mode = SljitMarkProbeMarkerFilterMode(join, ops[filter_idx]);
	if (apply_filter_selection && candidate.filter_mode == SljitMarkProbeFilterMode::NONE) {
		return false;
	}
	if (has_projection) {
		const bool reads_marker = SljitProjectionReferencesInputColumn(
		    ops[downstream_projection_idx], join.output_types.size(), join.output_types.size() - 1);
		if (apply_filter_selection && !materialize_filter_selection && !reads_marker) {
			candidate.marker_mode = SljitMarkProbeFilterBoundaryMarkerMode::OMIT_MARKER;
		} else if (candidate.filter_mode == SljitMarkProbeFilterMode::MATCHES || !reads_marker) {
			candidate.marker_mode = SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_TRUE;
		}
	} else if (materialize_filter_selection) {
		candidate.marker_mode = candidate.filter_mode == SljitMarkProbeFilterMode::MATCHES
		                            ? SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_TRUE
		                            : SljitMarkProbeFilterBoundaryMarkerMode::REFERENCE_FALSE;
	}
	const bool omit_marker = candidate.marker_mode == SljitMarkProbeFilterBoundaryMarkerMode::OMIT_MARKER;
	if ((omit_marker || materialize_filter_selection) && candidate.filter_mode != SljitMarkProbeFilterMode::NONE &&
	    !join.hash_join_probe.plan.perfect_hash_probe && !join.hash_join_probe.plan.residual_predicate) {
		candidate.strategy = materialize_filter_selection ? SljitMarkProbeFilterBoundaryStrategy::FILTERED_MATERIALIZED
		                                                  : SljitMarkProbeFilterBoundaryStrategy::FILTERED_SELECTION;
	}
	vector<SljitBatchLayoutColumn> columns;
	const auto column_count = join.output_types.size() - (omit_marker ? 1 : 0);
	columns.reserve(column_count);
	for (idx_t column_idx = 0; column_idx < lhs_columns.size(); column_idx++) {
		const auto input_idx = lhs_columns[column_idx];
		if (input_idx >= input_types.size() || input_types[input_idx] != join.output_types[column_idx]) {
			return false;
		}
		columns.push_back({input_idx, input_types[input_idx], join.output_types[column_idx]});
	}
	if (!omit_marker) {
		columns.push_back({DConstants::INVALID_INDEX, LogicalType(), LogicalType::BOOLEAN});
	}
	candidate.output_layout = make_shared_ptr<SljitBatchLayout>(hash_join_idx, input_types, std::move(columns));
	primitive = std::move(candidate);
	return true;
}

bool SljitProjectionAggregateRecipeBinding::TryMakeMarkProbeFilterBoundaryStep(
    idx_t hash_join_idx, idx_t filter_idx, bool apply_filter_selection, idx_t downstream_projection_idx,
    bool materialize_filter_selection, SljitFullPipelinePrimitiveStep &step) const {
	SljitMarkProbeFilterBoundaryPrimitive primitive;
	if (!SljitTryBindMarkProbeFilterBoundaryPrimitive(ops, hash_join_idx, filter_idx, apply_filter_selection,
	                                                  downstream_projection_idx, materialize_filter_selection,
	                                                  primitive)) {
		return false;
	}
	step = SljitFullPipelinePrimitiveStep::MarkProbeFilterBoundary(primitive);
	return true;
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
	if (facts.JoinCount() > 1 || facts.HasSourceFilter() || facts.HasJoinInputProjection(0)) {
		auto sequence = MakeProjectionAggregatePreMarkJoinPrefix(facts);
		sequence.Add(MakeMarkProbeFilterBoundaryStep(facts.FinalHashJoinIdx(), facts.mark_filter_idx, true,
		                                             downstream_projection_idx, materialize_filter_selection));
		return sequence;
	}
	return MakeMarkFilterPrefix(facts.HashJoinIdx(0), facts.mark_filter_idx, true, downstream_projection_idx,
	                            materialize_filter_selection);
}

bool SljitProjectionAggregateRecipeBinding::TryBindPostJoinProjectionAggregatePrimitive(
    const SljitFullPipelineProjectionAggregateShape &shape, idx_t hash_join_idx,
    SljitPostJoinProjectionAggregatePrimitive &primitive) const {
	SljitPostJoinProjectionAggregatePrimitive candidate;
	if (!SljitTryBindPostJoinProjectionPrimitive(ops, hash_join_idx, shape.first_projection_idx,
	                                             shape.final_projection_idx, candidate.post_join_projection)) {
		return false;
	}
	candidate.aggregate_idx = shape.aggregate_idx;
	if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, candidate)) {
		return false;
	}
	primitive = std::move(candidate);
	return true;
}

SljitPreJoinProjectionRecipeBinding SljitProjectionAggregateRecipeBinding::PreJoinProjectionBinding() const {
	return SljitPreJoinProjectionRecipeBinding(ops, source_output_types, source_min_values, source_max_values);
}

bool SljitProjectionAggregateRecipeBinding::TryMakeProjectionAggregateRecipe(
    SljitFullPipelinePrimitiveSequence sequence, const SljitFullPipelineProjectionAggregateShape &shape,
    bool allow_direct_projected_primitive_payload_update, SljitFullPipelineRecipe &recipe) const {
	SljitUngroupedAggregateUpdatePrimitive aggregate_update;
	if (SljitTryBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx, aggregate_update)) {
		SljitProjectionChainPrimitive projection;
		if (!SljitTryBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx,
		                                          projection)) {
			return false;
		}
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
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
	SljitProjectionChainPrimitive projection;
	if (!SljitTryBindGroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx, grouped_update) ||
	    !SljitTryBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx,
	                                          projection)) {
		return false;
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
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
	SljitProjectionChainPrimitive projection;
	if (!SljitTryBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx, projection)) {
		return false;
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
	return TryMakeNativeTailRecipe(std::move(sequence), tail_start_idx, recipe);
}

} // namespace duckdb
