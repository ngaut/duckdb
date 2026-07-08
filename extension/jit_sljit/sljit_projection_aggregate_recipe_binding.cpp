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

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeMarkFilterProjectionNativeTailRecipe(
    const SljitMarkFilterProjectionNativeTailFacts &facts) const {
	auto sequence = MakeMarkFilterPrefix(facts.hash_join_idx, facts.filter_idx, true, facts.first_projection_idx);
	return MakeProjectionNativeTailRecipe(std::move(sequence), facts.first_projection_idx, facts.final_projection_idx,
	                                      facts.tail_start_idx);
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeSourceProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape) const {
	auto sequence = MakeSourceSequence();
	if (ProjectionAggregateHasDedicatedBackend(shape, true)) {
		return MakeProjectionAggregateRecipe(std::move(sequence), shape, true);
	}
	if (!CanMakeNativeTailRecipe(shape.aggregate_idx)) {
		throw InternalException("SLJIT source projection aggregate tail has no valid grouped backend or native tail");
	}
	return MakeProjectionNativeTailRecipe(std::move(sequence), shape);
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeJoinDirectProjectionAggregateRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts) const {
	const auto aggregate_join_idx = facts.FinalHashJoinIdx();
	auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(shape, aggregate_join_idx);
	SljitFullPipelinePrimitiveSequence sequence;
	if (facts.JoinCount() > 1) {
		sequence = MakeJoinPrefixSelectionSequence(facts);
	} else {
		sequence = MakeSingleJoinDirectPrefixSequence(facts, post_join_aggregate);
	}
	sequence.Add(SljitFullPipelinePrimitiveStep::PostJoinProjectionAggregateUpdate(post_join_aggregate));
	return MakePrimitiveSequence(std::move(sequence));
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeJoinProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts) const {
	SljitFullPipelinePrimitiveSequence sequence;
	if (facts.JoinCount() > 1) {
		sequence = MakeJoinPrefixSelectionSequence(facts);
	} else {
		sequence = MakeSingleJoinProjectionAggregateTailPrefixSequence(facts);
	}
	return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeMarkFilterProjectionAggregateRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape, const SljitProjectionAggregatePrefixFacts &facts) const {
	auto sequence = MakeProjectionAggregateMarkFilterPrefix(facts, false, shape.first_projection_idx);
	return MakeProjectionAggregateRecipe(std::move(sequence), shape, true);
}

SljitFullPipelineRecipe
SljitProjectionAggregateRecipeBinding::MakeMarkFilterNativeTailRecipe(
    const SljitProjectionAggregatePrefixFacts &facts) const {
	auto sequence = MakeProjectionAggregateMarkFilterPrefix(facts, true, DConstants::INVALID_INDEX);
	return MakeNativeTailRecipe(std::move(sequence), facts.mark_filter_idx + 1);
}

bool SljitProjectionAggregateRecipeBinding::ProjectionAggregateHasDedicatedBackend(
    const SljitFullPipelineProjectionAggregateShape &shape, bool allow_direct_projected_primitive_payload_update) const {
	if (SljitCanBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx)) {
		return SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx);
	}
	return ChooseProjectionGroupedAggregateRecipeMode(shape, allow_direct_projected_primitive_payload_update) !=
	       SljitProjectionGroupedAggregateRecipeMode::INVALID;
}

bool SljitProjectionAggregateRecipeBinding::CanMakeProjectionAggregateTailRecipe(
    const SljitFullPipelineProjectionAggregateShape &shape) const {
	return ProjectionAggregateHasDedicatedBackend(shape) || CanMakeNativeTailRecipe(shape.aggregate_idx);
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

SljitPostJoinProjectionAggregatePrimitive
SljitProjectionAggregateRecipeBinding::BindPostJoinProjectionAggregatePrimitive(
    const SljitFullPipelineProjectionAggregateShape &shape, idx_t hash_join_idx) const {
	SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
	post_join_aggregate.post_join_projection =
	    SljitBindPostJoinProjectionPrimitive(ops, hash_join_idx, shape.first_projection_idx, shape.final_projection_idx);
	post_join_aggregate.aggregate_idx = shape.aggregate_idx;
	if (!SljitCanBindPostJoinProjectionAggregatePrimitive(ops, post_join_aggregate)) {
		throw InternalException("SLJIT post-join projection aggregate primitive cannot bind requested aggregate");
	}
	return post_join_aggregate;
}

SljitProjectionGroupedAggregateRecipeMode
SljitProjectionAggregateRecipeBinding::ChooseProjectionGroupedAggregateRecipeMode(
    const SljitFullPipelineProjectionAggregateShape &shape, bool allow_direct_projected_primitive_payload_update) const {
	if (!SljitGroupedAggregateUpdateHasDedicatedBackend(ops, shape.aggregate_idx)) {
		return SljitProjectionGroupedAggregateRecipeMode::INVALID;
	}
	if (SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(
	        ops, shape.first_projection_idx, shape.final_projection_idx, shape.aggregate_idx,
	        allow_direct_projected_primitive_payload_update)) {
		return SljitProjectionGroupedAggregateRecipeMode::PROJECTED_INPUT;
	}
	if (SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx)) {
		return SljitProjectionGroupedAggregateRecipeMode::MATERIALIZED_PROJECTION;
	}
	return SljitProjectionGroupedAggregateRecipeMode::INVALID;
}

SljitPreJoinProjectionRecipeBinding SljitProjectionAggregateRecipeBinding::PreJoinProjectionBinding() const {
	return SljitPreJoinProjectionRecipeBinding(ops, source_output_types, source_min_values, source_max_values,
	                                           uses_extended_source_fetch_budget);
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeProjectionAggregateRecipe(
    SljitFullPipelinePrimitiveSequence sequence, const SljitFullPipelineProjectionAggregateShape &shape,
    bool allow_direct_projected_primitive_payload_update) const {
	if (!SljitCanBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx)) {
		return MakeProjectionGroupedAggregateRecipe(std::move(sequence), shape,
		                                            allow_direct_projected_primitive_payload_update);
	}
	if (!SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx)) {
		throw InternalException("SLJIT projected ungrouped aggregate recipe cannot bind projection chain");
	}
	AddProjectionChainStep(sequence, shape.first_projection_idx, shape.final_projection_idx);
	auto aggregate_update = SljitBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx);
	sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
	return MakePrimitiveSequence(std::move(sequence));
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeProjectionGroupedAggregateRecipe(
    SljitFullPipelinePrimitiveSequence sequence, const SljitFullPipelineProjectionAggregateShape &shape,
    bool allow_direct_projected_primitive_payload_update) const {
	switch (ChooseProjectionGroupedAggregateRecipeMode(shape, allow_direct_projected_primitive_payload_update)) {
	case SljitProjectionGroupedAggregateRecipeMode::PROJECTED_INPUT: {
		auto grouped_update = SljitBindProjectedInputGroupedAggregateUpdatePrimitive(
		    ops, shape.first_projection_idx, shape.final_projection_idx, shape.aggregate_idx,
		    allow_direct_projected_primitive_payload_update);
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update));
		return MakePrimitiveSequence(std::move(sequence));
	}
	case SljitProjectionGroupedAggregateRecipeMode::MATERIALIZED_PROJECTION: {
		AddProjectionChainStep(sequence, shape.first_projection_idx, shape.final_projection_idx);
		auto grouped_update = SljitBindGroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update));
		return MakePrimitiveSequence(std::move(sequence));
	}
	case SljitProjectionGroupedAggregateRecipeMode::INVALID:
		throw InternalException("SLJIT selected projection aggregate recipe cannot bind grouped aggregate backend");
	}
	throw InternalException("SLJIT selected projection aggregate recipe has an unknown grouped backend mode");
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeProjectionAggregateTailRecipe(
    SljitFullPipelinePrimitiveSequence sequence, const SljitFullPipelineProjectionAggregateShape &shape) const {
	if (ProjectionAggregateHasDedicatedBackend(shape)) {
		return MakeProjectionAggregateRecipe(std::move(sequence), shape);
	}
	if (!CanMakeNativeTailRecipe(shape.aggregate_idx)) {
		throw InternalException("SLJIT projection aggregate tail has no valid grouped backend or native tail");
	}
	return MakeProjectionNativeTailRecipe(std::move(sequence), shape);
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeProjectionNativeTailRecipe(
    SljitFullPipelinePrimitiveSequence sequence, const SljitFullPipelineProjectionAggregateShape &shape) const {
	return MakeProjectionNativeTailRecipe(std::move(sequence), shape.first_projection_idx, shape.final_projection_idx,
	                                      shape.aggregate_idx);
}

SljitFullPipelineRecipe SljitProjectionAggregateRecipeBinding::MakeProjectionNativeTailRecipe(
    SljitFullPipelinePrimitiveSequence sequence, idx_t first_projection_idx, idx_t final_projection_idx,
    idx_t tail_start_idx) const {
	if (SljitCanBindProjectedDelimJoinSinkPrimitive(ops, first_projection_idx, final_projection_idx, tail_start_idx)) {
		auto delim_sink =
		    SljitBindProjectedDelimJoinSinkPrimitive(ops, first_projection_idx, final_projection_idx, tail_start_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::DelimJoinSink(delim_sink));
		return MakePrimitiveSequence(std::move(sequence));
	}
	if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)) {
		throw InternalException("SLJIT projection native-tail recipe cannot bind projection chain");
	}
	AddProjectionChainStep(sequence, first_projection_idx, final_projection_idx);
	return MakeNativeTailRecipe(std::move(sequence), tail_start_idx);
}

} // namespace duckdb
