//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_full_pipeline_recipe_binding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

#include "sljit_full_pipeline_primitive_contract.hpp"
#include "sljit_full_pipeline_recipe_facts.hpp"
#include "sljit_full_pipeline_recipe_state.hpp"
#include "sljit_full_pipeline_shape.hpp"
#include "sljit_generated_filter_primitive.hpp"
#include "sljit_projection_chain_runtime.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

#include <utility>

namespace duckdb {

class SljitFullPipelineRecipeBinding {
public:
	SljitFullPipelineRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<LogicalType> &source_output_types_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               bool uses_extended_source_fetch_budget_p)
	    : ops(ops_p), source_output_types(source_output_types_p), source_min_values(source_min_values_p),
	      source_max_values(source_max_values_p),
	      uses_extended_source_fetch_budget(uses_extended_source_fetch_budget_p) {
	}

	SljitFullPipelineRecipePlan MakeNativeOnlyPlan() const {
		return SljitMakeFullPipelineNativeOnlyPlan(uses_extended_source_fetch_budget);
	}

	SljitFullPipelineRecipePlan MakePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) const {
		if (!SljitFullPipelinePrimitiveSequenceIsExecutable(ops, recipe.primitive_sequence)) {
			throw InternalException("SLJIT recipe builder accepted an invalid full-pipeline primitive sequence");
		}
		return SljitMakeFullPipelinePrimitiveRecipePlan(std::move(recipe));
	}

	bool CanMakeNativeTailRecipe(idx_t tail_start_idx) const {
		return SljitNativeTailHandoffCanConsumeTail(ops, tail_start_idx);
	}

	SljitFullPipelineRecipe
	MakeProjectionFilterProjectionNativeTailRecipe(const SljitProjectionFilterProjectionNativeTailFacts &facts) const {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
		auto sequence = MakeSourceSequence();
		AddProjectionChainStep(sequence, facts.pre_projection_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, facts.projection_idx);
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionNativeTailRecipe(const SljitMarkFilterProjectionNativeTailFacts &facts) const {
		auto sequence = MakeMarkFilterPrefix(facts.hash_join_idx, facts.filter_idx, true, facts.first_projection_idx);
		return MakeProjectionNativeTailRecipe(std::move(sequence), facts.first_projection_idx,
		                                      facts.final_projection_idx, facts.tail_start_idx);
	}

	SljitFullPipelineRecipe
	MakeSourceProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const {
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, shape.first_projection_idx);
		return MakeProjectionAggregateRecipe(std::move(sequence), shape, true);
	}

	SljitFullPipelineRecipe MakeSourceBatchNativeTailRecipe(const SljitSourceBatchNativeTailFacts &facts) const {
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(facts.boundary_op_idx));
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe MakeSourceUngroupedAggregateRecipe(const SljitSourceUngroupedAggregateFacts &facts) const {
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, facts.aggregate_idx);
		auto aggregate_update = SljitBindUngroupedAggregateUpdatePrimitive(ops, facts.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::UngroupedAggregateUpdate(aggregate_update));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakeGeneratedFilterProjectionNativeTailRecipe(const SljitGeneratedFilterProjectionNativeTailFacts &facts) const {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, facts.projection_idx);
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe MakeHashJoinDelimJoinSinkRecipe(idx_t first_hash_join_idx, idx_t final_hash_join_idx,
	                                                        idx_t sink_idx) const {
		if (first_hash_join_idx > final_hash_join_idx) {
			throw InternalException("SLJIT hash-join delimiter recipe has an invalid hash-join range");
		}
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, first_hash_join_idx);
		for (idx_t hash_join_idx = first_hash_join_idx; hash_join_idx < final_hash_join_idx; hash_join_idx++) {
			sequence.Add(MakeHashJoinProbeMaterializeStep(hash_join_idx));
		}
		sequence.Add(MakeHashJoinProbeSelectionStep(final_hash_join_idx));
		auto delim_sink = SljitBindSelectedHashJoinDelimJoinSinkPrimitive(ops, final_hash_join_idx, sink_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::DelimJoinSink(delim_sink));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakeJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape, idx_t hash_join_idx) const {
		auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(shape, hash_join_idx);
		auto terminal = MakeJoinProjectionAggregateTerminal(post_join_aggregate);
		auto hash_join_selection =
		    SljitBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx, SourceKey0RangeFitsInt32(hash_join_idx));
		return MakePrimitiveSequence(MakeSourceHashJoinProbeSelectionSequence(hash_join_selection, terminal));
	}

	SljitFullPipelineRecipe MakeSelectedJoinAggregateRecipe(idx_t hash_join_idx, idx_t aggregate_idx) const {
		SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
		post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
		    ops, hash_join_idx, DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
		post_join_aggregate.direct_join_output_aggregate =
		    SljitBindDirectJoinOutputAggregatePrimitive(ops, aggregate_idx);
		auto terminal = SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate));
		auto hash_join_selection = SljitBindHashJoinProbeSelectionPrimitive(
		    ops, hash_join_idx,
		    hash_join_idx == 0 &&
		        SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values));
		return MakePrimitiveSequence(MakeSourceHashJoinProbeSelectionSequence(hash_join_selection, terminal));
	}

	SljitFullPipelineRecipe MakePreProjectionSelectedJoinAggregateRecipe(idx_t pre_projection_idx, idx_t hash_join_idx,
	                                                                     idx_t aggregate_idx) const {
		SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
		post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
		    ops, hash_join_idx, DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
		post_join_aggregate.direct_join_output_aggregate =
		    SljitBindDirectJoinOutputAggregatePrimitive(ops, aggregate_idx);
		auto terminal = SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate));
		SljitPreJoinProjectionViewDescriptor pre_join_view;
		optional_ptr<const SljitPreJoinProjectionViewDescriptor> pre_join_view_ptr;
		if (TryBuildPreJoinProjectionView(pre_projection_idx, hash_join_idx, pre_join_view)) {
			pre_join_view_ptr = pre_join_view;
		}
		auto sequence =
		    MakePreJoinProjectionHashJoinSelectionSequence(pre_projection_idx, hash_join_idx, true, pre_join_view_ptr);
		sequence.Add(terminal);
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe MakeTwoJoinSelectedAggregateRecipe(idx_t first_hash_join_idx, idx_t second_hash_join_idx,
	                                                           idx_t aggregate_idx) const {
		SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
		post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
		    ops, second_hash_join_idx, DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
		post_join_aggregate.direct_join_output_aggregate =
		    SljitBindDirectJoinOutputAggregatePrimitive(ops, aggregate_idx);
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		sequence.Add(SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate)));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakePreProjectionJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                               idx_t hash_join_idx) const {
		D_ASSERT(hash_join_idx > 0);
		auto pre_join_projection_idx = hash_join_idx - 1;
		auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(shape, hash_join_idx);
		SljitPreJoinProjectionViewDescriptor pre_join_view;
		const auto has_pre_join_view =
		    TryBuildPreJoinProjectionView(pre_join_projection_idx, hash_join_idx, pre_join_view);
		SljitStringSetCaseGroupedPayloadProjection string_set_case_projection;
		if (has_pre_join_view &&
		    SljitTryBuildStringSetCaseGroupedPayloadProjection(
		        ops, pre_join_view, post_join_aggregate.post_join_projection.first_projection_idx,
		        post_join_aggregate.post_join_projection.final_projection_idx, string_set_case_projection)) {
			post_join_aggregate.post_join_projection.EnableStringSetCaseGroupedPayload(string_set_case_projection);
		}
		auto terminal = MakeJoinProjectionAggregateTerminal(post_join_aggregate);
		optional_ptr<const SljitPreJoinProjectionViewDescriptor> pre_join_view_ptr;
		if (has_pre_join_view) {
			pre_join_view_ptr = pre_join_view;
		}
		auto sequence =
		    MakePreJoinProjectionHashJoinSelectionSequence(pre_join_projection_idx, hash_join_idx, true, pre_join_view_ptr);
		sequence.Add(terminal);
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe MakeFilterProjectionJoinProjectionAggregateRecipe(
	    const SljitFullPipelineProjectionAggregateShape &shape, idx_t filter_idx, idx_t source_projection_idx,
	    idx_t hash_join_idx) const {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, filter_idx);
		auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(shape, hash_join_idx);
		auto hash_join_selection =
		    SljitBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx, SourceKey0RangeFitsInt32(hash_join_idx));
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, source_projection_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(hash_join_selection));
		sequence.Add(MakeJoinProjectionAggregateTerminal(post_join_aggregate));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe MakeSingleJoinProjectionAggregateTailRecipe(
	    const SljitFullPipelineProjectionAggregateShape &shape, idx_t source_filter_idx, idx_t source_projection_idx,
	    idx_t pre_join_projection_idx, idx_t hash_join_idx) const {
		auto sequence = MakeSourceSequence();
		const bool has_source_filter_projection =
		    source_filter_idx != DConstants::INVALID_INDEX && source_projection_idx != DConstants::INVALID_INDEX;
		if (has_source_filter_projection) {
			auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, source_filter_idx);
			sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
			AddProjectionChainStep(sequence, source_projection_idx);
		}
		if (pre_join_projection_idx != DConstants::INVALID_INDEX) {
			if (!has_source_filter_projection) {
				if (TryAppendElidedPreJoinHashJoinProbeSelection(sequence, pre_join_projection_idx, hash_join_idx,
				                                                 false)) {
					return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
				}
			}
			AddProjectionChainStep(sequence, pre_join_projection_idx);
		} else if (!has_source_filter_projection) {
			AddSourceBatchBoundaryIfUseful(sequence, hash_join_idx);
		}
		sequence.Add(MakeHashJoinProbeProjectionInputStep(hash_join_idx));
		return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeSingleJoinProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                            idx_t hash_join_idx) const {
		return MakeSingleJoinProjectionAggregateTailRecipe(shape, DConstants::INVALID_INDEX, DConstants::INVALID_INDEX,
		                                                   DConstants::INVALID_INDEX, hash_join_idx);
	}

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape, idx_t hash_join_idx,
	                                        idx_t filter_idx) const {
		auto sequence = MakeMarkFilterPrefix(hash_join_idx, filter_idx, true, shape.first_projection_idx);
		return MakeProjectionAggregateRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe MakeMarkFilterNativeTailRecipe(idx_t hash_join_idx, idx_t filter_idx) const {
		auto sequence = MakeMarkFilterPrefix(hash_join_idx, filter_idx, true, DConstants::INVALID_INDEX, true);
		return MakeNativeTailRecipe(std::move(sequence), filter_idx + 1);
	}

	bool ProjectionAggregateHasDedicatedBackend(
	    const SljitFullPipelineProjectionAggregateShape &shape,
	    bool allow_direct_projected_primitive_payload_update = false) const {
		if (SljitCanBindUngroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx)) {
			return SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx,
			                                            shape.final_projection_idx);
		}
		return ProjectionGroupedAggregateHasDedicatedBackend(shape, allow_direct_projected_primitive_payload_update);
	}

	bool CanMakeProjectionAggregateTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const {
		return ProjectionAggregateHasDedicatedBackend(shape) || CanMakeNativeTailRecipe(shape.aggregate_idx);
	}

	SljitFullPipelineRecipe
	MakeBetweenProjectionTwoJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                      idx_t first_hash_join_idx, idx_t between_projection_idx,
	                                                      idx_t second_hash_join_idx) const {
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		AddProjectionChainStep(sequence, between_projection_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeBetweenProjectionTwoJoinDirectAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                  idx_t first_hash_join_idx, idx_t between_projection_idx,
	                                                  idx_t second_hash_join_idx) const {
		auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(shape, second_hash_join_idx);
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		AddProjectionChainStep(sequence, between_projection_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		sequence.Add(SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate)));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakePreProjectionTwoJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                  idx_t pre_join_projection_idx, idx_t first_hash_join_idx,
	                                                  idx_t between_projection_idx, idx_t second_hash_join_idx) const {
		auto sequence = MakeSourceSequence();
		AddProjectionChainStep(sequence, pre_join_projection_idx);
		sequence.Add(MakeHashJoinProbeProjectionInputStep(first_hash_join_idx));
		AddProjectionChainStep(sequence, between_projection_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeTwoJoinDirectProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                           idx_t first_hash_join_idx) const {
		const auto second_hash_join_idx = first_hash_join_idx + 1;
		auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(shape, second_hash_join_idx);
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		sequence.Add(SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate)));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakeTwoJoinProjectionChainAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                          idx_t first_hash_join_idx, idx_t second_hash_join_idx) const {
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeFilterProjectionTwoJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                     idx_t filter_idx, idx_t source_projection_idx,
	                                                     idx_t first_hash_join_idx, idx_t second_hash_join_idx) const {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, filter_idx);
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		AddProjectionChainStep(sequence, source_projection_idx);
		sequence.Add(MakeHashJoinProbeProjectionInputStep(first_hash_join_idx));
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeTwoJoinMarkFilterProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                               idx_t first_hash_join_idx, idx_t second_hash_join_idx,
	                                               idx_t filter_idx) const {
		auto sequence = MakeTwoJoinMarkFilterPrefix(first_hash_join_idx, second_hash_join_idx, filter_idx, true,
		                                            shape.first_projection_idx);
		return MakeProjectionAggregateRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeTwoJoinMarkFilterNativeTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                      idx_t first_hash_join_idx, idx_t second_hash_join_idx,
	                                      idx_t filter_idx) const {
		auto sequence = MakeTwoJoinMarkFilterPrefix(first_hash_join_idx, second_hash_join_idx, filter_idx, true,
		                                            DConstants::INVALID_INDEX, true);
		return MakeNativeTailRecipe(std::move(sequence), filter_idx + 1);
	}

private:
	SljitFullPipelineRecipe MakePrimitiveSequence(std::initializer_list<SljitFullPipelinePrimitiveStep> steps) const {
		return SljitMakeFullPipelinePrimitiveRecipe(uses_extended_source_fetch_budget, steps);
	}

	SljitFullPipelineRecipe MakePrimitiveSequence(SljitFullPipelinePrimitiveSequence sequence) const {
		return SljitMakeFullPipelinePrimitiveRecipe(uses_extended_source_fetch_budget, std::move(sequence));
	}

	SljitFullPipelinePrimitiveSequence MakeSourceSequence() const {
		SljitFullPipelinePrimitiveSequence sequence;
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceFetch());
		return sequence;
	}

	SljitFullPipelinePrimitiveStep MakeProjectionChainStep(idx_t projection_idx) const {
		auto projection = SljitBindProjectionChainPrimitive(ops, projection_idx);
		return SljitFullPipelinePrimitiveStep::ProjectionChain(projection);
	}

	SljitFullPipelinePrimitiveStep MakeProjectionChainStep(idx_t first_projection_idx,
	                                                       idx_t final_projection_idx) const {
		auto projection = SljitBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx);
		return SljitFullPipelinePrimitiveStep::ProjectionChain(projection);
	}

	void AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence, idx_t projection_idx) const {
		sequence.Add(MakeProjectionChainStep(projection_idx));
	}

	void AddProjectionChainStep(SljitFullPipelinePrimitiveSequence &sequence, idx_t first_projection_idx,
	                            idx_t final_projection_idx) const {
		sequence.Add(MakeProjectionChainStep(first_projection_idx, final_projection_idx));
	}

	SljitFullPipelineRecipe MakeNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                             idx_t tail_start_idx) const {
		if (tail_start_idx >= ops.size()) {
			throw InternalException("SLJIT native-tail recipe has an invalid tail start operator");
		}
		if (!CanMakeNativeTailRecipe(tail_start_idx)) {
			throw InternalException("SLJIT native-tail recipe cannot consume generated aggregate payload tail");
		}
		sequence.Add(SljitFullPipelinePrimitiveStep::NativeTailHandoff(tail_start_idx));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeMaterializeStep(idx_t hash_join_idx) const {
		const auto source_key0_int64_to_int32_unchecked =
		    hash_join_idx == 0 &&
		    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
		auto primitive =
		    SljitBindHashJoinProbeMaterializePrimitive(ops, hash_join_idx, source_key0_int64_to_int32_unchecked);
		return SljitFullPipelinePrimitiveStep::HashJoinProbeMaterialize(primitive);
	}

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeSelectionStep(idx_t hash_join_idx) const {
		const auto source_key0_int64_to_int32_unchecked =
		    hash_join_idx == 0 &&
		    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
		auto primitive =
		    SljitBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx, source_key0_int64_to_int32_unchecked);
		return SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(primitive);
	}

	SljitFullPipelinePrimitiveStep MakeHashJoinProbeProjectionInputStep(idx_t hash_join_idx) const {
		if (SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
			return MakeHashJoinProbeSelectionStep(hash_join_idx);
		}
		return MakeHashJoinProbeMaterializeStep(hash_join_idx);
	}

	SljitFullPipelinePrimitiveStep
	MakeMarkProbeFilterBoundaryStep(idx_t hash_join_idx, idx_t filter_idx, bool apply_filter_selection,
	                                idx_t downstream_projection_idx = DConstants::INVALID_INDEX,
	                                bool materialize_filter_selection = false) const {
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
		auto primitive = SljitBindMarkProbeFilterBoundaryPrimitive(ops, hash_join_idx, filter_idx,
		                                                           apply_filter_selection, downstream_projection_idx,
		                                                           allow_marker_omission, materialize_filter_selection);
		return SljitFullPipelinePrimitiveStep::MarkProbeFilterBoundary(primitive);
	}

	SljitFullPipelinePrimitiveSequence
	MakeMarkFilterPrefix(idx_t hash_join_idx, idx_t filter_idx, bool apply_filter_selection,
	                     idx_t downstream_projection_idx = DConstants::INVALID_INDEX,
	                     bool materialize_filter_selection = false) const {
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, hash_join_idx);
		sequence.Add(MakeMarkProbeFilterBoundaryStep(hash_join_idx, filter_idx, apply_filter_selection,
		                                             downstream_projection_idx, materialize_filter_selection));
		return sequence;
	}

	SljitFullPipelinePrimitiveSequence MakeTwoJoinMarkFilterPrefix(
	    idx_t first_hash_join_idx, idx_t second_hash_join_idx, idx_t filter_idx, bool apply_filter_selection,
	    idx_t downstream_projection_idx = DConstants::INVALID_INDEX, bool materialize_filter_selection = false) const {
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		sequence.Add(MakeMarkProbeFilterBoundaryStep(second_hash_join_idx, filter_idx, apply_filter_selection,
		                                             downstream_projection_idx, materialize_filter_selection));
		return sequence;
	}

	SljitFullPipelinePrimitiveSequence
	MakeSourceHashJoinProbeSelectionSequence(const SljitHashJoinProbeSelectionPrimitive &hash_join_selection,
	                                         const SljitFullPipelinePrimitiveStep &terminal) const {
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, hash_join_selection.hash_join_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(hash_join_selection));
		sequence.Add(terminal);
		return sequence;
	}

	SljitFullPipelinePrimitiveSequence
	MakePreJoinProjectionHashJoinSelectionSequence(idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                                               bool add_source_batch_boundary_for_elided,
	                                               optional_ptr<const SljitPreJoinProjectionViewDescriptor>
	                                                   pre_join_view = nullptr) const {
		auto sequence = MakeSourceSequence();
		if (pre_join_view &&
		    TryAppendElidedPreJoinHashJoinProbeSelection(sequence, *pre_join_view, pre_join_projection_idx,
		                                                 hash_join_idx, add_source_batch_boundary_for_elided)) {
			return sequence;
		}
		if (!pre_join_view &&
		    TryAppendElidedPreJoinHashJoinProbeSelection(sequence, pre_join_projection_idx, hash_join_idx,
		                                                 add_source_batch_boundary_for_elided)) {
			return sequence;
		}
		AddMaterializedPreJoinProjectionHashJoinSelection(sequence, pre_join_projection_idx, hash_join_idx);
		return sequence;
	}

	SljitFullPipelinePrimitiveSequence MakeSourceHashJoinProjectionInputSequence(idx_t hash_join_idx) const {
		auto sequence = MakeSourceSequence();
		AddSourceBatchBoundaryIfUseful(sequence, hash_join_idx);
		sequence.Add(MakeHashJoinProbeProjectionInputStep(hash_join_idx));
		return sequence;
	}

	bool TryBuildPreJoinProjectionView(idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                                   SljitPreJoinProjectionViewDescriptor &pre_join_view) const {
		return SljitTryBuildPreJoinProjectionViewDescriptor(ops, pre_join_projection_idx, hash_join_idx,
		                                                    source_min_values, source_max_values, pre_join_view);
	}

	bool TryAppendElidedPreJoinHashJoinProbeSelection(SljitFullPipelinePrimitiveSequence &sequence,
	                                                  idx_t pre_join_projection_idx, idx_t hash_join_idx,
	                                                  bool add_source_batch_boundary) const {
		SljitPreJoinProjectionViewDescriptor pre_join_view;
		if (!TryBuildPreJoinProjectionView(pre_join_projection_idx, hash_join_idx, pre_join_view)) {
			return false;
		}
		return TryAppendElidedPreJoinHashJoinProbeSelection(sequence, pre_join_view, pre_join_projection_idx,
		                                                    hash_join_idx, add_source_batch_boundary);
	}

	bool TryAppendElidedPreJoinHashJoinProbeSelection(
	    SljitFullPipelinePrimitiveSequence &sequence, const SljitPreJoinProjectionViewDescriptor &pre_join_view,
	    idx_t pre_join_projection_idx, idx_t hash_join_idx, bool add_source_batch_boundary) const {
		if (!pre_join_view.CanElideProjectionWithCurrentHashProbe()) {
			return false;
		}
		if (add_source_batch_boundary) {
			AddSourceBatchBoundaryIfUseful(sequence, hash_join_idx);
		}
		auto remapped_hash_join_selection =
		    BindElidedPreJoinHashJoinProbeSelection(pre_join_view, pre_join_projection_idx, hash_join_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(remapped_hash_join_selection));
		return true;
	}

	void AddMaterializedPreJoinProjectionHashJoinSelection(SljitFullPipelinePrimitiveSequence &sequence,
	                                                       idx_t pre_join_projection_idx, idx_t hash_join_idx) const {
		AddProjectionChainStep(sequence, pre_join_projection_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(hash_join_idx));
	}

	SljitHashJoinProbeSelectionPrimitive
	BindElidedPreJoinHashJoinProbeSelection(const SljitPreJoinProjectionViewDescriptor &pre_join_view,
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

	SljitPostJoinProjectionAggregatePrimitive
	BindPostJoinProjectionAggregatePrimitive(const SljitFullPipelineProjectionAggregateShape &shape,
	                                         idx_t hash_join_idx) const {
		SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
		post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
		    ops, hash_join_idx, shape.first_projection_idx, shape.final_projection_idx);
		post_join_aggregate.direct_join_output_aggregate =
		    SljitBindDirectJoinOutputAggregatePrimitive(ops, shape.aggregate_idx);
		return post_join_aggregate;
	}

	SljitFullPipelinePrimitiveStep
	MakeJoinProjectionAggregateTerminal(const SljitPostJoinProjectionAggregatePrimitive &post_join_aggregate) const {
		return SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate));
	}

	bool SourceKey0RangeFitsInt32(idx_t hash_join_idx) const {
		return hash_join_idx == 0 &&
		       SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
	}

	bool SourceBatchBoundaryCanCoalesce(idx_t consumer_op_idx) const {
		if (source_output_types.empty() || consumer_op_idx >= ops.size()) {
			return false;
		}
		for (auto &type : source_output_types) {
			if (!TypeIsConstantSize(type.InternalType())) {
				return false;
			}
		}
		return true;
	}

	void AddSourceBatchBoundaryIfUseful(SljitFullPipelinePrimitiveSequence &sequence, idx_t consumer_op_idx) const {
		if (SourceBatchBoundaryCanCoalesce(consumer_op_idx)) {
			sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(consumer_op_idx));
		}
	}

	bool ProjectionGroupedAggregateHasDedicatedBackend(
	    const SljitFullPipelineProjectionAggregateShape &shape,
	    bool allow_direct_projected_primitive_payload_update = false) const {
		if (!SljitGroupedAggregateUpdateHasDedicatedBackend(ops, shape.aggregate_idx)) {
			return false;
		}
		if (SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(
		        ops, shape.first_projection_idx, shape.final_projection_idx, shape.aggregate_idx,
		        allow_direct_projected_primitive_payload_update)) {
			return true;
		}
		return SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx);
	}

	SljitFullPipelineRecipe
	MakeProjectionAggregateRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                              const SljitFullPipelineProjectionAggregateShape &shape,
	                              bool allow_direct_projected_primitive_payload_update = false) const {
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

	SljitFullPipelineRecipe
	MakeProjectionGroupedAggregateRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                     const SljitFullPipelineProjectionAggregateShape &shape,
	                                     bool allow_direct_projected_primitive_payload_update = false) const {
		if (!SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx)) {
			throw InternalException("SLJIT selected projection aggregate recipe cannot bind projection chain");
		}
		if (SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(
		        ops, shape.first_projection_idx, shape.final_projection_idx, shape.aggregate_idx,
		        allow_direct_projected_primitive_payload_update)) {
			auto grouped_update = SljitBindProjectedInputGroupedAggregateUpdatePrimitive(
			    ops, shape.first_projection_idx, shape.final_projection_idx, shape.aggregate_idx,
			    allow_direct_projected_primitive_payload_update);
			sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update));
			return MakePrimitiveSequence(std::move(sequence));
		}
		AddProjectionChainStep(sequence, shape.first_projection_idx, shape.final_projection_idx);
		auto grouped_update = SljitBindGroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakeProjectionAggregateTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                  const SljitFullPipelineProjectionAggregateShape &shape) const {
		if (ProjectionAggregateHasDedicatedBackend(shape)) {
			return MakeProjectionAggregateRecipe(std::move(sequence), shape);
		}
		if (!CanMakeNativeTailRecipe(shape.aggregate_idx)) {
			throw InternalException("SLJIT projection aggregate tail has no valid grouped backend or native tail");
		}
		return MakeProjectionNativeTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeProjectionNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                               const SljitFullPipelineProjectionAggregateShape &shape) const {
		return MakeProjectionNativeTailRecipe(std::move(sequence), shape.first_projection_idx,
		                                      shape.final_projection_idx, shape.aggregate_idx);
	}

	SljitFullPipelineRecipe MakeProjectionNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                                       idx_t first_projection_idx, idx_t final_projection_idx,
	                                                       idx_t tail_start_idx) const {
		if (SljitCanBindProjectedDelimJoinSinkPrimitive(ops, first_projection_idx, final_projection_idx,
		                                                tail_start_idx)) {
			auto delim_sink = SljitBindProjectedDelimJoinSinkPrimitive(ops, first_projection_idx, final_projection_idx,
			                                                           tail_start_idx);
			sequence.Add(SljitFullPipelinePrimitiveStep::DelimJoinSink(delim_sink));
			return MakePrimitiveSequence(std::move(sequence));
		}
		if (!SljitCanBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx)) {
			throw InternalException("SLJIT projection native-tail recipe cannot bind projection chain");
		}
		AddProjectionChainStep(sequence, first_projection_idx, final_projection_idx);
		return MakeNativeTailRecipe(std::move(sequence), tail_start_idx);
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	const vector<LogicalType> &source_output_types;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	bool uses_extended_source_fetch_budget;
};

} // namespace duckdb
