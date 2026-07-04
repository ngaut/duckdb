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
#include "sljit_generated_filter_projection_runtime.hpp"
#include "sljit_native_tail_handoff_runtime.hpp"
#include "sljit_projection_chain_runtime.hpp"
#include "sljit_string_set_case_projection_runtime.hpp"

#include <utility>

namespace duckdb {

class SljitFullPipelineRecipeBinding {
public:
	SljitFullPipelineRecipeBinding(const vector<SljitExecutableRegionOp> &ops_p,
	                               const vector<Value> &source_min_values_p, const vector<Value> &source_max_values_p,
	                               bool uses_scan_filters_p)
	    : ops(ops_p), source_min_values(source_min_values_p), source_max_values(source_max_values_p),
	      uses_scan_filters(uses_scan_filters_p) {
	}

	SljitFullPipelineRecipePlan MakeNativeOnlyPlan() const {
		return SljitMakeFullPipelineNativeOnlyPlan(UsesExtendedSourceFetchBudget());
	}

	SljitFullPipelineRecipePlan MakePrimitiveRecipePlan(SljitFullPipelineRecipe recipe) const {
		if (!SljitFullPipelinePrimitiveSequenceIsExecutable(ops, recipe.primitive_sequence)) {
			throw InternalException("SLJIT recipe builder accepted an invalid full-pipeline primitive sequence");
		}
		return SljitMakeFullPipelinePrimitiveRecipePlan(std::move(recipe));
	}

	SljitFullPipelineRecipe
	MakeProjectionFilterProjectionNativeTailRecipe(const SljitProjectionFilterProjectionNativeTailFacts &facts) const {
		auto pre_projection = SljitBindProjectionChainPrimitive(ops, facts.pre_projection_idx);
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
		auto projection = SljitBindProjectionChainPrimitive(ops, facts.projection_idx);
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(pre_projection));
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionNativeTailRecipe(const SljitMarkFilterProjectionNativeTailFacts &facts) const {
		auto sequence = MakeMarkFilterPrefix(facts.hash_join_idx, facts.filter_idx, true, facts.first_projection_idx);
		return MakeProjectionNativeTailRecipe(std::move(sequence), facts.first_projection_idx,
		                                      facts.final_projection_idx, facts.tail_start_idx);
	}

	SljitFullPipelineRecipe
	MakeSourceProjectionGroupedAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape) const {
		auto sequence = MakeSourceSequence();
		return MakeProjectionGroupedAggregateRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe MakeSourceBatchNativeTailRecipe(const SljitSourceBatchNativeTailFacts &facts) const {
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(facts.boundary_op_idx));
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe
	MakeGeneratedFilterProjectionNativeTailRecipe(const SljitGeneratedFilterProjectionNativeTailFacts &facts) const {
		auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, facts.filter_idx);
		auto projection_chain = SljitBindProjectionChainPrimitive(ops, facts.projection_idx);
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection_chain));
		return MakeNativeTailRecipe(std::move(sequence), facts.tail_start_idx);
	}

	SljitFullPipelineRecipe MakeHashJoinDelimJoinSinkRecipe(idx_t first_hash_join_idx, idx_t final_hash_join_idx,
	                                                        idx_t sink_idx) const {
		if (first_hash_join_idx > final_hash_join_idx) {
			throw InternalException("SLJIT hash-join delimiter recipe has an invalid hash-join range");
		}
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(first_hash_join_idx));
		for (idx_t hash_join_idx = first_hash_join_idx; hash_join_idx < final_hash_join_idx; hash_join_idx++) {
			sequence.Add(MakeHashJoinProbeMaterializeStep(hash_join_idx));
		}
		sequence.Add(MakeHashJoinProbeSelectionStep(final_hash_join_idx));
		auto delim_sink = SljitBindSelectedHashJoinDelimJoinSinkPrimitive(ops, final_hash_join_idx, sink_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::DelimJoinSink(delim_sink));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakeJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape, idx_t hash_join_idx,
	                                  SljitDirectJoinOutputAggregateUpdateSchedule update_schedule) const {
		auto primitive = BindJoinProjectionAggregatePrimitive(shape, hash_join_idx, update_schedule);
		return MakeJoinProjectionAggregateRecipe(primitive);
	}

	SljitFullPipelineRecipe MakeSelectedJoinAggregateRecipe(idx_t hash_join_idx, idx_t aggregate_idx) const {
		SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
		post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
		    ops, hash_join_idx, DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
		post_join_aggregate.direct_join_output_aggregate = SljitBindDirectJoinOutputAggregatePrimitive(
		    ops, aggregate_idx, SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
		auto terminal = SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate));
		auto hash_join_selection = SljitBindHashJoinProbeSelectionPrimitive(
		    ops, hash_join_idx,
		    hash_join_idx == 0 &&
		        SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values));
		return MakePrimitiveSequence(MakeSourceHashJoinProbeSelectionSequence(hash_join_selection, terminal));
	}

	SljitFullPipelineRecipe MakeTwoJoinSelectedAggregateRecipe(idx_t first_hash_join_idx, idx_t second_hash_join_idx,
	                                                           idx_t aggregate_idx) const {
		SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
		post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
		    ops, second_hash_join_idx, DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
		post_join_aggregate.direct_join_output_aggregate = SljitBindDirectJoinOutputAggregatePrimitive(
		    ops, aggregate_idx, SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		sequence.Add(SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate)));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakePreProjectionJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                               idx_t hash_join_idx,
	                                               SljitDirectJoinOutputAggregateUpdateSchedule update_schedule) const {
		D_ASSERT(hash_join_idx > 0);
		auto primitive = BindJoinProjectionAggregatePrimitive(shape, hash_join_idx, update_schedule);
		auto pre_join_projection_idx = hash_join_idx - 1;
		primitive.input_kind = SljitJoinProjectionAggregateSourceKind::PRE_JOIN_PROJECTION;
		primitive.pre_join_projection_idx = pre_join_projection_idx;
		SljitTryBuildInt64ToInt32PreJoinProjection(ops, pre_join_projection_idx, hash_join_idx, source_min_values,
		                                           source_max_values, primitive.pre_join_projection);
		SljitStringSetCaseGroupedPayloadProjection string_set_case_projection;
		if (SljitTryBuildStringSetCaseGroupedPayloadProjection(
		        ops, primitive.pre_join_projection, primitive.post_join_projection.first_projection_idx,
		        primitive.post_join_projection.final_projection_idx, string_set_case_projection)) {
			primitive.post_join_projection.EnableStringSetCaseGroupedPayload(string_set_case_projection);
		}
		return MakeJoinProjectionAggregateRecipe(primitive);
	}

	SljitFullPipelineRecipe MakeFilterProjectionJoinProjectionAggregateRecipe(
	    const SljitFullPipelineProjectionAggregateShape &shape, idx_t filter_idx, idx_t source_projection_idx,
	    idx_t hash_join_idx, SljitDirectJoinOutputAggregateUpdateSchedule update_schedule) const {
		auto primitive = BindJoinProjectionAggregatePrimitive(shape, hash_join_idx, update_schedule);
		primitive.input_kind = SljitJoinProjectionAggregateSourceKind::FILTER_PROJECTION;
		primitive.filter_idx = filter_idx;
		primitive.source_projection_idx = source_projection_idx;
		return MakeJoinProjectionAggregateRecipe(primitive);
	}

	SljitFullPipelineRecipe
	MakeMarkFilterProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape, idx_t hash_join_idx,
	                                        idx_t filter_idx) const {
		auto sequence = MakeMarkFilterPrefix(hash_join_idx, filter_idx, true, shape.first_projection_idx);
		return MakeProjectionGroupedAggregateRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe MakeMarkFilterNativeTailRecipe(idx_t hash_join_idx, idx_t filter_idx) const {
		auto sequence = MakeMarkFilterPrefix(hash_join_idx, filter_idx, false);
		return MakeNativeTailRecipe(std::move(sequence), filter_idx);
	}

	bool SelectedProjectionAggregateHasDedicatedBackend(const SljitFullPipelineProjectionAggregateShape &shape) const {
		return SljitGroupedAggregateUpdateHasDedicatedBackend(ops, shape.aggregate_idx);
	}

	bool SelectedProjectionAggregateCanConsumeHashJoinSelection(
	    const SljitFullPipelineProjectionAggregateShape &shape) const {
		if (!SljitCanBindGroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx)) {
			return false;
		}
		return SljitChooseGroupedAggregateUpdateStrategy(ops[shape.aggregate_idx]) ==
		       SljitGroupedAggregateUpdateStrategyKind::DISTINCT_COUNT_POINTER;
	}

	SljitFullPipelineRecipe
	MakeBetweenProjectionTwoJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                      idx_t first_hash_join_idx, idx_t between_projection_idx,
	                                                      idx_t second_hash_join_idx) const {
		auto between_projection = SljitBindProjectionChainPrimitive(ops, between_projection_idx);
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(between_projection));
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeBetweenProjectionTwoJoinDirectAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                  idx_t first_hash_join_idx, idx_t between_projection_idx,
	                                                  idx_t second_hash_join_idx) const {
		auto between_projection = SljitBindProjectionChainPrimitive(ops, between_projection_idx);
		auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(
		    shape, second_hash_join_idx, SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(between_projection));
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		sequence.Add(SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate)));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakePreProjectionTwoJoinProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                                  idx_t pre_join_projection_idx, idx_t first_hash_join_idx,
	                                                  idx_t between_projection_idx, idx_t second_hash_join_idx) const {
		auto pre_join_projection = SljitBindProjectionChainPrimitive(ops, pre_join_projection_idx);
		auto between_projection = SljitBindProjectionChainPrimitive(ops, between_projection_idx);
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(pre_join_projection));
		sequence.Add(MakeHashJoinProbeProjectionInputStep(first_hash_join_idx));
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(between_projection));
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeTwoJoinDirectProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                           idx_t first_hash_join_idx) const {
		const auto second_hash_join_idx = first_hash_join_idx + 1;
		auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(
		    shape, second_hash_join_idx, SljitDirectJoinOutputAggregateUpdateSchedule::PENDING_ROW_POINTER_BATCH);
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
		auto source_projection = SljitBindProjectionChainPrimitive(ops, source_projection_idx);
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter));
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(source_projection));
		sequence.Add(MakeHashJoinProbeProjectionInputStep(first_hash_join_idx));
		sequence.Add(MakeHashJoinProbeSelectionStep(second_hash_join_idx));
		return MakeProjectionAggregateTailRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeTwoJoinMarkFilterProjectionAggregateRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                               idx_t first_hash_join_idx, idx_t second_hash_join_idx,
	                                               idx_t filter_idx) const {
		const bool preserve_selected_hash_join =
		    SljitCanBindHashJoinProbeSelectionPrimitive(ops, first_hash_join_idx) &&
		    SelectedProjectionAggregateCanConsumeHashJoinSelection(shape);
		auto sequence = MakeTwoJoinMarkFilterPrefix(first_hash_join_idx, second_hash_join_idx, filter_idx, true,
		                                            shape.first_projection_idx, preserve_selected_hash_join);
		if (preserve_selected_hash_join) {
			auto primitive = BindJoinProjectionAggregatePrimitive(
			    shape, first_hash_join_idx, SljitDirectJoinOutputAggregateUpdateSchedule::IMMEDIATE_ROW_POINTER_UPDATE);
			sequence.Add(SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
			    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(
			        BindPostJoinProjectionAggregatePrimitive(primitive))));
			return MakePrimitiveSequence(std::move(sequence));
		}
		return MakeProjectionGroupedAggregateRecipe(std::move(sequence), shape);
	}

	SljitFullPipelineRecipe
	MakeTwoJoinMarkFilterNativeTailRecipe(const SljitFullPipelineProjectionAggregateShape &shape,
	                                      idx_t first_hash_join_idx, idx_t second_hash_join_idx,
	                                      idx_t filter_idx) const {
		auto sequence = MakeTwoJoinMarkFilterPrefix(first_hash_join_idx, second_hash_join_idx, filter_idx, false);
		return MakeNativeTailRecipe(std::move(sequence), filter_idx);
	}

private:
	bool UsesExtendedSourceFetchBudget() const {
		if (UsesScanFilteredAggregateTerminal()) {
			return true;
		}
		bool has_hash_join_probe = false;
		for (auto &op : ops) {
			if (op.kind == SljitNativeRegionOpKind::HASH_JOIN_PROBE) {
				has_hash_join_probe = true;
				continue;
			}
			if (op.kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE) {
				return has_hash_join_probe &&
				       op.aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE;
			}
		}
		if (ops.empty()) {
			return false;
		}
		switch (ops.back().kind) {
		case SljitNativeRegionOpKind::HASH_JOIN_BUILD:
		case SljitNativeRegionOpKind::DELIM_JOIN_SINK:
			return true;
		default:
			return false;
		}
	}

	bool UsesScanFilteredAggregateTerminal() const {
		return uses_scan_filters && !ops.empty() && ops.back().kind == SljitNativeRegionOpKind::AGGREGATE_UPDATE &&
		       (ops.back().aggregate_update.plan.sink_info.kind ==
		            ExecutionRegionSinkKind::UNGROUPED_AGGREGATE_UPDATE ||
		        ops.back().aggregate_update.plan.sink_info.kind == ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE);
	}

	SljitFullPipelineRecipe MakePrimitiveSequence(std::initializer_list<SljitFullPipelinePrimitiveStep> steps) const {
		return SljitMakeFullPipelinePrimitiveRecipe(UsesExtendedSourceFetchBudget(), steps);
	}

	SljitFullPipelineRecipe MakePrimitiveSequence(SljitFullPipelinePrimitiveSequence sequence) const {
		return SljitMakeFullPipelinePrimitiveRecipe(UsesExtendedSourceFetchBudget(), std::move(sequence));
	}

	SljitFullPipelinePrimitiveSequence MakeSourceSequence() const {
		SljitFullPipelinePrimitiveSequence sequence;
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceFetch());
		return sequence;
	}

	SljitFullPipelineRecipe MakeNativeTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                             idx_t tail_start_idx) const {
		SljitBindNativeTailHandoffPrimitive(ops, tail_start_idx);
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
	                                bool preserve_selected_hash_join = false) const {
		bool allow_marker_omission = false;
		if (apply_filter_selection && downstream_projection_idx != DConstants::INVALID_INDEX) {
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
		                                                           preserve_selected_hash_join, allow_marker_omission);
		return SljitFullPipelinePrimitiveStep::MarkProbeFilterBoundary(primitive);
	}

	SljitFullPipelinePrimitiveSequence
	MakeMarkFilterPrefix(idx_t hash_join_idx, idx_t filter_idx, bool apply_filter_selection,
	                     idx_t downstream_projection_idx = DConstants::INVALID_INDEX) const {
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(hash_join_idx));
		sequence.Add(MakeMarkProbeFilterBoundaryStep(hash_join_idx, filter_idx, apply_filter_selection,
		                                             downstream_projection_idx));
		return sequence;
	}

	SljitFullPipelinePrimitiveSequence MakeTwoJoinMarkFilterPrefix(
	    idx_t first_hash_join_idx, idx_t second_hash_join_idx, idx_t filter_idx, bool apply_filter_selection,
	    idx_t downstream_projection_idx = DConstants::INVALID_INDEX, bool preserve_selected_hash_join = false) const {
		auto sequence = MakeSourceHashJoinProjectionInputSequence(first_hash_join_idx);
		sequence.Add(MakeMarkProbeFilterBoundaryStep(second_hash_join_idx, filter_idx, apply_filter_selection,
		                                             downstream_projection_idx, preserve_selected_hash_join));
		return sequence;
	}

	SljitJoinProjectionAggregatePrimitive
	BindJoinProjectionAggregatePrimitive(const SljitFullPipelineProjectionAggregateShape &shape, idx_t hash_join_idx,
	                                     SljitDirectJoinOutputAggregateUpdateSchedule update_schedule) const {
		SljitJoinProjectionAggregatePrimitive primitive;
		primitive.post_join_projection = SljitBindPostJoinProjectionPrimitive(
		    ops, hash_join_idx, shape.first_projection_idx, shape.final_projection_idx);
		primitive.direct_join_output_aggregate =
		    SljitBindDirectJoinOutputAggregatePrimitive(ops, shape.aggregate_idx, update_schedule);
		primitive.source_key0_int64_to_int32_unchecked =
		    hash_join_idx == 0 &&
		    SljitHashJoinSourceKey0RangeFitsInt32(ops, hash_join_idx, source_min_values, source_max_values);
		return primitive;
	}

	SljitFullPipelineRecipe
	MakeJoinProjectionAggregateRecipe(const SljitJoinProjectionAggregatePrimitive &primitive) const {
		const auto hash_join_idx = primitive.post_join_projection.hash_join_idx;
		if (!SljitCanBindHashJoinProbeSelectionPrimitive(ops, hash_join_idx)) {
			throw InternalException("SLJIT join-projection aggregate recipe requires a selection hash join probe");
		}
		auto post_join_aggregate = BindPostJoinProjectionAggregatePrimitive(primitive);
		auto hash_join_selection = SljitBindHashJoinProbeSelectionPrimitive(
		    ops, hash_join_idx, primitive.source_key0_int64_to_int32_unchecked);
		auto terminal = SljitFullPipelinePrimitiveStep::JoinProjectionAggregateUpdate(
		    SljitMakeSelectedJoinOutputAggregateUpdatePrimitive(post_join_aggregate));
		switch (primitive.input_kind) {
		case SljitJoinProjectionAggregateSourceKind::SOURCE_CHUNK:
			return MakePrimitiveSequence(MakeSourceHashJoinProbeSelectionSequence(hash_join_selection, terminal));
		case SljitJoinProjectionAggregateSourceKind::PRE_JOIN_PROJECTION: {
			SljitPreJoinProjectionViewDescriptor pre_join_view;
			if (SljitTryBuildPreJoinProjectionViewDescriptor(ops, primitive.pre_join_projection_idx, hash_join_idx,
			                                                 source_min_values, source_max_values, pre_join_view) &&
			    pre_join_view.CanElideProjectionWithCurrentHashProbe()) {
				SljitHashJoinProbeInputRemap input_remap;
				if (!pre_join_view.hash_probe_key_inputs_match_source) {
					input_remap.key_input_indices = pre_join_view.hash_probe_key_source_indices;
				}
				input_remap.residual_probe_source_indices = pre_join_view.residual_probe_source_indices;
				auto remapped_hash_join_selection = SljitBindHashJoinProbeSelectionPrimitive(
				    ops, hash_join_idx, pre_join_view.source_key0_int64_to_int32_unchecked, std::move(input_remap),
				    pre_join_view.projected_to_source, primitive.pre_join_projection_idx);
				return MakePrimitiveSequence(
				    MakeSourceHashJoinProbeSelectionSequence(remapped_hash_join_selection, terminal));
			}
			auto pre_join_projection = SljitBindProjectionChainPrimitive(ops, primitive.pre_join_projection_idx);
			return MakePrimitiveSequence({SljitFullPipelinePrimitiveStep::SourceFetch(),
			                              SljitFullPipelinePrimitiveStep::ProjectionChain(pre_join_projection),
			                              SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(hash_join_selection),
			                              terminal});
		}
		case SljitJoinProjectionAggregateSourceKind::FILTER_PROJECTION: {
			auto generated_filter = SljitBindGeneratedFilterPrimitive(ops, primitive.filter_idx);
			auto source_projection = SljitBindProjectionChainPrimitive(ops, primitive.source_projection_idx);
			return MakePrimitiveSequence({SljitFullPipelinePrimitiveStep::SourceFetch(),
			                              SljitFullPipelinePrimitiveStep::GeneratedFilter(generated_filter),
			                              SljitFullPipelinePrimitiveStep::ProjectionChain(source_projection),
			                              SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(hash_join_selection),
			                              terminal});
		}
		}
		throw InternalException("SLJIT join-projection aggregate recipe has an unknown input kind");
	}

	SljitFullPipelinePrimitiveSequence
	MakeSourceHashJoinProbeSelectionSequence(const SljitHashJoinProbeSelectionPrimitive &hash_join_selection,
	                                         const SljitFullPipelinePrimitiveStep &terminal) const {
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(hash_join_selection.hash_join_idx));
		sequence.Add(SljitFullPipelinePrimitiveStep::HashJoinProbeSelection(hash_join_selection));
		sequence.Add(terminal);
		return sequence;
	}

	SljitFullPipelinePrimitiveSequence MakeSourceHashJoinProjectionInputSequence(idx_t hash_join_idx) const {
		auto sequence = MakeSourceSequence();
		sequence.Add(SljitFullPipelinePrimitiveStep::SourceBatchBoundary(hash_join_idx));
		sequence.Add(MakeHashJoinProbeProjectionInputStep(hash_join_idx));
		return sequence;
	}

	SljitPostJoinProjectionAggregatePrimitive
	BindPostJoinProjectionAggregatePrimitive(const SljitFullPipelineProjectionAggregateShape &shape,
	                                         idx_t hash_join_idx,
	                                         SljitDirectJoinOutputAggregateUpdateSchedule update_schedule) const {
		SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
		post_join_aggregate.post_join_projection = SljitBindPostJoinProjectionPrimitive(
		    ops, hash_join_idx, shape.first_projection_idx, shape.final_projection_idx);
		post_join_aggregate.direct_join_output_aggregate =
		    SljitBindDirectJoinOutputAggregatePrimitive(ops, shape.aggregate_idx, update_schedule);
		return post_join_aggregate;
	}

	SljitPostJoinProjectionAggregatePrimitive
	BindPostJoinProjectionAggregatePrimitive(const SljitJoinProjectionAggregatePrimitive &primitive) const {
		SljitPostJoinProjectionAggregatePrimitive post_join_aggregate;
		post_join_aggregate.post_join_projection = primitive.post_join_projection;
		post_join_aggregate.direct_join_output_aggregate = primitive.direct_join_output_aggregate;
		return post_join_aggregate;
	}

	SljitFullPipelineRecipe
	MakeProjectionGroupedAggregateRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                     const SljitFullPipelineProjectionAggregateShape &shape) const {
		if (!SljitCanBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx)) {
			throw InternalException("SLJIT selected projection aggregate recipe cannot bind projection chain");
		}
		if (SljitCanBindProjectedInputGroupedAggregateUpdatePrimitive(
		        ops, shape.first_projection_idx, shape.final_projection_idx, shape.aggregate_idx)) {
			auto grouped_update = SljitBindProjectedInputGroupedAggregateUpdatePrimitive(
			    ops, shape.first_projection_idx, shape.final_projection_idx, shape.aggregate_idx);
			sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update));
			return MakePrimitiveSequence(std::move(sequence));
		}
		auto projection =
		    SljitBindProjectionChainPrimitive(ops, shape.first_projection_idx, shape.final_projection_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
		auto grouped_update = SljitBindGroupedAggregateUpdatePrimitive(ops, shape.aggregate_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::GroupedAggregateUpdate(grouped_update));
		return MakePrimitiveSequence(std::move(sequence));
	}

	SljitFullPipelineRecipe
	MakeProjectionAggregateTailRecipe(SljitFullPipelinePrimitiveSequence sequence,
	                                  const SljitFullPipelineProjectionAggregateShape &shape) const {
		if (SelectedProjectionAggregateHasDedicatedBackend(shape)) {
			return MakeProjectionGroupedAggregateRecipe(std::move(sequence), shape);
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
		auto projection = SljitBindProjectionChainPrimitive(ops, first_projection_idx, final_projection_idx);
		sequence.Add(SljitFullPipelinePrimitiveStep::ProjectionChain(projection));
		return MakeNativeTailRecipe(std::move(sequence), tail_start_idx);
	}

private:
	const vector<SljitExecutableRegionOp> &ops;
	const vector<Value> &source_min_values;
	const vector<Value> &source_max_values;
	bool uses_scan_filters;
};

} // namespace duckdb
