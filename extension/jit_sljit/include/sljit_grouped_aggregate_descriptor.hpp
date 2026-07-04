//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sljit_grouped_aggregate_descriptor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sljit_aggregate_fused_payload_sources.hpp"
#include "sljit_grouped_aggregate_group_key_source.hpp"
#include "sljit_join_projection_aggregate_state.hpp"
#include "sljit_projection_source_runtime.hpp"
#include "sljit_region_runtime_state.hpp"

namespace duckdb {

template <class HANDLE_COUNT_STAR, class HANDLE_FUSED_SOURCE, class CHECK_DIRECT_PAYLOAD, class HANDLE_DIRECT_PAYLOAD,
          class SET_BLOCKER>
static bool SljitTryBuildAggregatePayloadSourceIndices(
    SljitExecutableAggregateUpdate &aggregate_update, const vector<ExecutionRegionAggregateInput> &aggregates,
    bool &uses_fused_update, const char *payload_reference_blocker, HANDLE_COUNT_STAR &&handle_count_star,
    HANDLE_FUSED_SOURCE &&handle_fused_source, CHECK_DIRECT_PAYLOAD &&check_direct_payload,
    HANDLE_DIRECT_PAYLOAD &&handle_direct_payload, SET_BLOCKER &&set_blocker) {
	uses_fused_update = false;
	vector<idx_t> fused_payload_sources;
	if (aggregate_update.fused_payload_update_function &&
	    SljitFusedAggregatePayloadsUseTypedExpressionTrees(aggregate_update.payloads, aggregates) &&
	    SljitTryGetFusedTypedPayloadCombinedSources(aggregate_update.payloads, aggregates, fused_payload_sources)) {
		for (auto source_idx : fused_payload_sources) {
			if (!handle_fused_source(source_idx)) {
				return false;
			}
		}
		uses_fused_update = true;
		return true;
	}

	for (idx_t payload_idx = 0; payload_idx < aggregates.size(); payload_idx++) {
		auto &aggregate = aggregates[payload_idx];
		auto &payload = aggregate_update.payloads[payload_idx].plan;
		if (aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT_STAR) {
			if (aggregate.child_count != 0) {
				return set_blocker("count_star_payload");
			}
			if (!handle_count_star()) {
				return false;
			}
			continue;
		}
		if (aggregate.child_count != 1 || aggregate.child_types.size() != 1) {
			return set_blocker("payload_contract");
		}
		if (!check_direct_payload(aggregate, payload_idx)) {
			return false;
		}
		if (payload.kind != SljitNativeRegionExpressionKind::REFERENCE ||
		    payload.source_index != aggregate.payload_index) {
			return set_blocker(payload_reference_blocker);
		}
		if (!handle_direct_payload(aggregate, payload_idx)) {
			return false;
		}
	}
	return true;
}

static bool SljitTryAddJoinProjectionAggregateInput(SljitJoinProjectionAggregateDescriptor &descriptor,
                                                    vector<idx_t> &projection_to_input, idx_t projection_idx,
                                                    idx_t &input_idx) {
	auto &projection_op = descriptor.Projection();
	if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
		return false;
	}
	if (projection_to_input[projection_idx] != DConstants::INVALID_INDEX) {
		input_idx = projection_to_input[projection_idx];
		return true;
	}
	auto &projection = projection_op.projections[projection_idx];
	auto &output_type = projection_op.output_types[projection_idx];
	if (projection.plan.return_type != output_type) {
		return false;
	}
	input_idx = descriptor.input_sources.size();
	projection_to_input[projection_idx] = input_idx;
	descriptor.output_to_projection.push_back(projection_idx);
	descriptor.input_types.push_back(output_type);
	SljitJoinProjectionAggregateInputSource source;
	source.kind = SljitJoinProjectionAggregateInputKind::PROJECTION_OUTPUT;
	source.projection_idx = projection_idx;
	source.type = output_type;
	descriptor.input_sources.push_back(std::move(source));
	return true;
}

static bool SljitTryAddJoinLHSInputAggregateInput(SljitJoinProjectionAggregateDescriptor &descriptor,
                                                  idx_t input_column_idx, const LogicalType &input_type,
                                                  idx_t &input_idx) {
	for (idx_t existing_idx = 0; existing_idx < descriptor.input_sources.size(); existing_idx++) {
		auto &source = descriptor.input_sources[existing_idx];
		if (source.kind == SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT &&
		    source.input_idx == input_column_idx && source.type == input_type) {
			input_idx = existing_idx;
			return true;
		}
	}
	input_idx = descriptor.input_sources.size();
	SljitJoinProjectionAggregateInputSource source;
	source.kind = SljitJoinProjectionAggregateInputKind::HASH_JOIN_LHS_INPUT;
	source.input_idx = input_column_idx;
	source.type = input_type;
	descriptor.input_sources.push_back(std::move(source));
	descriptor.output_to_projection.push_back(DConstants::INVALID_INDEX);
	descriptor.input_types.push_back(input_type);
	return true;
}

static bool SljitTryAddJoinLHSInputAggregateInputFromProjection(const ExecutionHashJoinProbeBinding &binding,
	SljitJoinProjectionAggregateDescriptor &descriptor,
	SljitExecutableRegionOp &projection_op,
	idx_t projection_idx, idx_t &input_idx,
	optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr) {
	if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
		return false;
	}
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
	                                                   join_output_source_index) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan)) {
		return false;
	}
	if (descriptor.has_producer_output_column_map &&
	    join_output_source_index < descriptor.producer_output_column_map.size()) {
		if (join_output_source_index >= binding.lhs_output_column_indices.size() || !producer_projection_op) {
			return false;
		}
		const auto projected_input_idx = binding.lhs_output_column_indices[join_output_source_index];
		if (projected_input_idx >= descriptor.producer_output_column_map.size() ||
		    projected_input_idx >= producer_projection_op->projections.size()) {
			return false;
		}
		const auto input_col = descriptor.producer_output_column_map[projected_input_idx];
		if (input_col == DConstants::INVALID_INDEX) {
			return false;
		}
		SljitExecutableRegionExpression producer_expr;
		idx_t producer_source_idx;
		if (!SljitTryBuildSingleSourceProjectionExpression(producer_projection_op->projections[projected_input_idx],
		                                                   producer_expr, producer_source_idx) ||
		    !SljitProjectionIsSingleSourceReferenceLike(producer_expr.plan) || producer_source_idx != input_col) {
			return false;
		}
		auto &source_type = projection_op.output_types[projection_idx];
		if (remapped_expr.plan.return_type != source_type || producer_expr.plan.return_type != source_type) {
			return false;
		}
		return SljitTryAddJoinLHSInputAggregateInput(descriptor, input_col, source_type, input_idx);
	}
	if (join_output_source_index >= binding.lhs_output_column_indices.size() ||
	    join_output_source_index >= binding.output_types.size()) {
		return false;
	}
	auto &source_type = binding.output_types[join_output_source_index];
	auto &projection_type = projection_op.output_types[projection_idx];
	if (remapped_expr.plan.return_type != projection_type || source_type != projection_type) {
		return false;
	}
	return SljitTryAddJoinLHSInputAggregateInput(
	    descriptor, binding.lhs_output_column_indices[join_output_source_index], source_type, input_idx);
}

static bool SljitInputVectorGroupKeyHasOriginalInput(const ExecutionRowPointerGroupKeySource &group_source) {
	return group_source.ready && group_source.source_kind == ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR &&
	       group_source.input_vector_index != DConstants::INVALID_INDEX &&
	       group_source.source_type.InternalType() != PhysicalType::INVALID;
}

static bool SljitProjectionOutputKnownNotNull(SljitExecutableRegionOp &projection_op, idx_t projection_idx) {
	return projection_idx < projection_op.output_not_null.size() && projection_op.output_not_null[projection_idx];
}

static bool SljitProjectionReferencesAllValidHashJoinRHSColumn(const ExecutionHashJoinProbeBinding &binding,
                                                               SljitExecutableRegionOp &projection_op,
                                                               idx_t projection_idx) {
	if (projection_idx >= projection_op.projections.size()) {
		return false;
	}
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
	                                                   join_output_source_index) ||
	    !SljitProjectionIsSingleSourceReferenceLike(remapped_expr.plan)) {
		return false;
	}
	const auto lhs_column_count = binding.lhs_output_column_indices.size();
	if (join_output_source_index < lhs_column_count) {
		return false;
	}
	const auto rhs_col_idx = join_output_source_index - lhs_column_count;
	ExecutionHashJoinRHSFixedColumnSource rhs_source;
	return ExecutionGetHashJoinRHSFixedColumnSource(binding, rhs_col_idx, rhs_source) && rhs_source.all_valid;
}

static bool SljitProjectionOutputIsCountOnePayload(const ExecutionHashJoinProbeBinding &binding,
                                                   SljitExecutableRegionOp &projection_op, idx_t projection_idx) {
	return SljitProjectionOutputKnownNotNull(projection_op, projection_idx) ||
	       SljitProjectionReferencesAllValidHashJoinRHSColumn(binding, projection_op, projection_idx);
}

static bool SljitTryBuildProducerReferenceGroupCastPlan(const LogicalType &source_type, const LogicalType &target_type,
                                                        SljitNativeRegionExpressionPlan &plan) {
	auto signed_width = [](PhysicalType type, SljitNativeSignedIntegerWidth &width) {
		switch (type) {
		case PhysicalType::INT8:
			width = SljitNativeSignedIntegerWidth::INT8;
			return true;
		case PhysicalType::INT16:
			width = SljitNativeSignedIntegerWidth::INT16;
			return true;
		case PhysicalType::INT32:
			width = SljitNativeSignedIntegerWidth::INT32;
			return true;
		case PhysicalType::INT64:
			width = SljitNativeSignedIntegerWidth::INT64;
			return true;
		default:
			return false;
		}
	};
	SljitNativeSignedIntegerWidth source_width;
	SljitNativeSignedIntegerWidth target_width;
	if (!signed_width(source_type.InternalType(), source_width) ||
	    !signed_width(target_type.InternalType(), target_width)) {
		return false;
	}
	plan = SljitNativeRegionExpressionPlan();
	plan.kind = SljitNativeRegionExpressionKind::INTEGER_CAST;
	plan.return_type = target_type;
	plan.source_index = 0;
	plan.cast_source_width = source_width;
	plan.cast_target_width = target_width;
	plan.try_cast = false;
	return true;
}

static bool SljitTryMapProducerGroupSource(
    const ExecutionHashJoinProbeBinding &binding, const SljitJoinProjectionAggregateDescriptor &descriptor,
    SljitExecutableRegionOp &projection_op, optional_ptr<SljitExecutableRegionOp> producer_projection_op,
    optional_ptr<const vector<idx_t>> semantic_to_projection, const ExecutionRegionGroupInput &group,
    ExecutionRowPointerGroupKeySource &group_source, bool &mapped, string &blocker) {
	mapped = false;
	if (!producer_projection_op || !descriptor.has_producer_output_column_map ||
	    group_source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
		return true;
	}
	idx_t projection_idx;
	if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, group.input_index,
	                                            projection_idx)) {
		blocker = "producer_group_projection_index";
		return false;
	}
	SljitExecutableRegionExpression remapped_expr;
	idx_t join_output_source_index;
	if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
	                                                   join_output_source_index)) {
		blocker = "producer_group_projection_source";
		return false;
	}
	idx_t projected_input_idx;
	idx_t condition_idx;
	bool repeats_with_row_pointer;
	if (!SljitTryResolveHashJoinMatchedProbeInputForOutput(binding, join_output_source_index, projected_input_idx,
	                                                       condition_idx, repeats_with_row_pointer)) {
		return true;
	}
	if (projected_input_idx >= descriptor.producer_output_column_map.size() ||
	    projected_input_idx >= producer_projection_op->projections.size()) {
		blocker = "producer_group_output_map";
		return false;
	}
	const auto input_col = descriptor.producer_output_column_map[projected_input_idx];
	if (input_col == DConstants::INVALID_INDEX) {
		blocker = "producer_group_output_map";
		return false;
	}
	SljitExecutableRegionExpression producer_expr;
	idx_t producer_source_idx;
	if (!SljitTryBuildSingleSourceProjectionExpression(producer_projection_op->projections[projected_input_idx],
	                                                   producer_expr, producer_source_idx)) {
		blocker = "producer_group_projection_source";
		return false;
	}
	if (producer_source_idx != input_col) {
		blocker = "producer_group_map_mismatch_projection_source_" + to_string(producer_source_idx) + "_map_" +
		          to_string(input_col);
		return false;
	}
	if (producer_source_idx >= producer_projection_op->input_types.size()) {
		blocker = "producer_group_source_bounds";
		return false;
	}
	const auto &source_type = producer_projection_op->input_types[producer_source_idx];
	ExecutionRowPointerGroupKeySource producer_group_source;
	SljitInitializeInputVectorGroupKeySource(input_col, source_type, group.type, producer_group_source, condition_idx);
	producer_group_source.input_vector_repeats_with_row_pointer = repeats_with_row_pointer;
	SljitAttachHashJoinBuildConditionType(binding, producer_group_source, condition_idx);
	if (!SljitTryFinalizeRowPointerGroupKeySource(producer_expr.plan, group.type, producer_group_source)) {
		SljitNativeRegionExpressionPlan producer_cast_plan;
		if (!SljitProjectionIsSingleSourceReferenceLike(producer_expr.plan) ||
		    !SljitTryBuildProducerReferenceGroupCastPlan(source_type, group.type, producer_cast_plan) ||
		    !SljitTryFinalizeRowPointerGroupKeySource(producer_cast_plan, group.type, producer_group_source)) {
			blocker = "producer_group_cast_kind_" + to_string(static_cast<int>(producer_expr.plan.kind)) + "_source_" +
			          source_type.ToString() + "_return_" + producer_expr.plan.return_type.ToString() + "_target_" +
			          group.type.ToString() + "_source_index_" + to_string(producer_expr.plan.source_index) + "_cast_" +
			          to_string(static_cast<int>(producer_expr.plan.cast_source_width)) + "_" +
			          to_string(static_cast<int>(producer_expr.plan.cast_target_width));
			return false;
		}
	}
	group_source = std::move(producer_group_source);
	mapped = true;
	return true;
}

static bool SljitTryBuildProjectionRowPointerAggregateDescriptor(
    const ExecutionHashJoinProbeBinding &binding, SljitExecutableRegionOp &aggregate_op,
    SljitJoinProjectionAggregateDescriptor &descriptor,
    optional_ptr<const vector<idx_t>> semantic_to_projection = nullptr,
    optional_ptr<SljitExecutableRegionOp> producer_projection_op = nullptr) {
	auto &aggregate_update = aggregate_op.aggregate_update;
	auto &sink_info = aggregate_update.plan.sink_info;
	auto &projection_op = descriptor.Projection();
	const bool uses_distinct_count_pointer = sink_info.aggregate_contract.distinct_count_pointer_keys;
	if (sink_info.kind != ExecutionRegionSinkKind::HASH_AGGREGATE_UPDATE || sink_info.groups.empty() ||
	    (!uses_distinct_count_pointer && !aggregate_update.plan.use_primitive_payloads) ||
	    (!uses_distinct_count_pointer && !aggregate_update.plan.use_grouped_state_addresses) ||
	    aggregate_update.plan.use_perfect_hash_group_lookup ||
	    aggregate_update.fused_payload_update_owns_group_lookup ||
	    (!uses_distinct_count_pointer && aggregate_update.payloads.size() != sink_info.aggregates.size())) {
		return descriptor.Block("aggregate_shape");
	}
	string group_source_blocker;
	vector<uint8_t> group_source_uses_projection_output;
	if (!SljitTryBuildRowPointerGroupKeySources(binding, projection_op, aggregate_op, descriptor.group_sources,
	                                            semantic_to_projection,
	                                            optional_ptr<vector<uint8_t>>(&group_source_uses_projection_output),
	                                            optional_ptr<string>(&group_source_blocker))) {
		if (group_source_blocker.empty()) {
			group_source_blocker = "group_sources";
		}
		return descriptor.Block(group_source_blocker.c_str());
	}
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		bool mapped = false;
		string producer_group_blocker;
		if (!SljitTryMapProducerGroupSource(binding, descriptor, projection_op, producer_projection_op,
		                                    semantic_to_projection, sink_info.groups[group_idx],
		                                    descriptor.group_sources[group_idx], mapped, producer_group_blocker)) {
			if (producer_group_blocker.empty()) {
				producer_group_blocker = "producer_group_source";
			}
			return descriptor.Block(producer_group_blocker.c_str());
		}
		if (mapped) {
			group_source_uses_projection_output[group_idx] = 0;
		}
	}

	vector<idx_t> projection_to_input(projection_op.projections.size(), DConstants::INVALID_INDEX);
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		auto &group = sink_info.groups[group_idx];
		auto &group_source = descriptor.group_sources[group_idx];
		if (group_source.source_kind != ExecutionRowPointerGroupKeySourceKind::INPUT_VECTOR) {
			continue;
		}
		idx_t input_idx;
			const bool uses_projection_output = group_idx < group_source_uses_projection_output.size() &&
			                                    group_source_uses_projection_output[group_idx] != 0;
			if (!uses_projection_output && !SljitInputVectorGroupSourceUsesProjection(group_source) &&
			    SljitInputVectorGroupKeyHasOriginalInput(group_source)) {
				if (!SljitTryAddJoinLHSInputAggregateInput(descriptor, group_source.input_vector_index,
				                                           group_source.source_type, input_idx)) {
					return descriptor.Block("input_group_source");
			}
			group_source.input_vector_index = input_idx;
		} else {
			idx_t projection_idx;
			if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, group.input_index,
			                                            projection_idx) ||
			    projection_op.output_types[projection_idx].InternalType() != group.type.InternalType() ||
			    !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) {
				return descriptor.Block("input_group_projection");
			}
			const bool repeats_with_row_pointer = group_source.input_vector_repeats_with_row_pointer;
			SljitInitializeInputVectorGroupKeySource(input_idx, group.type, group.type, group_source);
			group_source.input_vector_repeats_with_row_pointer = repeats_with_row_pointer;
			group_source.ready = true;
			group_source.cast_kind = ExecutionRowPointerGroupKeyCastKind::NONE;
		}
	}
	bool descriptor_uses_row_pointer_group_source = false;
	for (auto &group_source : descriptor.group_sources) {
		if (group_source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			descriptor_uses_row_pointer_group_source = true;
			break;
		}
	}

	if (uses_distinct_count_pointer) {
		if (sink_info.aggregates.size() != 1) {
			return descriptor.Block("distinct_payload_count");
		}
		auto &aggregate = sink_info.aggregates[0];
		if (aggregate.child_count != 1 || aggregate.child_types.size() != 1) {
			return descriptor.Block("distinct_payload_contract");
		}
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, aggregate.payload_index,
		                                            projection_idx)) {
			return descriptor.Block("distinct_payload_projection");
		}
		if (projection_idx >= projection_op.output_types.size() ||
		    projection_op.output_types[projection_idx].InternalType() != aggregate.child_types[0].InternalType()) {
			return descriptor.Block("distinct_payload_type");
		}
		idx_t input_idx;
		if ((!SljitTryAddJoinLHSInputAggregateInputFromProjection(
		         binding, descriptor, projection_op, projection_idx, input_idx, producer_projection_op) &&
		     !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) ||
		    descriptor.input_types[input_idx].InternalType() != aggregate.child_types[0].InternalType()) {
			return descriptor.Block("distinct_payload_projection");
		}
		descriptor.payload_source_indices.push_back(input_idx);
		descriptor.MarkReady();
		return true;
	}

	auto add_count_star_payload = [&]() {
		descriptor.payload_source_indices.push_back(DConstants::INVALID_INDEX);
		return true;
	};
	auto add_fused_payload_source = [&](idx_t semantic_projection_idx) {
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, semantic_projection_idx,
		                                            projection_idx)) {
			return descriptor.Block("fused_payload_projection");
		}
		idx_t input_idx;
		if (!SljitTryAddJoinLHSInputAggregateInputFromProjection(
		        binding, descriptor, projection_op, projection_idx, input_idx, producer_projection_op) &&
		    !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) {
			return descriptor.Block("fused_payload_projection");
		}
		descriptor.payload_source_indices.push_back(input_idx);
		return true;
	};
	auto add_direct_payload_source = [&](const ExecutionRegionAggregateInput &aggregate, idx_t) {
		idx_t projection_idx;
		if (!SljitTryResolveProjectionSemanticIndex(projection_op, semantic_to_projection, aggregate.payload_index,
		                                            projection_idx)) {
			return descriptor.Block("payload_projection");
		}
		if (!descriptor_uses_row_pointer_group_source &&
		    aggregate.primitive_update_kind == AggregatePrimitiveUpdateKind::COUNT &&
		    SljitProjectionOutputIsCountOnePayload(binding, projection_op, projection_idx)) {
			descriptor.payload_source_indices.push_back(DConstants::INVALID_INDEX);
			return true;
		}
		idx_t input_idx;
		if ((!SljitTryAddJoinLHSInputAggregateInputFromProjection(
		         binding, descriptor, projection_op, projection_idx, input_idx, producer_projection_op) &&
		     !SljitTryAddJoinProjectionAggregateInput(descriptor, projection_to_input, projection_idx, input_idx)) ||
		    descriptor.input_types[input_idx].InternalType() != aggregate.child_types[0].InternalType()) {
			return descriptor.Block("payload_projection");
		}
		descriptor.payload_source_indices.push_back(input_idx);
		return true;
	};
	bool uses_fused_payload_update;
	auto check_direct_payload = [&](const ExecutionRegionAggregateInput &, idx_t) {
		return true;
	};
	if (!SljitTryBuildAggregatePayloadSourceIndices(
	        aggregate_update, sink_info.aggregates, uses_fused_payload_update, "payload_contract",
	        add_count_star_payload, add_fused_payload_source, check_direct_payload, add_direct_payload_source,
	        [&](const char *blocker) { return descriptor.Block(blocker); })) {
		return false;
	}
	D_ASSERT(!uses_fused_payload_update || !descriptor.payload_source_indices.empty());
	if (descriptor.payload_source_indices.empty()) {
		return descriptor.Block("payload_sources");
	}
	descriptor.MarkReady();
	return true;
}

template <class INPUT_COLUMN_IS_OMITTED>
static bool SljitTryBuildProjectionInputRowPointerGroupDescriptor(SljitExecutableRegionOp &projection_op,
                                                                  SljitExecutableRegionOp &aggregate_op,
                                                                  DataChunk &payload_input,
                                                                  const vector<idx_t> &group_projection_indices,
                                                                  INPUT_COLUMN_IS_OMITTED input_column_is_omitted,
                                                                  SljitJoinProjectionAggregateDescriptor &descriptor) {
	auto &sink_info = aggregate_op.aggregate_update.plan.sink_info;
	if (sink_info.groups.size() != group_projection_indices.size() || descriptor.payload_source_indices.empty()) {
		return descriptor.Block("aggregate_shape");
	}
	descriptor.group_sources.clear();
	descriptor.group_sources.reserve(sink_info.groups.size());
	for (idx_t group_idx = 0; group_idx < sink_info.groups.size(); group_idx++) {
		auto &group = sink_info.groups[group_idx];
		const auto projection_idx = group_projection_indices[group_idx];
		if (projection_idx >= projection_op.projections.size() || projection_idx >= projection_op.output_types.size()) {
			return descriptor.Block("group_key_index");
		}
		SljitExecutableRegionExpression remapped_expr;
		idx_t input_source_idx;
		if (!SljitTryBuildSingleSourceProjectionExpression(projection_op.projections[projection_idx], remapped_expr,
		                                                   input_source_idx)) {
			return descriptor.Block("group_key_projection");
		}
		if (input_source_idx >= payload_input.ColumnCount()) {
			return descriptor.Block("group_key_source");
		}
		if (input_column_is_omitted(input_source_idx)) {
			return descriptor.Block("group_key_omitted_input");
		}
		auto &input_type = payload_input.data[input_source_idx].GetType();
		ExecutionRowPointerGroupKeySource group_source;
		SljitInitializeInputVectorGroupKeySource(input_source_idx, input_type, group.type, group_source);
		if (!SljitTryFinalizeRowPointerGroupKeySource(remapped_expr.plan, group.type, group_source)) {
			return descriptor.Block("group_key_cast");
		}
		descriptor.group_sources.push_back(std::move(group_source));
	}
	if (descriptor.group_sources.empty()) {
		return descriptor.Block("group_sources");
	}
	descriptor.MarkReady();
	return true;
}

template <class INPUT_COLUMN_IS_OMITTED>
static bool SljitTryBuildProjectionInputRowPointerAggregateDescriptor(
    SljitExecutableRegionOp &projection_op, SljitExecutableRegionOp &aggregate_op, DataChunk &payload_input,
    const vector<idx_t> &group_projection_indices, const vector<idx_t> &payload_source_indices,
    const char *disabled_reason, INPUT_COLUMN_IS_OMITTED input_column_is_omitted,
    SljitJoinProjectionAggregateDescriptor &descriptor) {
	if (descriptor.Built()) {
		return descriptor.Ready();
	}
	descriptor.ClearBuiltState();
	descriptor.BorrowProjection(projection_op);
	if (disabled_reason) {
		return descriptor.Block(disabled_reason);
	}
	if (payload_source_indices.empty()) {
		return descriptor.Block("payload_sources");
	}
	descriptor.payload_source_indices = payload_source_indices;
	return SljitTryBuildProjectionInputRowPointerGroupDescriptor(
	    projection_op, aggregate_op, payload_input, group_projection_indices, input_column_is_omitted, descriptor);
}

static bool SljitDescriptorUsesRowPointerGroupSource(const SljitJoinProjectionAggregateDescriptor &descriptor) {
	for (auto &source : descriptor.group_sources) {
		if (source.source_kind == ExecutionRowPointerGroupKeySourceKind::ROW_POINTER_FIELD) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
